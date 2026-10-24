/* Original retained terminal scan. Native base/era/authority remain separate.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#include "postgres.h"
#include "cluster/cluster_relmap_terminal_scan.h"
#ifdef USE_PGRAC_CLUSTER
#include <unistd.h>
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_recovery_record_hash.h"
#include "cluster/cluster_relmap_recovery.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_retention.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_undo_recovery.h"
#include "miscadmin.h"
#include "replication/origin.h"

struct ClusterRelmapTerminalScanV1 {
	pid_t pid;
	ClusterWalInputsV1 *inputs;
	uint32 index, count;
	ClusterWalInputV1 selected;
	ClusterRelmapTerminalObservationV1 observation;
	pg_cryptohash_ctx *hash;
	XLogRecPtr last_end;
	bool checkpoint_seen, terminal_seen, failed, prefix_only, native_base_retained;
	bool pending_required, pending_seen, execution_owner;
	uint64 pending_scope_serial;
	uint32 native_terminals, expected_native_terminals;
	ClusterRelmapV2Image pending;
};

static void
terminal_scan_free(ClusterRelmapTerminalScanV1 *scan)
{
	if (scan == NULL)
		return;
	if (scan->hash != NULL)
		pg_cryptohash_free(scan->hash);
	pfree(scan);
}

static bool
terminal_scan_xact(ClusterRelmapTerminalScanV1 *scan, XLogReaderState *reader)
{
	TransactionId xid = XLogRecGetXid(reader);
	TransactionId target = (TransactionId)scan->observation.requested_top_full_xid;
	uint8 info = XLogRecGetInfo(reader);
	bool commit = (info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT;
	bool aborting = (info & XLOG_XACT_OPMASK) == XLOG_XACT_ABORT;
	const TransactionId *children;
	int count;

	/* Validate every native XACT carrier, including later unrelated parents:
	 * a queried top must not be hidden as another terminal's child. Unknown
	 * assignment/prepare/invalidation carriers are unsupported, never skipped. */
	if ((!commit && !aborting) || !TransactionIdIsNormal(xid) || XLogRecMaxBlockId(reader) >= 0
		|| reader->record->has_page_version_edge || XLogRecGetOrigin(reader) != InvalidRepOriginId
		|| TransactionIdIsValid(reader->record->toplevel_xid)
		|| !rf_side_xact_completion_shape_v1(reader, commit))
		return false;
	if (commit) {
		xl_xact_parsed_commit parsed;
		if (!ParseCommitRecord(info, (xl_xact_commit *)XLogRecGetData(reader),
							   XLogRecGetDataLen(reader), &parsed)
			|| (parsed.xinfo & XACT_XINFO_HAS_TWOPHASE)
			|| !rf_side_xact_children_preflight_v1(xid, parsed.nsubxacts, parsed.subxacts)
			|| (parsed.has_tt_commit
				&& !rf_side_xact_tt_commit_preflight_v1(
					&parsed.tt_commit, scan->selected.source.claim.identity.origin_thread_id, xid,
					parsed.scn)))
			return false;
		children = parsed.subxacts;
		count = parsed.nsubxacts;
	} else {
		xl_xact_parsed_abort parsed;
		ParseAbortRecord(info, (xl_xact_abort *)XLogRecGetData(reader), &parsed);
		if ((parsed.xinfo & XACT_XINFO_HAS_TWOPHASE)
			|| !rf_side_xact_children_preflight_v1(xid, parsed.nsubxacts, parsed.subxacts))
			return false;
		children = parsed.subxacts;
		count = parsed.nsubxacts;
	}
	if (xid != target) {
		for (int i = 0; i < count; i++) {
			TransactionId child;
			memcpy(&child, children + i, sizeof(child));
			if (child == target)
				return false;
		}
	} else if (scan->terminal_seen
			   || (scan->prefix_only
					   ? (scan->checkpoint_seen || reader->ReadRecPtr < scan->selected.native_redo
						  || reader->EndRecPtr > scan->selected.checkpoint_start)
					   : !scan->checkpoint_seen))
		return false;
	if (scan->pending_required
		&& (scan->native_terminals == UINT32_MAX - 1
			|| !cluster_undo_recovery_cold_terminal_matches_v1(
				&scan->selected.source, &scan->selected.checkpoint, scan->selected.native_redo,
				scan->pending_scope_serial, reader)))
		return false;
	if (scan->pending_required)
		scan->native_terminals++;
	if (xid != target)
		return true;
	scan->terminal_seen = true;
	scan->observation.status = commit ? TRANSACTION_STATUS_COMMITTED : TRANSACTION_STATUS_ABORTED;
	scan->observation.terminal_start = reader->ReadRecPtr;
	scan->observation.terminal_end = reader->EndRecPtr;
	return true;
}

/* Native CLOG lifecycle is independent of finding a terminal record. Keep
 * observing the full source even after a base-invalidating operation, so the
 * original cut/ROOT checks still apply to the terminal-only observation. */
static bool
terminal_scan_clog_retains_base(const ClusterRelmapTerminalScanV1 *scan, XLogReaderState *reader)
{
	const char *data = XLogRecGetData(reader);
	uint32 length = XLogRecGetDataLen(reader);
	uint8 info = XLogRecGetInfo(reader);
	uint32 per_page = BLCKSZ * 4; /* Original native two-bit CLOG geometry. */
	TransactionId target = (TransactionId)scan->observation.requested_top_full_xid;

	if (XLogRecGetXid(reader) != InvalidTransactionId || XLogRecMaxBlockId(reader) >= 0
		|| reader->record->has_page_version_edge || XLogRecGetOrigin(reader) != InvalidRepOriginId
		|| TransactionIdIsValid(reader->record->toplevel_xid) || data == NULL)
		return false;
	if (info == CLOG_ZEROPAGE) {
		int page;
		if (length != sizeof(page))
			return false;
		memcpy(&page, data, sizeof(page));
		if (page < 0 || (uint64)page > UINT32_MAX / per_page)
			return false;
		return !scan->terminal_seen || (uint32)page != target / per_page;
	}
	if (info == CLOG_TRUNCATE) {
		xl_clog_truncate trunc;
		if (length != sizeof(trunc))
			return false;
		memcpy(&trunc, data, sizeof(trunc));
		if (trunc.pageno < 0 || !TransactionIdIsNormal(trunc.oldestXact)
			|| (uint64)trunc.pageno != trunc.oldestXact / per_page)
			return false;
		/* The native cutoff is raw32. Without an independently qualified
		 * cutoff era, even an apparently older cutoff cannot prove retained
		 * FullXid lifetime after this terminal. Do not guess wrap ordering. */
		return !scan->terminal_seen;
	}
	return false;
}

static bool
terminal_scan_visit(XLogReaderState *reader, void *arg)
{
	ClusterRelmapTerminalScanV1 *scan = arg;
	const ClusterWalInputV1 *input = &scan->selected;
	const ClusterControlRootIdentity *id = &input->source.claim.identity;
	TransactionId target = (TransactionId)scan->observation.requested_top_full_xid;
	uint8 info;

	CHECK_FOR_INTERRUPTS();
	if (reader == NULL || reader->record == NULL || reader->cluster_record_crc_failed
		|| reader->cluster_expected_thread_id != id->origin_thread_id
		|| reader->ReadRecPtr != reader->record->lsn
		|| reader->EndRecPtr != reader->record->next_lsn
		|| reader->ReadRecPtr < input->checkpoint.checkpoint_lower_lsn
		|| reader->ReadRecPtr < scan->last_end || reader->EndRecPtr <= reader->ReadRecPtr
		|| reader->EndRecPtr > input->checkpoint.validated_tail_lsn_exclusive)
		return false;
	info = XLogRecGetInfo(reader);
	if (reader->ReadRecPtr == input->checkpoint_start) {
		if (scan->checkpoint_seen || XLogRecGetRmid(reader) != RM_XLOG_ID
			|| (info != XLOG_CHECKPOINT_ONLINE && info != XLOG_CHECKPOINT_SHUTDOWN)
			|| XLogRecGetXid(reader) != InvalidTransactionId || XLogRecMaxBlockId(reader) >= 0
			|| reader->record->has_page_version_edge
			|| XLogRecGetOrigin(reader) != InvalidRepOriginId
			|| TransactionIdIsValid(reader->record->toplevel_xid) || XLogRecGetData(reader) == NULL
			|| XLogRecGetDataLen(reader) != sizeof(CheckPoint)
			|| memcmp(XLogRecGetData(reader), &input->native_checkpoint, sizeof(CheckPoint)) != 0
			|| !(scan->prefix_only
					 ? cluster_relmap_checkpoint_window_decode_v2(
						   input->native_window, &input->native_checkpoint, reader->ReadRecPtr,
						   reader->EndRecPtr, id, input->source.claim.database_incarnation,
						   &scan->observation.window, &scan->observation.post_flush_issued)
					 : cluster_relmap_checkpoint_window_decode_v1(
						   input->native_window, &input->native_checkpoint, reader->ReadRecPtr,
						   reader->EndRecPtr, id, input->source.claim.database_incarnation,
						   &scan->observation.window))
			|| scan->observation.requested_top_full_xid < scan->observation.window.native_floor
			|| scan->observation.requested_top_full_xid > scan->observation.window.last_issued)
			return false;
		if (scan->prefix_only)
			scan->observation.prefix_end = reader->EndRecPtr;
		scan->checkpoint_seen = true;
	}
	if (scan->pending_required && reader->EndRecPtr == scan->pending.pending_owner.wal_end) {
		if (scan->pending_seen || scan->terminal_seen
			|| !cluster_relmap_owner_wal_matches_v2(&scan->pending, &input->source, reader))
			return false;
		scan->pending_seen = true;
	}
	if ((scan->terminal_seen && XLogRecGetXid(reader) == target)
		|| (XLogRecGetRmid(reader) == RM_XACT_ID && !terminal_scan_xact(scan, reader)))
		return false;
	if (scan->prefix_only && XLogRecGetRmid(reader) == RM_CLOG_ID
		&& !terminal_scan_clog_retains_base(scan, reader))
		scan->native_base_retained = false;
	if (scan->observation.records == UINT64_MAX
		|| !cluster_recovery_record_hash_update(scan->hash, reader))
		return false;
	scan->observation.records++;
	scan->last_end = reader->EndRecPtr;
	return true;
}

static bool
terminal_scan_begin(ClusterRelmapTerminalScanV1 *scan)
{
	static const uint8 legacy_domain[] = "PGRAC-RELMAP-RETAINED-TERMINAL-V1";
	static const uint8 prefix_domain[] = "PGRAC-RELMAP-RETAINED-TERMINAL-PREFIX-V2";
	static const uint8 execution_domain[] = "PGRAC-RELMAP-EXECUTION-TERMINAL-PREFIX-V1";
	const uint8 *domain = scan->execution_owner ? execution_domain
						  : scan->prefix_only	? prefix_domain
												: legacy_domain;
	size_t domain_bytes = scan->execution_owner ? sizeof(execution_domain)
						  : scan->prefix_only	? sizeof(prefix_domain)
												: sizeof(legacy_domain);
	const ClusterWalInputV1 *input = &scan->selected;
	uint8 query[8];

	for (unsigned i = 0; i < sizeof(query); i++)
		query[i] = (uint8)(scan->observation.requested_top_full_xid >> (8 * i));
	scan->hash = pg_cryptohash_create(PG_SHA256);
	return scan->hash != NULL
		   && (!scan->pending_required
			   || ((scan->expected_native_terminals = cluster_undo_recovery_cold_terminal_count_v1(
						&input->source, &input->checkpoint, input->native_redo,
						scan->pending_scope_serial))
					   != UINT32_MAX
				   && scan->expected_native_terminals != 0))
		   && pg_cryptohash_init(scan->hash) == 0
		   && pg_cryptohash_update(scan->hash, domain, domain_bytes) == 0
		   && pg_cryptohash_update(scan->hash, input->native_window, 160) == 0
		   && pg_cryptohash_update(scan->hash, input->source.claim.identity.storage_uuid, 16) == 0
		   && pg_cryptohash_update(scan->hash, input->source.claim.claim_sha256, 32) == 0
		   && pg_cryptohash_update(scan->hash, query, sizeof(query)) == 0;
}

static bool
terminal_scan_fold_complete(ClusterRelmapTerminalScanV1 *scan)
{
	return scan->checkpoint_seen && scan->terminal_seen
		   && (!scan->pending_required
			   || (scan->native_terminals == scan->expected_native_terminals
				   && cluster_undo_recovery_cold_terminal_count_v1(
						  &scan->selected.source, &scan->selected.checkpoint,
						  scan->selected.native_redo, scan->pending_scope_serial)
						  == scan->expected_native_terminals
				   && scan->pending_seen && scan->native_base_retained
				   && scan->pending.pending_owner.wal_end <= scan->observation.terminal_start
				   && scan->pending.pending_owner.membership_epoch == scan->observation.window.epoch
				   && scan->pending.pending_owner.boot_incarnation
						  == scan->observation.window.boot_incarnation
				   && scan->pending.pending_owner.admitted_incarnation
						  == scan->observation.window.admitted_incarnation));
}

static bool
terminal_scan(ClusterWalInputsV1 *inputs, uint32 index, FullTransactionId requested_top,
			  bool prefix_only, const ClusterRelmapV2Image *pending, uint64 scope_serial,
			  ClusterRelmapTerminalScanV1 **out)
{
	const ClusterWalInputV1 *input;
	ClusterRelmapTerminalScanV1 *scan;
	uint8 zero[160] = { 0 };
	uint64 full = U64FromFullTransactionId(requested_top);
	uint32 count;
	bool accepted = false;

	if (out == NULL)
		return false;
	*out = NULL;
	if (CritSectionCount != 0 || !TransactionIdIsNormal(XidFromFullTransactionId(requested_top)))
		return false;
	input = cluster_wal_inputs_at_v1(inputs, index);
	if (input == NULL || input->kind != CLUSTER_WAL_INPUT_CHECKPOINT
		|| input->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| !cluster_control_root_identity_equal(&input->source.claim.identity,
												&input->checkpoint.identity)
		|| input->source.timeline != input->checkpoint.checkpoint_tli
		|| input->native_checkpoint.ThisTimeLineID != input->source.timeline
		|| input->native_checkpoint.redo != input->native_redo
		|| input->native_redo < input->checkpoint.checkpoint_lower_lsn
		|| input->native_redo > input->checkpoint_start
		|| (prefix_only && input->native_redo == input->checkpoint_start)
		|| memcmp(input->native_window, zero, sizeof(zero)) == 0)
		return false;
	count = cluster_wal_inputs_count_v1(inputs);
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *other = cluster_wal_inputs_at_v1(inputs, i);
		/* No partial history coverage: this first source seal supports only
		 * the complete single-physical-input origin. Other origins remain
		 * protected by the original whole-set ROOT/pin revalidation. */
		if (other == NULL
			|| (i != index
				&& other->source.claim.identity.origin_node_id
					   == input->source.claim.identity.origin_node_id))
			return false;
	}
	scan = palloc0(sizeof(*scan));
	scan->pid = getpid();
	scan->prefix_only = prefix_only;
	scan->native_base_retained = prefix_only;
	scan->inputs = inputs;
	scan->index = index;
	scan->count = count;
	scan->selected = *input;
	scan->pending_required = pending != NULL;
	scan->pending_scope_serial = scope_serial;
	if (pending != NULL)
		scan->pending = *pending;
	scan->observation.source = input->source;
	scan->observation.requested_top_full_xid = full;
	scan->observation.cut = input->checkpoint.validated_tail_lsn_exclusive;
	PG_TRY();
	{
		ClusterWalTailObservation observed = { 0 };
		if (terminal_scan_begin(scan)
			&& cluster_wal_inputs_visit_retained_v1(inputs, index, terminal_scan_visit, scan,
													&observed)
				   == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& terminal_scan_fold_complete(scan) && observed.records == scan->observation.records
			&& observed.complete_end == scan->observation.cut
			&& scan->last_end == scan->observation.cut
			&& pg_cryptohash_final(scan->hash, scan->observation.fingerprint, 32) == 0)
			accepted = true;
	}
	PG_CATCH();
	{
		terminal_scan_free(scan);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!accepted) {
		terminal_scan_free(scan);
		return false;
	}
	pg_cryptohash_free(scan->hash);
	scan->hash = NULL;
	*out = scan;
	return true;
}

bool
cluster_relmap_recovery_terminal_scan_v1(ClusterWalInputsV1 *inputs, uint32 index,
										 FullTransactionId requested_top,
										 ClusterRelmapTerminalScanV1 **out)
{
	return terminal_scan(inputs, index, requested_top, false, NULL, 0, out);
}

bool
cluster_relmap_recovery_terminal_prefix_scan_v2(ClusterWalInputsV1 *inputs, uint32 index,
												FullTransactionId requested_top,
												ClusterRelmapTerminalScanV1 **out)
{
	return terminal_scan(inputs, index, requested_top, true, NULL, 0, out);
}

static bool
terminal_scan_current(ClusterRelmapTerminalScanV1 *scan)
{
	const ClusterWalInputV1 *current;
	if (scan->execution_owner)
		return cluster_undo_recovery_cold_single_source_v1(
			&scan->selected.source, &scan->selected.checkpoint, scan->selected.native_redo,
			scan->pending_scope_serial);
	if (cluster_wal_inputs_revalidate_v1(scan->inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_wal_inputs_count_v1(scan->inputs) != scan->count)
		return false;
	current = cluster_wal_inputs_at_v1(scan->inputs, scan->index);
	return current != NULL && memcmp(current, &scan->selected, sizeof(*current)) == 0;
}

bool
cluster_relmap_recovery_terminal_prefix_base_read_v1(ClusterRelmapTerminalScanV1 *scan,
													 ClusterRelmapTerminalBaseObservationV1 *out)
{
	static const uint8 domain[] = "PGRAC-RELMAP-PREFIX-NATIVE-BASE-V1";
	ClusterRelmapTerminalBaseObservationV1 result = { 0 };
	pg_cryptohash_ctx *volatile hash = NULL;
	bool accepted = false;
	uint8 identity[13];
	uint32 fields[3];

	if (scan != NULL && out != NULL) {
		uintptr_t a = (uintptr_t)scan, b = (uintptr_t)out;
		if ((a <= b && b - a < sizeof(*scan)) || (b <= a && a - b < sizeof(*out)))
			return false;
	}
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (scan == NULL || scan->pid != getpid() || scan->failed)
		return false;
	/* Original owner/ROOT/native I/O can ERROR. Never revive this private
	 * observation until all source and image checks have succeeded. */
	scan->failed = true;
	if (CritSectionCount != 0 || !scan->prefix_only || !scan->native_base_retained
		|| scan->observation.post_flush_issued == 0 || scan->observation.prefix_end == 0
		|| scan->observation.window.origin
			   != scan->observation.source.claim.identity.origin_node_id)
		return false;
	PG_TRY();
	{
		TransactionId xid = (TransactionId)scan->observation.requested_top_full_xid;
		int origin = scan->observation.window.origin;
		if (terminal_scan_current(scan)
			&& cluster_undo_recovery_cold_source_matches_v1(
				&scan->selected.source, &scan->selected.checkpoint, scan->selected.native_redo)
			&& cluster_clog_native_recovery_base_read_v1(origin, xid, &result.native_base)
			&& result.native_base.origin == origin
			&& result.native_base.xid == xid
			/* Native CLOG stores four two-bit transactions per byte. */
			&& result.native_base.page_number == xid / (BLCKSZ * 4)
			&& result.native_base.status == scan->observation.status) {
			result.terminal = scan->observation;
			fields[0] = (uint32)origin;
			fields[1] = xid;
			fields[2] = result.native_base.page_number;
			for (unsigned f = 0; f < 3; f++)
				for (unsigned i = 0; i < 4; i++)
					identity[f * 4 + i] = (uint8)(fields[f] >> (8 * i));
			identity[12] = result.native_base.status;
			hash = pg_cryptohash_create(PG_SHA256);
			if (hash != NULL && pg_cryptohash_init(hash) == 0
				&& pg_cryptohash_update(hash, domain, sizeof(domain)) == 0
				&& pg_cryptohash_update(hash, result.terminal.fingerprint, 32) == 0
				&& pg_cryptohash_update(hash, identity, sizeof(identity)) == 0
				&& pg_cryptohash_update(hash, result.native_base.image, BLCKSZ) == 0
				&& pg_cryptohash_final(hash, result.fingerprint, 32) == 0
				&& terminal_scan_current(scan)
				&& cluster_undo_recovery_cold_source_matches_v1(
					&scan->selected.source, &scan->selected.checkpoint, scan->selected.native_redo))
				accepted = true;
		}
	}
	PG_CATCH();
	{
		if (hash != NULL)
			pg_cryptohash_free(hash);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (hash != NULL)
		pg_cryptohash_free(hash);
	if (!accepted)
		return false;
	scan->failed = false;
	*out = result;
	return true;
}

bool
cluster_relmap_recovery_terminal_scan_get_v1(ClusterRelmapTerminalScanV1 *scan,
											 ClusterRelmapTerminalObservationV1 *out)
{
	if (scan != NULL && out != NULL) {
		uintptr_t a = (uintptr_t)scan, b = (uintptr_t)out;
		if ((a <= b && b - a < sizeof(*scan)) || (b <= a && a - b < sizeof(*out)))
			return false;
	}
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (scan == NULL || scan->pid != getpid() || scan->failed || CritSectionCount != 0)
		return false;
	/* Sticky even if the original owner throws; no local hash/FD is held. */
	scan->failed = true;
	if (!terminal_scan_current(scan))
		return false;
	*out = scan->observation;
	scan->failed = false;
	return true;
}

void
cluster_relmap_recovery_terminal_scan_release_v1(ClusterRelmapTerminalScanV1 **scan)
{
	ClusterRelmapTerminalScanV1 *old;
	if (scan == NULL)
		return;
	old = *scan;
	*scan = NULL;
	terminal_scan_free(old);
}

/* Custody of a complete pending view and the original recovery round only.
 * This deliberately exports no terminal result or qualification flag. */
struct ClusterRelmapRecoveryHandle {
	pid_t pid;
	uint64 scope_serial;
	bool failed;
	ClusterRelmapTerminalScanV1 *scan;
	uint8 pending_wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	uint8 committed_wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	uint8 native_base_fingerprint[32];
};

static bool
recovery_overlap(const void *a, size_t an, const void *b, size_t bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && ((x <= y && y - x < an) || (y <= x && x - y < bn));
}

typedef struct RelmapRetentionCheckpointInput {
	ClusterWalInputV1 selected;
	ClusterRelmapCheckpointWindow window;
	XLogRecPtr last_end;
	uint64 records;
	bool checkpoint_seen;
} RelmapRetentionCheckpointInput;

static bool
retention_checkpoint_input_valid(const ClusterWalInputV1 *input)
{
	return input->kind == CLUSTER_WAL_INPUT_CHECKPOINT
		   && input->checkpoint.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		   && cluster_control_root_identity_equal(&input->source.claim.identity,
												  &input->checkpoint.identity)
		   && input->source.timeline == input->checkpoint.checkpoint_tli
		   && input->native_checkpoint.ThisTimeLineID == input->source.timeline
		   && input->native_checkpoint.redo == input->native_redo
		   && input->native_redo >= input->checkpoint.checkpoint_lower_lsn
		   && input->native_redo <= input->checkpoint_start;
}

static bool
retention_checkpoint_visit(XLogReaderState *reader, void *arg)
{
	RelmapRetentionCheckpointInput *work = arg;
	const ClusterWalInputV1 *input = &work->selected;
	const ClusterControlRootIdentity *id = &input->source.claim.identity;

	CHECK_FOR_INTERRUPTS();
	if (reader == NULL || reader->record == NULL || reader->cluster_record_crc_failed
		|| reader->cluster_expected_thread_id != id->origin_thread_id
		|| reader->ReadRecPtr != reader->record->lsn
		|| reader->EndRecPtr != reader->record->next_lsn
		|| reader->ReadRecPtr < input->checkpoint.checkpoint_lower_lsn
		|| reader->ReadRecPtr < work->last_end || reader->EndRecPtr <= reader->ReadRecPtr
		|| reader->EndRecPtr > input->checkpoint.validated_tail_lsn_exclusive
		|| work->records == UINT64_MAX)
		return false;
	if (reader->ReadRecPtr == input->checkpoint_start) {
		uint8 info = XLogRecGetInfo(reader);
		uint64 post_issued, next = U64FromFullTransactionId(input->native_checkpoint.nextXid);
		uint64 upper;
		if (work->checkpoint_seen || XLogRecGetRmid(reader) != RM_XLOG_ID
			|| (info != XLOG_CHECKPOINT_ONLINE && info != XLOG_CHECKPOINT_SHUTDOWN)
			|| XLogRecGetXid(reader) != InvalidTransactionId || XLogRecMaxBlockId(reader) >= 0
			|| reader->record->has_page_version_edge
			|| XLogRecGetOrigin(reader) != InvalidRepOriginId
			|| TransactionIdIsValid(reader->record->toplevel_xid) || XLogRecGetData(reader) == NULL
			|| XLogRecGetDataLen(reader) != sizeof(CheckPoint)
			|| memcmp(XLogRecGetData(reader), &input->native_checkpoint, sizeof(CheckPoint)) != 0
			|| !cluster_relmap_checkpoint_window_decode_v2(
				input->native_window, &input->native_checkpoint, reader->ReadRecPtr,
				reader->EndRecPtr, id, input->source.claim.database_incarnation, &work->window,
				&post_issued))
			return false;
		/* These original references must share one half-space. Neither the
		 * checkpoint's nextXid nor the post-flush sample replaces last-issued. */
		upper = Max(next, post_issued);
		if (!TransactionIdIsNormal((TransactionId)next) || upper < work->window.native_floor
			|| upper - work->window.native_floor >= UINT64CONST(0x80000000))
			return false;
		work->checkpoint_seen = true;
	}
	work->records++;
	work->last_end = reader->EndRecPtr;
	return true;
}

bool
cluster_relmap_recovery_retention_build_start_v1(ClusterWalInputsV1 *inputs, uint16 origin,
												 ClusterRelmapRetentionDirectory *out)
{
	RelmapRetentionCheckpointInput work = { 0 };
	ClusterRelmapRetentionDirectory directory = { 0 };
	ClusterWalTailObservation observed = { 0 };
	const ClusterWalInputV1 *selected = NULL;
	uint32 index = 0, count;
	uint64 serial;

	if (out == NULL || origin >= 128 || MyBackendType != B_STARTUP || !RecoveryInProgress()
		|| CritSectionCount != 0)
		return false;
	count = cluster_wal_inputs_count_v1(inputs);
	if (count == 0 || count > CLUSTER_WAL_INPUTS_MAX)
		return false;
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *input = cluster_wal_inputs_at_v1(inputs, i);
		if (input == NULL || recovery_overlap(out, sizeof(*out), input, sizeof(*input)))
			return false;
		if (input->source.claim.identity.origin_node_id == origin) {
			if (selected != NULL)
				return false; /* No partial origin/history directory. */
			selected = input;
			index = i;
		}
	}
	if (selected == NULL || !retention_checkpoint_input_valid(selected))
		return false;
	/* Old UNKNOWN/BUILDING storage and unoccupied stale pins are not a new
	 * directory. Do not call the resetting core on caller storage. */
	for (size_t i = 0; i < sizeof(*out); i++)
		if (((const unsigned char *)out)[i] != 0)
			return false;
	work.selected = *selected;
	if (cluster_wal_inputs_revalidate_v1(inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| (serial = cluster_undo_recovery_cold_scope_id_v1(
				&work.selected.source, &work.selected.checkpoint, work.selected.native_redo))
			   == 0
		|| cluster_wal_inputs_visit_retained_v1(inputs, index, retention_checkpoint_visit, &work,
												&observed)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !work.checkpoint_seen || observed.records != work.records
		|| observed.complete_end != work.selected.checkpoint.validated_tail_lsn_exclusive
		|| work.last_end != observed.complete_end
		|| cluster_wal_inputs_revalidate_v1(inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_undo_recovery_cold_scope_id_v1(&work.selected.source, &work.selected.checkpoint,
												  work.selected.native_redo)
			   != serial
		|| cluster_wal_inputs_revalidate_v1(inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_wal_inputs_count_v1(inputs) != count
		|| (selected = cluster_wal_inputs_at_v1(inputs, index)) == NULL
		|| memcmp(selected, &work.selected, sizeof(*selected)) != 0 || MyBackendType != B_STARTUP
		|| !RecoveryInProgress() || CritSectionCount != 0
		|| cluster_relmap_retention_build_start(
			   &directory, work.window.system_identifier, work.window.database_incarnation,
			   work.window.origin, serial, work.window.native_floor, work.window.last_issued)
			   != CLUSTER_RELMAP_RETENTION_OK)
		return false;
	/* No local allocation, FD, scope or pin needs cleanup on an earlier ERROR.
	 * Only the original caller can qualify and eventually publish this build. */
	*out = directory;
	return true;
}

typedef struct RelmapExecutionCheckpointInput {
	RelmapRetentionCheckpointInput input;
	uint64 serial;
	uint16 origin;
} RelmapExecutionCheckpointInput;

static bool
retention_execution_input_set(ClusterWalInputV1 *selected,
							  const ClusterWalRecoveryCheckpointV1 *checkpoint,
							  const ClusterControlRootSnapshot *root)
{
	if (checkpoint == NULL || root == NULL)
		return false;
	selected->kind = CLUSTER_WAL_INPUT_CHECKPOINT;
	selected->source = checkpoint->source;
	selected->checkpoint = *root;
	selected->checkpoint_start = checkpoint->checkpoint_start;
	selected->native_redo = checkpoint->checkpoint.redo;
	selected->native_checkpoint = checkpoint->checkpoint;
	memcpy(selected->native_window, checkpoint->native_window, sizeof(selected->native_window));
	return retention_checkpoint_input_valid(selected);
}

static bool
retention_execution_checkpoint_visit(const ClusterWalRecoveryCheckpointV1 *checkpoint,
									 const ClusterControlRootSnapshot *root, uint64 serial,
									 XLogReaderState *reader, void *arg)
{
	RelmapExecutionCheckpointInput *work = arg;
	ClusterWalInputV1 *selected = &work->input.selected;

	if (checkpoint == NULL || root == NULL || serial == 0
		|| checkpoint->source.claim.identity.origin_node_id != work->origin)
		return false;
	if (work->input.records == 0) {
		if (!retention_execution_input_set(selected, checkpoint, root))
			return false;
		work->serial = serial;
	}
	return work->serial == serial && retention_checkpoint_visit(reader, &work->input);
}

bool
cluster_relmap_recovery_retention_build_start_cold_v1(uint16 origin,
													  ClusterRelmapRetentionDirectory *out)
{
	RelmapExecutionCheckpointInput work = { 0 };
	ClusterRelmapRetentionDirectory directory = { 0 };

	if (out == NULL || origin >= 128 || MyBackendType != B_STARTUP || !RecoveryInProgress()
		|| CritSectionCount != 0)
		return false;
	for (size_t i = 0; i < sizeof(*out); i++)
		if (((const unsigned char *)out)[i] != 0)
			return false;
	work.origin = origin;
	if (!cluster_undo_recovery_cold_checkpoint_visit_v1(origin,
														retention_execution_checkpoint_visit, &work)
		|| !work.input.checkpoint_seen || work.serial == 0
		|| work.input.last_end != work.input.selected.checkpoint.validated_tail_lsn_exclusive
		|| MyBackendType != B_STARTUP || !RecoveryInProgress() || CritSectionCount != 0
		|| cluster_relmap_retention_build_start(
			   &directory, work.input.window.system_identifier,
			   work.input.window.database_incarnation, work.input.window.origin, work.serial,
			   work.input.window.native_floor, work.input.window.last_issued)
			   != CLUSTER_RELMAP_RETENTION_OK)
		return false;
	*out = directory;
	return true;
}

static bool
terminal_execution_visit(const ClusterWalRecoveryCheckpointV1 *checkpoint,
						 const ClusterControlRootSnapshot *root, uint64 serial,
						 XLogReaderState *reader, void *arg)
{
	ClusterRelmapTerminalScanV1 *scan = arg;

	if (serial == 0)
		return false;
	if (scan->observation.records == 0) {
		if (!retention_execution_input_set(&scan->selected, checkpoint, root)
			|| scan->selected.native_redo == scan->selected.checkpoint_start
			|| scan->selected.source.claim.identity.origin_node_id
				   != scan->pending.pending_owner.node
			|| scan->selected.source.claim.identity.system_identifier
				   != scan->pending.identity.system_identifier
			|| scan->selected.source.claim.database_incarnation
				   != scan->pending.identity.database_incarnation)
			return false;
		scan->pending_scope_serial = serial;
		scan->observation.source = scan->selected.source;
		scan->observation.cut = scan->selected.checkpoint.validated_tail_lsn_exclusive;
		if (!terminal_scan_current(scan) || !terminal_scan_begin(scan))
			return false;
	}
	return scan->pending_scope_serial == serial && terminal_scan_visit(reader, scan);
}

static bool
terminal_scan_cold(const ClusterRelmapV2Image *pending, ClusterRelmapTerminalScanV1 **out)
{
	ClusterRelmapTerminalScanV1 *scan = palloc0(sizeof(*scan));
	bool accepted = false;

	scan->pid = getpid();
	scan->execution_owner = scan->prefix_only = scan->native_base_retained = true;
	scan->pending_required = true;
	scan->pending = *pending;
	scan->observation.requested_top_full_xid = pending->pending_owner.top_full_xid;
	PG_TRY();
	{
		accepted = cluster_undo_recovery_cold_checkpoint_visit_v1(pending->pending_owner.node,
																  terminal_execution_visit, scan)
				   && scan->observation.records != 0 && terminal_scan_current(scan)
				   && terminal_scan_fold_complete(scan) && scan->last_end == scan->observation.cut
				   && MyBackendType == B_STARTUP && RecoveryInProgress() && CritSectionCount == 0
				   && pg_cryptohash_final(scan->hash, scan->observation.fingerprint, 32) == 0;
	}
	PG_CATCH();
	{
		terminal_scan_free(scan);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!accepted) {
		terminal_scan_free(scan);
		return false;
	}
	pg_cryptohash_free(scan->hash);
	scan->hash = NULL;
	*out = scan;
	return true;
}

static bool
recovery_pending_encode(const ClusterRelmapV2Namespace *identity, const ClusterRelmapV2View *view,
						ClusterRelmapV2Image *image,
						uint8 pending_wire[CLUSTER_RELMAP_V2_FILE_SIZE],
						uint8 committed_wire[CLUSTER_RELMAP_V2_FILE_SIZE])
{
	ClusterRelmapV2Image current = { 0 };
	if (identity == NULL || view == NULL || !view->pending_complete || !view->has_pending
		|| view->used_backup)
		return false;
	memset(image, 0, sizeof(*image));
	image->identity = *identity;
	image->committed_generation = view->pending_committed_generation;
	image->pending_generation = view->pending_generation;
	image->pending_owner = view->pending_owner;
	image->committed = view->pending_committed;
	image->pending = view->pending;
	if (cluster_relmap_v2_encode(image, pending_wire, CLUSTER_RELMAP_V2_FILE_SIZE)
			!= CLUSTER_RELMAP_V2_VALID
		|| (view->committed_generation == view->pending_committed_generation
				? memcmp(&view->committed, &view->pending_committed, sizeof(RelMapFile)) != 0
				: view->committed_generation != view->pending_generation
					  || memcmp(&view->committed, &view->pending, sizeof(RelMapFile)) != 0))
		return false;
	current.identity = *identity;
	current.committed_generation = view->committed_generation;
	current.committed = view->committed;
	return cluster_relmap_v2_encode(&current, committed_wire, CLUSTER_RELMAP_V2_FILE_SIZE)
		   == CLUSTER_RELMAP_V2_VALID;
}

static bool
recovery_handle_current(ClusterRelmapRecoveryHandle *handle)
{
	ClusterRelmapTerminalScanV1 *scan = handle->scan;
	return terminal_scan_current(scan)
		   && cluster_undo_recovery_cold_scope_id_v1(
				  &scan->selected.source, &scan->selected.checkpoint, scan->selected.native_redo)
				  == handle->scope_serial
		   && terminal_scan_current(scan);
}

/* Bind the original native durable page observation, without exporting its
 * status or upgrading it to FullXid/base-lifetime qualification. Every read
 * still uses the original native owner and its own held-FD sync/readback. */
static bool
recovery_handle_base_fingerprint(ClusterRelmapRecoveryHandle *handle, uint8 out[32])
{
	ClusterRelmapTerminalBaseObservationV1 base;

	if (!recovery_handle_current(handle)
		|| !cluster_relmap_recovery_terminal_prefix_base_read_v1(handle->scan, &base)
		|| !recovery_handle_current(handle))
		return false;
	memcpy(out, base.fingerprint, 32);
	return true;
}

bool
cluster_relmap_recovery_prepare_v1(ClusterWalInputsV1 *inputs,
								   const ClusterRelmapV2Namespace *identity,
								   const ClusterRelmapV2View *pending,
								   ClusterRelmapRecoveryHandle **out)
{
	ClusterRelmapV2Image image;
	uint8 pending_wire[CLUSTER_RELMAP_V2_FILE_SIZE], committed_wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	ClusterRelmapRecoveryHandle *handle;
	const ClusterWalInputV1 *selected = NULL;
	uint32 index = 0, count;
	uint64 serial;
	bool accepted = false;

	if (out == NULL || recovery_overlap(out, sizeof(*out), identity, sizeof(*identity))
		|| recovery_overlap(out, sizeof(*out), pending, sizeof(*pending)))
		return false;
	*out = NULL;
	if (CritSectionCount != 0
		|| !recovery_pending_encode(identity, pending, &image, pending_wire, committed_wire))
		return false;
	count = cluster_wal_inputs_count_v1(inputs);
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *input = cluster_wal_inputs_at_v1(inputs, i);
		if (input == NULL)
			return false;
		if (input->source.claim.identity.origin_node_id == image.pending_owner.node) {
			if (selected != NULL)
				return false; /* No partial origin/history set. */
			selected = input;
			index = i;
		}
	}
	if (selected == NULL || selected->kind != CLUSTER_WAL_INPUT_CHECKPOINT)
		return false;
	serial = cluster_undo_recovery_cold_scope_id_v1(&selected->source, &selected->checkpoint,
													selected->native_redo);
	if (serial == 0)
		return false;
	handle = palloc0(sizeof(*handle));
	handle->pid = getpid();
	handle->scope_serial = serial;
	memcpy(handle->pending_wire, pending_wire, sizeof(pending_wire));
	memcpy(handle->committed_wire, committed_wire, sizeof(committed_wire));
	PG_TRY();
	{
		accepted = terminal_scan(inputs, index,
								 FullTransactionIdFromU64(image.pending_owner.top_full_xid), true,
								 &image, serial, &handle->scan)
				   && recovery_handle_base_fingerprint(handle, handle->native_base_fingerprint);
	}
	PG_CATCH();
	{
		terminal_scan_free(handle->scan);
		pfree(handle);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!accepted) {
		terminal_scan_free(handle->scan);
		pfree(handle);
		return false;
	}
	*out = handle;
	return true;
}

bool
cluster_relmap_recovery_prepare_cold_v1(const ClusterRelmapV2Namespace *identity,
										const ClusterRelmapV2View *pending,
										ClusterRelmapRecoveryHandle **out)
{
	ClusterRelmapV2Image image;
	uint8 pending_wire[CLUSTER_RELMAP_V2_FILE_SIZE], committed_wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	ClusterRelmapRecoveryHandle *handle;
	bool accepted = false;

	if (out == NULL || recovery_overlap(out, sizeof(*out), identity, sizeof(*identity))
		|| recovery_overlap(out, sizeof(*out), pending, sizeof(*pending)))
		return false;
	*out = NULL;
	if (MyBackendType != B_STARTUP || !RecoveryInProgress() || CritSectionCount != 0
		|| !recovery_pending_encode(identity, pending, &image, pending_wire, committed_wire))
		return false;
	handle = palloc0(sizeof(*handle));
	handle->pid = getpid();
	memcpy(handle->pending_wire, pending_wire, sizeof(pending_wire));
	memcpy(handle->committed_wire, committed_wire, sizeof(committed_wire));
	PG_TRY();
	{
		if (terminal_scan_cold(&image, &handle->scan)) {
			handle->scope_serial = handle->scan->pending_scope_serial;
			accepted = recovery_handle_base_fingerprint(handle, handle->native_base_fingerprint);
		}
	}
	PG_CATCH();
	{
		terminal_scan_free(handle->scan);
		pfree(handle);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!accepted) {
		terminal_scan_free(handle->scan);
		pfree(handle);
		return false;
	}
	*out = handle;
	return true;
}

bool
cluster_relmap_recovery_handle_source_v1(ClusterRelmapRecoveryHandle *handle,
										 ClusterRelmapRecoverySourceObservationV1 *out)
{
	ClusterRelmapRecoverySourceObservationV1 result = { 0 };

	if (out == NULL || recovery_overlap(out, sizeof(*out), handle, sizeof(*handle))
		|| (handle != NULL && handle->scan != NULL
			&& recovery_overlap(out, sizeof(*out), handle->scan, sizeof(*handle->scan))))
		return false;
	memset(out, 0, sizeof(*out));
	if (handle == NULL || handle->pid != getpid() || handle->failed || handle->scan == NULL)
		return false;
	/* Original source/owner checks may ERROR. No native page I/O or status is
	 * needed to identify this already selected, still borrowed source. */
	handle->failed = true;
	if (CritSectionCount != 0 || !recovery_handle_current(handle))
		return false;
	result.source = handle->scan->selected.source;
	result.checkpoint = handle->scan->selected.checkpoint;
	result.native_redo = handle->scan->selected.native_redo;
	result.scope_serial = handle->scope_serial;
	handle->failed = false;
	*out = result;
	return true;
}

bool
cluster_relmap_recovery_handle_matches_v1(ClusterRelmapRecoveryHandle *handle,
										  const ClusterRelmapV2Namespace *identity,
										  const ClusterRelmapV2View *pending)
{
	ClusterRelmapV2Image image;
	uint8 native_base_fingerprint[32];
	uint8 pending_wire[CLUSTER_RELMAP_V2_FILE_SIZE], committed_wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	if (handle == NULL || handle->pid != getpid() || handle->failed
		|| recovery_overlap(handle, sizeof(*handle), identity, sizeof(*identity))
		|| recovery_overlap(handle, sizeof(*handle), pending, sizeof(*pending)))
		return false;
	/* Original source/owner checks can ERROR. Leave this local handle dead. */
	handle->failed = true;
	if (CritSectionCount != 0
		|| !recovery_pending_encode(identity, pending, &image, pending_wire, committed_wire)
		|| memcmp(pending_wire, handle->pending_wire, sizeof(pending_wire)) != 0
		|| memcmp(committed_wire, handle->committed_wire, sizeof(committed_wire)) != 0
		|| !recovery_handle_base_fingerprint(handle, native_base_fingerprint)
		|| memcmp(native_base_fingerprint, handle->native_base_fingerprint, 32) != 0)
		return false;
	handle->failed = false;
	return true;
}

/* The era is taken from the original authenticated window, not guessed from
 * this raw xid. Bound every native checkpoint/issuance reference in its one
 * half-space; sparse allocation may never stand in for actual last-issued. */
static bool
recovery_terminal_window(const ClusterRelmapRecoveryHandle *handle)
{
	const ClusterRelmapTerminalScanV1 *scan = handle->scan;
	const ClusterRelmapTerminalObservationV1 *terminal = &scan->observation;
	const ClusterRelmapCheckpointWindow *window = &terminal->window;
	const ClusterRelmapV2Owner *owner = &scan->pending.pending_owner;
	uint64 next = U64FromFullTransactionId(scan->selected.native_checkpoint.nextXid);
	uint64 upper = Max(next, terminal->post_flush_issued);

	return scan->pending_required && scan->pending_seen && scan->prefix_only
		&& scan->native_base_retained && scan->checkpoint_seen && scan->terminal_seen
		&& scan->expected_native_terminals != 0
		&& scan->expected_native_terminals == scan->native_terminals
		&& handle->scope_serial == scan->pending_scope_serial
		&& window->system_identifier == scan->pending.identity.system_identifier
		&& window->database_incarnation == scan->pending.identity.database_incarnation
		&& window->origin == owner->node
		&& window->epoch == owner->membership_epoch
		&& window->admitted_incarnation == owner->admitted_incarnation
		&& window->boot_incarnation == owner->boot_incarnation
		&& TransactionIdIsNormal((TransactionId)next)
		&& window->native_floor <= owner->top_full_xid
		&& owner->top_full_xid <= window->last_issued
		&& owner->top_full_xid == terminal->requested_top_full_xid
		&& window->last_issued < window->allocation_next && window->allocation_next <= next
		&& terminal->post_flush_issued >= window->last_issued
		&& upper >= window->native_floor
		&& upper - window->native_floor < UINT64CONST(0x80000000)
		&& owner->wal_end <= terminal->terminal_start
		&& terminal->terminal_start >= scan->selected.native_redo
		&& terminal->terminal_start < terminal->terminal_end
		&& terminal->terminal_end <= scan->selected.checkpoint_start
		&& terminal->prefix_end <= terminal->cut
		&& (terminal->status == TRANSACTION_STATUS_COMMITTED
			|| terminal->status == TRANSACTION_STATUS_ABORTED);
}

bool
cluster_relmap_recovery_terminal_read(ClusterRelmapRecoveryHandle *handle,
	const ClusterRelmapV2Namespace *identity, const ClusterRelmapV2View *pending,
	ClusterRelmapRecoveryTerminalV1 *out)
{
	ClusterRelmapRecoveryTerminalV1 result = { 0 };
	const ClusterRelmapTerminalObservationV1 *terminal;
	const ClusterRelmapCheckpointWindow *window;
	if (out == NULL || recovery_overlap(out, sizeof(*out), handle, sizeof(*handle))
		|| (handle != NULL && handle->scan != NULL
			&& recovery_overlap(out, sizeof(*out), handle->scan, sizeof(*handle->scan)))
		|| recovery_overlap(out, sizeof(*out), identity, sizeof(*identity))
		|| recovery_overlap(out, sizeof(*out), pending, sizeof(*pending)))
		return false;
	memset(out, 0, sizeof(*out));
	/* This exact original operation/source/round check also synchronizes and
	 * rereads the original native base, with source checks around that I/O.
	 * Corruption, drift or ERROR leaves original pins and debt in place. */
	if (!cluster_relmap_recovery_handle_matches_v1(handle, identity, pending))
		return false;
	handle->failed = true;
	if (!recovery_terminal_window(handle) || !recovery_handle_current(handle))
		return false;
	terminal = &handle->scan->observation;
	window = &terminal->window;
	result.bounds.system_identifier = window->system_identifier;
	result.bounds.database_incarnation = window->database_incarnation;
	result.bounds.origin = window->origin;
	result.bounds.membership_epoch = window->epoch;
	result.bounds.admitted_incarnation = window->admitted_incarnation;
	result.bounds.boot_incarnation = window->boot_incarnation;
	result.bounds.native_floor = window->native_floor;
	result.bounds.last_issued = window->last_issued;
	result.top_full_xid = terminal->requested_top_full_xid;
	result.status = terminal->status;
	result.terminal_start = terminal->terminal_start;
	result.terminal_end = terminal->terminal_end;
	result.source_cut = terminal->cut;
	memcpy(result.fingerprint, handle->native_base_fingerprint, sizeof(result.fingerprint));
	handle->failed = false;
	*out = result;
	return true;
}

void
cluster_relmap_recovery_handle_release_v1(ClusterRelmapRecoveryHandle **handle)
{
	ClusterRelmapRecoveryHandle *old;
	if (handle == NULL)
		return;
	old = *handle;
	*handle = NULL;
	if (old != NULL) {
		terminal_scan_free(old->scan);
		pfree(old);
	}
}

#endif
