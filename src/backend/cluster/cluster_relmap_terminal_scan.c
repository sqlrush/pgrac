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
#include "cluster/cluster_side_xact.h"
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
	bool checkpoint_seen, terminal_seen, failed;
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
		return true;
	}
	if (!scan->checkpoint_seen || scan->terminal_seen)
		return false;
	scan->terminal_seen = true;
	scan->observation.status = commit ? TRANSACTION_STATUS_COMMITTED : TRANSACTION_STATUS_ABORTED;
	scan->observation.terminal_start = reader->ReadRecPtr;
	scan->observation.terminal_end = reader->EndRecPtr;
	return true;
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
			|| !cluster_relmap_checkpoint_window_decode_v1(
				input->native_window, &input->native_checkpoint, reader->ReadRecPtr,
				reader->EndRecPtr, id, input->source.claim.database_incarnation,
				&scan->observation.window)
			|| scan->observation.requested_top_full_xid < scan->observation.window.native_floor
			|| scan->observation.requested_top_full_xid > scan->observation.window.last_issued)
			return false;
		scan->checkpoint_seen = true;
	}
	if ((scan->terminal_seen && XLogRecGetXid(reader) == target)
		|| (XLogRecGetRmid(reader) == RM_XACT_ID && !terminal_scan_xact(scan, reader)))
		return false;
	if (scan->observation.records == UINT64_MAX
		|| !cluster_recovery_record_hash_update(scan->hash, reader))
		return false;
	scan->observation.records++;
	scan->last_end = reader->EndRecPtr;
	return true;
}

bool
cluster_relmap_recovery_terminal_scan_v1(ClusterWalInputsV1 *inputs, uint32 index,
										 FullTransactionId requested_top,
										 ClusterRelmapTerminalScanV1 **out)
{
	static const uint8 domain[] = "PGRAC-RELMAP-RETAINED-TERMINAL-V1";
	const ClusterWalInputV1 *input;
	ClusterRelmapTerminalScanV1 *scan;
	uint8 query[8], zero[160] = { 0 };
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
	scan->inputs = inputs;
	scan->index = index;
	scan->count = count;
	scan->selected = *input;
	scan->observation.source = input->source;
	scan->observation.requested_top_full_xid = full;
	scan->observation.cut = input->checkpoint.validated_tail_lsn_exclusive;
	for (unsigned i = 0; i < sizeof(query); i++)
		query[i] = (uint8)(full >> (8 * i));
	PG_TRY();
	{
		ClusterWalTailObservation observed = { 0 };
		scan->hash = pg_cryptohash_create(PG_SHA256);
		if (scan->hash != NULL && pg_cryptohash_init(scan->hash) == 0
			&& pg_cryptohash_update(scan->hash, domain, sizeof(domain)) == 0
			&& pg_cryptohash_update(scan->hash, input->native_window, 160) == 0
			&& pg_cryptohash_update(scan->hash, input->source.claim.identity.storage_uuid, 16) == 0
			&& pg_cryptohash_update(scan->hash, input->source.claim.claim_sha256, 32) == 0
			&& pg_cryptohash_update(scan->hash, query, sizeof(query)) == 0
			&& cluster_wal_inputs_visit_retained_v1(inputs, index, terminal_scan_visit, scan,
													&observed)
				   == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& scan->checkpoint_seen && scan->terminal_seen
			&& observed.records == scan->observation.records
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
cluster_relmap_recovery_terminal_scan_get_v1(ClusterRelmapTerminalScanV1 *scan,
											 ClusterRelmapTerminalObservationV1 *out)
{
	const ClusterWalInputV1 *current;
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
	if (cluster_wal_inputs_revalidate_v1(scan->inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_wal_inputs_count_v1(scan->inputs) != scan->count)
		return false;
	current = cluster_wal_inputs_at_v1(scan->inputs, scan->index);
	if (current == NULL || memcmp(current, &scan->selected, sizeof(*current)) != 0)
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
#endif
