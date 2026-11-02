/*-------------------------------------------------------------------------
 * cluster_relmap_control.c
 *    Control-selected relmap cache and complete checkpoint pair receipts.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/backend/cluster/cluster_relmap_control.c
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_relmap_control.h"
#include "cluster/cluster_cold_recovery_plan_internal.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_cf_enqueue.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "common/cluster_relmap_create.h"
#include "catalog/pg_tablespace_d.h"
#include "storage/fd.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"

struct ClusterRelmapCheckpointReceiptV2
{
	bool valid;
	ClusterRelmapCheckpointSummaryV2 summary;
	ClusterRecoveryFileIdentityV2 *keys;
	ClusterRelmapFilePairV2 *pairs;
	ClusterRecoveryCheckpointSourceV2 *sources;
};

typedef struct RelmapControlWork
{
	ClusterControlRelmapSelectionV2 *selection;
	ClusterRelmapCheckpointReceiptV2 *receipt;
	ClusterRelmapFilePairV2 pair;
	pg_cryptohash_ctx *hash;
	bool mapping;
} RelmapControlWork;

static bool
relmap_control_overlap(const void *a, Size an, const void *b, Size bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a && b && an && bn && (x <= y ? y - x < an : x - y < bn);
}

static bool
relmap_receipt_alias(const ClusterRelmapCheckpointReceiptV2 *r, const void *out, Size n)
{
	return relmap_control_overlap(r, sizeof(*r), out, n)
		|| relmap_control_overlap(r->keys, r->summary.control.logical_count * sizeof(*r->keys), out, n)
		|| relmap_control_overlap(r->pairs, r->summary.control.logical_count * sizeof(*r->pairs), out, n)
		|| relmap_control_overlap(r->sources, r->summary.control.source_count * sizeof(*r->sources), out, n);
}

void
cluster_relmap_checkpoint_destroy_v2(ClusterRelmapCheckpointReceiptV2 **out)
{
	ClusterRelmapCheckpointReceiptV2 *r;
	if (out == NULL || *out == NULL)
		return;
	r = *out;
	if (relmap_receipt_alias(r, out, sizeof(*out)))
		return;
	*out = NULL;
	r->valid = false;
	if (r->sources) pfree(r->sources);
	if (r->pairs) pfree(r->pairs);
	if (r->keys) pfree(r->keys);
	pfree(r);
}

static void
relmap_control_cleanup(RelmapControlWork *w)
{
	/* Cleanup cannot service an interrupt between releasing our lock and
	 * discarding private copies. Never release an inherited CF or MappingLock. */
	HOLD_INTERRUPTS();
	if (w->mapping) {
		w->mapping = false;
		LWLockRelease(RelationMappingLock);
	}
	cluster_control_root_relmap_selection_destroy_v4(&w->selection);
	cluster_relmap_checkpoint_destroy_v2(&w->receipt);
	if (w->hash) pg_cryptohash_free(w->hash);
	pfree(w);
	RESUME_INTERRUPTS();
}

static bool
relmap_control_pair(const ClusterRecoveryFileIdentityV2 *id, ClusterRelmapFilePairV2 *pair)
{
	ClusterRelmapV2Image image;
	ClusterRelmapV2Namespace ns = {id->system_identifier, id->database_incarnation,
		id->database_oid, id->database_oid == InvalidOid};
	ClusterRelmapV2View view;
	for (unsigned i = 0; i < 2; i++)
		if (!cluster_relmap_file_decode_v2(pair->bytes[i], sizeof(pair->bytes[i]), id, &image))
			return false;
	if (cluster_relmap_v2_read_pair(pair->bytes[0] + CLUSTER_RELMAP_FILE_HEADER_SIZE_V2,
		CLUSTER_RELMAP_V2_FILE_SIZE, pair->bytes[1] + CLUSTER_RELMAP_FILE_HEADER_SIZE_V2,
		CLUSTER_RELMAP_V2_FILE_SIZE, &ns, &view) != CLUSTER_RELMAP_V2_PAIR_VALID
		|| !view.pending_complete)
		return false;
	pair->view = view;
	return true;
}

bool
cluster_relmap_control_read_v2(const ClusterSharedConfigRef *expected, Size budget,
	Oid dbid, Oid tsid, RelMapFile *out)
{
	RelmapControlWork *w;
	ClusterSharedConfigRef captured;
	ClusterRecoveryFileIdentityV2 id;
	volatile bool ok = false;
	if (!expected || !out || CritSectionCount || LWLockHeldByMe(ControlFileLock)
		|| LWLockHeldByMe(RelationMappingLock)
		|| relmap_control_overlap(expected, sizeof(*expected), out, sizeof(*out)))
		return false;
	captured = *expected;
	w = palloc0(sizeof(*w));
	PG_TRY();
	{
		if (cluster_control_root_relmap_selection_read_locked_v4(&captured, budget, &w->selection)
			== CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& cluster_control_root_relmap_selection_lookup_v4(w->selection, dbid, tsid, 0, &id)) {
			LWLockAcquire(RelationMappingLock, LW_SHARED);
			w->mapping = true;
			ok = cluster_relmap_authority_managed_read_v2(&id, &w->pair)
				&& relmap_control_pair(&id, &w->pair);
			w->mapping = false;
			LWLockRelease(RelationMappingLock);
			if (ok)
				ok = cluster_control_root_relmap_selection_recheck_locked_v4(w->selection)
					== CLUSTER_CONTROL_ROOT_OK_PRIMARY
					&& !memcmp(expected, &captured, sizeof(captured));
		}
	}
	PG_CATCH();
	{
		relmap_control_cleanup(w);
		PG_RE_THROW();
	}
	PG_END_TRY();
	/* No fallible operation follows the final local adoption. */
	if (ok) {
		RelMapFile map = w->pair.view.committed;
		relmap_control_cleanup(w);
		*out = map;
	} else relmap_control_cleanup(w);
	return ok;
}

static bool
relmap_receipt_budget(uint64 files, uint64 sources, Size budget)
{
	Size need = sizeof(ClusterRelmapCheckpointReceiptV2);
	const Size item = sizeof(ClusterRecoveryFileIdentityV2) + sizeof(ClusterRelmapFilePairV2);
	budget = Min(budget, MaxAllocSize);
	if (!files || !sources || need > budget || files > (budget - need) / item)
		return false;
	need += files * item;
	return sources <= (budget - need) / sizeof(ClusterRecoveryCheckpointSourceV2);
}

static bool
relmap_receipt_digest(RelmapControlWork *w)
{
	ClusterRelmapCheckpointReceiptV2 *r = w->receipt;
	uint8 encoded[128];
	w->hash = pg_cryptohash_create(PG_SHA256);
	if (!w->hash || pg_cryptohash_init(w->hash) < 0)
		return false;
	for (uint64 i = 0; i < r->summary.control.logical_count; i++)
		if (!cluster_recovery_file_identity_encode_v2(&r->keys[i], encoded, sizeof(encoded))
			|| pg_cryptohash_update(w->hash, encoded, sizeof(encoded)) < 0
			|| pg_cryptohash_update(w->hash, (const uint8 *)r->pairs[i].bytes,
				sizeof(r->pairs[i].bytes)) < 0)
			return false;
	return pg_cryptohash_final(w->hash, r->summary.files_sha256,
		sizeof(r->summary.files_sha256)) == 0;
}

bool
cluster_relmap_control_checkpoint_v2(const ClusterSharedConfigRef *expected,
	Size registry_budget, Size receipt_budget, ClusterRelmapCheckpointReceiptV2 **out)
{
	RelmapControlWork *w;
	ClusterSharedConfigRef captured;
	volatile bool ok = false;
	if (!expected || !out || *out || CritSectionCount || LWLockHeldByMe(ControlFileLock)
		|| LWLockHeldByMe(RelationMappingLock)
		|| relmap_control_overlap(expected, sizeof(*expected), out, sizeof(*out)))
		return false;
	captured = *expected;
	w = palloc0(sizeof(*w));
	PG_TRY();
	{
		if (cluster_control_root_relmap_selection_read_locked_v4(&captured, registry_budget, &w->selection)
			== CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			uint64 count = cluster_control_root_relmap_selection_count_v4(w->selection);
			uint64 sources = cluster_control_root_relmap_selection_source_count_v4(w->selection);
			if (relmap_receipt_budget(count, sources, receipt_budget)) {
				ClusterRelmapCheckpointReceiptV2 *r;
				w->receipt = palloc0(sizeof(*w->receipt));
				r = w->receipt;
				r->summary.control.logical_count = count;
				r->summary.control.source_count = sources;
				/* Whole capacity, complete original identities and sources before
				 * the first target file I/O; no prefix can become a receipt. */
				r->keys = palloc0(count * sizeof(*r->keys));
				r->pairs = palloc0(count * sizeof(*r->pairs));
				r->sources = palloc0(sources * sizeof(*r->sources));
				ok = true;
				for (uint64 i = 0; i < count && ok; i++)
					ok = cluster_control_root_relmap_selection_at_v4(w->selection, i, &r->keys[i]);
				for (uint64 i = 0; i < sources && ok; i++)
					ok = cluster_control_root_relmap_selection_source_at_v4(w->selection, i, &r->sources[i]);
				if (ok) {
					LWLockAcquire(RelationMappingLock, LW_SHARED);
					w->mapping = true;
					for (uint64 i = 0; i < count && ok; i++)
						ok = cluster_relmap_authority_managed_checkpoint_v2(&r->keys[i], &r->pairs[i])
							&& relmap_control_pair(&r->keys[i], &r->pairs[i]);
					w->mapping = false;
					LWLockRelease(RelationMappingLock);
				}
				if (ok)
					ok = cluster_control_root_relmap_selection_complete_locked_v4(w->selection, r->keys, count)
						== CLUSTER_CONTROL_ROOT_OK_PRIMARY
						&& cluster_control_root_relmap_selection_reference_v4(w->selection, &r->summary.control)
						&& !memcmp(expected, &captured, sizeof(captured))
						&& relmap_receipt_digest(w);
			}
		}
	}
	PG_CATCH();
	{
		relmap_control_cleanup(w);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (ok) {
		ClusterRelmapCheckpointReceiptV2 *r = w->receipt;
		w->receipt = NULL;
		relmap_control_cleanup(w);
		r->valid = true;
		*out = r;
	} else relmap_control_cleanup(w);
	return ok;
}

bool
cluster_relmap_checkpoint_recheck_v2(ClusterRelmapCheckpointReceiptV2 *r,
	ClusterControlRelmapSelectionV2 *s)
{
	ClusterControlRelmapReferenceV2 ref;
	ClusterRecoveryCheckpointSourceV2 source;
	if (!r || !r->valid)
		return false;
	r->valid = false; /* First operation: ERROR or late refusal cannot revive it. */
	if (!s || CritSectionCount || LWLockHeldByMe(ControlFileLock)
		|| LWLockHeldByMe(RelationMappingLock)
		|| cluster_control_root_relmap_selection_complete_locked_v4(s, r->keys,
			r->summary.control.logical_count) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !cluster_control_root_relmap_selection_reference_v4(s, &ref)
		|| memcmp(&ref, &r->summary.control, sizeof(ref)))
		return false;
	for (uint64 i = 0; i < ref.source_count; i++)
		if (!cluster_control_root_relmap_selection_source_at_v4(s, i, &source)
			|| memcmp(&source, &r->sources[i], sizeof(source)))
			return false;
	r->valid = true;
	return true;
}

bool
cluster_relmap_checkpoint_summary_v2(const ClusterRelmapCheckpointReceiptV2 *r,
	ClusterRelmapCheckpointSummaryV2 *out)
{
	if (!r || !r->valid || !out || relmap_receipt_alias(r, out, sizeof(*out)))
		return false;
	*out = r->summary;
	return true;
}

bool
cluster_relmap_checkpoint_file_at_v2(const ClusterRelmapCheckpointReceiptV2 *r,
	uint64 index, ClusterRecoveryFileIdentityV2 *id, ClusterRelmapFilePairV2 *pair)
{
	if (!r || !r->valid || index >= r->summary.control.logical_count || !id || !pair
		|| relmap_receipt_alias(r, id, sizeof(*id)) || relmap_receipt_alias(r, pair, sizeof(*pair))
		|| relmap_control_overlap(id, sizeof(*id), pair, sizeof(*pair)))
		return false;
	*id = r->keys[index];
	*pair = r->pairs[index];
	return true;
}

bool
cluster_relmap_checkpoint_source_at_v2(const ClusterRelmapCheckpointReceiptV2 *r,
	uint64 index, ClusterRecoveryCheckpointSourceV2 *out)
{
	if (!r || !r->valid || index >= r->summary.control.source_count || !out
		|| relmap_receipt_alias(r, out, sizeof(*out)))
		return false;
	*out = r->sources[index];
	return true;
}

/* Heap state survives ERROR longjmp; failed borrow retirement retains its
 * address-bound original counter, and PANICs rather than discarding custody. */
/* Only the original current decoded record supplies EndRecPtr. This does not
 * authenticate an episode or replace its independent RESERVED registration. */
bool
cluster_relmap_control_create_redo_input_v1(XLogReaderState *reader,
	const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch[16],
	ClusterRelmapCreateReplayV1 *out)
{
	DecodedXLogRecord *record;
	if (reader == NULL || out == NULL
		|| relmap_control_overlap(out, sizeof(*out), reader, sizeof(*reader)))
		return false;
	record = reader->record;
	if (record == NULL || relmap_control_overlap(out, sizeof(*out), record, sizeof(*record)))
		return false;
	if (record->size < sizeof(*record)
		|| relmap_control_overlap(out, sizeof(*out), record, record->size)
		|| XLogRecGetRmid(reader) != RM_RELMAP_ID
		|| (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) != CLUSTER_RELMAP_CREATE_WAL_INFO_V1
		|| XLogRecHasAnyBlockRefs(reader) || record->has_page_version_edge
		|| XLogRecGetData(reader) == NULL
		|| XLogRecGetDataLen(reader) != CLUSTER_RELMAP_CREATE_WAL_SIZE_V1
		|| reader->ReadRecPtr != record->lsn || reader->EndRecPtr != record->next_lsn
		|| XLogRecPtrIsInvalid(reader->ReadRecPtr) || reader->EndRecPtr <= reader->ReadRecPtr)
		return false;
	return cluster_relmap_create_rebuild_v1(XLogRecGetData(reader), XLogRecGetDataLen(reader),
		expected, batch, reader->EndRecPtr, out);
}

/* One successful original execution owns these LOCAL saved observations.
 * Keep the exact confirmed authorization for A's later read-only recheck;
 * neither a saved digest nor a copied observation is current authority. */
struct ClusterRelmapRecoveryCreateReceiptV2
{
	uintptr_t self;
	int pid;
	bool failed, checking;
	ClusterColdTypedV1 *typed;
	struct ClusterRecoveryFencePlan *barrier_fence;
	uint64 barrier_episode_id;
	ClusterControlRelmapRecoveryCreateV2 *recovery;
	ClusterRelmapRecoveryCreateObservationV2 observation;
	ClusterRecoveryCheckpointSourceV2 *cuts;
	ClusterRecoveryCheckpointFileSourceV2 *sources;
	ClusterRecoveryCheckpointFileV2 files[2];
};

static bool
relmap_create_receipt_owned(const ClusterRelmapRecoveryCreateReceiptV2 *r)
{
	return r != NULL && r->self == (uintptr_t)r && r->pid == MyProcPid;
}

/* The original plan calls this storage-only predicate. Do not recurse into
 * that plan's output predicate while inspecting an attached receipt. */
static bool
relmap_create_receipt_storage_distinct(const void *value, const void *out, Size n)
{
	const ClusterRelmapRecoveryCreateReceiptV2 *r = value;
	return relmap_create_receipt_owned(r)
		&& !relmap_control_overlap(r, sizeof(*r), out, n)
		&& !relmap_control_overlap(r->cuts, r->observation.source_count * sizeof(*r->cuts), out, n)
		&& !relmap_control_overlap(r->sources, r->observation.source_count * sizeof(*r->sources), out, n);
}

static bool
relmap_create_receipt_alias(const ClusterRelmapRecoveryCreateReceiptV2 *r, const void *out, Size n)
{
	return !relmap_create_receipt_storage_distinct(r, out, n)
		|| !cold_relmap_output_distinct(r->typed, out, n)
		|| !cluster_control_native_files_output_distinct_v2(out, n);
}

void
cluster_relmap_recovery_create_destroy_v2(ClusterRelmapRecoveryCreateReceiptV2 **out)
{
	ClusterRelmapRecoveryCreateReceiptV2 *r;
	if (out == NULL || (r = *out) == NULL || !relmap_create_receipt_owned(r)
		|| relmap_create_receipt_alias(r, out, sizeof(*out))) return;
	*out = NULL;
	r->self = 0;
	cluster_control_relmap_recovery_create_destroy_v2(&r->recovery);
	if (r->cuts) pfree(r->cuts);
	if (r->sources) pfree(r->sources);
	pfree(r);
}

/* These are saved-input getters, deliberately not a currentness check. */
static bool
relmap_create_receipt_output(const ClusterRelmapRecoveryCreateReceiptV2 *r, void *out, Size n)
{
	return out != NULL && relmap_create_receipt_owned(r) && r->recovery != NULL
		&& r->observation.source_count != 0 && r->cuts != NULL && r->sources != NULL
		&& !relmap_create_receipt_alias(r, out, n);
}

bool
cluster_relmap_recovery_create_observation_v2(const ClusterRelmapRecoveryCreateReceiptV2 *r,
	ClusterRelmapRecoveryCreateObservationV2 *out)
{
	if (!relmap_create_receipt_output(r, out, sizeof(*out))) return false;
	*out = r->observation;
	return true;
}

bool
cluster_relmap_recovery_create_file_at_v2(const ClusterRelmapRecoveryCreateReceiptV2 *r,
	uint32 index, ClusterRecoveryCheckpointFileV2 *out)
{
	if (!relmap_create_receipt_output(r, out, sizeof(*out)) || index >= 2) return false;
	*out = r->files[index];
	return true;
}

bool
cluster_relmap_recovery_create_source_at_v2(const ClusterRelmapRecoveryCreateReceiptV2 *r,
	uint64 index, ClusterRecoveryCheckpointSourceV2 *out)
{
	if (!relmap_create_receipt_output(r, out, sizeof(*out)) || index >= r->observation.source_count)
		return false;
	*out = r->cuts[index];
	return true;
}

typedef struct RelmapCreateWork
{
	ClusterRelmapRecoveryCreateReceiptV2 *receipt;
	ClusterControlRelmapSelectionV2 *selection;
	ClusterControlRelmapRecoveryCreateV2 *recovery;
	ClusterControlNativeFileBorrowV2 source, target;
	ClusterRelmapFilePairV2 source_pair, target_pair;
	ClusterRecoveryCheckpointFileSourceV2 *sources;
	ClusterRecoveryCheckpointFileV2 receipts[2];
	unsigned char creation_wal[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
	ClusterRelmapCreateReplayV1 creation;
	XLogRecPtr creation_end;
	pg_cryptohash_ctx *hash;
	bool source_held, target_held, mapping;
} RelmapCreateWork;

static void
relmap_create_cleanup(RelmapCreateWork *w, bool error_cleanup)
{
	HOLD_INTERRUPTS();
	/* These are table borrows, not the caller's original global X, CF,
	 * target file duty, reservation or debt. Never retry a failed release. */
	if (w->source_held) {
		w->source_held = false;
		if (!cluster_control_native_file_release_v2(&w->source))
			elog(PANIC, "could not retire original relmap source borrow");
	}
	if (w->target_held) {
		w->target_held = false;
		if (!cluster_control_native_file_release_v2(&w->target))
			elog(PANIC, "could not retire original relmap target borrow");
	}
	if (w->mapping && LWLockHeldByMe(RelationMappingLock)) {
		/* errfinish(ERROR) resets the acquisition's interrupt count. Match
		 * this retail release as LWLockReleaseAll does, while preserving
		 * our separate cleanup holdoff. A second ERROR resets again and
		 * escapes directly to the original outer owner, without RESUME. */
		if (error_cleanup) HOLD_INTERRUPTS();
		w->mapping = false;
		LWLockRelease(RelationMappingLock);
	}
	cluster_relmap_recovery_create_destroy_v2(&w->receipt);
	cluster_control_relmap_recovery_create_destroy_v2(&w->recovery);
	cluster_control_root_relmap_selection_destroy_v4(&w->selection);
	if (w->hash) pg_cryptohash_free(w->hash);
	if (w->sources) pfree(w->sources);
	pfree(w);
	RESUME_INTERRUPTS();
}

bool
cluster_relmap_control_copy_create_v2(ClusterControlNativeRelmapCreateV2 *c,
	Oid source_database, Oid source_tablespace)
{
	RelmapCreateWork *w;
	ClusterControlRelmapReferenceV2 reference;
	ClusterRecoveryCheckpointSourceV2 source;
	volatile bool ok = false;

	if (!cluster_control_native_files_output_distinct_v2(c, sizeof(*c))
		|| c->private_self != (uintptr_t)c || c->private_pid != MyProcPid
		|| c->private_attempted || c->private_copy_attempted)
		return false;
	c->private_copy_attempted = true;
	if (!cluster_relmap_authority_x_held() || !cluster_cf_held_is_clusterwide(ExclusiveLock)
		|| c->private_cf_sequence != cluster_cf_attempt_sequence() || CritSectionCount
		|| LWLockHeldByMe(ControlFileLock) || LWLockHeldByMe(RelationMappingLock)
		|| LWLockHeldByMe(XidGenLock) || LWLockHeldByMe(XactSLRULock)
		|| LWLockHeldByMe(XactTruncationLock) || !enableFsync || RecoveryInProgress()
		|| !OidIsValid(source_database) || source_database == c->expected.database_oid
		|| source_tablespace != DEFAULTTABLESPACE_OID
		|| c->expected.tablespace_oid != DEFAULTTABLESPACE_OID)
		return false;
	w = palloc0(sizeof(*w));
	PG_TRY();
	{
		do {
			uint64 count;
			/* Full original target authorization and sources precede both
			 * MappingLock and native I/O. No target identity is cloned. */
			if (cluster_control_native_file_reserved_acquire_locked_v2(&c->control.config,
					CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, c->startup_generation,
					c->batch_uuid, &c->expected, &w->target) != 0) break;
			w->target_held = true;
			if (cluster_control_root_relmap_selection_read_locked_v4(&c->control.config,
					CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, &w->selection) != 0
				|| !cluster_control_root_relmap_selection_reference_v4(w->selection, &reference)
				|| memcmp(&reference, &c->control, sizeof(reference))) break;
			count = cluster_control_root_relmap_selection_source_count_v4(w->selection);
			if (!count || count > CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2 / sizeof(*w->sources)) break;
			w->sources = palloc0(count * sizeof(*w->sources));
			for (uint64 i = 0; i < count; ++i) {
				if (!cluster_control_root_relmap_selection_source_at_v4(w->selection, i, &source)) goto finished;
				w->sources[i] = (ClusterRecoveryCheckpointFileSourceV2){i,source.redo_start,source.replay_end};
			}
			LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
			w->mapping = true;
			if (cluster_control_native_file_borrow_by_address_v2(CLUSTER_RECOVERY_FILE_RELMAP_V2,
					source_database, source_tablespace, 0, &w->source) != 0) break;
			w->source_held = true;
			if (!cluster_control_native_file_current_v2(&w->source)
				|| !cluster_control_native_file_current_v2(&w->target)
				|| !cluster_relmap_authority_managed_read_v2(&w->source.expected, &w->source_pair)
				|| !relmap_control_pair(&w->source.expected, &w->source_pair)
				|| w->source_pair.view.has_pending
				|| !cluster_control_native_file_current_v2(&w->source)
				|| !cluster_control_native_file_current_v2(&w->target)
				|| !cluster_relmap_authority_x_held()
				|| !cluster_cf_held_is_clusterwide(ExclusiveLock)
				|| c->private_cf_sequence != cluster_cf_attempt_sequence()) break;
			/* An independent creation record carries only the original target,
			 * creation batch and complete clean generation-one image. Neither
			 * a pending transaction nor a guessed record end is manufactured.
			 * The carrier's one-shot latch already owns failure after WAL. */
			if (!cluster_relmap_create_encode_v1(&w->target.expected, c->batch_uuid,
					&w->source_pair.view.committed, w->creation_wal, sizeof(w->creation_wal))) break;
			XLogBeginInsert();
			XLogRegisterData((char *) w->creation_wal, sizeof(w->creation_wal));
			w->creation_end = XLogInsert(RM_RELMAP_ID, CLUSTER_RELMAP_CREATE_WAL_INFO_V1);
			if (XLogRecPtrIsInvalid(w->creation_end) || w->creation_end == UINT64_MAX) break;
			XLogFlush(w->creation_end);
			/* No target name is opened for writing until the actual insertion
			 * end has flushed and the original protected inputs remain current.
			 * Install exactly the WAL image, never a newly sampled local map. */
			if (!cluster_relmap_authority_x_held()
				|| !cluster_cf_held_is_clusterwide(ExclusiveLock)
				|| c->private_cf_sequence != cluster_cf_attempt_sequence()
				|| !cluster_control_native_file_current_v2(&w->source)
				|| !cluster_control_native_file_current_v2(&w->target)
				|| !cluster_relmap_create_rebuild_v1(w->creation_wal, sizeof(w->creation_wal),
					&c->expected, c->batch_uuid, w->creation_end, &w->creation)
				|| memcmp(&w->creation.record.committed, &w->source_pair.view.committed, sizeof(RelMapFile))
				|| !cluster_relmap_authority_managed_create_v2(&w->target.expected,
					&w->creation.record.committed, &w->target_pair)
				|| !cluster_control_native_file_current_v2(&w->source)
				|| !cluster_control_native_file_current_v2(&w->target)) break;
			for (unsigned i = 0; i < 2; ++i) {
				ClusterRecoveryCheckpointFileV2 *r = &w->receipts[i];
				r->identity = w->target.expected;
				r->synced_bytes = CLUSTER_RELMAP_FILE_SIZE_V2;
				r->page_count = 1;
				r->copy_index = i;
				r->copy_count = 2;
				r->source_count = count;
				r->sources = w->sources;
				w->hash = pg_cryptohash_create(PG_SHA256);
				if (!w->hash || pg_cryptohash_init(w->hash) < 0
					|| pg_cryptohash_update(w->hash, w->target_pair.bytes[i], sizeof(w->target_pair.bytes[i])) < 0
					|| pg_cryptohash_final(w->hash, r->image_digest, sizeof(r->image_digest)) < 0) goto finished;
				pg_cryptohash_free(w->hash); w->hash = NULL;
			}
			/* Publish needs a new table generation. Retire BOTH borrowed
			 * counters while retaining SAME original X/CF/file protection. */
			w->source_held = false;
			if (!cluster_control_native_file_release_v2(&w->source))
				elog(PANIC, "could not retire original relmap source borrow");
			w->target_held = false;
			if (!cluster_control_native_file_release_v2(&w->target))
				elog(PANIC, "could not retire original relmap target borrow");
			w->mapping = false;
			LWLockRelease(RelationMappingLock);
			if (!cluster_relmap_authority_x_held()
				|| cluster_control_root_relmap_selection_recheck_locked_v4(w->selection) != 0
				|| c->private_cf_sequence != cluster_cf_attempt_sequence()) break;
			ok = cluster_control_native_files_complete_relmap_create_locked_v2(c, w->receipts, 2) == 0
				&& cluster_relmap_authority_x_held();
		} while (0);
finished:;
	}
	PG_CATCH();
	{
		relmap_create_cleanup(w, true);
		PG_RE_THROW();
	}
	PG_END_TRY();
	relmap_create_cleanup(w, false);
	return ok;
}

/* The existing native scanner/frame identity, not merely an equal position
 * or a valid inner RMC1 CRC. Original XLogReader owns native CRC validation. */
static bool
relmap_recovery_create_frame(const ClusterColdTypedV1 *typed, uint32 participant,
	const ClusterWalSourceRef *source, const XLogReaderState *reader,
	ClusterColdRecordFrameV1 *out)
{
	ClusterColdRecordFrameV1 frame, found;
	uint32 count, matches = 0;
	if (!typed->plan || typed->refusal != CLUSTER_COLD_OK || !reader->record
		|| reader->cluster_record_crc_failed
		|| reader->system_identifier != source->claim.identity.system_identifier
		|| reader->cluster_expected_thread_id != source->claim.identity.origin_thread_id
		|| reader->seg.ws_tli != source->timeline
		|| !cluster_cold_plan_input_frame_count_v1(typed->plan, &count))
		return false;
	for (uint32 i = 0; i < count; ++i) {
		if (!cluster_cold_plan_input_frame_v1(typed->plan, i, &frame)) return false;
		if (frame.participant != participant || frame.read_rec_ptr != reader->ReadRecPtr
			|| frame.end_rec_ptr != reader->EndRecPtr) continue;
		if (frame.history || frame.page_components || frame.space_effects
			|| frame.rmid != XLogRecGetRmid(reader) || frame.info != XLogRecGetInfo(reader)
			|| frame.record_crc != reader->record->header.xl_crc
			|| frame.source_xid != XLogRecGetXid(reader)
			|| frame.source_top_xid != XLogRecGetTopXid(reader)) return false;
		found = frame;
		++matches;
	}
	if (matches != 1) return false;
	if (out != NULL) *out = found;
	return true;
}

/* Only the creation consumer can replace serial custody with the original H
 * episode. Ordinary pending and generic source validation keep their own gates. */
static bool
relmap_create_sources_current(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 episode_id)
{
	return episode_id != 0
		? cluster_cold_typed_relmap_create_barrier_current_v2(typed, fence, episode_id)
		: cluster_cold_typed_sources_current_v1(typed, fence);
}

static bool
relmap_recovery_create(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 episode_id, uint32 participant, const ClusterWalSourceRef *source,
	XLogReaderState *reader, const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch[16],
	Size receipt_budget, ClusterRelmapRecoveryCreateReceiptV2 **fresh)
{
	RelmapCreateWork *w;
	ClusterRelmapRecoveryCreateReceiptV2 *retained = NULL;
	ClusterControlRelmapFormatV2 format;
	ClusterRecoveryCheckpointSourceV2 cut;
	ClusterRelmapCreateReplayV1 reread;
	volatile bool ok = false;

	if (!typed || !fence || !source || !reader || !expected || !batch
		|| !cluster_relmap_authority_x_held() || !cluster_cf_held_is_clusterwide(ExclusiveLock)
		|| CritSectionCount || !enableFsync || !RecoveryInProgress() || MyBackendType != B_STARTUP
		|| LWLockHeldByMe(ControlFileLock) || LWLockHeldByMe(RelationMappingLock)
		|| LWLockHeldByMe(XidGenLock) || LWLockHeldByMe(XactSLRULock)
		|| LWLockHeldByMe(XactTruncationLock))
		return false;
	w = palloc0(sizeof(*w));
	PG_TRY();
	{
		do {
			uint64 count;
			if (fresh != NULL) {
				w->receipt = palloc0(sizeof(*w->receipt));
				w->receipt->self = (uintptr_t)w->receipt;
				w->receipt->pid = MyProcPid;
				w->receipt->typed = typed;
				w->receipt->barrier_fence = episode_id ? fence : NULL;
				w->receipt->barrier_episode_id = episode_id;
				w->receipt->observation.participant = participant;
				w->receipt->observation.source = *source;
			}
			/* No runtime creator, current-file batch inference or new scope.
			 * The original Startup owner independently authenticates the sealed
			 * record and durable RESERVED/AVAILABLE registration before I/O. */
			if (!cluster_relmap_control_create_redo_input_v1(reader, expected, batch, &w->creation)
				|| !relmap_recovery_create_frame(typed, participant, source, reader,
					w->receipt != NULL ? &w->receipt->observation.frame : NULL)
				|| (episode_id != 0
					? cluster_control_relmap_recovery_create_begin_barrier_locked_v2(typed, fence, episode_id,
						participant, source, reader->ReadRecPtr, w->creation.actual_end, expected, batch, &w->recovery)
					: cluster_control_relmap_recovery_create_begin_locked_v2(typed, fence, participant,
						source, reader->ReadRecPtr, w->creation.actual_end, expected, batch, &w->recovery)) != 0
				|| cluster_control_root_relmap_format_locked_v2(
					CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, &format) != 0
				|| format.root_version != 4
				|| cluster_control_root_relmap_selection_read_locked_v4(&format.config,
					CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, &w->selection) != 0) break;
			count = cluster_control_root_relmap_selection_source_count_v4(w->selection);
			if (!count || count > CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2 / sizeof(*w->sources)) break;
			/* Reserve the COMPLETE saved vectors before the first target write.
			 * The original selection/authorization keeps its existing budget. */
			if (w->receipt != NULL) {
				if (count > (receipt_budget - sizeof(*w->receipt)) /
					(sizeof(*w->receipt->cuts) + sizeof(*w->sources))) break;
				w->receipt->observation.source_count = count;
				w->receipt->cuts = palloc0(count * sizeof(*w->receipt->cuts));
			}
			w->sources = palloc0(count * sizeof(*w->sources));
			for (uint64 i = 0; i < count; ++i) {
				if (!cluster_control_root_relmap_selection_source_at_v4(w->selection, i, &cut)) goto done;
				w->sources[i] = (ClusterRecoveryCheckpointFileSourceV2){i, cut.redo_start, cut.replay_end};
				if (w->receipt != NULL) w->receipt->cuts[i] = cut;
			}
			if (!cluster_control_relmap_recovery_create_current_v2(w->recovery)
				|| !cluster_relmap_authority_x_held()) break;
			LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
			w->mapping = true;
			if (!cluster_control_relmap_recovery_create_current_v2(w->recovery)
				|| !cluster_relmap_authority_managed_replay_create_v2(&w->creation.record.identity,
					&w->creation.record.committed, &w->target_pair)
				|| !cluster_control_relmap_recovery_create_current_v2(w->recovery)
				|| !cluster_relmap_authority_x_held()) break;
			for (unsigned i = 0; i < 2; ++i) {
				ClusterRecoveryCheckpointFileV2 *r = &w->receipts[i];
				r->identity = w->creation.record.identity;
				r->synced_bytes = sizeof(w->target_pair.bytes[i]);
				r->page_count = 1;
				r->copy_index = i;
				r->copy_count = 2;
				r->source_count = count;
				r->sources = w->sources;
				w->hash = pg_cryptohash_create(PG_SHA256);
				if (!w->hash || pg_cryptohash_init(w->hash) < 0
					|| pg_cryptohash_update(w->hash, w->target_pair.bytes[i], sizeof(w->target_pair.bytes[i])) < 0
					|| pg_cryptohash_final(w->hash, r->image_digest, sizeof(r->image_digest)) < 0) goto done;
				pg_cryptohash_free(w->hash); w->hash = NULL;
			}
			/* Retain the acquired flag across a fallible release. Cleanup
			 * checks actual ownership so an interrupt after unlock cannot
			 * release another lock or decrement its holdoff twice. */
			LWLockRelease(RelationMappingLock);
			w->mapping = false;
			if (!cluster_control_relmap_recovery_create_current_v2(w->recovery)
				|| !cluster_relmap_authority_x_held() || !enableFsync
				|| !cluster_relmap_control_create_redo_input_v1(reader, expected, batch, &reread)
				|| !relmap_recovery_create_frame(typed, participant, source, reader, NULL)
				|| memcmp(&reread, &w->creation, sizeof(reread))
				|| cluster_control_root_relmap_selection_recheck_locked_v4(w->selection) != 0) break;
			ok = cluster_control_relmap_recovery_create_confirm_locked_v2(w->recovery, w->receipts, 2) == 0
				&& cluster_relmap_authority_x_held();
		} while (0);
done:;
	}
	PG_CATCH();
	{
		cluster_cold_typed_relmap_create_failed_v2(typed);
		relmap_create_cleanup(w, true);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!ok) cluster_cold_typed_relmap_create_failed_v2(typed);
	if (ok && fresh != NULL) {
		retained = w->receipt;
		retained->observation.creation = w->creation;
		memcpy(retained->files, w->receipts, sizeof(retained->files));
		/* Move the ORIGINAL confirmed carrier and vector, never dangling
		 * scratch pointers or a reconstruction obtained by replaying again. */
		retained->sources = w->sources; w->sources = NULL;
		retained->recovery = w->recovery; w->recovery = NULL;
		w->receipt = NULL;
	}
	relmap_create_cleanup(w, false);
	if (retained != NULL) *fresh = retained;
	return ok;
}

bool
cluster_relmap_control_recovery_create_v2(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint32 participant, const ClusterWalSourceRef *source,
	XLogReaderState *reader, const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch[16])
{
	return relmap_recovery_create(typed, fence, 0, participant, source, reader, expected, batch, 0, NULL);
}

static bool
relmap_recovery_create_capture(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 episode_id, uint32 participant, const ClusterWalSourceRef *source,
	XLogReaderState *reader, const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch[16],
	Size receipt_budget, ClusterRelmapRecoveryCreateReceiptV2 **out)
{
	/* Reject overlaps BEFORE looking at *out or acquiring original authority.
	 * Neither a NULL field inside the reader nor a plan slot is a fresh output. */
	if (out == NULL || typed == NULL || reader == NULL || source == NULL || expected == NULL || batch == NULL
		|| receipt_budget < sizeof(ClusterRelmapRecoveryCreateReceiptV2)
		|| !cold_relmap_output_distinct(typed, out, sizeof(*out))
		|| !cluster_control_native_files_output_distinct_v2(out, sizeof(*out))
		|| relmap_control_overlap(source, sizeof(*source), out, sizeof(*out))
		|| relmap_control_overlap(expected, sizeof(*expected), out, sizeof(*out))
		|| relmap_control_overlap(batch, 16, out, sizeof(*out))
		|| relmap_control_overlap(reader, sizeof(*reader), out, sizeof(*out))
		|| (reader->record != NULL &&
			(relmap_control_overlap(reader->record, Max(sizeof(*reader->record), reader->record->size), out, sizeof(*out))
			 || relmap_control_overlap(XLogRecGetData(reader), XLogRecGetDataLen(reader), out, sizeof(*out))))
		|| *out != NULL) return false;
	return relmap_recovery_create(typed, fence, episode_id, participant, source, reader, expected, batch, receipt_budget, out);
}

bool
cluster_relmap_control_recovery_create_capture_v2(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint32 participant, const ClusterWalSourceRef *source,
	XLogReaderState *reader, const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch[16],
	Size budget, ClusterRelmapRecoveryCreateReceiptV2 **out)
{
	return relmap_recovery_create_capture(typed, fence, 0, participant, source, reader, expected, batch, budget, out);
}

bool
cluster_relmap_control_recovery_create_capture_barrier_v2(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 episode_id, uint32 participant,
	const ClusterWalSourceRef *source, XLogReaderState *reader,
	const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch[16],
	Size budget, ClusterRelmapRecoveryCreateReceiptV2 **out)
{
	return episode_id != 0 && relmap_recovery_create_capture(typed, fence, episode_id,
		participant, source, reader, expected, batch, budget, out);
}

static void
relmap_create_observation_failed(ClusterRelmapRecoveryCreateReceiptV2 *r)
{
	if (!relmap_create_receipt_owned(r)) return;
	r->failed = true;
	if (r->typed != NULL) cluster_cold_plan_relmap_invalidate_v1(r->typed->plan);
	cluster_control_relmap_recovery_create_fail_v2(r->recovery);
}

static bool
relmap_create_saved_owner(const ClusterRelmapRecoveryCreateReceiptV2 *r,
	const ClusterColdTypedV1 *typed, const struct ClusterRecoveryFencePlan *fence,
	const ClusterColdPlanV1 *plan, uint64 episode_id)
{
	const ClusterRelmapRecoveryCreateObservationV2 *o = &r->observation;
	uint32 matches = 0, p = o->participant;
	if (r->failed || !r->checking || r->typed != typed || r->recovery == NULL
		|| r->barrier_episode_id != episode_id
		|| (episode_id != 0 ? r->barrier_fence != fence : typed->tx_fence != fence)
		|| typed->plan != plan || plan->magic != CLUSTER_COLD_PLAN_MAGIC
		|| plan->phase != COLD_PHASE_SEALED || plan->relmap_failed || typed->refusal != CLUSTER_COLD_OK
		|| !typed->participant_count || typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| plan->participant_count != typed->participant_count || !typed->replay_count
		|| typed->replay_count > typed->participant_count || p >= typed->replay_count
		|| typed->system_identifier != o->source.claim.identity.system_identifier
		|| memcmp(&o->source, &typed->sources[p], sizeof(o->source))) return false;
	for (uint32 i = 0; i < plan->relmap_create_count; ++i) {
		const ColdRelmapCreateInput *entry = cold_relmap_create_at(plan, i);
		const ClusterColdRelmapCreateInputV1 *in;
		if (entry == NULL) return false;
		if (entry->receipt != r) continue;
		in = &entry->input;
		if (!entry->captured || !entry->attempted || !entry->applied
			|| entry->receipt_release == NULL || entry->receipt_output_distinct == NULL
			|| in->participant != p || !cold_relmap_cut_equal(&in->cut, &typed->participants[p])
			|| memcmp(&in->source, &o->source, sizeof(in->source))
			|| memcmp(&in->frame, &o->frame, sizeof(in->frame))
			|| memcmp(&in->creation, &o->creation.record, sizeof(in->creation))) return false;
		++matches;
	}
	return matches == 1;
}

static bool
relmap_create_saved_sources(const ClusterRelmapRecoveryCreateReceiptV2 *r,
	ClusterControlRelmapSelectionV2 *selection)
{
	const ClusterRelmapRecoveryCreateObservationV2 *o = &r->observation;
	ClusterRecoveryCheckpointSourceV2 source;
	ClusterRecoveryFileIdentityV2 identity;
	if (!o->source_count || !r->cuts || !r->sources
		|| cluster_control_root_relmap_selection_source_count_v4(selection) != o->source_count
		|| !cluster_control_root_relmap_selection_lookup_v4(selection,
			o->creation.record.identity.database_oid, o->creation.record.identity.tablespace_oid,
			o->creation.record.identity.object_id, &identity)
		|| memcmp(&identity, &o->creation.record.identity, sizeof(identity))) return false;
	for (uint64 j = 0; j < o->source_count; ++j) {
		if (!cluster_control_root_relmap_selection_source_at_v4(selection, j, &source)
			|| memcmp(&source, &r->cuts[j], sizeof(source)) || r->sources[j].source_index != j
			|| r->sources[j].redo_start != source.redo_start
			|| r->sources[j].replay_end != source.replay_end) return false;
	}
	for (unsigned i = 0; i < 2; ++i) {
		const ClusterRecoveryCheckpointFileV2 *file = &r->files[i];
		if (memcmp(&file->identity, &identity, sizeof(identity)) || file->copy_index != i
			|| file->copy_count != 2 || file->synced_bytes != CLUSTER_RELMAP_FILE_SIZE_V2
			|| file->first_page || file->page_count != 1 || file->source_count != o->source_count
			|| file->sources != r->sources) return false;
	}
	return true;
}

static bool
relmap_create_receipt_recheck(ClusterRelmapRecoveryCreateReceiptV2 *r,
	ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence, uint64 episode_id, bool confirmed)
{
	ClusterColdPlanV1 *plan;
	RelmapCreateWork *volatile work = NULL;
	volatile bool charged = false, ok = false;
	volatile Size bytes = sizeof(RelmapCreateWork);
	if (!relmap_create_receipt_owned(r)) return false;
	if (r->failed || r->checking) { relmap_create_observation_failed(r); return false; }
	/* Latch before the first provider/allocation. An interrupted or reentrant
	 * check never turns the saved successful execution into retry permission. */
	r->checking = true;
	plan = r->typed != NULL ? r->typed->plan : NULL;
	PG_TRY();
	{
		do {
			RelmapCreateWork *w;
			ClusterControlRelmapFormatV2 format;
			uint8 digest[32];
			if (typed == NULL || fence == NULL || plan == NULL
				|| !relmap_create_saved_owner(r, typed, fence, plan, episode_id)
				|| !cluster_relmap_authority_x_held() || !cluster_cf_held_is_clusterwide(ExclusiveLock)
				|| CritSectionCount || !enableFsync || !RecoveryInProgress() || MyBackendType != B_STARTUP
				|| LWLockHeldByMe(ControlFileLock) || LWLockHeldByMe(RelationMappingLock)
				|| LWLockHeldByMe(XidGenLock) || LWLockHeldByMe(XactSLRULock)
				|| LWLockHeldByMe(XactTruncationLock)) break;
			if (confirmed) {
				if (!r->sources || !r->cuts || !r->observation.source_count || r->observation.source_count >
					(CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2 - bytes) / sizeof(*w->sources)) break;
				bytes += r->observation.source_count * sizeof(*w->sources);
			}
			if (!cold_plan_reserve(plan, bytes)) break;
			charged = true;
			work = w = palloc0(sizeof(*w));
			if (confirmed) {
				/* The confirmed reader forbids aliases with original plan storage.
				 * Copy its ORIGINAL immutable receipts, not new file-derived ones. */
				w->sources = palloc(bytes - sizeof(*w));
				memcpy(w->sources, r->sources, bytes - sizeof(*w));
				memcpy(w->receipts, r->files, sizeof(w->receipts));
				for (unsigned i = 0; i < 2; ++i) w->receipts[i].sources = w->sources;
			}
			if (!relmap_create_sources_current(typed, fence, episode_id)
				|| !relmap_create_saved_owner(r, typed, fence, plan, episode_id)
				|| !cluster_relmap_create_encode_v1(&r->observation.creation.record.identity,
					r->observation.creation.record.batch_uuid, &r->observation.creation.record.committed,
					w->creation_wal, sizeof(w->creation_wal))
				|| cluster_control_root_relmap_format_locked_v2(CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2,
					&format) != 0 || format.root_version != 4
				|| cluster_control_root_relmap_selection_read_locked_v4(&format.config,
					CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, &w->selection) != 0
				|| !relmap_create_saved_sources(r, w->selection)) break;
			LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
			w->mapping = true;
			if (!cluster_relmap_authority_managed_read_v2(&r->observation.creation.record.identity,
				&w->target_pair)) break;
			for (unsigned i = 0; i < 2; ++i) {
				if (memcmp(w->target_pair.bytes[i], w->creation_wal + CLUSTER_RELMAP_CREATE_HEADER_SIZE_V1,
					CLUSTER_RELMAP_FILE_SIZE_V2)) goto checked;
				w->hash = pg_cryptohash_create(PG_SHA256);
				if (!w->hash || pg_cryptohash_init(w->hash) < 0
					|| pg_cryptohash_update(w->hash, w->target_pair.bytes[i], sizeof(w->target_pair.bytes[i])) < 0
					|| pg_cryptohash_final(w->hash, digest, sizeof(digest)) < 0
					|| memcmp(digest, r->files[i].image_digest, sizeof(digest))) goto checked;
				pg_cryptohash_free(w->hash); w->hash = NULL;
			}
			LWLockRelease(RelationMappingLock);
			w->mapping = false;
			ok = relmap_create_sources_current(typed, fence, episode_id)
				&& relmap_create_saved_owner(r, typed, fence, plan, episode_id)
				&& relmap_create_saved_sources(r, w->selection)
				&& cluster_control_root_relmap_selection_recheck_locked_v4(w->selection) == 0
				&& relmap_create_saved_owner(r, typed, fence, plan, episode_id)
				&& cluster_relmap_authority_x_held() && cluster_cf_held_is_clusterwide(ExclusiveLock);
			if (ok && confirmed)
				ok = (episode_id != 0
					? cluster_control_relmap_recovery_create_confirmed_barrier_current_v2(
						r->recovery, episode_id, w->receipts, 2)
					: cluster_control_relmap_recovery_create_confirmed_current_v2(r->recovery, w->receipts, 2))
					&& relmap_create_sources_current(typed, fence, episode_id)
					&& relmap_create_saved_owner(r, typed, fence, plan, episode_id)
					&& relmap_create_saved_sources(r, w->selection)
					&& cluster_control_root_relmap_selection_recheck_locked_v4(w->selection) == 0;
		} while (0);
checked:
		if (work != NULL) {
			RelmapCreateWork *cleanup = work;
			work = NULL; /* A cleanup ERROR must not use freed scratch again. */
			relmap_create_cleanup(cleanup, false);
		}
		if (charged) { cold_plan_release(plan, bytes); charged = false; }
	}
	PG_CATCH();
	{
		relmap_create_observation_failed(r);
		if (work != NULL) {
			RelmapCreateWork *cleanup = work;
			work = NULL;
			relmap_create_cleanup(cleanup, true);
		}
		if (charged) cold_plan_release(plan, bytes);
		PG_RE_THROW();
	}
	PG_END_TRY();
	r->checking = false;
	if (!ok) relmap_create_observation_failed(r);
	return ok;
}

bool
cluster_relmap_recovery_create_physical_recheck_v2(ClusterRelmapRecoveryCreateReceiptV2 *r,
	ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence)
{
	return relmap_create_receipt_recheck(r, typed, fence, 0, false);
}

bool
cluster_relmap_recovery_create_recheck_v2(ClusterRelmapRecoveryCreateReceiptV2 *r,
	ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence)
{
	return relmap_create_receipt_recheck(r, typed, fence, 0, true);
}

bool
cluster_relmap_recovery_create_physical_recheck_barrier_v2(ClusterRelmapRecoveryCreateReceiptV2 *r,
	ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence, uint64 episode_id)
{
	return episode_id != 0 && relmap_create_receipt_recheck(r, typed, fence, episode_id, false);
}

bool
cluster_relmap_recovery_create_recheck_barrier_v2(ClusterRelmapRecoveryCreateReceiptV2 *r,
	ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence, uint64 episode_id)
{
	return episode_id != 0 && relmap_create_receipt_recheck(r, typed, fence, episode_id, true);
}

static void
relmap_create_receipt_release(void *value)
{
	ClusterRelmapRecoveryCreateReceiptV2 *r = value;
	cluster_relmap_recovery_create_destroy_v2(&r);
}

/* Check every original declaration and every selected SIDE plan, including
 * sources declaring zero creations. A current record cannot stand in for the
 * complete selected generation set or an earlier unfinished attempt. */
static bool
relmap_create_batch_current(const ClusterColdTypedV1 *typed,
	const ColdRelmapCreateInput *current, const XLogReaderState *reader)
{
	const ClusterColdPlanV1 *plan = typed->plan;
	if (typed->refusal != CLUSTER_COLD_OK || plan->relmap_failed || plan->phase != COLD_PHASE_SEALED
		|| typed->participant_count == 0 || typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->participant_count != plan->participant_count
		|| typed->replay_count > typed->participant_count || plan->relmap_create_count == 0)
		return false;
	for (uint32 p = 0; p < typed->participant_count; ++p) {
		uint32 expected = 0;
		const RfSideOnlinePlanV1 *side = typed->side_plans[p];
		if (!cold_relmap_cut_equal(&typed->participants[p],
				&plan->participants[plan->canonical[p]].cut)
			|| typed->sources[p].claim.identity.system_identifier != typed->system_identifier
			|| !rf_side_online_plan_recovery_sources_match_v1(side, typed->sources, typed->participant_count))
			return false;
		for (uint32 i = 0; i < plan->relmap_create_count; ++i) {
			const ColdRelmapCreateInput *entry = cold_relmap_create_at(plan, i);
			const ClusterColdRelmapCreateInputV1 *in;
			if (entry == NULL || !entry->captured) return false;
			in = &entry->input;
			if (in->participant >= typed->replay_count
				|| (entry != current && entry->attempted && (!entry->applied || entry->receipt == NULL
					|| entry->receipt_release == NULL || entry->receipt_output_distinct == NULL))) return false;
			if (in->participant != p) continue;
			if (!cold_relmap_cut_equal(&in->cut, &typed->participants[p])
				|| memcmp(&in->source, &typed->sources[p], sizeof(in->source))
				|| !rf_side_online_plan_relmap_create_input_matches_v1(side, in)) return false;
			++expected;
		}
		if (rf_side_online_plan_relmap_create_count_v1(side, &typed->sources[p]) != expected) return false;
	}
	return !plan->relmap_failed && plan->phase == COLD_PHASE_SEALED
		&& current->input.participant < typed->replay_count
		&& (reader == NULL || rf_side_online_plan_relmap_create_matches_v1(
			typed->side_plans[current->input.participant], &current->input.source, reader))
		&& !plan->relmap_failed;
}

static bool
relmap_create_apply(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 episode_id, const ClusterColdStepV1 *step,
	XLogReaderState *reader, Size receipt_budget)
{
	ClusterColdPlanV1 *plan;
	const ClusterColdRelmapCreateInputV1 *input;
	ColdRelmapCreateInput *entry = NULL;
	ClusterColdRelmapCreateInputV1 original;
	volatile bool ok = false;

	if (typed == NULL || (plan = typed->plan) == NULL || plan->magic != CLUSTER_COLD_PLAN_MAGIC)
		return false;
	input = cluster_cold_plan_relmap_create_step_v1(plan, step);
	if (input != NULL)
		for (uint32 i = 0; i < plan->relmap_create_count; ++i) {
			ColdRelmapCreateInput *candidate = cold_relmap_create_at(plan, i);
			if (candidate != NULL && &candidate->input == input) { entry = candidate; break; }
		}
	if (entry == NULL || plan->relmap_failed || plan->phase != COLD_PHASE_SEALED) goto failed;
	if (entry->attempted) {
		if (!entry->applied || entry->receipt == NULL) goto failed;
		PG_TRY();
		{
			ok = fence != NULL && (episode_id != 0 || typed->tx_fence == fence) && reader != NULL
				&& relmap_create_batch_current(typed, entry, reader)
				&& relmap_create_receipt_recheck(entry->receipt, typed, fence, episode_id, true)
				&& relmap_create_batch_current(typed, entry, reader);
		}
		PG_CATCH();
		{
			cluster_cold_plan_relmap_invalidate_v1(plan);
			cluster_cold_typed_relmap_create_failed_v2(typed);
			PG_RE_THROW();
		}
		PG_END_TRY();
		if (ok) return true;
		goto failed;
	}
	/* No current/side/allocator callback may precede the first-attempt latch. */
	if (entry->receipt != NULL) goto failed;
	entry->attempted = true;
	original = *input;
	PG_TRY();
	{
		do {
			ClusterRelmapRecoveryCreateReceiptV2 *receipt = NULL;
			Size saved;
			if (fence == NULL || (episode_id == 0 && typed->tx_fence != fence) || reader == NULL
				|| receipt_budget < sizeof(*receipt) || !cold_plan_reserve(plan, receipt_budget)) break;
			entry->receipt_bytes = receipt_budget;
			if (!relmap_create_sources_current(typed, fence, episode_id)
				|| !relmap_create_batch_current(typed, entry, reader)
				|| memcmp(&entry->input, &original, sizeof(original)) || plan->relmap_failed) break;
			if (!relmap_recovery_create_capture(typed, fence, episode_id, original.participant,
				&original.source, reader, &original.creation.identity, original.creation.batch_uuid,
				receipt_budget, &receipt)) break;
			/* Attach BEFORE any fallible postcheck: a confirmed creation and
			 * its original receipt survive a late refusal or ERROR together. */
			entry->receipt = receipt;
			entry->receipt_release = relmap_create_receipt_release;
			entry->receipt_output_distinct = relmap_create_receipt_storage_distinct;
			saved = sizeof(*receipt) + receipt->observation.source_count *
				(sizeof(*receipt->cuts) + sizeof(*receipt->sources));
			cold_plan_release(plan, receipt_budget - saved);
			entry->receipt_bytes = saved;
			if (memcmp(&receipt->observation.creation.record, &original.creation, sizeof(original.creation))
				/* Own confirmation changes the original control/table baseline.
				 * Retain its receipt before the fallible typed-owner acceptance,
				 * then recheck the original sources with that exact accepted link. */
				|| (episode_id != 0 && !cluster_cold_typed_relmap_create_accept_confirmation_v2(
					typed, fence, episode_id, &original.creation.identity,
					original.creation.batch_uuid, receipt->recovery))
				|| !relmap_create_sources_current(typed, fence, episode_id)
				|| !relmap_create_batch_current(typed, entry, reader)
				|| memcmp(&entry->input, &original, sizeof(original)) || plan->relmap_failed) break;
			entry->applied = true;
			ok = true;
		} while (0);
	}
	PG_CATCH();
	{
		if (entry->receipt == NULL && entry->receipt_bytes != 0) {
			cold_plan_release(plan, entry->receipt_bytes);
			entry->receipt_bytes = 0;
		}
		cluster_cold_plan_relmap_invalidate_v1(plan);
		cluster_cold_typed_relmap_create_failed_v2(typed);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (ok) return true;
	if (entry->receipt == NULL && entry->receipt_bytes != 0) {
		cold_plan_release(plan, entry->receipt_bytes);
		entry->receipt_bytes = 0;
	}
failed:
	cluster_cold_plan_relmap_invalidate_v1(plan);
	cluster_cold_typed_relmap_create_failed_v2(typed);
	return false;
}

static bool
relmap_create_ready(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 episode_id)
{
	ClusterColdPlanV1 *plan;
	volatile bool ok = false;
	if (typed == NULL || fence == NULL || (episode_id == 0 && typed->tx_fence != fence) || typed->refusal != CLUSTER_COLD_OK
		|| (plan = typed->plan) == NULL || plan->magic != CLUSTER_COLD_PLAN_MAGIC
		|| !typed->participant_count || typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->participant_count != plan->participant_count || !typed->replay_count
		|| typed->replay_count > typed->participant_count
		|| plan->phase != COLD_PHASE_SEALED || plan->relmap_failed) return false;
	/* An incomplete batch is not consumed, and never implies an empty one. */
	if (plan->relmap_create_count == 0)
		return episode_id == 0 || relmap_create_sources_current(typed, fence, episode_id);
	for (uint32 i = 0; i < plan->relmap_create_count; ++i) {
		const ColdRelmapCreateInput *entry = cold_relmap_create_at(plan, i);
		if (entry == NULL || !entry->captured || !entry->attempted || !entry->applied
			|| entry->receipt == NULL) return false;
	}
	PG_TRY();
	{
		ok = true;
		for (uint32 i = 0; i < plan->relmap_create_count && ok; ++i) {
			const ColdRelmapCreateInput *entry = cold_relmap_create_at(plan, i);
			ok = relmap_create_batch_current(typed, entry, NULL)
				&& relmap_create_receipt_recheck(entry->receipt, typed, fence, episode_id, true);
		}
		for (uint32 i = 0; i < plan->relmap_create_count && ok; ++i)
			ok = relmap_create_batch_current(typed, cold_relmap_create_at(plan, i), NULL);
		ok = ok && relmap_create_sources_current(typed, fence, episode_id) && !plan->relmap_failed;
	}
	PG_CATCH();
	{
		cluster_cold_plan_relmap_invalidate_v1(plan);
		cluster_cold_typed_relmap_create_failed_v2(typed);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!ok) {
		cluster_cold_plan_relmap_invalidate_v1(plan);
		cluster_cold_typed_relmap_create_failed_v2(typed);
	}
	return ok;
}

bool
cluster_cold_typed_relmap_create_apply_v2(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, const ClusterColdStepV1 *step,
	XLogReaderState *reader, Size budget)
{
	return relmap_create_apply(typed, fence, 0, step, reader, budget);
}

bool
cluster_cold_typed_relmap_create_apply_barrier_v2(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 episode_id, const ClusterColdStepV1 *step,
	XLogReaderState *reader, Size budget)
{
	return episode_id != 0 && relmap_create_apply(typed, fence, episode_id, step, reader, budget);
}

bool
cluster_cold_typed_relmap_create_ready_v2(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence)
{
	return relmap_create_ready(typed, fence, 0);
}

bool
cluster_cold_typed_relmap_create_ready_barrier_v2(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 episode_id)
{
	return episode_id != 0 && relmap_create_ready(typed, fence, episode_id);
}

bool
cluster_cold_typed_relmap_create_receipt_at_barrier_v2(ClusterColdTypedV1 *typed,
	uint64 episode_id, uint32 index, const ClusterRelmapRecoveryCreateReceiptV2 **receipt,
	const ClusterControlRelmapRecoveryCreateV2 **confirmation)
{
	ClusterColdPlanV1 *plan;
	const ColdRelmapCreateInput *entry;
	const ClusterRelmapRecoveryCreateReceiptV2 *r;
	if (typed == NULL || episode_id == 0 || receipt == NULL || confirmation == NULL
		|| (plan = typed->plan) == NULL || plan->magic != CLUSTER_COLD_PLAN_MAGIC
		|| plan->phase != COLD_PHASE_SEALED || plan->relmap_failed || typed->refusal != CLUSTER_COLD_OK
		|| !cold_relmap_output_distinct(typed, receipt, sizeof(*receipt))
		|| !cold_relmap_output_distinct(typed, confirmation, sizeof(*confirmation))
		|| !cluster_control_native_files_output_distinct_v2(receipt, sizeof(*receipt))
		|| !cluster_control_native_files_output_distinct_v2(confirmation, sizeof(*confirmation))
		|| relmap_control_overlap(receipt, sizeof(*receipt), confirmation, sizeof(*confirmation))
		|| (entry = cold_relmap_create_at(plan, index)) == NULL
		|| !entry->captured || !entry->attempted || !entry->applied || entry->receipt == NULL)
		return false;
	r = entry->receipt;
	if (!relmap_create_receipt_owned(r) || r->failed || r->checking || r->typed != typed
		|| r->barrier_episode_id != episode_id || r->barrier_fence == NULL || r->recovery == NULL
		|| relmap_create_receipt_alias(r, receipt, sizeof(*receipt))
		|| relmap_create_receipt_alias(r, confirmation, sizeof(*confirmation))
		|| !cluster_control_relmap_recovery_create_output_distinct_v2(r->recovery, receipt, sizeof(*receipt))
		|| !cluster_control_relmap_recovery_create_output_distinct_v2(r->recovery, confirmation, sizeof(*confirmation)))
		return false;
	*receipt = r;
	*confirmation = r->recovery;
	return true;
}
#endif /* USE_PGRAC_CLUSTER */
