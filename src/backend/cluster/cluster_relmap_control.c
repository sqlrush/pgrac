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
#include "cluster/cluster_relmap_authority.h"
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
#endif /* USE_PGRAC_CLUSTER */
