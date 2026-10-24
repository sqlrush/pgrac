/* PGRAC: relmap pending census through the original cold ROOT reader.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_retention_census.h"
#include "common/cryptohash.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_relmap_arb.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_wal_inputs.h"

typedef struct CensusOrigin {
	ClusterRelmapRetentionDirectory *directory;
	uint64 generation;
	uint64 native_floor;
	uint64 last_issued;
} CensusOrigin;

struct ClusterRelmapRetentionCensus {
	ClusterRecoveryRootSet *roots;
	uint64 system_identifier;
	uint64 database_incarnation;
	uint8 storage_uuid[16];
	uint8 digest[32];
	CensusOrigin origins[CLUSTER_RELMAP_RETENTION_MAX_ORIGINS];
	uint16 nodes[CLUSTER_RELMAP_RETENTION_MAX_ORIGINS];
	size_t origin_count;
	size_t check_origin;
	uint32 count;
	uint32 next_entry;
	ClusterRelmapCensusResult result;
	bool global_seen;
	bool base_seen;
	bool rechecking;
	bool checked;
	char namespace_path[MAXPGPATH];
	ClusterRelmapV2Namespace identity;
	uint8 copies[2][CLUSTER_RELMAP_V2_FILE_SIZE];
	unsigned present;
	bool reading;
	unsigned copy;
	size_t offset;
	/* Valid only between an emitted namespace and the next cursor advance. */
	ClusterRelmapV2View rebuild_view;
	bool rebuild_ready;
};

typedef struct CensusRecoveryItem {
	ClusterRelmapCensusRecovery input;
	ClusterRelmapRetentionPin pin;
} CensusRecoveryItem;

struct ClusterRelmapCensusRecoveryBatch {
	ClusterRelmapRetentionCensus *census;
	ClusterWalInputsV1 *inputs;
	size_t count;
	CensusRecoveryItem items[FLEXIBLE_ARRAY_MEMBER];
};

static ClusterRelmapCensusResult
census_input_failed(ClusterRecoveryRootSet *roots,
	ClusterRelmapRetentionDirectory *const directories[], size_t count,
	ClusterRelmapCensusResult result)
{
	if (directories != NULL && count <= CLUSTER_RELMAP_RETENTION_MAX_ORIGINS)
		for (size_t i = 0; i < count; i++)
			cluster_relmap_retention_invalidate(directories[i]);
	cluster_recovery_root_set_invalidate(roots);
	return result;
}

static ClusterRelmapCensusResult
census_failed(ClusterRelmapRetentionCensus *census, ClusterRelmapCensusResult result)
{
	census->result = result;
	census->rebuild_ready = false;
	memset(&census->rebuild_view, 0, sizeof(census->rebuild_view));
	for (size_t i = 0; i < census->origin_count; i++)
		cluster_relmap_retention_invalidate(census->origins[census->nodes[i]].directory);
	cluster_recovery_root_set_invalidate(census->roots);
	return result;
}

static ClusterRelmapCensusResult
census_root_failed(ClusterRelmapRetentionCensus *census, ClusterRecoveryRootsResult result)
{
	return census_failed(census, result == CLUSTER_RECOVERY_ROOTS_CAPACITY
		? CLUSTER_RELMAP_CENSUS_CAPACITY : result == CLUSTER_RECOVERY_ROOTS_IO
		? CLUSTER_RELMAP_CENSUS_UNAVAILABLE : CLUSTER_RELMAP_CENSUS_CHANGED);
}

static bool
census_origin_same_in_state(const ClusterRelmapRetentionCensus *census, uint16 node,
							ClusterRelmapRetentionState state)
{
	const CensusOrigin *saved = &census->origins[node];
	const ClusterRelmapRetentionDirectory *directory = saved->directory;

	return directory != NULL && directory->state == state
		&& directory->system_identifier == census->system_identifier
		&& directory->database_incarnation == census->database_incarnation
		&& directory->origin == node && directory->generation == saved->generation
		&& directory->native_floor == saved->native_floor
		&& directory->last_issued == saved->last_issued && directory->truncate_serial == 0
		&& directory->inflight.generation == 0 && directory->inflight.serial == 0
		&& directory->inflight.cutoff == 0;
}

static bool
census_origin_same(const ClusterRelmapRetentionCensus *census, uint16 node)
{
	return census_origin_same_in_state(census, node, CLUSTER_RELMAP_RETENTION_BUILDING);
}

static bool
census_root_same(const ClusterRelmapRetentionCensus *census)
{
	uint8 digest[32];

	return cluster_recovery_root_set_storage_matches(census->roots,
		census->system_identifier, census->database_incarnation, census->storage_uuid)
		&& cluster_recovery_root_set_count(census->roots) == census->count
		&& cluster_recovery_root_set_digest(census->roots, digest)
		&& memcmp(census->digest, digest, sizeof(digest)) == 0;
}

static bool
census_recovery_current(const ClusterRelmapRetentionCensus *census)
{
	if (!census_root_same(census))
		return false;
	for (size_t i = 0; i < census->origin_count; i++)
		if (!census_origin_same(census, census->nodes[i]))
			return false;
	return true;
}

static bool
census_recovery_sources(const ClusterRelmapRetentionCensus *census, ClusterWalInputsV1 *inputs)
{
	uint32 count;

	if (inputs == NULL || cluster_wal_inputs_revalidate_v1(inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	count = cluster_wal_inputs_count_v1(inputs);
	if (count == 0 || count > CLUSTER_WAL_INPUTS_MAX)
		return false;
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *input = cluster_wal_inputs_at_v1(inputs, i);

		if (input == NULL
			|| input->source.claim.identity.system_identifier != census->system_identifier
			|| input->source.claim.database_incarnation != census->database_incarnation
			|| memcmp(input->source.claim.identity.storage_uuid, census->storage_uuid, 16) != 0)
			return false;
	}
	return count == cluster_wal_inputs_count_v1(inputs);
}

static const ClusterRelmapRetentionPin *
census_recovery_pin(const ClusterRelmapRetentionCensus *census, const ClusterRelmapV2Namespace *ns,
					const ClusterRelmapV2View *view)
{
	const ClusterRelmapV2Owner *owner = &view->pending_owner;
	const ClusterRelmapRetentionDirectory *directory;
	const ClusterRelmapRetentionPin *found = NULL;

	if (owner->node >= lengthof(census->origins)
		|| (directory = census->origins[owner->node].directory) == NULL)
		return NULL;
	for (size_t i = 0; i < lengthof(directory->pins); i++) {
		const ClusterRelmapRetentionPin *pin = &directory->pins[i];
		const ClusterRelmapRetentionKey *key = &pin->key;

		if (!pin->occupied || key->identity.dbid != ns->dbid || key->identity.shared != ns->shared
			|| key->generation != view->pending_generation)
			continue;
		if (found != NULL || !pin->may_be_staged || pin->wal_end != owner->wal_end
			|| key->identity.system_identifier != ns->system_identifier
			|| key->identity.database_incarnation != ns->database_incarnation
			|| key->origin != owner->node || key->top_full_xid != owner->top_full_xid
			|| key->membership_epoch != owner->membership_epoch
			|| key->admitted_incarnation != owner->admitted_incarnation
			|| key->boot_incarnation != owner->boot_incarnation)
			return NULL;
		found = pin;
	}
	return found;
}

/* Read the saved physical object, then compare its complete SHA rather than
 * treating a valid CRC or restored metadata as the original observed bytes. */
static bool
census_recovery_read(ClusterRelmapRetentionCensus *census, uint32 index, uint8 *bytes)
{
	const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(census->roots, index);
	pg_cryptohash_ctx *ctx;
	uint8 digest[32];
	bool ok;

	if (entry == NULL
		|| cluster_recovery_root_set_read(census->roots, index, 0, bytes,
										  CLUSTER_RELMAP_V2_FILE_SIZE)
			   != CLUSTER_RECOVERY_ROOTS_BYTES)
		return false;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) == 0
		 && pg_cryptohash_update(ctx, bytes, CLUSTER_RELMAP_V2_FILE_SIZE) == 0
		 && pg_cryptohash_final(ctx, digest, sizeof(digest)) == 0
		 && memcmp(digest, entry->content_sha256, sizeof(digest)) == 0;
	pg_cryptohash_free(ctx);
	return ok;
}


/* Same exact physical pair reader for one or all prepared namespaces. */
static bool
census_recovery_pair(ClusterRelmapRetentionCensus *census, Oid dbid,
					 ClusterRelmapCensusRecovery *out)
{
	uint32 indices[2] = { UINT32_MAX, UINT32_MAX };
	uint8 copies[2][CLUSTER_RELMAP_V2_FILE_SIZE];
	char path[MAXPGPATH], backup[MAXPGPATH];
	bool shape = true;

	if (dbid == InvalidOid)
		strlcpy(path, "global/pgrac_relmap_authority", sizeof(path));
	else if (snprintf(path, sizeof(path), "base/%u/pgrac_relmap_authority", dbid) >= sizeof(path))
		return false;
	if (snprintf(backup, sizeof(backup), "%s.bak", path) >= sizeof(backup))
		return false;
	out->identity
		= (ClusterRelmapV2Namespace){ census->system_identifier, census->database_incarnation, dbid,
									  dbid == InvalidOid };
	for (uint32 i = 0; i < census->count; i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(census->roots, i);
		unsigned copy;

		if (entry == NULL) {
			shape = false;
			break;
		}
		if (strcmp(entry->file.path, path) == 0)
			copy = 0;
		else if (strcmp(entry->file.path, backup) == 0)
			copy = 1;
		else
			continue;
		if (indices[copy] != UINT32_MAX
			|| entry->file.kind
				   != (copy == 0 ? CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY
								 : CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP)
			|| (entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0
			|| !S_ISREG(entry->file.observed.st_mode)
			|| entry->file.size != CLUSTER_RELMAP_V2_FILE_SIZE || entry->file.locator.dbOid != dbid
			|| entry->file.locator.spcOid
				   != (dbid == InvalidOid ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID)) {
			shape = false;
			break;
		}
		indices[copy] = i;
	}
	return shape && indices[0] != UINT32_MAX && indices[1] != UINT32_MAX
		   && census_recovery_read(census, indices[0], copies[0])
		   && census_recovery_read(census, indices[1], copies[1])
		   && cluster_relmap_v2_read_pair(copies[0], sizeof(copies[0]), copies[1],
										  sizeof(copies[1]), &out->identity, &out->pending)
				  == CLUSTER_RELMAP_V2_PAIR_VALID
		   && out->pending.pending_complete;
}

bool
cluster_relmap_retention_census_recovery_prepare(ClusterRelmapRetentionCensus *census,
												 struct ClusterWalInputsV1 *inputs, Oid dbid,
												 ClusterRelmapCensusRecovery *out)
{
	ClusterRelmapRecoveryHandle *volatile handle = NULL;
	volatile bool accepted = false;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (census == NULL || census->result != CLUSTER_RELMAP_CENSUS_DRAINED)
		return false; /* An unfinished scan stays provisional, not failed. */
	PG_TRY();
	{
		do {
			ClusterRelmapCensusRecovery scratch = { 0 };
			const ClusterRelmapRetentionPin *pin;
			ClusterRelmapRetentionPin saved_pin;

			if (!census->checked || !census_recovery_current(census)
				|| !census_recovery_sources(census, inputs))
				break;
			if (!census_recovery_pair(census, dbid, &scratch) || !scratch.pending.has_pending
				|| (pin = census_recovery_pin(census, &scratch.identity, &scratch.pending)) == NULL)
				break;
			saved_pin = *pin;
			if (!census_recovery_current(census)
				|| !cluster_relmap_recovery_prepare_v1(inputs, &scratch.identity, &scratch.pending,
													   (ClusterRelmapRecoveryHandle **)&handle)
				|| !census_recovery_current(census) || !census_recovery_sources(census, inputs)
				|| census_recovery_pin(census, &scratch.identity, &scratch.pending) != pin
				|| memcmp(pin, &saved_pin, sizeof(saved_pin)) != 0)
				break;
			scratch.handle = handle;
			*out = scratch;
			handle = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (handle != NULL)
			cluster_relmap_recovery_handle_release_v1((ClusterRelmapRecoveryHandle **)&handle);
		if (!accepted)
			census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
	}
	PG_END_TRY();
	return accepted;
}

static bool
census_recovery_overlap(const void *a, size_t an, const void *b, size_t bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && ((x <= y && y - x < an) || (y <= x && x - y < bn));
}

static bool
census_recovery_output_overlap(const ClusterRelmapRetentionCensus *census,
							   const ClusterRelmapCensusRecovery *out)
{
	if (census == NULL)
		return false;
	if (census_recovery_overlap(out, sizeof(*out), census, sizeof(*census))
		|| census_recovery_overlap(out, sizeof(*out), census->roots, 1))
		return true;
	for (size_t i = 0; i < census->origin_count; i++)
		if (census_recovery_overlap(out, sizeof(*out), census->origins[census->nodes[i]].directory,
									sizeof(ClusterRelmapRetentionDirectory)))
			return true;
	return false;
}

/* Compare all original pin bytes in this exclusive in-process observation,
 * including unselected origins/slots. This is not a persistent encoding. */
static bool
census_recovery_pins_digest(const ClusterRelmapRetentionCensus *census, uint8 digest[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	bool ok;

	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) == 0;
	for (size_t i = 0; ok && i < census->origin_count; i++) {
		const ClusterRelmapRetentionDirectory *directory
			= census->origins[census->nodes[i]].directory;
		ok = directory != NULL
			 && pg_cryptohash_update(ctx, (const uint8 *)directory->pins, sizeof(directory->pins))
					== 0;
	}
	ok = ok && pg_cryptohash_final(ctx, digest, 32) == 0;
	pg_cryptohash_free(ctx);
	return ok;
}

static bool
census_cold_source_current(const ClusterRelmapRetentionCensus *census,
						   const ClusterRelmapV2View *pending,
						   const ClusterRelmapRecoverySourceObservationV1 *source)
{
	return source->scope_serial != 0
		   && source->source.claim.identity.system_identifier == census->system_identifier
		   && source->source.claim.database_incarnation == census->database_incarnation
		   && source->source.claim.identity.origin_node_id == pending->pending_owner.node
		   && memcmp(source->source.claim.identity.storage_uuid, census->storage_uuid, 16) == 0
		   && cluster_undo_recovery_cold_single_source_v1(
			   &source->source, &source->checkpoint, source->native_redo, source->scope_serial);
}

bool
cluster_relmap_retention_census_recovery_prepare_cold(ClusterRelmapRetentionCensus *census,
													  Oid dbid, ClusterRelmapCensusRecovery *out)
{
	ClusterRelmapRecoveryHandle *volatile handle = NULL;
	volatile bool accepted = false;

	if (out == NULL || census_recovery_output_overlap(census, out))
		return false;
	memset(out, 0, sizeof(*out));
	if (census == NULL || census->result != CLUSTER_RELMAP_CENSUS_DRAINED || !cluster_shared_catalog
		|| !cluster_recovery_merge_claim_is_held() || !cluster_relmap_authority_x_held())
		return false;
	PG_TRY();
	{
		do {
			ClusterRelmapCensusRecovery scratch = { 0 };
			ClusterRelmapRecoverySourceObservationV1 source, current;
			const ClusterRelmapRetentionPin *pin;
			uint8 saved_pins[32], current_pins[32];
			ClusterRecoveryRootsResult root_result;

			if (!census->checked || !census_recovery_current(census)
				|| !census_recovery_pins_digest(census, saved_pins)
				|| !census_recovery_pair(census, dbid, &scratch) || !scratch.pending.has_pending
				|| (pin = census_recovery_pin(census, &scratch.identity, &scratch.pending)) == NULL
				|| !census_recovery_current(census)
				|| !cluster_relmap_recovery_prepare_cold_v1(&scratch.identity, &scratch.pending,
															(ClusterRelmapRecoveryHandle **)&handle)
				|| !cluster_relmap_recovery_handle_source_v1(handle, &source)
				|| !census_cold_source_current(census, &scratch.pending, &source))
				break;
			/* A saved OBSERVED digest cannot conceal physical changes during
			 * native preparation, including unrelated/new namespaces. */
			root_result = cluster_recovery_root_set_recheck_begin(census->roots);
			while (root_result == CLUSTER_RECOVERY_ROOTS_MORE
				   || root_result == CLUSTER_RECOVERY_ROOTS_ENTRY
				   || root_result == CLUSTER_RECOVERY_ROOTS_BYTES) {
				ClusterRecoveryRootsEvent event;
				root_result = cluster_recovery_root_set_step(census->roots, &event);
			}
			if (root_result != CLUSTER_RECOVERY_ROOTS_OBSERVED
				|| !cluster_relmap_recovery_handle_source_v1(handle, &current)
				|| memcmp(&source.source, &current.source, sizeof(source.source)) != 0
				|| memcmp(&source.checkpoint, &current.checkpoint, sizeof(source.checkpoint)) != 0
				|| source.native_redo != current.native_redo
				|| source.scope_serial != current.scope_serial
				|| !census_cold_source_current(census, &scratch.pending, &current)
				|| !census_recovery_current(census)
				|| !census_recovery_pins_digest(census, current_pins)
				|| memcmp(saved_pins, current_pins, sizeof(saved_pins)) != 0
				|| census_recovery_pin(census, &scratch.identity, &scratch.pending) != pin
				|| !cluster_shared_catalog || !cluster_recovery_merge_claim_is_held()
				|| !cluster_relmap_authority_x_held())
				break;
			scratch.handle = handle;
			*out = scratch;
			handle = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (!accepted)
			census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		if (handle != NULL)
			cluster_relmap_recovery_handle_release_v1((ClusterRelmapRecoveryHandle **)&handle);
	}
	PG_END_TRY();
	return accepted;
}

/* After this call revokes physical observations, UNKNOWN is a required state,
 * never a qualification. Original native authority and every pin stay held. */
static bool
census_cold_consumer_current(const ClusterRelmapRetentionCensus *census,
							 const ClusterRelmapCensusRecovery *input,
							 const ClusterRelmapRecoverySourceObservationV1 *source,
							 const uint8 saved_pins[32], ClusterRelmapRetentionState state)
{
	uint8 pins[32];

	/* Complete original source/owner I/O before the final local pin check. */
	if (!census_cold_source_current(census, &input->pending, source))
		return false;
	for (size_t i = 0; i < census->origin_count; i++)
		if (!census_origin_same_in_state(census, census->nodes[i], state))
			return false;
	return census_recovery_pins_digest(census, pins) && memcmp(pins, saved_pins, sizeof(pins)) == 0
		   && cluster_shared_catalog && cluster_recovery_merge_claim_is_held()
		   && cluster_relmap_authority_x_held();
}

ClusterRelmapV2ClearResult
cluster_relmap_retention_census_recovery_arbitrate_cold(ClusterRelmapRetentionCensus *census,
														Oid dbid)
{
	ClusterRelmapRecoveryHandle *volatile handle = NULL;
	ClusterRelmapV2ClearResult result = CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;

	if (census == NULL || census->result != CLUSTER_RELMAP_CENSUS_DRAINED || !cluster_shared_catalog
		|| !cluster_recovery_merge_claim_is_held() || !cluster_relmap_authority_x_held())
		return result;
	PG_TRY();
	{
		do {
			ClusterRelmapCensusRecovery input = { 0 };
			ClusterRelmapRecoverySourceObservationV1 source;
			uint8 saved_pins[32];
			ClusterRelmapV2ClearResult native_result;

			if (!census->checked || !census_recovery_current(census)
				|| !census_recovery_pins_digest(census, saved_pins)
				|| !cluster_relmap_retention_census_recovery_prepare_cold(census, dbid, &input))
				break;
			handle = input.handle;
			input.handle = NULL;
			/* Preparation just freshly checked the full physical ROOT after
			 * native base I/O, within this same original isolated call. */
			if (!cluster_relmap_recovery_handle_source_v1(handle, &source)
				|| !census_recovery_current(census)
				|| !census_cold_consumer_current(census, &input, &source, saved_pins,
												 CLUSTER_RELMAP_RETENTION_BUILDING))
				break;
			/* Native control-ROOT authority is independent of these physical
			 * observations. Revoke them BEFORE the first arbiter attempt. */
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
			if (!census_cold_consumer_current(census, &input, &source, saved_pins,
											  CLUSTER_RELMAP_RETENTION_UNKNOWN))
				break;
			native_result = cluster_relmap_arbitrate_pending_v2(
				(ClusterRelmapRecoveryHandle **)&handle, &input.identity, &input.pending);
			if (!census_cold_consumer_current(census, &input, &source, saved_pins,
											  CLUSTER_RELMAP_RETENTION_UNKNOWN))
				break;
			if (native_result == CLUSTER_RELMAP_V2_CLEARED_POSTREAD)
				result = CLUSTER_RELMAP_V2_CLEARED_POSTREAD;
		} while (false);
	}
	PG_FINALLY();
	{
		/* UNKNOWN/ERROR may follow durable target writes; never restore an
		 * old observation or discharge an original pin from a pair result. */
		census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		if (handle != NULL)
			cluster_relmap_recovery_handle_release_v1((ClusterRelmapRecoveryHandle **)&handle);
	}
	PG_END_TRY();
	return result;
}

void
cluster_relmap_retention_census_recovery_batch_release(ClusterRelmapCensusRecoveryBatch **out)
{
	ClusterRelmapCensusRecoveryBatch *batch;

	if (out == NULL || *out == NULL)
		return;
	batch = *out;
	*out = NULL;
	for (size_t i = 0; i < batch->count; i++)
		if (batch->items[i].input.handle != NULL)
			cluster_relmap_recovery_handle_release_v1(&batch->items[i].input.handle);
	free(batch);
}

bool
cluster_relmap_retention_census_recovery_prepare_all(ClusterRelmapRetentionCensus *census,
													 ClusterWalInputsV1 *inputs,
													 size_t memory_budget,
													 ClusterRelmapCensusRecoveryBatch **out)
{
	ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
	volatile bool accepted = false;

	if (out == NULL)
		return false;
	*out = NULL;
	if (census == NULL || census->result != CLUSTER_RELMAP_CENSUS_DRAINED || !cluster_shared_catalog
		|| !cluster_recovery_merge_claim_is_held() || !cluster_relmap_authority_x_held())
		return false;
	PG_TRY();
	{
		do {
			size_t capacity = 0, bytes;
			bool complete = true;
			ClusterRecoveryRootsResult root_result;

			if (!census->checked || !census_recovery_current(census)
				|| !census_recovery_sources(census, inputs))
				break;
			for (uint32 i = 0; i < census->count; i++) {
				const ClusterRecoveryRootEntry *entry
					= cluster_recovery_root_set_entry(census->roots, i);
				if (entry == NULL) {
					complete = false;
					break;
				}
				if (entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY)
					capacity++;
			}
			if (!complete || capacity == 0
				|| capacity > (SIZE_MAX - sizeof(*batch)) / sizeof(batch->items[0]))
				break;
			bytes = sizeof(*batch) + capacity * sizeof(batch->items[0]);
			if (bytes > memory_budget || (batch = calloc(1, bytes)) == NULL)
				break;
			batch->census = census;
			batch->inputs = inputs;
			for (uint32 i = 0; i < census->count; i++) {
				const ClusterRecoveryRootEntry *entry
					= cluster_recovery_root_set_entry(census->roots, i);
				ClusterRelmapCensusRecovery pair = { 0 };
				const ClusterRelmapRetentionPin *pin;
				CensusRecoveryItem *item;

				if (entry == NULL) {
					complete = false;
					break;
				}
				if (entry->file.kind != CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY)
					continue;
				if (!census_recovery_pair(census, entry->file.locator.dbOid, &pair)) {
					complete = false;
					break;
				}
				if (!pair.pending.has_pending)
					continue;
				if (batch->count >= capacity) {
					complete = false;
					break;
				}
				item = &batch->items[batch->count];
				if (!cluster_relmap_retention_census_recovery_prepare(
						census, inputs, pair.identity.dbid, &item->input)) {
					complete = false;
					break;
				}
				/* Count the owned handle before any later check can fail. */
				batch->count++;
				pin = census_recovery_pin(census, &item->input.identity, &item->input.pending);
				if (pin == NULL) {
					complete = false;
					break;
				}
				item->pin = *pin;
			}
			if (!complete || !census_recovery_current(census)
				|| !census_recovery_sources(census, inputs))
				break;
			/* Later native I/O cannot conceal new physical namespaces or
			 * unrelated changes behind the saved OBSERVED digest. */
			root_result = cluster_recovery_root_set_recheck_begin(census->roots);
			while (root_result == CLUSTER_RECOVERY_ROOTS_MORE
				   || root_result == CLUSTER_RECOVERY_ROOTS_ENTRY
				   || root_result == CLUSTER_RECOVERY_ROOTS_BYTES) {
				ClusterRecoveryRootsEvent event;
				root_result = cluster_recovery_root_set_step(census->roots, &event);
			}
			if (root_result != CLUSTER_RECOVERY_ROOTS_OBSERVED || !census_recovery_current(census)
				|| !census_recovery_sources(census, inputs) || !cluster_shared_catalog
				|| !cluster_recovery_merge_claim_is_held() || !cluster_relmap_authority_x_held())
				break;
			for (size_t i = 0; i < batch->count; i++) {
				const CensusRecoveryItem *item = &batch->items[i];
				const ClusterRelmapRetentionPin *pin
					= census_recovery_pin(census, &item->input.identity, &item->input.pending);
				if (pin == NULL || memcmp(pin, &item->pin, sizeof(*pin)) != 0) {
					complete = false;
					break;
				}
			}
			if (!complete)
				break;
			*out = batch;
			batch = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (!accepted)
			census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		cluster_relmap_retention_census_recovery_batch_release(
			(ClusterRelmapCensusRecoveryBatch **)&batch);
	}
	PG_END_TRY();
	return accepted;
}

/* Does not adopt a physical ROOT observation or qualify an UNKNOWN directory.
 * Native arbitration independently checks its original cold authority. */
static bool
census_recovery_batch_current(const ClusterRelmapCensusRecoveryBatch *batch,
							  ClusterRelmapRetentionState state)
{
	const ClusterRelmapRetentionCensus *census = batch->census;

	if (!cluster_shared_catalog || !cluster_recovery_merge_claim_is_held()
		|| !cluster_relmap_authority_x_held() || !census_recovery_sources(census, batch->inputs))
		return false;
	for (size_t i = 0; i < census->origin_count; i++)
		if (!census_origin_same_in_state(census, census->nodes[i], state))
			return false;
	for (size_t i = 0; i < batch->count; i++) {
		const CensusRecoveryItem *item = &batch->items[i];
		const ClusterRelmapRetentionPin *pin
			= census_recovery_pin(census, &item->input.identity, &item->input.pending);

		if (pin == NULL || memcmp(pin, &item->pin, sizeof(*pin)) != 0)
			return false;
	}
	return cluster_shared_catalog && cluster_recovery_merge_claim_is_held()
		   && cluster_relmap_authority_x_held();
}

ClusterRelmapV2ClearResult
cluster_relmap_retention_census_recovery_arbitrate_all(ClusterRelmapCensusRecoveryBatch **custody)
{
	ClusterRelmapCensusRecoveryBatch *batch;
	ClusterRelmapRetentionCensus *census;
	volatile bool invalidate = false;
	ClusterRelmapV2ClearResult result = CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;

	if (custody == NULL || *custody == NULL)
		return result;
	batch = *custody;
	*custody = NULL;
	census = batch->census;
	PG_TRY();
	{
		do {
			ClusterRecoveryRootsResult root_result;
			size_t consumed;

			if (!cluster_shared_catalog || !cluster_recovery_merge_claim_is_held()
				|| !cluster_relmap_authority_x_held())
				break;
			invalidate = true;
			if (census == NULL || census->result != CLUSTER_RELMAP_CENSUS_DRAINED
				|| !census->checked || !census_recovery_current(census)
				|| !census_recovery_batch_current(batch, CLUSTER_RELMAP_RETENTION_BUILDING))
				break;
			/* Preparation and consumption are separate calls. Do not trust a
			 * cached OBSERVED digest across their original-owner interval. */
			root_result = cluster_recovery_root_set_recheck_begin(census->roots);
			while (root_result == CLUSTER_RECOVERY_ROOTS_MORE
				   || root_result == CLUSTER_RECOVERY_ROOTS_ENTRY
				   || root_result == CLUSTER_RECOVERY_ROOTS_BYTES) {
				ClusterRecoveryRootsEvent event;
				root_result = cluster_recovery_root_set_step(census->roots, &event);
			}
			if (root_result != CLUSTER_RECOVERY_ROOTS_OBSERVED || !census_recovery_current(census)
				|| !census_recovery_batch_current(batch, CLUSTER_RELMAP_RETENTION_BUILDING))
				break;
			if (batch->count == 0) {
				invalidate = false;
				result = CLUSTER_RELMAP_V2_CLEARED_POSTREAD;
				break;
			}
			/* Only physical observations are revoked here. The original
			 * native control-ROOT/source/round authority remains independent. */
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
			for (consumed = 0; consumed < batch->count; consumed++) {
				ClusterRelmapCensusRecovery *item = &batch->items[consumed].input;

				/* UNKNOWN is required only because this operation just set it;
				 * unchanged metadata/pins are not directory qualification. */
				if (!census_recovery_batch_current(batch, CLUSTER_RELMAP_RETENTION_UNKNOWN)
					|| cluster_relmap_arbitrate_pending_v2(&item->handle, &item->identity,
															 &item->pending)
						   != CLUSTER_RELMAP_V2_CLEARED_POSTREAD
					|| !census_recovery_batch_current(batch, CLUSTER_RELMAP_RETENTION_UNKNOWN))
					break;
			}
			if (consumed == batch->count)
				result = CLUSTER_RELMAP_V2_CLEARED_POSTREAD;
		} while (false);
	}
	PG_FINALLY();
	{
		if (invalidate && census != NULL)
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		cluster_relmap_retention_census_recovery_batch_release(&batch);
	}
	PG_END_TRY();
	return result;
}

ClusterRelmapV2ClearResult
cluster_relmap_retention_census_recovery_arbitrate(ClusterRelmapRetentionCensus *census,
												   ClusterWalInputsV1 *inputs, Oid dbid)
{
	ClusterRelmapCensusRecovery input = { 0 };
	ClusterRelmapRecoveryHandle *volatile handle = NULL;
	volatile bool attempted = false;
	ClusterRelmapV2ClearResult result = CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;

	/* Missing original ownership is not permission to acquire it here, nor
	 * to invalidate an unfinished observation that no target has consumed. */
	if (census == NULL || census->result != CLUSTER_RELMAP_CENSUS_DRAINED || !cluster_shared_catalog
		|| !cluster_recovery_merge_claim_is_held() || !cluster_relmap_authority_x_held())
		return result;
	PG_TRY();
	{
		if (cluster_relmap_retention_census_recovery_prepare(census, inputs, dbid, &input)) {
			handle = input.handle;
			input.handle = NULL;
			attempted = true;
			/* The original consumer rechecks claim/X after native I/O and
			 * makes its own terminal decision. Never route this via v1's
			 * raw local XID or a caller-supplied completion boolean. */
			result = cluster_relmap_arbitrate_pending_v2((ClusterRelmapRecoveryHandle **)&handle,
														 &input.identity, &input.pending);
		}
	}
	PG_FINALLY();
	{
		/* Even UNKNOWN/ERROR may follow durable target writes. Revoke the
		 * old observation before releasing only local native custody; all
		 * original pins and their unresolved obligations survive. */
		if (attempted)
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		if (handle != NULL)
			cluster_relmap_recovery_handle_release_v1((ClusterRelmapRecoveryHandle **)&handle);
	}
	PG_END_TRY();
	return result;
}

ClusterRelmapCensusResult
cluster_relmap_retention_census_begin(
	ClusterRecoveryRootSet *roots, uint64 system_identifier, uint64 database_incarnation,
	const uint8 storage_uuid[16], ClusterRelmapRetentionDirectory *const directories[],
	size_t count, size_t memory_budget, ClusterRelmapRetentionCensus **out)
{
	ClusterRelmapRetentionCensus *census;
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_INVALID;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || system_identifier == 0 || database_incarnation == 0
		|| storage_uuid == NULL || directories == NULL || count == 0
		|| count > CLUSTER_RELMAP_RETENTION_MAX_ORIGINS)
		goto fail;
	if (memory_budget < sizeof(*census)) {
		result = CLUSTER_RELMAP_CENSUS_CAPACITY;
		goto fail;
	}
	if (!cluster_recovery_root_set_storage_matches(roots, system_identifier,
		database_incarnation, storage_uuid))
		goto fail;
	census = calloc(1, sizeof(*census));
	if (census == NULL) {
		result = CLUSTER_RELMAP_CENSUS_CAPACITY;
		goto fail;
	}
	census->roots = roots;
	census->system_identifier = system_identifier;
	census->database_incarnation = database_incarnation;
	memcpy(census->storage_uuid, storage_uuid, sizeof(census->storage_uuid));
	census->count = cluster_recovery_root_set_count(roots);
	if (!cluster_recovery_root_set_digest(roots, census->digest)) {
		free(census);
		goto fail;
	}
	for (size_t i = 0; i < count; i++) {
		ClusterRelmapRetentionDirectory *directory = directories[i];
		CensusOrigin *saved;

		if (directory == NULL || directory->origin >= lengthof(census->origins)
			|| census->origins[directory->origin].directory != NULL
			|| directory->generation == 0) {
			free(census);
			goto fail;
		}
		saved = &census->origins[directory->origin];
		saved->directory = directory;
		saved->generation = directory->generation;
		saved->native_floor = directory->native_floor;
		saved->last_issued = directory->last_issued;
		if (!census_origin_same(census, directory->origin)) {
			free(census);
			goto fail;
		}
		census->nodes[census->origin_count++] = directory->origin;
	}
	*out = census;
	return CLUSTER_RELMAP_CENSUS_MORE;
fail:
	return census_input_failed(roots, directories, count, result);
}

/* Reuse the original canonical classifier for global and direct base/db
 * directories. This does not declare other D1 file obligations explained. */
static bool
census_namespace(const char *path, Oid *dbid)
{
	char authority[MAXPGPATH];
	RelFileLocator locator;
	ForkNumber forknum;

	if (strcmp(path, "global") != 0
		&& (strncmp(path, "base/", 5) != 0 || strchr(path + 5, '/') != NULL))
		return false;
	if (snprintf(authority, sizeof(authority), "%s/pgrac_relmap_authority", path)
		>= sizeof(authority)
		|| cluster_recovery_namespace_classify(authority, &locator, &forknum)
			!= CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY)
		return false;
	*dbid = locator.dbOid;
	return true;
}

static ClusterRelmapCensusResult
census_namespace_finish(ClusterRelmapRetentionCensus *census, ClusterRelmapCensusCommitted *out)
{
	ClusterRelmapV2View view;
	ClusterRelmapRetentionKey key = { 0 };
	ClusterRelmapRetentionResult result;
	uint16 node;

	if (census->present != 3
		|| cluster_relmap_v2_read_pair(census->copies[0], sizeof(census->copies[0]),
			census->copies[1], sizeof(census->copies[1]), &census->identity, &view)
			!= CLUSTER_RELMAP_V2_PAIR_VALID || !view.pending_complete)
		return census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
	if (view.has_pending) {
		node = view.pending_owner.node;
		if (node >= lengthof(census->origins) || census->origins[node].directory == NULL)
			return census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		if (!census_origin_same(census, node))
			return census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		key.identity = census->identity;
		key.generation = view.pending_generation;
		key.origin = node;
		key.top_full_xid = view.pending_owner.top_full_xid;
		key.membership_epoch = view.pending_owner.membership_epoch;
		key.admitted_incarnation = view.pending_owner.admitted_incarnation;
		key.boot_incarnation = view.pending_owner.boot_incarnation;
		result = cluster_relmap_retention_build_add(census->origins[node].directory,
			&key, view.pending_owner.wal_end);
		if (result != CLUSTER_RELMAP_RETENTION_OK)
			return census_failed(census, result == CLUSTER_RELMAP_RETENTION_CAPACITY_EXCEEDED
				? CLUSTER_RELMAP_CENSUS_CAPACITY : CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
	}
	if (out != NULL) {
		census->rebuild_view = view;
		census->rebuild_ready = true;
		out->identity = census->identity;
		out->generation = view.committed_generation;
		out->map = view.committed;
		out->present = true;
	}
	census->namespace_path[0] = '\0';
	census->present = 0;
	memset(census->copies, 0, sizeof(census->copies));
	return CLUSTER_RELMAP_CENSUS_MORE;
}

ClusterRelmapCensusResult
cluster_relmap_retention_census_next_committed(ClusterRelmapRetentionCensus *census,
	ClusterRelmapCensusCommitted *out)
{
	const ClusterRecoveryRootEntry *entry;
	ClusterRecoveryRootsResult result;
	Oid dbid;
	bool directory;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (census == NULL)
		return CLUSTER_RELMAP_CENSUS_INVALID;
	census->rebuild_ready = false;
	memset(&census->rebuild_view, 0, sizeof(census->rebuild_view));
	if (census->result > CLUSTER_RELMAP_CENSUS_DRAINED)
		return census->result;
	if (census->checked || census->result == CLUSTER_RELMAP_CENSUS_DRAINED) {
		/* Separate from the last recheck I/O tick; at most128 origin checks. */
		for (size_t i = 0; i < census->origin_count; i++)
			if (!census_origin_same(census, census->nodes[i]))
				return census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		if (!census_root_same(census))
			return census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		census->result = CLUSTER_RELMAP_CENSUS_DRAINED;
		return census->result;
	}
	/* The build is exclusive throughout. Check one qualification each tick,
	 * its exact pending origin before add, and all origins before DRAINED. */
	if (!census_origin_same(census, census->nodes[census->check_origin]))
		return census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
	census->check_origin = (census->check_origin + 1) % census->origin_count;
	if (census->rechecking) {
		ClusterRecoveryRootsEvent event;

		result = cluster_recovery_root_set_step(census->roots, &event);
		if (result > CLUSTER_RECOVERY_ROOTS_OBSERVED)
			return census_root_failed(census, result);
		if (result == CLUSTER_RECOVERY_ROOTS_OBSERVED)
			census->checked = true;
		return CLUSTER_RELMAP_CENSUS_MORE;
	}
	if (!census_root_same(census))
		return census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
	if (census->reading) {
		size_t length = Min((size_t)BLCKSZ, CLUSTER_RELMAP_V2_FILE_SIZE - census->offset);

		result = cluster_recovery_root_set_read(census->roots, census->next_entry,
			census->offset, census->copies[census->copy] + census->offset, length);
		if (result != CLUSTER_RECOVERY_ROOTS_BYTES)
			return census_root_failed(census, result);
		census->offset += length;
		if (census->offset == CLUSTER_RELMAP_V2_FILE_SIZE) {
			census->present |= 1U << census->copy;
			census->reading = false;
			census->next_entry++;
		}
		return CLUSTER_RELMAP_CENSUS_MORE;
	}
	entry = census->next_entry < census->count
		? cluster_recovery_root_set_entry(census->roots, census->next_entry) : NULL;
	if (census->namespace_path[0] != '\0') {
		size_t length = strlen(census->namespace_path);

		if (entry == NULL || strncmp(entry->file.path, census->namespace_path, length) != 0
			|| entry->file.path[length] != '/')
			return census_namespace_finish(census, out);
	}
	if (entry == NULL) {
		if (census->next_entry != census->count || !census->global_seen || !census->base_seen)
			return census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		result = cluster_recovery_root_set_recheck_begin(census->roots);
		if (result != CLUSTER_RECOVERY_ROOTS_MORE)
			return census_root_failed(census, result);
		census->rechecking = true;
		return CLUSTER_RELMAP_CENSUS_MORE;
	}
	directory = entry->file.kind == CLUSTER_RECOVERY_FILE_DIRECTORY
		&& (entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) != 0
		&& S_ISDIR(entry->file.observed.st_mode);
	if (strcmp(entry->file.path, "base") == 0) {
		if (!directory)
			return census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		census->base_seen = true;
	}
	if (census_namespace(entry->file.path, &dbid)) {
		if (!directory || census->namespace_path[0] != '\0')
			return census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		strlcpy(census->namespace_path, entry->file.path, sizeof(census->namespace_path));
		census->identity = (ClusterRelmapV2Namespace){ census->system_identifier,
			census->database_incarnation, dbid, dbid == InvalidOid };
		if (dbid == InvalidOid)
			census->global_seen = true;
	} else if (entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY
		|| entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP) {
		census->copy = entry->file.kind == CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY ? 0 : 1;
		if (census->namespace_path[0] == '\0'
			|| entry->file.locator.dbOid != census->identity.dbid
			|| entry->file.locator.spcOid != (census->identity.shared
				? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID)
			|| (census->present & (1U << census->copy)) != 0
			|| (entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0
			|| !S_ISREG(entry->file.observed.st_mode)
			|| entry->file.size != CLUSTER_RELMAP_V2_FILE_SIZE)
			return census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		census->reading = true;
		census->offset = 0;
		return CLUSTER_RELMAP_CENSUS_MORE;
	}
	census->next_entry++;
	return CLUSTER_RELMAP_CENSUS_MORE;
}

ClusterRelmapCensusResult
cluster_relmap_retention_census_next(ClusterRelmapRetentionCensus *census)
{
	return cluster_relmap_retention_census_next_committed(census, NULL);
}

static ClusterRelmapPendingResult
census_rebuild_ready(ClusterRelmapRetentionCensus *census)
{
	if (census == NULL)
		return CLUSTER_RELMAP_PENDING_INVALID_INPUT;
	if (census->result != CLUSTER_RELMAP_CENSUS_MORE || !census->rebuild_ready)
		return CLUSTER_RELMAP_PENDING_NOT_READY;
	/* No I/O: the whole physical ROOT recheck is still required before adopting
	 * any copied set. Recheck all borrowed qualifications between the two calls. */
	if (!census_root_same(census)) {
		census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		return CLUSTER_RELMAP_PENDING_NOT_READY;
	}
	for (size_t i = 0; i < census->origin_count; i++)
		if (!census_origin_same(census, census->nodes[i])) {
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
			return CLUSTER_RELMAP_PENDING_NOT_READY;
		}
	return CLUSTER_RELMAP_PENDING_OK;
}

ClusterRelmapPendingResult
cluster_relmap_retention_census_rebuild_pending(ClusterRelmapRetentionCensus *census,
	ClusterRelmapPendingSet *pending_out, const ClusterRelmapPendingDescriptor *descriptors,
	size_t descriptor_count)
{
	ClusterRelmapPendingResult result = census_rebuild_ready(census);

	if (result != CLUSTER_RELMAP_PENDING_OK)
		return result;
	/* Only the original full legal pair reaches rebuild_ready. Never use the
	 * externally copied map or reconstruct a view from its generation alone. */
	result = cluster_relmap_pending_rebuild_from_committed(pending_out, &census->identity,
		CLUSTER_RELMAP_V2_PAIR_VALID, &census->rebuild_view, descriptors, descriptor_count);
	census->rebuild_ready = false;
	memset(&census->rebuild_view, 0, sizeof(census->rebuild_view));
	if (result != CLUSTER_RELMAP_PENDING_OK)
		census_failed(census, result == CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED
			? CLUSTER_RELMAP_CENSUS_CAPACITY : CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
	return result;
}

static ClusterRelmapPendingResult
census_rebuild_pending_wal(ClusterRelmapRetentionCensus *census,
	ClusterRelmapPendingSet *pending_out, struct ClusterWalInputsV1 *inputs,
	const ClusterRelmapPendingDescriptor *descriptors, size_t descriptor_count,
	const ClusterRelmapPendingSideSource *sources, size_t source_count)
{
	ClusterRelmapPendingResult result = census_rebuild_ready(census);
	ClusterRelmapV2Image image = { 0 };

	if (result != CLUSTER_RELMAP_PENDING_OK)
		return result;
	image.identity = census->identity;
	image.committed_generation = census->rebuild_view.committed_generation;
	image.committed = census->rebuild_view.committed;
	if (census->rebuild_view.has_pending) {
		/* A published primary can have a newer committed map than the backup
		 * that still pins pending. Preserve that pending copy's original base. */
		image.committed_generation = census->rebuild_view.pending_committed_generation;
		image.committed = census->rebuild_view.pending_committed;
		image.pending_generation = census->rebuild_view.pending_generation;
		image.pending_owner = census->rebuild_view.pending_owner;
		image.pending = census->rebuild_view.pending;
	}
	PG_TRY();
	{
		ClusterControlRootResult retained = CLUSTER_CONTROL_ROOT_OK_PRIMARY;

		if (image.pending_generation != 0 || sources != NULL) {
			/* The combined path must bind its whole sealed participant vector
			 * to the actual claims/cuts in this same original retained scan,
			 * including structural obligations when both relmap copies are clean. */
			retained = sources == NULL
				? cluster_relmap_owner_retained_wal_v2(&image, inputs)
				: cluster_relmap_owner_retained_side_wal_v2(&image, inputs, sources, source_count);
		}
		if (retained != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
			result = CLUSTER_RELMAP_PENDING_NOT_READY;
		} else {
			/* Recheck borrowed ROOT/origin qualifications after the retained I/O,
			 * then let the original atomic core consume this one exact window. */
			result = cluster_relmap_retention_census_rebuild_pending(census, pending_out,
				descriptors, descriptor_count);
		}
	}
	PG_CATCH();
	{
		census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

ClusterRelmapPendingResult
cluster_relmap_retention_census_rebuild_pending_wal(ClusterRelmapRetentionCensus *census,
	ClusterRelmapPendingSet *pending_out, struct ClusterWalInputsV1 *inputs,
	const ClusterRelmapPendingDescriptor *descriptors, size_t descriptor_count)
{
	return census_rebuild_pending_wal(census, pending_out, inputs, descriptors,
		descriptor_count, NULL, 0);
}

ClusterRelmapPendingResult
cluster_relmap_retention_census_rebuild_pending_side(ClusterRelmapRetentionCensus *census,
	ClusterRelmapPendingSet *pending_out, const ClusterRelmapPendingSideSource *sources,
	size_t source_count)
{
	ClusterRelmapPendingDescriptor descriptors[CLUSTER_RELMAP_PENDING_CAPACITY];
	size_t count = 0;
	ClusterRelmapPendingResult result = census_rebuild_ready(census);

	if (result != CLUSTER_RELMAP_PENDING_OK)
		return result;
	/* Keep the original namespace/pair and SIDE selection together. A collector
	 * refusal or ERROR must not leave a reusable ROOT census. */
	PG_TRY();
	{
		result = cluster_relmap_pending_collect_side(&census->identity, census->storage_uuid,
			sources, source_count, descriptors, lengthof(descriptors), &count);
	}
	PG_CATCH();
	{
		census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result != CLUSTER_RELMAP_PENDING_OK) {
		census_failed(census, result == CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED
			? CLUSTER_RELMAP_CENSUS_CAPACITY : CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		return result;
	}
	/* The core installs atomically and consumes this exact pair window once. */
	return cluster_relmap_retention_census_rebuild_pending(census, pending_out, descriptors, count);
}

ClusterRelmapPendingResult
cluster_relmap_retention_census_rebuild_pending_side_wal(ClusterRelmapRetentionCensus *census,
	ClusterRelmapPendingSet *pending_out, struct ClusterWalInputsV1 *inputs,
	const ClusterRelmapPendingSideSource *sources, size_t source_count)
{
	ClusterRelmapPendingDescriptor descriptors[CLUSTER_RELMAP_PENDING_CAPACITY];
	size_t count = 0;
	ClusterRelmapPendingResult result = census_rebuild_ready(census);

	if (result != CLUSTER_RELMAP_PENDING_OK)
		return result;
	/* Neither extraction nor retained I/O may install a partial set. Both
	 * gates must use this same private namespace view before it is consumed. */
	PG_TRY();
	{
		result = cluster_relmap_pending_collect_side(&census->identity, census->storage_uuid,
			sources, source_count, descriptors, lengthof(descriptors), &count);
	}
	PG_CATCH();
	{
		census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result != CLUSTER_RELMAP_PENDING_OK) {
		census_failed(census, result == CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED
			? CLUSTER_RELMAP_CENSUS_CAPACITY : CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		return result;
	}
	/* Original scope, final borrowed-state recheck and ERROR/pin ownership
	 * remain in the existing WAL path; only its atomic core consumes the view. */
	return census_rebuild_pending_wal(census, pending_out, inputs, descriptors, count,
		sources, source_count);
}

void
cluster_relmap_retention_census_destroy(ClusterRelmapRetentionCensus *census)
{
	if (census != NULL && census->result == CLUSTER_RELMAP_CENSUS_MORE)
		census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
	free(census);
}
#endif
