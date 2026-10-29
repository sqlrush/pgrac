/* PGRAC: relmap pending census through the original cold ROOT reader.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_retention_census.h"
#include "common/cryptohash.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_recovery_wal_targets.h"
#include "cluster/cluster_relmap_arb.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_relmap_terminal_scan.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/cluster_wal_inputs.h"
#include "miscadmin.h"
#include "storage/fd.h"

typedef struct CensusOrigin {
	ClusterRelmapRetentionDirectory *directory;
	uint64 generation;
	uint64 native_floor;
	uint64 last_issued;
} CensusOrigin;

typedef struct CensusEpisode {
	uint32 count;
	ClusterRecoveryRelmapSourceV1 sources[FLEXIBLE_ARRAY_MEMBER];
} CensusEpisode;

struct ClusterRelmapRetentionCensus {
	CensusEpisode *episode; /* same original all-source owner, never refreshed */
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
	/* One-shot, original execution-owner observation provenance only. */
	ClusterRelmapRecoverySourceObservationV1 cold_observe_source;
	uint8 cold_observe_pins[32];
	uint16 cold_observe_origin;
	bool cold_observe_ready;
	/* Borrowed construction owns its directory/block; restart also owns its ROOT.
	 * Full initial construction additionally owns its new tracked fd. */
	ClusterRelmapRetentionDirectory *owned_directory;
	bool owns_roots;
	int owned_root_fd; /* -1 unless full initial construction transfers custody. */
	bool cold_input_bound;
	ClusterRelmapRecoverySourceObservationV1 cold_input_source;
	/* Borrowed original protection; dependents are destroyed first. */
	const ClusterRelmapRetentionCensus *cold_parent;
	bool owns_cold_parent; /* Only the one-shot executor adopts its local input. */
};

typedef struct CensusRecoveryItem {
	ClusterRelmapCensusRecovery input;
	ClusterRelmapRetentionPin pin;
} CensusRecoveryItem;

struct ClusterRelmapCensusRecoveryBatch {
	ClusterRelmapRetentionCensus *census;
	ClusterWalInputsV1 *inputs;
	bool cold;
	ClusterRelmapRecoverySourceObservationV1 cold_source;
	uint8 pins[32];
	size_t bytes;
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
	if (census->episode != NULL)
		cluster_wal_inputs_relmap_source_fail_v1(&census->episode->sources[0]);
	census->result = result;
	census->rebuild_ready = false;
	census->cold_observe_ready = false;
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

static bool census_initial_current(ClusterRelmapRetentionCensus *census);
static bool census_episode_validate(ClusterRelmapRetentionCensus *census);

static bool
census_recovery_current(ClusterRelmapRetentionCensus *census)
{
	if (!census_episode_validate(census))
		return false;
	if (!census_initial_current(census) || !census_root_same(census))
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

	if (inputs == NULL
		|| (census->episode != NULL && inputs != census->episode->sources[0].scope)
		|| cluster_wal_inputs_revalidate_v1(inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
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
census_recovery_output_overlap(const ClusterRelmapRetentionCensus *census, const void *out,
							   size_t bytes)
{
	if (census == NULL)
		return false;
	if (census_recovery_overlap(out, bytes, census, sizeof(*census))
		|| census_recovery_overlap(out, bytes, census->roots, 1))
		return true;
	for (size_t i = 0; i < census->origin_count; i++)
		if (census_recovery_overlap(out, bytes, census->origins[census->nodes[i]].directory,
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
census_cold_source_current(const ClusterRelmapRetentionCensus *census, uint16 origin,
						   const ClusterRelmapRecoverySourceObservationV1 *source)
{
	return source->scope_serial != 0
		   && source->source.claim.identity.system_identifier == census->system_identifier
		   && source->source.claim.database_incarnation == census->database_incarnation
		   && source->source.claim.identity.origin_node_id == origin
		   && memcmp(source->source.claim.identity.storage_uuid, census->storage_uuid, 16) == 0
		   && cluster_undo_recovery_cold_single_source_v1(
			   &source->source, &source->checkpoint, source->native_redo, source->scope_serial);
}

static bool
census_open_context(void)
{
	return MyBackendType == B_STARTUP && RecoveryInProgress() && CritSectionCount == 0
		   && cluster_shared_catalog && cluster_recovery_merge_claim_is_held()
		   && cluster_relmap_authority_x_held();
}

/* These original source checks do no physical ROOT I/O. Never invoke them
 * from inside the native ROOT reader; the wrapper brackets each returned tick. */
static bool
census_episode_current(const ClusterRelmapRetentionCensus *census)
{
	const CensusEpisode *episode = census->episode;

	if (episode == NULL)
		return true;
	if (!census_open_context()
		|| cluster_wal_inputs_relmap_count_v1(episode->sources[0].scope) != episode->count)
		return false;
	for (uint32 i = 0; i < episode->count; i++)
		if (!cluster_wal_inputs_relmap_source_current_v1(&episode->sources[i]))
			return false;
	return census_open_context()
		&& cluster_wal_inputs_relmap_count_v1(episode->sources[0].scope) == episode->count;
}

/* Reuse paths outside the physical tick wrapper need the same sticky ERROR
 * rule, including the very first owner check. No failure retires protections. */
static bool
census_episode_validate(ClusterRelmapRetentionCensus *census)
{
	volatile bool accepted = false;

	if (census->episode == NULL)
		return true;
	PG_TRY();
	{
		accepted = census_episode_current(census);
	}
	PG_FINALLY();
	{
		if (!accepted)
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
	}
	PG_END_TRY();
	return accepted;
}

static bool
census_cold_input_context(const ClusterRelmapRetentionCensus *census)
{
	for (const ClusterRelmapRetentionCensus *current = census; current != NULL;
		 current = current->cold_parent)
		if (current->owned_root_fd >= 0)
			return census_open_context();
	return true;
}

/* Original source I/O must precede these exclusively held local checks.
 * Old physical observations stay invalid; their pins are still protection. */
static bool
census_cold_ancestors_current(const ClusterRelmapRetentionCensus *census)
{
	const ClusterRelmapRecoverySourceObservationV1 *source = &census->cold_input_source;

	for (const ClusterRelmapRetentionCensus *parent = census->cold_parent; parent != NULL;
		 parent = parent->cold_parent) {
		const ClusterRelmapRecoverySourceObservationV1 *saved = &parent->cold_observe_source;
		uint8 pins[32];

		if (parent->result != CLUSTER_RELMAP_CENSUS_CHANGED || parent->origin_count != 1
			|| parent->nodes[0] != census->nodes[0]
			|| parent->cold_observe_origin != census->nodes[0]
			|| memcmp(&saved->source, &source->source, sizeof(source->source)) != 0
			|| memcmp(&saved->checkpoint, &source->checkpoint, sizeof(source->checkpoint)) != 0
			|| saved->native_redo != source->native_redo
			|| saved->scope_serial != source->scope_serial
			|| !census_origin_same_in_state(parent, parent->nodes[0],
											CLUSTER_RELMAP_RETENTION_UNKNOWN)
			|| !census_recovery_pins_digest(parent, pins)
			|| memcmp(pins, parent->cold_observe_pins, sizeof(pins)) != 0)
			return false;
	}
	return true;
}

/* A produced cursor cannot outlive or replace its original source owner.
 * The source check does I/O; revoke observations even when it raises ERROR. */
static bool
census_initial_current(ClusterRelmapRetentionCensus *census)
{
	volatile bool accepted = false;

	if (!census->cold_input_bound)
		return true;
	PG_TRY();
	{
		accepted
			= census->origin_count == 1 && cluster_shared_catalog
			  && cluster_recovery_merge_claim_is_held() && cluster_relmap_authority_x_held()
			  && census_cold_input_context(census)
			  && census_cold_source_current(census, census->nodes[0], &census->cold_input_source)
			  && census_cold_ancestors_current(census) && cluster_shared_catalog
			  && cluster_recovery_merge_claim_is_held() && cluster_relmap_authority_x_held()
			  && census_cold_input_context(census);
	}
	PG_FINALLY();
	{
		if (!accepted)
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
	}
	PG_END_TRY();
	return accepted;
}

bool
cluster_relmap_retention_census_begin_cold(ClusterRecoveryRootSet *roots, uint16 origin,
										   size_t memory_budget, ClusterRelmapRetentionCensus **out)
{
	ClusterRelmapRetentionDirectory *volatile directory = NULL;
	ClusterRelmapRetentionCensus *volatile fresh = NULL;
	ClusterRelmapRecoverySourceObservationV1 source = { 0 };
	uint8 digest[32];
	volatile bool accepted = false;

	if (out == NULL || census_recovery_overlap(out, sizeof(*out), roots, 1))
		return false;
	*out = NULL;
	if (origin >= CLUSTER_RELMAP_RETENTION_MAX_ORIGINS || !cluster_shared_catalog
		|| !cluster_recovery_merge_claim_is_held() || !cluster_relmap_authority_x_held()
		|| !cluster_recovery_root_set_digest(roots, digest))
		return false;
	PG_TRY();
	{
		do {
			ClusterRelmapRetentionDirectory *directories[1];
			ClusterRecoveryRootsResult result;
			ClusterRecoveryRootsEvent event;

			if (memory_budget < sizeof(*fresh)
				|| memory_budget - sizeof(*fresh) < sizeof(*directory))
				break;
			directory = calloc(1, sizeof(*directory));
			if (directory == NULL
				|| !cluster_relmap_recovery_retention_build_start_cold_source_v1(
					origin, (ClusterRelmapRetentionDirectory *)directory, &source)
				|| directory->system_identifier != source.source.claim.identity.system_identifier
				|| directory->database_incarnation != source.source.claim.database_incarnation
				|| directory->origin != origin || directory->generation != source.scope_serial)
				break;
			directories[0] = (ClusterRelmapRetentionDirectory *)directory;
			if (cluster_relmap_retention_census_begin(
					roots, directory->system_identifier, directory->database_incarnation,
					source.source.claim.identity.storage_uuid, directories, 1,
					memory_budget - sizeof(*directory), (ClusterRelmapRetentionCensus **)&fresh)
				!= CLUSTER_RELMAP_CENSUS_MORE)
				break;
			fresh->owned_directory = (ClusterRelmapRetentionDirectory *)directory;
			directory = NULL;
			fresh->cold_input_source = source;
			fresh->cold_input_bound = true;
			if (!census_recovery_current((ClusterRelmapRetentionCensus *)fresh))
				break;
			result = cluster_recovery_root_set_recheck_begin(roots);
			while (result < CLUSTER_RECOVERY_ROOTS_OBSERVED) {
				CHECK_FOR_INTERRUPTS();
				result = cluster_recovery_root_set_step(roots, &event);
			}
			if (result != CLUSTER_RECOVERY_ROOTS_OBSERVED
				|| !census_recovery_current((ClusterRelmapRetentionCensus *)fresh))
				break;
			*out = (ClusterRelmapRetentionCensus *)fresh;
			fresh = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (!accepted)
			cluster_recovery_root_set_invalidate(roots);
		cluster_relmap_retention_census_destroy((ClusterRelmapRetentionCensus *)fresh);
		free((ClusterRelmapRetentionDirectory *)directory);
	}
	PG_END_TRY();
	return accepted;
}

bool
cluster_relmap_retention_census_open_cold(uint16 origin, size_t root_budget, size_t memory_budget,
										  size_t payload_budget, ClusterRelmapRetentionCensus **out)
{
	ClusterRelmapRetentionDirectory *volatile directory = NULL;
	ClusterRecoveryRootSet *volatile roots = NULL;
	ClusterRelmapRetentionCensus *volatile fresh = NULL;
	int volatile fd = -1;
	volatile bool accepted = false;

	if (out == NULL || *out != NULL || origin >= CLUSTER_RELMAP_RETENTION_MAX_ORIGINS
		|| root_budget == 0 || payload_budget == 0 || memory_budget < sizeof(*fresh)
		|| memory_budget - sizeof(*fresh) < sizeof(*directory) || !census_open_context())
		return false;
	PG_TRY();
	{
		do {
			ClusterRelmapRecoverySourceObservationV1 source = { 0 };
			ClusterRelmapRecoveryRootObservationV1 observed = { 0 };
			ClusterRelmapRetentionDirectory *directories[1];
			ClusterRecoveryRootsEvent event;
			ClusterRecoveryRootsResult result;

			directory = calloc(1, sizeof(*directory));
			if (directory == NULL
				|| !cluster_relmap_recovery_retention_build_start_cold_source_v1(
					origin, (ClusterRelmapRetentionDirectory *)directory, &source)
				|| directory->system_identifier != source.source.claim.identity.system_identifier
				|| directory->database_incarnation != source.source.claim.database_incarnation
				|| directory->origin != origin || directory->generation != source.scope_serial
				|| !cluster_relmap_recovery_root_observe_cold_v1(&source, root_budget,
																 payload_budget, &observed))
				break;
			roots = observed.roots;
			fd = observed.root_fd;
			directories[0] = (ClusterRelmapRetentionDirectory *)directory;
			if (cluster_relmap_retention_census_begin(
					roots, directory->system_identifier, directory->database_incarnation,
					source.source.claim.identity.storage_uuid, directories, 1,
					memory_budget - sizeof(*directory), (ClusterRelmapRetentionCensus **)&fresh)
				!= CLUSTER_RELMAP_CENSUS_MORE)
				break;
			fresh->owned_directory = (ClusterRelmapRetentionDirectory *)directory;
			fresh->owns_roots = true;
			fresh->owned_root_fd = fd;
			fresh->cold_input_source = source;
			fresh->cold_input_bound = true;
			directory = NULL;
			roots = NULL;
			fd = -1;
			if (!census_recovery_current((ClusterRelmapRetentionCensus *)fresh))
				break;
			/* Retain all original protections. No new observation may skip
			 * a complete verification after original-source producer I/O. */
			result = cluster_recovery_root_set_recheck_begin(fresh->roots);
			while (result < CLUSTER_RECOVERY_ROOTS_OBSERVED) {
				CHECK_FOR_INTERRUPTS();
				if (!census_open_context())
					break;
				result = cluster_recovery_root_set_step(fresh->roots, &event);
			}
			if (result != CLUSTER_RECOVERY_ROOTS_OBSERVED
				|| !census_recovery_current((ClusterRelmapRetentionCensus *)fresh))
				break;
			*out = (ClusterRelmapRetentionCensus *)fresh;
			fresh = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		cluster_relmap_retention_census_destroy((ClusterRelmapRetentionCensus *)fresh);
		cluster_recovery_root_set_destroy((ClusterRecoveryRootSet *)roots);
		if (fd >= 0)
			(void)CloseTransientFile(fd);
		free((ClusterRelmapRetentionDirectory *)directory);
	}
	PG_END_TRY();
	return accepted;
}

/* Only the synchronous whole batch may defer this physical pass. Its local
 * handles remain unpublished until the final whole-ROOT recheck. */
static bool
census_recovery_prepare_cold(ClusterRelmapRetentionCensus *census, Oid dbid,
							 ClusterRelmapCensusRecovery *out, bool recheck)
{
	ClusterRelmapRecoveryHandle *volatile handle = NULL;
	volatile bool accepted = false;

	if (out == NULL || census_recovery_output_overlap(census, out, sizeof(*out)))
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
				|| !census_cold_source_current(census, scratch.pending.pending_owner.node, &source))
				break;
			/* A saved OBSERVED digest cannot conceal physical changes during
			 * native preparation, including unrelated/new namespaces. */
			root_result = CLUSTER_RECOVERY_ROOTS_OBSERVED;
			if (recheck) {
				root_result = cluster_recovery_root_set_recheck_begin(census->roots);
				while (root_result == CLUSTER_RECOVERY_ROOTS_MORE
					   || root_result == CLUSTER_RECOVERY_ROOTS_ENTRY
					   || root_result == CLUSTER_RECOVERY_ROOTS_BYTES) {
					ClusterRecoveryRootsEvent event;
					root_result = cluster_recovery_root_set_step(census->roots, &event);
				}
			}
			if (root_result != CLUSTER_RECOVERY_ROOTS_OBSERVED
				|| !cluster_relmap_recovery_handle_source_v1(handle, &current)
				|| memcmp(&source.source, &current.source, sizeof(source.source)) != 0
				|| memcmp(&source.checkpoint, &current.checkpoint, sizeof(source.checkpoint)) != 0
				|| source.native_redo != current.native_redo
				|| source.scope_serial != current.scope_serial
				|| !census_cold_source_current(census, scratch.pending.pending_owner.node, &current)
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

bool
cluster_relmap_retention_census_recovery_prepare_cold(ClusterRelmapRetentionCensus *census,
													  Oid dbid, ClusterRelmapCensusRecovery *out)
{
	return census_recovery_prepare_cold(census, dbid, out, true);
}

/* After this call revokes physical observations, UNKNOWN is a required state,
 * never a qualification. Original native authority and every pin stay held. */
static bool
census_cold_consumer_current(const ClusterRelmapRetentionCensus *census, uint16 origin,
							 const ClusterRelmapRecoverySourceObservationV1 *source,
							 const uint8 saved_pins[32], ClusterRelmapRetentionState state)
{
	uint8 pins[32];

	/* Complete original source/owner I/O before the final local pin check. */
	if (!census_cold_input_context(census) || !census_cold_source_current(census, origin, source)
		|| !census_cold_ancestors_current(census))
		return false;
	for (size_t i = 0; i < census->origin_count; i++)
		if (!census_origin_same_in_state(census, census->nodes[i], state))
			return false;
	return census_recovery_pins_digest(census, pins) && memcmp(pins, saved_pins, sizeof(pins)) == 0
		   && cluster_shared_catalog && cluster_recovery_merge_claim_is_held()
		   && cluster_relmap_authority_x_held() && census_cold_input_context(census);
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
				|| !census_cold_consumer_current(census, input.pending.pending_owner.node, &source,
												 saved_pins, CLUSTER_RELMAP_RETENTION_BUILDING))
				break;
			/* Native control-ROOT authority is independent of these physical
			 * observations. Revoke them BEFORE the first arbiter attempt. */
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
			if (!census_cold_consumer_current(census, input.pending.pending_owner.node, &source,
											  saved_pins, CLUSTER_RELMAP_RETENTION_UNKNOWN))
				break;
			native_result = cluster_relmap_arbitrate_pending_v2(
				(ClusterRelmapRecoveryHandle **)&handle, &input.identity, &input.pending);
			if (!census_cold_consumer_current(census, input.pending.pending_owner.node, &source,
											  saved_pins, CLUSTER_RELMAP_RETENTION_UNKNOWN))
				break;
			if (native_result == CLUSTER_RELMAP_V2_CLEARED_POSTREAD) {
				census->cold_observe_source = source;
				memcpy(census->cold_observe_pins, saved_pins, sizeof(saved_pins));
				census->cold_observe_origin = input.pending.pending_owner.node;
				result = CLUSTER_RELMAP_V2_CLEARED_POSTREAD;
			}
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
	/* Cleanup above revokes all old observations. Only a normal successful
	 * original-owner return may expose this one-shot follow-on; never ERROR. */
	if (result == CLUSTER_RELMAP_V2_CLEARED_POSTREAD)
		census->cold_observe_ready = true;
	return result;
}

/* The public entry has already consumed the original one-shot seed. */
static bool
census_reobserve_cold(ClusterRelmapRetentionCensus *census, size_t root_budget,
					  ClusterRecoveryRootSet **out)
{
	ClusterRecoveryRootSet *volatile fresh = NULL;
	volatile bool accepted = false;

	PG_TRY();
	{
		do {
			ClusterRecoveryRootsResult result;

			if (!census_cold_consumer_current(
					census, census->cold_observe_origin, &census->cold_observe_source,
					census->cold_observe_pins, CLUSTER_RELMAP_RETENTION_UNKNOWN))
				break;
			result = cluster_recovery_root_set_reobserve_begin(census->roots, root_budget,
															   (ClusterRecoveryRootSet **)&fresh);
			while (result == CLUSTER_RECOVERY_ROOTS_MORE || result == CLUSTER_RECOVERY_ROOTS_ENTRY
				   || result == CLUSTER_RECOVERY_ROOTS_BYTES) {
				ClusterRecoveryRootsEvent event;
				result = cluster_recovery_root_set_step((ClusterRecoveryRootSet *)fresh, &event);
			}
			if (result != CLUSTER_RECOVERY_ROOTS_OBSERVED
				|| !census_cold_consumer_current(
					census, census->cold_observe_origin, &census->cold_observe_source,
					census->cold_observe_pins, CLUSTER_RELMAP_RETENTION_UNKNOWN))
				break;
			*out = (ClusterRecoveryRootSet *)fresh;
			fresh = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (!accepted)
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		cluster_recovery_root_set_destroy((ClusterRecoveryRootSet *)fresh);
	}
	PG_END_TRY();
	return accepted;
}

static bool
census_cold_follow_on_ready(const ClusterRelmapRetentionCensus *census)
{
	return census != NULL && census->cold_observe_ready
		   && census->result == CLUSTER_RELMAP_CENSUS_CHANGED && cluster_shared_catalog
		   && cluster_recovery_merge_claim_is_held() && cluster_relmap_authority_x_held();
}

bool
cluster_relmap_retention_census_reobserve_cold(ClusterRelmapRetentionCensus *census,
											   size_t root_budget, ClusterRecoveryRootSet **out)
{
	if (out == NULL || census_recovery_output_overlap(census, out, sizeof(*out)))
		return false;
	*out = NULL;
	if (!census_cold_follow_on_ready(census))
		return false;
	/* Consume before any source/hash/I/O work, including work which ERRORs. */
	census->cold_observe_ready = false;
	return census_reobserve_cold(census, root_budget, out);
}

bool
cluster_relmap_retention_census_restart_cold(ClusterRelmapRetentionCensus *census,
											 size_t root_budget, size_t memory_budget,
											 ClusterRelmapRetentionCensus **out)
{
	ClusterRelmapRetentionDirectory *volatile directory = NULL;
	ClusterRecoveryRootSet *volatile roots = NULL;
	ClusterRelmapRetentionCensus *volatile fresh = NULL;
	volatile bool accepted = false;

	if (out == NULL || census_recovery_output_overlap(census, out, sizeof(*out)))
		return false;
	*out = NULL;
	if (!census_cold_follow_on_ready(census) || census->origin_count != 1
		|| census->nodes[0] != census->cold_observe_origin)
		return false;
	census->cold_observe_ready = false;
	PG_TRY();
	{
		do {
			const CensusOrigin *original = &census->origins[census->cold_observe_origin];
			ClusterRelmapRetentionDirectory *directories[1];

			if (memory_budget < sizeof(*fresh)
				|| memory_budget - sizeof(*fresh) < sizeof(*directory)
				|| !census_cold_consumer_current(
					census, census->cold_observe_origin, &census->cold_observe_source,
					census->cold_observe_pins, CLUSTER_RELMAP_RETENTION_UNKNOWN))
				break;
			directory = calloc(1, sizeof(*directory));
			if (directory == NULL
				|| !cluster_relmap_recovery_retention_build_start_cold_v1(
					census->cold_observe_origin, (ClusterRelmapRetentionDirectory *)directory)
				|| directory->system_identifier != census->system_identifier
				|| directory->database_incarnation != census->database_incarnation
				|| directory->origin != census->cold_observe_origin
				|| directory->generation != census->cold_observe_source.scope_serial
				|| directory->native_floor != original->native_floor
				|| directory->last_issued != original->last_issued
				|| !census_cold_consumer_current(
					census, census->cold_observe_origin, &census->cold_observe_source,
					census->cold_observe_pins, CLUSTER_RELMAP_RETENTION_UNKNOWN))
				break;
			/* Physical observation follows all checkpoint producer I/O. Old
			 * pins are not copied, promoted or retired by this new build. */
			if (!census_reobserve_cold(census, root_budget, (ClusterRecoveryRootSet **)&roots))
				break;
			directories[0] = (ClusterRelmapRetentionDirectory *)directory;
			if (cluster_relmap_retention_census_begin(
					roots, census->system_identifier, census->database_incarnation,
					census->storage_uuid, directories, 1, memory_budget - sizeof(*directory),
					(ClusterRelmapRetentionCensus **)&fresh)
				!= CLUSTER_RELMAP_CENSUS_MORE)
				break;
			fresh->owned_directory = (ClusterRelmapRetentionDirectory *)directory;
			fresh->owns_roots = true;
			fresh->cold_input_source = census->cold_observe_source;
			fresh->cold_input_bound = true;
			fresh->cold_parent = census;
			directory = NULL;
			roots = NULL;
			if (!census_cold_consumer_current(
					census, census->cold_observe_origin, &census->cold_observe_source,
					census->cold_observe_pins, CLUSTER_RELMAP_RETENTION_UNKNOWN)
				|| !census_recovery_current((ClusterRelmapRetentionCensus *)fresh))
				break;
			*out = (ClusterRelmapRetentionCensus *)fresh;
			fresh = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (!accepted)
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		cluster_relmap_retention_census_destroy((ClusterRelmapRetentionCensus *)fresh);
		cluster_recovery_root_set_destroy((ClusterRecoveryRootSet *)roots);
		free((ClusterRelmapRetentionDirectory *)directory);
	}
	PG_END_TRY();
	return accepted;
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

static bool census_recovery_batch_current(const ClusterRelmapCensusRecoveryBatch *batch,
										  ClusterRelmapRetentionState state);

static bool
census_recovery_prepare_all(ClusterRelmapRetentionCensus *census, ClusterWalInputsV1 *inputs,
							size_t memory_budget, ClusterRelmapCensusRecoveryBatch **out, bool cold)
{
	ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
	volatile bool accepted = false;

	if (out == NULL || (cold && census_recovery_output_overlap(census, out, sizeof(*out))))
		return false;
	*out = NULL;
	if (census == NULL || census->result != CLUSTER_RELMAP_CENSUS_DRAINED || !cluster_shared_catalog
		|| !cluster_recovery_merge_claim_is_held() || !cluster_relmap_authority_x_held()
		|| (cold && census->origin_count != 1))
		return false;
	PG_TRY();
	{
		do {
			size_t capacity = 0, bytes;
			bool complete = true;
			ClusterRecoveryRootsResult root_result;

			if (!census->checked || !census_recovery_current(census)
				|| (!cold && !census_recovery_sources(census, inputs)))
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
			batch->cold = cold;
			batch->bytes = bytes;
			if (!census_recovery_pins_digest(census, batch->pins))
				break;
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
				if (!(cold ? census_recovery_prepare_cold(census, pair.identity.dbid, &item->input,
														  false)
						   : cluster_relmap_retention_census_recovery_prepare(
								 census, inputs, pair.identity.dbid, &item->input))) {
					complete = false;
					break;
				}
				/* Count the owned handle before any later check can fail. */
				batch->count++;
				if (cold) {
					ClusterRelmapRecoverySourceObservationV1 source;
					if (item->input.pending.pending_owner.node != census->nodes[0]
						|| !cluster_relmap_recovery_handle_source_v1(item->input.handle, &source)) {
						complete = false;
						break;
					}
					if (batch->count == 1)
						batch->cold_source = source;
					else if (memcmp(&source.source, &batch->cold_source.source,
									sizeof(source.source))
								 != 0
							 || memcmp(&source.checkpoint, &batch->cold_source.checkpoint,
									   sizeof(source.checkpoint))
									!= 0
							 || source.native_redo != batch->cold_source.native_redo
							 || source.scope_serial != batch->cold_source.scope_serial) {
						complete = false;
						break;
					}
					if (!census_cold_consumer_current(census, census->nodes[0], &batch->cold_source,
														  batch->pins,
													  CLUSTER_RELMAP_RETENTION_BUILDING)) {
						complete = false;
						break;
					}
				}
				pin = census_recovery_pin(census, &item->input.identity, &item->input.pending);
				if (pin == NULL) {
					complete = false;
					break;
				}
				item->pin = *pin;
				if (!cold
					&& !census_recovery_batch_current(batch, CLUSTER_RELMAP_RETENTION_BUILDING)) {
					complete = false;
					break;
				}
			}
			if (!complete || !census_recovery_current(census)
				|| (cold && batch->count == 0)
				|| !census_recovery_batch_current(batch, CLUSTER_RELMAP_RETENTION_BUILDING))
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
				|| !census_recovery_batch_current(batch, CLUSTER_RELMAP_RETENTION_BUILDING)
				|| !cluster_shared_catalog || !cluster_recovery_merge_claim_is_held()
				|| !cluster_relmap_authority_x_held())
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

bool
cluster_relmap_retention_census_recovery_prepare_all(ClusterRelmapRetentionCensus *census,
													 ClusterWalInputsV1 *inputs,
													 size_t memory_budget,
													 ClusterRelmapCensusRecoveryBatch **out)
{
	return census_recovery_prepare_all(census, inputs, memory_budget, out, false);
}

bool
cluster_relmap_retention_census_recovery_prepare_all_cold(ClusterRelmapRetentionCensus *census,
														  size_t memory_budget,
														  ClusterRelmapCensusRecoveryBatch **out)
{
	return census_recovery_prepare_all(census, NULL, memory_budget, out, true);
}

/* Does not adopt a physical ROOT observation or qualify an UNKNOWN directory.
 * Native arbitration independently checks its original cold authority. */
static bool
census_recovery_batch_current(const ClusterRelmapCensusRecoveryBatch *batch,
							  ClusterRelmapRetentionState state)
{
	const ClusterRelmapRetentionCensus *census = batch->census;
	uint8 pins[32];

	if (batch->cold)
		return census->origin_count == 1 && batch->count != 0
			   && census_cold_consumer_current(census, census->nodes[0], &batch->cold_source,
											   batch->pins, state);
	if (!cluster_shared_catalog || !cluster_recovery_merge_claim_is_held()
		|| !cluster_relmap_authority_x_held() || !census_recovery_sources(census, batch->inputs))
		return false;
	for (size_t i = 0; i < census->origin_count; i++)
		if (!census_origin_same_in_state(census, census->nodes[i], state))
			return false;
	if (!census_recovery_pins_digest(census, pins) || memcmp(pins, batch->pins, sizeof(pins)) != 0)
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

static ClusterRelmapV2ClearResult
census_recovery_arbitrate_all(ClusterRelmapCensusRecoveryBatch **custody, bool cold)
{
	ClusterRelmapCensusRecoveryBatch *batch;
	ClusterRelmapRetentionCensus *census;
	volatile bool invalidate = false;
	ClusterRelmapV2ClearResult result = CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;

	if (custody == NULL || *custody == NULL)
		return result;
	batch = *custody;
	census = batch->census;
	if (cold
		&& (census_recovery_output_overlap(census, custody, sizeof(*custody))
			|| census_recovery_overlap(custody, sizeof(*custody), batch, batch->bytes)))
		return result;
	*custody = NULL;
	PG_TRY();
	{
		do {
			ClusterRecoveryRootsResult root_result;
			size_t consumed;

			if (batch->cold != cold || !cluster_shared_catalog
				|| !cluster_recovery_merge_claim_is_held() || !cluster_relmap_authority_x_held())
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
				ClusterRelmapV2ClearResult native_result;

				/* UNKNOWN is required only because this operation just set it;
				 * unchanged metadata/pins are not directory qualification. */
				if (!census_recovery_batch_current(batch, CLUSTER_RELMAP_RETENTION_UNKNOWN))
					break;
				native_result = cluster_relmap_arbitrate_pending_v2(&item->handle, &item->identity,
																	&item->pending);
				/* UNKNOWN may follow target writes too. Complete the
				 * original source/pin checks on EVERY normal return. */
				if (!census_recovery_batch_current(batch, CLUSTER_RELMAP_RETENTION_UNKNOWN))
					break;
				if (native_result != CLUSTER_RELMAP_V2_CLEARED_POSTREAD)
					break;
			}
			if (consumed == batch->count) {
				if (cold) {
					census->cold_observe_source = batch->cold_source;
					memcpy(census->cold_observe_pins, batch->pins, sizeof(batch->pins));
					census->cold_observe_origin = census->nodes[0];
				}
				result = CLUSTER_RELMAP_V2_CLEARED_POSTREAD;
			}
		} while (false);
	}
	PG_FINALLY();
	{
		if (invalidate && census != NULL)
			census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		cluster_relmap_retention_census_recovery_batch_release(&batch);
	}
	PG_END_TRY();
	if (cold && result == CLUSTER_RELMAP_V2_CLEARED_POSTREAD)
		census->cold_observe_ready = true;
	return result;
}

ClusterRelmapV2ClearResult
cluster_relmap_retention_census_recovery_arbitrate_all(ClusterRelmapCensusRecoveryBatch **custody)
{
	return census_recovery_arbitrate_all(custody, false);
}

ClusterRelmapV2ClearResult
cluster_relmap_retention_census_recovery_arbitrate_all_cold(
	ClusterRelmapCensusRecoveryBatch **custody)
{
	return census_recovery_arbitrate_all(custody, true);
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
	census->owned_root_fd = -1;
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

ClusterRelmapCensusResult
cluster_relmap_retention_census_begin_episode(
	const ClusterRecoveryRelmapSourceV1 *source, ClusterRecoveryRootSet *roots,
	ClusterRelmapRetentionDirectory *const directories[], size_t count,
	size_t memory_budget, ClusterRelmapRetentionCensus **out)
{
	ClusterRecoveryRelmapSourceV1 original;
	ClusterRelmapRetentionCensus *volatile fresh = NULL;
	CensusEpisode *volatile episode = NULL;
	volatile ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_INVALID;
	volatile bool accepted = false;

	if (out == NULL || source == NULL
		|| census_recovery_overlap(out, sizeof(*out), source, sizeof(*source))
		|| census_recovery_overlap(out, sizeof(*out), roots, 1)
		|| (directories != NULL && count <= CLUSTER_RELMAP_RETENTION_MAX_ORIGINS
			&& census_recovery_overlap(out, sizeof(*out), directories,
				count * sizeof(*directories))))
		return result;
	if (directories != NULL && count <= CLUSTER_RELMAP_RETENTION_MAX_ORIGINS)
		for (size_t i = 0; i < count; i++)
			if (census_recovery_overlap(out, sizeof(*out), directories[i], sizeof(*directories[i])))
				return result;
	if (*out != NULL || MyBackendType != B_STARTUP || CritSectionCount != 0)
		return result;
	memcpy(&original, source, sizeof(original));
	PG_TRY();
	{
		do {
			bool origins[CLUSTER_RELMAP_RETENTION_MAX_ORIGINS] = {false};
			uint32 sources;
			size_t bytes, origin_count = 0;
			const ClusterControlRootIdentity *identity = &original.selected.source.claim.identity;

			if (!census_open_context()
				|| !cluster_wal_inputs_relmap_source_current_v1(&original)
				|| (sources = cluster_wal_inputs_relmap_count_v1(original.scope)) == 0
				|| sources > CLUSTER_WAL_INPUTS_MAX || directories == NULL || count == 0
				|| count > CLUSTER_RELMAP_RETENTION_MAX_ORIGINS)
				break;
			bytes = sizeof(*episode) + (size_t)sources * sizeof(episode->sources[0]);
			if (memory_budget < bytes || memory_budget - bytes < sizeof(*fresh)) {
				result = CLUSTER_RELMAP_CENSUS_CAPACITY;
				break;
			}
			episode = calloc(1, bytes);
			if (episode == NULL) {
				result = CLUSTER_RELMAP_CENSUS_CAPACITY;
				break;
			}
			episode->count = sources;
			for (uint32 i = 0; i < sources; i++) {
				ClusterRecoveryRelmapSourceV1 *saved = &episode->sources[i];
				const ClusterControlRootIdentity *id;

				*saved = cluster_wal_inputs_relmap_source_v1(original.scope, i);
				id = &saved->selected.source.claim.identity;
				if (!cluster_wal_inputs_relmap_source_current_v1(saved)
					|| saved->scope != original.scope || saved->input_index != i
					|| saved->input_count != sources || id->origin_node_id < 0
					|| id->origin_node_id >= CLUSTER_RELMAP_RETENTION_MAX_ORIGINS
					|| id->system_identifier != identity->system_identifier
					|| saved->selected.source.claim.database_incarnation
						!= original.selected.source.claim.database_incarnation
					|| memcmp(id->storage_uuid, identity->storage_uuid, 16) != 0)
					break;
				if (!origins[id->origin_node_id]) {
					origins[id->origin_node_id] = true;
					origin_count++;
				}
				if (i + 1 == sources)
					result = CLUSTER_RELMAP_CENSUS_MORE;
			}
			if (result != CLUSTER_RELMAP_CENSUS_MORE || count != origin_count)
				break;
			result = CLUSTER_RELMAP_CENSUS_INVALID;
			for (size_t i = 0; i < count; i++) {
				if (directories[i] == NULL
					|| directories[i]->origin >= CLUSTER_RELMAP_RETENTION_MAX_ORIGINS
					|| !origins[directories[i]->origin])
					break;
				/* Also rejects a duplicate directory instead of losing another origin. */
				origins[directories[i]->origin] = false;
				if (i + 1 == count)
					result = CLUSTER_RELMAP_CENSUS_MORE;
			}
			if (result != CLUSTER_RELMAP_CENSUS_MORE)
				break;
			result = cluster_relmap_retention_census_begin(roots, identity->system_identifier,
				original.selected.source.claim.database_incarnation, identity->storage_uuid,
				directories, count, memory_budget - bytes,
				(ClusterRelmapRetentionCensus **)&fresh);
			if (result != CLUSTER_RELMAP_CENSUS_MORE)
				break;
			fresh->episode = (CensusEpisode *)episode;
			episode = NULL;
			if (!census_episode_current((ClusterRelmapRetentionCensus *)fresh)
				|| !cluster_wal_inputs_relmap_source_current_v1(&original))
				break;
			*out = (ClusterRelmapRetentionCensus *)fresh;
			fresh = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (!accepted) {
			if (result <= CLUSTER_RELMAP_CENSUS_DRAINED)
				result = CLUSTER_RELMAP_CENSUS_CHANGED;
			census_input_failed(roots, directories, count, result);
			cluster_wal_inputs_relmap_source_fail_v1(&original);
		}
		cluster_relmap_retention_census_destroy((ClusterRelmapRetentionCensus *)fresh);
		free((CensusEpisode *)episode);
	}
	PG_END_TRY();
	return result;
}

/* The temporary source snapshot precedes all producer I/O. It is not a
 * native authority carrier and is never refreshed after a checkpoint visit. */
static bool
census_native_episode_current(const CensusEpisode *saved)
{
	if (!census_open_context() || saved->count == 0
		|| cluster_wal_inputs_relmap_count_v1(saved->sources[0].scope) != saved->count)
		return false;
	for (uint32 i = 0; i < saved->count; i++)
		if (!cluster_wal_inputs_relmap_source_current_v1(&saved->sources[i]))
			return false;
	return census_open_context()
		&& cluster_wal_inputs_relmap_count_v1(saved->sources[0].scope) == saved->count;
}

ClusterRelmapCensusResult
cluster_relmap_retention_census_begin_native_episode(
	const ClusterRecoveryRelmapSourceV1 *source, ClusterRecoveryRootSet *roots,
	size_t memory_budget, ClusterRelmapRetentionCensus **out)
{
	ClusterRecoveryRelmapSourceV1 original;
	ClusterRelmapRetentionCensus *volatile fresh = NULL;
	ClusterRelmapRetentionDirectory *volatile directories = NULL;
	CensusEpisode *volatile saved = NULL;
	volatile ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_INVALID;
	volatile bool accepted = false;

	if (out == NULL || source == NULL
		|| census_recovery_overlap(out, sizeof(*out), source, sizeof(*source))
		|| census_recovery_overlap(out, sizeof(*out), roots, 1)
		|| *out != NULL || MyBackendType != B_STARTUP || CritSectionCount != 0)
		return result;
	memcpy(&original, source, sizeof(original));
	PG_TRY();
	{
		do {
			ClusterRelmapRetentionDirectory *pointers[CLUSTER_RELMAP_RETENTION_MAX_ORIGINS];
			bool origins[CLUSTER_RELMAP_RETENTION_MAX_ORIGINS] = {false};
			const ClusterControlRootIdentity *identity = &original.selected.source.claim.identity;
			uint32 count;
			size_t source_bytes, directory_bytes;
			ClusterRecoveryRootsResult root_result;

			if (!census_open_context()
				|| !cluster_wal_inputs_relmap_source_current_v1(&original)
				|| (count = cluster_wal_inputs_relmap_count_v1(original.scope)) == 0
				|| count > CLUSTER_WAL_INPUTS_MAX
				|| count > CLUSTER_RELMAP_RETENTION_MAX_ORIGINS || original.work_generation == 0
				|| original.replay_count != count)
				break;
			/* Bound the peak: directories, temporary saved sources, and the
			 * original cursor's second saved set coexist until publication. */
			source_bytes = sizeof(*saved) + (size_t)count * sizeof(saved->sources[0]);
			directory_bytes = (size_t)count * sizeof(*directories);
			if (memory_budget < directory_bytes + 2 * source_bytes + sizeof(*fresh)) {
				result = CLUSTER_RELMAP_CENSUS_CAPACITY;
				break;
			}
			saved = calloc(1, source_bytes);
			directories = calloc(count, sizeof(*directories));
			if (saved == NULL || directories == NULL) {
				result = CLUSTER_RELMAP_CENSUS_CAPACITY;
				break;
			}
			saved->count = count;
			for (uint32 i = 0; i < count; i++) {
				ClusterRecoveryRelmapSourceV1 *entry = &saved->sources[i];
				const ClusterControlRootIdentity *id;

				*entry = cluster_wal_inputs_relmap_source_v1(original.scope, i);
				id = &entry->selected.source.claim.identity;
				if (!cluster_wal_inputs_relmap_source_current_v1(entry)
					|| entry->scope != original.scope || entry->input_index != i
					|| entry->input_count != count || entry->replay_count != count
					|| entry->work_generation != original.work_generation
					|| entry->selected.kind != CLUSTER_WAL_INPUT_CHECKPOINT
					|| id->origin_node_id < 0
					|| id->origin_node_id >= CLUSTER_RELMAP_RETENTION_MAX_ORIGINS
					|| origins[id->origin_node_id]
					|| id->system_identifier != identity->system_identifier
					|| entry->selected.source.claim.database_incarnation
						!= original.selected.source.claim.database_incarnation
					|| memcmp(id->storage_uuid, identity->storage_uuid, 16) != 0)
					break;
				origins[id->origin_node_id] = true;
				if (i + 1 == count)
					result = CLUSTER_RELMAP_CENSUS_MORE;
			}
			if (result != CLUSTER_RELMAP_CENSUS_MORE)
				break;
			result = CLUSTER_RELMAP_CENSUS_CHANGED;
			for (uint32 i = 0; i < count; i++) {
				ClusterRecoveryRelmapNativeInputV1 native;
				const ClusterRelmapCheckpointWindow *window = &native.window;

				if (!census_native_episode_current((CensusEpisode *)saved))
					break;
				native = cluster_wal_inputs_relmap_native_v1(original.scope, i);
				if (!census_native_episode_current((CensusEpisode *)saved)
					|| memcmp(&native.source, &saved->sources[i], sizeof(native.source)) != 0
					|| window->system_identifier != identity->system_identifier
					|| window->database_incarnation != original.selected.source.claim.database_incarnation
					|| window->origin != saved->sources[i].selected.source.claim.identity.origin_node_id
					|| window->native_floor > window->last_issued
					|| window->last_issued > native.post_flush_issued
					|| native.post_flush_issued - window->native_floor >= UINT64CONST(0x80000000)
					|| cluster_relmap_retention_build_start(&directories[i], window->system_identifier,
						window->database_incarnation, window->origin, original.work_generation,
						window->native_floor, native.post_flush_issued) != CLUSTER_RELMAP_RETENTION_OK)
					break;
				pointers[i] = &directories[i];
				if (i + 1 == count)
					result = CLUSTER_RELMAP_CENSUS_MORE;
			}
			if (result != CLUSTER_RELMAP_CENSUS_MORE)
				break;
			result = cluster_relmap_retention_census_begin_episode(&original, roots, pointers, count,
				memory_budget - source_bytes - directory_bytes,
				(ClusterRelmapRetentionCensus **)&fresh);
			if (result != CLUSTER_RELMAP_CENSUS_MORE)
				break;
			/* One contiguous block, freed by the original local destructor. */
			fresh->owned_directory = (ClusterRelmapRetentionDirectory *)directories;
			directories = NULL;
			root_result = cluster_recovery_root_set_recheck_begin(roots);
			while (root_result == CLUSTER_RECOVERY_ROOTS_MORE
				|| root_result == CLUSTER_RECOVERY_ROOTS_ENTRY || root_result == CLUSTER_RECOVERY_ROOTS_BYTES) {
				ClusterRecoveryRootsEvent event;
				CHECK_FOR_INTERRUPTS();
				root_result = cluster_recovery_root_set_step(roots, &event);
			}
			if (root_result != CLUSTER_RECOVERY_ROOTS_OBSERVED
				|| !census_native_episode_current((CensusEpisode *)saved)
				|| !census_recovery_current((ClusterRelmapRetentionCensus *)fresh))
				break;
			*out = (ClusterRelmapRetentionCensus *)fresh;
			fresh = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (!accepted) {
			if (result <= CLUSTER_RELMAP_CENSUS_DRAINED)
				result = CLUSTER_RELMAP_CENSUS_CHANGED;
			cluster_recovery_root_set_invalidate(roots);
			cluster_wal_inputs_relmap_source_fail_v1(&original);
		}
		cluster_relmap_retention_census_destroy((ClusterRelmapRetentionCensus *)fresh);
		free((ClusterRelmapRetentionDirectory *)directories);
		free((CensusEpisode *)saved);
	}
	PG_END_TRY();
	return result;
}

typedef struct CensusNativeRoot {
	CensusEpisode *saved;
	ClusterRecoveryRootSet *roots;
	size_t payload_budget;
	uint32 source_index, completed;
	uint64 records, source_records;
	bool active;
	bool seen[CLUSTER_RELMAP_RETENTION_MAX_ORIGINS];
	RfContributorStreamCutV1 cut;
} CensusNativeRoot;

static bool
census_native_root_source_same(const ClusterWalSourceRef *left, const ClusterWalSourceRef *right)
{
	/* Compare the complete fixed claim, excluding source's trailing padding. */
	return left->timeline == right->timeline
		&& memcmp(&left->claim, &right->claim, sizeof(left->claim)) == 0;
}

static RfPageProofDetailV1
census_native_root_source(const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut,
	const ClusterWalTailObservation *completed, void *arg)
{
	CensusNativeRoot *work = arg;
	uint32 index;

	if (source == NULL || cut == NULL || !census_native_episode_current(work->saved)
		|| cut->flags != RF_CONTRIBUTOR_CUT_COMPLETE
		|| cut->failed_thread != source->claim.identity.origin_thread_id
		|| cut->timeline_id != source->timeline
		|| cut->origin_owner_incarnation != source->claim.identity.origin_owner_incarnation
		|| cut->scan_begin_inclusive == InvalidXLogRecPtr
		|| cut->scan_end_exclusive <= cut->scan_begin_inclusive)
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	if (completed == NULL) {
		if (work->active)
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		/* The original cold census sorts full sources independently of
		 * selected-input order. Match the complete generation, never ordinal. */
		for (index = 0; index < work->saved->count; index++)
			if (census_native_root_source_same(source, &work->saved->sources[index].selected.source))
				break;
		if (index == work->saved->count || work->seen[index])
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		work->source_index = index;
		work->cut = *cut;
		work->source_records = 0;
		work->active = true;
	} else {
		if (!work->active || completed->records != work->source_records
			|| !census_native_root_source_same(source,
				&work->saved->sources[work->source_index].selected.source)
			|| memcmp(cut, &work->cut, sizeof(*cut)) != 0)
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		work->seen[work->source_index] = true;
		work->completed++;
		work->active = false;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
census_native_root_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	CensusNativeRoot *work = arg;
	ClusterRecoveryWalTargetsDiag diag;

	if (!work->active || source == NULL || cut == NULL
		|| work->records == UINT64_MAX || work->source_records == UINT64_MAX
		|| !census_native_episode_current(work->saved)
		|| !census_native_root_source_same(source,
			&work->saved->sources[work->source_index].selected.source)
		|| memcmp(cut, &work->cut, sizeof(*cut)) != 0
		|| !cluster_recovery_wal_targets_collect(work->roots, reader, source, cut,
			work->payload_budget, &diag)
		|| !census_native_episode_current(work->saved))
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	work->records++;
	work->source_records++;
	return RF_PAGE_PROOF_DETAIL_OK;
}

ClusterRelmapCensusResult
cluster_relmap_retention_census_open_native_episode(
	const ClusterRecoveryRelmapSourceV1 *source, size_t root_budget,
	size_t memory_budget, size_t payload_budget, ClusterRelmapRetentionCensus **out)
{
	ClusterRecoveryRelmapSourceV1 original;
	ClusterRecoveryRootSet *volatile roots = NULL;
	ClusterRelmapRetentionCensus *volatile fresh = NULL;
	CensusEpisode *volatile saved = NULL;
	volatile ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_INVALID;
	volatile bool accepted = false;

	if (out == NULL || source == NULL
		|| census_recovery_overlap(out, sizeof(*out), source, sizeof(*source))
		|| *out != NULL || MyBackendType != B_STARTUP || CritSectionCount != 0)
		return result;
	memcpy(&original, source, sizeof(original));
	PG_TRY();
	{
		do {
			CensusNativeRoot work = {0};
			bool origins[CLUSTER_RELMAP_RETENTION_MAX_ORIGINS] = {false};
			const ClusterControlRootIdentity *identity = &original.selected.source.claim.identity;
			ClusterRecoveryRootsResult root_result;
			RfPageProofDetailV1 detail;
			uint64 records = 0;
			uint32 count;
			size_t bytes;
			int fd = -1, current_fd = -1;

			if (!census_open_context() || root_budget == 0 || payload_budget == 0
				|| !cluster_wal_inputs_relmap_source_current_v1(&original)
				|| (count = cluster_wal_inputs_relmap_count_v1(original.scope)) == 0
				|| count > CLUSTER_WAL_INPUTS_MAX || count > CLUSTER_RELMAP_RETENTION_MAX_ORIGINS
				|| original.work_generation == 0 || original.replay_count != count)
				break;
			bytes = sizeof(*saved) + (size_t)count * sizeof(saved->sources[0]);
			/* This saved set coexists with both snapshots/directories in the
			 * original native-input factory. ROOT/payload have separate budgets. */
			if (memory_budget < 3 * bytes + (size_t)count * sizeof(ClusterRelmapRetentionDirectory)
				+ sizeof(*fresh) || (saved = calloc(1, bytes)) == NULL) {
				result = CLUSTER_RELMAP_CENSUS_CAPACITY;
				break;
			}
			saved->count = count;
			for (uint32 i = 0; i < count; i++) {
				ClusterRecoveryRelmapSourceV1 *entry = &saved->sources[i];
				const ClusterControlRootIdentity *id;

				*entry = cluster_wal_inputs_relmap_source_v1(original.scope, i);
				id = &entry->selected.source.claim.identity;
				if (!cluster_wal_inputs_relmap_source_current_v1(entry)
					|| entry->scope != original.scope || entry->input_index != i
					|| entry->input_count != count || entry->replay_count != count
					|| entry->work_generation != original.work_generation
					|| entry->selected.kind != CLUSTER_WAL_INPUT_CHECKPOINT
					|| id->origin_node_id < 0 || id->origin_node_id >= lengthof(origins)
					|| origins[id->origin_node_id]
					|| id->system_identifier != identity->system_identifier
					|| entry->selected.source.claim.database_incarnation
						!= original.selected.source.claim.database_incarnation
					|| memcmp(id->storage_uuid, identity->storage_uuid, 16) != 0)
					break;
				origins[id->origin_node_id] = true;
				if (i + 1 == count) result = CLUSTER_RELMAP_CENSUS_MORE;
			}
			if (result != CLUSTER_RELMAP_CENSUS_MORE)
				break;
			result = CLUSTER_RELMAP_CENSUS_UNAVAILABLE;
			if (!census_native_episode_current((CensusEpisode *)saved)
				|| cluster_wal_inputs_ctrc_namespace_v1(original.scope, &fd)
					!= CLUSTER_CONTROL_ROOT_OK_PRIMARY || fd < 0
				|| !census_native_episode_current((CensusEpisode *)saved))
				break;
			root_result = cluster_recovery_root_set_begin(fd, identity->system_identifier,
				original.selected.source.claim.database_incarnation, identity->storage_uuid,
				root_budget, (ClusterRecoveryRootSet **)&roots);
			if (root_result != CLUSTER_RECOVERY_ROOTS_MORE) {
				if (root_result == CLUSTER_RECOVERY_ROOTS_CAPACITY) result = CLUSTER_RELMAP_CENSUS_CAPACITY;
				break;
			}
			work.saved = (CensusEpisode *)saved;
			work.roots = (ClusterRecoveryRootSet *)roots;
			work.payload_budget = payload_budget;
			if (cluster_wal_inputs_cold_census_v1(original.scope, census_native_root_record,
				census_native_root_source, &work, &records, &detail) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				|| work.active || work.completed != count || work.records != records
				|| !census_native_episode_current((CensusEpisode *)saved))
				break;
			/* Preserve ALL retained targets before original physical discovery,
			 * including missing targets. Non-file/other effects stay owner duties. */
			do {
				ClusterRecoveryRootsEvent event;
				CHECK_FOR_INTERRUPTS();
				if (!census_native_episode_current((CensusEpisode *)saved)) break;
				root_result = cluster_recovery_root_set_step((ClusterRecoveryRootSet *)roots, &event);
				if (!census_native_episode_current((CensusEpisode *)saved)) break;
			} while (root_result == CLUSTER_RECOVERY_ROOTS_MORE
				|| root_result == CLUSTER_RECOVERY_ROOTS_ENTRY || root_result == CLUSTER_RECOVERY_ROOTS_BYTES);
			if (root_result != CLUSTER_RECOVERY_ROOTS_OBSERVED
				|| !census_native_episode_current((CensusEpisode *)saved))
				break;
			result = cluster_relmap_retention_census_begin_native_episode(&original,
				(ClusterRecoveryRootSet *)roots, memory_budget - bytes,
				(ClusterRelmapRetentionCensus **)&fresh);
			if (result != CLUSTER_RELMAP_CENSUS_MORE)
				break;
			fresh->owns_roots = true;
			roots = NULL;
			/* Scope alone owns/closes fd. Compare its SAME original namespace
			 * after all producer I/O; never replace or adopt the descriptor. */
			if (cluster_wal_inputs_ctrc_namespace_v1(original.scope, &current_fd)
					!= CLUSTER_CONTROL_ROOT_OK_PRIMARY || current_fd != fd
				|| !census_native_episode_current((CensusEpisode *)saved)
				|| !census_recovery_current((ClusterRelmapRetentionCensus *)fresh))
				break;
			*out = (ClusterRelmapRetentionCensus *)fresh;
			fresh = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (!accepted) {
			if (result <= CLUSTER_RELMAP_CENSUS_DRAINED) result = CLUSTER_RELMAP_CENSUS_CHANGED;
			cluster_recovery_root_set_invalidate((ClusterRecoveryRootSet *)roots);
			cluster_wal_inputs_relmap_source_fail_v1(&original);
		}
		cluster_relmap_retention_census_destroy((ClusterRelmapRetentionCensus *)fresh);
		cluster_recovery_root_set_destroy((ClusterRecoveryRootSet *)roots);
		free((CensusEpisode *)saved);
	}
	PG_END_TRY();
	return result;
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

static ClusterRelmapCensusResult
census_next_committed(ClusterRelmapRetentionCensus *census,
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
		/* Separate from the last physical I/O tick. Produced cold cursors
		 * recheck original source/protection, which can perform CF I/O. */
		if (!census_recovery_current(census))
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
cluster_relmap_retention_census_next_committed(ClusterRelmapRetentionCensus *census,
	ClusterRelmapCensusCommitted *out)
{
	ClusterRelmapCensusResult result;

	if (census == NULL || census->episode == NULL
		|| census->result > CLUSTER_RELMAP_CENSUS_DRAINED)
		return census_next_committed(census, out);
	PG_TRY();
	{
		result = census_episode_current(census)
			? census_next_committed(census, out)
			: census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		if (result <= CLUSTER_RELMAP_CENSUS_DRAINED && !census_episode_current(census))
			result = census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
	}
	PG_CATCH();
	{
		census_failed(census, CLUSTER_RELMAP_CENSUS_CHANGED);
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result > CLUSTER_RELMAP_CENSUS_DRAINED && out != NULL)
		memset(out, 0, sizeof(*out));
	return result;
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
	if (!census_episode_validate(census) || !census_root_same(census)) {
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

static bool
census_execute_drain(ClusterRelmapRetentionCensus *census)
{
	ClusterRelmapCensusResult result;

	do {
		CHECK_FOR_INTERRUPTS();
		result = cluster_relmap_retention_census_next(census);
	} while (result == CLUSTER_RELMAP_CENSUS_MORE);
	return result == CLUSTER_RELMAP_CENSUS_DRAINED && census_recovery_current(census);
}

static bool
census_execute_pending(const ClusterRelmapRetentionCensus *census)
{
	for (size_t o = 0; o < census->origin_count; o++) {
		const ClusterRelmapRetentionDirectory *directory
			= census->origins[census->nodes[o]].directory;

		for (size_t i = 0; i < lengthof(directory->pins); i++)
			if (directory->pins[i].occupied)
				return true;
	}
	return false;
}

bool
cluster_relmap_retention_census_execute_native_episode(
	const ClusterRecoveryRelmapSourceV1 *source, size_t root_budget,
	size_t memory_budget, size_t payload_budget, ClusterRelmapRetentionCensus **out)
{
	ClusterRecoveryRelmapSourceV1 original;
	ClusterRelmapRetentionCensus *volatile fresh = NULL;
	volatile bool accepted = false;

	if (out == NULL || source == NULL
		|| census_recovery_overlap(out, sizeof(*out), source, sizeof(*source))
		|| *out != NULL || MyBackendType != B_STARTUP || CritSectionCount != 0)
		return false;
	memcpy(&original, source, sizeof(original));
	PG_TRY();
	{
		if (cluster_relmap_retention_census_open_native_episode(&original, root_budget,
			memory_budget, payload_budget, (ClusterRelmapRetentionCensus **)&fresh)
				== CLUSTER_RELMAP_CENSUS_MORE
			&& census_execute_drain((ClusterRelmapRetentionCensus *)fresh)
			&& !census_execute_pending((ClusterRelmapRetentionCensus *)fresh)
			&& census_recovery_current((ClusterRelmapRetentionCensus *)fresh)) {
			/* Only a complete clean local observation crosses this boundary.
			 * Pending arbitration and joint completion retain their owners. */
			*out = (ClusterRelmapRetentionCensus *)fresh;
			fresh = NULL;
			accepted = true;
		}
	}
	PG_FINALLY();
	{
		if (!accepted) {
			if (fresh != NULL)
				census_failed((ClusterRelmapRetentionCensus *)fresh,
					CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
			else
				cluster_wal_inputs_relmap_source_fail_v1(&original);
		}
		cluster_relmap_retention_census_destroy((ClusterRelmapRetentionCensus *)fresh);
	}
	PG_END_TRY();
	return accepted;
}

bool
cluster_relmap_retention_census_committed_cold(ClusterRelmapRetentionCensus *census, Oid dbid,
											   ClusterRelmapCensusCommitted *out)
{
	volatile bool accepted = false;

	if (out == NULL)
		return false;
	for (const ClusterRelmapRetentionCensus *current = census; current != NULL;
		 current = current->cold_parent)
		if (census_recovery_output_overlap(current, out, sizeof(*out)))
			return false;
	memset(out, 0, sizeof(*out));
	if (census == NULL || census->result != CLUSTER_RELMAP_CENSUS_DRAINED
		|| !census->cold_input_bound || census->origin_count != 1 || !census_open_context())
		return false;
	PG_TRY();
	{
		do {
			ClusterRelmapCensusRecovery pair = { 0 };
			ClusterRelmapCensusCommitted committed = { 0 };
			uint8 saved_pins[32], current_pins[32];
			ClusterRecoveryRootsResult result;
			ClusterRecoveryRootsEvent event;

			if (!census->checked || !census_recovery_current(census)
				|| census_execute_pending(census)
				|| !census_recovery_pins_digest(census, saved_pins)
				|| !census_recovery_pair(census, dbid, &pair) || pair.pending.has_pending)
				break;
			/* A selected clean pair cannot hide drift in another namespace,
			 * including new physical files with no original WAL reference. */
			result = cluster_recovery_root_set_recheck_begin(census->roots);
			while (result < CLUSTER_RECOVERY_ROOTS_OBSERVED) {
				CHECK_FOR_INTERRUPTS();
				if (!census_open_context())
					break;
				result = cluster_recovery_root_set_step(census->roots, &event);
			}
			if (result != CLUSTER_RECOVERY_ROOTS_OBSERVED || !census_recovery_current(census)
				|| !census_recovery_pins_digest(census, current_pins)
				|| memcmp(saved_pins, current_pins, sizeof(saved_pins)) != 0
				|| census_execute_pending(census) || !census_open_context())
				break;
			committed.identity = pair.identity;
			committed.generation = pair.pending.committed_generation;
			committed.map = pair.pending.committed;
			committed.present = true;
			*out = committed;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		if (!accepted)
			census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
	}
	PG_END_TRY();
	return accepted;
}

bool
cluster_relmap_retention_census_execute_cold(uint16 origin, size_t root_budget,
											 size_t memory_budget, size_t payload_budget,
											 ClusterRelmapRetentionCensus **out)
{
	const size_t fixed
		= sizeof(ClusterRelmapRetentionCensus) + sizeof(ClusterRelmapRetentionDirectory);
	ClusterRelmapRetentionCensus *volatile original = NULL, *volatile fresh = NULL;
	ClusterRelmapCensusRecoveryBatch *volatile batch = NULL;
	volatile bool accepted = false;

	if (out == NULL || *out != NULL || memory_budget / 2 < fixed || root_budget / 2 == 0
		|| !census_open_context())
		return false;
	PG_TRY();
	{
		do {
			if (!cluster_relmap_retention_census_open_cold(
					origin, root_budget / 2, fixed, payload_budget,
					(ClusterRelmapRetentionCensus **)&original)
				|| !census_execute_drain((ClusterRelmapRetentionCensus *)original))
				break;
			if (!census_execute_pending((ClusterRelmapRetentionCensus *)original)) {
				*out = (ClusterRelmapRetentionCensus *)original;
				original = NULL;
				accepted = true;
				break;
			}
			if (!cluster_relmap_retention_census_recovery_prepare_all_cold(
					(ClusterRelmapRetentionCensus *)original, memory_budget - fixed,
					(ClusterRelmapCensusRecoveryBatch **)&batch)
				|| cluster_relmap_retention_census_recovery_arbitrate_all_cold(
					   (ClusterRelmapCensusRecoveryBatch **)&batch)
					   != CLUSTER_RELMAP_V2_CLEARED_POSTREAD
				|| !cluster_relmap_retention_census_restart_cold(
					(ClusterRelmapRetentionCensus *)original, root_budget / 2,
					memory_budget - fixed, (ClusterRelmapRetentionCensus **)&fresh)
				|| !census_execute_drain((ClusterRelmapRetentionCensus *)fresh)
				|| census_execute_pending((ClusterRelmapRetentionCensus *)fresh))
				break;
			/* Ordinary restart borrows its parent. This original is local to
			 * this call, so transfer it only after the final complete scan. */
			fresh->owns_cold_parent = true;
			original = NULL;
			*out = (ClusterRelmapRetentionCensus *)fresh;
			fresh = NULL;
			accepted = true;
		} while (false);
	}
	PG_FINALLY();
	{
		cluster_relmap_retention_census_recovery_batch_release(
			(ClusterRelmapCensusRecoveryBatch **)&batch);
		cluster_relmap_retention_census_destroy((ClusterRelmapRetentionCensus *)fresh);
		cluster_relmap_retention_census_destroy((ClusterRelmapRetentionCensus *)original);
	}
	PG_END_TRY();
	return accepted;
}

void
cluster_relmap_retention_census_destroy(ClusterRelmapRetentionCensus *census)
{
	while (census != NULL) {
		ClusterRelmapRetentionCensus *parent
			= census->owns_cold_parent ? (ClusterRelmapRetentionCensus *)census->cold_parent : NULL;

		if (census->result == CLUSTER_RELMAP_CENSUS_MORE)
			census_failed(census, CLUSTER_RELMAP_CENSUS_UNAVAILABLE);
		if (census->owned_directory != NULL) {
			if (census->owns_roots)
				cluster_recovery_root_set_destroy(census->roots);
			if (census->owned_root_fd >= 0)
				(void)CloseTransientFile(census->owned_root_fd);
			free(census->owned_directory);
		}
		free(census->episode);
		free(census);
		census = parent;
	}
}
#endif
