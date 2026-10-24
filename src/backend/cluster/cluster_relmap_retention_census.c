/* PGRAC: relmap pending census through the original cold ROOT reader.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_retention_census.h"

#ifdef USE_PGRAC_CLUSTER
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
census_origin_same(const ClusterRelmapRetentionCensus *census, uint16 node)
{
	const CensusOrigin *saved = &census->origins[node];
	const ClusterRelmapRetentionDirectory *directory = saved->directory;

	return directory != NULL && directory->state == CLUSTER_RELMAP_RETENTION_BUILDING
		&& directory->system_identifier == census->system_identifier
		&& directory->database_incarnation == census->database_incarnation
		&& directory->origin == node && directory->generation == saved->generation
		&& directory->native_floor == saved->native_floor
		&& directory->last_issued == saved->last_issued && directory->truncate_serial == 0
		&& directory->inflight.generation == 0 && directory->inflight.serial == 0
		&& directory->inflight.cutoff == 0;
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
