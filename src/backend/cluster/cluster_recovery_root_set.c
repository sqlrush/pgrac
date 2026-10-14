/*-------------------------------------------------------------------------
 * cluster_recovery_root_set.c
 *   Stable candidate roots for the cold recovery obligation graph.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <unistd.h>
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_recovery_root_set.h"
#include "common/cryptohash.h"

#ifdef USE_PGRAC_CLUSTER

#define ROOTS_MAX_ENTRIES 65536
#define ROOTS_TICK 128

struct ClusterRecoveryRootSet {
	int root_fd;
	struct stat root_stat;
	uint64 system_identifier, database_incarnation;
	uint8 storage_uuid[16], digest[32];
	ClusterRecoveryNamespace *scan;
	size_t scan_budget;
	ClusterRecoveryRootEntry *entries;
	uint32 *buckets, *order, *merge;
	bool *verified;
	uint32 capacity, bucket_count, count, current;
	uint32 verify_at, width, left, mid, right, a, b, pos, hash_at;
	uint64 offset;
	pg_cryptohash_ctx *file_hash, *set_hash;
	uint8 bytes[BLCKSZ];
	unsigned pass, stage;
	bool started, reading, merging;
	ClusterRecoveryRootsResult status;
};

static ClusterRecoveryRootsResult
roots_fail(ClusterRecoveryRootSet *roots, ClusterRecoveryRootsResult result)
{
	if (roots != NULL)
		roots->status = result;
	return result;
}

static ClusterRecoveryRootsResult
roots_namespace_result(ClusterRecoveryNamespaceResult result)
{
	switch (result) {
	case CLUSTER_RECOVERY_NAMESPACE_CHANGED:
		return CLUSTER_RECOVERY_ROOTS_CHANGED;
	case CLUSTER_RECOVERY_NAMESPACE_UNSAFE:
		return CLUSTER_RECOVERY_ROOTS_UNSAFE;
	case CLUSTER_RECOVERY_NAMESPACE_CAPACITY:
		return CLUSTER_RECOVERY_ROOTS_CAPACITY;
	case CLUSTER_RECOVERY_NAMESPACE_IO:
		return CLUSTER_RECOVERY_ROOTS_IO;
	default:
		return CLUSTER_RECOVERY_ROOTS_INVALID;
	}
}

static bool
roots_hash_number(pg_cryptohash_ctx *hash, uint64 value, unsigned width)
{
	uint8 bytes[8];

	for (unsigned i = 0; i < width; i++)
		bytes[i] = value >> (8 * i);
	return pg_cryptohash_update(hash, bytes, width) >= 0;
}

/* FNV only selects an in-memory bucket; exact paths decide identity. */
static bool
roots_lookup(ClusterRecoveryRootSet *roots, const char *path, bool create, uint32 *index)
{
	uint64 hash = UINT64CONST(14695981039346656037);
	uint32 bucket;

	for (const unsigned char *p = (const unsigned char *)path; *p; p++) {
		hash ^= *p;
		hash *= UINT64CONST(1099511628211);
	}
	bucket = (uint32)hash & (roots->bucket_count - 1);
	while (roots->buckets[bucket] != 0) {
		*index = roots->buckets[bucket] - 1;
		if (strcmp(roots->entries[*index].file.path, path) == 0)
			return true;
		bucket = (bucket + 1) & (roots->bucket_count - 1);
	}
	if (!create)
		return false;
	if (roots->count == roots->capacity) {
		roots_fail(roots, CLUSTER_RECOVERY_ROOTS_CAPACITY);
		return false;
	}
	*index = roots->count++;
	strlcpy(roots->entries[*index].file.path, path, MAXPGPATH);
	roots->buckets[bucket] = *index + 1;
	return true;
}

ClusterRecoveryRootsResult
cluster_recovery_root_set_begin(int root_fd, uint64 system_identifier, uint64 database_incarnation,
								const uint8 storage_uuid[16], size_t memory_budget,
								ClusterRecoveryRootSet **out)
{
	ClusterRecoveryRootSet *roots;
	pg_cryptohash_ctx *file_hash, *set_hash;
	ClusterRecoveryNamespaceResult result;
	struct stat observed;
	size_t capacity, auxiliary_budget;
	uint32 buckets = 2;
	uint8 nonzero = 0;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || root_fd < 0 || system_identifier == 0 || database_incarnation == 0
		|| storage_uuid == NULL)
		return CLUSTER_RECOVERY_ROOTS_INVALID;
	for (unsigned i = 0; i < 16; i++)
		nonzero |= storage_uuid[i];
	if (nonzero == 0)
		return CLUSTER_RECOVERY_ROOTS_INVALID;
	if (fstat(root_fd, &observed) != 0)
		return CLUSTER_RECOVERY_ROOTS_IO;
	if (!S_ISDIR(observed.st_mode))
		return CLUSTER_RECOVERY_ROOTS_UNSAFE;
	/* Directory streams have their own enforced sub-budget; the remainder
	 * reserves temporary crypto/allocation overhead. Entries and index/sort
	 * arrays below are explicit. This bounds owned storage, not process RSS. */
	auxiliary_budget = Max((size_t)256 * 1024, memory_budget / 4);
	if (memory_budget <= sizeof(*roots) + auxiliary_budget)
		return CLUSTER_RECOVERY_ROOTS_CAPACITY;
	capacity = (memory_budget - sizeof(*roots) - auxiliary_budget)
			   / (sizeof(ClusterRecoveryRootEntry) + 6 * sizeof(uint32) + sizeof(bool));
	if (capacity == 0)
		return CLUSTER_RECOVERY_ROOTS_CAPACITY;
	capacity = Min(capacity, ROOTS_MAX_ENTRIES);
	while (buckets < 2 * capacity)
		buckets *= 2;
	/* Backend crypto allocation may ERROR: create it before malloc-owned
	 * storage so the original ResourceOwner can clean an allocation failure. */
	file_hash = pg_cryptohash_create(PG_SHA256);
	set_hash = pg_cryptohash_create(PG_SHA256);
	if (file_hash == NULL || set_hash == NULL) {
		pg_cryptohash_free(file_hash);
		pg_cryptohash_free(set_hash);
		return CLUSTER_RECOVERY_ROOTS_CAPACITY;
	}
	roots = calloc(1, sizeof(*roots));
	if (roots == NULL) {
		pg_cryptohash_free(file_hash);
		pg_cryptohash_free(set_hash);
		return CLUSTER_RECOVERY_ROOTS_CAPACITY;
	}
	roots->file_hash = file_hash;
	roots->set_hash = set_hash;
	roots->entries = calloc(capacity, sizeof(*roots->entries));
	roots->buckets = calloc(buckets, sizeof(uint32));
	roots->order = calloc(capacity, sizeof(uint32));
	roots->merge = calloc(capacity, sizeof(uint32));
	roots->verified = calloc(capacity, sizeof(bool));
	if (roots->entries == NULL || roots->buckets == NULL || roots->order == NULL
		|| roots->merge == NULL || roots->verified == NULL) {
		cluster_recovery_root_set_destroy(roots);
		return CLUSTER_RECOVERY_ROOTS_CAPACITY;
	}
	roots->root_fd = root_fd;
	roots->root_stat = observed;
	roots->system_identifier = system_identifier;
	roots->database_incarnation = database_incarnation;
	memcpy(roots->storage_uuid, storage_uuid, 16);
	roots->capacity = capacity;
	roots->bucket_count = buckets;
	roots->scan_budget = auxiliary_budget - 32 * 1024;
	result = cluster_recovery_namespace_begin_bounded(root_fd, roots->capacity, roots->scan_budget,
													  &roots->scan);
	if (result != CLUSTER_RECOVERY_NAMESPACE_MORE) {
		cluster_recovery_root_set_destroy(roots);
		return roots_namespace_result(result);
	}
	*out = roots;
	return CLUSTER_RECOVERY_ROOTS_MORE;
}

ClusterRecoveryRootsResult
cluster_recovery_root_set_add_target(ClusterRecoveryRootSet *roots, const RfPageIdentityV1 *page)
{
	char path[MAXPGPATH];
	const char *suffix;
	ClusterRecoveryRootEntry *entry;
	uint32 index;

	if (roots == NULL)
		return CLUSTER_RECOVERY_ROOTS_INVALID;
	if (roots->status > CLUSTER_RECOVERY_ROOTS_OBSERVED)
		return roots->status;
	if (roots->started || page == NULL || page->system_identifier != roots->system_identifier
		|| memcmp(page->storage_uuid, roots->storage_uuid, 16) != 0 || page->reserved_zero != 0
		|| page->locator.relNumber == 0 || page->blockno == InvalidBlockNumber
		|| (page->locator.spcOid == GLOBALTABLESPACE_OID
				? page->locator.dbOid != InvalidOid
				: page->locator.spcOid != DEFAULTTABLESPACE_OID
					  || page->locator.dbOid == InvalidOid))
		return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_INVALID);
	switch (page->forknum) {
	case MAIN_FORKNUM:
		suffix = "";
		break;
	case FSM_FORKNUM:
		suffix = "_fsm";
		break;
	case VISIBILITYMAP_FORKNUM:
		suffix = "_vm";
		break;
	case SPACE_FORKNUM:
		suffix = "_space";
		break;
	default:
		return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_INVALID);
	}
	if (page->locator.spcOid == GLOBALTABLESPACE_OID)
		snprintf(path, sizeof(path), "global/%u%s", page->locator.relNumber, suffix);
	else
		snprintf(path, sizeof(path), "base/%u/%u%s", page->locator.dbOid, page->locator.relNumber,
				 suffix);
	if (!roots_lookup(roots, path, true, &index))
		return roots->status;
	entry = &roots->entries[index];
	entry->file.kind = CLUSTER_RECOVERY_FILE_RELATION;
	entry->file.locator = page->locator;
	entry->file.forknum = page->forknum;
	entry->origins |= CLUSTER_RECOVERY_ROOT_FROM_WAL;
	entry->required_blocks = Max(entry->required_blocks, (uint64)page->blockno + 1);
	return CLUSTER_RECOVERY_ROOTS_MORE;
}

static ClusterRecoveryRootsResult
roots_file_finish(ClusterRecoveryRootSet *roots)
{
	uint8 digest[32];
	ClusterRecoveryRootEntry *entry = &roots->entries[roots->current];

	if (pg_cryptohash_final(roots->file_hash, digest, sizeof(digest)) < 0)
		return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_IO);
	if (roots->pass == 0)
		memcpy(entry->content_sha256, digest, sizeof(digest));
	else if (memcmp(entry->content_sha256, digest, sizeof(digest)) != 0)
		return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_CHANGED);
	roots->reading = false;
	return CLUSTER_RECOVERY_ROOTS_MORE;
}

/* Bottom-up merge sort is resumable; even a large namespace never turns
 * finalization into an unbounded single-tick sort. */
static void
roots_sort_step(ClusterRecoveryRootSet *roots)
{
	for (unsigned work = 0; work < ROOTS_TICK; work++) {
		if (roots->width >= roots->count) {
			roots->stage = 3;
			return;
		}
		if (!roots->merging) {
			if (roots->left >= roots->count) {
				uint32 *tmp = roots->order;

				roots->order = roots->merge;
				roots->merge = tmp;
				roots->left = 0;
				roots->width *= 2;
				continue;
			}
			roots->mid = Min(roots->left + roots->width, roots->count);
			roots->right = Min(roots->mid + roots->width, roots->count);
			roots->a = roots->left;
			roots->b = roots->mid;
			roots->pos = roots->left;
			roots->merging = true;
		}
		if (roots->pos == roots->right) {
			roots->left = roots->right;
			roots->merging = false;
			continue;
		}
		if (roots->a < roots->mid
			&& (roots->b == roots->right
				|| strcmp(roots->entries[roots->order[roots->a]].file.path,
						  roots->entries[roots->order[roots->b]].file.path)
					   < 0))
			roots->merge[roots->pos++] = roots->order[roots->a++];
		else
			roots->merge[roots->pos++] = roots->order[roots->b++];
	}
}

static ClusterRecoveryRootsResult
roots_digest_step(ClusterRecoveryRootSet *roots)
{
	static const uint8 domain[] = "PGRAC-COLD-ROOT-CANDIDATES-V1";
	struct stat current;

	if (roots->hash_at == 0) {
		if (pg_cryptohash_init(roots->set_hash) < 0
			|| pg_cryptohash_update(roots->set_hash, domain, sizeof(domain)) < 0
			|| !roots_hash_number(roots->set_hash, roots->system_identifier, 8)
			|| !roots_hash_number(roots->set_hash, roots->database_incarnation, 8)
			|| pg_cryptohash_update(roots->set_hash, roots->storage_uuid, 16) < 0
			|| !roots_hash_number(roots->set_hash, roots->count, 4))
			return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_IO);
	}
	for (unsigned work = 0; work < ROOTS_TICK && roots->hash_at < roots->count; work++) {
		const ClusterRecoveryRootEntry *entry = &roots->entries[roots->order[roots->hash_at++]];
		size_t length = strlen(entry->file.path);

		if (!roots_hash_number(roots->set_hash, length, 4)
			|| pg_cryptohash_update(roots->set_hash, (const uint8 *)entry->file.path, length) < 0
			|| !roots_hash_number(roots->set_hash, entry->file.kind, 4)
			|| !roots_hash_number(roots->set_hash, entry->origins, 4)
			|| !roots_hash_number(roots->set_hash, entry->file.size, 8)
			|| !roots_hash_number(roots->set_hash, entry->required_blocks, 8)
			|| pg_cryptohash_update(roots->set_hash, entry->content_sha256, 32) < 0)
			return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_IO);
	}
	if (roots->hash_at != roots->count)
		return CLUSTER_RECOVERY_ROOTS_MORE;
	if (fstat(roots->root_fd, &current) != 0)
		return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_IO);
	if (!cluster_recovery_namespace_stat_same(&roots->root_stat, &current))
		return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_CHANGED);
	if (pg_cryptohash_final(roots->set_hash, roots->digest, sizeof(roots->digest)) < 0)
		return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_IO);
	roots->status = CLUSTER_RECOVERY_ROOTS_OBSERVED;
	return roots->status;
}

ClusterRecoveryRootsResult
cluster_recovery_root_set_step(ClusterRecoveryRootSet *roots, ClusterRecoveryRootsEvent *out)
{
	ClusterRecoveryNamespaceEntry file;
	ClusterRecoveryNamespaceResult result;
	ClusterRecoveryRootEntry *entry;
	struct stat current;
	uint32 index;
	int fd;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (roots == NULL || out == NULL)
		return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_INVALID);
	if (roots->status != CLUSTER_RECOVERY_ROOTS_MORE)
		return roots->status;
	roots->started = true;
	out->pass = roots->pass;
	if (roots->stage == 1) {
		for (unsigned work = 0; work < ROOTS_TICK && roots->verify_at < roots->count; work++) {
			index = roots->verify_at++;
			if ((roots->entries[index].origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) != 0
				&& !roots->verified[index])
				return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_CHANGED);
			roots->order[index] = index;
		}
		if (roots->verify_at == roots->count) {
			roots->stage = 2;
			roots->width = 1;
		}
		return CLUSTER_RECOVERY_ROOTS_MORE;
	}
	if (roots->stage == 2) {
		roots_sort_step(roots);
		return CLUSTER_RECOVERY_ROOTS_MORE;
	}
	if (roots->stage == 3)
		return roots_digest_step(roots);
	if (roots->reading) {
		size_t length;

		entry = &roots->entries[roots->current];
		if (roots->offset == entry->file.size)
			return roots_file_finish(roots);
		length = Min((uint64)BLCKSZ, entry->file.size - roots->offset);
		result = cluster_recovery_namespace_read_current(roots->scan, roots->offset, roots->bytes,
														 length);
		if (result != CLUSTER_RECOVERY_NAMESPACE_ENTRY)
			return roots_fail(roots, roots_namespace_result(result));
		if (pg_cryptohash_update(roots->file_hash, roots->bytes, length) < 0)
			return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_IO);
		out->entry = entry;
		out->bytes = roots->bytes;
		out->offset = roots->offset;
		out->length = length;
		roots->offset += length;
		return CLUSTER_RECOVERY_ROOTS_BYTES;
	}
	result = cluster_recovery_namespace_next(roots->scan, &file, &fd);
	if (result == CLUSTER_RECOVERY_NAMESPACE_MORE)
		return CLUSTER_RECOVERY_ROOTS_MORE;
	if (result == CLUSTER_RECOVERY_NAMESPACE_END) {
		if (fstat(roots->root_fd, &current) != 0)
			return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_IO);
		if (!cluster_recovery_namespace_stat_same(&roots->root_stat, &current))
			return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_CHANGED);
		cluster_recovery_namespace_destroy(roots->scan);
		roots->scan = NULL;
		if (roots->pass == 0) {
			roots->pass = 1;
			result = cluster_recovery_namespace_begin_bounded(roots->root_fd, roots->capacity,
															  roots->scan_budget, &roots->scan);
			if (result != CLUSTER_RECOVERY_NAMESPACE_MORE)
				return roots_fail(roots, roots_namespace_result(result));
		} else
			roots->stage = 1;
		return CLUSTER_RECOVERY_ROOTS_MORE;
	}
	if (result != CLUSTER_RECOVERY_NAMESPACE_ENTRY)
		return roots_fail(roots, roots_namespace_result(result));
	if (!roots_lookup(roots, file.path, roots->pass == 0, &index))
		return roots->status == CLUSTER_RECOVERY_ROOTS_MORE
				   ? roots_fail(roots, CLUSTER_RECOVERY_ROOTS_CHANGED)
				   : roots->status;
	entry = &roots->entries[index];
	if (roots->pass == 0) {
		if ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) != 0
			|| ((entry->origins & CLUSTER_RECOVERY_ROOT_FROM_WAL) != 0
				&& file.kind != CLUSTER_RECOVERY_FILE_RELATION))
			return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_CHANGED);
		entry->file = file;
		entry->origins |= CLUSTER_RECOVERY_ROOT_PHYSICAL;
	} else {
		if ((entry->origins & CLUSTER_RECOVERY_ROOT_PHYSICAL) == 0 || roots->verified[index]
			|| entry->file.kind != file.kind || entry->file.size != file.size
			|| !cluster_recovery_namespace_stat_same(&entry->file.observed, &file.observed))
			return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_CHANGED);
		roots->verified[index] = true;
	}
	roots->current = index;
	roots->offset = 0;
	if (file.kind != CLUSTER_RECOVERY_FILE_DIRECTORY) {
		if (pg_cryptohash_init(roots->file_hash) < 0)
			return roots_fail(roots, CLUSTER_RECOVERY_ROOTS_IO);
		roots->reading = true;
	}
	out->entry = entry;
	return CLUSTER_RECOVERY_ROOTS_ENTRY;
}

uint32
cluster_recovery_root_set_count(const ClusterRecoveryRootSet *roots)
{
	return roots == NULL ? 0 : roots->count;
}

const ClusterRecoveryRootEntry *
cluster_recovery_root_set_entry(const ClusterRecoveryRootSet *roots, uint32 index)
{
	return roots == NULL || roots->status != CLUSTER_RECOVERY_ROOTS_OBSERVED
				   || index >= roots->count
			   ? NULL
			   : &roots->entries[roots->order[index]];
}

bool
cluster_recovery_root_set_digest(const ClusterRecoveryRootSet *roots, uint8 out[32])
{
	if (out != NULL)
		memset(out, 0, 32);
	if (roots == NULL || out == NULL || roots->status != CLUSTER_RECOVERY_ROOTS_OBSERVED)
		return false;
	memcpy(out, roots->digest, 32);
	return true;
}

void
cluster_recovery_root_set_destroy(ClusterRecoveryRootSet *roots)
{
	if (roots != NULL) {
		cluster_recovery_namespace_destroy(roots->scan);
		pg_cryptohash_free(roots->file_hash);
		pg_cryptohash_free(roots->set_hash);
		free(roots->entries);
		free(roots->buckets);
		free(roots->order);
		free(roots->merge);
		free(roots->verified);
		free(roots);
	}
}
#endif
