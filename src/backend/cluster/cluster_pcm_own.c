/*-------------------------------------------------------------------------
 *
 * cluster_pcm_own.c
 *	  pgrac per-buffer node-local ownership tuple (generation + token + flags).
 *
 *	  See cluster_pcm_own.h for the design.  This file owns the shmem array
 *	  allocation (NBuffers entries, indexed by buf_id) and its registration
 *	  with the cluster shmem region registry.  The coherent transition/read
 *	  helpers live in bufmgr.c (they need BufferDesc + the header spinlock).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_pcm_own.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-4.7a-hold-until-revoked.md (ownership-generation wave).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "miscadmin.h"
#include "storage/shmem.h"

#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_shmem.h"

ClusterPcmOwnEntry *ClusterPcmOwnArray = NULL;

Size
cluster_pcm_own_shmem_size(void)
{
	return mul_size((Size)NBuffers, sizeof(ClusterPcmOwnEntry));
}

void
cluster_pcm_own_shmem_init(void)
{
	bool found;

	ClusterPcmOwnArray = (ClusterPcmOwnEntry *)ShmemInitStruct(
		"pgrac cluster pcm ownership", cluster_pcm_own_shmem_size(), &found);
	if (!found) {
		int i;

		for (i = 0; i < NBuffers; i++) {
			pg_atomic_init_u64(&ClusterPcmOwnArray[i].generation, 0);
			pg_atomic_init_u64(&ClusterPcmOwnArray[i].reservation_token, 0);
			pg_atomic_init_u64(&ClusterPcmOwnArray[i].writer_activation_token, 0);
			pg_atomic_init_u32(&ClusterPcmOwnArray[i].flags, 0);
			ClusterPcmOwnArray[i]._pad = 0;
		}
	}
}

static bool
cluster_pcm_own_entry_for_buf(int buf_id, ClusterPcmOwnEntry **out_entry)
{
	if (out_entry == NULL)
		return false;
	*out_entry = NULL;
	if (ClusterPcmOwnArray == NULL || buf_id < 0 || buf_id >= NBuffers)
		return false;
	*out_entry = &ClusterPcmOwnArray[buf_id];
	return true;
}

static bool
cluster_pcm_own_reservation_flag_valid(uint32 reservation_flag)
{
	return reservation_flag == PCM_OWN_FLAG_GRANT_PENDING
		   || reservation_flag == PCM_OWN_FLAG_REVOKING;
}

ClusterPcmOwnResult
cluster_pcm_own_reservation_begin_exact(int buf_id, uint64 expected_generation,
										uint32 reservation_flag, uint64 *out_token)
{
	ClusterPcmOwnEntry *entry;
	ClusterPcmOwnResult live_result;
	uint64 generation;
	uint64 old_token;
	uint64 writer_activation_token;
	uint32 flags;

	if (out_token == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	*out_token = 0;
	if (!cluster_pcm_own_reservation_flag_valid(reservation_flag))
		return CLUSTER_PCM_OWN_INVALID;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return CLUSTER_PCM_OWN_NOT_READY;

	generation = pg_atomic_read_u64(&entry->generation);
	old_token = pg_atomic_read_u64(&entry->reservation_token);
	writer_activation_token = pg_atomic_read_u64(&entry->writer_activation_token);
	flags = pg_atomic_read_u32(&entry->flags);
	if (generation != expected_generation)
		return CLUSTER_PCM_OWN_STALE;
	if (writer_activation_token != 0 && writer_activation_token != old_token)
		return CLUSTER_PCM_OWN_CORRUPT;
	live_result = cluster_pcm_own_classify_live_flags(flags, old_token);
	if (live_result != CLUSTER_PCM_OWN_OK) {
		if (writer_activation_token != 0)
			return CLUSTER_PCM_OWN_CORRUPT;
		return live_result;
	}
	if (writer_activation_token != 0) {
		if (reservation_flag == PCM_OWN_FLAG_REVOKING)
			return CLUSTER_PCM_OWN_BUSY;
		return CLUSTER_PCM_OWN_CORRUPT;
	}
	if (generation == UINT64_MAX || old_token == UINT64_MAX)
		return CLUSTER_PCM_OWN_EXHAUSTED;

	old_token++;
	/* Both stores are serialized by the BufferDesc header lock required by
	 * the public contract.  The flag is the active marker; token stays
	 * monotonic and is never cleared. */
	pg_atomic_write_u64(&entry->reservation_token, old_token);
	pg_atomic_write_u32(&entry->flags, reservation_flag);
	*out_token = old_token;
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_own_reservation_abort_exact(int buf_id, uint64 expected_generation,
										uint64 reservation_token, uint32 reservation_flag)
{
	ClusterPcmOwnEntry *entry;

	if (!cluster_pcm_own_reservation_flag_valid(reservation_flag) || reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return CLUSTER_PCM_OWN_NOT_READY;
	if (pg_atomic_read_u64(&entry->writer_activation_token) != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	if (pg_atomic_read_u64(&entry->generation) != expected_generation
		|| pg_atomic_read_u64(&entry->reservation_token) != reservation_token
		|| pg_atomic_read_u32(&entry->flags) != reservation_flag)
		return CLUSTER_PCM_OWN_STALE;

	pg_atomic_write_u32(&entry->flags, 0);
	return CLUSTER_PCM_OWN_OK;
}

static ClusterPcmOwnResult
cluster_pcm_own_reservation_handoff_exact(int buf_id, uint64 expected_generation,
										  uint64 reservation_token, uint32 source_flag,
										  uint32 target_flag)
{
	ClusterPcmOwnEntry *entry;
	ClusterPcmOwnResult live_result;
	uint64 live_token;
	uint32 flags;

	if (reservation_token == 0 || !cluster_pcm_own_reservation_flag_valid(source_flag)
		|| !cluster_pcm_own_reservation_flag_valid(target_flag) || source_flag == target_flag)
		return CLUSTER_PCM_OWN_INVALID;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return CLUSTER_PCM_OWN_NOT_READY;
	if (pg_atomic_read_u64(&entry->generation) != expected_generation)
		return CLUSTER_PCM_OWN_STALE;
	if (pg_atomic_read_u64(&entry->writer_activation_token) != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	live_token = pg_atomic_read_u64(&entry->reservation_token);
	flags = pg_atomic_read_u32(&entry->flags);
	live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
	if (live_result == CLUSTER_PCM_OWN_CORRUPT)
		return live_result;
	if (live_token != reservation_token)
		return CLUSTER_PCM_OWN_STALE;
	/* An exact duplicate observes the already-published role and succeeds
	 * without touching the tuple. */
	if (flags == target_flag)
		return CLUSTER_PCM_OWN_OK;
	if (flags == 0)
		return CLUSTER_PCM_OWN_STALE;
	if (flags != source_flag)
		return CLUSTER_PCM_OWN_BUSY;

	pg_atomic_write_u32(&entry->flags, target_flag);
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_own_revoke_to_grant_handoff_exact(int buf_id, uint64 expected_generation,
											  uint64 reservation_token)
{
	return cluster_pcm_own_reservation_handoff_exact(buf_id, expected_generation, reservation_token,
													 PCM_OWN_FLAG_REVOKING,
													 PCM_OWN_FLAG_GRANT_PENDING);
}

ClusterPcmOwnResult
cluster_pcm_own_grant_commit_exact(int buf_id, uint64 expected_generation, uint64 reservation_token,
								   uint64 *out_committed_generation)
{
	ClusterPcmOwnEntry *entry;
	ClusterPcmOwnResult live_result;
	uint64 generation;
	uint64 live_token;
	uint32 flags;

	if (out_committed_generation == NULL || reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	*out_committed_generation = 0;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return CLUSTER_PCM_OWN_NOT_READY;

	generation = pg_atomic_read_u64(&entry->generation);
	if (generation != expected_generation)
		return CLUSTER_PCM_OWN_STALE;
	live_token = pg_atomic_read_u64(&entry->reservation_token);
	flags = pg_atomic_read_u32(&entry->flags);
	if (pg_atomic_read_u64(&entry->writer_activation_token) != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
	if (live_result == CLUSTER_PCM_OWN_CORRUPT)
		return live_result;
	if (flags == 0)
		return CLUSTER_PCM_OWN_STALE;
	if (flags != PCM_OWN_FLAG_GRANT_PENDING)
		return CLUSTER_PCM_OWN_BUSY;
	if (live_token != reservation_token)
		return CLUSTER_PCM_OWN_STALE;
	if (generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_EXHAUSTED;

	generation++;
	pg_atomic_write_u64(&entry->generation, generation);
	pg_atomic_write_u32(&entry->flags, 0);
	*out_committed_generation = generation;
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_own_writer_grant_commit_exact(int buf_id, uint64 expected_generation,
									  uint64 reservation_token,
									  uint64 *out_committed_generation)
{
	ClusterPcmOwnEntry *entry;
	ClusterPcmOwnResult live_result;
	uint64 generation;
	uint64 live_token;
	uint32 flags;

	if (out_committed_generation == NULL || reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	*out_committed_generation = 0;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return CLUSTER_PCM_OWN_NOT_READY;
	generation = pg_atomic_read_u64(&entry->generation);
	live_token = pg_atomic_read_u64(&entry->reservation_token);
	flags = pg_atomic_read_u32(&entry->flags);
	if (pg_atomic_read_u64(&entry->writer_activation_token) != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	if (generation != expected_generation)
		return CLUSTER_PCM_OWN_STALE;
	live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
	if (live_result == CLUSTER_PCM_OWN_CORRUPT)
		return live_result;
	if (flags == 0)
		return CLUSTER_PCM_OWN_STALE;
	if (flags != PCM_OWN_FLAG_GRANT_PENDING)
		return CLUSTER_PCM_OWN_BUSY;
	if (live_token != reservation_token)
		return CLUSTER_PCM_OWN_STALE;
	if (generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_EXHAUSTED;
	/* Caller holds the matching BufferDesc header lock.  The active flag is
	 * cleared and the fence published before that lock can be released, so a
	 * REVOKING begin can never pass between the two stores. */
	generation++;
	pg_atomic_write_u64(&entry->generation, generation);
	pg_atomic_write_u32(&entry->flags, 0);
	pg_atomic_write_u64(&entry->writer_activation_token, reservation_token);
	*out_committed_generation = generation;
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_own_writer_activation_clear_exact(int buf_id, uint64 expected_generation,
										  uint64 reservation_token)
{
	ClusterPcmOwnEntry *entry;
	uint64 live_activation;

	if (reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return CLUSTER_PCM_OWN_NOT_READY;
	if (pg_atomic_read_u64(&entry->generation) != expected_generation
		|| pg_atomic_read_u64(&entry->reservation_token) != reservation_token
		|| pg_atomic_read_u32(&entry->flags) != 0)
		return CLUSTER_PCM_OWN_STALE;
	live_activation = pg_atomic_read_u64(&entry->writer_activation_token);
	if (live_activation == 0 || live_activation != reservation_token)
		return CLUSTER_PCM_OWN_STALE;
	pg_atomic_write_u64(&entry->writer_activation_token, 0);
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_own_revoke_commit_exact(int buf_id, uint64 expected_generation,
									uint64 reservation_token, uint64 *out_committed_generation)
{
	ClusterPcmOwnEntry *entry;
	ClusterPcmOwnResult live_result;
	uint64 generation;
	uint64 live_token;
	uint32 flags;

	if (out_committed_generation == NULL || reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	*out_committed_generation = 0;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return CLUSTER_PCM_OWN_NOT_READY;

	generation = pg_atomic_read_u64(&entry->generation);
	live_token = pg_atomic_read_u64(&entry->reservation_token);
	flags = pg_atomic_read_u32(&entry->flags);
	if (pg_atomic_read_u64(&entry->writer_activation_token) != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	if (generation != expected_generation)
		return CLUSTER_PCM_OWN_STALE;
	live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
	if (live_result == CLUSTER_PCM_OWN_CORRUPT)
		return live_result;
	if (flags == 0)
		return CLUSTER_PCM_OWN_STALE;
	if (flags != PCM_OWN_FLAG_REVOKING)
		return CLUSTER_PCM_OWN_BUSY;
	if (live_token != reservation_token)
		return CLUSTER_PCM_OWN_STALE;
	if (generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_EXHAUSTED;

	generation++;
	pg_atomic_write_u64(&entry->generation, generation);
	pg_atomic_write_u32(&entry->flags, 0);
	*out_committed_generation = generation;
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_own_revoke_retain_commit_exact(int buf_id, uint64 expected_generation,
										   uint64 reservation_token,
										   uint64 *out_committed_generation)
{
	ClusterPcmOwnEntry *entry;
	ClusterPcmOwnResult live_result;
	uint64 generation;
	uint64 live_token;
	uint32 flags;

	if (out_committed_generation == NULL || reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	*out_committed_generation = 0;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return CLUSTER_PCM_OWN_NOT_READY;

	generation = pg_atomic_read_u64(&entry->generation);
	live_token = pg_atomic_read_u64(&entry->reservation_token);
	flags = pg_atomic_read_u32(&entry->flags);
	if (pg_atomic_read_u64(&entry->writer_activation_token) != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	if (generation != expected_generation)
		return CLUSTER_PCM_OWN_STALE;
	live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
	if (live_result == CLUSTER_PCM_OWN_CORRUPT)
		return live_result;
	if (flags == 0)
		return CLUSTER_PCM_OWN_STALE;
	if (flags != PCM_OWN_FLAG_REVOKING)
		return CLUSTER_PCM_OWN_BUSY;
	if (live_token != reservation_token)
		return CLUSTER_PCM_OWN_STALE;
	if (generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_EXHAUSTED;

	generation++;
	pg_atomic_write_u64(&entry->generation, generation);
	/* Unlike an ordinary revoke, this token is the descriptor-retention pin.
	 * Keep REVOKING published until exact image-record drain. */
	*out_committed_generation = generation;
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_own_revoke_retain_release_exact(int buf_id, uint64 committed_generation,
											uint64 reservation_token)
{
	ClusterPcmOwnEntry *entry;
	ClusterPcmOwnResult live_result;
	uint64 live_token;
	uint32 flags;

	if (reservation_token == 0)
		return CLUSTER_PCM_OWN_INVALID;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return CLUSTER_PCM_OWN_NOT_READY;
	if (pg_atomic_read_u64(&entry->generation) != committed_generation)
		return CLUSTER_PCM_OWN_STALE;
	if (pg_atomic_read_u64(&entry->writer_activation_token) != 0)
		return CLUSTER_PCM_OWN_CORRUPT;
	live_token = pg_atomic_read_u64(&entry->reservation_token);
	flags = pg_atomic_read_u32(&entry->flags);
	live_result = cluster_pcm_own_classify_live_flags(flags, live_token);
	if (live_result == CLUSTER_PCM_OWN_CORRUPT)
		return live_result;
	if (flags == 0)
		return CLUSTER_PCM_OWN_STALE;
	if (flags != PCM_OWN_FLAG_REVOKING)
		return CLUSTER_PCM_OWN_BUSY;
	if (live_token != reservation_token)
		return CLUSTER_PCM_OWN_STALE;

	pg_atomic_write_u32(&entry->flags, 0);
	return CLUSTER_PCM_OWN_OK;
}

bool
cluster_pcm_own_gen_bump_checked(int buf_id, uint64 *out_generation)
{
	ClusterPcmOwnEntry *entry;
	uint64 generation;
	uint32 flags;

	if (out_generation != NULL)
		*out_generation = 0;
	if (!cluster_pcm_own_entry_for_buf(buf_id, &entry))
		return false;
	generation = pg_atomic_read_u64(&entry->generation);
	flags = pg_atomic_read_u32(&entry->flags);
	if (out_generation != NULL)
		*out_generation = generation;
	if (generation == UINT64_MAX || flags != 0
		|| pg_atomic_read_u64(&entry->writer_activation_token) != 0)
		return false;
	generation++;
	pg_atomic_write_u64(&entry->generation, generation);
	/* Descriptor/tag reuse and every ordinary transition start with no
	 * activation lifecycle.  A nonzero fence was rejected above. */
	pg_atomic_write_u64(&entry->writer_activation_token, 0);
	if (out_generation != NULL)
		*out_generation = generation;
	return true;
}

static const ClusterShmemRegion cluster_pcm_own_region = {
	.name = "pgrac cluster pcm ownership",
	.size_fn = cluster_pcm_own_shmem_size,
	.init_fn = cluster_pcm_own_shmem_init,
	.lwlock_count = 0, /* atomics only; the buffer header spinlock serializes the tuple */
	.owner_subsys = "cluster_pcm_own",
	.reserved_flags = 0,
};

void
cluster_pcm_own_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_pcm_own_region);
}

#endif /* USE_PGRAC_CLUSTER */
