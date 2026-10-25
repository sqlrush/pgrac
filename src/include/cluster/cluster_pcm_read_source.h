/*-------------------------------------------------------------------------
 * cluster_pcm_read_source.h -- process-local first-fill source receipt.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PCM_READ_SOURCE_H
#define CLUSTER_PCM_READ_SOURCE_H

#include "storage/buf_internals.h"

typedef enum ClusterPcmReadSourceKind {
	CLUSTER_PCM_SOURCE_FAIL = 0,
	CLUSTER_PCM_SOURCE_DISK_GRANT,
	CLUSTER_PCM_SOURCE_IMAGE_INSTALLED,
	CLUSTER_PCM_SOURCE_RETRY
} ClusterPcmReadSourceKind;

/* Stack receipt for an existing GRANT_PENDING reservation, not new shared
 * authority. The native IO owner keeps the reservation through first fill. */
typedef struct ClusterPcmReadSource {
	BufferTag tag;
	uint64 base_generation;
	uint64 reservation_token;
	uint64 watermark;
	uint64 request_id;
	uint64 request_epoch;
	int32 master_node;
	ClusterPcmReadSourceKind kind;
	bool durable;
	bool quiescent;
	bool pending_s; /* Master pre-registration; DONE follows native publication. */
} ClusterPcmReadSource;

/* Wire identity, not a grant. The master also checks its pinned registry binding. */
typedef struct ClusterPcmReadGrantKey {
	BufferTag tag;
	uint64 request_id;
	uint64 epoch;
	int32 requester_node;
	int32 backend_id;
} ClusterPcmReadGrantKey;

typedef struct ClusterPcmReadGrant {
	ClusterPcmReadGrantKey key;
	uint64 binding_generation;
	int32 holder_node; /* -1: disk grant, otherwise existing current/S source. */
	bool holder_was_x;
	bool revoking;
} ClusterPcmReadGrant;

static inline bool
cluster_pcm_read_source_bound(const ClusterPcmReadSource *source, const BufferTag *tag,
							  uint64 generation, uint64 token)
{
	return source != NULL && tag != NULL && source->quiescent && generation != UINT64_MAX
		   && token != 0 && token != UINT64_MAX && BufferTagsEqual(&source->tag, tag)
		   && source->base_generation == generation && source->reservation_token == token;
}

static inline bool
cluster_pcm_read_source_allows_disk(const ClusterPcmReadSource *source, const BufferTag *tag,
									uint64 generation, uint64 token)
{
	return cluster_pcm_read_source_bound(source, tag, generation, token)
		   && source->kind == CLUSTER_PCM_SOURCE_DISK_GRANT && source->durable;
}

#endif
