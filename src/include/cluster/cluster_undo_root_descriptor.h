/*-------------------------------------------------------------------------
 *
 * cluster_undo_root_descriptor.h
 *    PGRD V1 persistent undo-root descriptor pure codec and identity math.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_ROOT_DESCRIPTOR_H
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_H

#include "c.h"

#include "cluster/cluster_conf.h"
#include "cluster/storage/cluster_undo_block0.h"


#define CLUSTER_UNDO_ROOT_DESCRIPTOR_MAGIC UINT32_C(0x50475244)
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_VERSION UINT16_C(1)
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_HEADER_LEN UINT16_C(64)
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES 512
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_CRC_OFFSET 508
#define CLUSTER_UNDO_ROOT_UUID_BYTES 16
#define CLUSTER_UNDO_ROOT_COUNT 129
#define CLUSTER_UNDO_ROOT_FILE_SLOTS 32768
#define CLUSTER_UNDO_ROOT_MAX_NAMESPACE (UINT64_MAX >> 15)
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_SLOT (6 * CLUSTER_MAX_NODES + 2)
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_LOCAL_SLOT(node_id) (6 * CLUSTER_MAX_NODES + 3 + (node_id))
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_OFFSET                                                 \
	((off_t)CLUSTER_UNDO_ROOT_DESCRIPTOR_SHARED_SLOT * CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES)
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_LOCAL_OFFSET(node_id)                                         \
	((off_t)CLUSTER_UNDO_ROOT_DESCRIPTOR_LOCAL_SLOT(node_id) * CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES)
/*
 * B′ P0 fix: keep in lockstep with CLUSTER_VOTING_PGRD_FILE_BYTES_MIN —
 * the attested capacity now also covers region 7 (cold-formation marker
 * slots [7N+3, 7N+3+N)) which follows the undo descriptors.
 */
#define CLUSTER_UNDO_ROOT_DESCRIPTOR_FILE_BYTES_MIN                                                \
	((off_t)(8 * CLUSTER_MAX_NODES + 3) * CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES)

typedef enum ClusterUndoRootKind {
	CLUSTER_UNDO_ROOT_KIND_SHARED = 1,
	CLUSTER_UNDO_ROOT_KIND_LOCAL = 2
} ClusterUndoRootKind;

typedef enum ClusterUndoRootDescriptorState {
	CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED = 0,
	CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID = 1,
	CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD = 2
} ClusterUndoRootDescriptorState;

typedef struct ClusterUndoRootDescriptorV1 {
	uint64 descriptor_incarnation;
	uint8 root_kind;
	int32 owner_node;
	uint32 root_ordinal;
	uint8 root_uuid[CLUSTER_UNDO_ROOT_UUID_BYTES];
	uint64 namespace_id;
	uint64 system_identifier;
} ClusterUndoRootDescriptorV1;

extern bool cluster_undo_root_namespace_id(uint64 descriptor_incarnation, uint32 root_ordinal,
										   uint64 *namespace_id);
extern bool cluster_undo_root_file_slot(uint32 owner_instance, uint32 segment_slot,
										uint32 *file_slot);
extern bool cluster_undo_root_id(uint64 namespace_id, uint32 file_slot, uint64 *root_id);
extern bool cluster_undo_root_descriptor_encode(const ClusterUndoRootDescriptorV1 *descriptor,
												uint8 out[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES]);
extern ClusterUndoRootDescriptorState
cluster_undo_root_descriptor_decode(const uint8 bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
									uint64 expected_system_identifier,
									ClusterUndoRootDescriptorV1 *out);
extern bool cluster_undo_root_descriptor_resolve(const ClusterUndoRootDescriptorV1 *descriptor,
												 ClusterUndoPathIntent intent,
												 uint32 owner_instance, uint32 segment_id,
												 ClusterUndoBlock0ResolvedRoot *out);

#endif /* CLUSTER_UNDO_ROOT_DESCRIPTOR_H */
