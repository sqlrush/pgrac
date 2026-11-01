/* Original origin identity carried by existing current/grant replies. */
#ifndef CLUSTER_CTRC_ORIGIN_FULLXID_H
#define CLUSTER_CTRC_ORIGIN_FULLXID_H

#include "c.h"
#include "access/transam.h"

#define CLUSTER_CTRC_FORMAT_VERSION UINT8_C(1)
#define CLUSTER_CTRC_TXN_KEY_BYTES 96

/* The original key layout and reserved-zero semantics are unchanged. */
typedef struct ClusterCtrcTxnKeyV1 {
	uint8 format_version;
	uint8 owner_instance;
	uint16 origin_node_id;
	uint32 segment_id;
	uint32 segment_generation;
	uint16 slot_offset;
	uint16 slot_wrap;
	TransactionId xid;
	uint32 cluster_epoch;
	uint64 system_identifier;
	uint64 origin_boot_incarnation;
	uint64 formation_epoch;
	uint64 admission_record_generation;
	uint64 root_descriptor_incarnation;
	uint64 root_id;
	uint64 root_generation;
	uint8 reserved[16];
} ClusterCtrcTxnKeyV1;

StaticAssertDecl(sizeof(ClusterCtrcTxnKeyV1) == CLUSTER_CTRC_TXN_KEY_BYTES,
	"CTRC transaction key must remain 96 bytes");

#define CLUSTER_CTRC_ORIGIN_FULLXID_V2_VERSION 2
#define CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES 144
/* One explicit IMAGE V3 record: header8 + immutable original144 + hop72. */
#define CLUSTER_CTRC_DELEGATION_V1_VERSION 1
#define CLUSTER_CTRC_DELEGATION_V1_BYTES 224

/* Compact complete path: header8 + immutable original144 + root32, then
 * receiver32/sequence8 per hop. V1's 224 bytes retain their first-hop meaning. */
#define CLUSTER_CTRC_DELEGATION_V2_VERSION 2
#define CLUSTER_CTRC_DELEGATION_V2_BASE_BYTES 184
#define CLUSTER_CTRC_DELEGATION_V2_HOP_BYTES 40
#define CLUSTER_CTRC_DELEGATION_V2_MAX_HOPS 15
#define CLUSTER_CTRC_DELEGATION_V2_MAX_BYTES 784

#endif /* CLUSTER_CTRC_ORIGIN_FULLXID_H */
