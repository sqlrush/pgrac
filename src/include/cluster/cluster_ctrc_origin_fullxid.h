/* Original origin identity carried by existing current/grant replies. */
#ifndef CLUSTER_CTRC_ORIGIN_FULLXID_H
#define CLUSTER_CTRC_ORIGIN_FULLXID_H

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
