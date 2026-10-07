/* PGRAC: acknowledged frozen block census, never a DATA or grant receipt.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_BLOCK_REDECLARE_H
#define CLUSTER_BLOCK_REDECLARE_H

#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_scn.h"
#include "storage/buf_internals.h"

#define CLUSTER_BLOCK_REDECLARE_BYTES 128
#define CLUSTER_BLOCK_REDECLARE_REQUEST 1
#define CLUSTER_BLOCK_REDECLARE_ACK 2

typedef struct ClusterBlockRedeclareV1 {
	uint64 epoch, nonce, source_boot, master_boot, system_identifier;
	uint64 census_hash;
	uint8 storage_uuid[16];
	int32 source_node, master_node;
	BufferTag tag;
	XLogRecPtr page_lsn;
	SCN page_scn;
	uint8 kind, mode;
} ClusterBlockRedeclareV1;

extern bool cluster_block_redeclare_encode_v1(const ClusterBlockRedeclareV1 *value,
											  uint8 bytes[CLUSTER_BLOCK_REDECLARE_BYTES]);
extern bool cluster_block_redeclare_decode_v1(const void *bytes, Size length,
											  ClusterBlockRedeclareV1 *out);
/* LMON only. False retains the scan position; each retry uses the original
 * immutable request until acknowledged or its frozen episode changes. */
extern bool cluster_block_redeclare_poll_v1(BufferTag tag, uint8 mode, XLogRecPtr page_lsn,
											SCN page_scn, uint64 epoch, int master);
extern void cluster_block_redeclare_ingress_v1(const ClusterICEnvelope *env, const void *bytes);

#endif
