/* PGRAC: bounded GCS writeback and qualified physical PI completion.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PI_WRITEBACK_H
#define CLUSTER_PI_WRITEBACK_H

#include "cluster/cluster_pi_data.h"
#include "cluster/cluster_wal_writer.h"

#define CLUSTER_PI_WRITEBACK_MAX 16
#define CLUSTER_PI_WRITEBACK_HEADER_BYTES 176
#define CLUSTER_PI_WRITEBACK_FACT_BYTES 592
#define CLUSTER_PI_WRITEBACK_MAX_BYTES                                                             \
	(CLUSTER_PI_WRITEBACK_HEADER_BYTES + CLUSTER_PI_WRITEBACK_MAX * CLUSTER_PI_WRITEBACK_FACT_BYTES)
#define CLUSTER_PI_WRITEBACK_NOTIFY 1
#define CLUSTER_PI_WRITEBACK_ACK 2

/* Untrusted transport projection; one of the two cuts must be zero. Only
 * the actual DATA owner can export it and only a qualified notice can
 * import it. Neither this value nor its codec grants physical disposal. */
typedef struct ClusterPiDataFactV1 {
	ClusterPageWalBindingV1 binding;
	ClusterPcmPiWriteCutV1 write_cut;
	ClusterPcmPiStorageCutV1 storage_cut;
} ClusterPiDataFactV1;

typedef struct ClusterPiWritebackMessageV1 {
	uint32 verb;
	/* NOTIFY has 1..MAX facts. ACK carries its ordered exact success subset;
	 * zero facts acknowledge no page and only finish that request attempt. */
	uint32 count;
	uint64 nonce;
	uint64 epoch;
	ClusterWalSourceRef peer;
	ClusterPiDataFactV1 facts[CLUSTER_PI_WRITEBACK_MAX];
} ClusterPiWritebackMessageV1;

typedef struct ClusterPiWritebackNoticeV1 ClusterPiWritebackNoticeV1;
typedef struct ClusterPiWritebackJobV1 ClusterPiWritebackJobV1;

typedef enum ClusterPiWritebackRejectionV1 {
	CLUSTER_PI_WRITEBACK_DATA_PROOF = 0,
	CLUSTER_PI_WRITEBACK_LOCAL_ACK,
	CLUSTER_PI_WRITEBACK_REMOTE_ACK,
	CLUSTER_PI_WRITEBACK_MASTER_CUT,
	CLUSTER_PI_WRITEBACK_PEER_PHYSICAL,
	CLUSTER_PI_WRITEBACK_RECOVERY_PROOF,
	CLUSTER_PI_WRITEBACK_REJECTION_COUNT
} ClusterPiWritebackRejectionV1;

/* Attempt counters, not a count of distinct pages or a retirement proof.
 * Shared across background owners; the last sample is diagnostic only. */
typedef struct ClusterPiWritebackRejectionsV1 {
	uint64 attempts[CLUSTER_PI_WRITEBACK_REJECTION_COUNT];
	uint64 log_events;
	BufferTag last_resource;
	uint32 last_reason;
	int32 last_peer;
} ClusterPiWritebackRejectionsV1;
extern bool cluster_pi_writeback_rejections_v1(ClusterPiWritebackRejectionsV1 *out);

extern bool cluster_pi_writeback_encode_v1(const ClusterPiWritebackMessageV1 *message, uint8 *bytes,
										   Size capacity, Size *length);
extern bool cluster_pi_writeback_decode_v1(const void *bytes, Size length,
										   ClusterPiWritebackMessageV1 *out);
extern bool cluster_page_data_pi_fact_v1(const ClusterPageDataReceiptV1 *receipt,
										 ClusterPiDataFactV1 *out);
extern bool cluster_page_data_from_notice_v1(const ClusterPiWritebackNoticeV1 *notice, uint32 index,
											 ClusterPageDataReceiptV1 **out);
extern bool cluster_pi_writeback_notice_read_v1(const ClusterPiWritebackNoticeV1 *notice,
												uint32 index, ClusterPiDataFactV1 *out);
extern bool cluster_page_data_pi_ack_export_v1(const ClusterPiPhysicalAckV1 *ack,
											   const ClusterPageDataReceiptV1 *receipt,
											   ClusterWalWriterToken *out);
extern bool cluster_page_data_pi_ack_import_v1(const ClusterPiWritebackJobV1 *job, uint32 index,
											   const ClusterPageDataReceiptV1 *receipt,
											   ClusterPiPhysicalAckV1 **out);
extern bool cluster_pi_writeback_ack_read_v1(const ClusterPiWritebackJobV1 *job, uint32 index,
											 const ClusterPageDataReceiptV1 *receipt,
											 ClusterWalWriterToken *out);
/* Recheck an already imported acknowledgement's peer/namespace/epoch. This
 * predicate cannot construct a receipt or replace the final master cut check. */
extern bool cluster_pi_writeback_ack_current_v1(const ClusterPiDataFactV1 *fact,
												const ClusterWalWriterToken *peer);

/* Background job only. A batch targets one original PI instance. Every
 * receipt must already have exact DATA/ancestry proof in the caller's held
 * full input. Poll confirms all physical owners, not a partial subset. */
extern ClusterControlRootResult
cluster_pi_writeback_begin_v1(const ClusterPageDataReceiptV1 *const *receipts, uint32 count,
							  const ClusterWalSourceRef *peer, ClusterPiWritebackJobV1 **out);
extern ClusterControlRootResult cluster_pi_writeback_poll_v1(ClusterPiWritebackJobV1 *job);
extern void cluster_pi_writeback_release_v1(ClusterPiWritebackJobV1 **job);

extern void cluster_pi_writeback_ingress_v1(const ClusterICEnvelope *env, const void *payload);
extern void cluster_pi_writeback_lmon_tick_v1(void);
extern bool cluster_pi_writeback_bgwriter_tick_v1(void);
extern bool cluster_pi_writeback_checkpointer_tick_v1(void);
extern void cluster_pi_writeback_checkpointer_release_v1(void);
extern ClusterNormalStopPollResult cluster_pi_writeback_normal_stop_poll_v1(const char **reason);
extern void cluster_pi_writeback_register_v1(void);
extern void cluster_pi_writeback_shmem_register_v1(void);

#endif
