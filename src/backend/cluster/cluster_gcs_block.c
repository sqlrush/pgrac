/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block.c
 *	  pgrac cluster GCS block-shipping substrate (Cache Fusion data plane).
 *
 *	  spec-2.33 activates cross-node 8KB block shipping on top of the
 *	  spec-2.32 GCS control plane.  Implements:
 *	    - cluster_gcs_send_block_request_and_wait sender (BufferDesc-aware)
 *	    - Master-side handler: HC82 XLogFlush(page_lsn) BEFORE shipping bytes,
 *	      HC88 master-not-holder decisions, HC89 single-retry revalidation
 *	    - Sender-side handler: HC83 CRC32C verify, HC84 PageSetLSN install
 *	    - Per-backend outstanding-block-request table (LWLock protected)
 *	    - 8 block-plane observability counters
 *
 *	  Wire ABI definitions live in cluster_gcs_block.h (HC79/HC80).
 *	  Master lookup remains in cluster_gcs.c (shared with control plane);
 *	  this module focuses on the data-plane request/reply cycle.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_gcs_block.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.33-gcs-block-shipping-substrate.md (FROZEN v0.4)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full) + AD-002 (PCM lock state machine)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_shmem.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/backendid.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/pg_crc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"


/* ============================================================
 * Shared-memory layout.
 *
 *	Per-backend block-outstanding table mirrors spec-2.32 cluster_gcs.c
 *	layout but uses a separate shmem region + LWLock tranche so that
 *	observability can distinguish data-plane contention from control-plane
 *	contention.  HC80 reply routing uses the compound key
 *	(requester_backend_id, request_id) so master replies to the right
 *	backend slot without scanning all backends.
 * ============================================================ */

#define MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND 8

typedef struct ClusterGcsBlockOutstandingSlot
{
	bool		in_use;
	uint64		request_id;
	uint8		transition_id;
	BufferTag	tag;
	int32		master_node;
	bool		reply_received;
	GcsBlockReplyHeader reply_header;
	char		reply_block_data[GCS_BLOCK_DATA_SIZE];
	ConditionVariable reply_cv;
} ClusterGcsBlockOutstandingSlot;

typedef struct ClusterGcsBlockBackendBlock
{
	LWLockPadded lock;
	ClusterGcsBlockOutstandingSlot slots[MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND];
	uint64		next_request_id;
} ClusterGcsBlockBackendBlock;

typedef struct ClusterGcsBlockShared
{
	pg_atomic_uint64 block_request_count;
	pg_atomic_uint64 block_reply_count;
	pg_atomic_uint64 block_timeout_count;
	pg_atomic_uint64 block_checksum_fail_count;
	pg_atomic_uint64 block_storage_fallback_count;
	pg_atomic_uint64 block_master_not_holder_count;
	pg_atomic_uint64 block_wal_flush_before_ship_count;
	pg_atomic_uint64 block_ship_bytes_total;
} ClusterGcsBlockShared;


static ClusterGcsBlockShared *ClusterGcsBlock = NULL;
static ClusterGcsBlockBackendBlock *gcs_block_backend_blocks = NULL;


/* ============================================================
 * Test-only injection hooks (USE_CLUSTER_UNIT only).
 * ============================================================ */
#ifdef USE_CLUSTER_UNIT
void		(*cluster_gcs_block_test_xlog_flush_hook) (uint64 page_lsn) = NULL;
int			(*cluster_gcs_block_test_lsn_drift_hook) (void) = NULL;
#endif


/* ============================================================
 * Forward decls (static helpers).
 * ============================================================ */
static ClusterGcsBlockBackendBlock *gcs_block_my_block(void);
static ClusterGcsBlockOutstandingSlot *gcs_block_reserve_slot(BufferTag tag,
															  uint8 transition_id,
															  int32 master_node,
															  uint64 *out_request_id);
static void gcs_block_release_slot(ClusterGcsBlockOutstandingSlot *slot);
static void gcs_block_send_reply(int32 dest_node,
								 const GcsBlockRequestPayload *req,
								 GcsBlockReplyStatus status,
								 XLogRecPtr page_lsn,
								 const char *block_data);
static uint32 gcs_block_compute_checksum(const char *block_data);
static void gcs_block_install_block(BufferDesc *buf,
									const char *block_data,
									XLogRecPtr page_lsn);


/* ============================================================
 * Module init + shmem registration.
 * ============================================================ */

Size
cluster_gcs_block_shmem_size(void)
{
	Size		sz;

	sz = MAXALIGN(sizeof(ClusterGcsBlockShared));
	sz = add_size(sz, mul_size(MaxBackends, sizeof(ClusterGcsBlockBackendBlock)));
	return sz;
}

void
cluster_gcs_block_shmem_init(void)
{
	bool		found;
	char	   *base;
	int			i;
	int			j;

	base = (char *) ShmemInitStruct("pgrac cluster gcs block",
									cluster_gcs_block_shmem_size(), &found);
	ClusterGcsBlock = (ClusterGcsBlockShared *) base;
	gcs_block_backend_blocks = (ClusterGcsBlockBackendBlock *)
		(base + MAXALIGN(sizeof(ClusterGcsBlockShared)));

	if (!found)
	{
		memset(ClusterGcsBlock, 0, sizeof(*ClusterGcsBlock));
		pg_atomic_init_u64(&ClusterGcsBlock->block_request_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_reply_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_timeout_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_checksum_fail_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_storage_fallback_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_master_not_holder_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_wal_flush_before_ship_count, 0);
		pg_atomic_init_u64(&ClusterGcsBlock->block_ship_bytes_total, 0);

		for (i = 0; i < MaxBackends; i++)
		{
			ClusterGcsBlockBackendBlock *blk = &gcs_block_backend_blocks[i];

			LWLockInitialize(&blk->lock.lock, LWTRANCHE_CLUSTER_GCS_BLOCK);
			blk->next_request_id = 1;
			for (j = 0; j < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; j++)
			{
				ClusterGcsBlockOutstandingSlot *slot = &blk->slots[j];

				slot->in_use = false;
				slot->request_id = 0;
				slot->reply_received = false;
				ConditionVariableInit(&slot->reply_cv);
			}
		}
	}
}

static const ClusterShmemRegion cluster_gcs_block_region = {
	.name = "pgrac cluster gcs block",
	.size_fn = cluster_gcs_block_shmem_size,
	.init_fn = cluster_gcs_block_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_gcs_block",
	.reserved_flags = 0,
};

void
cluster_gcs_block_module_init(void)
{
	cluster_shmem_register_region(&cluster_gcs_block_region);
}


/* ============================================================
 * Outstanding-slot management.
 * ============================================================ */

static ClusterGcsBlockBackendBlock *
gcs_block_my_block(void)
{
	int			idx;

	idx = MyBackendId - 1;
	if (idx < 0 || idx >= MaxBackends)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_gcs_block: MyBackendId=%d out of [1, MaxBackends=%d] range",
						(int) MyBackendId, MaxBackends)));
	return &gcs_block_backend_blocks[idx];
}

static ClusterGcsBlockOutstandingSlot *
gcs_block_reserve_slot(BufferTag tag, uint8 transition_id, int32 master_node,
					   uint64 *out_request_id)
{
	ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
	ClusterGcsBlockOutstandingSlot *slot = NULL;
	int			i;

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++)
	{
		if (!blk->slots[i].in_use)
		{
			slot = &blk->slots[i];
			slot->in_use = true;
			slot->reply_received = false;
			slot->request_id = blk->next_request_id++;
			slot->transition_id = transition_id;
			slot->tag = tag;
			slot->master_node = master_node;
			*out_request_id = slot->request_id;
			break;
		}
	}
	LWLockRelease(&blk->lock.lock);

	if (slot == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
				 errmsg("cluster_gcs_block: outstanding-block table full (max %d per backend)",
						MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND),
				 errhint("Reduce concurrent block-ship acquisitions; "
						 "per-backend cap GUC may land in spec-2.34+.")));
	return slot;
}

static void
gcs_block_release_slot(ClusterGcsBlockOutstandingSlot *slot)
{
	ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	slot->in_use = false;
	slot->reply_received = false;
	slot->request_id = 0;
	slot->transition_id = 0;
	slot->master_node = -1;
	LWLockRelease(&blk->lock.lock);
}


/* ============================================================
 * Checksum + block install helpers.
 * ============================================================ */

static uint32
gcs_block_compute_checksum(const char *block_data)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, block_data, GCS_BLOCK_DATA_SIZE);
	FIN_CRC32C(crc);
	return (uint32) crc;
}

/*
 * HC84:  install received block bytes into the requester's buffer under
 * content_lock EXCLUSIVE and PageSetLSN to the master-side LSN so recovery
 * sees a monotonic LSN across nodes.
 */
static void
gcs_block_install_block(BufferDesc *buf, const char *block_data, XLogRecPtr page_lsn)
{
	LWLock	   *content_lock;
	Page		page;

	Assert(buf != NULL);
	content_lock = BufferDescriptorGetContentLock(buf);

	LWLockAcquire(content_lock, LW_EXCLUSIVE);
	page = BufferGetPage(BufferDescriptorGetBuffer(buf));
	memcpy(page, block_data, GCS_BLOCK_DATA_SIZE);
	PageSetLSN(page, page_lsn);
	LWLockRelease(content_lock);
}


/* ============================================================
 * Sender API (D3).
 * ============================================================ */

void
cluster_gcs_send_block_request_and_wait(BufferDesc *buf,
										PcmLockTransition transition_id,
										int master_node)
{
	ClusterGcsBlockOutstandingSlot *slot;
	uint64		request_id = 0;
	GcsBlockRequestPayload payload;
	TimestampTz deadline;
	BufferTag	tag;
	bool		got_reply = false;
	bool		granted = false;
	bool		granted_storage_fallback = false;
	uint8		final_status = GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE;
	XLogRecPtr	final_page_lsn = InvalidXLogRecPtr;

	if (buf == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_gcs_send_block_request_and_wait: NULL BufferDesc")));

	if (transition_id < PCM_TRANS_N_TO_S || transition_id > PCM_TRANS_S_TO_X_CLEANOUT)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_gcs_send_block_request_and_wait: illegal transition_id=%d",
						(int) transition_id)));

	tag = buf->tag;
	slot = gcs_block_reserve_slot(tag, (uint8) transition_id, master_node, &request_id);

	memset(&payload, 0, sizeof(payload));
	payload.request_id = request_id;
	payload.epoch = cluster_epoch_get_current();
	payload.tag = tag;
	payload.sender_node = cluster_node_id;
	payload.requester_backend_id = (int32) MyBackendId;	/* HC80 compound key */
	payload.transition_id = (uint8) transition_id;

	pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_request_count, 1);

	PG_TRY();
	{
		if (cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, master_node,
									 &payload, sizeof(payload)) != CLUSTER_IC_SEND_DONE)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("cluster_gcs_block: failed to send GCS_BLOCK_REQUEST to node %d",
							master_node)));

		/* HC85: timeout via cluster.gcs_reply_timeout_ms PGC_SUSET GUC. */
		deadline = GetCurrentTimestamp()
			+ ((TimestampTz) cluster_gcs_reply_timeout_ms) * (TimestampTz) 1000;

		ConditionVariablePrepareToSleep(&slot->reply_cv);
		for (;;)
		{
			TimestampTz now;
			long		timeout_ms;
			ClusterGcsBlockBackendBlock *blk = gcs_block_my_block();
			bool		have_reply;

			LWLockAcquire(&blk->lock.lock, LW_SHARED);
			have_reply = slot->in_use && slot->reply_received;
			LWLockRelease(&blk->lock.lock);
			if (have_reply)
			{
				got_reply = true;
				break;
			}

			now = GetCurrentTimestamp();
			if (now >= deadline)
			{
				pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_timeout_count, 1);
				break;
			}
			timeout_ms = (long) ((deadline - now) / 1000);
			if (timeout_ms <= 0)
				timeout_ms = 1;
			(void) ConditionVariableTimedSleep(&slot->reply_cv, timeout_ms,
											   WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
		}
		ConditionVariableCancelSleep();

		if (got_reply)
		{
			final_status = slot->reply_header.status;
			final_page_lsn = (XLogRecPtr) slot->reply_header.page_lsn;

			if (final_status == GCS_BLOCK_REPLY_GRANTED)
			{
				uint32		expected = slot->reply_header.checksum;
				uint32		got = gcs_block_compute_checksum(slot->reply_block_data);

				if (expected != got)
				{
					/* HC83 fail-closed:  do not pollute buffer. */
					pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_checksum_fail_count, 1);
					final_status = GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL;
				}
				else
				{
					gcs_block_install_block(buf, slot->reply_block_data, final_page_lsn);
					granted = true;
				}
			}
			else if (final_status == GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK)
			{
				/* HC88: requester keeps ReadBuffer() page; no install. */
				granted_storage_fallback = true;
			}
		}
	}
	PG_CATCH();
	{
		gcs_block_release_slot(slot);
		PG_RE_THROW();
	}
	PG_END_TRY();

	gcs_block_release_slot(slot);

	if (granted || granted_storage_fallback)
		return;

	switch ((GcsBlockReplyStatus) final_status)
	{
		case GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT:
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cluster_gcs_block: master rejected transition_id=%d as illegal",
							(int) transition_id)));
			break;
		case GCS_BLOCK_REPLY_DENIED_EPOCH_STALE:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cluster_gcs_block: request rejected due to stale epoch"),
					 errhint("Reconfig cascading invalidation lands in spec-2.34.")));
			break;
		case GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL:
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cluster_gcs_block: received block failed CRC32C verify"),
					 errhint("Possible wire-ABI drift or network corruption.")));
			break;
		case GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER:
			pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_master_not_holder_count, 1);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cluster_gcs_block: master does not hold tag and state != N"),
					 errhint("Cross-node holder migration handling lands in spec-2.34+.")));
			break;
		case GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE:
		default:
			if (!got_reply)
				ereport(ERROR,
						(errcode(ERRCODE_QUERY_CANCELED),
						 errmsg("cluster_gcs_block: reply timeout after %d ms",
								cluster_gcs_reply_timeout_ms),
						 errhint("Adjust cluster.gcs_reply_timeout_ms or wait for "
								 "spec-2.34 retransmit.")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cluster_gcs_block: transition denied (status=%d)",
								(int) final_status)));
			break;
		case GCS_BLOCK_REPLY_GRANTED:
		case GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK:
			Assert(false);		/* handled above */
			break;
	}
}


/* ============================================================
 * Receiver: master-side (D5).
 *
 *	HC82 invariant: XLogFlush(page_lsn) BEFORE shipping bytes;  enforced by
 *	cluster_bufmgr_copy_block_for_gcs (D4).  HC88: master-not-holder + state=N
 *	→ GRANTED_STORAGE_FALLBACK; state!=N → DENIED_MASTER_NOT_HOLDER fail-closed.
 *	HC89 revalidation single-retry lives inside the bufmgr helper.
 *
 *	Transition apply MUST NOT precede buffer availability decision (HC88).
 * ============================================================ */

static void
gcs_block_send_reply(int32 dest_node,
					 const GcsBlockRequestPayload *req,
					 GcsBlockReplyStatus status,
					 XLogRecPtr page_lsn,
					 const char *block_data)
{
	/*
	 * Reply payload = header (48B) + 8192B block_data.  Heap-alloc here so
	 * the envelope encoder can carry the full 8240B contiguous buffer; sender
	 * loops on shmem outstanding slot which stores the decoded form.
	 */
	GcsBlockReplyHeader *hdr;
	char	   *buf;
	uint32		total = (uint32) (sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE);

	buf = (char *) palloc0(total);
	hdr = (GcsBlockReplyHeader *) buf;

	hdr->request_id = req->request_id;
	hdr->page_lsn = (uint64) page_lsn;
	hdr->epoch = cluster_epoch_get_current();
	hdr->sender_node = cluster_node_id;
	hdr->requester_backend_id = req->requester_backend_id;
	hdr->transition_id = req->transition_id;
	hdr->status = (uint8) status;

	if (status == GCS_BLOCK_REPLY_GRANTED && block_data != NULL)
	{
		memcpy(buf + sizeof(GcsBlockReplyHeader), block_data, GCS_BLOCK_DATA_SIZE);
		hdr->checksum = gcs_block_compute_checksum(block_data);
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_ship_bytes_total,
								GCS_BLOCK_DATA_SIZE);
	}
	else
	{
		/* GRANTED_STORAGE_FALLBACK + all DENIED_*: zero block_data + checksum. */
		hdr->checksum = gcs_block_compute_checksum(buf + sizeof(GcsBlockReplyHeader));
	}

	(void) cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, dest_node, buf, total);

	pfree(buf);
}

void
cluster_gcs_handle_block_request_envelope(const ClusterICEnvelope *env,
										  const void *payload)
{
	const GcsBlockRequestPayload *req;
	uint64		current_epoch;
	PcmLockMode	state;
	bool		found;
	char		block_buf[GCS_BLOCK_DATA_SIZE];
	XLogRecPtr	page_lsn = InvalidXLogRecPtr;

	(void) env;

	if (env == NULL || payload == NULL ||
		env->payload_length != sizeof(GcsBlockRequestPayload))
		return;

	req = (const GcsBlockRequestPayload *) payload;

	/* HC75 range + spec-2.30 validator integration. */
	if (req->transition_id < PCM_TRANS_N_TO_S ||
		req->transition_id > PCM_TRANS_S_TO_X_CLEANOUT)
	{
		gcs_block_send_reply(req->sender_node, req,
							 GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT,
							 InvalidXLogRecPtr, NULL);
		return;
	}

	/* HC73 epoch freshness. */
	current_epoch = cluster_epoch_get_current();
	if (req->epoch < current_epoch)
	{
		gcs_block_send_reply(req->sender_node, req,
							 GCS_BLOCK_REPLY_DENIED_EPOCH_STALE,
							 InvalidXLogRecPtr, NULL);
		return;
	}

	/*
	 * HC88: inspect availability before mutating PCM state.  Master is an
	 * ownership coordinator, not necessarily a local data holder.
	 *  - no buffer && state == N: GRANTED_STORAGE_FALLBACK (apply transition,
	 *    requester reads from shared storage)
	 *  - no buffer && state != N: DENIED_MASTER_NOT_HOLDER (fail-closed)
	 *  - buffer present: D4 helper handles HC82 + HC89 then reply GRANTED
	 */
	state = cluster_pcm_lock_query(req->tag);
	found = cluster_bufmgr_probe_block_for_gcs(req->tag);

	if (!found && state == PCM_LOCK_MODE_N)
	{
		if (!cluster_pcm_lock_apply_gcs_transition(req->tag,
												   (PcmLockTransition) req->transition_id,
												   req->sender_node))
		{
			gcs_block_send_reply(req->sender_node, req,
								 GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE,
								 InvalidXLogRecPtr, NULL);
			return;
		}
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_storage_fallback_count, 1);
		gcs_block_send_reply(req->sender_node, req,
							 GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK,
							 InvalidXLogRecPtr, NULL);
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
		return;
	}

	if (!found)
	{
		pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_master_not_holder_count, 1);
		gcs_block_send_reply(req->sender_node, req,
							 GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER,
							 InvalidXLogRecPtr, NULL);
		return;
	}

	/*
	 * D4 bufmgr helper performs HC82 XLogFlush(page_lsn) + content_lock dance
	 * + HC89 single-retry revalidation.  Returns false if revalidation cannot
	 * stabilize after one retry → DENIED_MASTER_NOT_HOLDER fail-closed.
	 */
	if (!cluster_bufmgr_copy_block_for_gcs(req->tag, &page_lsn, block_buf))
	{
		gcs_block_send_reply(req->sender_node, req,
							 GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER,
							 InvalidXLogRecPtr, NULL);
		return;
	}
	pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_wal_flush_before_ship_count, 1);

	/* HC77: master-side is the single transition-apply owner. */
	if (!cluster_pcm_lock_apply_gcs_transition(req->tag,
											   (PcmLockTransition) req->transition_id,
											   req->sender_node))
	{
		gcs_block_send_reply(req->sender_node, req,
							 GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE,
							 InvalidXLogRecPtr, NULL);
		return;
	}

	gcs_block_send_reply(req->sender_node, req,
						 GCS_BLOCK_REPLY_GRANTED, page_lsn, block_buf);
	pg_atomic_fetch_add_u64(&ClusterGcsBlock->block_reply_count, 1);
}


/* ============================================================
 * Receiver: sender-side (D6).
 *
 *	HC80 compound key (requester_backend_id, request_id) so this handler
 *	does NOT scan all backends to find the matching outstanding slot — it
 *	indexes directly via requester_backend_id.
 * ============================================================ */

void
cluster_gcs_handle_block_reply_envelope(const ClusterICEnvelope *env,
										const void *payload)
{
	const GcsBlockReplyHeader *hdr;
	const char *block_data;
	uint32		expected_size;
	int			backend_idx;
	ClusterGcsBlockBackendBlock *blk;
	int			i;

	(void) env;

	expected_size = (uint32) (sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE);
	if (env == NULL || payload == NULL || env->payload_length != expected_size)
		return;

	hdr = (const GcsBlockReplyHeader *) payload;
	block_data = ((const char *) payload) + sizeof(GcsBlockReplyHeader);

	/* HC80: direct index by requester_backend_id (1..MaxBackends → 0..MaxBackends-1). */
	backend_idx = hdr->requester_backend_id - 1;
	if (backend_idx < 0 || backend_idx >= MaxBackends)
		return;					/* malformed key; drop */

	blk = &gcs_block_backend_blocks[backend_idx];

	LWLockAcquire(&blk->lock.lock, LW_EXCLUSIVE);
	for (i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++)
	{
		ClusterGcsBlockOutstandingSlot *slot = &blk->slots[i];

		if (slot->in_use && slot->request_id == hdr->request_id)
		{
			slot->reply_header = *hdr;
			memcpy(slot->reply_block_data, block_data, GCS_BLOCK_DATA_SIZE);
			slot->reply_received = true;
			ConditionVariableSignal(&slot->reply_cv);
			LWLockRelease(&blk->lock.lock);
			return;
		}
	}
	LWLockRelease(&blk->lock.lock);
	/* No matching slot — stale/late reply; drop silently (HC74 semantics). */
}


/* ============================================================
 * Dispatch table registration.
 * ============================================================ */

static const ClusterICMsgTypeInfo gcs_block_request_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REQUEST,
	.name = "gcs_block_request",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BACKEND | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_request_envelope,
};

static const ClusterICMsgTypeInfo gcs_block_reply_info = {
	.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY,
	.name = "gcs_block_reply",
	.allowed_producer_mask = CLUSTER_IC_PRODUCER_BACKEND | CLUSTER_IC_PRODUCER_LMON,
	.broadcast_ok = false,
	.handler = cluster_gcs_handle_block_reply_envelope,
};

void
cluster_gcs_register_block_msg_types(void)
{
	cluster_ic_register_msg_type(&gcs_block_request_info);
	cluster_ic_register_msg_type(&gcs_block_reply_info);
}


/* ============================================================
 * Observability accessors (dump_gcs +8 NEW rows for block plane).
 * ============================================================ */

uint64
cluster_gcs_get_block_request_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_request_count) : 0;
}

uint64
cluster_gcs_get_block_reply_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_reply_count) : 0;
}

uint64
cluster_gcs_get_block_timeout_count(void)
{
	return ClusterGcsBlock ? pg_atomic_read_u64(&ClusterGcsBlock->block_timeout_count) : 0;
}

uint64
cluster_gcs_get_block_checksum_fail_count(void)
{
	return ClusterGcsBlock ?
		pg_atomic_read_u64(&ClusterGcsBlock->block_checksum_fail_count) : 0;
}

uint64
cluster_gcs_get_block_storage_fallback_count(void)
{
	return ClusterGcsBlock ?
		pg_atomic_read_u64(&ClusterGcsBlock->block_storage_fallback_count) : 0;
}

uint64
cluster_gcs_get_block_master_not_holder_count(void)
{
	return ClusterGcsBlock ?
		pg_atomic_read_u64(&ClusterGcsBlock->block_master_not_holder_count) : 0;
}

uint64
cluster_gcs_get_block_wal_flush_before_ship_count(void)
{
	return ClusterGcsBlock ?
		pg_atomic_read_u64(&ClusterGcsBlock->block_wal_flush_before_ship_count) : 0;
}

uint64
cluster_gcs_get_block_ship_bytes_total(void)
{
	return ClusterGcsBlock ?
		pg_atomic_read_u64(&ClusterGcsBlock->block_ship_bytes_total) : 0;
}


#endif							/* USE_PGRAC_CLUSTER */
