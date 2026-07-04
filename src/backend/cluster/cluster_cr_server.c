/*-------------------------------------------------------------------------
 *
 * cluster_cr_server.c
 *	  pgrac spec-6.12b — CR-server runtime: the LMON↔LMS slot handoff and
 *	  the LMON-side result shipping for cross-instance CR construction.
 *
 *	  Split of labour (see cluster_cr_server.h banner):
 *	    LMON (IC dispatch)   validates + parks the CR request in a shmem
 *	                         slot (cluster_lms_cr_submit) — light work
 *	                         only, the dispatch loop never walks undo.
 *	    LMS  (aux process)   drains PENDING slots and constructs the CR
 *	                         page (cluster_lms_cr_drain →
 *	                         cluster_cr_construct_page_for_server); every
 *	                         construction error becomes a DENIED result,
 *	                         never an LMS exit (fail-closed at the
 *	                         requester, Rule 8.A).
 *	    LMON (tick)          ships READY results direct to the requester
 *	                         (cluster_lms_cr_ship_ready) — the 72-byte
 *	                         outbound ring cannot carry a page and only
 *	                         LMON owns the IC connections.
 *
 *	  The slot state word is an atomic; each transition has exactly one
 *	  writer (LMON: FREE→PENDING, READY→FREE; LMS: PENDING→BUSY→READY),
 *	  so no lock is needed beyond the CAS on FREE→PENDING.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_server.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave b)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlog.h" /* GetFlushRecPtr (spec-6.12i live_hwm_lsn) */
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_elog.h" /* cluster_node_id */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h" /* cluster_ic_send_envelope */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_record_api.h" /* tt_retention_rollover_count */
#include "cluster/cluster_undo_smgr.h"		 /* cluster_undo_smgr_read_block */
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/memutils.h"

/*
 * Shmem: the slot table + the published LMS latch pointer (set by LmsMain
 * at entry so the LMON submit path can cut the 100 ms idle-poll latency;
 * a stale pointer after an LMS crash only risks a spurious latch set on a
 * reused PGPROC — benign).
 */
typedef struct ClusterCrServerShared {
	pg_atomic_uint64 lms_latch_ptr; /* (uintptr_t) Latch*; 0 = not running */
	ClusterLmsCrSlot slots[CLUSTER_LMS_CR_SLOTS];
} ClusterCrServerShared;

static ClusterCrServerShared *CrServerShared = NULL;

static Size
cluster_cr_server_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCrServerShared));
}

static void
cluster_cr_server_shmem_init(void)
{
	bool found;

	CrServerShared
		= ShmemInitStruct("pgrac cluster cr server", cluster_cr_server_shmem_size(), &found);

	if (!found) {
		memset(CrServerShared, 0, sizeof(*CrServerShared));
		pg_atomic_init_u64(&CrServerShared->lms_latch_ptr, 0);
		for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++)
			pg_atomic_init_u32(&CrServerShared->slots[i].state, CLUSTER_LMS_CR_FREE);
	}
}

static const ClusterShmemRegion cluster_cr_server_region = {
	.name = "pgrac cluster cr server",
	.size_fn = cluster_cr_server_shmem_size,
	.init_fn = cluster_cr_server_shmem_init,
	.lwlock_count = 0, /* atomic slot states; no lock */
	.owner_subsys = "cluster_cr_server",
	.reserved_flags = 0,
};

void
cluster_cr_server_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_server_region);
}

/* LmsMain lifecycle hooks: publish / retract the LMS wake latch. */
void
cluster_cr_server_publish_lms_latch(struct Latch *latch)
{
	if (CrServerShared != NULL)
		pg_atomic_write_u64(&CrServerShared->lms_latch_ptr, (uint64)(uintptr_t)latch);
}

static void
cr_server_wake_lms(void)
{
	uint64 raw;

	if (CrServerShared == NULL)
		return;
	raw = pg_atomic_read_u64(&CrServerShared->lms_latch_ptr);
	if (raw != 0)
		SetLatch((Latch *)(uintptr_t)raw);
}

/*
 * cluster_lms_cr_submit — LMON dispatch side.  Park a CR request.
 *
 *	The caller (the GCS_BLOCK_FORWARD handler) has already range-checked
 *	the transition id and knows the payload carries the CR flag.  false =
 *	data plane off / no capacity: the caller replies the fail-closed
 *	DENIED immediately (the requester keeps 53R9G — Rule 8.A).
 */
bool
cluster_lms_cr_submit(const GcsBlockForwardPayload *fwd)
{
	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_cr_data_plane)
		return false;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_FREE;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_PENDING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(fwd);
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_CR;

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * cluster_lms_undo_fetch_submit — LMON dispatch side (spec-6.12i D-i1).
 *
 *	Park an undo-TT fetch request.  The caller (the GCS_BLOCK_FORWARD
 *	handler) branches on the undo-fetch flag BEFORE any GRD / holder logic
 *	can interpret the synthetic tag.  false = wave GUC off on this node /
 *	malformed synthetic tag / no capacity: the caller replies the
 *	fail-closed DENIED immediately (the requester keeps 53R97 — Rule 8.A).
 */
bool
cluster_lms_undo_fetch_submit(const GcsBlockForwardPayload *fwd)
{
	uint32 segment_id = 0;
	uint32 block_no = 0;

	if (CrServerShared == NULL || fwd == NULL)
		return false;
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (!GcsBlockUndoFetchTagDecode(fwd->tag, &segment_id, &block_no))
		return false;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_FREE;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_PENDING))
			continue;

		slot->tag = fwd->tag;
		slot->read_scn = InvalidScn; /* no snapshot semantics on this kind */
		slot->request_id = fwd->request_id;
		slot->epoch = fwd->epoch;
		slot->requester_node = fwd->original_requester_node;
		slot->requester_backend = fwd->requester_backend_id;
		slot->reply_master_node = fwd->master_node;
		slot->transition_id = fwd->transition_id;
		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;
		slot->req_kind = (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_FETCH;
		slot->undo_segment_id = segment_id;
		slot->undo_block_no = block_no;
		memset(&slot->undo_auth, 0, sizeof(slot->undo_auth));

		/* Publish the request fields before LMS can observe PENDING. */
		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_PENDING);
		cr_server_wake_lms();
		return true;
	}

	return false; /* all slots busy — fail closed, requester retries/refuses */
}

/*
 * lms_undo_fetch_serve — LMS side of one KIND_UNDO_FETCH slot (spec-6.12i).
 *
 *	Samples the live authority triple FIRST, then reads the block: the
 *	watermark is then conservative relative to the shipped content (every
 *	TT stamp the authority claims coverage for is already in the bytes the
 *	requester receives; a stamp landing between sample and read only makes
 *	the content newer than claimed, which is additive and safe — a stamp
 *	can never be retracted).  live_hwm_lsn = GetFlushRecPtr() is sound as
 *	the "durable AND TT-applied" high-water because the durable TT stamp is
 *	a pre-commit targeted pwrite issued BEFORE the commit record is even
 *	inserted (cluster_tt_durable.h): any commit whose record LSN is at or
 *	below the flush pointer has a peer-readable TT stamp.
 *
 *	Only block 0 (the TT-bearing segment header) is served: undo DATA
 *	blocks can lag their pool image under the spec-3.25 D1b keep-clean
 *	WAL deferral, so shipping them from the file would not be origin-fresh
 *	— refuse fail-closed (feature #119 full undo-block CF is the
 *	downstream forward of this slice).
 *
 *	true = result_page holds the block and slot->undo_auth the co-sampled
 *	triple; false = refuse (caller ships DENIED — requester keeps 53R97).
 */
static bool
lms_undo_fetch_serve(ClusterLmsCrSlot *slot)
{
	if (!cluster_crossnode_runtime_visibility)
		return false;
	if (slot->undo_block_no != 0)
		return false;
	if (slot->undo_segment_id == 0 || slot->undo_segment_id > UINT16_MAX)
		return false;

	/* Co-sample the authority triple BEFORE the content read (see above). */
	slot->undo_auth.origin_epoch = cluster_epoch_get_current();
	slot->undo_auth.live_hwm_lsn = GetFlushRecPtr(NULL);
	slot->undo_auth.tt_generation = cluster_undo_tt_retention_rollover_count();

	/* Serve only SELF-owned undo: the owner derives from this node's own
	 * id, never from the wire (a forged request cannot redirect the read). */
	return cluster_undo_smgr_read_block(slot->undo_segment_id, (uint8)(cluster_node_id + 1),
										slot->undo_block_no, slot->result_page);
}

/*
 * cluster_lms_cr_drain — LMS main-loop side.  Construct every PENDING slot.
 *
 *	The current block must be resident here (this node's undo holds the
 *	newest chains, so it was — or recently was — the writer); a stable copy
 *	is taken with the raw-pin ship helper.  Every failure (not resident,
 *	interleaved homes, snapshot-too-old, corruption, injection) becomes a
 *	DENIED result: the requester keeps its unchanged 53R9G fail-closed and
 *	LMS itself NEVER exits over a serve (PG_TRY + FlushErrorState).
 */
void
cluster_lms_cr_drain(void)
{
	if (CrServerShared == NULL)
		return;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_PENDING;
		PGAlignedBlock cur_copy;
		XLogRecPtr page_lsn = InvalidXLogRecPtr;
		bool partial = false;
		bool constructed = false;

		if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, CLUSTER_LMS_CR_BUSY))
			continue;
		pg_read_barrier(); /* pair with the submit-side publish barrier */

		slot->result_status = (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER;

		/*
		 * spec-6.12i D-i1 — undo-TT fetch kind.  No construction: read the
		 * self-owned TT header block + co-sample the live authority triple.
		 * Every refusal (GUC raced off, non-header block, bad segment, read
		 * failure, injection) keeps the DENIED status — the requester keeps
		 * its unchanged 53R97 fail-closed (Rule 8.A).
		 */
		if (slot->req_kind == (uint8)CLUSTER_LMS_SLOT_KIND_UNDO_FETCH) {
			bool served = false;

			CLUSTER_INJECTION_POINT("cluster-lms-undo-fetch");
			if (!cluster_injection_should_skip("cluster-lms-undo-fetch")) {
				PG_TRY();
				{
					served = lms_undo_fetch_serve(slot);
				}
				PG_CATCH();
				{
					/* Fail-closed serve; keep LMS alive (as the CR kind). */
					served = false;
					MemoryContextSwitchTo(TopMemoryContext);
					FlushErrorState();
				}
				PG_END_TRY();
			}

			if (served) {
				slot->result_status = (uint8)GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT;
				cluster_cr_server_stat_bump(CLUSTER_CR_SERVER_STAT_UNDO_SERVED);
			} else {
				cluster_cr_server_stat_bump(CLUSTER_CR_SERVER_STAT_UNDO_DENIED);
			}

			pg_write_barrier();
			pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_READY);
			continue;
		}

		/* spec-6.12b injection — force the DENIED serve path. */
		CLUSTER_INJECTION_POINT("cluster-lms-cr-construct");
		if (!cluster_injection_should_skip("cluster-lms-cr-construct")
			&& cluster_crossnode_cr_data_plane
			&& cluster_bufmgr_copy_block_for_gcs(slot->tag, &page_lsn, cur_copy.data)) {
			PG_TRY();
			{
				cluster_cr_construct_page_for_server(cur_copy.data, slot->read_scn, slot->tag,
													 slot->result_page, &partial);
				constructed = true;
			}
			PG_CATCH();
			{
				/*
				 * Fail-closed serve: keep the DENIED status and keep LMS
				 * alive.  The taxonomy counters were already bumped by the
				 * construction wrapper; drop the error state entirely (an
				 * aux-process ERROR would otherwise escalate to exit).
				 */
				constructed = false;
				MemoryContextSwitchTo(TopMemoryContext);
				FlushErrorState();
			}
			PG_END_TRY();
		}

		if (constructed) {
			slot->result_status = (uint8)(partial ? GCS_BLOCK_REPLY_CR_RESULT_PARTIAL
												  : GCS_BLOCK_REPLY_CR_RESULT_FULL);
			cluster_cr_server_stat_bump(partial ? CLUSTER_CR_SERVER_STAT_PARTIAL
												: CLUSTER_CR_SERVER_STAT_FULL);
		} else {
			cluster_cr_server_stat_bump(CLUSTER_CR_SERVER_STAT_DENIED);
		}

		pg_write_barrier();
		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_READY);
	}
}

/*
 * cluster_lms_cr_ship_ready — LMON tick side.  Ship READY results.
 *
 *	Builds the standard GCS_BLOCK_REPLY (header + BLCKSZ page) with the
 *	HC109 forwarding-master echo the requester's HC108 chain expects, and
 *	frees the slot.  A DENIED result ships a zero page under a matching
 *	checksum (the requester never consumes DENIED bytes).
 */
void
cluster_lms_cr_ship_ready(void)
{
	if (CrServerShared == NULL)
		return;

	for (int i = 0; i < CLUSTER_LMS_CR_SLOTS; i++) {
		ClusterLmsCrSlot *slot = &CrServerShared->slots[i];
		uint32 expected = CLUSTER_LMS_CR_READY;
		uint32 header_len;
		uint32 total;
		char *buf;
		GcsBlockReplyHeader *hdr;

		if (pg_atomic_read_u32(&slot->state) != CLUSTER_LMS_CR_READY)
			continue;
		(void)expected; /* single shipper (LMON tick); state re-checked above */
		pg_read_barrier();

		header_len = (uint32)sizeof(GcsBlockReplyHeader);
		total = header_len + GCS_BLOCK_DATA_SIZE;
		/* spec-6.12i: a served undo-TT fetch appends the authority trailer
		 * (tt_generation); DENIED undo replies stay v1-sized. */
		if (slot->result_status == (uint8)GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT)
			total += (uint32)sizeof(ClusterGcsUndoAuthTrailer);
		buf = (char *)palloc0(total);
		hdr = (GcsBlockReplyHeader *)buf;
		hdr->request_id = slot->request_id;
		hdr->page_lsn = 0;
		hdr->epoch = cluster_epoch_get_current();
		hdr->sender_node = cluster_node_id;
		hdr->requester_backend_id = slot->requester_backend;
		hdr->transition_id = slot->transition_id;
		hdr->status = slot->result_status;
		GcsBlockReplyHeaderSetForwardingMasterNode(hdr, slot->reply_master_node);

		if (hdr->status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_FULL
			|| hdr->status == (uint8)GCS_BLOCK_REPLY_CR_RESULT_PARTIAL)
			memcpy(buf + header_len, slot->result_page, BLCKSZ);
		else if (hdr->status == (uint8)GCS_BLOCK_REPLY_UNDO_TT_FETCH_RESULT) {
			ClusterGcsUndoAuthTrailer *trailer
				= (ClusterGcsUndoAuthTrailer *)(buf + header_len + GCS_BLOCK_DATA_SIZE);

			/*
			 * spec-6.12i co-sample carriage: the authority the LMS sampled
			 * ATOMICALLY with the content read overrides the ship-time
			 * header fields — epoch carries the sampled origin epoch (the
			 * HC100 `hdr.epoch >= request_epoch` check then drops a
			 * mid-reconfig reply, which IS the D-i3 fail-closed) and
			 * page_lsn carries live_hwm_lsn.
			 */
			hdr->epoch = slot->undo_auth.origin_epoch;
			hdr->page_lsn = slot->undo_auth.live_hwm_lsn;
			memcpy(buf + header_len, slot->result_page, BLCKSZ);
			ClusterGcsUndoAuthTrailerSetTtGeneration(trailer, slot->undo_auth.tt_generation);
		}
		hdr->checksum = cluster_gcs_block_compute_checksum(buf + header_len);

		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, slot->requester_node, buf,
									   total);
		pfree(buf);

		pg_atomic_write_u32(&slot->state, CLUSTER_LMS_CR_FREE);
	}
}

#endif /* USE_PGRAC_CLUSTER */
