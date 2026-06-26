/*-------------------------------------------------------------------------
 *
 * cluster_lmd_probe_collector.c
 *	  pgrac LMD cross-node DEADLOCK_REPORT collector — shared-memory
 *	  LMON->LMD hand-off (spec-5.8 D8 / Option B).
 *
 *	  See cluster_lmd_probe_collector.h for the role split and the Rule 8.A
 *	  safety contract.  The shared region holds one in-flight probe: its
 *	  probe_id, the expected-responder bitmap (armed by LMD), the
 *	  received-responder bitmap + count (set by LMON), an overflow flag, and
 *	  a bounded ring of the remote wait-for edges accumulated this round.
 *
 *	  Concurrency: a single region LWLock serializes the LMON append path
 *	  against the LMD arm / drain / reset path.  Only one probe is in flight
 *	  at a time (the LMD coordinator scan is single-threaded and serializes
 *	  its own rounds), so probe_id is the round identity and any REPORT not
 *	  matching it is dropped stale.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lmd_probe_collector.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.8-full-cross-node-deadlock-detector.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES */
#include "cluster/cluster_ges.h"  /* GesDeadlockReportHeader */
#include "cluster/cluster_guc.h"  /* cluster_lmd_max_wait_edges */
#include "cluster/cluster_lmd.h"  /* ClusterLmdWaitEdge + cluster_lmd_probe_member_admit */
#include "cluster/cluster_lmd_probe_collector.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#ifdef USE_PGRAC_CLUSTER

typedef struct ClusterLmdProbeShmem {
	LWLock lock; /* serializes LMON append vs LMD arm/drain/reset */

	uint64 probe_id; /* current in-flight round; 0 = idle (LMD arms) */
	uint64 expected_lo;
	uint64 expected_hi; /* probed peers (LMD arms) */
	uint64 received_lo;
	uint64 received_hi; /* distinct accepted responders (LMON sets) */
	int32 n_received;	/* popcount(received) */
	int32 n_edges;		/* remote edges currently in the ring */
	int32 max_edges;	/* ring capacity (snapshot of GUC at init) */
	bool overflow;		/* a REPORT did not fit — round is INCOMPLETE */

	/* Counters (spec-5.8 D8 — constraint 6). */
	pg_atomic_uint64 report_enqueue_count;
	pg_atomic_uint64 drop_stale_count;
	pg_atomic_uint64 drop_duplicate_count;
	pg_atomic_uint64 queue_full_count;
	pg_atomic_uint64 partial_report_count;

	ClusterLmdWaitEdge edges[FLEXIBLE_ARRAY_MEMBER];
} ClusterLmdProbeShmem;

static ClusterLmdProbeShmem *cluster_lmd_probe = NULL;


/* ============================================================
 * Ring capacity.
 * ============================================================ */

static int
probe_ring_capacity(void)
{
	int cap = cluster_lmd_max_wait_edges;

	if (cap < 64)
		cap = 64;
	if (cap > 65536)
		cap = 65536;
	return cap;
}


/* ============================================================
 * Shmem region.
 * ============================================================ */

Size
cluster_lmd_probe_collector_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return 0;
	return MAXALIGN(offsetof(ClusterLmdProbeShmem, edges)
					+ (Size)probe_ring_capacity() * sizeof(ClusterLmdWaitEdge));
}

void
cluster_lmd_probe_collector_shmem_init(void)
{
	bool found;

	if (IsBootstrapProcessingMode() || !cluster_enabled)
		return;

	cluster_lmd_probe = (ClusterLmdProbeShmem *)ShmemInitStruct(
		"pgrac cluster lmd probe", cluster_lmd_probe_collector_shmem_size(), &found);

	if (!found) {
		LWLockInitialize(&cluster_lmd_probe->lock, LWTRANCHE_CLUSTER_LMD);
		cluster_lmd_probe->probe_id = 0;
		cluster_lmd_probe->expected_lo = 0;
		cluster_lmd_probe->expected_hi = 0;
		cluster_lmd_probe->received_lo = 0;
		cluster_lmd_probe->received_hi = 0;
		cluster_lmd_probe->n_received = 0;
		cluster_lmd_probe->n_edges = 0;
		cluster_lmd_probe->max_edges = probe_ring_capacity();
		cluster_lmd_probe->overflow = false;
		pg_atomic_init_u64(&cluster_lmd_probe->report_enqueue_count, 0);
		pg_atomic_init_u64(&cluster_lmd_probe->drop_stale_count, 0);
		pg_atomic_init_u64(&cluster_lmd_probe->drop_duplicate_count, 0);
		pg_atomic_init_u64(&cluster_lmd_probe->queue_full_count, 0);
		pg_atomic_init_u64(&cluster_lmd_probe->partial_report_count, 0);
	}
}

static const ClusterShmemRegion cluster_lmd_probe_region = {
	.name = "pgrac cluster lmd probe",
	.size_fn = cluster_lmd_probe_collector_shmem_size,
	.init_fn = cluster_lmd_probe_collector_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "spec-5.8 LMD probe collector",
	.reserved_flags = 0,
};

void
cluster_lmd_probe_collector_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lmd_probe_region);
}


/* ============================================================
 * LMD coordinator side: arm / poll / drain / reset.
 * ============================================================ */

void
cluster_lmd_probe_arm(uint64 probe_id, uint64 expected_lo, uint64 expected_hi)
{
	if (cluster_lmd_probe == NULL)
		return;

	LWLockAcquire(&cluster_lmd_probe->lock, LW_EXCLUSIVE);
	cluster_lmd_probe->probe_id = probe_id;
	cluster_lmd_probe->expected_lo = expected_lo;
	cluster_lmd_probe->expected_hi = expected_hi;
	cluster_lmd_probe->received_lo = 0;
	cluster_lmd_probe->received_hi = 0;
	cluster_lmd_probe->n_received = 0;
	cluster_lmd_probe->n_edges = 0;
	cluster_lmd_probe->overflow = false;
	LWLockRelease(&cluster_lmd_probe->lock);
}

int
cluster_lmd_probe_n_received(void)
{
	int n;

	if (cluster_lmd_probe == NULL)
		return 0;

	LWLockAcquire(&cluster_lmd_probe->lock, LW_SHARED);
	n = cluster_lmd_probe->n_received;
	LWLockRelease(&cluster_lmd_probe->lock);
	return n;
}

int
cluster_lmd_probe_drain(ClusterLmdWaitEdge *out_buf, int max_edges, ClusterLmdProbeDrain *out)
{
	int n;

	Assert(out != NULL);
	memset(out, 0, sizeof(*out));

	if (cluster_lmd_probe == NULL)
		return 0;

	LWLockAcquire(&cluster_lmd_probe->lock, LW_EXCLUSIVE);
	n = cluster_lmd_probe->n_edges;
	if (out_buf != NULL && max_edges > 0) {
		if (n > max_edges)
			n = max_edges; /* defensive — ring is capped at max_edges anyway */
		if (n > 0)
			memcpy(out_buf, cluster_lmd_probe->edges, (Size)n * sizeof(ClusterLmdWaitEdge));
	} else {
		n = 0;
	}
	out->n_edges = n;
	out->n_received = cluster_lmd_probe->n_received;
	out->member_lo = cluster_lmd_probe->received_lo;
	out->member_hi = cluster_lmd_probe->received_hi;
	out->overflow = cluster_lmd_probe->overflow;
	LWLockRelease(&cluster_lmd_probe->lock);
	return n;
}

void
cluster_lmd_probe_reset(void)
{
	if (cluster_lmd_probe == NULL)
		return;

	LWLockAcquire(&cluster_lmd_probe->lock, LW_EXCLUSIVE);
	cluster_lmd_probe->probe_id = 0;
	cluster_lmd_probe->expected_lo = 0;
	cluster_lmd_probe->expected_hi = 0;
	cluster_lmd_probe->received_lo = 0;
	cluster_lmd_probe->received_hi = 0;
	cluster_lmd_probe->n_received = 0;
	cluster_lmd_probe->n_edges = 0;
	cluster_lmd_probe->overflow = false;
	LWLockRelease(&cluster_lmd_probe->lock);
}


/* ============================================================
 * LMON receive side: validate + append one REPORT's edges.
 * ============================================================ */

bool
cluster_lmd_probe_collect_receive(const GesDeadlockReportHeader *report, Size report_len)
{
	int32 rid;
	uint32 nedges;
	ClusterLmdProbeAdmit admit;
	const ClusterLmdWaitEdge *report_edges;
	bool accepted = false;

	if (cluster_lmd_probe == NULL || report == NULL)
		return false;
	if (report_len < sizeof(GesDeadlockReportHeader))
		return false;

	rid = (int32)report->responding_node_id;
	nedges = report->nedges;

	/* Bound nedges by what the wire frame actually carries (defense against a
	 * truncated / malformed REPORT claiming more edges than its bytes). */
	if ((Size)nedges * sizeof(ClusterLmdWaitEdge) > report_len - sizeof(GesDeadlockReportHeader))
		return false;

	report_edges
		= (const ClusterLmdWaitEdge *)(((const char *)report) + sizeof(GesDeadlockReportHeader));

	LWLockAcquire(&cluster_lmd_probe->lock, LW_EXCLUSIVE);

	/* Only the current in-flight round (Rule 8.A constraint 3). */
	if (cluster_lmd_probe->probe_id == 0 || report->probe_id != cluster_lmd_probe->probe_id) {
		pg_atomic_fetch_add_u64(&cluster_lmd_probe->drop_stale_count, 1);
		LWLockRelease(&cluster_lmd_probe->lock);
		return false;
	}

	admit = cluster_lmd_probe_member_admit(
		cluster_lmd_probe->expected_lo, cluster_lmd_probe->expected_hi,
		cluster_lmd_probe->received_lo, cluster_lmd_probe->received_hi, rid);
	if (admit == CLUSTER_LMD_PROBE_DROP_UNEXPECTED) {
		pg_atomic_fetch_add_u64(&cluster_lmd_probe->drop_stale_count, 1);
		LWLockRelease(&cluster_lmd_probe->lock);
		return false;
	}
	if (admit == CLUSTER_LMD_PROBE_DROP_DUPLICATE) {
		pg_atomic_fetch_add_u64(&cluster_lmd_probe->drop_duplicate_count, 1);
		LWLockRelease(&cluster_lmd_probe->lock);
		return false;
	}

	/*
	 * Append all of this REPORT's edges, or none.  If they do not fit, mark
	 * the round overflowed and do NOT append a partial set — the LMD drain
	 * treats an overflowed round as incomplete and will not confirm / cancel
	 * (constraints 2 + 5).  A responder whose edges did not fit is NOT marked
	 * received (the round is already doomed to be discarded).
	 */
	if (cluster_lmd_probe->n_edges + (int)nedges > cluster_lmd_probe->max_edges) {
		cluster_lmd_probe->overflow = true;
		pg_atomic_fetch_add_u64(&cluster_lmd_probe->queue_full_count, 1);
		pg_atomic_fetch_add_u64(&cluster_lmd_probe->partial_report_count, 1);
		LWLockRelease(&cluster_lmd_probe->lock);
		return false;
	}

	if (nedges > 0)
		memcpy(&cluster_lmd_probe->edges[cluster_lmd_probe->n_edges], report_edges,
			   (Size)nedges * sizeof(ClusterLmdWaitEdge));
	cluster_lmd_probe->n_edges += (int)nedges;

	if (rid < 64)
		cluster_lmd_probe->received_lo |= (UINT64CONST(1) << rid);
	else
		cluster_lmd_probe->received_hi |= (UINT64CONST(1) << (rid - 64));
	cluster_lmd_probe->n_received++;
	pg_atomic_fetch_add_u64(&cluster_lmd_probe->report_enqueue_count, 1);
	accepted = true;

	LWLockRelease(&cluster_lmd_probe->lock);
	return accepted;
}


/* ============================================================
 * Counters.
 * ============================================================ */

#define DEFINE_PROBE_GET(field)                                                                    \
	uint64 cluster_lmd_probe_##field##_get(void)                                                   \
	{                                                                                              \
		if (cluster_lmd_probe == NULL)                                                             \
			return 0;                                                                              \
		return pg_atomic_read_u64(&cluster_lmd_probe->field);                                      \
	}

DEFINE_PROBE_GET(report_enqueue_count)
DEFINE_PROBE_GET(drop_stale_count)
DEFINE_PROBE_GET(drop_duplicate_count)
DEFINE_PROBE_GET(queue_full_count)
DEFINE_PROBE_GET(partial_report_count)

#undef DEFINE_PROBE_GET

#endif /* USE_PGRAC_CLUSTER */
