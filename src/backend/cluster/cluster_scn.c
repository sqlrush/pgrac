/*-------------------------------------------------------------------------
 *
 * cluster_scn.c
 *	  pgrac cluster SCN encoding + comparison + single-node advance —
 *	  Stage 1.15 implementation of docs/scn-protocol-design.md §3.1 +
 *	  §3.2.1 cmp contract.
 *
 *	  Spec-1.15 single-node scope:
 *	    - 3 cmp functions (time / total / recovery) per §3.2.1; raw
 *	      `<` / `==` / `>` on SCN forbidden by CI grep gate (D8).
 *	    - cluster_scn_advance() under LW_EXCLUSIVE; ++current_local_scn
 *	      with wraparound watermark hooks.
 *	    - cluster_scn_observe(remote) updates max_observed_remote
 *	      statistic only; does NOT bump local_scn (spec-1.16 Lamport
 *	      observe).
 *	    - cluster_scn_current() / 6 read-only accessors via LW_SHARED.
 *
 *	  Stage 1.15 NOT included (deferred):
 *	    - BOC (broadcast on commit) 100us flush — spec-1.16+
 *	    - Piggyback (cross-instance SCN tracking) — Stage 2+
 *	    - Persistence (control file / shared FS / WAL hosting) — spec-1.16
 *	    - Lamport bump in observe() — spec-1.16
 *	    - Reconfig SCN freeze protocol — Stage 2+ reconfig
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_scn.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.15-scn-encoding-layer.md (frozen 2026-05-04)
 *	  Design: docs/scn-protocol-design.md v1.1 §3.2 + §3.2.1
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_scn.h"

#include "cluster/cluster_guc.h"	/* cluster_node_id GUC */
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT */
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"


/*
 * StaticAssertDecl: encoding invariants (compile-time sanity).
 */
StaticAssertDecl(SCN_INVARIANT_BITS_SUM == 64, "SCN_NODE_ID_BITS + SCN_LOCAL_BITS must equal 64");
StaticAssertDecl(sizeof(SCN) == SCN_INVARIANT_SIZE, "SCN must be 8 bytes (uint64 alias)");


/*
 * Shmem state struct.  Single writer for current_local_scn / pid /
 * timestamps: cluster_scn_advance() under LW_EXCLUSIVE.  Multi-reader
 * via cluster_scn_current() / accessor functions under LW_SHARED.
 */
typedef struct ClusterScnSharedState {
	LWLock lwlock;						  /* LWTRANCHE_CLUSTER_SCN */
	NodeId node_id;						  /* set by shmem_init from cluster_node_id GUC */
	uint64 current_local_scn;			  /* 56-bit; ++ under lwlock */
	uint64 max_observed_remote_scn;		  /* spec-1.15 stat only */
	pg_atomic_uint64 total_advance_count; /* monotone counter */
	TimestampTz initialized_at;
	TimestampTz last_advance_at;
} ClusterScnSharedState;


static ClusterScnSharedState *cluster_scn_state = NULL;

/* WARNING throttle: at most 1/min */
static TimestampTz last_warn_emitted_at = 0;


/*
 * ============================================================
 * Comparison functions (spec-1.15 §3.2.1; Q2 + L4)
 * ============================================================
 */

/*
 * scn_time_cmp -- visibility / MVCC ordering.
 *
 *	Only local_scn matters; node_id high bits ignored.  Critical
 *	contract for cross-instance visibility (HeapTupleSatisfiesMVCC
 *	etc.).
 */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}

/*
 * scn_total_cmp -- ITL / global unique ordering.
 *
 *	Spec-1.15 L4: NOT raw `CMP(a, b)`.  Raw uint64 comparison would
 *	let high node_id bits dominate when local_scn is equal across
 *	nodes; ITL slot ordering would degenerate to node_id-priority on
 *	cross-node ties, breaking the time-priority contract.  Implement
 *	as local_scn → node_id two-level tie-break.
 */
int
scn_total_cmp(SCN a, SCN b)
{
	int c = scn_time_cmp(a, b);

	if (c != 0)
		return c;

	{
		NodeId na = scn_node_id(a);
		NodeId nb = scn_node_id(b);

		if (na < nb)
			return -1;
		if (na > nb)
			return 1;
		return 0;
	}
}

/*
 * scn_recovery_cmp -- WAL k-way merge / standby apply ordering.
 *
 *	Three-level tie-break: local_scn → LSN → node_id.  Ensures
 *	deterministic recovery order across instances on identical SCN
 *	values (rare but possible at cluster boundaries).
 */
int
scn_recovery_cmp(SCN a, XLogRecPtr a_lsn, NodeId a_node, SCN b, XLogRecPtr b_lsn, NodeId b_node)
{
	int c = scn_time_cmp(a, b);

	if (c != 0)
		return c;

	if (a_lsn < b_lsn)
		return -1;
	if (a_lsn > b_lsn)
		return 1;

	if (a_node < b_node)
		return -1;
	if (a_node > b_node)
		return 1;
	return 0;
}


/*
 * ============================================================
 * Wraparound watermark check (spec-1.15 D7; Q9 + L6)
 * ============================================================
 *
 *	Caller holds cluster_scn_state->lwlock LW_EXCLUSIVE.
 */
static void
scn_check_wraparound_watermark(uint64 current)
{
	if (current >= SCN_WRAP_PANIC_THRESHOLD) {
		ereport(
			PANIC,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("cluster_scn: local_scn (%lu) reached PANIC threshold (2^55 ≈ 228000 years of "
					"advance)",
					(unsigned long)current),
			 errhint(
				 "This is a theoretical sentinel; reaching it indicates a runaway advance loop or "
				 "external manipulation.  spec-1.16 introduces real wraparound protection.")));
	} else if (current >= SCN_WRAP_WARNING_THRESHOLD) {
		TimestampTz now_ts = GetCurrentTimestamp();

		/* Throttle: at most 1 WARNING per minute. */
		if (last_warn_emitted_at == 0
			|| TimestampDifferenceExceeds(last_warn_emitted_at, now_ts, 60 * 1000)) {
			last_warn_emitted_at = now_ts;
			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					 errmsg("cluster_scn: local_scn (%lu) crossed WARNING threshold (2^50 ≈ 3568 "
							"years of advance)",
							(unsigned long)current),
					 errhint("Theoretical sentinel for monitoring discipline; spec-1.16 implements "
							 "full wrap protection.  WARNING throttled to 1/min.")));
		}
	}
}


/*
 * ============================================================
 * Public API
 * ============================================================
 */

/*
 * cluster_scn_advance -- bump local SCN by 1 and return encoded SCN.
 *
 *	Spec-1.15 Q3: LW_EXCLUSIVE around the multi-word critical section
 *	(++ current_local_scn + wraparound check + last_advance_at +
 *	total_advance_count).  Single-node advance frequency << 1kHz; 50ns
 *	critical section is acceptable.  spec-1.16 BOC may switch to
 *	atomic + LWLock hybrid if frequency rises.
 *
 *	Spec-1.15 L3: ereport(ERROR) when cluster.node_id is unset (-1) or
 *	out of valid range (>127).  Caller must SET cluster.node_id=0..127
 *	before first advance.
 */
SCN
cluster_scn_advance(void)
{
	SCN encoded;
	uint64 new_local;
	NodeId node;

	Assert(cluster_scn_state != NULL);

	CLUSTER_INJECTION_POINT("cluster-scn-advance-pre");

	LWLockAcquire(&cluster_scn_state->lwlock, LW_EXCLUSIVE);

	node = cluster_scn_state->node_id;
	if (!SCN_NODE_ID_VALID(node)) {
		LWLockRelease(&cluster_scn_state->lwlock);
		ereport(
			ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("cluster_scn_advance: cluster.node_id (%d) is not in the valid range 0..%d",
					node, SCN_MAX_VALID_NODE_ID),
			 errhint("Set cluster.node_id to a value in 0..127 before advancing SCN.  -1 is the "
					 "unset / single-node-fallback sentinel and is not valid for SCN encoding.")));
	}

	new_local = ++cluster_scn_state->current_local_scn;

	scn_check_wraparound_watermark(new_local);

	cluster_scn_state->last_advance_at = GetCurrentTimestamp();

	pg_atomic_fetch_add_u64(&cluster_scn_state->total_advance_count, 1);

	encoded = scn_encode(node, new_local);

	LWLockRelease(&cluster_scn_state->lwlock);

	CLUSTER_INJECTION_POINT("cluster-scn-advance-post");

	return encoded;
}

/*
 * cluster_scn_observe -- update max_observed_remote statistic.
 *
 *	Spec-1.15 Q4 + L5: single-node observe is a no-op for current_
 *	local_scn (no Lamport bump).  Only updates max_observed_remote_scn
 *	statistic for monitoring.  spec-1.16 implements true Lamport bump
 *	(current_local = max(current_local, scn_local(remote) + 1)).
 *
 *	Silently ignore InvalidScn input to ease forward-compat with
 *	multi-node code paths that may pass remote SCN before any cross-
 *	node traffic exists.
 */
void
cluster_scn_observe(SCN remote_scn)
{
	uint64 remote_local;

	Assert(cluster_scn_state != NULL);

	CLUSTER_INJECTION_POINT("cluster-scn-observe-entry");

	if (!SCN_VALID(remote_scn))
		return;

	remote_local = scn_local(remote_scn);

	LWLockAcquire(&cluster_scn_state->lwlock, LW_EXCLUSIVE);

	if (remote_local > cluster_scn_state->max_observed_remote_scn)
		cluster_scn_state->max_observed_remote_scn = remote_local;

	/*
	 * Spec-1.15 L5: explicitly DO NOT bump current_local_scn here.
	 * Lamport observe is spec-1.16 territory.  Statistic-only.
	 */

	LWLockRelease(&cluster_scn_state->lwlock);
}

/*
 * cluster_scn_current -- read current encoded SCN without advancing.
 *
 *	Single-node fallback (cluster.node_id = -1): returns InvalidScn
 *	rather than asserting in scn_encode.  Spec-1.15 L3: SCN encoding
 *	requires a valid node_id (0..127) so single-node mode (-1) cannot
 *	produce real SCN values; cluster_scn_advance() reports the same
 *	condition via ereport(ERROR).  Read paths (dump_scn, SRF) must not
 *	crash when called before cluster.node_id is configured.
 */
SCN
cluster_scn_current(void)
{
	SCN encoded;
	NodeId node;
	uint64 local;

	Assert(cluster_scn_state != NULL);

	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	node = cluster_scn_state->node_id;
	local = cluster_scn_state->current_local_scn;
	LWLockRelease(&cluster_scn_state->lwlock);

	if (!SCN_NODE_ID_VALID(node))
		return InvalidScn;

	encoded = scn_encode(node, local);
	return encoded;
}

/*
 * Read-only accessors (LW_SHARED).  Used by dump_scn (cluster_debug.c)
 * and TAP regression for SQL view validation.
 */
uint64
cluster_scn_advance_count(void)
{
	Assert(cluster_scn_state != NULL);
	return pg_atomic_read_u64(&cluster_scn_state->total_advance_count);
}

uint64
cluster_scn_max_observed_remote(void)
{
	uint64 v;

	Assert(cluster_scn_state != NULL);
	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	v = cluster_scn_state->max_observed_remote_scn;
	LWLockRelease(&cluster_scn_state->lwlock);
	return v;
}

NodeId
cluster_scn_node_id(void)
{
	Assert(cluster_scn_state != NULL);
	return cluster_scn_state->node_id; /* set once at shmem_init */
}

TimestampTz
cluster_scn_initialized_at(void)
{
	Assert(cluster_scn_state != NULL);
	return cluster_scn_state->initialized_at;
}

TimestampTz
cluster_scn_last_advance_at(void)
{
	TimestampTz v;

	Assert(cluster_scn_state != NULL);
	LWLockAcquire(&cluster_scn_state->lwlock, LW_SHARED);
	v = cluster_scn_state->last_advance_at;
	LWLockRelease(&cluster_scn_state->lwlock);
	return v;
}


/*
 * ============================================================
 * Shmem hookup
 * ============================================================
 */

Size
cluster_scn_shmem_size(void)
{
	return sizeof(ClusterScnSharedState);
}

void
cluster_scn_shmem_init(void)
{
	bool found;

	cluster_scn_state = ShmemInitStruct("pgrac cluster scn", cluster_scn_shmem_size(), &found);
	if (!found) {
		LWLockInitialize(&cluster_scn_state->lwlock, LWTRANCHE_CLUSTER_SCN);
		cluster_scn_state->node_id = cluster_node_id; /* may be -1; advance() rejects */
		cluster_scn_state->current_local_scn = 0;
		cluster_scn_state->max_observed_remote_scn = 0;
		pg_atomic_init_u64(&cluster_scn_state->total_advance_count, 0);
		cluster_scn_state->initialized_at = GetCurrentTimestamp();
		cluster_scn_state->last_advance_at = 0;
	}
}

static const ClusterShmemRegion cluster_scn_region = {
	.name = "pgrac cluster scn",
	.size_fn = cluster_scn_shmem_size,
	.init_fn = cluster_scn_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_scn",
	.reserved_flags = 0,
};


void
cluster_scn_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_scn_region);
}


/*
 * ============================================================
 * SQL UDF wrappers (spec-1.15 D11; Q7 + L2 + L7)
 *
 *	Wrappers take _sql suffix to avoid C symbol collision with the
 *	C API names (cluster_scn_advance / current / observe).  Mutating
 *	APIs (advance / observe) gate on superuser; current is read-only
 *	and public.
 * ============================================================
 */

#endif /* USE_PGRAC_CLUSTER */


/* ============================================================
 * SQL UDF wrappers (always linked; body guarded by USE_PGRAC_CLUSTER).
 * ============================================================
 *
 *	pg_proc.dat references cluster_scn_advance_sql / current_sql /
 *	observe_sql unconditionally, so these symbols must resolve at link
 *	time even in --disable-cluster builds.  In disable mode the bodies
 *	raise ERRCODE_FEATURE_NOT_SUPPORTED.
 *
 *	Wrappers take _sql suffix to avoid C symbol collision with the
 *	C API names (cluster_scn_advance / current / observe).  Mutating
 *	APIs (advance / observe) gate on superuser; current is read-only
 *	and public.
 */

PG_FUNCTION_INFO_V1(cluster_scn_advance_sql);
PG_FUNCTION_INFO_V1(cluster_scn_current_sql);
PG_FUNCTION_INFO_V1(cluster_scn_observe_sql);

Datum
cluster_scn_advance_sql(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	SCN scn;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_scn_advance() is restricted to superuser")));

	scn = cluster_scn_advance();
	PG_RETURN_INT64((int64)scn);
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_scn_advance() requires --enable-cluster")));
	PG_RETURN_INT64(0);
#endif
}

Datum
cluster_scn_current_sql(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	SCN scn = cluster_scn_current();

	PG_RETURN_INT64((int64)scn);
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_scn_current() requires --enable-cluster")));
	PG_RETURN_INT64(0);
#endif
}

Datum
cluster_scn_observe_sql(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	int64 remote_int = PG_GETARG_INT64(0);

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_scn_observe() is restricted to superuser")));

	cluster_scn_observe((SCN)remote_int);
	PG_RETURN_VOID();
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_scn_observe() requires --enable-cluster")));
	PG_RETURN_VOID();
#endif
}
