/*-------------------------------------------------------------------------
 *
 * cluster_inject.c
 *	  pgrac cluster error-injection framework (Stage 0.27).
 *
 *	  Implements:
 *	    - The compile-time injection-point registry (six points covering
 *	      cluster_init_shmem / cluster_ic_init / cluster_conf_load).
 *	    - cluster_injection_run -- look up the named entry, dispatch the
 *	      armed fault type, increment the lifetime hit counter.
 *	    - Five fault dispatchers (ERROR / WARNING / SLEEP / CRASH / SKIP);
 *	      NONE is the implicit "counter only" path.
 *	    - Two SRFs (cluster_inject_fault / cluster_get_injection_state)
 *	      backing pg_stat_cluster_injections.
 *	    - The GUC startup-arm path: cluster.injection_points lists names
 *	      that should be auto-armed to fault_type=WARNING at backend
 *	      startup (counter + warn).  Wired via the GUC's assign_hook so
 *	      that postgresql.conf changes take effect on the next reload.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_inject.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The two SRF entry points are unconditionally compiled because
 *	  pg_proc.dat references them in both build modes; their bodies
 *	  are #ifdef USE_PGRAC_CLUSTER guarded.  The internal registry,
 *	  hit counters, and dispatcher are compiled out completely on
 *	  --disable-cluster builds (spec-0.3 contract).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "utils/builtins.h"

#include "cluster/cluster_inject.h"


/* SRF info-V1 declarations -- always linked because pg_proc.dat
 * references these regardless of build mode. */
PG_FUNCTION_INFO_V1(cluster_inject_fault);
PG_FUNCTION_INFO_V1(cluster_get_injection_state);


#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * Registry (compile-time static array).
 * ============================================================ */

typedef struct ClusterInjectPoint {
	const char *name;			   /* compile-time string literal */
	pg_atomic_uint64 hits;		   /* lifetime hit counter */
	pg_atomic_uint32 armed_type;   /* ClusterInjectFaultType */
	pg_atomic_uint64 armed_param;  /* sleep us / SKIP cookie */
	pg_atomic_uint32 skip_pending; /* set by SKIP dispatch, read+reset by probe */
} ClusterInjectPoint;

/*
 * The 20 cluster injection points.  Order in this array drives the
 * SRF row order; entries kept sorted by name within each stage block.
 * 6 baseline points were established in spec-0.27; 8 more added at
 * spec-0.30 to cover every stage-0.x already-implemented subsystem
 * with a non-signal-handler function-level hook position; 3 added at
 * spec-1.1 for cluster_shared_fs; 3 more at spec-1.2 for cluster_smgr.
 *
 * Two subsystems deliberately excluded (spec-0.30 §1.4 exceptions):
 *   - cluster_elog: CLUSTER_LOG is a header macro that expands inline
 *     at each call site; cluster_elog.c has only a global variable
 *     definition with no function-level position.
 *   - cluster_signal: cluster_signal.c contains only the
 *     cluster_handle_reconfig_start_interrupt signal handler;
 *     docs/error-injection-design.md §4.2 forbids injection points
 *     inside signal handlers (async-signal-safe constraint).
 *
 * See docs/error-injection-design.md §2.4 for the naming convention
 * and §5 for the Stage 1+ roadmap.
 */
static ClusterInjectPoint cluster_injection_points[] = {
	/* Stage 0.27 baseline + 0.30 sweep (14 entries) */
	{ .name = "cluster-conf-load-success" },
	{ .name = "cluster-conf-parse-fail" },
	{ .name = "cluster-conf-shmem-init" },
	{ .name = "cluster-debug-dump-entry" },
	{ .name = "cluster-guc-init-pre-define" },
	{ .name = "cluster-ic-mock-send-pre-enqueue" },
	{ .name = "cluster-ic-tier-selected" },
	{ .name = "cluster-init-post-shmem" },
	{ .name = "cluster-init-pre-shmem" },
	{ .name = "cluster-init-top" },
	{ .name = "cluster-pgstat-mirror-sync" },
	{ .name = "cluster-shmem-request" },
	{ .name = "cluster-shutdown-top" },
	{ .name = "cluster-views-srf-entry" },

	/* Stage 1.1 cluster_shared_fs (3 entries) */
	{ .name = "cluster-shared-fs-backend-register" },
	{ .name = "cluster-shared-fs-init-top" },
	{ .name = "cluster-shared-fs-local-open" },

	/* Stage 1.2 cluster_smgr (3 entries) */
	{ .name = "cluster-smgr-create-top" },
	{ .name = "cluster-smgr-open-top" },
	{ .name = "cluster-smgr-which-decision" },

	/* Stage 1.3 cluster shmem region registry (4 entries) */
	{ .name = "cluster-shmem-region-init-post" },
	{ .name = "cluster-shmem-region-init-pre" },
	{ .name = "cluster-shmem-register-region" },
	{ .name = "cluster-shmem-views-srf-entry" },

	/* Stage 1.7 cluster PCM lock framework (4 entries; Q6 user 修订
	 * 2026-05-02 release-pre instead of release-post for naming honesty
	 * since 1.7 stub never reaches a 'post' point). */
	{ .name = "cluster-pcm-acquire-entry" },
	{ .name = "cluster-pcm-convert-pre" },
	{ .name = "cluster-pcm-downgrade-pre" },
	{ .name = "cluster-pcm-release-pre" },

	/*
	 * Stage 1.10 cluster startup phase machinery (17 entries; spec-1.10
	 * §2.5).  Per phase 0/1/2/3/4 three points: -enter / -exit / -fail
	 * (15 phase-specific) plus driver-top entries cluster-run-startup-
	 * top + cluster-run-shutdown-top (2 driver-level).  Symmetric layout
	 * lets 1.11-1.14 real handlers + Stage 4 recovery + Stage 6 reconfig
	 * arm faults at any granularity without re-amending the injection
	 * registry.  HC4: -enter + sleep fault simulates a stuck phase to
	 * verify cluster.phase{N}_timeout enforcement.
	 */
	{ .name = "cluster-run-shutdown-top" },
	{ .name = "cluster-run-startup-top" },
	{ .name = "cluster-startup-phase-0-enter" },
	{ .name = "cluster-startup-phase-0-exit" },
	{ .name = "cluster-startup-phase-0-fail" },
	{ .name = "cluster-startup-phase-1-enter" },
	{ .name = "cluster-startup-phase-1-exit" },
	{ .name = "cluster-startup-phase-1-fail" },
	{ .name = "cluster-startup-phase-2-enter" },
	{ .name = "cluster-startup-phase-2-exit" },
	{ .name = "cluster-startup-phase-2-fail" },
	{ .name = "cluster-startup-phase-3-enter" },
	{ .name = "cluster-startup-phase-3-exit" },
	{ .name = "cluster-startup-phase-3-fail" },
	{ .name = "cluster-startup-phase-4-enter" },
	{ .name = "cluster-startup-phase-4-exit" },
	{ .name = "cluster-startup-phase-4-fail" },
	/* Stage 1.11 Sprint B (spec-1.11 D10) — 6 LMON lifecycle injects. */
	{ .name = "cluster-lmon-pre-spawn" },
	{ .name = "cluster-lmon-post-spawn" },
	{ .name = "cluster-lmon-ready-publish" },
	{ .name = "cluster-lmon-main-loop-iter" },
	{ .name = "cluster-lmon-shutdown-pre" },
	{ .name = "cluster-lmon-shutdown-post" },
};

#define CLUSTER_INJECTION_COUNT lengthof(cluster_injection_points)


/*
 * Fast-path gate.  Read once per CLUSTER_INJECTION_POINT() expansion.
 * Maintained as a plain int (with atomic load semantics on the gate
 * write side) because all writes happen under the SRF arm/disarm path
 * which is serialised by GUC / superuser context.  Reads are racy by
 * design -- a stale "armed" read worst-case takes the slow path which
 * re-checks per-entry state atomically.
 */
int cluster_injection_armed_count = 0;


/*
 * Static initialiser hook.  pg_atomic_init_u32/u64 needs to be called
 * before first read/write; we lazy-init on the first arm/run.  Since
 * all writes funnel through cluster_injection_arm_internal (under
 * superuser context) and reads through cluster_injection_run (which
 * calls into init if needed), there is no early-fork race.
 */
static bool cluster_injection_initialised = false;

static void
cluster_injection_initialise(void)
{
	if (cluster_injection_initialised)
		return;

	for (int i = 0; i < CLUSTER_INJECTION_COUNT; i++) {
		pg_atomic_init_u64(&cluster_injection_points[i].hits, 0);
		pg_atomic_init_u32(&cluster_injection_points[i].armed_type, CLUSTER_FAULT_NONE);
		pg_atomic_init_u64(&cluster_injection_points[i].armed_param, 0);
		pg_atomic_init_u32(&cluster_injection_points[i].skip_pending, 0);
	}
	cluster_injection_initialised = true;
}


/* ============================================================
 * Lookup, arm/disarm, run, dispatchers.
 * ============================================================ */

static ClusterInjectPoint *
cluster_injection_lookup(const char *name)
{
	if (name == NULL)
		return NULL;

	for (int i = 0; i < CLUSTER_INJECTION_COUNT; i++) {
		if (strcmp(cluster_injection_points[i].name, name) == 0)
			return &cluster_injection_points[i];
	}
	return NULL;
}


static ClusterInjectFaultType
parse_fault_type(const char *s)
{
	if (s == NULL)
		return CLUSTER_FAULT_NONE;

	if (pg_strcasecmp(s, "none") == 0)
		return CLUSTER_FAULT_NONE;
	if (pg_strcasecmp(s, "error") == 0)
		return CLUSTER_FAULT_ERROR;
	if (pg_strcasecmp(s, "warning") == 0)
		return CLUSTER_FAULT_WARNING;
	if (pg_strcasecmp(s, "sleep") == 0)
		return CLUSTER_FAULT_SLEEP;
	if (pg_strcasecmp(s, "crash") == 0)
		return CLUSTER_FAULT_CRASH;
	if (pg_strcasecmp(s, "skip") == 0)
		return CLUSTER_FAULT_SKIP;
	return CLUSTER_FAULT_NONE; /* unknown -> NONE; caller may warn */
}


static const char *
fault_type_name(ClusterInjectFaultType t)
{
	switch (t) {
	case CLUSTER_FAULT_NONE:
		return "none";
	case CLUSTER_FAULT_ERROR:
		return "error";
	case CLUSTER_FAULT_WARNING:
		return "warning";
	case CLUSTER_FAULT_SLEEP:
		return "sleep";
	case CLUSTER_FAULT_CRASH:
		return "crash";
	case CLUSTER_FAULT_SKIP:
		return "skip";
	}
	return "unknown";
}


/*
 * cluster_injection_arm_internal -- atomic arm / disarm core.
 *
 *	Caller must have already validated the name (via lookup) and the
 *	fault type.  Updates armed_type + armed_param atomically and
 *	maintains cluster_injection_armed_count so the fast-path gate
 *	stays accurate.
 */
static void
cluster_injection_arm_internal(ClusterInjectPoint *p, ClusterInjectFaultType new_type, int64 param)
{
	uint32 old_type;

	cluster_injection_initialise();

	old_type = pg_atomic_exchange_u32(&p->armed_type, new_type);
	pg_atomic_write_u64(&p->armed_param, (uint64)param);

	if (old_type == CLUSTER_FAULT_NONE && new_type != CLUSTER_FAULT_NONE)
		cluster_injection_armed_count++;
	else if (old_type != CLUSTER_FAULT_NONE && new_type == CLUSTER_FAULT_NONE)
		cluster_injection_armed_count--;
	/* same-direction transition (e.g. error -> warning): no count change */

	if (cluster_injection_armed_count < 0)
		cluster_injection_armed_count = 0; /* defensive */
}


/*
 * cluster_injection_run -- the slow-path entry called from
 * CLUSTER_INJECTION_POINT(name) when the fast-path gate is non-zero.
 */
void
cluster_injection_run(const char *name)
{
	ClusterInjectPoint *p;
	uint32 armed_type;
	uint64 param;

	cluster_injection_initialise();

	p = cluster_injection_lookup(name);
	if (p == NULL)
		return; /* unknown name; treat as no-op */

	(void)pg_atomic_fetch_add_u64(&p->hits, 1);

	armed_type = pg_atomic_read_u32(&p->armed_type);
	param = pg_atomic_read_u64(&p->armed_param);

	switch ((ClusterInjectFaultType)armed_type) {
	case CLUSTER_FAULT_NONE:
		/* counter-only mode (e.g. GUC startup arm without behaviour) */
		break;

	case CLUSTER_FAULT_ERROR:
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster injection point \"%s\" armed with ERROR", name)));
		break;

	case CLUSTER_FAULT_WARNING:
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
						  errmsg("cluster injection point \"%s\" armed with WARNING", name)));
		break;

	case CLUSTER_FAULT_SLEEP:
		pg_usleep((long)param);
		break;

	case CLUSTER_FAULT_CRASH:
#ifdef USE_ASSERT_CHECKING
		abort();
#else
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster injection point \"%s\" armed with CRASH "
							   "(degraded to ERROR in non-cassert build)",
							   name)));
#endif
		break;

	case CLUSTER_FAULT_SKIP:
		pg_atomic_write_u32(&p->skip_pending, 1);
		break;
	}
}


/*
 * cluster_injection_should_skip -- caller-side probe for SKIP fault.
 *
 *	Returns true iff a SKIP fault was just dispatched at the named
 *	point.  Auto-resets the pending flag so a single arm produces a
 *	single skip (no loops).  Caller code typically reads this right
 *	after CLUSTER_INJECTION_POINT(name) and short-circuits its
 *	protected logic when true.
 */
bool
cluster_injection_should_skip(const char *name)
{
	ClusterInjectPoint *p;

	cluster_injection_initialise();

	p = cluster_injection_lookup(name);
	if (p == NULL)
		return false;

	return pg_atomic_exchange_u32(&p->skip_pending, 0) != 0;
}


/*
 * cluster_injection_get_count -- registry size accessor (stage 0.29).
 *
 *	Returns the compile-time number of injection points.  Used by
 *	cluster_debug.c for enumerating the registry without exposing the
 *	static array.
 */
int
cluster_injection_get_count(void)
{
	return CLUSTER_INJECTION_COUNT;
}

/*
 * cluster_injection_get_state_at -- read one registry entry (stage 0.29).
 *
 *	Iterator companion to cluster_injection_get_count.  Returns false
 *	if `idx` is out of range; otherwise fills *name_out / *type_out /
 *	*hits_out from the entry at index `idx`.  Pointer outputs are
 *	read atomically from the live atomic_uint32 / atomic_uint64.
 *
 *	`name_out` points into the compile-time registry's string literal;
 *	caller must not free or modify.
 */
bool
cluster_injection_get_state_at(int idx, const char **name_out, ClusterInjectFaultType *type_out,
							   uint64 *hits_out)
{
	ClusterInjectPoint *p;

	if (idx < 0 || idx >= CLUSTER_INJECTION_COUNT)
		return false;

	cluster_injection_initialise();

	p = &cluster_injection_points[idx];

	if (name_out != NULL)
		*name_out = p->name;
	if (type_out != NULL)
		*type_out = (ClusterInjectFaultType)pg_atomic_read_u32(&p->armed_type);
	if (hits_out != NULL)
		*hits_out = pg_atomic_read_u64(&p->hits);

	return true;
}


/*
 * cluster_injection_init_from_guc -- apply the cluster.injection_points
 * GUC value at backend startup.
 *
 *	Wired as the assign_hook for the GUC; runs whenever the value is
 *	loaded (postmaster start, SIGHUP, ALTER SYSTEM).  Re-arms named
 *	points to WARNING (counter + warn).  Names not in the new list
 *	revert to NONE iff they were previously WARNING-armed via this path
 *	(otherwise an explicit SQL arm wins).
 *
 *	For Stage 0.27 we keep the implementation simple: every point not
 *	named in the list is set back to NONE.  Mixed-mode (GUC arm +
 *	manual SRF arm) lives in a Stage 2+ richer GUC; for now,
 *	`cluster.injection_points` is "the WARNING set" and SRF is "the
 *	other behaviours".  See docs/error-injection-design.md §6.3.
 */
/*
 * cluster_injection_assign_hook -- assign_hook for cluster.injection_points.
 *
 *	Public so cluster_guc.c can pass it to DefineCustomStringVariable
 *	without having to know the registry internals.
 */
void
cluster_injection_assign_hook(const char *newval, void *extra)
{
	const char *cursor;
	bool seen[CLUSTER_INJECTION_COUNT];

	(void)extra;
	cluster_injection_initialise();

	memset(seen, 0, sizeof(seen));

	cursor = newval ? newval : "";
	while (*cursor != '\0') {
		const char *start;
		size_t len;
		char buf[128];
		ClusterInjectPoint *p;
		ClusterInjectFaultType arm_type = CLUSTER_FAULT_WARNING;
		int64 arm_param = 0;
		char *colon;
		ClusterInjectFaultType last_type;

		while (*cursor == ' ' || *cursor == ',' || *cursor == '\t')
			cursor++;
		if (*cursor == '\0')
			break;

		start = cursor;
		while (*cursor != '\0' && *cursor != ',' && *cursor != ' ' && *cursor != '\t')
			cursor++;
		len = cursor - start;
		if (len == 0)
			continue;
		if (len >= sizeof(buf))
			len = sizeof(buf) - 1;
		memcpy(buf, start, len);
		buf[len] = '\0';

		/*
		 * Spec-1.10.1 D7 extension: optional ':type[:param]' suffix lets
		 * tests arm a specific fault type from postmaster GUC time
		 * (before any SQL connection exists).  Backward compatible:
		 * bare 'name' still arms to WARNING/0.
		 *
		 *	cluster.injection_points = 'cluster-startup-phase-1-enter:sleep:2'
		 */
		colon = strchr(buf, ':');
		if (colon != NULL) {
			char *type_str = colon + 1;
			char *param_colon = strchr(type_str, ':');

			*colon = '\0';
			if (param_colon != NULL) {
				*param_colon = '\0';
				arm_param = strtoll(param_colon + 1, NULL, 10);
			}
			arm_type = parse_fault_type(type_str);
			if (arm_type == CLUSTER_FAULT_NONE) {
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unknown cluster injection fault type: \"%s\"", type_str)));
				continue;
			}
		}

		p = cluster_injection_lookup(buf);
		if (p == NULL) {
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown cluster injection point in cluster.injection_points: \"%s\"",
							buf)));
			continue;
		}
		cluster_injection_arm_internal(p, arm_type, arm_param);
		seen[p - cluster_injection_points] = true;
		(void)last_type; /* silence unused under -Wunused-variable on some builds */
	}

	/*
	 * Disarm points that were not in the new list, but only the
	 * WARNING-armed kind.  SQL-armed faults (cluster_inject_fault
	 * with ERROR / SLEEP / CRASH / SKIP) are independent and survive
	 * GUC reloads.  Spec-1.10.1 D7 colon-syntax extensions go through
	 * the same path: a postmaster-time SLEEP arm survives subsequent
	 * cluster.injection_points reloads -- callers must explicitly
	 * disarm via cluster_inject_fault('name', 'none', 0) if needed.
	 */
	for (int i = 0; i < CLUSTER_INJECTION_COUNT; i++) {
		if (seen[i])
			continue;
		if (pg_atomic_read_u32(&cluster_injection_points[i].armed_type) == CLUSTER_FAULT_WARNING) {
			cluster_injection_arm_internal(&cluster_injection_points[i], CLUSTER_FAULT_NONE, 0);
		}
	}
}


/*
 * cluster_injection_init_from_guc -- public entry called by
 * cluster_init_guc to wire the assign_hook.  Cannot register the
 * assign_hook within DefineCustomStringVariable directly because
 * cluster_inject.h must not depend on guc.h.
 */
void
cluster_injection_init_from_guc(void)
{
	cluster_injection_initialise();
}


#endif /* USE_PGRAC_CLUSTER */


/* ============================================================
 * SRF entry points (always linked; bodies guarded by USE_PGRAC_CLUSTER).
 * ============================================================ */

Datum
cluster_inject_fault(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	text *name_text;
	text *type_text;
	int64 param;
	char *name;
	char *type_str;
	ClusterInjectPoint *p;
	ClusterInjectFaultType new_type;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to arm cluster injection points")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("name and fault_type must not be NULL")));

	name_text = PG_GETARG_TEXT_PP(0);
	type_text = PG_GETARG_TEXT_PP(1);
	param = PG_ARGISNULL(2) ? 0 : PG_GETARG_INT64(2);

	name = text_to_cstring(name_text);
	type_str = text_to_cstring(type_text);

	p = cluster_injection_lookup(name);
	if (p == NULL) {
		ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						  errmsg("unknown cluster injection point: \"%s\"", name)));
		PG_RETURN_BOOL(false);
	}

	new_type = parse_fault_type(type_str);
	cluster_injection_arm_internal(p, new_type, param);

	PG_RETURN_BOOL(true);
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_inject_fault requires --enable-cluster")));
	PG_RETURN_BOOL(false);
#endif
}


Datum
cluster_get_injection_state(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

		cluster_injection_initialise();

		for (int i = 0; i < CLUSTER_INJECTION_COUNT; i++) {
			ClusterInjectPoint *p = &cluster_injection_points[i];
			Datum values[4];
			bool nulls[4] = { false, false, false, false };
			uint32 t;

			t = pg_atomic_read_u32(&p->armed_type);

			values[0] = CStringGetTextDatum(p->name);
			values[1] = CStringGetTextDatum(fault_type_name((ClusterInjectFaultType)t));
			values[2] = Int64GetDatum((int64)pg_atomic_read_u64(&p->armed_param));
			values[3] = Int64GetDatum((int64)pg_atomic_read_u64(&p->hits));

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_stat_cluster_injections requires --enable-cluster")));
#endif

	return (Datum)0;
}
