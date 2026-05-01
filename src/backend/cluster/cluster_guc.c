/*-------------------------------------------------------------------------
 *
 * cluster_guc.c
 *	  pgrac cluster GUC registration and storage.
 *
 *	  Stage 0.13 establishes the cluster GUC framework and registers
 *	  the first cluster GUC, cluster_node_id.  See spec-0.13 and
 *	  docs/cluster-guc-design.md for the registration policy and the
 *	  full SSOT roster of planned GUCs.
 *
 *	  Why one GUC and not the full ~24 in the design doc:
 *
 *	  GUCs are user-visible knobs that promise behavior on change.
 *	  Registering a GUC that no subsystem reads turns SHOW into a
 *	  liar (DBA sets it, observes nothing) and violates CLAUDE.md
 *	  rule 8.  Each remaining GUC ships with the spec that introduces
 *	  its first reader.  This file gains a new DefineCustomXxxVariable
 *	  block per GUC over the next stages.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_guc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All exported symbols use the cluster_ prefix and live under the
 *	  "cluster_*" GUC namespace per CLAUDE.md rule 16.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/guc.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"				   /* ClusterICTier enum values */
#include "cluster/cluster_inject.h"			   /* cluster_injection_assign_hook (stage 0.27) */
#include "cluster/storage/cluster_shared_fs.h" /* ClusterSharedFsBackendId (stage 1.1) */


/*
 * Storage for cluster GUC variables.  PG's GUC machinery writes here
 * on startup and (for non-PGC_POSTMASTER variables) on SIGHUP / SET.
 *
 * Boot values must match the boot value passed to DefineCustomXxxVariable
 * below, so that reads before cluster_init_guc() runs (e.g. from very
 * early postmaster code) see a sane default.
 */
int cluster_node_id = -1;
int cluster_interconnect_tier = CLUSTER_IC_TIER_STUB;
char *cluster_config_file = NULL;	   /* boot value filled by DefineCustomStringVariable */
char *cluster_injection_points = NULL; /* boot value filled by DefineCustomStringVariable */
int cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_STUB;
bool cluster_smgr_user_relations = false;


/*
 * Mapping from the cluster.interconnect_tier GUC enum string to the
 * ClusterICTier C enum.  PG's GUC machinery copies the int into
 * cluster_interconnect_tier; cluster_ic_init then dispatches.  The
 * "hidden" flag is false because we want SHOW / pg_settings to display
 * every legal value to the DBA (even tiers that are not yet implemented
 * are shown -- attempting to use one fails at startup with a precise
 * errhint pointing to the Stage where it lands).
 */
static const struct config_enum_entry cluster_interconnect_tier_options[]
	= { { "stub", CLUSTER_IC_TIER_STUB, false }, { "mock", CLUSTER_IC_TIER_MOCK, false },
		{ "tier1", CLUSTER_IC_TIER_1, false },	 { "tier2", CLUSTER_IC_TIER_2, false },
		{ "tier3", CLUSTER_IC_TIER_3, false },	 { NULL, 0, false } };


/*
 * Mapping for cluster.shared_storage_backend.  Mirrors
 * ClusterSharedFsBackendId enum positionally.  All six backends are
 * advertised; only stub and local are registered at stage 1.1, so
 * picking one of the other four causes cluster_shared_fs_init to
 * FATAL with an errhint pointing to Stage 2.  See
 * docs/cluster-shared-fs-design.md §4.
 */
static const struct config_enum_entry cluster_shared_storage_backend_options[]
	= { { "stub", CLUSTER_SHARED_FS_BACKEND_STUB, false },
		{ "local", CLUSTER_SHARED_FS_BACKEND_LOCAL, false },
		{ "block_device", CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE, false },
		{ "cluster_fs", CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS, false },
		{ "rbd", CLUSTER_SHARED_FS_BACKEND_RBD, false },
		{ "multi_attach", CLUSTER_SHARED_FS_BACKEND_MULTI_ATTACH, false },
		{ NULL, 0, false } };


/*
 * cluster_init_guc -- register all cluster GUC variables.
 *
 *	Called once from PostmasterMain after PG's built-in GUCs are
 *	loaded.  See cluster_guc.h for contract and docs/cluster-guc-design.md
 *	§2 for the placement rationale.
 *
 *	When adding a new GUC: (1) extend cluster_guc.h with the extern
 *	declaration, (2) add the storage definition above, (3) add a new
 *	DefineCustomXxxVariable block here, and (4) update
 *	docs/cluster-guc-design.md §3.1 with the registration stage.
 *	cluster_unit and cluster_tap should grow tests for the new GUC in
 *	the same commit.
 */
void
cluster_init_guc(void)
{
	CLUSTER_INJECTION_POINT("cluster-guc-init-pre-define");

	/*
	 * GUC name uses the dot-prefixed "cluster.node_id" form per PG's
	 * convention for custom (non-built-in) GUCs (valid_custom_variable_name
	 * in guc.c requires at least one '.').  The underlying C variable
	 * stays cluster_node_id (snake_case per CLAUDE.md rule 12).
	 */
	DefineCustomIntVariable("cluster.node_id",
							gettext_noop("Numeric identifier of this node in the cluster."),
							gettext_noop("Set to -1 (the default) when running outside a cluster.  "
										 "When configured, the value is logged by CLUSTER_LOG and "
										 "will be used for cross-node coordination starting in "
										 "Stage 1+ subsystem implementations."),
							&cluster_node_id, -1, /* boot value */
							-1,					  /* min */
							127,				  /* max */
							PGC_POSTMASTER,		  /* requires restart */
							0,					  /* flags */
							NULL,				  /* check_hook */
							NULL,				  /* assign_hook */
							NULL);				  /* show_hook */

	/*
	 * cluster.interconnect_tier -- selects the cluster_ic vtable.
	 * Stage 0.18 only ships the stub vtable; tier1 / tier2 / tier3
	 * are accepted by GUC parsing but rejected at cluster_ic_init
	 * with a precise errhint.  See cluster_ic.c::cluster_ic_init and
	 * docs/cluster-ic-design.md §3.
	 */
	DefineCustomEnumVariable(
		"cluster.interconnect_tier", gettext_noop("Cluster interconnect tier vtable selection."),
		gettext_noop("stub (default) keeps cross-node IPC disabled; tier1 (TCP) "
					 "lands in Stage 2; tier2 / tier3 (RDMA) land in Stage 6+. "
					 "See docs/cluster-ic-design.md."),
		&cluster_interconnect_tier, CLUSTER_IC_TIER_STUB,  /* boot value */
		cluster_interconnect_tier_options, PGC_POSTMASTER, /* tier change requires restart */
		0,												   /* flags */
		NULL,											   /* check_hook */
		NULL,											   /* assign_hook */
		NULL);											   /* show_hook */

	/*
	 * cluster.config_file -- path to pgrac.conf.  Default "pgrac.conf"
	 * is interpreted relative to postmaster cwd (which is PGDATA after
	 * ChangeToDataDir).  Stage 2+ multi-node setups typically point
	 * this at shared storage.  See spec-0.19-conf-framework.md §2.4
	 * and docs/cluster-conf-design.md §4.
	 */
	DefineCustomStringVariable(
		"cluster.config_file",
		gettext_noop("Path to the pgrac cluster topology configuration file."),
		gettext_noop("Default \"pgrac.conf\" is resolved relative to PGDATA. "
					 "Set to an absolute path to use shared storage for "
					 "multi-node deployments."),
		&cluster_config_file, "pgrac.conf", /* boot value */
		PGC_POSTMASTER,						/* topology reload requires restart */
		0,									/* flags */
		NULL,								/* check_hook */
		NULL,								/* assign_hook */
		NULL);								/* show_hook */

	/*
	 * cluster.injection_points -- comma-separated list of injection point
	 * names to auto-arm at startup with fault_type=WARNING (counter-only +
	 * warn).  Runtime arming via the cluster_inject_fault() SRF is
	 * independent and not gated by this GUC.  PGC_SUSET so DBAs can flip
	 * it on a running server via ALTER SYSTEM + SIGHUP without a restart.
	 * See spec-0.27-error-injection.md §2.5 and docs/error-injection-design.md.
	 */
	DefineCustomStringVariable(
		"cluster.injection_points",
		gettext_noop("Comma-separated list of cluster injection points to auto-arm at startup."),
		gettext_noop("Each named point is armed with fault_type=WARNING (counter + warn). "
					 "Names not in the registry yield a WARNING and are ignored. "
					 "Default empty (no auto-arm)."),
		&cluster_injection_points, "", /* boot value */
		PGC_SUSET,					   /* superuser, runtime SET allowed */
		GUC_LIST_INPUT,				   /* comma-separated list */
		NULL,						   /* check_hook */
		cluster_injection_assign_hook, /* assign_hook */
		NULL);						   /* show_hook */

	/*
	 * cluster.shared_storage_backend -- selects the cluster_shared_fs
	 * vtable activated by cluster_shared_fs_init.  Boot default "stub"
	 * keeps stage-0 behaviour unchanged for users who upgrade without
	 * explicitly opting into the new abstraction layer.  See
	 * docs/cluster-shared-fs-design.md §4 and
	 * spec-1.1-shared-fs-skeleton.md.
	 */
	DefineCustomEnumVariable("cluster.shared_storage_backend",
							 gettext_noop("Cluster shared-storage backend selection."),
							 gettext_noop("stub (default) keeps cluster_shared_fs disabled "
										  "(every call ereports FEATURE_NOT_SUPPORTED); local "
										  "is single-node passthrough to fd.c; block_device, "
										  "cluster_fs, rbd, and multi_attach land in Stage 2."),
							 &cluster_shared_storage_backend, CLUSTER_SHARED_FS_BACKEND_STUB,
							 cluster_shared_storage_backend_options,
							 PGC_POSTMASTER, /* backend selection requires restart */
							 0,				 /* flags */
							 NULL,			 /* check_hook */
							 NULL,			 /* assign_hook */
							 NULL);			 /* show_hook */

	/*
	 * cluster.smgr_user_relations -- opt-in switch routing user-
	 * relation block I/O through cluster_smgr (smgr_which=1) instead
	 * of md.c (smgr_which=0).  Default off keeps the existing PG
	 * smgr path completely unchanged; the GUC only takes effect for
	 * permanent (non-temp) relations and only when
	 * shared_storage_backend != stub.  Startup-time cross-check
	 * lives in cluster_shared_fs_init (cluster_shared_fs.c).  See
	 * spec-1.2-smgr-cluster.md §3.2 + docs/cluster-smgr-design.md §6.
	 */
	DefineCustomBoolVariable("cluster.smgr_user_relations",
							 gettext_noop("Route permanent relations through cluster_smgr instead of md.c."),
							 gettext_noop("When on (combined with shared_storage_backend != stub), "
										  "permanent non-temp relations route through cluster_smgr "
										  "-> cluster_shared_fs at smgropen time.  Stage 1.2 single-"
										  "node single-file passthrough; user data stored as one "
										  "file per (rlocator, fork) without the md.c .seg suffix."),
							 &cluster_smgr_user_relations, false,
							 PGC_POSTMASTER, /* smgr_which is cached per-relation; restart required */
							 0,				 /* flags */
							 NULL,			 /* check_hook */
							 NULL,			 /* assign_hook */
							 NULL);			 /* show_hook */
}
