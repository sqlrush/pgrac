/*-------------------------------------------------------------------------
 *
 * cluster_catalog_bootstrap.c
 *	  Shared-catalog runtime bootstrap (spec-6.14 D2).
 *
 *	  cluster_catalog_startup_prepare() runs postmaster-once at startup.  When
 *	  cluster.shared_catalog is on it seeds the shared OID authority from the
 *	  shared pg_control's next-OID high-water (a value both seed and join nodes
 *	  read identically), so the OID lease (D6) has a durable authority to draw
 *	  from.  "Seed if absent" makes this idempotent: the seed node (authority
 *	  absent) creates it, a join node (authority present) skips it.
 *
 *	  cluster_catalog_migrate_tree() (called below) then establishes the
 *	  shared catalog relation tree and the relmap authority: the seed node
 *	  copies its catalog files in and publishes the authority marker last;
 *	  a join node adopts the existing tree behind sysid + catalog-version
 *	  vets.  pgrac-init --cluster-seed/--cluster-join wire the GUC set and
 *	  provision joiner data directories around this boot-time contract.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_catalog_bootstrap.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <sys/stat.h>

#include "access/transam.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_catalog_bootstrap.h"
#include "cluster/cluster_catalog_migrate.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_oid_lease.h"
#include "cluster/cluster_recovery_anchor.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_xid_authority.h"
#include "miscadmin.h"
#include "storage/fd.h"

static bool
cluster_catalog_backup_label_present(void)
{
	char label_path[MAXPGPATH];
	struct stat st;

	snprintf(label_path, sizeof(label_path), "%s/%s", DataDir, BACKUP_LABEL_FILE);
	return stat(label_path, &st) == 0;
}

static void
cluster_catalog_xid_authority_corrupt_fatal(const char *detail)
{
	ereport(FATAL, (errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
					errmsg("shared XID authority is unavailable at catalog bootstrap"),
					errdetail("%s", detail),
					errhint("Restore \"%s/%s\" (or its .bak) from a backup of the shared tree; "
							"do not delete or re-seed it from a stale xid high-water.",
							cluster_shared_data_dir, CLUSTER_XID_AUTHORITY_REL_PATH)));
}


/*
 * Early D6 topology sniff: cluster_conf_load() runs later when shmem exists,
 * but shared_catalog bootstrap must reject multi-node xid_striping=off before
 * it seeds/adopts any authority.  This deliberately counts only [node.N]
 * section headers; the real parser remains the startup SSOT and will still
 * validate syntax, required fields, and node_id consistency later.
 */
static int
cluster_catalog_declared_node_count_early(void)
{
	const char *path = (cluster_config_file != NULL && cluster_config_file[0] != '\0')
						   ? cluster_config_file
						   : "pgrac.conf";
	FILE *f;
	char line[1024];
	int nodes = 0;

	f = AllocateFile(path, "r");
	if (f == NULL)
		return 1;

	while (fgets(line, sizeof(line), f) != NULL) {
		const char *p = line;

		while (*p != '\0' && isspace((unsigned char)*p))
			p++;
		if (strncmp(p, "[node.", 6) == 0)
			nodes++;
	}
	FreeFile(f);

	return nodes > 0 ? nodes : 1;
}

static void
cluster_catalog_vet_xid_striping_for_shared_catalog(void)
{
	int nodes;

	if (!cluster_enabled || cluster_xid_striping)
		return;

	nodes = cluster_catalog_declared_node_count_early();
	if (nodes > 1)
		ereport(
			FATAL,
			(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
			 errmsg("cluster.shared_catalog on a multi-node cluster requires cluster.xid_striping"),
			 errdetail("The configured cluster declares %d nodes.", nodes),
			 errhint("Set cluster.xid_striping=on (and cluster.online_join=on) on every "
					 "shared_catalog node.")));
}

static void
cluster_catalog_prepare_xid_authority(const ControlFileData *cf,
									  ClusterCatalogMigrateResult migrate_result)
{
	ClusterXidAuthorityHeader auth;
	bool have_auth;

	have_auth = cluster_xid_authority_read(&auth);
	if (!have_auth && cluster_xid_authority_present())
		cluster_catalog_xid_authority_corrupt_fatal(
			"Neither the primary shared XID authority nor its .bak fallback passes validation.");

	if (!have_auth) {
		if (migrate_result != CLUSTER_CATALOG_MIGRATE_SEEDED)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
					 errmsg("shared XID authority is absent for an existing shared catalog tree"),
					 errdetail("The shared catalog marker exists, but \"%s/%s\" does not.",
							   cluster_shared_data_dir, CLUSTER_XID_AUTHORITY_REL_PATH),
					 errhint("Re-seed the shared tree with a build carrying spec-6.15b.")));

		if (cluster_xid_authority_seed_if_absent(
				U64FromFullTransactionId(cf->checkPointCopy.nextXid)))
			elog(LOG, "cluster shared_catalog: seeded XID authority native high-water at %llu",
				 (unsigned long long)U64FromFullTransactionId(cf->checkPointCopy.nextXid));

		if (!cluster_xid_authority_read(&auth))
			cluster_catalog_xid_authority_corrupt_fatal(
				"The shared XID authority was just seeded but cannot be read back.");
	}

	if (!cluster_enabled) {
		if (auth.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA)
			ereport(
				FATAL,
				(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
				 errmsg("native seed era cannot be re-entered on a formed shared catalog tree"),
				 errdetail("The shared XID authority is already marked cluster-era started."),
				 errhint(
					 "Keep cluster.enabled=on for this shared tree, or destroy and re-form it.")));
		return;
	}

	if ((auth.flags & CLUSTER_XID_AUTHORITY_FLAG_SEALED) == 0)
		ereport(
			FATAL,
			(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
			 errmsg("shared XID authority is not sealed"),
			 errdetail("The seed node has not completed a clean native-era shutdown."),
			 errhint(
				 "Start the seed with cluster.enabled=off, stop it cleanly, then start joiners.")));

	if (cluster_catalog_backup_label_present())
		elog(LOG, "cluster shared_catalog: skipped XID prehistory adopt on backup_label boot");
	else {
		ClusterRecoveryAnchor ra;
		uint64 own_next;

		if (!cluster_recovery_anchor_read(cf->system_identifier, &ra, NULL))
			ereport(
				FATAL,
				(errcode(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE),
				 errmsg("per-node recovery anchor is unavailable for XID prehistory adopt"),
				 errdetail("The joiner cannot determine its own pre-adopt nextXid without \"%s\".",
						   cluster_recovery_anchor_path() != NULL ? cluster_recovery_anchor_path()
																  : "(unset)"),
				 errhint(
					 "Re-provision this node or verify cluster.shared_data_dir before startup.")));

		own_next = U64FromFullTransactionId(ra.checkPointCopy.nextXid);
		if (own_next < auth.native_hw_full) {
			cluster_xid_prehistory_adopt(DataDir, auth.native_hw_full);
			elog(LOG,
				 "cluster shared_catalog: adopted XID prehistory through native high-water %llu "
				 "(own nextXid was %llu)",
				 (unsigned long long)auth.native_hw_full, (unsigned long long)own_next);
		} else
			elog(LOG,
				 "cluster shared_catalog: skipped XID prehistory adopt; own nextXid %llu >= native "
				 "high-water %llu",
				 (unsigned long long)own_next, (unsigned long long)auth.native_hw_full);
	}

	if ((auth.flags & CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA) == 0) {
		cluster_xid_authority_mark_cluster_era();
		elog(LOG,
			 "cluster shared_catalog: marked XID authority cluster era started at native "
			 "high-water %llu",
			 (unsigned long long)auth.native_hw_full);
	}
}

void
cluster_catalog_startup_prepare(void)
{
	ControlFileData cf;
	ClusterCatalogMigrateResult migrate_result;

	/* Postmaster-once: only the postmaster seeds; forked backends inherit. */
	if (IsUnderPostmaster)
		return;

	if (!cluster_shared_catalog) {
		/*
		 * Off-mode boots against a shared tree that already holds a catalog
		 * authority are refused (D5 off-flip vet): the local catalog files
		 * are stale once any DDL ran under the shared catalog.
		 */
		cluster_catalog_vet_off_mode();
		return; /* off: stock per-node catalog */
	}

	cluster_catalog_vet_xid_striping_for_shared_catalog();

	/*
	 * shared_catalog=on requires the shared pg_control authority (D1 vet), so
	 * it is the single source of the cluster-wide next-OID high-water.  Reading
	 * it fail-closed keeps a seed value that every node agrees on.
	 */
	if (!cluster_cf_authority_read(&cf))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
						errmsg("shared pg_control authority is unavailable at catalog bootstrap"),
						errhint("cluster.shared_catalog requires a readable shared "
								"pg_control under cluster.shared_data_dir.")));

	/*
	 * Seed the OID authority from the shared high-water if absent (seed node);
	 * a join node whose authority already exists is a no-op.  The high-water is
	 * never lowered, so a fresh cluster starts allocating cluster-wide OIDs
	 * from the same floor every node observes.
	 *
	 * A present-but-unreadable authority is FATAL, not a re-seed (§3.6
	 * fail-closed): the checkpointed shared pg_control high-water can lag OID
	 * ranges the cluster already leased out, so silently re-seeding from it
	 * could hand the same OID range to two nodes.
	 */
	{
		Oid oid_hw;

		if (!cluster_oid_authority_read(&oid_hw) && cluster_oid_authority_present())
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
							errmsg("shared OID authority is present but corrupt"),
							errdetail("Neither \"%s/global/pgrac_oid_authority\" nor its "
									  ".bak fallback passes validation.",
									  cluster_shared_data_dir),
							errhint("Restore the shared OID authority files from a backup "
									"of the shared tree; do not delete them (re-seeding "
									"from a stale high-water can reissue leased OIDs).")));
	}
	if (cluster_oid_authority_seed_if_absent(cf.checkPointCopy.nextOid))
		elog(LOG, "cluster shared_catalog: seeded OID authority high-water at %u",
			 cf.checkPointCopy.nextOid);

	/*
	 * Establish the shared catalog relation tree: the seed node copies its
	 * catalog relation files into the shared tree; a join node adopts the
	 * existing tree after an identity gate.  Runs here, postmaster-once and
	 * before the startup process, so it precedes any catalog access through
	 * the (D3-flipped) shared smgr route.  system_identifier is the shared
	 * pg_control's cluster-wide value both seed and join agree on.
	 */
	migrate_result = cluster_catalog_migrate_tree(DataDir, cf.system_identifier);
	cluster_catalog_prepare_xid_authority(&cf, migrate_result);
}

/*
 * cluster_catalog_services_ready -- spec-6.14 D8 visibility phase gate.
 *
 * True once catalog scans in THIS process may run under cluster snapshot
 * semantics.  CLUSTER_PHASE_RUNNING is the aggregate judge: the phase driver
 * reaches it only after phases 1-4 all published READY (LMON/interconnect,
 * LCK/LMS lock services, cluster recovery, normal-operation children), which
 * is exactly the service set a catalog-tuple remote-xid resolution depends
 * on.  Before that -- and in bootstrap / standalone / off-mode contexts,
 * where the phase state stays PRE_INIT -- catalog snapshots keep the
 * spec-3.3 D3 LOCAL posture.
 *
 * Recovery contexts stay LOCAL as well: on an ADG physical standby the local
 * pg_xact is replayed from the primary's own WAL stream and is authoritative
 * for every replayed xid (t/242 L9 RL1), and crash-recovery replay never
 * runs MVCC catalog scans.
 *
 * The RUNNING observation is latched per backend: phases never regress
 * except into SHUTDOWN, where the desired posture is the resolver's own
 * fail-closed 53R97 (services torn down, not wrong answers), not a silent
 * flip back to LOCAL judgement of foreign tuples.
 */
bool
cluster_catalog_services_ready(void)
{
	static bool ready_latched = false;

	if (!cluster_shared_catalog || !cluster_storage_mode_enabled())
		return false;

	if (RecoveryInProgress())
		return false;

	/*
	 * Test-only lever (t/337 L4 / R11): SKIP-arming this point forces the
	 * gate closed even after the RUNNING latch, driving the pre-ready
	 * posture -- LOCAL catalog snapshots whose foreign-evidence reads
	 * fail-close 53R97 -- from a running cluster, which no natural SQL
	 * timing can reach.  Checked before the latch so disarming restores
	 * normal service.
	 */
	CLUSTER_INJECTION_POINT("cluster-catalog-services-ready-force-closed");
	if (cluster_injection_should_skip("cluster-catalog-services-ready-force-closed"))
		return false;

	if (ready_latched)
		return true;

	if (cluster_current_phase() == CLUSTER_PHASE_RUNNING) {
		ready_latched = true;
		return true;
	}

	return false;
}
