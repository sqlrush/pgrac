/*-------------------------------------------------------------------------
 *
 * cluster_relmap_arb.c
 *	  Crash arbitration of staged relmap-authority pending images
 *	  (spec-6.14 D5-activation, §3.2).  See cluster_relmap_arb.h for the
 *	  model.
 *
 *	  Runs in the startup process at the merged-recovery completion point,
 *	  under the sole-merger claim (INV-D9-R): no live writer can race the
 *	  arbitration (writers take the singleton relmap X lock and are blocked
 *	  behind recovery anyway), and no concurrent merger exists.
 *
 *	  A publish here is the deferred half of a committed owner's
 *	  post-commit publish; the relmap invalidation broadcast follows it
 *	  (INV-14-8) so any live peer that reloaded the OLD committed image
 *	  after the owner's death (reconfig RESET-all) drops it.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_relmap_arb.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D5, §3.2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "access/transam.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_relmap_arb.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_sinval.h"
#include "storage/fd.h"
#include "storage/sinval.h"

/* Bounded broadcast retries before fail-stop (mirrors the commit path). */
#define CLUSTER_RELMAP_ARB_BCAST_RETRIES 10

/*
 * broadcast_relmap_inval -- post-publish invalidation (INV-14-8): local
 * ring + cross-node fanout with the all-alive-peer ack barrier.  Same
 * retry-then-PANIC posture as AtEOXact_ClusterRelmapPublish: once the
 * publish is durable, a peer serving the stale map would read the wrong
 * relation file, so failure to invalidate is fail-stop.
 */
static void
broadcast_relmap_inval(Oid dbid)
{
	SharedInvalidationMessage msg;

	msg.rm.id = SHAREDINVALRELMAP_ID;
	msg.rm.dbId = dbid;
	SendSharedInvalidMessages(&msg, 1);

	if (cluster_peer_mode_enabled()) {
		int attempt;
		ClusterSinvalAckResult r = CLUSTER_SINVAL_ACK_TIMEOUT;

		for (attempt = 0; attempt < CLUSTER_RELMAP_ARB_BCAST_RETRIES; attempt++) {
			r = cluster_sinval_enqueue_and_wait_ack(&msg, 1);
			if (r == CLUSTER_SINVAL_ACK_DONE)
				break;
			ereport(WARNING, (errmsg("cluster relmap arbitration invalidation not "
									 "acknowledged (attempt %d of %d), retrying",
									 attempt + 1, CLUSTER_RELMAP_ARB_BCAST_RETRIES)));
		}
		if (r != CLUSTER_SINVAL_ACK_DONE)
			ereport(PANIC, (errmsg("cluster relmap arbitration invalidation was not "
								   "acknowledged by all live peers"),
							errdetail("The relation map authority was already published "
									  "during crash arbitration."),
							errhint("Fail-stop: the next merger repeats the (idempotent) "
									"arbitration.")));
	}
}

/*
 * arbitrate_one -- resolve one authority's pending image, if any.
 */
static void
arbitrate_one(bool shared_map, Oid dbid)
{
	ClusterRelmapAuthorityHeader hdr;
	bool committed = false;
	bool known = false;

	if (!cluster_relmap_authority_read_header(shared_map, dbid, &hdr))
		return; /* absent/unreadable: nothing to arbitrate;
								 * reads and writes fail closed elsewhere */

	if (hdr.pending_generation == 0)
		return; /* clean */

	if ((int)hdr.owner_node == cluster_node_id) {
		/*
		 * Own pending: the local pg_xact is authoritative for own xids
		 * (merged replay diverts foreign XACT/CLOG records away from it,
		 * so no raw-xid aliasing pollutes it).  An owner that never
		 * reached its commit record is crashed-in-flight = aborted.
		 */
		if (TransactionIdIsNormal(hdr.owner_xid)
			&& TransactionIdPrecedes(hdr.owner_xid, ReadNextTransactionId())) {
			committed = TransactionIdDidCommit(hdr.owner_xid);
			known = true;
		}
	} else {
		/*
		 * Foreign pending: the per-origin remote-xact store, durable-
		 * checked (a commit that cannot be durably proven stays INDOUBT,
		 * D9 posture).  The store only holds terminal records seen inside
		 * this merge's redo window; an older terminal record (checkpoint
		 * slid between the owner's commit and its crash) stays INDOUBT
		 * here and resolves when the owner node itself next merges.
		 */
		SCN scn;

		switch (cluster_remote_outcome_durable_checked((int)hdr.owner_node, hdr.owner_xid, &scn)) {
		case CLUSTER_REMOTE_XACT_COMMITTED:
			committed = true;
			known = true;
			break;
		case CLUSTER_REMOTE_XACT_ABORTED:
			committed = false;
			known = true;
			break;
		case CLUSTER_REMOTE_XACT_INDOUBT:
			break;
		}
	}

	if (!known) {
		ereport(LOG, (errmsg("cluster relmap pending image for %s (generation %llu, "
							 "owner node %d, xid %u) is unresolved; relmap writes stay "
							 "fail-closed",
							 shared_map ? "the shared map" : psprintf("database %u", dbid),
							 (unsigned long long)hdr.pending_generation, (int)hdr.owner_node,
							 hdr.owner_xid)));
		return;
	}

	if (committed) {
		cluster_relmap_authority_publish(shared_map, dbid, hdr.pending_generation);
		ereport(LOG, (errmsg("cluster relmap arbitration published pending generation "
							 "%llu for %s (owner node %d, xid %u committed)",
							 (unsigned long long)hdr.pending_generation,
							 shared_map ? "the shared map" : psprintf("database %u", dbid),
							 (int)hdr.owner_node, hdr.owner_xid)));
		broadcast_relmap_inval(dbid);
	} else {
		cluster_relmap_authority_discard_pending(shared_map, dbid, hdr.pending_generation);
		ereport(LOG, (errmsg("cluster relmap arbitration discarded pending generation "
							 "%llu for %s (owner node %d, xid %u did not commit)",
							 (unsigned long long)hdr.pending_generation,
							 shared_map ? "the shared map" : psprintf("database %u", dbid),
							 (int)hdr.owner_node, hdr.owner_xid)));
	}
}

void
cluster_relmap_arbitrate_pendings(void)
{
	char basepath[MAXPGPATH];
	DIR *dir;
	struct dirent *de;

	if (!cluster_shared_catalog)
		return;
	if (!cluster_recovery_merge_claim_is_held())
		return; /* not the sole merger: nothing to do (a
								 * clean boot cannot leave a pending) */

	arbitrate_one(true, InvalidOid);

	/* Per-db authorities live under the SHARED tree's base/<dbid>/. */
	snprintf(basepath, sizeof(basepath), "%s/base", cluster_shared_data_dir);
	dir = AllocateDir(basepath);
	if (dir == NULL)
		return; /* no per-db tree yet */
	while ((de = ReadDir(dir, basepath)) != NULL) {
		if (!isdigit((unsigned char)de->d_name[0]))
			continue;
		arbitrate_one(false, atooid(de->d_name));
	}
	FreeDir(dir);
}
