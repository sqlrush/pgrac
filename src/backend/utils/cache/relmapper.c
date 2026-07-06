/*-------------------------------------------------------------------------
 *
 * relmapper.c
 *	  Catalog-to-filenumber mapping
 *
 * For most tables, the physical file underlying the table is specified by
 * pg_class.relfilenode.  However, that obviously won't work for pg_class
 * itself, nor for the other "nailed" catalogs for which we have to be able
 * to set up working Relation entries without access to pg_class.  It also
 * does not work for shared catalogs, since there is no practical way to
 * update other databases' pg_class entries when relocating a shared catalog.
 * Therefore, for these special catalogs (henceforth referred to as "mapped
 * catalogs") we rely on a separately maintained file that shows the mapping
 * from catalog OIDs to filenumbers.  Each database has a map file for
 * its local mapped catalogs, and there is a separate map file for shared
 * catalogs.  Mapped catalogs have zero in their pg_class.relfilenode entries.
 *
 * Relocation of a normal table is committed (ie, the new physical file becomes
 * authoritative) when the pg_class row update commits.  For mapped catalogs,
 * the act of updating the map file is effectively commit of the relocation.
 * We postpone the file update till just before commit of the transaction
 * doing the rewrite, but there is necessarily a window between.  Therefore
 * mapped catalogs can only be relocated by operations such as VACUUM FULL
 * and CLUSTER, which make no transactionally-significant changes: it must be
 * safe for the new file to replace the old, even if the transaction itself
 * aborts.  An important factor here is that the indexes and toast table of
 * a mapped catalog must also be mapped, so that the rewrites/relocations of
 * all these files commit in a single map file update rather than being tied
 * to transaction commit.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/relmapper.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * ============================================================
 * PGRAC MODIFICATIONS
 *
 * Modified by: SqlRush <sqlrush@gmail.com>
 *
 * What changed:
 *   - Added a single hook call inside write_relmap_file() (the
 *     SOURCE side, alongside CacheInvalidateRelmap()) to mirror
 *     relation-map updates across cluster instances.  The hook body
 *     itself is a stub at Stage 2.7 (only bumps the cross-instance
 *     broadcast STUB counter); spec-2.27 SI Broadcaster will turn
 *     it into a real wire send.
 *
 *   - perform_relmap_update() commits through the shared relmap
 *     AUTHORITY under cluster.shared_catalog=on (the pending/publish
 *     protocol: cluster_perform_relmap_update stages pre-commit,
 *     AtEOXact_ClusterRelmapPublish publishes + invalidates
 *     post-commit, AtAbort_ClusterRelmapPending discards on abort).
 *     The earlier interim fail-closed gate in RelationMapUpdateMap()
 *     is gone: mapped-catalog rewrites (VACUUM FULL / CLUSTER /
 *     REINDEX of mapped rels) are supported cluster-wide.
 *     Spec: spec-6.14-shared-catalog-single-authority.md (D5, §3.2).
 *
 *   - read_relmap_file() reads the shared relmap AUTHORITY instead of
 *     the local pg_filenode.map under cluster.shared_catalog=on
 *     (cluster_read_relmap_from_authority below): readers adopt the
 *     authority's COMMITTED image only (INV-14-7), fail-closed 53RB0
 *     when no trustworthy authority exists — never the stale local
 *     file.  Local map files are no longer read (AD-001: no local
 *     persistent map); relmap_redo still writes them (harmless, and
 *     keeps off-mode/PITR semantics byte-identical).
 *     Spec: spec-6.14-shared-catalog-single-authority.md (D5, §3.2).
 *
 *   - History note: Hardening v1.0.0 (2026-05-09 pre-ship F3) moved
 *     the hook from RelationMapInvalidate() (the sinval RECEIVE
 *     callback) to write_relmap_file() source side to avoid
 *     cross-instance re-broadcast amplification when peers receive
 *     a relmap inval and run the local reload callback.  Hardening
 *     v1.0.1 (2026-05-09 post-ship F4) refreshed this header text
 *     after v1.0.0 amend — earlier copy still said
 *     "inside RelationMapInvalidate()" by mistake.
 *
 * Why:
 *   - PG sinval is process-local on its own.  When two cluster
 *     instances each have a backend with shared / per-database map
 *     files cached, the local RelationMapInvalidate path only
 *     reloads the calling backend's copy.  spec-2.7 §2.4 wires the
 *     boundary now so that spec-2.27 can flip the stub to real
 *     without touching this PG-original file again (per AD-001
 *     Option C "wire boundaries early, activate late" pattern).
 *
 * IMPORTANT — critical section forward-contract (Hardening v1.0.1 F2 / L93):
 *   write_relmap_file() enters START_CRIT_SECTION() at line ~1012;
 *   the hook call sits inside that critical section (line ~1048,
 *   right after CacheInvalidateRelmap()).  The current stub body
 *   is nofail (atomic counter add only) — safe inside crit section.
 *   spec-2.27 ACTIVATION CONSTRAINT: hook body must remain
 *   nofail/nonblocking inside START_CRIT_SECTION;the actual SI
 *   Broadcaster send + peer ack MUST happen outside the critical
 *   section (post-commit callback / aux process drain shmem queue).
 *   ANY ereport(ERROR) / palloc-failure / wait inside crit section
 *   is upgraded to PANIC by PG core (CLAUDE.md rule 16).  spec-2.27
 *   drafter MUST design queue+drain pattern, not "just change hook
 *   body to call SI Broadcaster directly".
 *
 * Spec: spec-2.7-smgr-cluster-2node-concurrent-open.md (v0.2 frozen
 *       2026-05-09;Q2 v0.2 `bool shared` sig + Hardening v1.0.0 F3
 *       source-side relocation + Hardening v1.0.1 F4 stale text fix
 *       + L93 critical-section-hook-must-nofail-enqueue forward-contract).
 * ============================================================
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/inval.h"
#include "utils/relmapper.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"		  /* cluster_enabled + cluster_smgr_user_relations */
#include "cluster/cluster_inject.h"	  /* crash-window test levers (D5) */
#include "cluster/cluster_mode.h"		  /* cluster_peer_mode_enabled (D5) */
#include "cluster/cluster_reconfig.h"	  /* observed epoch for the owner header */
#include "cluster/cluster_relmap_authority.h" /* shared relmap authority (D5) */
#include "cluster/cluster_relmap_lock.h"  /* singleton relmap X lock (D5) */
#include "cluster/cluster_sinval.h"		  /* cross-node inval ack barrier (2.39) */
#include "cluster/storage/cluster_smgr.h" /* cluster_smgr_invalidate_relmap */
#include "storage/sinval.h"				  /* SendSharedInvalidMessages */
#endif


/*
 * The map file is critical data: we have no automatic method for recovering
 * from loss or corruption of it.  We use a CRC so that we can detect
 * corruption.  Since the file might be more than one standard-size disk
 * sector in size, we cannot rely on overwrite-in-place. Instead, we generate
 * a new file and rename it into place, atomically replacing the original file.
 *
 * Entries in the mappings[] array are in no particular order.  We could
 * speed searching by insisting on OID order, but it really shouldn't be
 * worth the trouble given the intended size of the mapping sets.
 */
#define RELMAPPER_FILENAME		"pg_filenode.map"
#define RELMAPPER_TEMP_FILENAME	"pg_filenode.map.tmp"

#define RELMAPPER_FILEMAGIC		0x592717	/* version ID value */

/*
 * There's no need for this constant to have any particular value, and we
 * can raise it as necessary if we end up with more mapped relations. For
 * now, we just pick a round number that is modestly larger than the expected
 * number of mappings.
 */
#define MAX_MAPPINGS			64

typedef struct RelMapping
{
	Oid			mapoid;			/* OID of a catalog */
	RelFileNumber mapfilenumber;	/* its rel file number */
} RelMapping;

typedef struct RelMapFile
{
	int32		magic;			/* always RELMAPPER_FILEMAGIC */
	int32		num_mappings;	/* number of valid RelMapping entries */
	RelMapping	mappings[MAX_MAPPINGS];
	pg_crc32c	crc;			/* CRC of all above */
} RelMapFile;

/*
 * State for serializing local and shared relmappings for parallel workers
 * (active states only).  See notes on active_* and pending_* updates state.
 */
typedef struct SerializedActiveRelMaps
{
	RelMapFile	active_shared_updates;
	RelMapFile	active_local_updates;
} SerializedActiveRelMaps;

/*
 * The currently known contents of the shared map file and our database's
 * local map file are stored here.  These can be reloaded from disk
 * immediately whenever we receive an update sinval message.
 */
static RelMapFile shared_map;
static RelMapFile local_map;

/*
 * We use the same RelMapFile data structure to track uncommitted local
 * changes in the mappings (but note the magic and crc fields are not made
 * valid in these variables).  Currently, map updates are not allowed within
 * subtransactions, so one set of transaction-level changes is sufficient.
 *
 * The active_xxx variables contain updates that are valid in our transaction
 * and should be honored by RelationMapOidToFilenumber.  The pending_xxx
 * variables contain updates we have been told about that aren't active yet;
 * they will become active at the next CommandCounterIncrement.  This setup
 * lets map updates act similarly to updates of pg_class rows, ie, they
 * become visible only at the next CommandCounterIncrement boundary.
 *
 * Active shared and active local updates are serialized by the parallel
 * infrastructure, and deserialized within parallel workers.
 */
static RelMapFile active_shared_updates;
static RelMapFile active_local_updates;
static RelMapFile pending_shared_updates;
static RelMapFile pending_local_updates;


/* non-export function prototypes */
static void apply_map_update(RelMapFile *map, Oid relationId,
							 RelFileNumber fileNumber, bool add_okay);
static void merge_map_updates(RelMapFile *map, const RelMapFile *updates,
							  bool add_okay);
static void load_relmap_file(bool shared, bool lock_held);
static void read_relmap_file(RelMapFile *map, char *dbpath, bool lock_held,
							 int elevel);
static void write_relmap_file(RelMapFile *newmap, bool write_wal,
							  bool send_sinval, bool preserve_files,
							  Oid dbid, Oid tsid, const char *dbpath);
static void perform_relmap_update(bool shared, const RelMapFile *updates);


/*
 * RelationMapOidToFilenumber
 *
 * The raison d' etre ... given a relation OID, look up its filenumber.
 *
 * Although shared and local relation OIDs should never overlap, the caller
 * always knows which we need --- so pass that information to avoid useless
 * searching.
 *
 * Returns InvalidRelFileNumber if the OID is not known (which should never
 * happen, but the caller is in a better position to report a meaningful
 * error).
 */
RelFileNumber
RelationMapOidToFilenumber(Oid relationId, bool shared)
{
	const RelMapFile *map;
	int32		i;

	/* If there are active updates, believe those over the main maps */
	if (shared)
	{
		map = &active_shared_updates;
		for (i = 0; i < map->num_mappings; i++)
		{
			if (relationId == map->mappings[i].mapoid)
				return map->mappings[i].mapfilenumber;
		}
		map = &shared_map;
		for (i = 0; i < map->num_mappings; i++)
		{
			if (relationId == map->mappings[i].mapoid)
				return map->mappings[i].mapfilenumber;
		}
	}
	else
	{
		map = &active_local_updates;
		for (i = 0; i < map->num_mappings; i++)
		{
			if (relationId == map->mappings[i].mapoid)
				return map->mappings[i].mapfilenumber;
		}
		map = &local_map;
		for (i = 0; i < map->num_mappings; i++)
		{
			if (relationId == map->mappings[i].mapoid)
				return map->mappings[i].mapfilenumber;
		}
	}

	return InvalidRelFileNumber;
}

/*
 * RelationMapFilenumberToOid
 *
 * Do the reverse of the normal direction of mapping done in
 * RelationMapOidToFilenumber.
 *
 * This is not supposed to be used during normal running but rather for
 * information purposes when looking at the filesystem or xlog.
 *
 * Returns InvalidOid if the OID is not known; this can easily happen if the
 * relfilenumber doesn't pertain to a mapped relation.
 */
Oid
RelationMapFilenumberToOid(RelFileNumber filenumber, bool shared)
{
	const RelMapFile *map;
	int32		i;

	/* If there are active updates, believe those over the main maps */
	if (shared)
	{
		map = &active_shared_updates;
		for (i = 0; i < map->num_mappings; i++)
		{
			if (filenumber == map->mappings[i].mapfilenumber)
				return map->mappings[i].mapoid;
		}
		map = &shared_map;
		for (i = 0; i < map->num_mappings; i++)
		{
			if (filenumber == map->mappings[i].mapfilenumber)
				return map->mappings[i].mapoid;
		}
	}
	else
	{
		map = &active_local_updates;
		for (i = 0; i < map->num_mappings; i++)
		{
			if (filenumber == map->mappings[i].mapfilenumber)
				return map->mappings[i].mapoid;
		}
		map = &local_map;
		for (i = 0; i < map->num_mappings; i++)
		{
			if (filenumber == map->mappings[i].mapfilenumber)
				return map->mappings[i].mapoid;
		}
	}

	return InvalidOid;
}

/*
 * RelationMapOidToFilenumberForDatabase
 *
 * Like RelationMapOidToFilenumber, but reads the mapping from the indicated
 * path instead of using the one for the current database.
 */
RelFileNumber
RelationMapOidToFilenumberForDatabase(char *dbpath, Oid relationId)
{
	RelMapFile	map;
	int			i;

	/* Read the relmap file from the source database. */
	read_relmap_file(&map, dbpath, false, ERROR);

	/* Iterate over the relmap entries to find the input relation OID. */
	for (i = 0; i < map.num_mappings; i++)
	{
		if (relationId == map.mappings[i].mapoid)
			return map.mappings[i].mapfilenumber;
	}

	return InvalidRelFileNumber;
}

/*
 * RelationMapCopy
 *
 * Copy relmapfile from source db path to the destination db path and WAL log
 * the operation. This is intended for use in creating a new relmap file
 * for a database that doesn't have one yet, not for replacing an existing
 * relmap file.
 */
void
RelationMapCopy(Oid dbid, Oid tsid, char *srcdbpath, char *dstdbpath)
{
	RelMapFile	map;

	/*
	 * Read the relmap file from the source database.
	 */
	read_relmap_file(&map, srcdbpath, false, ERROR);

	/*
	 * Write the same data into the destination database's relmap file.
	 *
	 * No sinval is needed because no one can be connected to the destination
	 * database yet.
	 *
	 * There's no point in trying to preserve files here. The new database
	 * isn't usable yet anyway, and won't ever be if we can't install a relmap
	 * file.
	 */
	LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
	write_relmap_file(&map, true, false, false, dbid, tsid, dstdbpath);
	LWLockRelease(RelationMappingLock);
}

/*
 * RelationMapUpdateMap
 *
 * Install a new relfilenumber mapping for the specified relation.
 *
 * If immediate is true (or we're bootstrapping), the mapping is activated
 * immediately.  Otherwise it is made pending until CommandCounterIncrement.
 */
void
RelationMapUpdateMap(Oid relationId, RelFileNumber fileNumber, bool shared,
					 bool immediate)
{
	RelMapFile *map;

	if (IsBootstrapProcessingMode())
	{
		/*
		 * In bootstrap mode, the mapping gets installed in permanent map.
		 */
		if (shared)
			map = &shared_map;
		else
			map = &local_map;
	}
	else
	{
		/*
		 * We don't currently support map changes within subtransactions, or
		 * when in parallel mode.  This could be done with more bookkeeping
		 * infrastructure, but it doesn't presently seem worth it.
		 */
		if (GetCurrentTransactionNestLevel() > 1)
			elog(ERROR, "cannot change relation mapping within subtransaction");

		if (IsInParallelMode())
			elog(ERROR, "cannot change relation mapping in parallel mode");

		if (immediate)
		{
			/* Make it active, but only locally */
			if (shared)
				map = &active_shared_updates;
			else
				map = &active_local_updates;
		}
		else
		{
			/* Make it pending */
			if (shared)
				map = &pending_shared_updates;
			else
				map = &pending_local_updates;
		}
	}
	apply_map_update(map, relationId, fileNumber, true);
}

/*
 * apply_map_update
 *
 * Insert a new mapping into the given map variable, replacing any existing
 * mapping for the same relation.
 *
 * In some cases the caller knows there must be an existing mapping; pass
 * add_okay = false to draw an error if not.
 */
static void
apply_map_update(RelMapFile *map, Oid relationId, RelFileNumber fileNumber,
				 bool add_okay)
{
	int32		i;

	/* Replace any existing mapping */
	for (i = 0; i < map->num_mappings; i++)
	{
		if (relationId == map->mappings[i].mapoid)
		{
			map->mappings[i].mapfilenumber = fileNumber;
			return;
		}
	}

	/* Nope, need to add a new mapping */
	if (!add_okay)
		elog(ERROR, "attempt to apply a mapping to unmapped relation %u",
			 relationId);
	if (map->num_mappings >= MAX_MAPPINGS)
		elog(ERROR, "ran out of space in relation map");
	map->mappings[map->num_mappings].mapoid = relationId;
	map->mappings[map->num_mappings].mapfilenumber = fileNumber;
	map->num_mappings++;
}

/*
 * merge_map_updates
 *
 * Merge all the updates in the given pending-update map into the target map.
 * This is just a bulk form of apply_map_update.
 */
static void
merge_map_updates(RelMapFile *map, const RelMapFile *updates, bool add_okay)
{
	int32		i;

	for (i = 0; i < updates->num_mappings; i++)
	{
		apply_map_update(map,
						 updates->mappings[i].mapoid,
						 updates->mappings[i].mapfilenumber,
						 add_okay);
	}
}

/*
 * RelationMapRemoveMapping
 *
 * Remove a relation's entry in the map.  This is only allowed for "active"
 * (but not committed) local mappings.  We need it so we can back out the
 * entry for the transient target file when doing VACUUM FULL/CLUSTER on
 * a mapped relation.
 */
void
RelationMapRemoveMapping(Oid relationId)
{
	RelMapFile *map = &active_local_updates;
	int32		i;

	for (i = 0; i < map->num_mappings; i++)
	{
		if (relationId == map->mappings[i].mapoid)
		{
			/* Found it, collapse it out */
			map->mappings[i] = map->mappings[map->num_mappings - 1];
			map->num_mappings--;
			return;
		}
	}
	elog(ERROR, "could not find temporary mapping for relation %u",
		 relationId);
}

/*
 * RelationMapInvalidate
 *
 * This routine is invoked for SI cache flush messages.  We must re-read
 * the indicated map file.  However, we might receive a SI message in a
 * process that hasn't yet, and might never, load the mapping files;
 * for example the autovacuum launcher, which *must not* try to read
 * a local map since it is attached to no particular database.
 * So, re-read only if the map is valid now.
 */
void
RelationMapInvalidate(bool shared)
{
	if (shared)
	{
		if (shared_map.magic == RELMAPPER_FILEMAGIC)
			load_relmap_file(true, false);
	}
	else
	{
		if (local_map.magic == RELMAPPER_FILEMAGIC)
			load_relmap_file(false, false);
	}

	/*
	 * PGRAC MODIFICATIONS by SqlRush:
	 * Hardening F3 (2026-05-09):  the cluster_smgr_invalidate_relmap
	 * hook used to fire here, but RelationMapInvalidate is the sinval
	 * RECEIVE side -- every backend that gets a relmap inval ran this
	 * callback to reload its own map.  Hooking the broadcast here
	 * meant every peer that received an inval would re-broadcast,
	 * which once spec-2.27 SI Broadcaster goes live would form a
	 * cross-instance loop with no origin suppression.
	 *
	 * The hook moved to the SOURCE side -- write_relmap_file()
	 * alongside the CacheInvalidateRelmap() call -- where it fires
	 * exactly once per relmap update with no origin amplification.
	 */
}

/*
 * RelationMapInvalidateAll
 *
 * Reload all map files.  This is used to recover from SI message buffer
 * overflow: we can't be sure if we missed an inval message.
 * Again, reload only currently-valid maps.
 */
void
RelationMapInvalidateAll(void)
{
	if (shared_map.magic == RELMAPPER_FILEMAGIC)
		load_relmap_file(true, false);
	if (local_map.magic == RELMAPPER_FILEMAGIC)
		load_relmap_file(false, false);
}

/*
 * AtCCI_RelationMap
 *
 * Activate any "pending" relation map updates at CommandCounterIncrement time.
 */
void
AtCCI_RelationMap(void)
{
	if (pending_shared_updates.num_mappings != 0)
	{
		merge_map_updates(&active_shared_updates,
						  &pending_shared_updates,
						  true);
		pending_shared_updates.num_mappings = 0;
	}
	if (pending_local_updates.num_mappings != 0)
	{
		merge_map_updates(&active_local_updates,
						  &pending_local_updates,
						  true);
		pending_local_updates.num_mappings = 0;
	}
}

/*
 * AtEOXact_RelationMap
 *
 * Handle relation mapping at main-transaction commit or abort.
 *
 * During commit, this must be called as late as possible before the actual
 * transaction commit, so as to minimize the window where the transaction
 * could still roll back after committing map changes.  Although nothing
 * critically bad happens in such a case, we still would prefer that it
 * not happen, since we'd possibly be losing useful updates to the relations'
 * pg_class row(s).
 *
 * During abort, we just have to throw away any pending map changes.
 * Normal post-abort cleanup will take care of fixing relcache entries.
 * Parallel worker commit/abort is handled by resetting active mappings
 * that may have been received from the leader process.  (There should be
 * no pending updates in parallel workers.)
 */
void
AtEOXact_RelationMap(bool isCommit, bool isParallelWorker)
{
	if (isCommit && !isParallelWorker)
	{
		/*
		 * We should not get here with any "pending" updates.  (We could
		 * logically choose to treat such as committed, but in the current
		 * code this should never happen.)
		 */
		Assert(pending_shared_updates.num_mappings == 0);
		Assert(pending_local_updates.num_mappings == 0);

		/*
		 * Write any active updates to the actual map files, then reset them.
		 */
		if (active_shared_updates.num_mappings != 0)
		{
			perform_relmap_update(true, &active_shared_updates);
			active_shared_updates.num_mappings = 0;
		}
		if (active_local_updates.num_mappings != 0)
		{
			perform_relmap_update(false, &active_local_updates);
			active_local_updates.num_mappings = 0;
		}
	}
	else
	{
		/* Abort or parallel worker --- drop all local and pending updates */
		Assert(!isParallelWorker || pending_shared_updates.num_mappings == 0);
		Assert(!isParallelWorker || pending_local_updates.num_mappings == 0);

		active_shared_updates.num_mappings = 0;
		active_local_updates.num_mappings = 0;
		pending_shared_updates.num_mappings = 0;
		pending_local_updates.num_mappings = 0;
	}
}

/*
 * AtPrepare_RelationMap
 *
 * Handle relation mapping at PREPARE.
 *
 * Currently, we don't support preparing any transaction that changes the map.
 */
void
AtPrepare_RelationMap(void)
{
	if (active_shared_updates.num_mappings != 0 ||
		active_local_updates.num_mappings != 0 ||
		pending_shared_updates.num_mappings != 0 ||
		pending_local_updates.num_mappings != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that modified relation mapping")));
}

/*
 * CheckPointRelationMap
 *
 * This is called during a checkpoint.  It must ensure that any relation map
 * updates that were WAL-logged before the start of the checkpoint are
 * securely flushed to disk and will not need to be replayed later.  This
 * seems unlikely to be a performance-critical issue, so we use a simple
 * method: we just take and release the RelationMappingLock.  This ensures
 * that any already-logged map update is complete, because write_relmap_file
 * will fsync the map file before the lock is released.
 */
void
CheckPointRelationMap(void)
{
	LWLockAcquire(RelationMappingLock, LW_SHARED);
	LWLockRelease(RelationMappingLock);
}

/*
 * RelationMapFinishBootstrap
 *
 * Write out the initial relation mapping files at the completion of
 * bootstrap.  All the mapped files should have been made known to us
 * via RelationMapUpdateMap calls.
 */
void
RelationMapFinishBootstrap(void)
{
	Assert(IsBootstrapProcessingMode());

	/* Shouldn't be anything "pending" ... */
	Assert(active_shared_updates.num_mappings == 0);
	Assert(active_local_updates.num_mappings == 0);
	Assert(pending_shared_updates.num_mappings == 0);
	Assert(pending_local_updates.num_mappings == 0);

	/* Write the files; no WAL or sinval needed */
	LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
	write_relmap_file(&shared_map, false, false, false,
					  InvalidOid, GLOBALTABLESPACE_OID, "global");
	write_relmap_file(&local_map, false, false, false,
					  MyDatabaseId, MyDatabaseTableSpace, DatabasePath);
	LWLockRelease(RelationMappingLock);
}

/*
 * RelationMapInitialize
 *
 * This initializes the mapper module at process startup.  We can't access the
 * database yet, so just make sure the maps are empty.
 */
void
RelationMapInitialize(void)
{
	/* The static variables should initialize to zeroes, but let's be sure */
	shared_map.magic = 0;		/* mark it not loaded */
	local_map.magic = 0;
	shared_map.num_mappings = 0;
	local_map.num_mappings = 0;
	active_shared_updates.num_mappings = 0;
	active_local_updates.num_mappings = 0;
	pending_shared_updates.num_mappings = 0;
	pending_local_updates.num_mappings = 0;
}

/*
 * RelationMapInitializePhase2
 *
 * This is called to prepare for access to pg_database during startup.
 * We should be able to read the shared map file now.
 */
void
RelationMapInitializePhase2(void)
{
	/*
	 * In bootstrap mode, the map file isn't there yet, so do nothing.
	 */
	if (IsBootstrapProcessingMode())
		return;

	/*
	 * Load the shared map file, die on error.
	 */
	load_relmap_file(true, false);
}

/*
 * RelationMapInitializePhase3
 *
 * This is called as soon as we have determined MyDatabaseId and set up
 * DatabasePath.  At this point we should be able to read the local map file.
 */
void
RelationMapInitializePhase3(void)
{
	/*
	 * In bootstrap mode, the map file isn't there yet, so do nothing.
	 */
	if (IsBootstrapProcessingMode())
		return;

	/*
	 * Load the local map file, die on error.
	 */
	load_relmap_file(false, false);
}

/*
 * EstimateRelationMapSpace
 *
 * Estimate space needed to pass active shared and local relmaps to parallel
 * workers.
 */
Size
EstimateRelationMapSpace(void)
{
	return sizeof(SerializedActiveRelMaps);
}

/*
 * SerializeRelationMap
 *
 * Serialize active shared and local relmap state for parallel workers.
 */
void
SerializeRelationMap(Size maxSize, char *startAddress)
{
	SerializedActiveRelMaps *relmaps;

	Assert(maxSize >= EstimateRelationMapSpace());

	relmaps = (SerializedActiveRelMaps *) startAddress;
	relmaps->active_shared_updates = active_shared_updates;
	relmaps->active_local_updates = active_local_updates;
}

/*
 * RestoreRelationMap
 *
 * Restore active shared and local relmap state within a parallel worker.
 */
void
RestoreRelationMap(char *startAddress)
{
	SerializedActiveRelMaps *relmaps;

	if (active_shared_updates.num_mappings != 0 ||
		active_local_updates.num_mappings != 0 ||
		pending_shared_updates.num_mappings != 0 ||
		pending_local_updates.num_mappings != 0)
		elog(ERROR, "parallel worker has existing mappings");

	relmaps = (SerializedActiveRelMaps *) startAddress;
	active_shared_updates = relmaps->active_shared_updates;
	active_local_updates = relmaps->active_local_updates;
}

/*
 * load_relmap_file -- load the shared or local map file
 *
 * Because these files are essential for access to core system catalogs,
 * failure to load either of them is a fatal error.
 *
 * Note that the local case requires DatabasePath to be set up.
 */
static void
load_relmap_file(bool shared, bool lock_held)
{
	if (shared)
		read_relmap_file(&shared_map, "global", lock_held, FATAL);
	else
		read_relmap_file(&local_map, DatabasePath, lock_held, FATAL);
}

#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC MODIFICATIONS by SqlRush (spec-6.14 D5-activation):
 *
 * cluster_read_relmap_from_authority -- on-mode replacement for the local
 * pg_filenode.map read: adopt the COMMITTED image of the shared relmap
 * authority (INV-14-7; a staged-but-unpublished pending image is never
 * readable).  dbpath identifies the map: "global" is the shared map, any
 * database path resolves through its trailing <dbid> component (the
 * authority is keyed by database OID, not by physical location).
 *
 * Fail-closed: no trustworthy authority, an unresolvable dbpath, or a
 * corrupt inner image all raise 53RB0 at the caller's elevel -- the stale
 * local file is never used as a fallback (INV-14-6).
 *
 * Lock discipline mirrors the vanilla read: RelationMappingLock SHARED
 * unless already held, interlocking against the in-memory map updates the
 * on-mode write path makes under the EXCLUSIVE lock.
 */
static void
cluster_read_relmap_from_authority(RelMapFile *map, const char *dbpath,
								   bool lock_held, int elevel)
{
	bool		shared_authority = (strcmp(dbpath, "global") == 0);
	Oid			dbid = InvalidOid;
	char		image[CLUSTER_RELMAP_IMAGE_MAX];
	uint32		image_len = 0;
	bool		ok;
	pg_crc32c	crc;

	Assert(elevel >= ERROR);

	if (!shared_authority)
	{
		const char *comp = strrchr(dbpath, '/');
		unsigned long parsed;
		char	   *endp;

		comp = (comp != NULL) ? comp + 1 : dbpath;
		errno = 0;
		parsed = strtoul(comp, &endp, 10);
		if (errno != 0 || endp == comp || *endp != '\0' ||
			parsed == 0 || parsed > PG_UINT32_MAX)
			ereport(elevel,
					(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
					 errmsg("cannot resolve database path \"%s\" to a relmap authority",
							dbpath)));
		dbid = (Oid) parsed;
	}

	if (!lock_held)
		LWLockAcquire(RelationMappingLock, LW_SHARED);

	pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_READ);
	ok = cluster_relmap_authority_read_committed(shared_authority, dbid,
												 image, &image_len);
	pgstat_report_wait_end();

	if (!lock_held)
		LWLockRelease(RelationMappingLock);

	if (!ok)
		ereport(elevel,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("cluster relmap authority for \"%s\" is unavailable or not trustworthy",
						dbpath),
				 errhint("Check cluster.shared_data_dir; the node-local relation "
						 "map is never used while cluster.shared_catalog is on.")));

	if (image_len != sizeof(RelMapFile))
		ereport(elevel,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("cluster relmap authority image for \"%s\" has wrong size %u",
						dbpath, image_len)));

	memcpy(map, image, sizeof(RelMapFile));

	/* Same content validation as the vanilla file read below. */
	if (map->magic != RELMAPPER_FILEMAGIC ||
		map->num_mappings < 0 ||
		map->num_mappings > MAX_MAPPINGS)
		ereport(elevel,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("cluster relmap authority image for \"%s\" contains invalid data",
						dbpath)));

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) map, offsetof(RelMapFile, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, map->crc))
		ereport(elevel,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("cluster relmap authority image for \"%s\" contains incorrect checksum",
						dbpath)));
}
#endif							/* USE_PGRAC_CLUSTER */

/*
 * read_relmap_file -- load data from any relation mapper file
 *
 * dbpath must be the relevant database path, or "global" for shared relations.
 *
 * RelationMappingLock will be acquired released unless lock_held = true.
 *
 * Errors will be reported at the indicated elevel, which should be at least
 * ERROR.
 */
static void
read_relmap_file(RelMapFile *map, char *dbpath, bool lock_held, int elevel)
{
	char		mapfilename[MAXPGPATH];
	pg_crc32c	crc;
	int			fd;
	int			r;

	Assert(elevel >= ERROR);

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: on-mode reads adopt the shared relmap authority (spec-6.14 D5). */
	if (cluster_shared_catalog && !IsBootstrapProcessingMode())
	{
		cluster_read_relmap_from_authority(map, dbpath, lock_held, elevel);
		return;
	}
#endif

	/*
	 * Grab the lock to prevent the file from being updated while we read it,
	 * unless the caller is already holding the lock.  If the file is updated
	 * shortly after we look, the sinval signaling mechanism will make us
	 * re-read it before we are able to access any relation that's affected by
	 * the change.
	 */
	if (!lock_held)
		LWLockAcquire(RelationMappingLock, LW_SHARED);

	/*
	 * Open the target file.
	 *
	 * Because Windows isn't happy about the idea of renaming over a file that
	 * someone has open, we only open this file after acquiring the lock, and
	 * for the same reason, we close it before releasing the lock. That way,
	 * by the time write_relmap_file() acquires an exclusive lock, no one else
	 * will have it open.
	 */
	snprintf(mapfilename, sizeof(mapfilename), "%s/%s", dbpath,
			 RELMAPPER_FILENAME);
	fd = OpenTransientFile(mapfilename, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						mapfilename)));

	/* Now read the data. */
	pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_READ);
	r = read(fd, map, sizeof(RelMapFile));
	if (r != sizeof(RelMapFile))
	{
		if (r < 0)
			ereport(elevel,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", mapfilename)));
		else
			ereport(elevel,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %d of %zu",
							mapfilename, r, sizeof(RelMapFile))));
	}
	pgstat_report_wait_end();

	if (CloseTransientFile(fd) != 0)
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						mapfilename)));

	if (!lock_held)
		LWLockRelease(RelationMappingLock);

	/* check for correct magic number, etc */
	if (map->magic != RELMAPPER_FILEMAGIC ||
		map->num_mappings < 0 ||
		map->num_mappings > MAX_MAPPINGS)
		ereport(elevel,
				(errmsg("relation mapping file \"%s\" contains invalid data",
						mapfilename)));

	/* verify the CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) map, offsetof(RelMapFile, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, map->crc))
		ereport(elevel,
				(errmsg("relation mapping file \"%s\" contains incorrect checksum",
						mapfilename)));
}

/*
 * Write out a new shared or local map file with the given contents.
 *
 * The magic number and CRC are automatically updated in *newmap.  On
 * success, we copy the data to the appropriate permanent static variable.
 *
 * If write_wal is true then an appropriate WAL message is emitted.
 * (It will be false for bootstrap and WAL replay cases.)
 *
 * If send_sinval is true then a SI invalidation message is sent.
 * (This should be true except in bootstrap case.)
 *
 * If preserve_files is true then the storage manager is warned not to
 * delete the files listed in the map.
 *
 * Because this may be called during WAL replay when MyDatabaseId,
 * DatabasePath, etc aren't valid, we require the caller to pass in suitable
 * values. Pass dbpath as "global" for the shared map.
 *
 * The caller is also responsible for being sure no concurrent map update
 * could be happening.
 */
static void
write_relmap_file(RelMapFile *newmap, bool write_wal, bool send_sinval,
				  bool preserve_files, Oid dbid, Oid tsid, const char *dbpath)
{
	int			fd;
	char		mapfilename[MAXPGPATH];
	char		maptempfilename[MAXPGPATH];

	/*
	 * Even without concurrent use of this map, CheckPointRelationMap() relies
	 * on this locking.  Without it, a restore of a base backup taken after
	 * this function's XLogInsert() and before its durable_rename() would not
	 * have the changes.  wal_level=minimal doesn't need the lock, but this
	 * isn't performance-critical enough for such a micro-optimization.
	 */
	Assert(LWLockHeldByMeInMode(RelationMappingLock, LW_EXCLUSIVE));

	/*
	 * Fill in the overhead fields and update CRC.
	 */
	newmap->magic = RELMAPPER_FILEMAGIC;
	if (newmap->num_mappings < 0 || newmap->num_mappings > MAX_MAPPINGS)
		elog(ERROR, "attempt to write bogus relation mapping");

	INIT_CRC32C(newmap->crc);
	COMP_CRC32C(newmap->crc, (char *) newmap, offsetof(RelMapFile, crc));
	FIN_CRC32C(newmap->crc);

	/*
	 * Construct filenames -- a temporary file that we'll create to write the
	 * data initially, and then the permanent name to which we will rename it.
	 */
	snprintf(mapfilename, sizeof(mapfilename), "%s/%s",
			 dbpath, RELMAPPER_FILENAME);
	snprintf(maptempfilename, sizeof(maptempfilename), "%s/%s",
			 dbpath, RELMAPPER_TEMP_FILENAME);

	/*
	 * Open a temporary file. If a file already exists with this name, it must
	 * be left over from a previous crash, so we can overwrite it. Concurrent
	 * calls to this function are not allowed.
	 */
	fd = OpenTransientFile(maptempfilename,
						   O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						maptempfilename)));

	/* Write new data to the file. */
	pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_WRITE);
	if (write(fd, newmap, sizeof(RelMapFile)) != sizeof(RelMapFile))
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						maptempfilename)));
	}
	pgstat_report_wait_end();

	/* And close the file. */
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						maptempfilename)));

	if (write_wal)
	{
		xl_relmap_update xlrec;
		XLogRecPtr	lsn;

		/* now errors are fatal ... */
		START_CRIT_SECTION();

		xlrec.dbid = dbid;
		xlrec.tsid = tsid;
		xlrec.nbytes = sizeof(RelMapFile);

		XLogBeginInsert();
		XLogRegisterData((char *) (&xlrec), MinSizeOfRelmapUpdate);
		XLogRegisterData((char *) newmap, sizeof(RelMapFile));

		lsn = XLogInsert(RM_RELMAP_ID, XLOG_RELMAP_UPDATE);

		/* As always, WAL must hit the disk before the data update does */
		XLogFlush(lsn);
	}

	/*
	 * durable_rename() does all the hard work of making sure that we rename
	 * the temporary file into place in a crash-safe manner.
	 *
	 * NB: Although we instruct durable_rename() to use ERROR, we will often
	 * be in a critical section at this point; if so, ERROR will become PANIC.
	 */
	pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_REPLACE);
	durable_rename(maptempfilename, mapfilename, ERROR);
	pgstat_report_wait_end();

	/*
	 * Now that the file is safely on disk, send sinval message to let other
	 * backends know to re-read it.  We must do this inside the critical
	 * section: if for some reason we fail to send the message, we have to
	 * force a database-wide PANIC.  Otherwise other backends might continue
	 * execution with stale mapping information, which would be catastrophic
	 * as soon as others began to use the now-committed data.
	 */
	if (send_sinval)
	{
		CacheInvalidateRelmap(dbid);

		/* PGRAC MODIFICATIONS by SqlRush (hardening F3 2026-05-09 +
		 * F2/L93 2026-05-09 critical section forward-contract):
		 *
		 * cluster_smgr_invalidate_relmap fires HERE -- the source
		 * side of the relmap inval -- not in RelationMapInvalidate
		 * (the receive side).  Sourcing here gives spec-2.27 SI
		 * Broadcaster a single per-update broadcast point with no
		 * origin amplification when peer instances receive +
		 * RelationMapInvalidate-callback the inval locally.
		 *
		 * Q4 v0.2 also requires `cluster.smgr_user_relations` gate
		 * because relmap has no smgr_which routing -- when the GUC
		 * is off the default md.c path covers all permanent rels and
		 * cross-instance map sync isn't part of the contract.
		 *
		 * ⚠ CRITICAL SECTION CONSTRAINT (Hardening v1.0.1 F2 / L93):
		 * write_relmap_file() entered START_CRIT_SECTION() ~1012;
		 * this hook executes INSIDE the critical section.  The current
		 * stub (atomic counter add only via cluster_smgr_remote_invalidation_inc)
		 * is nofail and therefore safe.  spec-2.27 ACTIVATION
		 * CONSTRAINT:  the hook body must remain nofail/nonblocking
		 * inside the critical section (e.g. atomic add, shmem queue
		 * enqueue, flag set).  ACTUAL SI Broadcaster send + peer ack
		 * MUST occur OUTSIDE this critical section -- e.g. via a
		 * post-commit callback, or by spec-2.18 SI Broadcaster aux
		 * process draining the shmem queue from its own loop.
		 * ANY ereport(ERROR) / palloc / wait inside this critical
		 * section will be upgraded to PANIC by PG core (CLAUDE.md
		 * rule 16) and crash the entire postmaster -- catastrophic.
		 * spec-2.27 drafter MUST NOT replace this hook body with a
		 * direct synchronous wire-send;design the queue+drain pattern
		 * first. */
#ifdef USE_PGRAC_CLUSTER
		if (cluster_enabled && cluster_smgr_user_relations)
			cluster_smgr_invalidate_relmap(dbid == InvalidOid);
#endif
	}

	/*
	 * Make sure that the files listed in the map are not deleted if the outer
	 * transaction aborts.  This had better be within the critical section
	 * too: it's not likely to fail, but if it did, we'd arrive at transaction
	 * abort with the files still vulnerable.  PANICing will leave things in a
	 * good state on-disk.
	 *
	 * Note: we're cheating a little bit here by assuming that mapped files
	 * are either in pg_global or the database's default tablespace.
	 */
	if (preserve_files)
	{
		int32		i;

		for (i = 0; i < newmap->num_mappings; i++)
		{
			RelFileLocator rlocator;

			rlocator.spcOid = tsid;
			rlocator.dbOid = dbid;
			rlocator.relNumber = newmap->mappings[i].mapfilenumber;
			RelationPreserveStorage(rlocator, false);
		}
	}

	/* Critical section done */
	if (write_wal)
		END_CRIT_SECTION();
}

#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC MODIFICATIONS by SqlRush (spec-6.14 D5-activation, §3.2):
 *
 * On-mode relmap write path.  A mapped-catalog rewrite commits its new map
 * through the shared authority, never through the local pg_filenode.map:
 *
 *   pre-commit  (cluster_perform_relmap_update, from AtEOXact_RelationMap):
 *     singleton relmap X lock (GES, cluster-wide) -> unresolved-pending
 *     guard (53RB) -> merge updates over the authority's committed image ->
 *     XLOG_RELMAP_UPDATE (WAL-before-data, arbitration breadcrumb) ->
 *     stage the image as PENDING with the owner identity header ->
 *     ForceSyncCommit().  Readers keep adopting the old committed image
 *     (INV-14-7); no invalidation is sent yet (INV-14-8).
 *
 *   post-commit (AtEOXact_ClusterRelmapPublish, from CommitTransaction
 *     right after ProcArrayEndTransaction):  publish (pending->committed,
 *     atomic+durable) -> local relmap sinval -> cross-node relmap sinval
 *     with the spec-2.39 all-alive-peer ack barrier -> release the relmap
 *     lock.  Failures here must never take the ordinary ERROR path (the
 *     owning transaction is durably committed): bounded retries, then
 *     PANIC (r4-P1) -- fail-stop degenerates into the crash-arbitration
 *     path, which publishes by the committed owner_xid.
 *
 *   abort       (AtAbort_ClusterRelmapPending, from AbortTransaction):
 *     discard the staged pending (it can never be published; leaving it
 *     would keep every cluster relmap write 53RB until arbitration) and
 *     let cluster_relmap_lock_abort_release drop the lock.
 *
 * The map-before-content ordering comes from locking (spec §3.2): the
 * mapped rel's AEL and the relmap lock are both held until the post-publish
 * invalidation is acknowledged by every live peer, and the old physical
 * files are unlinked later still (smgrDoPendingDeletes).  A peer that
 * next locks the rel processes the queued invalidation at AIM and reloads
 * the new committed image before it can touch any content.
 */

/* One staged-publish arm per map flavour: [0] = shared, [1] = per-db. */
typedef struct ClusterRelmapPublishArm
{
	bool		armed;
	bool		shared;
	Oid			dbid;
	uint64		generation;
} ClusterRelmapPublishArm;

static ClusterRelmapPublishArm cluster_relmap_publish_arms[2];

/* Bounded post-commit retries before fail-stop (r4-P1). */
#define CLUSTER_RELMAP_PUBLISH_RETRIES 10

static void
cluster_perform_relmap_update(bool shared, const RelMapFile *updates)
{
	Oid			dbid = shared ? InvalidOid : MyDatabaseId;
	Oid			tsid = shared ? GLOBALTABLESPACE_OID : MyDatabaseTableSpace;
	RelMapFile	newmap;
	ClusterRelmapAuthorityHeader hdr;
	ClusterRelmapOwner owner;
	ClusterRelmapPublishArm *arm = &cluster_relmap_publish_arms[shared ? 0 : 1];
	uint64		newgen;
	XLogRecPtr	lsn;
	xl_relmap_update xlrec;
	int32		i;

	Assert(!arm->armed);

	/*
	 * Singleton cluster X lock.  One transaction can commit both a shared
	 * and a per-db map update; both stage under a single hold, released
	 * after the post-commit publish round (or by abort).
	 */
	if (!cluster_relmap_authority_x_held() &&
		!cluster_relmap_authority_x_lock())
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("could not acquire the cluster relation map authority lock"),
				 errhint("The cluster lock service is unreachable; retry once the "
						 "cluster is healthy.")));

	/*
	 * Unresolved-pending guard (r4-P1): while any staged pending image is
	 * awaiting publish or arbitration, every other relmap write in the
	 * cluster fails closed.  Our own lock hold guarantees no LIVE writer
	 * races us, so a pending here is a dead writer's residue.
	 */
	if (!cluster_relmap_authority_read_header(shared, dbid, &hdr))
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("cluster relmap authority for %s map is unavailable",
						shared ? "the shared" : "this database's"),
				 errhint("Check cluster.shared_data_dir.")));
	if (hdr.pending_generation != 0)
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("cluster relmap write is blocked by an unresolved pending "
						"image (owner node %d, xid %u)",
						(int) hdr.owner_node, hdr.owner_xid),
				 errhint("A relation-map writer died before publishing; recovery "
						 "arbitration resolves the pending image.")));

	LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);

	/* Adopt the current committed image, then merge our updates over it. */
	load_relmap_file(shared, true);
	if (shared)
		memcpy(&newmap, &shared_map, sizeof(RelMapFile));
	else
		memcpy(&newmap, &local_map, sizeof(RelMapFile));
	merge_map_updates(&newmap, updates, allowSystemTableMods);

	newgen = hdr.committed_generation + 1;

	/* Finalize the image exactly as write_relmap_file would. */
	newmap.magic = RELMAPPER_FILEMAGIC;
	if (newmap.num_mappings < 0 || newmap.num_mappings > MAX_MAPPINGS)
		elog(ERROR, "attempt to write bogus relation mapping");
	INIT_CRC32C(newmap.crc);
	COMP_CRC32C(newmap.crc, (char *) &newmap, offsetof(RelMapFile, crc));
	FIN_CRC32C(newmap.crc);

	/*
	 * WAL first (same record as the vanilla path, so off-mode PITR and the
	 * per-thread merged replay see identical history); the record LSN goes
	 * into the owner header as the arbitration breadcrumb.
	 */
	START_CRIT_SECTION();
	xlrec.dbid = dbid;
	xlrec.tsid = tsid;
	xlrec.nbytes = sizeof(RelMapFile);
	XLogBeginInsert();
	XLogRegisterData((char *) (&xlrec), MinSizeOfRelmapUpdate);
	XLogRegisterData((char *) &newmap, sizeof(RelMapFile));
	lsn = XLogInsert(RM_RELMAP_ID, XLOG_RELMAP_UPDATE);
	XLogFlush(lsn);
	END_CRIT_SECTION();

	/*
	 * Stage the pending image (durable, torn-safe; the substrate PANICs on
	 * I/O failure -- fail-stop, same posture as the CF authority write).
	 * WAL precedes pending precedes commit: a crash at any point in
	 * between leaves an unpublished pending whose owner never committed,
	 * which arbitration discards.
	 */
	owner.owner_node = (int16) cluster_node_id;
	owner.owner_xid = GetTopTransactionId();
	owner.owner_epoch = cluster_reconfig_get_observed_epoch(cluster_node_id);
	owner.relmap_lsn = (uint64) lsn;
	cluster_relmap_authority_write_pending(shared, dbid, (const char *) &newmap,
										   (uint32) sizeof(RelMapFile),
										   newgen, &owner);

	/*
	 * Keep the new physical files across a later abort (same cheat as the
	 * vanilla path: mapped files live in pg_global or the database's
	 * default tablespace).
	 */
	for (i = 0; i < newmap.num_mappings; i++)
	{
		RelFileLocator rlocator;

		rlocator.spcOid = tsid;
		rlocator.dbOid = dbid;
		rlocator.relNumber = newmap.mappings[i].mapfilenumber;
		RelationPreserveStorage(rlocator, false);
	}

	/* Our own process adopts the new map now (vanilla semantics). */
	if (shared)
		memcpy(&shared_map, &newmap, sizeof(RelMapFile));
	else
		memcpy(&local_map, &newmap, sizeof(RelMapFile));

	LWLockRelease(RelationMappingLock);

	/* Arm the post-commit publish; the commit record must be durable. */
	arm->armed = true;
	arm->shared = shared;
	arm->dbid = dbid;
	arm->generation = newgen;
	ForceSyncCommit();

	/*
	 * Test-only crash lever (t/344): die AFTER the pending image is durable
	 * but BEFORE the commit record -- arbitration must DISCARD (the owner
	 * xid never committed).  SKIP-armed => designed fail-stop.
	 */
	CLUSTER_INJECTION_POINT("cluster-relmap-crash-after-stage");
	if (cluster_injection_should_skip("cluster-relmap-crash-after-stage"))
		ereport(PANIC,
				(errmsg("cluster-relmap-crash-after-stage injection: dying with a "
						"staged, uncommitted relmap pending image")));
}

/*
 * AtEOXact_ClusterRelmapPublish
 *
 * Post-commit half of the on-mode relmap write (see the block comment
 * above).  Called from CommitTransaction after ProcArrayEndTransaction,
 * with the relmap lock and the mapped rel's AEL still held.  No-op unless
 * this transaction staged a pending image.
 */
void
AtEOXact_ClusterRelmapPublish(void)
{
	int			slot;

	if (!cluster_relmap_publish_arms[0].armed &&
		!cluster_relmap_publish_arms[1].armed)
		return;

	/*
	 * Test-only crash lever (t/344): die AFTER the commit record is durable
	 * but BEFORE the publish -- arbitration must PUBLISH (the owner xid
	 * committed).  Checked only when this transaction actually staged
	 * (after the fast no-op return above), so arming never touches
	 * unrelated commits.
	 */
	CLUSTER_INJECTION_POINT("cluster-relmap-crash-before-publish");
	if (cluster_injection_should_skip("cluster-relmap-crash-before-publish"))
		ereport(PANIC,
				(errmsg("cluster-relmap-crash-before-publish injection: dying with "
						"a committed, unpublished relmap pending image")));

	for (slot = 0; slot < 2; slot++)
	{
		ClusterRelmapPublishArm *arm = &cluster_relmap_publish_arms[slot];
		SharedInvalidationMessage msg;

		if (!arm->armed)
			continue;

		/* Atomic durable flip; readers now adopt the new image. */
		cluster_relmap_authority_publish(arm->shared, arm->dbid, arm->generation);

		/*
		 * Invalidations strictly AFTER the publish (INV-14-8): any reload
		 * they trigger reads the new committed image.  Local ring first,
		 * then the cross-node fanout with the all-alive-peer ack barrier.
		 */
		msg.rm.id = SHAREDINVALRELMAP_ID;
		msg.rm.dbId = arm->dbid;
		SendSharedInvalidMessages(&msg, 1);

		if (cluster_peer_mode_enabled())
		{
			int			attempt;
			ClusterSinvalAckResult r = CLUSTER_SINVAL_ACK_TIMEOUT;

			for (attempt = 0; attempt < CLUSTER_RELMAP_PUBLISH_RETRIES; attempt++)
			{
				r = cluster_sinval_enqueue_and_wait_ack(&msg, 1);
				if (r == CLUSTER_SINVAL_ACK_DONE)
					break;
				ereport(WARNING,
						(errmsg("cluster relmap invalidation not acknowledged "
								"(attempt %d of %d), retrying",
								attempt + 1, CLUSTER_RELMAP_PUBLISH_RETRIES)));
			}
			if (r != CLUSTER_SINVAL_ACK_DONE)
				ereport(PANIC,
						(errmsg("cluster relmap invalidation was not acknowledged "
								"by all live peers"),
						 errdetail("The relation map authority was already "
								   "published; a peer serving the stale map would "
								   "read the wrong relation file."),
						 errhint("Fail-stop: recovery arbitration and reconfig "
								 "RESET-all restore coherence.")));
		}

		arm->armed = false;
	}

	cluster_relmap_authority_x_unlock();
}

/*
 * AtAbort_ClusterRelmapPending
 *
 * Abort half: a transaction that staged a pending image and then failed
 * before its commit record discards the staging (still under the relmap
 * lock; cluster_relmap_lock_abort_release drops the lock right after).
 * A post-commit failure never comes through here -- the publish path
 * PANICs instead (r4-P1).
 */
void
AtAbort_ClusterRelmapPending(void)
{
	int			slot;

	for (slot = 0; slot < 2; slot++)
	{
		ClusterRelmapPublishArm *arm = &cluster_relmap_publish_arms[slot];

		if (!arm->armed)
			continue;
		cluster_relmap_authority_discard_pending(arm->shared, arm->dbid,
												 arm->generation);
		arm->armed = false;
	}
}
#endif							/* USE_PGRAC_CLUSTER */

/*
 * Merge the specified updates into the appropriate "real" map,
 * and write out the changes.  This function must be used for committing
 * updates during normal multiuser operation.
 */
static void
perform_relmap_update(bool shared, const RelMapFile *updates)
{
	RelMapFile	newmap;

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: on-mode commits go through the shared authority (D5, §3.2). */
	if (cluster_shared_catalog)
	{
		cluster_perform_relmap_update(shared, updates);
		return;
	}
#endif

	/*
	 * Anyone updating a relation's mapping info should take exclusive lock on
	 * that rel and hold it until commit.  This ensures that there will not be
	 * concurrent updates on the same mapping value; but there could easily be
	 * concurrent updates on different values in the same file. We cover that
	 * by acquiring the RelationMappingLock, re-reading the target file to
	 * ensure it's up to date, applying the updates, and writing the data
	 * before releasing RelationMappingLock.
	 *
	 * There is only one RelationMappingLock.  In principle we could try to
	 * have one per mapping file, but it seems unlikely to be worth the
	 * trouble.
	 */
	LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);

	/* Be certain we see any other updates just made */
	load_relmap_file(shared, true);

	/* Prepare updated data in a local variable */
	if (shared)
		memcpy(&newmap, &shared_map, sizeof(RelMapFile));
	else
		memcpy(&newmap, &local_map, sizeof(RelMapFile));

	/*
	 * Apply the updates to newmap.  No new mappings should appear, unless
	 * somebody is adding indexes to system catalogs.
	 */
	merge_map_updates(&newmap, updates, allowSystemTableMods);

	/* Write out the updated map and do other necessary tasks */
	write_relmap_file(&newmap, true, true, true,
					  (shared ? InvalidOid : MyDatabaseId),
					  (shared ? GLOBALTABLESPACE_OID : MyDatabaseTableSpace),
					  (shared ? "global" : DatabasePath));

	/*
	 * We successfully wrote the updated file, so it's now safe to rely on the
	 * new values in this process, too.
	 */
	if (shared)
		memcpy(&shared_map, &newmap, sizeof(RelMapFile));
	else
		memcpy(&local_map, &newmap, sizeof(RelMapFile));

	/* Now we can release the lock */
	LWLockRelease(RelationMappingLock);
}

/*
 * RELMAP resource manager's routines
 */
void
relmap_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in relmap records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == XLOG_RELMAP_UPDATE)
	{
		xl_relmap_update *xlrec = (xl_relmap_update *) XLogRecGetData(record);
		RelMapFile	newmap;
		char	   *dbpath;

		if (xlrec->nbytes != sizeof(RelMapFile))
			elog(PANIC, "relmap_redo: wrong size %u in relmap update record",
				 xlrec->nbytes);
		memcpy(&newmap, xlrec->data, sizeof(newmap));

		/* We need to construct the pathname for this database */
		dbpath = GetDatabasePath(xlrec->dbid, xlrec->tsid);

		/*
		 * Write out the new map and send sinval, but of course don't write a
		 * new WAL entry.  There's no surrounding transaction to tell to
		 * preserve files, either.
		 *
		 * There shouldn't be anyone else updating relmaps during WAL replay,
		 * but grab the lock to interlock against load_relmap_file().
		 *
		 * Note that we use the same WAL record for updating the relmap of an
		 * existing database as we do for creating a new database. In the
		 * latter case, taking the relmap log and sending sinval messages is
		 * unnecessary, but harmless. If we wanted to avoid it, we could add a
		 * flag to the WAL record to indicate which operation is being
		 * performed.
		 */
		LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
		write_relmap_file(&newmap, false, true, false,
						  xlrec->dbid, xlrec->tsid, dbpath);
		LWLockRelease(RelationMappingLock);

		pfree(dbpath);
	}
	else
		elog(PANIC, "relmap_redo: unknown op code %u", info);
}
