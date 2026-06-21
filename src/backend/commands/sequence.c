/*-------------------------------------------------------------------------
 *
 * sequence.c
 *	  PostgreSQL sequences support code.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/sequence.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_type.h"
#include "catalog/storage_xlog.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_pcm_lock.h"		 /* spec-5.2a D5 — clean-page X-transfer arm */
#include "cluster/cluster_gcs_block.h"		 /* spec-5.2a D4 — backend eager flush */
#include "cluster/cluster_guc.h"			 /* spec-5.4 — cluster.sequence_* GUCs */
#include "cluster/cluster_sequence.h"		 /* spec-5.4 — SQ instance cache + refill */
#include "cluster/storage/cluster_smgr.h"	 /* spec-5.4 — shared-storage activation gate */
#include "utils/timestamp.h"				 /* spec-5.4 — refill-wait deadline */
#endif
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/varlena.h"


/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOG_VALS	32

/*
 * The "special area" of a sequence's buffer page looks like this.
 */
#define SEQ_MAGIC	  0x1717

typedef struct sequence_magic
{
	uint32		magic;
} sequence_magic;

/*
 * We store a SeqTable item for every sequence we have touched in the current
 * session.  This is needed to hold onto nextval/currval state.  (We can't
 * rely on the relcache, since it's only, well, a cache, and may decide to
 * discard entries.)
 */
typedef struct SeqTableData
{
	Oid			relid;			/* pg_class OID of this sequence (hash key) */
	RelFileNumber filenumber;	/* last seen relfilenumber of this sequence */
	LocalTransactionId lxid;	/* xact in which we last did a seq op */
	bool		last_valid;		/* do we have a valid "last" value? */
	int64		last;			/* value last returned by nextval */
	int64		cached;			/* last value already cached for nextval */
	/* if last != cached, we have not used up all the cached values */
	int64		increment;		/* copy of sequence's increment field */
	/* note that increment is zero until we first do nextval_internal() */
} SeqTableData;

typedef SeqTableData *SeqTable;

static HTAB *seqhashtab = NULL; /* hash table for SeqTable items */

/*
 * last_used_seq is updated by nextval() to point to the last used
 * sequence.
 */
static SeqTableData *last_used_seq = NULL;

static void fill_seq_with_data(Relation rel, HeapTuple tuple);
static void fill_seq_fork_with_data(Relation rel, HeapTuple tuple, ForkNumber forkNum);
static Relation lock_and_open_sequence(SeqTable seq);
static void create_seq_hashtable(void);
static void init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel);
static Form_pg_sequence_data read_seq_tuple(Relation rel,
											Buffer *buf, HeapTuple seqdatatuple);
static void init_params(ParseState *pstate, List *options, bool for_identity,
						bool isInit,
						Form_pg_sequence seqform,
						Form_pg_sequence_data seqdataform,
						bool *need_seq_rewrite,
						List **owned_by);
static void do_setval(Oid relid, int64 next, bool iscalled);
static void process_owned_by(Relation seqrel, List *owned_by, bool for_identity);

#ifdef USE_PGRAC_CLUSTER
/*
 * PGRAC: spec-5.2a D4 — flush a just-written cluster + shared-storage sequence
 * page to shared storage from THIS backend (the caller still holds the X
 * content lock on `buf`), so an eligible cross-node clean X-transfer in LMON
 * only ever drops a storage-current page (the LMON drop path cannot FlushBuffer
 * — its XLogFlush of a concurrently-inserted page fails).  Gated exactly on the
 * cluster PCM-track condition so single-node / non-shared / non-cluster
 * sequences keep PG-native behaviour.  Fails closed (ereport) on a write error
 * via the underlying smgr path.
 */
static inline void
cluster_seq_flush_if_shared(Relation rel, Buffer buf)
{
	if (cluster_pcm_is_active()
		&& rel->rd_locator.relNumber >= FirstNormalObjectId)
	{
		cluster_bufmgr_flush_seq_page_to_storage(buf);
	}
}

/*
 * PGRAC: spec-5.4 (v2.0 Q2-B, option B) — cluster sequence disposition.
 *
 *	CLSQ_NATIVE       not a cluster-managed sequence (single node / no peers /
 *	                  cluster off / a system or temp sequence) -> the PG-native
 *	                  nextval path is correct and unchanged.
 *	CLSQ_MANAGED      a user sequence on cluster shared storage in an active
 *	                  multi-node cluster -> the shared page is the single
 *	                  cross-node allocation boundary; *resid is filled.
 *	CLSQ_UNSUPPORTED  a user sequence in an active multi-node cluster that is
 *	                  NOT on shared storage -> cross-node uniqueness cannot be
 *	                  guaranteed.  nextval fails closed (Rule 8.A); setval /
 *	                  ALTER treat it like CLSQ_NATIVE (nothing is cached).
 */
typedef enum ClusterSqDisposition
{
	CLSQ_NATIVE,
	CLSQ_MANAGED,
	CLSQ_UNSUPPORTED,
} ClusterSqDisposition;

static ClusterSqDisposition
cluster_sq_classify(Relation seqrel, ClusterResId *resid)
{
	/* Single node / no peers / cluster disabled -> PG-native (no cross-node). */
	if (!cluster_pcm_is_active())
		return CLSQ_NATIVE;

	/* System catalog or temp sequence -> not cross-node shared -> PG-native. */
	if (seqrel->rd_locator.relNumber < FirstNormalObjectId
		|| RelationUsesLocalBuffers(seqrel))
		return CLSQ_NATIVE;

	/* User sequence in an active multi-node cluster: it must live on shared
	 * storage so the single sequence page is the cross-node boundary. */
	if (cluster_smgr_which_for(seqrel->rd_locator, InvalidBackendId) != 1)
		return CLSQ_UNSUPPORTED;

	if (resid != NULL)
		cluster_sq_resid_encode(MyDatabaseId, RelationGetRelid(seqrel),
								(uint32) seqrel->rd_locator.relNumber, resid);
	return CLSQ_MANAGED;
}

/*
 * PGRAC: spec-5.4 D3 — advance the shared sequence page by one batch.
 *
 *	Mirrors nextval_internal's WAL/page write but allocates a whole [start,
 *	end] segment via the direction-aware authority math (cluster_sq_alloc_
 *	segment) and always WAL-logs (so the page LSN advances into THIS node's
 *	stream, making the eager flush satisfiable for a foreign-LSN page that a
 *	peer's clean X-transfer installed — spec-5.2a).  read_seq_tuple takes the
 *	page X (PCM/CF) which serialises the boundary advance cross-node, and arms
 *	the clean-page X-transfer.  On EXHAUSTED the caller's nextval reports the
 *	PG-native "reached maximum/minimum value" error.
 */
static void
cluster_sq_refill_page(Relation seqrel, int64 incby, int64 minv, int64 maxv,
					   int64 cache, int64 *out_start, int64 *out_end)
{
	Buffer		buf;
	HeapTupleData seqtuple;
	Form_pg_sequence_data seq;
	Page		page;
	int64		gstart,
				gend,
				gcount,
				new_boundary;
	ClusterSqAllocStatus st;

	seq = read_seq_tuple(seqrel, &buf, &seqtuple);
	page = BufferGetPage(buf);

	st = cluster_sq_alloc_segment(seq->last_value, seq->is_called, incby, minv, maxv,
								  cache, &gstart, &gend, &gcount, &new_boundary);
	if (st == CLUSTER_SQ_ALLOC_EXHAUSTED)
	{
		UnlockReleaseBuffer(buf);
		if (incby > 0)
			ereport(ERROR,
					(errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
					 errmsg("nextval: reached maximum value of sequence \"%s\" (%lld)",
							RelationGetRelationName(seqrel), (long long) maxv)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
					 errmsg("nextval: reached minimum value of sequence \"%s\" (%lld)",
							RelationGetRelationName(seqrel), (long long) minv)));
	}

	/* Acquire an xid outside the critical section so commit triggers a WAL
	 * flush + syncrep wait (mirror nextval_internal). */
	if (RelationNeedsWAL(seqrel))
		GetTopTransactionId();

	START_CRIT_SECTION();
	MarkBufferDirty(buf);

	if (RelationNeedsWAL(seqrel))
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

		/* The whole granted segment is durable: WAL the boundary with
		 * log_cnt = 0 so redo restores last_value = new_boundary and a
		 * crash before consuming the segment is a gap, never a reissue. */
		seq->last_value = new_boundary;
		seq->is_called = true;
		seq->log_cnt = 0;

		xlrec.locator = seqrel->rd_locator;
		XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
		XLogRegisterData((char *) seqtuple.t_data, seqtuple.t_len);

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);
		PageSetLSN(page, recptr);
	}

	seq->last_value = new_boundary;
	seq->is_called = true;
	seq->log_cnt = 0;

	END_CRIT_SECTION();

	/* WAL-before-grant + storage-current for the cross-node transfer. */
	cluster_seq_flush_if_shared(seqrel, buf);
	cluster_sq_bump_page_writeback(); /* boundary made durable + storage-visible */

	UnlockReleaseBuffer(buf);

	*out_start = gstart;
	*out_end = gend;
}

/*
 * PGRAC: spec-5.4 D3 — cluster nextval (a value from the node instance cache).
 *
 *	Three-tier: the per-backend SeqTableData fast-path (caller, unchanged) ->
 *	the node instance cache (here) -> the shared sequence page.  begin_refill
 *	atomically serves, claims a refill, or reports a peer refill; only the
 *	claimant runs the (lock-free) page advance.  A bounded wait fails closed on
 *	a stuck peer (Rule 8.A: error, never a possibly-duplicate value).
 */
static int64
cluster_sq_nextval(Relation seqrel, const ClusterResId *resid,
				   int64 incby, int64 minv, int64 maxv, int64 cache)
{
	int64		v = 0;
	int64		gstart = 0,
				gend = 0;
	uint32		gen = resid->field3;
	TimestampTz deadline;

	if (cache < 1)
		cache = 1;

	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
										   cluster_sequence_refill_timeout_ms);

	cluster_sq_refill_prepare_wait();
	for (;;)
	{
		ClusterSqRefillClaim claim;

		claim = cluster_sq_instance_cache_begin_refill(resid, gen, incby, &v);
		if (claim == CLUSTER_SQ_REFILL_SERVED)
		{
			cluster_sq_refill_cancel_wait();
			return v;
		}
		if (claim == CLUSTER_SQ_REFILL_CLAIMED)
			break;

		/* CLUSTER_SQ_REFILL_WAIT: a peer is refilling this sequence. */
		if (GetCurrentTimestamp() >= deadline)
		{
			cluster_sq_refill_cancel_wait();
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_GES_TIMEOUT),
					 errmsg("timed out waiting for cluster sequence \"%s\" refill",
							RelationGetRelationName(seqrel)),
					 errhint("A peer backend's refill exceeded cluster.sequence_refill_timeout_ms.")));
		}
		cluster_sq_refill_sleep(100);
	}
	cluster_sq_refill_cancel_wait();

	/* This backend won the refill: advance the shared page, then publish. */
	PG_TRY();
	{
		cluster_sq_refill_page(seqrel, incby, minv, maxv, cache, &gstart, &gend);
	}
	PG_CATCH();
	{
		cluster_sq_instance_cache_abort_refill(resid);
		PG_RE_THROW();
	}
	PG_END_TRY();

	cluster_sq_instance_cache_finish_refill(resid, gen, incby, gstart, gend);

	/* Serve the first value of the freshly granted segment (a racing peer may
	 * have taken it already -> serve returns the next; if the cache is full and
	 * dropped the segment, serve directly from the grant). */
	if (!cluster_sq_instance_cache_serve(resid, &v))
		v = gstart;
	return v;
}

/*
 * PGRAC: spec-5.4 D6 — strong-invalidate this node's instance cache for a
 *	managed sequence after setval / ALTER changed the shared page.  Other nodes'
 *	caches drain naturally (their next refill reads the new boundary); they only
 *	ever emit values already durably granted to them, so this is 8.A-safe.  The
 *	full cross-node strong broadcast is a forward (spec §v2.0.5).
 */
static void
cluster_sq_invalidate_if_managed(Relation seqrel)
{
	ClusterResId resid;

	if (cluster_sq_classify(seqrel, &resid) == CLSQ_MANAGED)
		cluster_sq_instance_cache_invalidate(&resid);
}
#endif


/*
 * DefineSequence
 *				Creates a new sequence relation
 */
ObjectAddress
DefineSequence(ParseState *pstate, CreateSeqStmt *seq)
{
	FormData_pg_sequence seqform;
	FormData_pg_sequence_data seqdataform;
	bool		need_seq_rewrite;
	List	   *owned_by;
	CreateStmt *stmt = makeNode(CreateStmt);
	Oid			seqoid;
	ObjectAddress address;
	Relation	rel;
	HeapTuple	tuple;
	TupleDesc	tupDesc;
	Datum		value[SEQ_COL_LASTCOL];
	bool		null[SEQ_COL_LASTCOL];
	Datum		pgs_values[Natts_pg_sequence];
	bool		pgs_nulls[Natts_pg_sequence];
	int			i;

	/*
	 * If if_not_exists was given and a relation with the same name already
	 * exists, bail out. (Note: we needn't check this when not if_not_exists,
	 * because DefineRelation will complain anyway.)
	 */
	if (seq->if_not_exists)
	{
		RangeVarGetAndCheckCreationNamespace(seq->sequence, NoLock, &seqoid);
		if (OidIsValid(seqoid))
		{
			/*
			 * If we are in an extension script, insist that the pre-existing
			 * object be a member of the extension, to avoid security risks.
			 */
			ObjectAddressSet(address, RelationRelationId, seqoid);
			checkMembershipInCurrentExtension(&address);

			/* OK to skip */
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							seq->sequence->relname)));
			return InvalidObjectAddress;
		}
	}

	/* Check and set all option values */
	init_params(pstate, seq->options, seq->for_identity, true,
				&seqform, &seqdataform,
				&need_seq_rewrite, &owned_by);

	/*
	 * Create relation (and fill value[] and null[] for the tuple)
	 */
	stmt->tableElts = NIL;
	for (i = SEQ_COL_FIRSTCOL; i <= SEQ_COL_LASTCOL; i++)
	{
		ColumnDef  *coldef = makeNode(ColumnDef);

		coldef->inhcount = 0;
		coldef->is_local = true;
		coldef->is_not_null = true;
		coldef->is_from_type = false;
		coldef->storage = 0;
		coldef->raw_default = NULL;
		coldef->cooked_default = NULL;
		coldef->collClause = NULL;
		coldef->collOid = InvalidOid;
		coldef->constraints = NIL;
		coldef->location = -1;

		null[i - 1] = false;

		switch (i)
		{
			case SEQ_COL_LASTVAL:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "last_value";
				value[i - 1] = Int64GetDatumFast(seqdataform.last_value);
				break;
			case SEQ_COL_LOG:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "log_cnt";
				value[i - 1] = Int64GetDatum((int64) 0);
				break;
			case SEQ_COL_CALLED:
				coldef->typeName = makeTypeNameFromOid(BOOLOID, -1);
				coldef->colname = "is_called";
				value[i - 1] = BoolGetDatum(false);
				break;
		}
		stmt->tableElts = lappend(stmt->tableElts, coldef);
	}

	stmt->relation = seq->sequence;
	stmt->inhRelations = NIL;
	stmt->constraints = NIL;
	stmt->options = NIL;
	stmt->oncommit = ONCOMMIT_NOOP;
	stmt->tablespacename = NULL;
	stmt->if_not_exists = seq->if_not_exists;

	address = DefineRelation(stmt, RELKIND_SEQUENCE, seq->ownerId, NULL, NULL);
	seqoid = address.objectId;
	Assert(seqoid != InvalidOid);

	rel = table_open(seqoid, AccessExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* now initialize the sequence's data */
	tuple = heap_form_tuple(tupDesc, value, null);
	fill_seq_with_data(rel, tuple);

	/* process OWNED BY if given */
	if (owned_by)
		process_owned_by(rel, owned_by, seq->for_identity);

	table_close(rel, NoLock);

	/* fill in pg_sequence */
	rel = table_open(SequenceRelationId, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	memset(pgs_nulls, 0, sizeof(pgs_nulls));

	pgs_values[Anum_pg_sequence_seqrelid - 1] = ObjectIdGetDatum(seqoid);
	pgs_values[Anum_pg_sequence_seqtypid - 1] = ObjectIdGetDatum(seqform.seqtypid);
	pgs_values[Anum_pg_sequence_seqstart - 1] = Int64GetDatumFast(seqform.seqstart);
	pgs_values[Anum_pg_sequence_seqincrement - 1] = Int64GetDatumFast(seqform.seqincrement);
	pgs_values[Anum_pg_sequence_seqmax - 1] = Int64GetDatumFast(seqform.seqmax);
	pgs_values[Anum_pg_sequence_seqmin - 1] = Int64GetDatumFast(seqform.seqmin);
	pgs_values[Anum_pg_sequence_seqcache - 1] = Int64GetDatumFast(seqform.seqcache);
	pgs_values[Anum_pg_sequence_seqcycle - 1] = BoolGetDatum(seqform.seqcycle);

	tuple = heap_form_tuple(tupDesc, pgs_values, pgs_nulls);
	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);
	table_close(rel, RowExclusiveLock);

	return address;
}

/*
 * Reset a sequence to its initial value.
 *
 * The change is made transactionally, so that on failure of the current
 * transaction, the sequence will be restored to its previous state.
 * We do that by creating a whole new relfilenumber for the sequence; so this
 * works much like the rewriting forms of ALTER TABLE.
 *
 * Caller is assumed to have acquired AccessExclusiveLock on the sequence,
 * which must not be released until end of transaction.  Caller is also
 * responsible for permissions checking.
 */
void
ResetSequence(Oid seq_relid)
{
	Relation	seq_rel;
	SeqTable	elm;
	Form_pg_sequence_data seq;
	Buffer		buf;
	HeapTupleData seqdatatuple;
	HeapTuple	tuple;
	HeapTuple	pgstuple;
	Form_pg_sequence pgsform;
	int64		startv;

	/*
	 * Read the old sequence.  This does a bit more work than really
	 * necessary, but it's simple, and we do want to double-check that it's
	 * indeed a sequence.
	 */
	init_sequence(seq_relid, &elm, &seq_rel);
	(void) read_seq_tuple(seq_rel, &buf, &seqdatatuple);

	pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(seq_relid));
	if (!HeapTupleIsValid(pgstuple))
		elog(ERROR, "cache lookup failed for sequence %u", seq_relid);
	pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);
	startv = pgsform->seqstart;
	ReleaseSysCache(pgstuple);

	/*
	 * Copy the existing sequence tuple.
	 */
	tuple = heap_copytuple(&seqdatatuple);

	/* Now we're done with the old page */
	UnlockReleaseBuffer(buf);

	/*
	 * Modify the copied tuple to execute the restart (compare the RESTART
	 * action in AlterSequence)
	 */
	seq = (Form_pg_sequence_data) GETSTRUCT(tuple);
	seq->last_value = startv;
	seq->is_called = false;
	seq->log_cnt = 0;

	/*
	 * Create a new storage file for the sequence.
	 */
	RelationSetNewRelfilenumber(seq_rel, seq_rel->rd_rel->relpersistence);

	/*
	 * Ensure sequence's relfrozenxid is at 0, since it won't contain any
	 * unfrozen XIDs.  Same with relminmxid, since a sequence will never
	 * contain multixacts.
	 */
	Assert(seq_rel->rd_rel->relfrozenxid == InvalidTransactionId);
	Assert(seq_rel->rd_rel->relminmxid == InvalidMultiXactId);

	/*
	 * Insert the modified tuple into the new storage file.
	 */
	fill_seq_with_data(seq_rel, tuple);

	/* Clear local cache so that we don't think we have cached numbers */
	/* Note that we do not change the currval() state */
	elm->cached = elm->last;

	relation_close(seq_rel, NoLock);
}

/*
 * Initialize a sequence's relation with the specified tuple as content
 *
 * This handles unlogged sequences by writing to both the main and the init
 * fork as necessary.
 */
static void
fill_seq_with_data(Relation rel, HeapTuple tuple)
{
	fill_seq_fork_with_data(rel, tuple, MAIN_FORKNUM);

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
	{
		SMgrRelation srel;

		srel = smgropen(rel->rd_locator, InvalidBackendId);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(&rel->rd_locator, INIT_FORKNUM);
		fill_seq_fork_with_data(rel, tuple, INIT_FORKNUM);
		FlushRelationBuffers(rel);
		smgrclose(srel);
	}
}

/*
 * Initialize a sequence's relation fork with the specified tuple as content
 */
static void
fill_seq_fork_with_data(Relation rel, HeapTuple tuple, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	sequence_magic *sm;
	OffsetNumber offnum;

#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC: spec-5.2a D5a — shared-storage sequence DDL idempotency.
	 *
	 *	In a cluster with shared storage every node runs CREATE SEQUENCE on the
	 *	same (deterministic) relfilenode, so a peer may have ALREADY initialized
	 *	block 0 of this fork on the shared file.  PG-native ExtendBufferedRel
	 *	would then append an ORPHAN block 1 (benign only because nextval always
	 *	reads block 0; it trips the block-0 Assert on cassert).  Worse, blindly
	 *	re-initializing the existing block 0 back to the START value would RESET
	 *	a sequence a peer has already advanced -> duplicate numbers (Rule 8.A).
	 *
	 *	So this is "idempotent open-or-init", never blind re-init:
	 *	  - fork empty                 -> fall through to the PG-native init.
	 *	  - block 0 is a VALID seq page -> REUSE it untouched (the shared page is
	 *	    the authoritative state; preserve any advanced last_value).
	 *	  - block 0 exists but is NOT a valid seq page -> fail closed (never
	 *	    overwrite, never extend a second block).
	 *	This is a shared-storage DDL idempotency fix, NOT part of the CF
	 *	ownership protocol.  Single-node / non-cluster / non-shared sequences
	 *	fall through to the unchanged PG-native path below.  We take only a
	 *	SHARE content lock (validation read; we never mutate the reused page) so
	 *	CREATE does not force a writer-transfer.
	 */
	if (cluster_pcm_is_active()
		&& rel->rd_rel->relkind == RELKIND_SEQUENCE
		&& rel->rd_locator.relNumber >= FirstNormalObjectId
		&& RelationGetNumberOfBlocksInFork(rel, forkNum) > 0)
	{
		bool		valid_seq_page = false;

		buf = ReadBufferExtended(rel, forkNum, 0, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		if (!PageIsNew(page)
			&& PageGetSpecialSize(page) == MAXALIGN(sizeof(sequence_magic)))
		{
			sm = (sequence_magic *) PageGetSpecialPointer(page);
			if (sm->magic == SEQ_MAGIC
				&& PageGetMaxOffsetNumber(page) >= FirstOffsetNumber
				&& ItemIdIsNormal(PageGetItemId(page, FirstOffsetNumber)))
				valid_seq_page = true;
		}

		if (!valid_seq_page)
		{
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buf);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cluster: sequence \"%s\" relfilenode already has a "
							"non-sequence block 0 on shared storage",
							RelationGetRelationName(rel)),
					 errhint("A different relation may have reused this relfilenode; the "
							 "shared storage is not catalog-aligned for this sequence.")));
		}

		/* Reuse the peer-initialized page untouched (no re-init, no second
		 * block, no WAL) -- the shared page is the authoritative state. */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
		return;
	}
#endif

	/* Initialize first page of relation with special magic number */

	buf = ExtendBufferedRel(BMR_REL(rel), forkNum, NULL,
							EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);
	Assert(BufferGetBlockNumber(buf) == 0);

	page = BufferGetPage(buf);

	PageInit(page, BufferGetPageSize(buf), sizeof(sequence_magic));
	sm = (sequence_magic *) PageGetSpecialPointer(page);
	sm->magic = SEQ_MAGIC;

	/* Now insert sequence tuple */

	/*
	 * Since VACUUM does not process sequences, we have to force the tuple to
	 * have xmin = FrozenTransactionId now.  Otherwise it would become
	 * invisible to SELECTs after 2G transactions.  It is okay to do this
	 * because if the current transaction aborts, no other xact will ever
	 * examine the sequence tuple anyway.
	 */
	HeapTupleHeaderSetXmin(tuple->t_data, FrozenTransactionId);
	HeapTupleHeaderSetXminFrozen(tuple->t_data);
	HeapTupleHeaderSetCmin(tuple->t_data, FirstCommandId);
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	ItemPointerSet(&tuple->t_data->t_ctid, 0, FirstOffsetNumber);

	/* check the comment above nextval_internal()'s equivalent call. */
	if (RelationNeedsWAL(rel))
		GetTopTransactionId();

	START_CRIT_SECTION();

	MarkBufferDirty(buf);

	offnum = PageAddItem(page, (Item) tuple->t_data, tuple->t_len,
						 InvalidOffsetNumber, false, false);
	if (offnum != FirstOffsetNumber)
		elog(ERROR, "failed to add sequence tuple to page");

	/* XLOG stuff */
	if (RelationNeedsWAL(rel) || forkNum == INIT_FORKNUM)
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

		xlrec.locator = rel->rd_locator;

		XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
		XLogRegisterData((char *) tuple->t_data, tuple->t_len);

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

#ifdef USE_PGRAC_CLUSTER
	cluster_seq_flush_if_shared(rel, buf);	/* spec-5.2a D4 — durable on shared storage */
#endif

	UnlockReleaseBuffer(buf);
}

/*
 * AlterSequence
 *
 * Modify the definition of a sequence relation
 */
ObjectAddress
AlterSequence(ParseState *pstate, AlterSeqStmt *stmt)
{
	Oid			relid;
	SeqTable	elm;
	Relation	seqrel;
	Buffer		buf;
	HeapTupleData datatuple;
	Form_pg_sequence seqform;
	Form_pg_sequence_data newdataform;
	bool		need_seq_rewrite;
	List	   *owned_by;
	ObjectAddress address;
	Relation	rel;
	HeapTuple	seqtuple;
	HeapTuple	newdatatuple;

	/* Open and lock sequence, and check for ownership along the way. */
	relid = RangeVarGetRelidExtended(stmt->sequence,
									 ShareRowExclusiveLock,
									 stmt->missing_ok ? RVR_MISSING_OK : 0,
									 RangeVarCallbackOwnsRelation,
									 NULL);
	if (relid == InvalidOid)
	{
		ereport(NOTICE,
				(errmsg("relation \"%s\" does not exist, skipping",
						stmt->sequence->relname)));
		return InvalidObjectAddress;
	}

	init_sequence(relid, &elm, &seqrel);

	rel = table_open(SequenceRelationId, RowExclusiveLock);
	seqtuple = SearchSysCacheCopy1(SEQRELID,
								   ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(seqtuple))
		elog(ERROR, "cache lookup failed for sequence %u",
			 relid);

	seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);

	/* lock page's buffer and read tuple into new sequence structure */
	(void) read_seq_tuple(seqrel, &buf, &datatuple);

	/* copy the existing sequence data tuple, so it can be modified locally */
	newdatatuple = heap_copytuple(&datatuple);
	newdataform = (Form_pg_sequence_data) GETSTRUCT(newdatatuple);

	UnlockReleaseBuffer(buf);

	/* Check and set new values */
	init_params(pstate, stmt->options, stmt->for_identity, false,
				seqform, newdataform,
				&need_seq_rewrite, &owned_by);

	/* Clear local cache so that we don't think we have cached numbers */
	/* Note that we do not change the currval() state */
	elm->cached = elm->last;

	/* If needed, rewrite the sequence relation itself */
	if (need_seq_rewrite)
	{
		/* check the comment above nextval_internal()'s equivalent call. */
		if (RelationNeedsWAL(seqrel))
			GetTopTransactionId();

		/*
		 * Create a new storage file for the sequence, making the state
		 * changes transactional.
		 */
		RelationSetNewRelfilenumber(seqrel, seqrel->rd_rel->relpersistence);

		/*
		 * Ensure sequence's relfrozenxid is at 0, since it won't contain any
		 * unfrozen XIDs.  Same with relminmxid, since a sequence will never
		 * contain multixacts.
		 */
		Assert(seqrel->rd_rel->relfrozenxid == InvalidTransactionId);
		Assert(seqrel->rd_rel->relminmxid == InvalidMultiXactId);

		/*
		 * Insert the modified tuple into the new storage file.
		 */
		fill_seq_with_data(seqrel, newdatatuple);
	}

	/* process OWNED BY if given */
	if (owned_by)
		process_owned_by(seqrel, owned_by, stmt->for_identity);

	/* update the pg_sequence tuple (we could skip this in some cases...) */
	CatalogTupleUpdate(rel, &seqtuple->t_self, seqtuple);

	InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

	ObjectAddressSet(address, RelationRelationId, relid);

#ifdef USE_PGRAC_CLUSTER
	cluster_sq_invalidate_if_managed(seqrel); /* spec-5.4 D6 */
#endif

	table_close(rel, RowExclusiveLock);
	relation_close(seqrel, NoLock);

	return address;
}

void
SequenceChangePersistence(Oid relid, char newrelpersistence)
{
	SeqTable	elm;
	Relation	seqrel;
	Buffer		buf;
	HeapTupleData seqdatatuple;

	/*
	 * ALTER SEQUENCE acquires this lock earlier.  If we're processing an
	 * owned sequence for ALTER TABLE, lock now.  Without the lock, we'd
	 * discard increments from nextval() calls (in other sessions) between
	 * this function's buffer unlock and this transaction's commit.
	 */
	LockRelationOid(relid, AccessExclusiveLock);
	init_sequence(relid, &elm, &seqrel);

	/* check the comment above nextval_internal()'s equivalent call. */
	if (RelationNeedsWAL(seqrel))
		GetTopTransactionId();

	(void) read_seq_tuple(seqrel, &buf, &seqdatatuple);
	RelationSetNewRelfilenumber(seqrel, newrelpersistence);
	fill_seq_with_data(seqrel, &seqdatatuple);
	UnlockReleaseBuffer(buf);

	relation_close(seqrel, NoLock);
}

void
DeleteSequenceTuple(Oid relid)
{
	Relation	rel;
	HeapTuple	tuple;

	rel = table_open(SequenceRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for sequence %u", relid);

	CatalogTupleDelete(rel, &tuple->t_self);

	ReleaseSysCache(tuple);
	table_close(rel, RowExclusiveLock);
}

/*
 * Note: nextval with a text argument is no longer exported as a pg_proc
 * entry, but we keep it around to ease porting of C code that may have
 * called the function directly.
 */
Datum
nextval(PG_FUNCTION_ARGS)
{
	text	   *seqin = PG_GETARG_TEXT_PP(0);
	RangeVar   *sequence;
	Oid			relid;

	sequence = makeRangeVarFromNameList(textToQualifiedNameList(seqin));

	/*
	 * XXX: This is not safe in the presence of concurrent DDL, but acquiring
	 * a lock here is more expensive than letting nextval_internal do it,
	 * since the latter maintains a cache that keeps us from hitting the lock
	 * manager more than once per transaction.  It's not clear whether the
	 * performance penalty is material in practice, but for now, we do it this
	 * way.
	 */
	relid = RangeVarGetRelid(sequence, NoLock, false);

	PG_RETURN_INT64(nextval_internal(relid, true));
}

Datum
nextval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	PG_RETURN_INT64(nextval_internal(relid, true));
}

int64
nextval_internal(Oid relid, bool check_permissions)
{
	SeqTable	elm;
	Relation	seqrel;
	Buffer		buf;
	Page		page;
	HeapTuple	pgstuple;
	Form_pg_sequence pgsform;
	HeapTupleData seqdatatuple;
	Form_pg_sequence_data seq;
	int64		incby,
				maxv,
				minv,
				cache,
				log,
				fetch,
				last;
	int64		result,
				next,
				rescnt = 0;
	bool		cycle;
	bool		logit = false;

	/* open and lock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (check_permissions &&
		pg_class_aclcheck(elm->relid, GetUserId(),
						  ACL_USAGE | ACL_UPDATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	/* read-only transactions may only modify temp sequences */
	if (!seqrel->rd_islocaltemp)
		PreventCommandIfReadOnly("nextval()");

	/*
	 * Forbid this during parallel operation because, to make it work, the
	 * cooperating backends would need to share the backend-local cached
	 * sequence information.  Currently, we don't support that.
	 */
	PreventCommandIfParallelMode("nextval()");

	if (elm->last != elm->cached)	/* some numbers were cached */
	{
		Assert(elm->last_valid);
		Assert(elm->increment != 0);
		elm->last += elm->increment;
		relation_close(seqrel, NoLock);
		last_used_seq = elm;
		return elm->last;
	}

	pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(pgstuple))
		elog(ERROR, "cache lookup failed for sequence %u", relid);
	pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);
	incby = pgsform->seqincrement;
	maxv = pgsform->seqmax;
	minv = pgsform->seqmin;
	cache = pgsform->seqcache;
	cycle = pgsform->seqcycle;
	ReleaseSysCache(pgstuple);

#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC: spec-5.4 D5 — cluster sequence hook.  For a user sequence on
	 * cluster shared storage the value comes from the per-node instance cache
	 * (refilled from the shared page, the single cross-node boundary), not the
	 * per-backend page refill below.  The :776 fast-path stays unchanged; a
	 * cluster sequence simply keeps last == cached so it always re-enters here.
	 * A non-shared user sequence in a multi-node cluster fails closed (8.A);
	 * single-node / system / temp sequences fall through to PG-native.
	 */
	{
		ClusterResId sq_resid;
		ClusterSqDisposition sq_disp = cluster_sq_classify(seqrel, &sq_resid);

		if (sq_disp == CLSQ_UNSUPPORTED)
		{
			cluster_sq_bump_dup_guard_fail();
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_SEQUENCE_ALLOC_UNAVAILABLE),
					 errmsg("cannot guarantee cross-node uniqueness for sequence \"%s\" in cluster mode",
							RelationGetRelationName(seqrel)),
					 errdetail("The sequence is not on cluster shared storage."),
					 errhint("Place user relations on cluster shared storage "
							 "(cluster_smgr_user_relations) so the sequence page is the "
							 "single cross-node allocation boundary.")));
		}

		if (sq_disp == CLSQ_MANAGED)
		{
			int64		sq_val;

			/* Cluster CYCLE forward (spec §3.8 AL4): cross-node value reuse
			 * conflicts with the uniqueness guarantee. */
			if (cycle)
			{
				cluster_sq_bump_cycle_rejected();
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("CYCLE is not supported for cluster sequence \"%s\"",
								RelationGetRelationName(seqrel)),
						 errhint("Cross-node value reuse is not supported in this release; "
								 "recreate the sequence with NO CYCLE.")));
			}

			sq_val = cluster_sq_nextval(seqrel, &sq_resid, incby, minv, maxv, cache);

			elm->increment = incby;
			elm->last = sq_val;
			elm->cached = sq_val; /* keep last == cached: re-enter next time */
			elm->last_valid = true;
			last_used_seq = elm;

			relation_close(seqrel, NoLock);
			return sq_val;
		}
	}
#endif

	/* lock page' buffer and read tuple */
	seq = read_seq_tuple(seqrel, &buf, &seqdatatuple);
	page = BufferGetPage(buf);

	elm->increment = incby;
	last = next = result = seq->last_value;
	fetch = cache;
	log = seq->log_cnt;

	if (!seq->is_called)
	{
		rescnt++;				/* return last_value if not is_called */
		fetch--;
	}

	/*
	 * Decide whether we should emit a WAL log record.  If so, force up the
	 * fetch count to grab SEQ_LOG_VALS more values than we actually need to
	 * cache.  (These will then be usable without logging.)
	 *
	 * If this is the first nextval after a checkpoint, we must force a new
	 * WAL record to be written anyway, else replay starting from the
	 * checkpoint would fail to advance the sequence past the logged values.
	 * In this case we may as well fetch extra values.
	 */
	if (log < fetch || !seq->is_called)
	{
		/* forced log to satisfy local demand for values */
		fetch = log = fetch + SEQ_LOG_VALS;
		logit = true;
	}
	else
	{
		XLogRecPtr	redoptr = GetRedoRecPtr();

		if (PageGetLSN(page) <= redoptr)
		{
			/* last update of seq was before checkpoint */
			fetch = log = fetch + SEQ_LOG_VALS;
			logit = true;
		}
	}

#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC: spec-5.2a D4 — on shared storage the sequence page may carry a
	 * FOREIGN WAL LSN: it was installed from a peer's per-node WAL stream
	 * (spec-4.1) by a cross-node clean X-transfer.  The eager flush below
	 * (cluster_seq_flush_if_shared -> FlushBuffer -> XLogFlush(page_lsn))
	 * cannot satisfy a foreign LSN from THIS node's WAL ("flush request not
	 * satisfied").  Force this nextval to WAL-log so the page LSN advances into
	 * the LOCAL stream, making the flush's WAL-before-data satisfiable and the
	 * page durable + storage-current for the cross-node transfer / stale-holder
	 * storage-fallback.  For CACHE > 1 this only fires on a refill (the per-
	 * backend fast-path never reaches here), so the extra WAL is amortized.
	 */
	if (cluster_pcm_is_active()
		&& seqrel->rd_locator.relNumber >= FirstNormalObjectId)
		logit = true;
#endif

	while (fetch)				/* try to fetch cache [+ log ] numbers */
	{
		/*
		 * Check MAXVALUE for ascending sequences and MINVALUE for descending
		 * sequences
		 */
		if (incby > 0)
		{
			/* ascending sequence */
			if ((maxv >= 0 && next > maxv - incby) ||
				(maxv < 0 && next + incby > maxv))
			{
				if (rescnt > 0)
					break;		/* stop fetching */
				if (!cycle)
					ereport(ERROR,
							(errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
							 errmsg("nextval: reached maximum value of sequence \"%s\" (%lld)",
									RelationGetRelationName(seqrel),
									(long long) maxv)));
				next = minv;
			}
			else
				next += incby;
		}
		else
		{
			/* descending sequence */
			if ((minv < 0 && next < minv - incby) ||
				(minv >= 0 && next + incby < minv))
			{
				if (rescnt > 0)
					break;		/* stop fetching */
				if (!cycle)
					ereport(ERROR,
							(errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
							 errmsg("nextval: reached minimum value of sequence \"%s\" (%lld)",
									RelationGetRelationName(seqrel),
									(long long) minv)));
				next = maxv;
			}
			else
				next += incby;
		}
		fetch--;
		if (rescnt < cache)
		{
			log--;
			rescnt++;
			last = next;
			if (rescnt == 1)	/* if it's first result - */
				result = next;	/* it's what to return */
		}
	}

	log -= fetch;				/* adjust for any unfetched numbers */
	Assert(log >= 0);

	/* save info in local cache */
	elm->last = result;			/* last returned number */
	elm->cached = last;			/* last fetched number */
	elm->last_valid = true;

	last_used_seq = elm;

	/*
	 * If something needs to be WAL logged, acquire an xid, so this
	 * transaction's commit will trigger a WAL flush and wait for syncrep.
	 * It's sufficient to ensure the toplevel transaction has an xid, no need
	 * to assign xids subxacts, that'll already trigger an appropriate wait.
	 * (Have to do that here, so we're outside the critical section)
	 */
	if (logit && RelationNeedsWAL(seqrel))
		GetTopTransactionId();

	/* ready to change the on-disk (or really, in-buffer) tuple */
	START_CRIT_SECTION();

	/*
	 * We must mark the buffer dirty before doing XLogInsert(); see notes in
	 * SyncOneBuffer().  However, we don't apply the desired changes just yet.
	 * This looks like a violation of the buffer update protocol, but it is in
	 * fact safe because we hold exclusive lock on the buffer.  Any other
	 * process, including a checkpoint, that tries to examine the buffer
	 * contents will block until we release the lock, and then will see the
	 * final state that we install below.
	 */
	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (logit && RelationNeedsWAL(seqrel))
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;

		/*
		 * We don't log the current state of the tuple, but rather the state
		 * as it would appear after "log" more fetches.  This lets us skip
		 * that many future WAL records, at the cost that we lose those
		 * sequence values if we crash.
		 */
		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

		/* set values that will be saved in xlog */
		seq->last_value = next;
		seq->is_called = true;
		seq->log_cnt = 0;

		xlrec.locator = seqrel->rd_locator;

		XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
		XLogRegisterData((char *) seqdatatuple.t_data, seqdatatuple.t_len);

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

		PageSetLSN(page, recptr);
	}

	/* Now update sequence tuple to the intended final state */
	seq->last_value = last;		/* last fetched number */
	seq->is_called = true;
	seq->log_cnt = log;			/* how much is logged */

	END_CRIT_SECTION();

#ifdef USE_PGRAC_CLUSTER
	cluster_seq_flush_if_shared(seqrel, buf);	/* spec-5.2a D4 */
#endif

	UnlockReleaseBuffer(buf);

	relation_close(seqrel, NoLock);

	return result;
}

Datum
currval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		result;
	SeqTable	elm;
	Relation	seqrel;

	/* open and lock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(),
						  ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	if (!elm->last_valid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("currval of sequence \"%s\" is not yet defined in this session",
						RelationGetRelationName(seqrel))));

	result = elm->last;

	relation_close(seqrel, NoLock);

	PG_RETURN_INT64(result);
}

Datum
lastval(PG_FUNCTION_ARGS)
{
	Relation	seqrel;
	int64		result;

	if (last_used_seq == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lastval is not yet defined in this session")));

	/* Someone may have dropped the sequence since the last nextval() */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(last_used_seq->relid)))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("lastval is not yet defined in this session")));

	seqrel = lock_and_open_sequence(last_used_seq);

	/* nextval() must have already been called for this sequence */
	Assert(last_used_seq->last_valid);

	if (pg_class_aclcheck(last_used_seq->relid, GetUserId(),
						  ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	result = last_used_seq->last;
	relation_close(seqrel, NoLock);

	PG_RETURN_INT64(result);
}

/*
 * Main internal procedure that handles 2 & 3 arg forms of SETVAL.
 *
 * Note that the 3 arg version (which sets the is_called flag) is
 * only for use in pg_dump, and setting the is_called flag may not
 * work if multiple users are attached to the database and referencing
 * the sequence (unlikely if pg_dump is restoring it).
 *
 * It is necessary to have the 3 arg version so that pg_dump can
 * restore the state of a sequence exactly during data-only restores -
 * it is the only way to clear the is_called flag in an existing
 * sequence.
 */
static void
do_setval(Oid relid, int64 next, bool iscalled)
{
	SeqTable	elm;
	Relation	seqrel;
	Buffer		buf;
	HeapTupleData seqdatatuple;
	Form_pg_sequence_data seq;
	HeapTuple	pgstuple;
	Form_pg_sequence pgsform;
	int64		maxv,
				minv;

	/* open and lock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (pg_class_aclcheck(elm->relid, GetUserId(), ACL_UPDATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	pgstuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(pgstuple))
		elog(ERROR, "cache lookup failed for sequence %u", relid);
	pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);
	maxv = pgsform->seqmax;
	minv = pgsform->seqmin;
	ReleaseSysCache(pgstuple);

	/* read-only transactions may only modify temp sequences */
	if (!seqrel->rd_islocaltemp)
		PreventCommandIfReadOnly("setval()");

	/*
	 * Forbid this during parallel operation because, to make it work, the
	 * cooperating backends would need to share the backend-local cached
	 * sequence information.  Currently, we don't support that.
	 */
	PreventCommandIfParallelMode("setval()");

	/* lock page' buffer and read tuple */
	seq = read_seq_tuple(seqrel, &buf, &seqdatatuple);

	if ((next < minv) || (next > maxv))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("setval: value %lld is out of bounds for sequence \"%s\" (%lld..%lld)",
						(long long) next, RelationGetRelationName(seqrel),
						(long long) minv, (long long) maxv)));

	/* Set the currval() state only if iscalled = true */
	if (iscalled)
	{
		elm->last = next;		/* last returned number */
		elm->last_valid = true;
	}

	/* In any case, forget any future cached numbers */
	elm->cached = elm->last;

	/* check the comment above nextval_internal()'s equivalent call. */
	if (RelationNeedsWAL(seqrel))
		GetTopTransactionId();

	/* ready to change the on-disk (or really, in-buffer) tuple */
	START_CRIT_SECTION();

	seq->last_value = next;		/* last fetched number */
	seq->is_called = iscalled;
	seq->log_cnt = 0;

	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (RelationNeedsWAL(seqrel))
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;
		Page		page = BufferGetPage(buf);

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

		xlrec.locator = seqrel->rd_locator;
		XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
		XLogRegisterData((char *) seqdatatuple.t_data, seqdatatuple.t_len);

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

#ifdef USE_PGRAC_CLUSTER
	cluster_seq_flush_if_shared(seqrel, buf);	/* spec-5.2a D4 */
#endif

	UnlockReleaseBuffer(buf);

#ifdef USE_PGRAC_CLUSTER
	cluster_sq_invalidate_if_managed(seqrel); /* spec-5.4 D6 */
#endif

	relation_close(seqrel, NoLock);
}

/*
 * Implement the 2 arg setval procedure.
 * See do_setval for discussion.
 */
Datum
setval_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		next = PG_GETARG_INT64(1);

	do_setval(relid, next, true);

	PG_RETURN_INT64(next);
}

/*
 * Implement the 3 arg setval procedure.
 * See do_setval for discussion.
 */
Datum
setval3_oid(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		next = PG_GETARG_INT64(1);
	bool		iscalled = PG_GETARG_BOOL(2);

	do_setval(relid, next, iscalled);

	PG_RETURN_INT64(next);
}


/*
 * Open the sequence and acquire lock if needed
 *
 * If we haven't touched the sequence already in this transaction,
 * we need to acquire a lock.  We arrange for the lock to
 * be owned by the top transaction, so that we don't need to do it
 * more than once per xact.
 */
static Relation
lock_and_open_sequence(SeqTable seq)
{
	LocalTransactionId thislxid = MyProc->lxid;

	/* Get the lock if not already held in this xact */
	if (seq->lxid != thislxid)
	{
		ResourceOwner currentOwner;

		currentOwner = CurrentResourceOwner;
		CurrentResourceOwner = TopTransactionResourceOwner;

		LockRelationOid(seq->relid, RowExclusiveLock);

		CurrentResourceOwner = currentOwner;

		/* Flag that we have a lock in the current xact */
		seq->lxid = thislxid;
	}

	/* We now know we have the lock, and can safely open the rel */
	return relation_open(seq->relid, NoLock);
}

/*
 * Creates the hash table for storing sequence data
 */
static void
create_seq_hashtable(void)
{
	HASHCTL		ctl;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(SeqTableData);

	seqhashtab = hash_create("Sequence values", 16, &ctl,
							 HASH_ELEM | HASH_BLOBS);
}

/*
 * Given a relation OID, open and lock the sequence.  p_elm and p_rel are
 * output parameters.
 */
static void
init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel)
{
	SeqTable	elm;
	Relation	seqrel;
	bool		found;

	/* Find or create a hash table entry for this sequence */
	if (seqhashtab == NULL)
		create_seq_hashtable();

	elm = (SeqTable) hash_search(seqhashtab, &relid, HASH_ENTER, &found);

	/*
	 * Initialize the new hash table entry if it did not exist already.
	 *
	 * NOTE: seqhashtab entries are stored for the life of a backend (unless
	 * explicitly discarded with DISCARD). If the sequence itself is deleted
	 * then the entry becomes wasted memory, but it's small enough that this
	 * should not matter.
	 */
	if (!found)
	{
		/* relid already filled in */
		elm->filenumber = InvalidRelFileNumber;
		elm->lxid = InvalidLocalTransactionId;
		elm->last_valid = false;
		elm->last = elm->cached = 0;
	}

	/*
	 * Open the sequence relation.
	 */
	seqrel = lock_and_open_sequence(elm);

	if (seqrel->rd_rel->relkind != RELKIND_SEQUENCE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sequence",
						RelationGetRelationName(seqrel))));

	/*
	 * If the sequence has been transactionally replaced since we last saw it,
	 * discard any cached-but-unissued values.  We do not touch the currval()
	 * state, however.
	 */
	if (seqrel->rd_rel->relfilenode != elm->filenumber)
	{
		elm->filenumber = seqrel->rd_rel->relfilenode;
		elm->cached = elm->last;
	}

	/* Return results */
	*p_elm = elm;
	*p_rel = seqrel;
}


/*
 * Given an opened sequence relation, lock the page buffer and find the tuple
 *
 * *buf receives the reference to the pinned-and-ex-locked buffer
 * *seqdatatuple receives the reference to the sequence tuple proper
 *		(this arg should point to a local variable of type HeapTupleData)
 *
 * Function's return value points to the data payload of the tuple
 */
static Form_pg_sequence_data
read_seq_tuple(Relation rel, Buffer *buf, HeapTuple seqdatatuple)
{
	Page		page;
	ItemId		lp;
	sequence_magic *sm;
	Form_pg_sequence_data seq;

	*buf = ReadBuffer(rel, 0);

#ifdef USE_PGRAC_CLUSTER
	/*
	 * PGRAC: spec-5.2a D5 — arm the X LockBuffer below as a clean-page
	 * X-transfer.  A sequence page carries no MVCC / ITL / visibility state, so
	 * a cross-node X transfer of it can flush-data-before-drop and be served
	 * from shared storage on stale-holder recovery (spec-5.2a D3/D4).  We arm in
	 * read_seq_tuple — not only nextval_internal's refill — so EVERY sequence
	 * page X write (nextval refill AND setval / restart) flushes the current
	 * page to shared storage before any cross-node drop; the storage-fallback
	 * stale-holder break (D3) reads the current value only because that
	 * invariant holds for all sequence writes (Rule 8.A, inv③).  Gate exactly on
	 * the cluster PCM-track condition (cluster active + a normal-object relfile
	 * on shared storage, mirroring cluster_bufmgr_should_pcm_track) so the arm
	 * is consumed by THIS LockBuffer's PCM acquire and can never leak into a
	 * later heap access (one-shot, inv①/⑤).  nextval's per-backend cache
	 * fast-path returns before reaching read_seq_tuple, so it is unaffected; a
	 * non-cluster / single-node / non-shared sequence never arms (PG-native).
	 */
	if (cluster_pcm_is_active()
		&& rel->rd_locator.relNumber >= FirstNormalObjectId)
		cluster_pcm_clean_page_xfer_arm(true);
#endif

	LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(*buf);
	sm = (sequence_magic *) PageGetSpecialPointer(page);

	if (sm->magic != SEQ_MAGIC)
		elog(ERROR, "bad magic number in sequence \"%s\": %08X",
			 RelationGetRelationName(rel), sm->magic);

	lp = PageGetItemId(page, FirstOffsetNumber);
	Assert(ItemIdIsNormal(lp));

	/* Note we currently only bother to set these two fields of *seqdatatuple */
	seqdatatuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
	seqdatatuple->t_len = ItemIdGetLength(lp);

	/*
	 * Previous releases of Postgres neglected to prevent SELECT FOR UPDATE on
	 * a sequence, which would leave a non-frozen XID in the sequence tuple's
	 * xmax, which eventually leads to clog access failures or worse. If we
	 * see this has happened, clean up after it.  We treat this like a hint
	 * bit update, ie, don't bother to WAL-log it, since we can certainly do
	 * this again if the update gets lost.
	 */
	Assert(!(seqdatatuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));
	if (HeapTupleHeaderGetRawXmax(seqdatatuple->t_data) != InvalidTransactionId)
	{
		HeapTupleHeaderSetXmax(seqdatatuple->t_data, InvalidTransactionId);
		seqdatatuple->t_data->t_infomask &= ~HEAP_XMAX_COMMITTED;
		seqdatatuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
		MarkBufferDirtyHint(*buf, true);
	}

	seq = (Form_pg_sequence_data) GETSTRUCT(seqdatatuple);

	return seq;
}

/*
 * init_params: process the options list of CREATE or ALTER SEQUENCE, and
 * store the values into appropriate fields of seqform, for changes that go
 * into the pg_sequence catalog, and fields of seqdataform for changes to the
 * sequence relation itself.  Set *need_seq_rewrite to true if we changed any
 * parameters that require rewriting the sequence's relation (interesting for
 * ALTER SEQUENCE).  Also set *owned_by to any OWNED BY option, or to NIL if
 * there is none.
 *
 * If isInit is true, fill any unspecified options with default values;
 * otherwise, do not change existing options that aren't explicitly overridden.
 *
 * Note: we force a sequence rewrite whenever we change parameters that affect
 * generation of future sequence values, even if the seqdataform per se is not
 * changed.  This allows ALTER SEQUENCE to behave transactionally.  Currently,
 * the only option that doesn't cause that is OWNED BY.  It's *necessary* for
 * ALTER SEQUENCE OWNED BY to not rewrite the sequence, because that would
 * break pg_upgrade by causing unwanted changes in the sequence's
 * relfilenumber.
 */
static void
init_params(ParseState *pstate, List *options, bool for_identity,
			bool isInit,
			Form_pg_sequence seqform,
			Form_pg_sequence_data seqdataform,
			bool *need_seq_rewrite,
			List **owned_by)
{
	DefElem    *as_type = NULL;
	DefElem    *start_value = NULL;
	DefElem    *restart_value = NULL;
	DefElem    *increment_by = NULL;
	DefElem    *max_value = NULL;
	DefElem    *min_value = NULL;
	DefElem    *cache_value = NULL;
	DefElem    *is_cycled = NULL;
	ListCell   *option;
	bool		reset_max_value = false;
	bool		reset_min_value = false;

	*need_seq_rewrite = false;
	*owned_by = NIL;

	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "as") == 0)
		{
			if (as_type)
				errorConflictingDefElem(defel, pstate);
			as_type = defel;
			*need_seq_rewrite = true;
		}
		else if (strcmp(defel->defname, "increment") == 0)
		{
			if (increment_by)
				errorConflictingDefElem(defel, pstate);
			increment_by = defel;
			*need_seq_rewrite = true;
		}
		else if (strcmp(defel->defname, "start") == 0)
		{
			if (start_value)
				errorConflictingDefElem(defel, pstate);
			start_value = defel;
			*need_seq_rewrite = true;
		}
		else if (strcmp(defel->defname, "restart") == 0)
		{
			if (restart_value)
				errorConflictingDefElem(defel, pstate);
			restart_value = defel;
			*need_seq_rewrite = true;
		}
		else if (strcmp(defel->defname, "maxvalue") == 0)
		{
			if (max_value)
				errorConflictingDefElem(defel, pstate);
			max_value = defel;
			*need_seq_rewrite = true;
		}
		else if (strcmp(defel->defname, "minvalue") == 0)
		{
			if (min_value)
				errorConflictingDefElem(defel, pstate);
			min_value = defel;
			*need_seq_rewrite = true;
		}
		else if (strcmp(defel->defname, "cache") == 0)
		{
			if (cache_value)
				errorConflictingDefElem(defel, pstate);
			cache_value = defel;
			*need_seq_rewrite = true;
		}
		else if (strcmp(defel->defname, "cycle") == 0)
		{
			if (is_cycled)
				errorConflictingDefElem(defel, pstate);
			is_cycled = defel;
			*need_seq_rewrite = true;
		}
		else if (strcmp(defel->defname, "owned_by") == 0)
		{
			if (*owned_by)
				errorConflictingDefElem(defel, pstate);
			*owned_by = defGetQualifiedName(defel);
		}
		else if (strcmp(defel->defname, "sequence_name") == 0)
		{
			/*
			 * The parser allows this, but it is only for identity columns, in
			 * which case it is filtered out in parse_utilcmd.c.  We only get
			 * here if someone puts it into a CREATE SEQUENCE, where it'd be
			 * redundant.  (The same is true for the equally-nonstandard
			 * LOGGED and UNLOGGED options, but for those, the default error
			 * below seems sufficient.)
			 */
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid sequence option SEQUENCE NAME"),
					 parser_errposition(pstate, defel->location)));
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	/*
	 * We must reset log_cnt when isInit or when changing any parameters that
	 * would affect future nextval allocations.
	 */
	if (isInit)
		seqdataform->log_cnt = 0;

	/* AS type */
	if (as_type != NULL)
	{
		Oid			newtypid = typenameTypeId(pstate, defGetTypeName(as_type));

		if (newtypid != INT2OID &&
			newtypid != INT4OID &&
			newtypid != INT8OID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 for_identity
					 ? errmsg("identity column type must be smallint, integer, or bigint")
					 : errmsg("sequence type must be smallint, integer, or bigint")));

		if (!isInit)
		{
			/*
			 * When changing type and the old sequence min/max values were the
			 * min/max of the old type, adjust sequence min/max values to
			 * min/max of new type.  (Otherwise, the user chose explicit
			 * min/max values, which we'll leave alone.)
			 */
			if ((seqform->seqtypid == INT2OID && seqform->seqmax == PG_INT16_MAX) ||
				(seqform->seqtypid == INT4OID && seqform->seqmax == PG_INT32_MAX) ||
				(seqform->seqtypid == INT8OID && seqform->seqmax == PG_INT64_MAX))
				reset_max_value = true;
			if ((seqform->seqtypid == INT2OID && seqform->seqmin == PG_INT16_MIN) ||
				(seqform->seqtypid == INT4OID && seqform->seqmin == PG_INT32_MIN) ||
				(seqform->seqtypid == INT8OID && seqform->seqmin == PG_INT64_MIN))
				reset_min_value = true;
		}

		seqform->seqtypid = newtypid;
	}
	else if (isInit)
	{
		seqform->seqtypid = INT8OID;
	}

	/* INCREMENT BY */
	if (increment_by != NULL)
	{
		seqform->seqincrement = defGetInt64(increment_by);
		if (seqform->seqincrement == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("INCREMENT must not be zero")));
		seqdataform->log_cnt = 0;
	}
	else if (isInit)
	{
		seqform->seqincrement = 1;
	}

	/* CYCLE */
	if (is_cycled != NULL)
	{
		seqform->seqcycle = boolVal(is_cycled->arg);
		Assert(BoolIsValid(seqform->seqcycle));
		seqdataform->log_cnt = 0;
	}
	else if (isInit)
	{
		seqform->seqcycle = false;
	}

	/* MAXVALUE (null arg means NO MAXVALUE) */
	if (max_value != NULL && max_value->arg)
	{
		seqform->seqmax = defGetInt64(max_value);
		seqdataform->log_cnt = 0;
	}
	else if (isInit || max_value != NULL || reset_max_value)
	{
		if (seqform->seqincrement > 0 || reset_max_value)
		{
			/* ascending seq */
			if (seqform->seqtypid == INT2OID)
				seqform->seqmax = PG_INT16_MAX;
			else if (seqform->seqtypid == INT4OID)
				seqform->seqmax = PG_INT32_MAX;
			else
				seqform->seqmax = PG_INT64_MAX;
		}
		else
			seqform->seqmax = -1;	/* descending seq */
		seqdataform->log_cnt = 0;
	}

	/* Validate maximum value.  No need to check INT8 as seqmax is an int64 */
	if ((seqform->seqtypid == INT2OID && (seqform->seqmax < PG_INT16_MIN || seqform->seqmax > PG_INT16_MAX))
		|| (seqform->seqtypid == INT4OID && (seqform->seqmax < PG_INT32_MIN || seqform->seqmax > PG_INT32_MAX)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("MAXVALUE (%lld) is out of range for sequence data type %s",
						(long long) seqform->seqmax,
						format_type_be(seqform->seqtypid))));

	/* MINVALUE (null arg means NO MINVALUE) */
	if (min_value != NULL && min_value->arg)
	{
		seqform->seqmin = defGetInt64(min_value);
		seqdataform->log_cnt = 0;
	}
	else if (isInit || min_value != NULL || reset_min_value)
	{
		if (seqform->seqincrement < 0 || reset_min_value)
		{
			/* descending seq */
			if (seqform->seqtypid == INT2OID)
				seqform->seqmin = PG_INT16_MIN;
			else if (seqform->seqtypid == INT4OID)
				seqform->seqmin = PG_INT32_MIN;
			else
				seqform->seqmin = PG_INT64_MIN;
		}
		else
			seqform->seqmin = 1;	/* ascending seq */
		seqdataform->log_cnt = 0;
	}

	/* Validate minimum value.  No need to check INT8 as seqmin is an int64 */
	if ((seqform->seqtypid == INT2OID && (seqform->seqmin < PG_INT16_MIN || seqform->seqmin > PG_INT16_MAX))
		|| (seqform->seqtypid == INT4OID && (seqform->seqmin < PG_INT32_MIN || seqform->seqmin > PG_INT32_MAX)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("MINVALUE (%lld) is out of range for sequence data type %s",
						(long long) seqform->seqmin,
						format_type_be(seqform->seqtypid))));

	/* crosscheck min/max */
	if (seqform->seqmin >= seqform->seqmax)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("MINVALUE (%lld) must be less than MAXVALUE (%lld)",
						(long long) seqform->seqmin,
						(long long) seqform->seqmax)));

	/* START WITH */
	if (start_value != NULL)
	{
		seqform->seqstart = defGetInt64(start_value);
	}
	else if (isInit)
	{
		if (seqform->seqincrement > 0)
			seqform->seqstart = seqform->seqmin;	/* ascending seq */
		else
			seqform->seqstart = seqform->seqmax;	/* descending seq */
	}

	/* crosscheck START */
	if (seqform->seqstart < seqform->seqmin)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("START value (%lld) cannot be less than MINVALUE (%lld)",
						(long long) seqform->seqstart,
						(long long) seqform->seqmin)));
	if (seqform->seqstart > seqform->seqmax)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("START value (%lld) cannot be greater than MAXVALUE (%lld)",
						(long long) seqform->seqstart,
						(long long) seqform->seqmax)));

	/* RESTART [WITH] */
	if (restart_value != NULL)
	{
		if (restart_value->arg != NULL)
			seqdataform->last_value = defGetInt64(restart_value);
		else
			seqdataform->last_value = seqform->seqstart;
		seqdataform->is_called = false;
		seqdataform->log_cnt = 0;
	}
	else if (isInit)
	{
		seqdataform->last_value = seqform->seqstart;
		seqdataform->is_called = false;
	}

	/* crosscheck RESTART (or current value, if changing MIN/MAX) */
	if (seqdataform->last_value < seqform->seqmin)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("RESTART value (%lld) cannot be less than MINVALUE (%lld)",
						(long long) seqdataform->last_value,
						(long long) seqform->seqmin)));
	if (seqdataform->last_value > seqform->seqmax)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("RESTART value (%lld) cannot be greater than MAXVALUE (%lld)",
						(long long) seqdataform->last_value,
						(long long) seqform->seqmax)));

	/* CACHE */
	if (cache_value != NULL)
	{
		seqform->seqcache = defGetInt64(cache_value);
		if (seqform->seqcache <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("CACHE (%lld) must be greater than zero",
							(long long) seqform->seqcache)));
		seqdataform->log_cnt = 0;
	}
	else if (isInit)
	{
		seqform->seqcache = 1;
	}
}

/*
 * Process an OWNED BY option for CREATE/ALTER SEQUENCE
 *
 * Ownership permissions on the sequence are already checked,
 * but if we are establishing a new owned-by dependency, we must
 * enforce that the referenced table has the same owner and namespace
 * as the sequence.
 */
static void
process_owned_by(Relation seqrel, List *owned_by, bool for_identity)
{
	DependencyType deptype;
	int			nnames;
	Relation	tablerel;
	AttrNumber	attnum;

	deptype = for_identity ? DEPENDENCY_INTERNAL : DEPENDENCY_AUTO;

	nnames = list_length(owned_by);
	Assert(nnames > 0);
	if (nnames == 1)
	{
		/* Must be OWNED BY NONE */
		if (strcmp(strVal(linitial(owned_by)), "none") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid OWNED BY option"),
					 errhint("Specify OWNED BY table.column or OWNED BY NONE.")));
		tablerel = NULL;
		attnum = 0;
	}
	else
	{
		List	   *relname;
		char	   *attrname;
		RangeVar   *rel;

		/* Separate relname and attr name */
		relname = list_copy_head(owned_by, nnames - 1);
		attrname = strVal(llast(owned_by));

		/* Open and lock rel to ensure it won't go away meanwhile */
		rel = makeRangeVarFromNameList(relname);
		tablerel = relation_openrv(rel, AccessShareLock);

		/* Must be a regular or foreign table */
		if (!(tablerel->rd_rel->relkind == RELKIND_RELATION ||
			  tablerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE ||
			  tablerel->rd_rel->relkind == RELKIND_VIEW ||
			  tablerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("sequence cannot be owned by relation \"%s\"",
							RelationGetRelationName(tablerel)),
					 errdetail_relkind_not_supported(tablerel->rd_rel->relkind)));

		/* We insist on same owner and schema */
		if (seqrel->rd_rel->relowner != tablerel->rd_rel->relowner)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("sequence must have same owner as table it is linked to")));
		if (RelationGetNamespace(seqrel) != RelationGetNamespace(tablerel))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("sequence must be in same schema as table it is linked to")));

		/* Now, fetch the attribute number from the system cache */
		attnum = get_attnum(RelationGetRelid(tablerel), attrname);
		if (attnum == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							attrname, RelationGetRelationName(tablerel))));
	}

	/*
	 * Catch user explicitly running OWNED BY on identity sequence.
	 */
	if (deptype == DEPENDENCY_AUTO)
	{
		Oid			tableId;
		int32		colId;

		if (sequenceIsOwned(RelationGetRelid(seqrel), DEPENDENCY_INTERNAL, &tableId, &colId))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot change ownership of identity sequence"),
					 errdetail("Sequence \"%s\" is linked to table \"%s\".",
							   RelationGetRelationName(seqrel),
							   get_rel_name(tableId))));
	}

	/*
	 * OK, we are ready to update pg_depend.  First remove any existing
	 * dependencies for the sequence, then optionally add a new one.
	 */
	deleteDependencyRecordsForClass(RelationRelationId, RelationGetRelid(seqrel),
									RelationRelationId, deptype);

	if (tablerel)
	{
		ObjectAddress refobject,
					depobject;

		refobject.classId = RelationRelationId;
		refobject.objectId = RelationGetRelid(tablerel);
		refobject.objectSubId = attnum;
		depobject.classId = RelationRelationId;
		depobject.objectId = RelationGetRelid(seqrel);
		depobject.objectSubId = 0;
		recordDependencyOn(&depobject, &refobject, deptype);
	}

	/* Done, but hold lock until commit */
	if (tablerel)
		relation_close(tablerel, NoLock);
}


/*
 * Return sequence parameters in a list of the form created by the parser.
 */
List *
sequence_options(Oid relid)
{
	HeapTuple	pgstuple;
	Form_pg_sequence pgsform;
	List	   *options = NIL;

	pgstuple = SearchSysCache1(SEQRELID, relid);
	if (!HeapTupleIsValid(pgstuple))
		elog(ERROR, "cache lookup failed for sequence %u", relid);
	pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);

	/* Use makeFloat() for 64-bit integers, like gram.y does. */
	options = lappend(options,
					  makeDefElem("cache", (Node *) makeFloat(psprintf(INT64_FORMAT, pgsform->seqcache)), -1));
	options = lappend(options,
					  makeDefElem("cycle", (Node *) makeBoolean(pgsform->seqcycle), -1));
	options = lappend(options,
					  makeDefElem("increment", (Node *) makeFloat(psprintf(INT64_FORMAT, pgsform->seqincrement)), -1));
	options = lappend(options,
					  makeDefElem("maxvalue", (Node *) makeFloat(psprintf(INT64_FORMAT, pgsform->seqmax)), -1));
	options = lappend(options,
					  makeDefElem("minvalue", (Node *) makeFloat(psprintf(INT64_FORMAT, pgsform->seqmin)), -1));
	options = lappend(options,
					  makeDefElem("start", (Node *) makeFloat(psprintf(INT64_FORMAT, pgsform->seqstart)), -1));

	ReleaseSysCache(pgstuple);

	return options;
}

/*
 * Return sequence parameters (formerly for use by information schema)
 */
Datum
pg_sequence_parameters(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TupleDesc	tupdesc;
	Datum		values[7];
	bool		isnull[7];
	HeapTuple	pgstuple;
	Form_pg_sequence pgsform;

	if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_UPDATE | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						get_rel_name(relid))));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	memset(isnull, 0, sizeof(isnull));

	pgstuple = SearchSysCache1(SEQRELID, relid);
	if (!HeapTupleIsValid(pgstuple))
		elog(ERROR, "cache lookup failed for sequence %u", relid);
	pgsform = (Form_pg_sequence) GETSTRUCT(pgstuple);

	values[0] = Int64GetDatum(pgsform->seqstart);
	values[1] = Int64GetDatum(pgsform->seqmin);
	values[2] = Int64GetDatum(pgsform->seqmax);
	values[3] = Int64GetDatum(pgsform->seqincrement);
	values[4] = BoolGetDatum(pgsform->seqcycle);
	values[5] = Int64GetDatum(pgsform->seqcache);
	values[6] = ObjectIdGetDatum(pgsform->seqtypid);

	ReleaseSysCache(pgstuple);

	return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

/*
 * Return the last value from the sequence
 *
 * Note: This has a completely different meaning than lastval().
 */
Datum
pg_sequence_last_value(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	SeqTable	elm;
	Relation	seqrel;
	bool		is_called = false;
	int64		result = 0;

	/* open and lock sequence */
	init_sequence(relid, &elm, &seqrel);

	if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_USAGE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for sequence %s",
						RelationGetRelationName(seqrel))));

	/*
	 * We return NULL for other sessions' temporary sequences.  The
	 * pg_sequences system view already filters those out, but this offers a
	 * defense against ERRORs in case someone invokes this function directly.
	 *
	 * Also, for the benefit of the pg_sequences view, we return NULL for
	 * unlogged sequences on standbys instead of throwing an error.
	 */
	if (!RELATION_IS_OTHER_TEMP(seqrel) &&
		(RelationIsPermanent(seqrel) || !RecoveryInProgress()))
	{
		Buffer		buf;
		HeapTupleData seqtuple;
		Form_pg_sequence_data seq;

		seq = read_seq_tuple(seqrel, &buf, &seqtuple);

		is_called = seq->is_called;
		result = seq->last_value;

		UnlockReleaseBuffer(buf);
	}
	relation_close(seqrel, NoLock);

	if (is_called)
		PG_RETURN_INT64(result);
	else
		PG_RETURN_NULL();
}


void
seq_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	Buffer		buffer;
	Page		page;
	Page		localpage;
	char	   *item;
	Size		itemsz;
	xl_seq_rec *xlrec = (xl_seq_rec *) XLogRecGetData(record);
	sequence_magic *sm;

	if (info != XLOG_SEQ_LOG)
		elog(PANIC, "seq_redo: unknown op code %u", info);

	buffer = XLogInitBufferForRedo(record, 0);
	page = (Page) BufferGetPage(buffer);

	/*
	 * We always reinit the page.  However, since this WAL record type is also
	 * used for updating sequences, it's possible that a hot-standby backend
	 * is examining the page concurrently; so we mustn't transiently trash the
	 * buffer.  The solution is to build the correct new page contents in
	 * local workspace and then memcpy into the buffer.  Then only bytes that
	 * are supposed to change will change, even transiently. We must palloc
	 * the local page for alignment reasons.
	 */
	localpage = (Page) palloc(BufferGetPageSize(buffer));

	PageInit(localpage, BufferGetPageSize(buffer), sizeof(sequence_magic));
	sm = (sequence_magic *) PageGetSpecialPointer(localpage);
	sm->magic = SEQ_MAGIC;

	item = (char *) xlrec + sizeof(xl_seq_rec);
	itemsz = XLogRecGetDataLen(record) - sizeof(xl_seq_rec);

	if (PageAddItem(localpage, (Item) item, itemsz,
					FirstOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(PANIC, "seq_redo: failed to add item to page");

	PageSetLSN(localpage, lsn);

	memcpy(page, localpage, BufferGetPageSize(buffer));
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	pfree(localpage);
}

/*
 * Flush cached sequence information.
 */
void
ResetSequenceCaches(void)
{
	if (seqhashtab)
	{
		hash_destroy(seqhashtab);
		seqhashtab = NULL;
	}

	last_used_seq = NULL;
}

/*
 * Mask a Sequence page before performing consistency checks on it.
 */
void
seq_mask(char *page, BlockNumber blkno)
{
	mask_page_lsn_and_checksum(page);

	mask_unused_space(page);
}
