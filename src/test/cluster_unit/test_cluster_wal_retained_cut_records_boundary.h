/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_retained_cut_records_boundary.h
 *	  Fixtures of test_cluster_wal_retained_cut_records.c: the retained input
 *	  scope (sources and the records each one yields) and builders of
 *	  decoded records in their real WAL layouts.  Included once, after the
 *	  product census source.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_wal_retained_cut_records_boundary.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_WAL_RETAINED_CUT_RECORDS_BOUNDARY_H
#define TEST_CLUSTER_WAL_RETAINED_CUT_RECORDS_BOUNDARY_H

#define REC_SYSID UINT64_C(0x11223344)
#define REC_DBINC 42
#define REC_MAX_ITEMS 3
#define REC_MAX_RECORDS 16

/* ---- backend globals and services the census links against ---- */
bool cluster_enabled = true;
bool cluster_shared_config = true;
volatile sig_atomic_t ShutdownRequestPending = false;
volatile sig_atomic_t InterruptPending = false;
volatile uint32 CritSectionCount = 0;
AuxProcType MyAuxProcType = CheckpointerProcess;
bool log_checkpoints = false;
static TimestampTz fixture_now;

TimestampTz
GetCurrentTimestamp(void)
{
	return fixture_now += 1000;
}

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
ProcessInterrupts(void)
{}

void
pg_re_throw(void)
{
	abort();
}

/* SCN total order: local part, then node id. */
int
scn_total_cmp(SCN a, SCN b)
{
	if (scn_local(a) != scn_local(b))
		return scn_local(a) < scn_local(b) ? -1 : 1;
	if (scn_node_id(a) != scn_node_id(b))
		return scn_node_id(a) < scn_node_id(b) ? -1 : 1;
	return 0;
}

/* Page application is not part of a census. */
ClusterBlkApplyResult
cluster_block_apply_one(struct XLogReaderState *record pg_attribute_unused(),
						uint8 block_id pg_attribute_unused(), char *page pg_attribute_unused())
{
	abort();
}

/* The page owner's presence predicate (cluster_page_stable_base.c): a
 * version names its segment incarnation and a nonzero token. */
bool
rf_page_version_present_v1(const RfPageVersionV1 *version)
{
	uint8 any = 0;

	if (version == NULL)
		return false;
	for (int i = 0; i < 16; i++)
		any |= version->segment_incarnation[i];
	return any != 0 && version->mutation_token != 0;
}

bool
cluster_wal_claim_v2_ref_valid(const ClusterWalThreadClaimRefV2 *ref)
{
	return ref != NULL && ref->identity.origin_owner_incarnation != 0;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out pg_attribute_unused())
{
	abort();
}

/* No local PI responsibility: every source completes at its native redo. */
bool
cluster_pcm_local_pi_floor_v1(const ClusterWalSourceRef *source pg_attribute_unused(),
							  ClusterPcmLocalPiFloorV1 *out)
{
	memset(out, 0, sizeof(*out));
	return true;
}

ClusterControlRootResult
cluster_control_root_v3_retained_lower_publish(
	const ClusterControlRootIdentity *self pg_attribute_unused(),
	const ClusterControlRootFileToken *census_token pg_attribute_unused(),
	XLogRecPtr native_redo pg_attribute_unused(), XLogRecPtr lower pg_attribute_unused(),
	ClusterControlRootSnapshot *out pg_attribute_unused())
{
	abort();
}

/* No record here needs more history than one batch: never spilled. */
BufFile *
BufFileCreateTemp(bool interXact pg_attribute_unused())
{
	abort();
}

void
BufFileWrite(BufFile *file pg_attribute_unused(), const void *ptr pg_attribute_unused(),
			 size_t size pg_attribute_unused())
{
	abort();
}

int
BufFileSeek(BufFile *file pg_attribute_unused(), int fileno pg_attribute_unused(),
			off_t offset pg_attribute_unused(), int whence pg_attribute_unused())
{
	abort();
}

void
BufFileReadExact(BufFile *file pg_attribute_unused(), void *ptr pg_attribute_unused(),
				 size_t size pg_attribute_unused())
{
	abort();
}

void
BufFileClose(BufFile *file pg_attribute_unused())
{
	abort();
}

/* ---- the retained input scope ---- */
typedef struct RealRecord {
	XLogReaderState reader;
	uint8 data[BLCKSZ];
	uint32 source;
	/* Last: DecodedXLogRecord ends in a flexible block array. */
	union {
		DecodedXLogRecord decoded;
		char pad[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
	} u;
} RealRecord;

static ClusterWalInputV1 items[REC_MAX_ITEMS];
static uint32 nitems;
static RealRecord records[REC_MAX_RECORDS];
static uint32 nrecords;
static int scopes_open;
static ClusterWalSourceRef self_ref;
/* Database incarnation the typed drops name (the scope's, unless a case
 * names another one). */
static uint64 rec_drop_dbinc = REC_DBINC;

struct ClusterWalInputsV1 {
	int unused;
};
static ClusterWalInputsV1 the_scope;

ClusterControlRootResult
cluster_wal_inputs_begin_v1(const uint8 storage_uuid[16] pg_attribute_unused(),
							uint64 system_identifier pg_attribute_unused(),
							ClusterWalInputsV1 **out)
{
	scopes_open++;
	*out = &the_scope;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

void
cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs)
{
	if (*inputs != NULL)
		scopes_open--;
	*inputs = NULL;
}

uint32
cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs pg_attribute_unused())
{
	return nitems;
}

const ClusterWalInputV1 *
cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs pg_attribute_unused(), uint32 index)
{
	return index < nitems ? &items[index] : NULL;
}

ClusterControlRootResult
cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs pg_attribute_unused())
{
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_wal_inputs_root_token_v1(ClusterWalInputsV1 *inputs pg_attribute_unused(),
								 ClusterControlRootFileToken *out)
{
	memset(out, 0, sizeof(*out));
	out->file_txn_seq = 5;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* Visit each record of its source's complete cut, as the real scope does. */
ClusterControlRootResult
cluster_wal_inputs_census_v1(ClusterWalInputsV1 *inputs pg_attribute_unused(),
							 ClusterWalCensusVisitorV1 visitor, void *arg, uint64 *out_record_count,
							 RfPageProofDetailV1 *out_detail)
{
	*out_record_count = 0;
	*out_detail = RF_PAGE_PROOF_DETAIL_OK;
	for (uint32 i = 0; i < nrecords; i++) {
		const ClusterWalInputV1 *item = &items[records[i].source];
		RfContributorStreamCutV1 cut = { 0 };
		RfPageProofDetailV1 detail;

		cut.failed_thread = item->source.claim.identity.origin_thread_id;
		cut.origin_owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
		cut.timeline_id = item->source.timeline;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cut.scan_begin_inclusive = item->checkpoint.checkpoint_lower_lsn;
		cut.scan_end_exclusive = item->checkpoint.validated_tail_lsn_exclusive;
		detail = visitor(&records[i].reader, &item->source, &cut, arg);
		if (detail != RF_PAGE_PROOF_DETAIL_OK) {
			*out_detail = detail;
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		}
		(*out_record_count)++;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* ---- sources ---- */
static void
records_reset(void)
{
	memset(items, 0, sizeof(items));
	memset(records, 0, sizeof(records));
	nitems = nrecords = 0;
	rec_drop_dbinc = REC_DBINC;
}

/* An OPEN writer generation: history [lower, redo), obligations to tail. */
static uint32
add_writer(uint16 thread, XLogRecPtr lower, XLogRecPtr redo, XLogRecPtr tail)
{
	ClusterWalInputV1 *item = &items[nitems];

	item->kind = CLUSTER_WAL_INPUT_CHECKPOINT;
	item->current = true;
	item->source.claim.identity.system_identifier = REC_SYSID;
	memset(item->source.claim.identity.storage_uuid, 0x44, 16);
	item->source.claim.identity.origin_thread_id = thread;
	item->source.claim.identity.origin_node_id = thread - 1;
	item->source.claim.identity.origin_owner_incarnation = 40 + thread;
	item->source.claim.identity.root_lineage_seq = 1;
	item->source.claim.database_incarnation = REC_DBINC;
	item->source.claim.max_config_generation = 1;
	item->source.claim.claim_sha256[0] = (uint8)thread;
	item->source.timeline = 1;
	item->checkpoint.identity = item->source.claim.identity;
	item->checkpoint.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	item->checkpoint.checkpoint_lower_lsn = lower;
	item->checkpoint.validated_tail_lsn_exclusive = tail;
	item->native_redo = redo;
	return nitems++;
}

/* Compute the bound of one writer generation, as its own checkpointer. */
static ClusterControlRootResult
compute_for(uint32 source, ClusterWalRetainedCutV1 *cut, RfPageProofDetailV1 *detail)
{
	ClusterControlRootResult result;

	self_ref = items[source].source;
	result = cluster_wal_retained_cut_compute_v1(&self_ref, cut, detail);
	UT_ASSERT_EQ(scopes_open, 0);
	return result;
}

/* ---- records in their real layouts ---- */
static RealRecord *
record_new(uint32 source, uint8 rmid, uint8 info, TransactionId xid, XLogRecPtr read,
		   XLogRecPtr end)
{
	RealRecord *r = &records[nrecords++];

	memset(r, 0, sizeof(*r));
	r->source = source;
	r->u.decoded.header.xl_rmid = rmid;
	r->u.decoded.header.xl_info = info;
	r->u.decoded.header.xl_xid = xid;
	r->u.decoded.header.xl_crc = (pg_crc32c)(read * 31 + 7);
	r->u.decoded.max_block_id = -1;
	r->u.decoded.main_data = (char *)r->data;
	r->reader.record = &r->u.decoded;
	r->u.decoded.lsn = read;
	r->u.decoded.next_lsn = end;
	r->reader.system_identifier = REC_SYSID;
	r->reader.ReadRecPtr = read;
	r->reader.EndRecPtr = end;
	return r;
}

static void
record_append(RealRecord *r, const void *bytes, size_t length)
{
	UT_ASSERT(r->u.decoded.main_data_len + length <= sizeof(r->data));
	memcpy(r->data + r->u.decoded.main_data_len, bytes, length);
	r->u.decoded.main_data_len += (uint32)length;
}

static RelFileLocator
rec_locator(RelFileNumber relnumber)
{
	RelFileLocator locator = { DEFAULTTABLESPACE_OID, 5, relnumber };

	return locator;
}

/* The typed DROP of a live relation, as the COMMIT that drops it carries. */
static ClusterSpaceStructureChange
rec_tombstone(RelFileNumber relnumber)
{
	ClusterSpaceStructureChange drop = { 0 };
	ClusterSpaceIdentity live = { 0 };

	live.key.system_identifier = REC_SYSID;
	live.key.database_incarnation = rec_drop_dbinc;
	memset(live.key.storage_uuid, 0x44, 16);
	live.key.locator = rec_locator(relnumber);
	memset(live.incarnation, 0x17, 16);
	live.sequence = live.operation = 1;
	live.state = CLUSTER_SPACE_IDENTITY_LIVE;
	drop.identity.action = CLUSTER_SPACE_WAL_TOMBSTONE;
	drop.identity.nblocks = InvalidBlockNumber;
	drop.identity.expected = live;
	drop.identity.result = live;
	drop.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	drop.identity.result.sequence++;
	drop.identity.result.operation++;
	drop.identity.before_token = 123;
	drop.identity.result_token = 170;
	drop.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	drop.reservation.before.identity = live;
	drop.reservation.before.next_block = 7;
	drop.reservation.result = drop.reservation.before;
	drop.reservation.result.identity = drop.identity.result;
	drop.reservation.before_token = 100;
	drop.reservation.result_token = 170;
	return drop;
}

/*
 * XLOG_XACT_COMMIT of a transaction of the source's thread: its TT commit
 * delta, optional dropped relations (with their typed tombstones when
 * tombstones is true) and optional invalidation messages, in the order
 * ParseCommitRecord reads them.
 */
static RealRecord *
rec_commit(uint32 source, XLogRecPtr read, XLogRecPtr end, TransactionId xid,
		   const RelFileNumber *drops, int ndrops, bool tombstones, int nmsgs)
{
	uint16 thread = items[source].source.claim.identity.origin_thread_id;
	RealRecord *r
		= record_new(source, RM_XACT_ID, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, xid, read, end);
	xl_xact_commit commit = { 0 };
	xl_xact_xinfo xinfo = { 0 };
	xl_xact_scn scn = { 0 };
	xl_xact_tt_commit tt = { 0 };

	commit.xact_time = 123456;
	xinfo.xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT;
	if (ndrops > 0)
		xinfo.xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
	if (ndrops > 0 && tombstones)
		xinfo.xinfo |= XACT_XINFO_HAS_SPACE_DROP;
	if (nmsgs > 0)
		xinfo.xinfo |= XACT_XINFO_HAS_INVALS;
	record_append(r, &commit, sizeof(commit));
	record_append(r, &xinfo, sizeof(xinfo));
	if (ndrops > 0) {
		record_append(r, &ndrops, sizeof(ndrops));
		for (int i = 0; i < ndrops; i++) {
			RelFileLocator locator = rec_locator(drops[i]);

			record_append(r, &locator, sizeof(locator));
		}
	}
	if (nmsgs > 0) {
		SharedInvalidationMessage msg;

		memset(&msg, 0, sizeof(msg));
		msg.rc.id = SHAREDINVALRELCACHE_ID;
		msg.rc.dbId = 5;
		msg.rc.relId = 16384;
		record_append(r, &nmsgs, sizeof(nmsgs));
		for (int i = 0; i < nmsgs; i++)
			record_append(r, &msg, sizeof(msg));
	}
	scn.scn = scn_encode(thread - 1, 900 + read);
	tt.instance = thread;
	tt.segment_id = (uint32)(thread - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	tt.segment_generation = 11;
	tt.slot_offset = 4;
	tt.wrap = 7;
	tt.xid = xid;
	tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
	tt.commit_scn = scn.scn;
	record_append(r, &scn, sizeof(scn));
	record_append(r, &tt, sizeof(tt));
	if (ndrops > 0 && tombstones) {
		uint32 count = (uint32)ndrops;

		record_append(r, &count, sizeof(count));
		for (int i = 0; i < ndrops; i++) {
			ClusterSpaceStructureChange drop = rec_tombstone(drops[i]);
			uint8 bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

			UT_ASSERT(cluster_space_structure_wal_encode(&drop, bytes, sizeof(bytes)));
			record_append(r, bytes, sizeof(bytes));
		}
	}
	return r;
}

/* XLOG_UNDO_TT_SLOT_COMMIT: the durable commit stamp of one TT slot of the
 * source's first undo segment. */
static RealRecord *
rec_tt_commit(uint32 source, XLogRecPtr read, XLogRecPtr end, TransactionId xid, uint16 slot)
{
	uint16 thread = items[source].source.claim.identity.origin_thread_id;
	RealRecord *r
		= record_new(source, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT, xid, read, end);
	xl_undo_tt_slot_commit rec = { 0 };

	rec.segment_id = (uint32)(thread - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	rec.slot_offset = slot;
	rec.wrap = 7;
	rec.xid = xid;
	rec.instance = (uint8)thread;
	rec.commit_scn = scn_encode(thread - 1, 900 + read);
	record_append(r, &rec, sizeof(rec));
	return r;
}

/* XLOG_XACT_COMMIT_PREPARED of twophase_xid, without the GID and TT
 * bindings the typed decoder requires: classified natively. */
static RealRecord *
rec_commit_prepared(uint32 source, XLogRecPtr read, XLogRecPtr end, TransactionId twophase_xid)
{
	uint16 thread = items[source].source.claim.identity.origin_thread_id;
	RealRecord *r = record_new(source, RM_XACT_ID, XLOG_XACT_COMMIT_PREPARED | XLOG_XACT_HAS_INFO,
							   InvalidTransactionId, read, end);
	xl_xact_commit commit = { 0 };
	xl_xact_xinfo xinfo = { 0 };
	xl_xact_twophase twophase = { 0 };
	xl_xact_scn scn = { 0 };

	commit.xact_time = 123456;
	xinfo.xinfo = XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_SCN;
	twophase.xid = twophase_xid;
	scn.scn = scn_encode(thread - 1, 900 + read);
	record_append(r, &commit, sizeof(commit));
	record_append(r, &xinfo, sizeof(xinfo));
	record_append(r, &twophase, sizeof(twophase));
	record_append(r, &scn, sizeof(scn));
	return r;
}

/* XLOG_XACT_ABORT with the relations it created and now drops. */
static RealRecord *
rec_abort(uint32 source, XLogRecPtr read, XLogRecPtr end, TransactionId xid,
		  const RelFileNumber *rels, int nrels)
{
	uint16 thread = items[source].source.claim.identity.origin_thread_id;
	RealRecord *r
		= record_new(source, RM_XACT_ID, XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO, xid, read, end);
	xl_xact_abort abort_record = { 0 };
	xl_xact_xinfo xinfo = { 0 };
	xl_xact_scn scn = { 0 };

	abort_record.xact_time = 123456;
	xinfo.xinfo = XACT_XINFO_HAS_SCN | (nrels > 0 ? XACT_XINFO_HAS_RELFILELOCATORS : 0);
	record_append(r, &abort_record, sizeof(abort_record));
	record_append(r, &xinfo, sizeof(xinfo));
	if (nrels > 0) {
		record_append(r, &nrels, sizeof(nrels));
		for (int i = 0; i < nrels; i++) {
			RelFileLocator locator = rec_locator(rels[i]);

			record_append(r, &locator, sizeof(locator));
		}
	}
	scn.scn = scn_encode(thread - 1, 900 + read);
	record_append(r, &scn, sizeof(scn));
	return r;
}

/* Native XLOG_SMGR_CREATE / XLOG_SMGR_TRUNCATE of a main fork. */
static RealRecord *
rec_smgr(uint32 source, XLogRecPtr read, XLogRecPtr end, bool truncate, RelFileNumber relnumber)
{
	RealRecord *r = record_new(source, RM_SMGR_ID, truncate ? XLOG_SMGR_TRUNCATE : XLOG_SMGR_CREATE,
							   InvalidTransactionId, read, end);

	if (truncate) {
		xl_smgr_truncate xlrec = { 0 };

		xlrec.blkno = 2;
		xlrec.rlocator = rec_locator(relnumber);
		xlrec.flags = SMGR_TRUNCATE_ALL;
		record_append(r, &xlrec, sizeof(xlrec));
	} else {
		xl_smgr_create xlrec = { 0 };

		xlrec.rlocator = rec_locator(relnumber);
		xlrec.forkNum = MAIN_FORKNUM;
		record_append(r, &xlrec, sizeof(xlrec));
	}
	return r;
}

#endif /* TEST_CLUSTER_WAL_RETAINED_CUT_RECORDS_BOUNDARY_H */
