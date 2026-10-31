/* Author: SqlRush <sqlrush@gmail.com> */
/* Original Startup scanner + durable writer + block0 owner. Only native
 * process state, WAL insertion and smgr are controlled boundaries. */
int block0_fixture_main(void);
#define main block0_fixture_main
#define cluster_undo_smgr_read_block startup_base_read
#define cluster_undo_smgr_write_block startup_base_write
#define XLogFlush startup_base_flush
#define errfinish startup_base_errfinish
#include "test_cluster_undo_block0.c"
#undef errfinish
#undef XLogFlush
#undef cluster_undo_smgr_write_block
#undef cluster_undo_smgr_read_block
#undef main

#include "access/transam.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_recovery.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "storage/proc.h"
#include "storage/procarray.h"

/* The included harness renamed these original declarations as providers. */
extern void errfinish(const char *, int, const char *) pg_attribute_noreturn();
extern void XLogFlush(XLogRecPtr);
extern bool cluster_undo_smgr_read_block(ClusterUndoPathIntent, uint32, uint8, uint32, char *);
extern bool cluster_undo_smgr_write_block(ClusterUndoPathIntent, uint32, uint8, uint32,
										const char *, bool);

bool cluster_enabled = true;
bool cluster_shared_config = false;
bool cluster_tt_recovery_resolve_active = true;
bool IsPostmasterEnvironment = false;
static PGPROC startup_proc;
PGPROC *MyProc = &startup_proc;
static bool startup_recovery, startup_hot;
static int LocalXLogInsertAllowed;
static uint32 startup_segment;
static unsigned startup_v1_wal, startup_v2_wal, startup_resolved, startup_raw_writes;
static xl_undo_tt_identity_v2 startup_wal[TT_SLOTS_PER_SEGMENT];
static unsigned startup_drift;
static bool startup_emit_error;
static int startup_error_code;

/* No shared cold scope exists in this local Startup fixture. */
ClusterUndoPathIntent
cluster_undo_recovery_intent_for_owner(uint8 owner)
{
	return cluster_undo_intent_for_owner(owner);
}

bool
RecoveryInProgress(void)
{
	return startup_recovery;
}

bool
HotStandbyActive(void)
{
	return startup_hot;
}

bool
TransactionIdDidCommit(TransactionId xid)
{
	return xid == 101;
}

bool
TransactionIdIsInProgress(TransactionId xid)
{
	return xid == 102;
}

int
errcode(int code)
{
	startup_error_code = code;
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	pg_re_throw();
}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent, uint32 segment, uint8 owner,
							 uint32 block, char *out)
{
	if (segment != startup_segment)
		return false;
	UT_ASSERT_EQ(owner, (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);
	return startup_base_read(intent, segment, owner, block, out);
}

bool
cluster_undo_smgr_write_block(ClusterUndoPathIntent intent, uint32 segment, uint8 owner,
							  uint32 block, const char *in, bool sync)
{
	bool result;

	UT_ASSERT_EQ(segment, startup_segment);
	UT_ASSERT_EQ(owner, (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);
	UT_ASSERT_EQ(xlog_last_flush_lsn, 100 + startup_v2_wal);
	result = startup_base_write(intent, segment, owner, block, in, sync);
	if (result)
		memcpy(smgr_image, in, BLCKSZ);
	return result;
}

void
XLogFlush(XLogRecPtr lsn)
{
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)smgr_image;

	startup_base_flush(lsn);
	if (startup_drift == 1)
		header->wrap_count++;
	else if (startup_drift == 2)
		header->tt_slots[0].original_full_xid += UINT64_C(1) << 32;
	else if (startup_drift == 3)
		cluster_shared_config = true;
	startup_drift = 0;
}

bool
cluster_undo_smgr_read_header_bytes(ClusterUndoPathIntent intent pg_attribute_unused(),
		uint32 segment, uint8 owner pg_attribute_unused(), uint32 offset, char *out, uint32 len)
{
	if (segment != startup_segment || offset + len > BLCKSZ)
		return false;
	memcpy(out, smgr_image + offset, len);
	return true;
}

bool
cluster_undo_smgr_write_header_bytes(ClusterUndoPathIntent intent pg_attribute_unused(),
		uint32 segment, uint8 owner pg_attribute_unused(), uint32 offset,
		const char *in, uint32 len)
{
	if (segment != startup_segment || offset + len > BLCKSZ)
		return false;
	startup_raw_writes++;
	memcpy(smgr_image + offset, in, len);
	return true;
}

XLogRecPtr
cluster_undo_emit_tt_slot_abort(uint8 owner pg_attribute_unused(), uint32 segment pg_attribute_unused(),
		uint16 offset pg_attribute_unused(), uint16 wrap pg_attribute_unused(),
		TransactionId xid pg_attribute_unused())
{
	startup_v1_wal++;
	return 100;
}

XLogRecPtr
cluster_undo_emit_tt_slot_abort_v2(const xl_undo_tt_identity_v2 *record)
{
	UT_ASSERT(startup_v2_wal < TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, DELAY_CHKPT_START);
	startup_wal[startup_v2_wal++] = *record;
	if (startup_emit_error)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("injected WAL error")));
	return 100 + startup_v2_wal;
}

void cluster_tt_durable_io_wait_start(void) {}
void cluster_tt_durable_io_wait_end(void) {}
void cluster_tt_recovery_count_active_resolved_aborted(void) { startup_resolved++; }

/* Complete private original Startup owner, extracted from xlog.c. */
#include "test_cluster_tt_startup_local_owner.inc"

static void
startup_seed(bool v2)
{
	UndoSegmentHeaderDataV2 *header;

	fresh_block0_region(1);
	cluster_enabled = cluster_tt_recovery_resolve_active = true;
	cluster_shared_config = startup_recovery = startup_hot = false;
	cluster_node_id = 0;
	MyAuxProcType = StartupProcess;
	MyBackendType = B_STARTUP;
	IsUnderPostmaster = true;
	IsPostmasterEnvironment = true;
	LocalXLogInsertAllowed = 1;
	MyProc->delayChkptFlags = 0;
	startup_segment = 1;
	startup_v1_wal = startup_v2_wal = startup_resolved = startup_raw_writes = 0;
	startup_drift = 0;
	startup_emit_error = false;
	startup_error_code = 0;
	if (!v2) {
		cluster_undo_segment_make_header_bytes(1, 1, smgr_image);
		((UndoSegmentHeaderData *)smgr_image)->tt_slots[0]
			= (TTSlot){ .xid = 100, .wrap = 2, .status = TT_SLOT_ACTIVE };
		return;
	}
	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(1, 1, smgr_image));
	header = (UndoSegmentHeaderDataV2 *)smgr_image;
	header->wrap_count = 4;
	for (uint16 i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
		header->tt_slots[i] = (TTSlotV2){ .xid = 100 + i, .wrap = 2,
			.status = i == 0 || i == 1 || i == 2 || i == 47 ? TT_SLOT_ACTIVE : TT_SLOT_COMMITTED,
			.commit_scn = i == 0 || i == 1 || i == 2 || i == 47 ? InvalidScn : 77,
			.original_full_xid = ((uint64)(i % 2) << 32) | (100 + i) };
	UT_ASSERT(cluster_undo_segment_header_seal(smgr_image));
}

static bool
startup_attempt(bool direct)
{
	volatile bool error = false;

	PG_TRY();
	{
		if (direct)
			(void)cluster_tt_recovery_resolve_active_slots();
		else
			(void)ClusterTTResolveActiveLocalStartup();
	}
	PG_CATCH();
	{
		error = true;
	}
	PG_END_TRY();
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(lwlock_release_without_holdoff_count, 0);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
	UT_ASSERT(!cluster_undo_block0_backend_has_resources());
	return error;
}

UT_TEST(test_startup_tt40_exact_wal_before_whole_page_and_checksum)
{
	PGAlignedBlock expected;
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)expected.data;

	startup_seed(true);
	memcpy(expected.data, smgr_image, BLCKSZ);
	for (uint16 i = 0; i < TT_SLOTS_PER_SEGMENT; i += 47) {
		header->tt_slots[i].status = TT_SLOT_ABORTED;
		header->tt_slots[i].flags = TT_FLAGS_RESERVED;
		header->tt_slots[i].commit_scn = InvalidScn;
		header->tt_slots[i].first_undo_block = (UBA)InvalidUba_init;
	}
	UT_ASSERT(cluster_undo_segment_header_seal(expected.data));
	UT_ASSERT(!startup_attempt(false));
	UT_ASSERT_EQ(startup_resolved, 2);
	UT_ASSERT_EQ(startup_v2_wal, 2);
	UT_ASSERT_EQ(startup_v1_wal, 0);
	UT_ASSERT_EQ(startup_raw_writes, 0);
	UT_ASSERT_EQ(smgr_write_calls, 2);
	UT_ASSERT(smgr_last_do_fsync);
	UT_ASSERT(memcmp(expected.data, smgr_image, BLCKSZ) == 0);
	UT_ASSERT_EQ(cluster_undo_segment_header_integrity(smgr_image), CLUSTER_UNDO_HEADER_VALID);
	for (unsigned i = 0; i < 2; i++) {
		uint16 offset = i == 0 ? 0 : 47;
		UT_ASSERT_EQ(startup_wal[i].slot_offset, offset);
		UT_ASSERT_EQ(startup_wal[i].segment_generation, 4);
		UT_ASSERT_EQ(startup_wal[i].original_full_xid, header->tt_slots[offset].original_full_xid);
		UT_ASSERT_EQ(startup_wal[i].format_version, CLUSTER_UNDO_TT_IDENTITY_V2_VERSION);
	}
}

UT_TEST(test_startup_v1_keeps_original_writer)
{
	startup_seed(false);
	UT_ASSERT(!startup_attempt(false));
	UT_ASSERT_EQ(startup_resolved, 1);
	UT_ASSERT_EQ(startup_v1_wal, 1);
	UT_ASSERT_EQ(startup_v2_wal, 0);
	UT_ASSERT_EQ(startup_raw_writes, 1);
}

UT_TEST(test_startup_disabled_cluster_preserves_native_startup)
{
	startup_seed(true);
	cluster_enabled = false;
	cluster_node_id = -1;
	MyAuxProcType = NotAnAuxProcess;
	UT_ASSERT(!startup_attempt(false));
	UT_ASSERT_EQ(startup_v2_wal + startup_v1_wal + smgr_read_calls, 0);
}

UT_TEST(test_startup_native_single_user_retains_exclusive_owner)
{
	startup_seed(true);
	MyAuxProcType = NotAnAuxProcess;
	MyBackendType = B_STANDALONE_BACKEND;
	IsUnderPostmaster = IsPostmasterEnvironment = false;
	UT_ASSERT(!startup_attempt(false));
	UT_ASSERT_EQ(startup_v2_wal, 2);
	UT_ASSERT_EQ(startup_resolved, 2);
}

UT_TEST(test_startup_tt40_resident_state_refuses_before_wal)
{
	ClusterUndoBlock0LogicalKey logical = make_key(1, 1);
	ClusterUndoBlock0ResolvedRoot root = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 3);
	ClusterUndoBlock0AuthorityProof proof = make_live_proof(1, 5);
	ClusterUndoBlock0FrameToken token = {0};
	ClusterUndoBlock0Pin pin;
	char *page;
	PGAlignedBlock before;

	startup_seed(true);
	memcpy(before.data, smgr_image, BLCKSZ);
	UT_ASSERT_EQ(cluster_undo_block0_frame_reserve_batch(1, &token), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(cluster_undo_block0_admit_runtime(&logical, &root, &proof, &token, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_OK);
	cluster_undo_block0_unpin(&pin);
	UT_ASSERT(startup_attempt(false));
	UT_ASSERT_EQ(startup_v2_wal + startup_v1_wal, 0);
	UT_ASSERT_EQ(smgr_write_calls + startup_raw_writes, 0);
	UT_ASSERT(memcmp(before.data, smgr_image, BLCKSZ) == 0);
}

UT_TEST(test_startup_scope_shared_connectable_and_nonstartup_refuse)
{
	for (unsigned mode = 0; mode < 6; mode++) {
		PGAlignedBlock before;

		startup_seed(true);
		if (mode == 0) cluster_shared_config = true;
		if (mode == 1) startup_hot = true;
		if (mode == 2) MyAuxProcType = NotAnAuxProcess;
		if (mode == 3) startup_recovery = true;
		if (mode == 4) LocalXLogInsertAllowed = 0;
		if (mode == 5) cluster_node_id = -1;
		memcpy(before.data, smgr_image, BLCKSZ);
		UT_ASSERT(startup_attempt(false));
		UT_ASSERT_EQ(startup_v1_wal + startup_v2_wal, 0);
		UT_ASSERT_EQ(smgr_write_calls + startup_raw_writes, 0);
		UT_ASSERT(memcmp(before.data, smgr_image, BLCKSZ) == 0);
	}
}

UT_TEST(test_startup_tt40_requires_original_lexical_scope)
{
	startup_seed(true);
	UT_ASSERT(startup_attempt(true));
	UT_ASSERT_EQ(startup_v1_wal + startup_v2_wal, 0);
	UT_ASSERT_EQ(smgr_write_calls + startup_raw_writes, 0);
}

UT_TEST(test_startup_tt40_bad_base_refuses_before_wal)
{
	for (unsigned mode = 0; mode < 5; mode++) {
		UndoSegmentHeaderDataV2 *header;

		startup_seed(true);
		header = (UndoSegmentHeaderDataV2 *)smgr_image;
		if (mode == 0) header->wrap_count = UINT32_MAX;
		if (mode == 1) header->tt_slots[47].original_full_xid++;
		if (mode == 2) header->owner_instance = 2;
		if (mode == 3) header->tt_slots[47].original_full_xid = UINT64_MAX;
		if (mode != 4) UT_ASSERT(cluster_undo_segment_header_seal(smgr_image));
		else smgr_image[BLCKSZ - 1] ^= 1;
		UT_ASSERT(startup_attempt(false));
		UT_ASSERT_EQ(startup_v1_wal + startup_v2_wal, 0);
		UT_ASSERT_EQ(smgr_write_calls + startup_raw_writes, 0);
	}
}

UT_TEST(test_startup_tt40_post_wal_identity_and_scope_drift_preserves_debt)
{
	for (unsigned mode = 1; mode < 4; mode++) {
		startup_seed(true);
		startup_drift = mode;
		UT_ASSERT(startup_attempt(false));
		UT_ASSERT_EQ(startup_v2_wal, 1);
		UT_ASSERT_EQ(smgr_write_calls + startup_raw_writes, 0);
	}
}

UT_TEST(test_startup_tt40_wal_or_write_error_releases_original_owner)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		PGAlignedBlock before;

		startup_seed(true);
		startup_emit_error = mode == 0;
		smgr_write_ok = mode == 0;
		memcpy(before.data, smgr_image, BLCKSZ);
		UT_ASSERT(startup_attempt(false));
		UT_ASSERT_EQ(startup_v2_wal, 1);
		UT_ASSERT(memcmp(before.data, smgr_image, BLCKSZ) == 0);
		startup_emit_error = false;
		smgr_write_ok = true;
		/* A new lexical scope can retry; no stale private guard survives. */
		UT_ASSERT(!startup_attempt(false));
		UT_ASSERT_EQ(startup_resolved, 2);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_startup_tt40_exact_wal_before_whole_page_and_checksum);
	UT_RUN(test_startup_v1_keeps_original_writer);
	UT_RUN(test_startup_scope_shared_connectable_and_nonstartup_refuse);
	UT_RUN(test_startup_tt40_requires_original_lexical_scope);
	UT_RUN(test_startup_tt40_bad_base_refuses_before_wal);
	UT_RUN(test_startup_tt40_post_wal_identity_and_scope_drift_preserves_debt);
	UT_RUN(test_startup_tt40_wal_or_write_error_releases_original_owner);
	UT_RUN(test_startup_disabled_cluster_preserves_native_startup);
	UT_RUN(test_startup_tt40_resident_state_refuses_before_wal);
	UT_RUN(test_startup_native_single_user_retains_exclusive_owner);
	UT_DONE();
	return ut_failed_count != 0;
}
