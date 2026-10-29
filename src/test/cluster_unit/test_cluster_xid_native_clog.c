/*-------------------------------------------------------------------------
 * test_cluster_xid_native_clog.c
 *     Native FULL CLOG initialization for the original allocator.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_native_clog.c
 * NOTES
 *     Original allocator and CLOG/SLRU bodies execute. Locks, lease and WAL
 *     insertion are controlled; no lifetime or completion authority.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_xid_native_clog_namespace.h"
int d3_xid_native_clog_main(void);
#define TEST_CLUSTER_EPISODE_CLOG
#include "test_cluster_clog_recovery.c"
#include "test_cluster_xid_native_clog.h"

static PGPROC native_proc;
PGPROC *MyProc = &native_proc;
static unsigned native_calls;

/* This write-only fixture does not lend authority to unused observers. */
bool cluster_wal_inputs_relmap_source_current_v1(const ClusterRecoveryRelmapSourceV1 *s)
{ (void)s; return false; }
void cluster_wal_inputs_relmap_source_fail_v1(const ClusterRecoveryRelmapSourceV1 *s)
{ (void)s; }
bool cluster_wal_inputs_relmap_source_revalidate_v1(const ClusterRecoveryRelmapSourceV1 *s)
{ (void)s; return false; }

void
test_xid_native_clog_start(bool fail)
{
	const char *dirs[] = {"native_side", "native_side/origin_0", "native_side/origin_0/pg_xact",
		"native_side/origin_1", "native_side/origin_1/pg_xact"};
	char path[MAXPGPATH];
	UT_ASSERT_EQ(allocation_count | fixture_lock_count, 0);
	UT_ASSERT(getcwd(fixture_cwd, sizeof(fixture_cwd)) != NULL);
	strlcpy(fixture_root, "/tmp/pgrac-xid-native-clog.XXXXXX", sizeof(fixture_root));
	UT_ASSERT(mkdtemp(fixture_root) != NULL);
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", fixture_root, dirs[i]);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	cluster_shared_data_dir = fixture_root;
	cluster_shared_config = enableFsync = true;
	cluster_node_id = fixture_origin = 0;
	MyBackendType = B_BACKEND; MyAuxProcType = NotAnAuxProcess;
	InRecovery = authority = c0_authority = relmap_base_interrupts = false;
	InterruptHoldoffCount = CritSectionCount = 0;
	expect_error = fail;
	native_calls = 0;
	memset(&birth_wal, 0, sizeof(birth_wal));
	birth_wal.mode = LW_EXCLUSIVE; birth_wal.fail = fail;
	memset(ClogRecoveryCtl, 0, sizeof(ClogRecoveryCtl));
	memset(XactCtl, 0, sizeof(*XactCtl));
	clog_recovery_shmem_init();
	MainLWLockArray = own_main_locks;
	LWLockInitialize(XactSLRULock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactTruncationLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XidGenLock, LWTRANCHE_XACT_BUFFER);
	SimpleLruInit(XactCtl, "Xact native allocator fixture", 4, CLOG_LSNS_PER_PAGE,
		XactSLRULock, "pg_xact", LWTRANCHE_XACT_BUFFER, SYNC_HANDLER_NONE);
	XactCtl->PagePrecedes = CLOGPagePrecedes;
	UT_ASSERT_EQ(chdir(fixture_root), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_0/pg_xact", fixture_root);
	UT_ASSERT_EQ(symlink(path, "pg_xact"), 0);
}

bool
test_xid_native_clog_extend(FullTransactionId candidate)
{
	volatile bool ok = true;
	++native_calls;
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	PG_TRY(); {
		ExtendCLOGFull(candidate);
	} PG_CATCH(); { ok = false; } PG_END_TRY();
	if (ok) LWLockRelease(XidGenLock);
	return ok;
}

void
test_xid_native_clog_state(XidNativeClogState *out)
{
	memset(out, 0, sizeof(*out));
	out->calls = native_calls; out->begins = birth_wal.begins; out->inserts = birth_wal.inserts;
	out->bytes = birth_wal.bytes; out->length = birth_wal.length; out->info = birth_wal.info;
	out->xid_held = LWLockHeldByMe(XidGenLock); out->slru_held = LWLockHeldByMe(XactSLRULock);
	out->page_zero = true;
	for (int i = 0; i < XactCtl->shared->num_slots; ++i) {
		if (!XactCtl->shared->page_dirty[i]) continue;
		++out->dirty_pages;
		for (unsigned b = 0; b < BLCKSZ; ++b)
			if (XactCtl->shared->page_buffer[i][b] != 0) out->page_zero = false;
	}
}

/* Replay the captured producer bytes, never a reconstructed expected record.
 * Independent file reads observe bytes only; SYNC_HANDLER_NONE grants no
 * fsync durability, FullXid era or recovery-completion authority. */
void
test_xid_native_clog_replay(XidNativeClogReplayFault fault,
							  XidNativeClogReplayState *out)
{
	XLogReaderState reader = {0};
	DecodedXLogRecord decoded = {0};
	xl_clog_zeropage_full image = birth_wal.bytes;
	PGAlignedBlock page;
	char path[MAXPGPATH];
	struct stat st;
	int target = birth_wal.bytes.pageno;
	int neighbor = target ^ 1;
	int fd;
	unsigned before_writes = writes, before_inserts = birth_wal.inserts;
	volatile bool caught = false;

	memset(out, 0, sizeof(*out));
	if (target < 0 || target > 1)
		return;
	fixture_path(target, path);
	out->file_existed = stat(path, &st) == 0;
	out->io_ok = !out->file_existed && errno == ENOENT;
	memset(page.data, 0xa5, BLCKSZ);
	fixture_page(target, page.data);
	memset(page.data, 0x5a, BLCKSZ);
	fixture_page(neighbor, page.data);
	reader.record = &decoded;
	decoded.main_data = (char *)&image;
	decoded.main_data_len = birth_wal.length;
	decoded.max_block_id = -1;
	decoded.header.xl_info = birth_wal.info;
	decoded.header.xl_rmid = RM_CLOG_ID;
	if (fault == XID_CLOG_REPLAY_RESERVED) image.reserved_zero = 1;
	if (fault == XID_CLOG_REPLAY_PAGE) image.pageno = target + 1;
	if (fault == XID_CLOG_REPLAY_SHORT) --decoded.main_data_len;
	if (fault == XID_CLOG_REPLAY_BLOCKREF) decoded.max_block_id = 0;
	expect_error = InRecovery = true;
	PG_TRY(); {
		clog_redo(&reader);
	} PG_CATCH(); { caught = true; } PG_END_TRY();
	expect_error = InRecovery = false;
	out->caught = caught;
	out->writes = writes - before_writes;
	out->inserts = birth_wal.inserts - before_inserts;
	fd = open(path, O_RDONLY);
	if (fd < 0) { out->io_ok = false; return; }
	if (fstat(fd, &st) != 0) out->io_ok = false;
	else out->file_size = st.st_size;
	if (pread(fd, page.data, BLCKSZ, (off_t)target * BLCKSZ) == BLCKSZ) {
		out->target_zero = out->target_sentinel = true;
		for (unsigned b = 0; b < BLCKSZ; ++b) {
			out->target_zero &= (unsigned char)page.data[b] == 0;
			out->target_sentinel &= (unsigned char)page.data[b] == 0xa5;
		}
	} else out->io_ok = false;
	if (pread(fd, page.data, BLCKSZ, (off_t)neighbor * BLCKSZ) == BLCKSZ) {
		out->neighbor_sentinel = true;
		for (unsigned b = 0; b < BLCKSZ; ++b)
			out->neighbor_sentinel &= (unsigned char)page.data[b] == 0x5a;
	} else out->io_ok = false;
	if (close(fd) != 0) out->io_ok = false;
}

void
test_xid_native_clog_stop(void)
{
	/* Original ERROR caller releases locks; no retry, flush or rollback. */
	if (LWLockHeldByMe(XactSLRULock)) LWLockRelease(XactSLRULock);
	if (LWLockHeldByMe(XidGenLock)) LWLockRelease(XidGenLock);
	expect_error = birth_wal.fail = false;
	fixture_idle();
	fixture_cleanup();
	allocation_count = fixture_lock_count = 0;
	fixture_shared_mutation = NULL; fixture_shared_mutation_size = 0;
	ClogRelmapMutations = NULL; ClogRecoveryLocks = NULL;
	memset(ClogRecoveryCtl, 0, sizeof(ClogRecoveryCtl));
	memset(XactCtl, 0, sizeof(*XactCtl));
	MainLWLockArray = NULL; cluster_shared_data_dir = NULL;
}
