/*-------------------------------------------------------------------------
 *
 * cluster_wal_thread.c
 *	  pgrac per-thread WAL routing: thread identity, claim file, startup
 *	  validation (spec-4.1).
 *
 *	  This module activates the spec-1.19 xlp_thread_id placeholder.
 *	  The write hot path gets exactly one entry point,
 *	  cluster_wal_thread_stamp(), called by AdvanceXLInsertBuffer per
 *	  new WAL page; everything else runs once at postmaster startup.
 *
 *	  Layout contract (spec-4.1 Q1-A): the WAL stream is relocated to
 *	  <cluster.wal_threads_dir>/thread_<id>/ by bootstrap tooling
 *	  (pgrac-init --wal-threads-dir, which passes initdb -X), NOT by
 *	  engine-side path rewriting.  The engine validates the routing at
 *	  startup fail-closed (53RA0 / 53RA1) and never falls back to the
 *	  flat layout silently.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_wal_thread.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.1-per-thread-wal-routing.md FROZEN v1.0
 *	  Design: docs/wal-record-format-design.md §4.2, feature-034, AD-009
 *
 *	  Shmem region "pgrac wal thread" exists so the dump accessors work
 *	  under EXEC_BACKEND too (children do not inherit postmaster globals
 *	  there). Restart input is written once by the postmaster. In PRE2 the
 *	  distinct writer is published once by the qualified startup executor.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#include <fcntl.h>

#include "access/xlog_internal.h" /* XLOGDIR */
#include "access/transam.h"
#include "catalog/catversion.h"
#include "common/controldata_utils.h"
#include "common/pgrac_initdb_wal.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_state.h" /* spec-4.2 ensure() */
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster_control_bootstrap_private.h"
#include "cluster_control_root_private.h"
#include "miscadmin.h" /* IsUnderPostmaster, DataDir */
#include "port/atomics.h"
#include "storage/fd.h" /* BasicOpenFile, pg_fsync */
#include "storage/bufpage.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/timestamp.h" /* GetCurrentTimestamp */
#include "utils/wait_event.h"

/* ----------------------------------------------------------------
 * Shmem region (L206 five-step registration; "pgrac wal thread")
 * ----------------------------------------------------------------
 */
typedef struct ClusterWalThreadShmemData {
	pg_atomic_uint64 page_stamp_count; /* real-id stamps since startup */

	/*
	 * Routing facts and restart input are immutable after postmaster setup.
	 * The writer reference is separate: 0=unpublished, 1=copying, 2=ready.
	 * Only the actual initializer publishes it, once, after root INSTALL.
	 */
	uint16 thread_id;	  /* cluster_wal_thread_id() at startup */
	uint8 dir_configured; /* cluster.wal_threads_dir != '' */
	uint8 dir_validated;  /* routing validation passed */
	uint8 claim_created;  /* this boot created the claim file */
	uint8 _pad[3];
	ClusterWalSourceRef v2_ref;
	pg_atomic_uint32 writer_ref_state;
	ClusterWalSourceRef restart_ref;
	bool restart_ref_valid;

	/* spec-4.2 D5: WAL-state registry refresh-failure counter (bumped by
	 * cluster_stats on best-effort refresh failures, read by the dump
	 * SRF in any backend). */
	pg_atomic_uint64 wal_state_refresh_fail_count;
	slock_t checkpoint_sample_lock;
	ClusterWalThreadCheckpointSampleV1 checkpoint_sample;
	char _reserved[8]; /* remaining headroom */
} ClusterWalThreadShmemData;

static ClusterWalThreadShmemData *cluster_wal_thread_shmem = NULL;

static Size
cluster_wal_thread_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterWalThreadShmemData));
}

static void
cluster_wal_thread_shmem_init(void)
{
	bool found;

	cluster_wal_thread_shmem = (ClusterWalThreadShmemData *)ShmemInitStruct(
		"pgrac wal thread", cluster_wal_thread_shmem_size(), &found);
	if (!found) {
		pg_atomic_init_u64(&cluster_wal_thread_shmem->page_stamp_count, 0);
		cluster_wal_thread_shmem->thread_id = XLP_THREAD_ID_LEGACY;
		cluster_wal_thread_shmem->dir_configured = 0;
		cluster_wal_thread_shmem->dir_validated = 0;
		cluster_wal_thread_shmem->claim_created = 0;
		memset(cluster_wal_thread_shmem->_pad, 0, sizeof(cluster_wal_thread_shmem->_pad));
		memset(&cluster_wal_thread_shmem->v2_ref, 0, sizeof(cluster_wal_thread_shmem->v2_ref));
		pg_atomic_init_u32(&cluster_wal_thread_shmem->writer_ref_state, 0);
		memset(&cluster_wal_thread_shmem->restart_ref, 0,
			   sizeof(cluster_wal_thread_shmem->restart_ref));
		cluster_wal_thread_shmem->restart_ref_valid = false;
		pg_atomic_init_u64(&cluster_wal_thread_shmem->wal_state_refresh_fail_count, 0);
		SpinLockInit(&cluster_wal_thread_shmem->checkpoint_sample_lock);
		memset(&cluster_wal_thread_shmem->checkpoint_sample, 0,
			   sizeof(cluster_wal_thread_shmem->checkpoint_sample));
		memset(cluster_wal_thread_shmem->_reserved, 0, sizeof(cluster_wal_thread_shmem->_reserved));
	}
}

static const ClusterShmemRegion cluster_wal_thread_region = {
	.name = "pgrac wal thread",
	.size_fn = cluster_wal_thread_shmem_size,
	.init_fn = cluster_wal_thread_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_wal_thread",
	.reserved_flags = 0,
};

void
cluster_wal_thread_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_wal_thread_region);
}

/* ----------------------------------------------------------------
 * Identity + stamp (the only hot-path entry points)
 * ----------------------------------------------------------------
 */

static PgracInitdbWalContext initdb_wal_context;

/* BKI bootstrap finishes with a real shutdown checkpoint, but has not allocated
 * any normal XID.  Post-bootstrap SQL necessarily advances it.  A valid pipe
 * must not turn a completed database into initdb or change its WAL thread. */
static bool
initdb_bootstrap_wal_matches(const ControlFileData *control,
							 const PgracInitdbWalContext *context)
{
	XLogLongPageHeaderData header;
	const TimeLineID bootstrap_tli = 1; /* BootStrapXLOG's original timeline */
	struct stat st;
	char path[MAXPGPATH];
	int fd;
	ssize_t n;
	bool valid;

	if (!IsValidWalSegSize(control->xlog_seg_size)
		|| control->checkPoint < (XLogRecPtr) control->xlog_seg_size + SizeOfXLogLongPHD
		|| control->checkPointCopy.redo != control->checkPoint
		|| U64FromFullTransactionId(control->checkPointCopy.nextXid) != FirstNormalTransactionId
		|| control->checkPointCopy.ThisTimeLineID != bootstrap_tli)
		return false;
	XLogFilePath(path, bootstrap_tli, 1, control->xlog_seg_size);
	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY | O_NOFOLLOW);
	if (fd < 0)
		return false;
	do { n = read(fd, &header, sizeof(header)); } while (n < 0 && errno == EINTR);
	valid = n == sizeof(header) && fstat(fd, &st) == 0
		&& S_ISREG(st.st_mode) && st.st_nlink == 1 && st.st_uid == geteuid()
		&& st.st_size == control->xlog_seg_size
		&& header.std.xlp_magic == XLOG_PAGE_MAGIC && header.std.xlp_info == XLP_LONG_HEADER
		&& header.std.xlp_tli == bootstrap_tli
		&& header.std.xlp_pageaddr == control->xlog_seg_size && header.std.xlp_rem_len == 0
		&& header.std.xlp_thread_id == context->thread_id
		&& header.std.xlp_cluster_flags == XLP_CLUSTER_FLAGS_RESERVED
		&& header.xlp_sysid == context->system_identifier
		&& header.xlp_seg_size == control->xlog_seg_size && header.xlp_xlog_blcksz == XLOG_BLCKSZ;
	if (close(fd) != 0)
		valid = false;
	return valid;
}

/* The original frontend has already created and pinned these two new
 * directories.  Consume its one-shot pipe before native control/WAL writes.
 * This accepts no path, checkpoint, claim or authority supplied by a GUC.
 */
void
cluster_wal_thread_initdb_accept(bool bootstrap)
{
	const char *value = getenv(PGRAC_INITDB_WAL_CONTEXT_ENV);
	PgracInitdbWalContext context;
	struct stat pipe_st, data_st, wal_st, control_st;
	char *end;
	long fd;
	size_t got = 0;
	char extra;
	ssize_t n;

	if (value == NULL)
		return;
	errno = 0;
	fd = strtol(value, &end, 10);
	if (IsUnderPostmaster || initdb_wal_context.thread_id != 0
		|| value[0] < '0' || value[0] > '9' || *end != '\0' || errno != 0
		|| fd < 3 || fd > INT_MAX || fstat((int) fd, &pipe_st) != 0
		|| !S_ISFIFO(pipe_st.st_mode) || pipe_st.st_uid != geteuid())
		ereport(FATAL, (errmsg("INITDB_WAL_CONTEXT: invalid original-creator pipe")));
	/* Never wait for an untrusted or unfinished context producer. */
	if (fcntl((int) fd, F_SETFL, O_NONBLOCK) < 0)
		ereport(FATAL, (errmsg("INITDB_WAL_CONTEXT: cannot read creator pipe: %m")));
	while (got < sizeof(context))
	{
		n = read((int) fd, (char *) &context + got, sizeof(context) - got);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			ereport(FATAL, (errmsg("INITDB_WAL_CONTEXT: incomplete creator pipe")));
		got += n;
	}
	do { n = read((int) fd, &extra, 1); } while (n < 0 && errno == EINTR);
	if (n != 0 || close((int) fd) != 0 || unsetenv(PGRAC_INITDB_WAL_CONTEXT_ENV) != 0)
		ereport(FATAL, (errmsg("INITDB_WAL_CONTEXT: creator pipe is not exactly closed")));
	if (context.magic != PGRAC_INITDB_WAL_CONTEXT_MAGIC
		|| context.thread_id == 0 || context.thread_id > CLUSTER_WAL_THREAD_MAX
		|| context.phase != (bootstrap ? PGRAC_INITDB_WAL_BOOTSTRAP : PGRAC_INITDB_WAL_POSTBOOTSTRAP)
		|| stat(".", &data_st) != 0 || stat(XLOGDIR, &wal_st) != 0
		|| !S_ISDIR(data_st.st_mode) || !S_ISDIR(wal_st.st_mode)
		|| data_st.st_uid != geteuid() || wal_st.st_uid != geteuid()
		|| (data_st.st_mode & 0022) != 0 || (wal_st.st_mode & 0022) != 0
		|| (uint64) data_st.st_dev != context.data_device
		|| (uint64) data_st.st_ino != context.data_inode
		|| (uint64) wal_st.st_dev != context.wal_device
		|| (uint64) wal_st.st_ino != context.wal_inode)
		ereport(FATAL, (errmsg("INITDB_WAL_CONTEXT: creator directory or phase changed")));
	if (bootstrap)
	{
		if (lstat(XLOG_CONTROL_FILE, &control_st) == 0 || errno != ENOENT)
			ereport(FATAL, (errmsg("INITDB_WAL_CONTEXT: bootstrap control already exists")));
	}
	else
	{
		ControlFileData *control;
		bool crc_ok;
		bool valid;

		if (lstat(XLOG_CONTROL_FILE, &control_st) != 0 || !S_ISREG(control_st.st_mode)
			|| control_st.st_nlink != 1 || control_st.st_uid != geteuid())
			ereport(FATAL, (errmsg("INITDB_WAL_CONTEXT: invalid bootstrap control file")));
		control = get_controlfile(DataDir, &crc_ok);
		valid = crc_ok && context.system_identifier != 0
			&& control->system_identifier == context.system_identifier
			&& control->pg_control_version == PG_CONTROL_VERSION
			&& control->catalog_version_no == CATALOG_VERSION_NO
			&& control->state == DB_SHUTDOWNED
			&& control->data_checksum_version == PG_DATA_CHECKSUM_VERSION
			&& initdb_bootstrap_wal_matches(control, &context);
		pfree(control);
		if (!valid)
			ereport(FATAL, (errmsg("INITDB_WAL_CONTEXT: bootstrap control identity changed")));
	}
	initdb_wal_context = context;
}

uint64
cluster_wal_thread_initdb_system_identifier(void)
{
	return initdb_wal_context.system_identifier;
}

uint16
cluster_wal_thread_initdb_stamp(void)
{
	return initdb_wal_context.thread_id;
}

uint16
cluster_wal_thread_id(void)
{
	return cluster_wal_thread_id_for(cluster_enabled, cluster_node_id);
}

uint16
cluster_wal_thread_stamp(void)
{
	uint16 tid = cluster_wal_thread_id();

	/* Initial DATA/catalog creation is still native standalone processing.
	 * Its page stamp must not install an online identity or call the SCN/GCS
	 * path merely to identify the original writer's WAL. */
	if (initdb_wal_context.thread_id != 0)
		return initdb_wal_context.thread_id;

	/*
	 * nofail + critical-section-safe: one conditional atomic add, no
	 * locks, no elog.  Tolerate an unattached region (L19) by skipping
	 * the counter, never the stamp value.
	 */
	if (tid != XLP_THREAD_ID_LEGACY && cluster_wal_thread_shmem != NULL)
		pg_atomic_fetch_add_u64(&cluster_wal_thread_shmem->page_stamp_count, 1);
	return tid;
}

/* ----------------------------------------------------------------
 * Dump accessors (cluster_debug.c category 'wal_thread')
 * ----------------------------------------------------------------
 */

uint64
cluster_wal_thread_page_stamp_count(void)
{
	if (cluster_wal_thread_shmem == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_wal_thread_shmem->page_stamp_count);
}

uint16
cluster_wal_thread_dump_thread_id(void)
{
	if (cluster_wal_thread_shmem == NULL)
		return XLP_THREAD_ID_LEGACY;
	return cluster_wal_thread_shmem->thread_id;
}

bool
cluster_wal_thread_dir_configured(void)
{
	return cluster_wal_thread_shmem != NULL && cluster_wal_thread_shmem->dir_configured != 0;
}

bool
cluster_wal_thread_dir_validated(void)
{
	return cluster_wal_thread_shmem != NULL && cluster_wal_thread_shmem->dir_validated != 0;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!cluster_enabled || !cluster_shared_config || cluster_wal_thread_shmem == NULL
		|| !cluster_wal_thread_shmem->dir_validated
		|| pg_atomic_read_u32(&cluster_wal_thread_shmem->writer_ref_state) != 2)
		return false;
	pg_read_barrier();
	*out = cluster_wal_thread_shmem->v2_ref;
	if (out->claim.identity.origin_node_id != cluster_node_id
		|| out->claim.identity.origin_thread_id != cluster_wal_thread_id()) {
		memset(out, 0, sizeof(*out));
		return false;
	}
	return true;
}

bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!cluster_enabled || !cluster_shared_config || cluster_wal_thread_shmem == NULL
		|| !cluster_wal_thread_shmem->restart_ref_valid || !cluster_wal_thread_shmem->dir_validated
		|| cluster_wal_thread_shmem->restart_ref.claim.identity.origin_node_id != cluster_node_id
		|| cluster_wal_thread_shmem->restart_ref.claim.identity.origin_thread_id
			   != cluster_wal_thread_id())
		return false;
	*out = cluster_wal_thread_shmem->restart_ref;
	return true;
}

void
cluster_wal_thread_checkpoint_observed_v1(const ClusterControlRootSnapshot *record,
										  XLogRecPtr native_redo)
{
	ClusterWalSourceRef ref;
	ClusterWalThreadCheckpointSampleV1 sample;

	/* Best-effort observation only. Never change the publisher's result or
	 * infer a floor from an unbound writer, another boot, or scalar LSNs. */
	if (MyBackendType != B_CHECKPOINTER || record == NULL
		|| !cluster_wal_thread_current_v2_ref(&ref)
		|| memcmp(&record->identity, &ref.claim.identity, sizeof(record->identity)) != 0
		|| record->root_publish_seq == 0 || record->checkpoint_tli != ref.timeline
		|| record->tail_tli != ref.timeline || record->checkpoint_lower_lsn == InvalidXLogRecPtr
		|| record->checkpoint_lower_lsn > native_redo || native_redo > record->tail_last_record_lsn
		|| record->tail_last_record_lsn >= record->validated_tail_lsn_exclusive)
		return;
	memset(&sample, 0, sizeof(sample));
	sample.root_publish_seq = record->root_publish_seq;
	sample.retained_lower = record->checkpoint_lower_lsn;
	sample.native_redo = native_redo;
	sample.validated_tail = record->validated_tail_lsn_exclusive;
	sample.published_at_usec = record->published_at_usec;
	SpinLockAcquire(&cluster_wal_thread_shmem->checkpoint_sample_lock);
	if (sample.root_publish_seq > cluster_wal_thread_shmem->checkpoint_sample.root_publish_seq)
		cluster_wal_thread_shmem->checkpoint_sample = sample;
	SpinLockRelease(&cluster_wal_thread_shmem->checkpoint_sample_lock);
}

bool
cluster_wal_thread_checkpoint_sample_v1(ClusterWalThreadCheckpointSampleV1 *out)
{
	ClusterWalSourceRef ref;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!cluster_wal_thread_current_v2_ref(&ref))
		return false;
	SpinLockAcquire(&cluster_wal_thread_shmem->checkpoint_sample_lock);
	*out = cluster_wal_thread_shmem->checkpoint_sample;
	SpinLockRelease(&cluster_wal_thread_shmem->checkpoint_sample_lock);
	return out->root_publish_seq != 0;
}

ClusterControlRootResult
cluster_wal_thread_install_startup(const ClusterWalStartupImage *expected)
{
	ClusterWalSourceRef installed;
	ClusterControlRootResult result;
	uint32 state = 0;
	if (expected == NULL || !cluster_enabled || !cluster_shared_config
		|| cluster_wal_thread_shmem == NULL || !cluster_wal_thread_shmem->restart_ref_valid
		|| !cluster_wal_thread_shmem->dir_validated)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* No caller flag stands in for the actual selected writer and its
	 * durability. This call also qualifies an uncertain same-process retry. */
	result = cluster_control_root_v3_startup_install_writer(expected, &installed);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_control_bootstrap_wal_route(DataDir, cluster_wal_threads_dir, &installed);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (installed.claim.identity.origin_node_id != cluster_node_id
		|| installed.claim.identity.origin_thread_id != cluster_wal_thread_id()
		|| !cluster_wal_writer_startup_matches(&installed.claim.identity, expected->operation_uuid,
												expected->first_segment_lsn))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!pg_atomic_compare_exchange_u32(&cluster_wal_thread_shmem->writer_ref_state, &state, 1)) {
		if (state != 2)
			return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		pg_read_barrier();
		return memcmp(&installed, &cluster_wal_thread_shmem->v2_ref, sizeof(installed)) == 0
				   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
				   : CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	}
	/* No I/O, allocation or error-capable work inside the once-only copy.
	 * Readers inspect READY then use an acquire barrier; bytes never change
	 * afterwards. The immutable predecessor mirror is never touched here. */
	cluster_wal_thread_shmem->v2_ref = installed;
	pg_write_barrier();
	pg_atomic_write_u32(&cluster_wal_thread_shmem->writer_ref_state, 2);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

bool
cluster_wal_thread_claim_created(void)
{
	return cluster_wal_thread_shmem != NULL && cluster_wal_thread_shmem->claim_created != 0;
}

/* spec-4.2: refresh-failure counter (L19-tolerant on both sides). */
uint64
cluster_wal_thread_refresh_fail_fetch_add(void)
{
	if (cluster_wal_thread_shmem == NULL)
		return 1; /* no shmem -> pretend non-first so callers stay quiet */
	return pg_atomic_fetch_add_u64(&cluster_wal_thread_shmem->wal_state_refresh_fail_count, 1);
}

uint64
cluster_wal_thread_refresh_fail_read(void)
{
	if (cluster_wal_thread_shmem == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_wal_thread_shmem->wal_state_refresh_fail_count);
}

/* ----------------------------------------------------------------
 * Startup validation
 * ----------------------------------------------------------------
 */

/*
 * same_directory -- do two paths name the same directory object?
 *
 *	st_dev/st_ino identity covers both symlink and bind-mount
 *	relocation (spec-4.1 §2.4).  stat() follows symlinks, which is
 *	exactly what we want: $PGDATA/pg_wal is typically a symlink to
 *	<wal_threads_dir>/thread_<id>.
 *
 *	WIN32 note: st_ino is not meaningful there; fall back to a
 *	canonicalized path comparison.  pgrac CI covers Linux + macOS;
 *	the fallback keeps the build honest on Windows without claiming
 *	tested support.
 */
static bool
same_directory(const char *a, const char *b)
{
#ifndef WIN32
	struct stat sa;
	struct stat sb;

	if (stat(a, &sa) != 0 || stat(b, &sb) != 0)
		return false;
	if (!S_ISDIR(sa.st_mode) || !S_ISDIR(sb.st_mode))
		return false;
	return sa.st_dev == sb.st_dev && sa.st_ino == sb.st_ino;
#else
	char ca[MAXPGPATH];
	char cb[MAXPGPATH];

	strlcpy(ca, a, sizeof(ca));
	strlcpy(cb, b, sizeof(cb));
	canonicalize_path(ca);
	canonicalize_path(cb);
	return strcmp(ca, cb) == 0;
#endif
}

/*
 * claim_read -- read the claim file into *claim.
 *
 *	Returns 1 on success, 0 when the file does not exist (first boot),
 *	and -1 on any other failure (open error, short read) with *claim
 *	zeroed; the caller turns -1 into FATAL 53RA1 (a half-written or
 *	unreadable claim is fail-closed, never auto-rebuilt).
 */
static int
claim_read(const char *path, ClusterWalThreadClaim *claim)
{
	int fd;
	ssize_t nread;

	memset(claim, 0, sizeof(*claim));

	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0) {
		if (errno == ENOENT)
			return 0;
		return -1;
	}

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_THREAD_CLAIM_READ);
	nread = read(fd, claim, sizeof(*claim));
	pgstat_report_wait_end();

	/*
	 * Keep errno meaningful for the caller's %m: a short read (torn
	 * claim) sets none, and close() may clobber whatever read() set.
	 */
	if (nread >= 0 && nread < (ssize_t)sizeof(*claim))
		errno = EIO;
	{
		int save_errno = errno;

		close(fd);
		errno = save_errno;
	}

	return (nread == (ssize_t)sizeof(*claim)) ? 1 : -1;
}

/*
 * claim_create -- first-boot claim creation (write-once semantics).
 *
 *	O_EXCL create + full write + pg_fsync(file) + pg_fsync(parent dir)
 *	(L47 create-side discipline: the dirent must be durable too).
 *	Returns false on any failure; the caller raises FATAL 53RA1.
 */
static bool
claim_create(const char *path, const char *dir, const ClusterWalThreadClaim *claim)
{
	int fd;
	ssize_t nwritten;

	/*
	 * Decision-style injection (L101): when armed, simulate a create
	 * failure so cluster_tap can exercise the 53RA1 path without real
	 * storage faults.
	 */
	if (cluster_injection_should_skip("cluster-wal-thread-claim-create-fail"))
		return false;

	fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0)
		return false;

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_THREAD_CLAIM_WRITE);
	nwritten = write(fd, claim, sizeof(*claim));
	if (nwritten == (ssize_t)sizeof(*claim) && pg_fsync(fd) == 0) {
		pgstat_report_wait_end();
		close(fd);
	} else {
		/*
		 * Preserve the write()/pg_fsync() errno across close()/unlink()
		 * so the caller's FATAL %m names the real failure; a short
		 * write sets none, so supply EIO.
		 */
		int save_errno = (nwritten >= 0 && nwritten < (ssize_t)sizeof(*claim)) ? EIO : errno;

		pgstat_report_wait_end();
		close(fd);
		(void)unlink(path); /* best-effort: do not leave a torn claim */
		errno = save_errno;
		return false;
	}

	/* fsync the parent directory so the new dirent is durable */
	fd = BasicOpenFile(dir, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WAL_THREAD_CLAIM_WRITE);
	if (pg_fsync(fd) != 0) {
		int save_errno = errno;

		pgstat_report_wait_end();
		close(fd);
		errno = save_errno;
		return false;
	}
	pgstat_report_wait_end();
	close(fd);
	return true;
}

void
cluster_wal_thread_init(void)
{
	uint16 tid;
	char dirname[64];
	char thread_dir[MAXPGPATH];
	char pg_wal_path[MAXPGPATH];
	char claim_path[MAXPGPATH];
	ClusterWalThreadClaim claim;
	int got;
	bool dir_set;

	/*
	 * Postmaster-once (CLAUDE.md rule 16): EXEC_BACKEND children re-run
	 * CreateSharedMemoryAndSemaphores; everything below must happen
	 * exactly once, in the postmaster (or a standalone backend), before
	 * StartupXLOG reads any WAL.
	 */
	if (IsUnderPostmaster)
		return;

	tid = cluster_wal_thread_id();
	dir_set = (cluster_wal_threads_dir != NULL && cluster_wal_threads_dir[0] != '\0');

	/* Mirror identity for the dump accessors (EXEC_BACKEND-safe). */
	if (cluster_wal_thread_shmem != NULL) {
		cluster_wal_thread_shmem->thread_id = tid;
		cluster_wal_thread_shmem->dir_configured = dir_set ? 1 : 0;
	}
	if (cluster_shared_config && !dir_set)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH),
						errmsg("PRE2 shared configuration requires an exact WAL root")));

	if (!dir_set) {
		if (cluster_shared_catalog && cluster_conf_has_peers())
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cluster.shared_catalog requires cluster.wal_threads_dir in a "
							"multi-node cluster"),
					 errhint("Set cluster.wal_threads_dir to the shared per-thread WAL root and "
							 "initialise each node with pgrac-init --wal-threads-dir.")));
		if (!cluster_hw_remaster_recoverable())
			ereport(WARNING,
					(errmsg("cluster HW remaster is not recoverable because "
							"cluster.wal_threads_dir is unset"),
					 errhint("A dead node's adopted HW authority cannot be rebuilt from per-thread "
							 "WAL until cluster.wal_threads_dir is configured and the node is "
							 "restarted.")));
		return; /* flat layout: identity stamping only (Q3-A) */
	}

	/*
	 * Configuration coherence, fail-closed (L58: enumerate the
	 * syntactically-valid-but-semantically-empty boundaries too).
	 */
	if (!cluster_enabled)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH),
						errmsg("cluster.wal_threads_dir is set but cluster.enabled is off"),
						errhint("Either enable the cluster or unset cluster.wal_threads_dir.")));
	if (cluster_node_id < 0)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH),
						errmsg("cluster.wal_threads_dir is set but cluster.node_id is not"),
						errhint("Set cluster.node_id to this node's identifier (0..127).")));

	/* PRE2 never falls back to the v1 claim/create or legacy registry path.
	 * Preparation and reobservation remain read-only namespace checks. The
	 * subsequent physical/fence/recovery/serving gates still own admission. */
	if (cluster_shared_config) {
		ClusterWalSourceRef ref;
		if (cluster_wal_thread_shmem == NULL)
			ereport(FATAL, (errmsg("shared WAL identity state is not initialized")));
		/* All actual DataDir reads/crypto stay in the adapter's temporary owner;
		 * the early postmaster has no transaction ResourceOwner to borrow. */
		cluster_control_bootstrap_wal_recheck(DataDir, &ref);
		cluster_wal_thread_shmem->restart_ref = ref;
		cluster_wal_thread_shmem->dir_validated = 1;
		cluster_wal_thread_shmem->restart_ref_valid = true;
		return;
	}

	CLUSTER_INJECTION_POINT("cluster-wal-thread-validate-pre");

	Assert(tid >= XLP_THREAD_ID_FIRST_REAL && tid <= CLUSTER_WAL_THREAD_MAX);
	cluster_wal_thread_dir_name(tid, dirname, sizeof(dirname));
	snprintf(thread_dir, sizeof(thread_dir), "%s/%s", cluster_wal_threads_dir, dirname);
	snprintf(pg_wal_path, sizeof(pg_wal_path), "%s/%s", DataDir, XLOGDIR);

	/*
	 * The thread directory must pre-exist (bootstrap creates it via
	 * initdb -X).  Never mkdir here: auto-creating on a typo would
	 * silently open a brand-new WAL stream (spec-4.1 t/243 L6).
	 */
	{
		struct stat st;

		if (stat(thread_dir, &st) != 0 || !S_ISDIR(st.st_mode))
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH),
					 errmsg("WAL thread directory \"%s\" does not exist", thread_dir),
					 errdetail("cluster.wal_threads_dir is \"%s\" and this node's thread id is %u.",
							   cluster_wal_threads_dir, (unsigned)tid),
					 errhint("Initialise the node with pgrac-init --wal-threads-dir, or create "
							 "and relocate the WAL directory manually (see the pgrac manual).")));
	}

	if (!same_directory(pg_wal_path, thread_dir))
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_WAL_THREAD_ROUTING_MISMATCH),
				 errmsg("\"%s\" does not resolve to this node's WAL thread directory", pg_wal_path),
				 errdetail("Expected %s to be the same directory as \"%s\" (thread %u for "
						   "cluster.node_id %d).",
						   XLOGDIR, thread_dir, (unsigned)tid, cluster_node_id),
				 errhint("Re-link " XLOGDIR " to the thread directory, or fix "
						 "cluster.wal_threads_dir / cluster.node_id.")));

	snprintf(claim_path, sizeof(claim_path), "%s/%s", thread_dir,
			 CLUSTER_WAL_THREAD_CLAIM_FILENAME);

	got = claim_read(claim_path, &claim);
	if (got == 0) {
		/* First validated boot of this thread directory: claim it. */
		cluster_wal_thread_claim_fill(&claim, tid, cluster_node_id, (int64)GetCurrentTimestamp());
		if (!claim_create(claim_path, thread_dir, &claim))
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_WAL_THREAD_CLAIM_CONFLICT),
					 errmsg("could not create WAL thread claim file \"%s\": %m", claim_path),
					 errhint("Check that the shared WAL storage is writable by this node.")));
		if (cluster_wal_thread_shmem != NULL)
			cluster_wal_thread_shmem->claim_created = 1;
		ereport(LOG, (errmsg("pgrac WAL thread %u claimed by node %d (\"%s\")", (unsigned)tid,
							 cluster_node_id, claim_path)));
	} else if (got < 0) {
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_WAL_THREAD_CLAIM_CONFLICT),
				 errmsg("could not read WAL thread claim file \"%s\": %m", claim_path),
				 errhint("The claim file is unreadable or torn.  After confirming no other node "
						 "owns this thread directory, remove the file and restart.")));
	} else {
		const char *reason = NULL;

		if (!cluster_wal_thread_claim_validate(&claim, tid, cluster_node_id, &reason))
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_WAL_THREAD_CLAIM_CONFLICT),
							errmsg("WAL thread directory \"%s\" is claimed by node %d (thread %u)",
								   thread_dir, claim.node_id, (unsigned)claim.thread_id),
							errdetail("Claim validation failed: %s.", reason ? reason : "unknown"),
							errhint("Each WAL thread directory belongs to exactly one node.  After "
									"confirming ownership, remove \"%s\" and restart (it is never "
									"rebuilt automatically).",
									claim_path)));
	}

	if (cluster_wal_thread_shmem != NULL)
		cluster_wal_thread_shmem->dir_validated = 1;

	ereport(LOG, (errmsg("pgrac WAL thread routing validated: thread %u at \"%s\"", (unsigned)tid,
						 thread_dir)));

	/*
	 * spec-4.2: bring up the ClusterWalState registry (create-once or
	 * validate; FATAL 53RA2 inside on corruption/IO).  The own slot is
	 * NOT touched here: ACTIVE is published only after recovery has
	 * succeeded, at the phase4 -> CLUSTER_PHASE_RUNNING transition
	 * (spec-4.2 v0.2 P1).
	 */
	CLUSTER_INJECTION_POINT("cluster-wal-state-ensure-pre");
	(void)cluster_wal_state_ensure();
}

#else /* !USE_PGRAC_CLUSTER */

/*
 * Disable-cluster build: this file compiles to nothing.  xlog.c gates
 * its calls under USE_PGRAC_CLUSTER and stamps the legacy constants
 * directly (spec-4.1 §3.1: byte-identical stream).
 */

#endif /* USE_PGRAC_CLUSTER */
