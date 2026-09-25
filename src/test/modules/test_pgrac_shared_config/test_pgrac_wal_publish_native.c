/* PGRAC: real critical-section memory/resource/crypto/IO boundary.
 * Only external formation facts are test-local. Never grants live admission.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/resowner_private.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_wal_durable_prefix.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"
#include "common/cryptohash.h"

static char *native_root, *native_data;
static ClusterWalDurablePrefixRef native_ref;
static uint64
native_epoch(void)
{
	return 7;
}
static uint64
native_inc(void)
{
	return 99;
}
static uint64
native_admitted(int32 node pg_attribute_unused())
{
	return 99;
}
static ClusterMembershipState
native_member(int32 node pg_attribute_unused())
{
	return CLUSTER_MEMBER_MEMBER;
}
static bool
native_active(void)
{
	return true;
}
static bool
native_prebump(void)
{
	return false;
}
static bool
native_reference(ClusterWalDurablePrefixRef *out)
{
	*out = native_ref;
	return true;
}
static void
native_fence(ClusterWriteFenceObservation *out)
{
	memset(out, 0, sizeof(*out));
	out->enforcing = out->attached = out->engaged = out->allowed = true;
	out->epoch_current = out->authorized_epoch = 7;
	out->now_us = 100;
	out->expiry_us = 10000;
}

void native_publish_init(void);
ClusterControlRootResult native_publish_ready(TimeLineID);
ClusterControlRootResult native_publish_call(TimeLineID, XLogRecPtr, XLogRecPtr, XLogRecPtr *);
#define cluster_wal_durable_publish_init native_publish_init
#define cluster_wal_durable_publish_ready native_publish_ready
#define cluster_wal_durable_publish native_publish_call
#define cluster_enabled true
#define cluster_shared_config true
#define cluster_node_id 0
#define cluster_wal_threads_dir native_root
#define DataDir native_data
#define cluster_epoch_get_current native_epoch
#define cluster_qvotec_get_self_incarnation native_inc
#define cluster_membership_get_last_admitted_incarnation native_admitted
#define cluster_membership_get_state native_member
#define cluster_external_fence_runtime_active native_active
#define cluster_reconfig_has_pending_prebump_stage native_prebump
#define cluster_wal_thread_current_v2_ref native_reference
#define cluster_write_fence_observe native_fence
#include "../../../backend/cluster/cluster_wal_durable_publish.c"
#undef DataDir

static void
native_write(const char *path, const void *bytes, size_t len)
{
	int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
	if (fd < 0 || write(fd, bytes, len) != len || close(fd) != 0)
		ereport(ERROR, (errmsg("could not create disposable native WAL fixture")));
}
#endif

PG_FUNCTION_INFO_V1(test_pgrac_wal_publish_native);
Datum
test_pgrac_wal_publish_native(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("native resource test requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		char scratch[MAXPGPATH], generation[MAXPGPATH], path[MAXPGPATH], name[MAXFNAMELEN];
		ClusterWalThreadClaimV2 claim = { 0 };
		ClusterWalDurablePrefix prefix;
		uint8 claim_bytes[112], prefix_bytes[256];
		char *page = palloc0(XLOG_BLCKSZ);
		XLogLongPageHeader header = (XLogLongPageHeader)page;
		XLogRecord record = { 0 }, next;
		pg_cryptohash_ctx *hash;
		ClusterControlRootResult result;
		MemoryContext saved_context = CurrentMemoryContext;
		ResourceOwner saved_owner = CurrentResourceOwner;
		XLogRecPtr covered, expected;
		int fd;
		bool success = true;
		snprintf(scratch, sizeof(scratch), "%s/native-publish-XXXXXX", DataDir);
		if (!mkdtemp(scratch))
			ereport(ERROR, (errmsg("native fixture mkdtemp failed")));
		native_root = psprintf("%s/wal", scratch);
		native_data = psprintf("%s/data", scratch);
		if (mkdir(native_root, 0700) || mkdir(native_data, 0700))
			elog(ERROR, "native fixture mkdir");
		snprintf(path, sizeof(path), "%s/thread_1", native_root);
		if (mkdir(path, 0700))
			elog(ERROR, "native fixture thread");
		snprintf(generation, sizeof(generation), "%s/generation_99", path);
		if (mkdir(generation, 0700))
			elog(ERROR, "native fixture generation");
		snprintf(path, sizeof(path), "%s/durable_prefix", generation);
		if (mkdir(path, 0700))
			elog(ERROR, "native fixture prefix");
		snprintf(path, sizeof(path), "%s/pg_wal", native_data);
		if (symlink(generation, path))
			elog(ERROR, "native fixture route");
		claim.identity.system_identifier = GetSystemIdentifier();
		memset(claim.identity.storage_uuid, 3, 16);
		memset(claim.identity.authority_uuid, 5, 16);
		claim.identity.origin_thread_id = 1;
		claim.identity.origin_owner_incarnation = 99;
		claim.identity.root_lineage_seq = 9;
		claim.identity.thread_claim_created_at = 23;
		claim.database_incarnation = 7;
		claim.config_generation = claim.claim_generation = 1;
		if (cluster_wal_claim_v2_encode(&claim, claim_bytes) != 0)
			elog(ERROR, "native fixture claim");
		memset(&native_ref, 0, sizeof(native_ref));
		native_ref.claim.identity = claim.identity;
		memcpy(&native_ref.claim.identity.thread_claim_crc32c, claim_bytes + 104, 4);
		native_ref.claim.database_incarnation = 7;
		native_ref.timeline = native_ref.claim.max_config_generation = 1;
		hash = pg_cryptohash_create(PG_SHA256);
		if (!hash || pg_cryptohash_init(hash) || pg_cryptohash_update(hash, claim_bytes, 112)
			|| pg_cryptohash_final(hash, native_ref.claim.claim_sha256, 32))
			elog(ERROR, "native fixture hash");
		pg_cryptohash_free(hash);
		snprintf(path, sizeof(path), "%s/pgrac_thread.claim", generation);
		native_write(path, claim_bytes, 112);
		header->std.xlp_magic = XLOG_PAGE_MAGIC;
		header->std.xlp_info = XLP_LONG_HEADER;
		header->std.xlp_tli = header->std.xlp_thread_id = 1;
		header->std.xlp_pageaddr = wal_segment_size;
		header->xlp_sysid = GetSystemIdentifier();
		header->xlp_seg_size = wal_segment_size;
		header->xlp_xlog_blcksz = XLOG_BLCKSZ;
		record.xl_tot_len = SizeOfXLogRecord;
		record.xl_rmid = RM_XLOG_ID;
		record.xl_info = XLOG_NOOP;
		INIT_CRC32C(record.xl_crc);
		COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(record.xl_crc);
		memcpy(page + SizeOfXLogLongPHD, &record, SizeOfXLogRecord);
		XLogFileName(name, 1, 1, wal_segment_size);
		snprintf(path, sizeof(path), "%s/%s", generation, name);
		native_write(path, page, XLOG_BLCKSZ);
		fd = open(path, O_RDWR);
		next = record;
		next.xl_prev = wal_segment_size + SizeOfXLogLongPHD;
		INIT_CRC32C(next.xl_crc);
		COMP_CRC32C(next.xl_crc, &next, offsetof(XLogRecord, xl_crc));
		FIN_CRC32C(next.xl_crc);
		if (fd < 0 || ftruncate(fd, wal_segment_size)
			|| pwrite(fd, &next, SizeOfXLogRecord, SizeOfXLogLongPHD + MAXALIGN(SizeOfXLogRecord))
				   != SizeOfXLogRecord
			|| close(fd))
			elog(ERROR, "native fixture segment");
		prefix = (ClusterWalDurablePrefix){ 5,
											wal_segment_size + SizeOfXLogLongPHD
												+ MAXALIGN(SizeOfXLogRecord),
											wal_segment_size + SizeOfXLogLongPHD, record.xl_crc };
		expected = prefix.exclusive_end + MAXALIGN(SizeOfXLogRecord);
		if (cluster_wal_durable_prefix_encode(&native_ref, &prefix, prefix_bytes) != 0)
			elog(ERROR, "native fixture encode");
		snprintf(path, sizeof(path), "%s/durable_prefix/current", generation);
		native_write(path, prefix_bytes, 256);
		native_publish_init();
		/* Independent REDs: successful crypto allocation and an early IO
		 * refusal that reaches cleanup without allocating a hash first. */
		if (PG_GETARG_BOOL(0)) {
			snprintf(path, sizeof(path), "%s/pgrac_thread.claim", generation);
			if (unlink(path))
				elog(ERROR, "native fixture missing claim");
		}
		for (int i = 0; i < 3; ++i) {
			START_CRIT_SECTION();
			LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
			result = native_publish_call(1, expected, prefix.exclusive_end, &covered);
			LWLockRelease(WALWriteLock);
			END_CRIT_SECTION();
			success = success
					  && (PG_GETARG_BOOL(0) ? result != 0 && covered == 0
											: result == 0 && covered == expected)
					  && CurrentMemoryContext == saved_context
					  && CurrentResourceOwner == saved_owner;
		}
		if (!rmtree(scratch, true))
			elog(ERROR, "native fixture cleanup");
		pfree(page);
		pfree(native_root);
		pfree(native_data);
		PG_RETURN_BOOL(success);
	}
#else
	ereport(ERROR, (errmsg("PGRAC cluster build required")));
	PG_RETURN_NULL();
#endif
}
