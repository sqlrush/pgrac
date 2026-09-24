/*-------------------------------------------------------------------------
 *
 * cluster_recovery_anchor.c
 *	  Per-node durable recovery anchor (spec-5.6a).
 *
 *	  Owns one small per-node sidecar file under the shared data root,
 *	  (shared_data_dir)/global/pgrac_recovery_anchor_n<node_id>, holding
 *	  this node's own last checkpoint record LSN, a full CheckPoint copy,
 *	  and this node's own DBState.  Under the shared pg_control authority
 *	  (cluster.controlfile_shared_authority=on) the shared control file's
 *	  checkpoint fields belong to whichever node wrote them last, so a
 *	  restarting node consumes its anchor instead: the anchor restores,
 *	  field for field, the restart inputs a per-node pg_control provided
 *	  before the authority was shared.
 *
 *	  Writes mirror the shared-authority torn-safe pattern (roll the live
 *	  primary into .bak, temp-write the new image, durable_rename it into
 *	  place) and PANIC on I/O failure: an anchor that silently stops
 *	  advancing loses this node's recoverability as soon as WAL recycling
 *	  passes its stale redo point.  Reads never ereport; they classify the
 *	  primary and then the .bak under the same strict validation and
 *	  return false so the caller decides the error face (FATAL 53RB3).
 *	  The PRIMARY anchor never points past recycled WAL (it is written
 *	  before the same checkpoint cycle recycles).  The .bak is best-effort
 *	  corruption recovery: after the next cycle recycles WAL it may point
 *	  below the retained floor, in which case adopting it fails closed
 *	  downstream (ReadCheckpointRecord PANICs on the unreadable record;
 *	  never a silent wrong recovery), so no extra acceptance probe is
 *	  applied.
 *
 *	  Single writer: the owning node's startup process or checkpointer,
 *	  serialized by the checkpoint interlock.  No cross-node access; other
 *	  nodes never read this file.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_recovery_anchor.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6a-per-node-recovery-anchor.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_recovery_anchor.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_stats.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"
#include "common/cryptohash.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"

#include "cluster_recovery_anchor_private.h"

/* PGRAC: v2 is explicitly encoded, not an overlay on the native v1 struct.
 * These are the frozen homogeneous PG16 CheckPoint offsets; a native layout
 * change requires a deliberate format/manifest update, not a new memcpy.
 * Author: SqlRush <sqlrush@gmail.com>
 */
StaticAssertDecl(sizeof(CheckPoint) == 88, "anchor v2 CheckPoint layout");
StaticAssertDecl(offsetof(CheckPoint, nextXid) == 24, "anchor v2 nextXid layout");
StaticAssertDecl(offsetof(CheckPoint, time) == 64, "anchor v2 time layout");
StaticAssertDecl(offsetof(CheckPoint, oldestActiveXid) == 80, "anchor v2 final field layout");

static uint64
anchor_v2_get(const uint8 *bytes, size_t offset, size_t width)
{
	uint64 value = 0;
	size_t i;

	for (i = 0; i < width; ++i)
		value |= (uint64)bytes[offset + i] << (8 * i);
	return value;
}

static void
anchor_v2_put(uint8 *bytes, size_t offset, uint64 value, size_t width)
{
	size_t i;

	for (i = 0; i < width; ++i)
		bytes[offset + i] = (uint8)(value >> (8 * i));
}

static bool
anchor_v2_zero(const void *ptr, size_t len)
{
	const uint8 *bytes = ptr;
	size_t i;

	for (i = 0; i < len; ++i)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
anchor_v2_identity_valid(const ClusterControlRootIdentity *id)
{
	return id->system_identifier != 0 && id->origin_thread_id > 0
		   && id->origin_thread_id <= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		   && id->origin_node_id == (int32)id->origin_thread_id - 1 && id->reserved42 == 0
		   && id->reserved60 == 0 && id->thread_claim_created_at > 0
		   && id->origin_owner_incarnation != 0 && id->root_lineage_seq != 0
		   && !anchor_v2_zero(id->storage_uuid, 16) && !anchor_v2_zero(id->authority_uuid, 16);
}

static bool
anchor_v2_ref_valid(const ClusterRecoveryAnchorRefV2 *ref)
{
	return ref != NULL && anchor_v2_identity_valid(&ref->identity) && ref->database_incarnation != 0
		   && ref->max_config_generation != 0 && ref->anchor_generation != 0
		   && !anchor_v2_zero(ref->anchor_sha256, 32) && !anchor_v2_zero(ref->claim_sha256, 32);
}

static ClusterControlRootResult
anchor_v2_fields_valid(const ClusterRecoveryAnchorV2 *anchor)
{
	if (anchor->identity.reserved42 != 0 || anchor->identity.reserved60 != 0)
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (!anchor_v2_identity_valid(&anchor->identity))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (anchor->state > DB_IN_PRODUCTION)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (anchor->database_incarnation == 0 || anchor->config_generation == 0
		|| anchor->anchor_generation == 0 || anchor_v2_zero(anchor->claim_sha256, 32)
		|| anchor->checkpoint == InvalidXLogRecPtr
		|| anchor->checkpoint_copy.redo == InvalidXLogRecPtr
		|| anchor->checkpoint_copy.ThisTimeLineID == 0
		|| anchor->checkpoint_copy.PrevTimeLineID == 0 || anchor->backup_end_required)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_recovery_anchor_v2_encode(const ClusterRecoveryAnchorV2 *anchor,
								  uint8 bytes[CLUSTER_RECOVERY_ANCHOR_SIZE])
{
	ClusterControlRootResult result;
	pg_crc32c crc;

	if (bytes == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(bytes, 0, CLUSTER_RECOVERY_ANCHOR_SIZE);
	if (anchor == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = anchor_v2_fields_valid(anchor);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
#define AP(offset, field, width) anchor_v2_put(bytes, offset, anchor->field, width)
	anchor_v2_put(bytes, 0, CLUSTER_RECOVERY_ANCHOR_MAGIC, 4);
	anchor_v2_put(bytes, 4, 2, 2);
	AP(8, identity.origin_node_id, 4);
	AP(12, state, 4);
	AP(16, identity.system_identifier, 8);
	AP(24, checkpoint, 8);
	AP(32, write_time, 8);
	AP(40, checkpoint_copy.redo, 8);
	AP(48, checkpoint_copy.ThisTimeLineID, 4);
	AP(52, checkpoint_copy.PrevTimeLineID, 4);
	AP(56, checkpoint_copy.fullPageWrites, 1);
	anchor_v2_put(bytes, 64, U64FromFullTransactionId(anchor->checkpoint_copy.nextXid), 8);
	AP(72, checkpoint_copy.nextOid, 4);
	AP(76, checkpoint_copy.nextMulti, 4);
	AP(80, checkpoint_copy.nextMultiOffset, 4);
	AP(84, checkpoint_copy.oldestXid, 4);
	AP(88, checkpoint_copy.oldestXidDB, 4);
	AP(92, checkpoint_copy.oldestMulti, 4);
	AP(96, checkpoint_copy.oldestMultiDB, 4);
	AP(104, checkpoint_copy.time, 8);
	AP(112, checkpoint_copy.oldestCommitTsXid, 4);
	AP(116, checkpoint_copy.newestCommitTsXid, 4);
	AP(120, checkpoint_copy.oldestActiveXid, 4);
	AP(128, unlogged_lsn, 8);
	AP(136, database_incarnation, 8);
	AP(144, identity.origin_owner_incarnation, 8);
	memcpy(bytes + 152, anchor->identity.storage_uuid, 16);
	memcpy(bytes + 168, anchor->identity.authority_uuid, 16);
	AP(184, identity.root_lineage_seq, 8);
	AP(192, config_generation, 8);
	AP(200, anchor_generation, 8);
	AP(208, identity.origin_thread_id, 2);
	anchor_v2_put(bytes, 212, UINT64_C(0x01020304), 4);
	memcpy(bytes + 216, anchor->claim_sha256, 32);
	AP(248, identity.thread_claim_created_at, 8);
	AP(256, identity.thread_claim_crc32c, 4);
	AP(260, min_recovery_point, 8);
	AP(268, min_recovery_tli, 4);
	AP(272, backup_start, 8);
	AP(280, backup_end, 8);
	AP(288, backup_end_required, 1);
#undef AP
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 508);
	FIN_CRC32C(crc);
	anchor_v2_put(bytes, 508, crc, 4);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_recovery_anchor_v2_decode(const uint8 *bytes, size_t len,
								  const ClusterRecoveryAnchorRefV2 *ref,
								  ClusterRecoveryAnchorV2 *out)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterControlRootResult result;
	pg_crc32c crc;
	pg_cryptohash_ctx *ctx;
	uint8 hash[32];
	bool hash_ok;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (bytes == NULL || !anchor_v2_ref_valid(ref))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (len != CLUSTER_RECOVERY_ANCHOR_SIZE)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 508);
	FIN_CRC32C(crc);
	if ((uint32)crc != anchor_v2_get(bytes, 508, 4))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (anchor_v2_get(bytes, 0, 4) != CLUSTER_RECOVERY_ANCHOR_MAGIC)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (anchor_v2_get(bytes, 4, 2) != 2)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (anchor_v2_get(bytes, 212, 4) != UINT64_C(0x01020304))
		return CLUSTER_CONTROL_ROOT_BAD_ENDIAN;
	if (!anchor_v2_zero(bytes + 6, 2) || !anchor_v2_zero(bytes + 57, 7)
		|| !anchor_v2_zero(bytes + 100, 4) || !anchor_v2_zero(bytes + 124, 4)
		|| !anchor_v2_zero(bytes + 210, 2) || !anchor_v2_zero(bytes + 289, 219) || bytes[56] > 1
		|| bytes[288] > 1)
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	memset(&anchor, 0, sizeof(anchor));
#define AG(offset, field, width) anchor.field = anchor_v2_get(bytes, offset, width)
	AG(8, identity.origin_node_id, 4);
	AG(12, state, 4);
	AG(16, identity.system_identifier, 8);
	AG(24, checkpoint, 8);
	AG(32, write_time, 8);
	AG(40, checkpoint_copy.redo, 8);
	AG(48, checkpoint_copy.ThisTimeLineID, 4);
	AG(52, checkpoint_copy.PrevTimeLineID, 4);
	AG(56, checkpoint_copy.fullPageWrites, 1);
	anchor.checkpoint_copy.nextXid = FullTransactionIdFromU64(anchor_v2_get(bytes, 64, 8));
	AG(72, checkpoint_copy.nextOid, 4);
	AG(76, checkpoint_copy.nextMulti, 4);
	AG(80, checkpoint_copy.nextMultiOffset, 4);
	AG(84, checkpoint_copy.oldestXid, 4);
	AG(88, checkpoint_copy.oldestXidDB, 4);
	AG(92, checkpoint_copy.oldestMulti, 4);
	AG(96, checkpoint_copy.oldestMultiDB, 4);
	AG(104, checkpoint_copy.time, 8);
	AG(112, checkpoint_copy.oldestCommitTsXid, 4);
	AG(116, checkpoint_copy.newestCommitTsXid, 4);
	AG(120, checkpoint_copy.oldestActiveXid, 4);
	AG(128, unlogged_lsn, 8);
	AG(136, database_incarnation, 8);
	AG(144, identity.origin_owner_incarnation, 8);
	memcpy(anchor.identity.storage_uuid, bytes + 152, 16);
	memcpy(anchor.identity.authority_uuid, bytes + 168, 16);
	AG(184, identity.root_lineage_seq, 8);
	AG(192, config_generation, 8);
	AG(200, anchor_generation, 8);
	AG(208, identity.origin_thread_id, 2);
	memcpy(anchor.claim_sha256, bytes + 216, 32);
	AG(248, identity.thread_claim_created_at, 8);
	AG(256, identity.thread_claim_crc32c, 4);
	AG(260, min_recovery_point, 8);
	AG(268, min_recovery_tli, 4);
	AG(272, backup_start, 8);
	AG(280, backup_end, 8);
	AG(288, backup_end_required, 1);
#undef AG
	result = anchor_v2_fields_valid(&anchor);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* The public identity has explicit reserved fields and a locked 80-byte
	 * layout. Both copies are validated, so this comparison has no padding.
	 */
	if (memcmp(&anchor.identity, &ref->identity, sizeof(anchor.identity)) != 0
		|| anchor.database_incarnation != ref->database_incarnation
		|| anchor.anchor_generation != ref->anchor_generation
		|| anchor.config_generation > ref->max_config_generation
		|| memcmp(anchor.claim_sha256, ref->claim_sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	hash_ok = pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, bytes, len) >= 0
			  && pg_cryptohash_final(ctx, hash, sizeof(hash)) >= 0;
	pg_cryptohash_free(ctx);
	if (!hash_ok)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (memcmp(hash, ref->anchor_sha256, sizeof(hash)) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	memcpy(out, &anchor, sizeof(*out));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_recovery_anchor_v2_project(const uint8 *bytes, size_t len,
								   const ClusterRecoveryAnchorRefV2 *ref,
								   const ControlFileData *common, ControlFileData *out)
{
	ClusterRecoveryAnchorV2 anchor;
	ClusterControlRootResult result;
	ControlFileData projected;
	pg_crc32c crc;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (common == NULL || !anchor_v2_ref_valid(ref))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (common->system_identifier != ref->identity.system_identifier)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (common->pg_control_version != PG_CONTROL_VERSION
		|| common->catalog_version_no != CATALOG_VERSION_NO)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, common, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, common->crc))
		return CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
	result = cluster_recovery_anchor_v2_decode(bytes, len, ref, &anchor);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (anchor.backup_start != InvalidXLogRecPtr || anchor.backup_end != InvalidXLogRecPtr
		|| anchor.backup_end_required)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	/* Never replace aggregated allocator/horizon fields with one thread's
	 * local CheckPoint copy. Per-origin commit-ts bounds are not aggregates.
	 */
	memcpy(&projected, common, sizeof(projected));
	projected.state = (DBState)anchor.state;
	projected.time = anchor.write_time;
	projected.checkPoint = anchor.checkpoint;
	projected.checkPointCopy.redo = anchor.checkpoint_copy.redo;
	projected.checkPointCopy.ThisTimeLineID = anchor.checkpoint_copy.ThisTimeLineID;
	projected.checkPointCopy.PrevTimeLineID = anchor.checkpoint_copy.PrevTimeLineID;
	projected.checkPointCopy.fullPageWrites = anchor.checkpoint_copy.fullPageWrites;
	projected.checkPointCopy.time = anchor.checkpoint_copy.time;
	projected.checkPointCopy.oldestCommitTsXid = anchor.checkpoint_copy.oldestCommitTsXid;
	projected.checkPointCopy.newestCommitTsXid = anchor.checkpoint_copy.newestCommitTsXid;
	projected.unloggedLSN = anchor.unlogged_lsn;
	projected.minRecoveryPoint = anchor.min_recovery_point;
	projected.minRecoveryPointTLI = anchor.min_recovery_tli;
	projected.backupStartPoint = anchor.backup_start;
	projected.backupEndPoint = anchor.backup_end;
	projected.backupEndRequired = anchor.backup_end_required;
	INIT_CRC32C(projected.crc);
	COMP_CRC32C(projected.crc, &projected, offsetof(ControlFileData, crc));
	FIN_CRC32C(projected.crc);
	memcpy(out, &projected, sizeof(*out));
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
anchor_v2_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode)) && st->st_uid == geteuid()
		   && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

ClusterControlRootResult
cluster_recovery_anchor_v2_read_locked(const ClusterRecoveryAnchorRefV2 *ref,
									   const ControlFileData *common, ControlFileData *out)
{
	const int dirflags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	uint8 bytes[CLUSTER_RECOVERY_ANCHOR_SIZE];
	char thread[32], generation[48], name[112], hex[65];
	const char *parts[4];
	struct stat st;
	int dirs[5] = { -1, -1, -1, -1, -1 };
	int fd = -1;
	size_t i, used = 0;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (!anchor_v2_ref_valid(ref) || common == NULL || cluster_shared_data_dir == NULL
		|| cluster_shared_data_dir[0] == '\0')
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ShareLock)
		&& !cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	snprintf(thread, sizeof(thread), "thread_%u", ref->identity.origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 ref->identity.origin_owner_incarnation);
	for (i = 0; i < 32; ++i)
		snprintf(hex + i * 2, 3, "%02x", ref->anchor_sha256[i]);
	snprintf(name, sizeof(name), "anchor_" UINT64_FORMAT "-%s.bin", ref->anchor_generation, hex);
	parts[0] = "global";
	parts[1] = "anchor_images";
	parts[2] = thread;
	parts[3] = generation;
	/* No allocation, elog or interrupt processing with raw FDs open. Decode
	 * (including crypto allocation) occurs only after every descriptor closes.
	 */
	dirs[0] = open(cluster_shared_data_dir, dirflags);
	if (dirs[0] < 0 || fstat(dirs[0], &st) != 0 || !anchor_v2_owned(&st, true))
		goto done;
	for (i = 0; i < lengthof(parts); ++i) {
		dirs[i + 1] = openat(dirs[i], parts[i], dirflags);
		if (dirs[i + 1] < 0) {
			if (errno == ENOENT)
				result = CLUSTER_CONTROL_ROOT_ABSENT;
			goto done;
		}
		if (fstat(dirs[i + 1], &st) != 0 || !anchor_v2_owned(&st, true))
			goto done;
	}
	fd = openat(dirs[4], name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (fd < 0) {
		if (errno == ENOENT)
			result = CLUSTER_CONTROL_ROOT_ABSENT;
		goto done;
	}
	if (fstat(fd, &st) != 0 || !anchor_v2_owned(&st, false))
		goto done;
	if (st.st_size != sizeof(bytes)) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	while (used < sizeof(bytes)) {
		ssize_t n = read(fd, bytes + used, sizeof(bytes) - used);

		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto done;
		used += n;
	}
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	if (fd >= 0 && close(fd) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	for (i = 0; i < lengthof(dirs); ++i)
		if (dirs[i] >= 0 && close(dirs[i]) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_recovery_anchor_v2_project(bytes, sizeof(bytes), ref, common, out);
	return result;
}

/* Suffixes of the torn-safe file family next to the primary. */
#define ANCHOR_TMP_SUFFIX ".tmp"
#define ANCHOR_BAK_SUFFIX ".bak"
#define ANCHOR_BAK_TMP_SUFFIX ".bak.tmp"

/*
 * Boot-time adoption statics (startup process only; loaded once per boot
 * by cluster_recovery_anchor_load).
 */
static ClusterRecoveryAnchor boot_anchor;
static bool boot_anchor_active = false;

/*
 * build_anchor_path -- join cluster_shared_data_dir with this node's anchor
 * name plus `suffix` ("" for the primary) into the caller's buffer.
 * Returns false (empty buffer) when the shared root is unset, so callers
 * fail-closed rather than touch a bogus path.
 */
static bool
build_anchor_path(char *dst, size_t dstlen, const char *suffix)
{
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0') {
		if (dstlen > 0)
			dst[0] = '\0';
		return false;
	}
	snprintf(dst, dstlen, "%s/" CLUSTER_RECOVERY_ANCHOR_REL_FMT "%s", cluster_shared_data_dir,
			 cluster_node_id, suffix);
	return true;
}

/*
 * cluster_recovery_anchor_path / cluster_recovery_anchor_bak_path --
 * accessors returning a per-function static buffer (see header).  NULL when
 * the shared root is unset.
 */
const char *
cluster_recovery_anchor_path(void)
{
	static char path[MAXPGPATH];

	if (!build_anchor_path(path, sizeof(path), ""))
		return NULL;
	return path;
}

const char *
cluster_recovery_anchor_bak_path(void)
{
	static char path[MAXPGPATH];

	if (!build_anchor_path(path, sizeof(path), ANCHOR_BAK_SUFFIX))
		return NULL;
	return path;
}

/*
 * cluster_recovery_anchor_classify -- pure classification of one raw image.
 *
 *	CRC is checked first: a torn image cannot have its other fields
 *	trusted, so magic/version and identity are only examined once the CRC
 *	validates.  Both identity legs (system_identifier and node_id) are
 *	always enforced -- every consumer knows both expected values, and a
 *	restart authority must never be adopted on partial identity.
 */
ClusterRecoveryAnchorValidity
cluster_recovery_anchor_classify(const char *buf, size_t len, uint64 expected_sysid,
								 int32 expected_node)
{
	ClusterRecoveryAnchor ra;
	pg_crc32c crc;

	if (buf == NULL || len < sizeof(ClusterRecoveryAnchor))
		return CLUSTER_RA_INVALID_SHORT;

	memcpy(&ra, buf, sizeof(ra));

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, offsetof(ClusterRecoveryAnchor, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, ra.crc))
		return CLUSTER_RA_INVALID_CRC;

	if (ra.magic != CLUSTER_RECOVERY_ANCHOR_MAGIC || ra.version != CLUSTER_RECOVERY_ANCHOR_VERSION)
		return CLUSTER_RA_INVALID_MAGIC;

	if (ra.system_identifier != expected_sysid || ra.node_id != expected_node)
		return CLUSTER_RA_INVALID_IDENTITY;

	return CLUSTER_RA_VALID;
}

/*
 * read_image -- read one anchor image from `path` into `image`
 * (CLUSTER_RECOVERY_ANCHOR_SIZE bytes) and classify it.  A missing/short
 * file classifies as INVALID_SHORT so the caller treats it as unusable.
 */
static ClusterRecoveryAnchorValidity
read_image(const char *path, char *image, uint64 expected_sysid)
{
	int fd;
	int r;

	if (path == NULL)
		return CLUSTER_RA_INVALID_SHORT;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return CLUSTER_RA_INVALID_SHORT;

	r = read(fd, image, CLUSTER_RECOVERY_ANCHOR_SIZE);
	CloseTransientFile(fd);

	if (r != CLUSTER_RECOVERY_ANCHOR_SIZE)
		return CLUSTER_RA_INVALID_SHORT;

	return cluster_recovery_anchor_classify(image, CLUSTER_RECOVERY_ANCHOR_SIZE, expected_sysid,
											cluster_node_id);
}

/*
 * cluster_recovery_anchor_read -- load this node's anchor into *out.
 *
 *	Tries the primary first; on failure falls back to a .bak that passes
 *	the same strict classification (VALID under CRC + magic + identity --
 *	a merely-stale .bak is safe, see the file header).  Returns false
 *	(leaving *out untouched) when the read must fail-closed.  Never
 *	ereports; the caller raises FATAL 53RB3 on the restart path.
 */
bool
cluster_recovery_anchor_read(uint64 expected_sysid, ClusterRecoveryAnchor *out, bool *used_bak)
{
	char primary_img[CLUSTER_RECOVERY_ANCHOR_SIZE];
	char bak_img[CLUSTER_RECOVERY_ANCHOR_SIZE];
	char bak_path[MAXPGPATH];

	if (used_bak != NULL)
		*used_bak = false;

	if (read_image(cluster_recovery_anchor_path(), primary_img, expected_sysid)
		== CLUSTER_RA_VALID) {
		memcpy(out, primary_img, sizeof(ClusterRecoveryAnchor));
		return true;
	}

	if (!build_anchor_path(bak_path, sizeof(bak_path), ANCHOR_BAK_SUFFIX))
		return false;

	if (read_image(bak_path, bak_img, expected_sysid) == CLUSTER_RA_VALID) {
		memcpy(out, bak_img, sizeof(ClusterRecoveryAnchor));
		if (used_bak != NULL)
			*used_bak = true;
		return true;
	}

	return false;
}

/*
 * write_durable -- write `buf` (CLUSTER_RECOVERY_ANCHOR_SIZE bytes) to
 * `tmp`, fsync it, then durable_rename() it over `final` (which fsyncs the
 * directory).  PANICs on any I/O failure, mirroring the shared-authority
 * write contract (see the file header for why WARNING-and-continue is not
 * an option here).
 */
static void
write_durable(const char *tmp, const char *final, const char *buf)
{
	int fd;

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", tmp)));

	errno = 0;
	if (write(fd, buf, CLUSTER_RECOVERY_ANCHOR_SIZE) != CLUSTER_RECOVERY_ANCHOR_SIZE) {
		if (errno == 0)
			errno = ENOSPC;
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not write file \"%s\": %m", tmp)));
	}

	if (pg_fsync(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", tmp)));

	if (CloseTransientFile(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmp)));

	if (durable_rename(tmp, final, PANIC) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not rename file \"%s\" to \"%s\": %m", tmp, final)));
}

/*
 * roll_primary_to_bak -- if the primary currently exists (and is a full
 * image), copy its raw bytes into the .bak durably so a subsequent bad
 * primary write is recoverable.  A missing or short primary (first write)
 * is simply skipped.
 */
static void
roll_primary_to_bak(const char *primary, const char *bak, const char *baktmp)
{
	char buf[CLUSTER_RECOVERY_ANCHOR_SIZE];
	int fd;
	int r;

	fd = OpenTransientFile(primary, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return; /* first write: no prior primary to preserve */

	r = read(fd, buf, CLUSTER_RECOVERY_ANCHOR_SIZE);
	CloseTransientFile(fd);
	if (r != CLUSTER_RECOVERY_ANCHOR_SIZE)
		return; /* short/odd primary: don't manufacture a .bak */

	write_durable(baktmp, bak, buf);
}

/*
 * cluster_recovery_anchor_write -- atomically replace this node's anchor
 * with *ra.  Recomputes the CRC, rolls the live primary into .bak, then
 * installs the new image via temp-write + durable_rename.
 */
void
cluster_recovery_anchor_write(const ClusterRecoveryAnchor *ra)
{
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	char tmp[MAXPGPATH];
	char baktmp[MAXPGPATH];
	ClusterRecoveryAnchor local;

	if (!build_anchor_path(primary, sizeof(primary), "")
		|| !build_anchor_path(bak, sizeof(bak), ANCHOR_BAK_SUFFIX)
		|| !build_anchor_path(tmp, sizeof(tmp), ANCHOR_TMP_SUFFIX)
		|| !build_anchor_path(baktmp, sizeof(baktmp), ANCHOR_BAK_TMP_SUFFIX))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	/* Recompute CRC over a private copy (ra is const). */
	memcpy(&local, ra, sizeof(local));
	INIT_CRC32C(local.crc);
	COMP_CRC32C(local.crc, (char *)&local, offsetof(ClusterRecoveryAnchor, crc));
	FIN_CRC32C(local.crc);

	roll_primary_to_bak(primary, bak, baktmp);
	write_durable(tmp, primary, (const char *)&local);
	cluster_cf_counter_inc(CLUSTER_CF_RECOVERY_ANCHOR_WRITE);
}

/*
 * cluster_recovery_anchor_build_from_controlfile -- map a ControlFileData
 * snapshot onto an anchor image (seed path: before the control file is
 * migrated into the shared authority, the local pg_control still carries
 * this node's own checkpoint fields).  The CRC is left to the writer.
 */
void
cluster_recovery_anchor_build_from_controlfile(const ControlFileData *cf,
											   ClusterRecoveryAnchor *out)
{
	memset(out, 0, sizeof(*out));
	out->magic = CLUSTER_RECOVERY_ANCHOR_MAGIC;
	out->version = CLUSTER_RECOVERY_ANCHOR_VERSION;
	out->node_id = cluster_node_id;
	out->state = (uint32)cf->state;
	out->system_identifier = cf->system_identifier;
	out->checkPoint = cf->checkPoint;
	out->write_time = (pg_time_t)time(NULL);
	out->checkPointCopy = cf->checkPointCopy;
	out->unloggedLSN = cf->unloggedLSN;
}

static bool
checkpoint_phase4_publisher_is_current(uint64 self_incarnation)
{
	ClusterWalStateSlot slot;
	ClusterWalSlotVerdict verdict;
	TimestampTz phase4_started_at;
	TimestampTz stats_spawned_at;
	uint16 own_thread;

	/* STOP01's initial phase4 checkpoint is deliberately before ordinary
	 * membership admission.  Its sole boot-local proof is the exact Cluster
	 * Stats incarnation that published this node's ACTIVE WAL slot. */
	if (self_incarnation == 0 || cluster_current_phase() != CLUSTER_PHASE_4_NORMAL
		|| cluster_stats_status() != CLUSTER_STATS_SPAWNING
		|| !cluster_cf_held_is_clusterwide(ExclusiveLock))
		return false;

	own_thread = cluster_wal_thread_id();
	if (own_thread < XLP_THREAD_ID_FIRST_REAL || own_thread > CLUSTER_WAL_THREAD_MAX
		|| !cluster_wal_thread_dir_configured() || !cluster_wal_thread_dir_validated()
		|| cluster_wal_thread_dump_thread_id() != own_thread || !cluster_wal_state_registry_ready())
		return false;

	phase4_started_at = cluster_phase_started_at(CLUSTER_PHASE_4_NORMAL);
	stats_spawned_at = cluster_stats_spawned_at();
	if (phase4_started_at == 0 || stats_spawned_at == 0 || stats_spawned_at < phase4_started_at)
		return false;

	memset(&slot, 0, sizeof(slot));
	verdict = cluster_wal_state_read_slot(own_thread, &slot);
	return verdict == CLUSTER_WAL_SLOT_OK && slot.thread_id == own_thread
		   && slot.node_id == cluster_node_id && slot.state == CLUSTER_WAL_SLOT_STATE_ACTIVE
		   && slot.started_at == stats_spawned_at && slot.started_at >= phase4_started_at;
}

static bool
checkpoint_publisher_is_current(uint64 expected_sysid)
{
	uint64 self_incarnation;

	if (expected_sysid == 0 || cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return false;
	/* The delegated EOR checkpointer runs before steady membership admission;
	 * its existing boot-local OWNER handoff is the sole recovery-actor arm. */
	if (cluster_cf_owner_eor_local_active())
		return true;
	self_incarnation = cluster_qvotec_get_self_incarnation();
	/*
	 * The shared-catalog bootstrap contract ends with a normal shutdown
	 * checkpoint after the delegated EOR handoff has closed.  It deliberately
	 * runs outside formed-cluster membership, so admit only its exact native
	 * owner shape: cluster mode disabled, shared control authority enabled,
	 * one declared self node, no qvotec/admission identity, and a held local
	 * CF(X).  This restores the frozen clean-seed path without allowing a
	 * formed or stale member to use native bootstrap as a publication bypass.
	 */
	if (!cluster_enabled && cluster_controlfile_shared_authority && self_incarnation == 0
		&& cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_ABSENT
		&& cluster_membership_get_last_admitted_incarnation(cluster_node_id) == 0
		&& cluster_cf_exactly_one_declared_node() && cluster_cf_held(ExclusiveLock))
		return true;
	if (self_incarnation != 0
		&& cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_MEMBER
		&& cluster_membership_get_last_admitted_incarnation(cluster_node_id) == self_incarnation)
		return true;
	return checkpoint_phase4_publisher_is_current(self_incarnation);
}

/*
 * cluster_recovery_anchor_publish_checkpoint -- write hook #1 (spec-5.6a
 * D2): publish this node's just-logged checkpoint as the new anchor.  The
 * caller (CreateCheckPoint) invokes this after UpdateControlFile and
 * before this cycle's WAL recycling, so the invariant "the anchor's redo
 * point lies within retained WAL" holds across any crash window.
 */
void
cluster_recovery_anchor_publish_checkpoint(XLogRecPtr checkpoint_lsn,
										   const CheckPoint *checkpoint_copy, uint64 sysid,
										   uint32 state, XLogRecPtr unlogged_lsn)
{
	ClusterRecoveryAnchor ra;

	/* RF-ROOT P5: the legacy source anchor remains selected until the R4
	 * bit22 OPEN cutover, but its checkpoint bypass may no longer publish for
	 * a fenced, excluded, or superseded node incarnation.  CreateCheckPoint
	 * calls this while its outer coordinated CF(X) is held.
	 *
	 * RF-ROOT P7 G1b (Stage 8 contract / follow-up): the frozen P5
	 * intent is SKIP when fenced — not publishing the anchor is the safe
	 * direction (publishing is what would be dangerous), and this check
	 * runs BEFORE any write, so there is nothing half-done to roll back.
	 * The previous reject_if_fenced PANICed inside the caller's critical
	 * section, contradicting the comment above; a site-4 pin reordering
	 * (pre-IR pinned projection) first exposed the contradiction at
	 * shutdown.  Skip with a LOG instead; the restart then takes the
	 * ordinary crash-rejoin chain (fail-closed). */
	if (!cluster_write_fence_allowed()) {
		ereport(LOG, (errmsg("cluster recovery anchor: checkpoint publication skipped "
							 "(this node is write-fenced); restart takes the ordinary "
							 "crash-rejoin chain")));
		return;
	}
	if (!checkpoint_publisher_is_current(sysid))
		ereport(PANIC,
				(errmsg("stale cluster member cannot publish a recovery checkpoint anchor")));

	memset(&ra, 0, sizeof(ra));
	ra.magic = CLUSTER_RECOVERY_ANCHOR_MAGIC;
	ra.version = CLUSTER_RECOVERY_ANCHOR_VERSION;
	ra.node_id = cluster_node_id;
	ra.state = state;
	ra.system_identifier = sysid;
	ra.checkPoint = checkpoint_lsn;
	ra.write_time = (pg_time_t)time(NULL);
	ra.checkPointCopy = *checkpoint_copy;
	ra.unloggedLSN = unlogged_lsn;

	cluster_recovery_anchor_write(&ra);

	/* A concurrent exclusion discovered after durable publication must stop
	 * this checkpoint before its caller can recycle WAL. */
	cluster_write_fence_reject_if_fenced("recovery anchor checkpoint post-publication");
	if (!checkpoint_publisher_is_current(sysid))
		ereport(PANIC, (errmsg("cluster recovery checkpoint publisher changed before WAL reuse")));
}

/*
 * cluster_recovery_anchor_refresh_state -- write hook #2 (spec-5.6a D2):
 * flip an existing valid anchor's state, keeping its checkpoint fields.
 * Without this, a clean shutdown followed by a restart that writes WAL and
 * then crashes would be misread as a clean shutdown on the next boot (the
 * anchor would still say DB_SHUTDOWNED), skipping crash recovery -- the
 * same lost-write hazard vanilla prevents by setting DB_IN_PRODUCTION in
 * pg_control at startup.
 *
 * A missing or invalid anchor is a no-op returning false: creation
 * happens only at the checkpoint hook and the seed path, and an invalid
 * leftover is either rewritten by the imminent first checkpoint or caught
 * fail-closed (53RB3) at the next label-less boot.
 */
bool
cluster_recovery_anchor_refresh_state(uint64 expected_sysid, uint32 state)
{
	ClusterRecoveryAnchor ra;

	if (!cluster_recovery_anchor_read(expected_sysid, &ra, NULL))
		return false;

	ra.state = state;
	ra.write_time = (pg_time_t)time(NULL);
	cluster_recovery_anchor_write(&ra);
	return true;
}

/*
 * cluster_recovery_anchor_load / _active / _get -- boot-time adoption
 * (spec-5.6a D3).  The startup process loads the anchor once, right after
 * the shared-authority bootstrap window, and the restart consumption
 * points read the process-local copy.  No shmem, no EXEC_BACKEND face:
 * only the startup process consumes these.
 */
bool
cluster_recovery_anchor_load(uint64 expected_sysid, bool *used_bak)
{
	if (!cluster_recovery_anchor_read(expected_sysid, &boot_anchor, used_bak)) {
		boot_anchor_active = false;
		return false;
	}
	boot_anchor_active = true;
	cluster_cf_counter_inc(CLUSTER_CF_RECOVERY_ANCHOR_BOOT_ADOPT);
	return true;
}

bool
cluster_recovery_anchor_active(void)
{
	return boot_anchor_active;
}

const ClusterRecoveryAnchor *
cluster_recovery_anchor_get(void)
{
	return boot_anchor_active ? &boot_anchor : NULL;
}
