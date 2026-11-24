/* Original BOOT file I/O. No WAL, registry or ROOT publication authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif
#include <fcntl.h>
#include <sys/stat.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include "access/clog_file.h"
#include "common/cluster_recovery_bootstrap.h"
#include "cluster/cluster_undo_segment_init.h"
#include "port/pg_crc32c.h"
#include "sha2_int.h"

#ifndef WIN32
static bool
boot_overlap(const void *a, Size na, const void *b, Size nb)
{
	uintptr_t x = (uintptr_t)a;
	uintptr_t y = (uintptr_t)b;

	return a != NULL && b != NULL && (x <= y ? y - x < na : x - y < nb);
}

static bool
boot_identity(const ClusterRecoveryFileIdentityV2 *id, ClusterRecoveryFileKindV2 kind,
			  ClusterRecoveryBootFileV2 *out)
{
	uint8 bytes[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];

	return id != NULL && out != NULL && !boot_overlap(id, sizeof(*id), out, sizeof(*out))
		&& id->kind == kind && id->object_id == 0 && id->database_oid == 0
		&& id->tablespace_oid == 0
		&& cluster_recovery_file_identity_encode_v2(id, bytes, sizeof(bytes));
}

/* The original creator owns continuous FD/I/O exclusion. fstat bounds a
 * readback; neither emptiness nor an inode supplies file creation authority. */
static bool
boot_fd(int fd, bool writing, uint64 size, struct stat *st)
{
	int flags = fcntl(fd, F_GETFL);

	return flags >= 0 && (flags & O_APPEND) == 0
		&& (writing ? (flags & O_ACCMODE) == O_RDWR : (flags & O_ACCMODE) != O_WRONLY)
		&& fstat(fd, st) == 0 && S_ISREG(st->st_mode) && st->st_size >= 0
		&& (uint64)st->st_size == size;
}

static bool
boot_same_stat(int fd, const struct stat *before)
{
	struct stat after;

	return fstat(fd, &after) == 0 && S_ISREG(after.st_mode)
		&& before->st_dev == after.st_dev && before->st_ino == after.st_ino
		&& before->st_size == after.st_size
#ifdef __APPLE__
		&& before->st_mtimespec.tv_sec == after.st_mtimespec.tv_sec
		&& before->st_mtimespec.tv_nsec == after.st_mtimespec.tv_nsec
		&& before->st_ctimespec.tv_sec == after.st_ctimespec.tv_sec
		&& before->st_ctimespec.tv_nsec == after.st_ctimespec.tv_nsec
#else
		&& before->st_mtim.tv_sec == after.st_mtim.tv_sec
		&& before->st_mtim.tv_nsec == after.st_mtim.tv_nsec
		&& before->st_ctim.tv_sec == after.st_ctim.tv_sec
		&& before->st_ctim.tv_nsec == after.st_ctim.tv_nsec
#endif
		;
}

static bool
boot_write(int fd, const void *bytes, Size size, off_t offset)
{
	ssize_t n;

	do { n = pg_pwrite(fd, bytes, size, offset); } while (n < 0 && errno == EINTR);
	/* A short write is a failed original creation, never a resumable file. */
	return n == (ssize_t)size;
}

static bool
boot_read(int fd, void *bytes, Size size, off_t offset)
{
	ssize_t n;

	do { n = pg_pread(fd, bytes, size, offset); } while (n < 0 && errno == EINTR);
	return n == (ssize_t)size;
}

static bool
boot_sync(int fd)
{
	int rc;

	do { rc = fsync(fd); } while (rc < 0 && errno == EINTR);
	return rc == 0;
}

static bool
boot_clog_images(const ClusterRecoveryFileIdentityV2 *id, FullTransactionId next,
				 ClusterClogFileBindingV2 *binding, uint8 header[8192], uint8 frame[8256])
{
	ClusterClogPageIdentityV2 page;
	uint8 payload[CLUSTER_CLOG_PAGE_BYTES] = {0};

	/* The original BootStrapXLOG initializer actually starts at epoch0/XID3.
	 * Accept its complete observation; never infer an epoch from a raw value. */
	if (U64FromFullTransactionId(next) != FirstNormalTransactionId)
		return false;
	memset(binding, 0, sizeof(*binding));
	binding->identity = *id;
	memset(&page, 0, sizeof(page));
	page.identity.page_generation = U64FromFullTransactionId(next);
	page.identity.birth_observed = true;
	page.identity.birth_full_xid = next;
	return cluster_clog_file_header_encode_v2(binding, header, 8192)
		&& cluster_clog_file_page_encode_v2(binding, &page, payload, sizeof(payload), frame, 8256);
}

static bool
boot_clog_read(int fd, const ClusterClogFileBindingV2 *binding,
			   const uint8 expected_header[8192], const uint8 expected_frame[8256],
			   bool initial, ClusterRecoveryBootFileV2 *out)
{
	uint8 header[8192], frame[8256], payload[8192];
	ClusterClogPageIdentityV2 page, birth;
	ClusterRecoveryBootFileV2 result;
	struct stat st;
	pg_sha256_ctx hash;

	if (!boot_fd(fd, false, sizeof(header) + sizeof(frame), &st)
		|| !boot_read(fd, header, sizeof(header), 0)
		|| !boot_read(fd, frame, sizeof(frame), sizeof(header))
		|| memcmp(header, expected_header, sizeof(header)) != 0
		|| (initial && memcmp(frame, expected_frame, sizeof(frame)) != 0)
		|| !cluster_clog_file_page_decode_v2(binding, 0, frame, sizeof(frame),
											 &page, payload, sizeof(payload))
		|| !cluster_clog_file_page_decode_v2(binding, 0, expected_frame, sizeof(frame),
											 &birth, payload, sizeof(payload))
		|| page.identity.page_generation != birth.identity.page_generation
		|| !FullTransactionIdEquals(page.identity.birth_full_xid, birth.identity.birth_full_xid)
		|| !boot_same_stat(fd, &st))
		return false;
	memset(&result, 0, sizeof(result));
	result.identity = binding->identity;
	result.page = page.identity;
	result.synced_bytes = st.st_size;
	pg_sha256_init(&hash);
	pg_sha256_update(&hash, header, sizeof(header));
	pg_sha256_update(&hash, frame, sizeof(frame));
	pg_sha256_final(&hash, result.image_digest);
	*out = result;
	return true;
}
#endif

bool
cluster_clog_bootstrap_v2(int fd, const ClusterRecoveryFileIdentityV2 *id,
						  FullTransactionId actual_initial_next, ClusterRecoveryBootFileV2 *out)
{
#ifndef WIN32
	ClusterClogFileBindingV2 binding;
	uint8 header[8192], frame[8256];
	struct stat st;

	if (!boot_identity(id, CLUSTER_RECOVERY_FILE_CLOG_V2, out)
		|| !boot_clog_images(id, actual_initial_next, &binding, header, frame)
		|| !boot_fd(fd, true, 0, &st)
		|| !boot_write(fd, header, sizeof(header), 0)
		|| !boot_write(fd, frame, sizeof(frame), sizeof(header))
		|| !boot_sync(fd))
		return false;
	return boot_clog_read(fd, &binding, header, frame, true, out);
#else
	(void)fd; (void)id; (void)actual_initial_next; (void)out;
	return false;
#endif
}

bool
cluster_clog_bootstrap_read_v2(int fd, const ClusterRecoveryFileIdentityV2 *id,
								 ClusterRecoveryBootFileV2 *out)
{
#ifndef WIN32
	ClusterClogFileBindingV2 binding;
	uint8 header[8192], frame[8256];

	if (!boot_identity(id, CLUSTER_RECOVERY_FILE_CLOG_V2, out)
		|| !boot_clog_images(id, FullTransactionIdFromEpochAndXid(0, FirstNormalTransactionId), &binding, header, frame))
		return false;
	return boot_clog_read(fd, &binding, header, frame, false, out);
#else
	(void)fd; (void)id; (void)out;
	return false;
#endif
}

#if defined(USE_PGRAC_CLUSTER) && !defined(WIN32)
static bool
boot_undo_image(const ClusterRecoveryFileIdentityV2 *id, PGAlignedBlock *block)
{
	if (id->origin != 0 || !cluster_undo_segment_make_boot_header_v2_bytes(block->data)
		|| !cluster_recovery_file_identity_encode_v2(id,
			(uint8 *)block->data + CLUSTER_TT_FILE_REFERENCE_OFFSET, CLUSTER_CLOG_FILE_REFERENCE_BYTES))
		return false;
	return cluster_undo_segment_header_seal(block->data);
}

static bool
boot_undo_read(int fd, const ClusterRecoveryFileIdentityV2 *id,
			   const PGAlignedBlock *block, ClusterRecoveryBootFileV2 *out)
{
	PGAlignedBlock observed;
	struct stat st;
	pg_sha256_ctx hash;
	ClusterRecoveryBootFileV2 result;
	pg_crc32c crc;
	static const uint8 zero[BLCKSZ] = {0};

	if (!boot_fd(fd, false, UNDO_SEGMENT_SIZE_BYTES, &st))
		return false;
	pg_sha256_init(&hash);
	for (off_t pos = 0; pos < UNDO_SEGMENT_SIZE_BYTES; pos += BLCKSZ) {
		if (!boot_read(fd, observed.data, BLCKSZ, pos)
			|| memcmp(observed.data, pos == 0 ? (const void *)block->data : (const void *)zero, BLCKSZ) != 0)
			return false;
		pg_sha256_update(&hash, (const uint8 *)observed.data, BLCKSZ);
	}
	if (!boot_same_stat(fd, &st))
		return false;
	memset(&result, 0, sizeof(result));
	result.identity = *id;
	/* TT0 has no transaction birth, epoch or coverage. Its complete block0
	 * checksum is sealed above; payload_crc describes the block0 bytes only. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, block->data, BLCKSZ);
	FIN_CRC32C(crc);
	result.page.payload_crc = (uint32)crc;
	result.synced_bytes = st.st_size;
	pg_sha256_final(&hash, result.image_digest);
	*out = result;
	return true;
}
#endif

bool
cluster_undo_bootstrap_v2(int fd, const ClusterRecoveryFileIdentityV2 *id,
						  ClusterRecoveryBootFileV2 *out)
{
#if defined(USE_PGRAC_CLUSTER) && !defined(WIN32)
	PGAlignedBlock block;
	ClusterRecoveryFileIdentityV2 captured;
	struct stat st;

	if (!boot_identity(id, CLUSTER_RECOVERY_FILE_UNDO_V2, out))
		return false;
	captured = *id;
	if (!boot_undo_image(&captured, &block) || !boot_fd(fd, true, 0, &st)
		|| !boot_write(fd, block.data, BLCKSZ, 0)
		|| ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES) != 0 || !boot_sync(fd))
		return false;
	return boot_undo_read(fd, &captured, &block, out);
#else
	(void)fd; (void)id; (void)out;
	return false;
#endif
}

bool
cluster_undo_bootstrap_read_v2(int fd, const ClusterRecoveryFileIdentityV2 *id,
								  ClusterRecoveryBootFileV2 *out)
{
#if defined(USE_PGRAC_CLUSTER) && !defined(WIN32)
	PGAlignedBlock block;
	ClusterRecoveryFileIdentityV2 captured;

	if (!boot_identity(id, CLUSTER_RECOVERY_FILE_UNDO_V2, out))
		return false;
	captured = *id;
	if (!boot_undo_image(&captured, &block))
		return false;
	return boot_undo_read(fd, &captured, &block, out);
#else
	(void)fd; (void)id; (void)out;
	return false;
#endif
}
