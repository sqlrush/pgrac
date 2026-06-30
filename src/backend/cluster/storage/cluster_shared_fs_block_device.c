/*-------------------------------------------------------------------------
 *
 * cluster_shared_fs_block_device.c
 *	  spec-6.0a raw block-device ClusterSharedFs backend.
 *
 *	  Production shared-storage provider for an O_DIRECT-capable raw block
 *	  device or regular-file test image.  The provider maintains a compact
 *	  on-device layout (superblock, free bitmap, directory, extent-slot
 *	  table) and exposes logical relation files through ClusterSharedFsOps.
 *	  Metadata updates are serialized and WAL-logged; data writes never
 *	  silently fall back when required durability/fencing settings are
 *	  missing.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_shared_fs_block_device.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The block_device backend is compiled only with --enable-cluster
 *	  (USE_PGRAC_CLUSTER defined).
 *
 *	  Spec: spec-6.0a-production-shared-storage-backend-matrix.md
 *	  (FROZEN, provider framework + raw block_device backend).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/storage/cluster_raw_xlog.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#ifdef USE_PGRAC_CLUSTER

#define CLUSTER_RAW_LAYOUT_MAGIC 0x5052574CU /* PRWL */
#define CLUSTER_RAW_LAYOUT_VERSION 1
#define CLUSTER_RAW_EXTENT_SIZE (1024 * 1024)
#define CLUSTER_RAW_BLOCKS_PER_EXTENT (CLUSTER_RAW_EXTENT_SIZE / BLCKSZ)
#define CLUSTER_RAW_SUPER_EXTENT 0
#define CLUSTER_RAW_BITMAP_EXTENT 1
#define CLUSTER_RAW_DIR_EXTENT 2
#define CLUSTER_RAW_DATA_START_EXTENT 3
#define CLUSTER_RAW_BITMAP_MAX_EXTENTS (CLUSTER_RAW_EXTENT_SIZE * BITS_PER_BYTE)
#define CLUSTER_RAW_DIR_REGION_BYTES (128 * 1024)
#define CLUSTER_RAW_ENTRY_IN_USE 0x00000001U
#define CLUSTER_RAW_SLOT_IN_USE 0x00000001U
#define CLUSTER_RAW_INVALID_SLOT PG_UINT64_MAX
#define CLUSTER_RAW_LAYOUT_RESID_TYPE 0xF3

StaticAssertDecl(CLUSTER_RAW_EXTENT_SIZE % BLCKSZ == 0,
				 "raw extent size must be a whole number of BLCKSZ blocks");
StaticAssertDecl(CLUSTER_RAW_LAYOUT_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "raw layout resid namespace must not collide with any PG LockTagType");

static const ClusterSharedFsCaps cluster_shared_fs_block_device_caps = {
	.supports_odirect = true,
	.required_io_alignment = PG_IO_ALIGN_SIZE,
	.supports_scsi3_pr = false,
	.durability_class = CLUSTER_DURABILITY_ODIRECT_BARRIER,
	.max_nodes = CLUSTER_MAX_NODES,
};

typedef struct ClusterRawSuperblock {
	uint32 magic;
	uint32 layout_version;
	uint32 block_size;
	uint32 extent_size;
	uint64 total_extents;
	uint64 free_map_extent;
	uint64 dir_root_extent;
	char storage_uuid[CLUSTER_SHARED_UUID_LEN];
	uint8 _pad[3];
	pg_crc32c crc;
} ClusterRawSuperblock;

typedef struct ClusterRawDirEntry {
	uint32 spcOid;
	uint32 dbOid;
	uint32 relNumber;
	int16 forknum;
	uint16 n_extents;
	uint32 logical_nblocks;
	uint64 first_extent;
	uint32 flags;
	uint8 _pad[28];
} ClusterRawDirEntry;

typedef struct ClusterRawExtentSlot {
	uint32 data_extent;
	uint32 next_slot;
	uint32 flags;
	uint32 _pad;
} ClusterRawExtentSlot;

typedef struct RawLayoutLock {
	bool held;
	bool coordinated;
	ClusterLockAcquireRequest req;
} RawLayoutLock;

struct ClusterSharedFsHandle {
	RelFileLocator rlocator;
	ForkNumber forknum;
	uint32 entry_index;
};

StaticAssertDecl(sizeof(ClusterRawSuperblock) <= BLCKSZ,
				 "raw superblock must fit in one metadata page");
StaticAssertDecl(sizeof(ClusterRawDirEntry) == 64, "raw dir entry ABI must stay 64 bytes");
StaticAssertDecl(sizeof(ClusterRawExtentSlot) == 16, "raw extent slot ABI must stay 16 bytes");

static File cluster_raw_device_file = -1;
static uint64 cluster_raw_total_extents = 0;

#define CLUSTER_RAW_DIR_MAX_ENTRIES (CLUSTER_RAW_DIR_REGION_BYTES / sizeof(ClusterRawDirEntry))
#define CLUSTER_RAW_SLOT_REGION_OFF CLUSTER_RAW_DIR_REGION_BYTES
#define CLUSTER_RAW_SLOT_MAX                                                                       \
	((CLUSTER_RAW_EXTENT_SIZE - CLUSTER_RAW_SLOT_REGION_OFF) / sizeof(ClusterRawExtentSlot))

static uint64
raw_extent_offset(uint64 extent)
{
	return extent * (uint64)CLUSTER_RAW_EXTENT_SIZE;
}

static uint64
raw_bitmap_page_offset(uint32 extent, Size *byte_off, uint8 *mask)
{
	uint64 bit_byte = extent / 8;

	*byte_off = (Size)(bit_byte % BLCKSZ);
	*mask = (uint8)(1U << (extent % 8));
	return raw_extent_offset(CLUSTER_RAW_BITMAP_EXTENT) + (bit_byte / BLCKSZ) * BLCKSZ;
}

static uint64
raw_dir_entry_offset(uint32 index, Size *page_off)
{
	uint64 off
		= raw_extent_offset(CLUSTER_RAW_DIR_EXTENT) + (uint64)index * sizeof(ClusterRawDirEntry);

	*page_off = (Size)(off % BLCKSZ);
	return off - *page_off;
}

static uint64
raw_slot_offset(uint32 index, Size *page_off)
{
	uint64 off = raw_extent_offset(CLUSTER_RAW_DIR_EXTENT) + CLUSTER_RAW_SLOT_REGION_OFF
				 + (uint64)index * sizeof(ClusterRawExtentSlot);

	*page_off = (Size)(off % BLCKSZ);
	return off - *page_off;
}

static pg_crc32c
raw_super_crc(const ClusterRawSuperblock *super)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, super, offsetof(ClusterRawSuperblock, crc));
	FIN_CRC32C(crc);
	return crc;
}

static bool
raw_page_all_zero(const char *page)
{
	int i;

	for (i = 0; i < BLCKSZ; i++) {
		if (page[i] != '\0')
			return false;
	}
	return true;
}

static void
raw_read_page(uint64 offset, PGIOAlignedBlock *page)
{
	int nbytes;

	if (cluster_raw_device_file < 0 || offset % BLCKSZ != 0)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_STORAGE_IO_ALIGNMENT),
						errmsg("raw layout read offset is not BLCKSZ-aligned")));

	nbytes = FileRead(cluster_raw_device_file, page->data, BLCKSZ, (off_t)offset,
					  WAIT_EVENT_DATA_FILE_READ);
	if (nbytes < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read raw layout page at offset " UINT64_FORMAT ": %m", offset)));
	if (nbytes != BLCKSZ)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("short read of raw layout page at offset " UINT64_FORMAT, offset),
						errdetail("Read %d bytes, expected %d.", nbytes, BLCKSZ)));
}

static void
raw_write_page(uint64 offset, const char *image, bool wal_log)
{
	PGIOAlignedBlock io;
	XLogRecPtr lsn = InvalidXLogRecPtr;
	int nbytes;

	if (cluster_raw_device_file < 0 || image == NULL || offset % BLCKSZ != 0)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_STORAGE_IO_ALIGNMENT),
						errmsg("raw layout write image or offset is invalid")));

	if (wal_log)
		lsn = cluster_raw_layout_emit_write(offset, image);
	if (wal_log && XLogRecPtrIsInvalid(lsn))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_SHARED_STORAGE_FAILED),
						errmsg("raw layout metadata write could not be WAL-logged")));
	if (!XLogRecPtrIsInvalid(lsn))
		XLogFlush(lsn);

	memcpy(io.data, image, BLCKSZ);
	nbytes = FileWrite(cluster_raw_device_file, io.data, BLCKSZ, (off_t)offset,
					   WAIT_EVENT_DATA_FILE_WRITE);
	if (nbytes < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write raw layout page at offset " UINT64_FORMAT ": %m",
							   offset)));
	if (nbytes != BLCKSZ)
		ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
						errmsg("short write of raw layout page at offset " UINT64_FORMAT, offset),
						errdetail("Wrote %d bytes, expected %d.", nbytes, BLCKSZ)));
}

static void
raw_read_dir_entry(uint32 index, ClusterRawDirEntry *entry)
{
	PGIOAlignedBlock page;
	Size page_off;
	uint64 page_offset;

	if (index >= CLUSTER_RAW_DIR_MAX_ENTRIES)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("raw directory index %u is out of range", index)));

	page_offset = raw_dir_entry_offset(index, &page_off);
	raw_read_page(page_offset, &page);
	memcpy(entry, page.data + page_off, sizeof(*entry));
}

static void
raw_write_dir_entry(uint32 index, const ClusterRawDirEntry *entry)
{
	PGIOAlignedBlock page;
	Size page_off;
	uint64 page_offset;

	if (index >= CLUSTER_RAW_DIR_MAX_ENTRIES)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("raw directory index %u is out of range", index)));

	page_offset = raw_dir_entry_offset(index, &page_off);
	raw_read_page(page_offset, &page);
	memcpy(page.data + page_off, entry, sizeof(*entry));
	raw_write_page(page_offset, page.data, true);
}

static void
raw_read_slot(uint32 index, ClusterRawExtentSlot *slot)
{
	PGIOAlignedBlock page;
	Size page_off;
	uint64 page_offset;

	if (index >= CLUSTER_RAW_SLOT_MAX)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("raw extent slot index %u is out of range", index)));

	page_offset = raw_slot_offset(index, &page_off);
	raw_read_page(page_offset, &page);
	memcpy(slot, page.data + page_off, sizeof(*slot));
}

static void
raw_write_slot(uint32 index, const ClusterRawExtentSlot *slot)
{
	PGIOAlignedBlock page;
	Size page_off;
	uint64 page_offset;

	if (index >= CLUSTER_RAW_SLOT_MAX)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("raw extent slot index %u is out of range", index)));

	page_offset = raw_slot_offset(index, &page_off);
	raw_read_page(page_offset, &page);
	memcpy(page.data + page_off, slot, sizeof(*slot));
	raw_write_page(page_offset, page.data, true);
}

static bool
raw_extent_allocated(uint32 extent)
{
	PGIOAlignedBlock page;
	Size byte_off;
	uint8 mask;
	uint64 page_offset;

	if (extent >= cluster_raw_total_extents)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED), errmsg("raw extent %u is out of range", extent)));

	page_offset = raw_bitmap_page_offset(extent, &byte_off, &mask);
	raw_read_page(page_offset, &page);
	return (page.data[byte_off] & mask) != 0;
}

static void
raw_set_extent_allocated(uint32 extent, bool allocated)
{
	PGIOAlignedBlock page;
	Size byte_off;
	uint8 mask;
	uint64 page_offset;

	if (extent >= cluster_raw_total_extents)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED), errmsg("raw extent %u is out of range", extent)));

	page_offset = raw_bitmap_page_offset(extent, &byte_off, &mask);
	raw_read_page(page_offset, &page);
	if (allocated)
		page.data[byte_off] |= mask;
	else
		page.data[byte_off] &= ~mask;
	raw_write_page(page_offset, page.data, true);
}

static uint32
raw_allocate_extent(void)
{
	uint32 extent;

	for (extent = CLUSTER_RAW_DATA_START_EXTENT; extent < cluster_raw_total_extents; extent++) {
		if (!raw_extent_allocated(extent)) {
			raw_set_extent_allocated(extent, true);
			return extent;
		}
	}

	ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
					errmsg("raw block-device layout has no free data extents")));
	return 0;
}

static uint32
raw_allocate_slot(uint32 data_extent)
{
	uint32 index;
	ClusterRawExtentSlot slot;

	for (index = 0; index < CLUSTER_RAW_SLOT_MAX; index++) {
		raw_read_slot(index, &slot);
		if ((slot.flags & CLUSTER_RAW_SLOT_IN_USE) == 0) {
			memset(&slot, 0, sizeof(slot));
			slot.data_extent = data_extent;
			slot.next_slot = UINT32_MAX;
			slot.flags = CLUSTER_RAW_SLOT_IN_USE;
			raw_write_slot(index, &slot);
			return index;
		}
	}

	ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					errmsg("raw block-device layout extent-slot table is full")));
	return 0;
}

static void
raw_release_slot_chain(uint64 first_slot)
{
	uint64 cur = first_slot;

	while (cur != CLUSTER_RAW_INVALID_SLOT) {
		ClusterRawExtentSlot slot;
		uint32 data_extent;
		uint64 next;

		if (cur >= CLUSTER_RAW_SLOT_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("raw extent chain references invalid slot " UINT64_FORMAT, cur)));
		raw_read_slot((uint32)cur, &slot);
		if ((slot.flags & CLUSTER_RAW_SLOT_IN_USE) == 0)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("raw extent chain references free slot " UINT64_FORMAT, cur)));

		next = slot.next_slot == UINT32_MAX ? CLUSTER_RAW_INVALID_SLOT : slot.next_slot;
		data_extent = slot.data_extent;
		memset(&slot, 0, sizeof(slot));
		raw_write_slot((uint32)cur, &slot);
		raw_set_extent_allocated(data_extent, false);
		cur = next;
	}
}

static bool
raw_entry_matches(const ClusterRawDirEntry *entry, RelFileLocator rlocator, ForkNumber forknum)
{
	return (entry->flags & CLUSTER_RAW_ENTRY_IN_USE) != 0
		   && entry->spcOid == (uint32)rlocator.spcOid && entry->dbOid == (uint32)rlocator.dbOid
		   && entry->relNumber == (uint32)rlocator.relNumber && entry->forknum == (int16)forknum;
}

static bool
raw_find_dir_entry(RelFileLocator rlocator, ForkNumber forknum, uint32 *entry_index,
				   ClusterRawDirEntry *entry, uint32 *free_index)
{
	uint32 index;
	uint32 first_free = UINT32_MAX;

	for (index = 0; index < CLUSTER_RAW_DIR_MAX_ENTRIES; index++) {
		ClusterRawDirEntry cur;

		raw_read_dir_entry(index, &cur);
		if (raw_entry_matches(&cur, rlocator, forknum)) {
			if (entry_index != NULL)
				*entry_index = index;
			if (entry != NULL)
				*entry = cur;
			if (free_index != NULL)
				*free_index = first_free;
			return true;
		}
		if (first_free == UINT32_MAX && (cur.flags & CLUSTER_RAW_ENTRY_IN_USE) == 0)
			first_free = index;
	}

	if (free_index != NULL)
		*free_index = first_free;
	return false;
}

static void
raw_resid_encode(ClusterResId *dst)
{
	memset(dst, 0, sizeof(*dst));
	dst->type = CLUSTER_RAW_LAYOUT_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

static bool
raw_layout_lock(RawLayoutLock *lock)
{
	int fd;
	ClusterLockAcquireResult r;

	memset(lock, 0, sizeof(*lock));

	if (!cluster_conf_has_peers() || MyProc == NULL) {
		fd = FileGetRawDesc(cluster_raw_device_file);
		if (fd < 0)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not access raw block device for layout lock: %m")));
		if (flock(fd, LOCK_EX) != 0)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not lock raw block device layout: %m")));
		lock->held = true;
		lock->coordinated = false;
		return true;
	}

	raw_resid_encode(&lock->req.resid);
	lock->req.lockmode = ExclusiveLock;
	lock->req.op = CLUSTER_LOCK_OP_REQUEST;
	lock->req.current_mode = NoLock;
	lock->req.lockmethod_id = DEFAULT_LOCKMETHOD;
	lock->req.dontwait = false;
	lock->req.sessionLock = false;
	lock->req.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	lock->req.wait_event = WAIT_EVENT_CLUSTER_REL_EXTEND_WAIT;

	r = cluster_lock_acquire_seven_step(&lock->req);
	if (r == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK || r == CLUSTER_LOCK_ACQUIRE_OK_GRANTED
		|| r == CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
		if (cluster_lock_acquire_s5_promote(&lock->req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return false;
		lock->held = true;
		lock->coordinated = true;
		return true;
	}

	return false;
}

static void
raw_layout_unlock(RawLayoutLock *lock)
{
	int fd;

	if (!lock->held)
		return;

	if (lock->coordinated)
		(void)cluster_lock_acquire_s6_release(&lock->req);
	else {
		fd = FileGetRawDesc(cluster_raw_device_file);
		if (fd >= 0 && flock(fd, LOCK_UN) != 0)
			ereport(WARNING, (errcode_for_file_access(),
							  errmsg("could not unlock raw block device layout: %m")));
	}

	lock->held = false;
	lock->coordinated = false;
}

static void
raw_load_super(ClusterRawSuperblock *super, bool *valid, bool *all_zero)
{
	PGIOAlignedBlock page;

	raw_read_page(0, &page);
	*all_zero = raw_page_all_zero(page.data);
	memcpy(super, page.data, sizeof(*super));

	*valid = false;
	if (*all_zero)
		return;
	if (super->magic != CLUSTER_RAW_LAYOUT_MAGIC)
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("raw block device has an unrecognized layout superblock")));
	if (super->layout_version != CLUSTER_RAW_LAYOUT_VERSION || super->block_size != BLCKSZ
		|| super->extent_size != CLUSTER_RAW_EXTENT_SIZE)
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("raw block device layout version or geometry is incompatible")));
	if (super->crc != raw_super_crc(super))
		ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("raw block device layout superblock CRC mismatch")));

	*valid = true;
}

static void
raw_initialize_layout(uint64 total_extents)
{
	PGIOAlignedBlock page;
	ClusterRawSuperblock super;
	Size byte_off;
	uint8 mask;
	uint32 extent;

	memset(&page, 0, sizeof(page));
	for (extent = 0; extent < CLUSTER_RAW_DATA_START_EXTENT; extent++) {
		(void)raw_bitmap_page_offset(extent, &byte_off, &mask);
		page.data[byte_off] |= mask;
	}
	raw_write_page(raw_extent_offset(CLUSTER_RAW_BITMAP_EXTENT), page.data, false);

	memset(&super, 0, sizeof(super));
	super.magic = CLUSTER_RAW_LAYOUT_MAGIC;
	super.layout_version = CLUSTER_RAW_LAYOUT_VERSION;
	super.block_size = BLCKSZ;
	super.extent_size = CLUSTER_RAW_EXTENT_SIZE;
	super.total_extents = total_extents;
	super.free_map_extent = CLUSTER_RAW_BITMAP_EXTENT;
	super.dir_root_extent = CLUSTER_RAW_DIR_EXTENT;
	if (cluster_shared_storage_uuid != NULL && cluster_shared_storage_uuid[0] != '\0')
		strlcpy(super.storage_uuid, cluster_shared_storage_uuid, sizeof(super.storage_uuid));
	else
		strlcpy(super.storage_uuid, "raw-block-device", sizeof(super.storage_uuid));
	super.crc = raw_super_crc(&super);

	memset(&page, 0, sizeof(page));
	memcpy(page.data, &super, sizeof(super));
	raw_write_page(0, page.data, false);

	if (FileSync(cluster_raw_device_file, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not fsync initialized raw block device layout: %m")));
}

static void
raw_ensure_layout(void)
{
	off_t size;
	uint64 total_extents;
	ClusterRawSuperblock super;
	bool valid;
	bool all_zero;
	RawLayoutLock lock;

	size = FileSize(cluster_raw_device_file);
	if (size < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not determine raw block device size: %m")));
	if (size < (off_t)(CLUSTER_RAW_DATA_START_EXTENT * CLUSTER_RAW_EXTENT_SIZE))
		ereport(FATAL,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("raw block device is too small for the pgrac layout"),
				 errdetail("Size is " INT64_FORMAT " bytes; minimum is %u bytes.", (int64)size,
						   CLUSTER_RAW_DATA_START_EXTENT * CLUSTER_RAW_EXTENT_SIZE)));

	total_extents = (uint64)size / CLUSTER_RAW_EXTENT_SIZE;
	if (total_extents > CLUSTER_RAW_BITMAP_MAX_EXTENTS)
		ereport(FATAL, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("raw block device is too large for layout v1 bitmap"),
						errdetail("Device has " UINT64_FORMAT " extents; maximum is %u.",
								  total_extents, CLUSTER_RAW_BITMAP_MAX_EXTENTS)));
	if (total_extents > UINT32_MAX)
		ereport(FATAL, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("raw block device has too many extents for layout v1")));
	cluster_raw_total_extents = total_extents;

	if (!raw_layout_lock(&lock))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_SHARED_STORAGE_FAILED),
						errmsg("could not prove exclusive ownership of raw layout metadata")));

	PG_TRY();
	{
		raw_load_super(&super, &valid, &all_zero);
		if (!valid) {
			if (!all_zero)
				ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("raw block device superblock is not zeroed")));
			raw_initialize_layout(total_extents);
		} else {
			if (super.total_extents > total_extents)
				ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("raw block device is smaller than recorded layout")));
			cluster_raw_total_extents = super.total_extents;
		}
	}
	PG_FINALLY();
	{
		raw_layout_unlock(&lock);
	}
	PG_END_TRY();
}

static uint64
raw_slot_for_ordinal(const ClusterRawDirEntry *entry, uint32 ordinal, ClusterRawExtentSlot *slot)
{
	uint64 cur;
	uint32 i;

	if ((entry->flags & CLUSTER_RAW_ENTRY_IN_USE) == 0 || ordinal >= entry->n_extents)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("raw extent ordinal %u is outside relation mapping", ordinal)));

	cur = entry->first_extent;
	for (i = 0; i <= ordinal; i++) {
		if (cur >= CLUSTER_RAW_SLOT_MAX)
			ereport(
				ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("raw relation extent chain references invalid slot " UINT64_FORMAT, cur)));
		raw_read_slot((uint32)cur, slot);
		if ((slot->flags & CLUSTER_RAW_SLOT_IN_USE) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("raw relation extent chain references free slot " UINT64_FORMAT, cur)));
		if (i == ordinal)
			return cur;
		cur = slot->next_slot == UINT32_MAX ? CLUSTER_RAW_INVALID_SLOT : slot->next_slot;
	}

	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED), errmsg("raw relation extent chain ended early")));
	return CLUSTER_RAW_INVALID_SLOT;
}

static uint64
raw_block_offset(const ClusterRawDirEntry *entry, BlockNumber blocknum)
{
	uint32 ordinal = blocknum / CLUSTER_RAW_BLOCKS_PER_EXTENT;
	uint32 in_extent = blocknum % CLUSTER_RAW_BLOCKS_PER_EXTENT;
	ClusterRawExtentSlot slot;

	(void)raw_slot_for_ordinal(entry, ordinal, &slot);
	if (slot.data_extent >= cluster_raw_total_extents)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("raw relation maps to out-of-range data extent %u", slot.data_extent)));

	return raw_extent_offset(slot.data_extent) + (uint64)in_extent * BLCKSZ;
}

static void
raw_refresh_handle_entry(ClusterSharedFsHandle *handle, ClusterRawDirEntry *entry)
{
	if (handle == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("raw shared-fs handle is NULL")));
	raw_read_dir_entry(handle->entry_index, entry);
	if (!raw_entry_matches(entry, handle->rlocator, handle->forknum))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("raw shared-fs handle no longer matches directory entry")));
}

static void
raw_zero_data_block(const ClusterRawDirEntry *entry, BlockNumber blocknum)
{
	PGIOAlignedBlock zero;
	int nbytes;

	memset(&zero, 0, sizeof(zero));
	nbytes = FileWrite(cluster_raw_device_file, zero.data, BLCKSZ,
					   (off_t)raw_block_offset(entry, blocknum), WAIT_EVENT_DATA_FILE_WRITE);
	if (nbytes < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not zero raw relation block %u: %m", blocknum)));
	if (nbytes != BLCKSZ)
		ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
						errmsg("short zero write of raw relation block %u", blocknum)));
}

static void
raw_append_extent(ClusterRawDirEntry *entry)
{
	uint32 data_extent;
	uint32 new_slot;
	ClusterRawExtentSlot slot;

	if (entry->n_extents >= UINT16_MAX)
		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("raw relation extent count exceeds layout v1 limit")));

	data_extent = raw_allocate_extent();
	new_slot = raw_allocate_slot(data_extent);

	if (entry->n_extents == 0) {
		entry->first_extent = new_slot;
	} else {
		uint64 tail = raw_slot_for_ordinal(entry, entry->n_extents - 1, &slot);

		slot.next_slot = new_slot;
		raw_write_slot((uint32)tail, &slot);
	}
	entry->n_extents++;
}

static bool
cluster_shared_fs_block_device_exists(RelFileLocator rlocator, ForkNumber forknum)
{
	return raw_find_dir_entry(rlocator, forknum, NULL, NULL, NULL);
}

static void
cluster_shared_fs_block_device_open_existing(RelFileLocator rlocator, ForkNumber forknum,
											 ClusterSharedFsHandle **out_handle)
{
	ClusterSharedFsHandle *handle;
	uint32 entry_index;
	MemoryContext oldcxt;

	if (!raw_find_dir_entry(rlocator, forknum, &entry_index, NULL, NULL))
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("raw block-device relation %u/%u/%u fork %d does not exist",
							   rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, forknum)));

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	handle = (ClusterSharedFsHandle *)palloc0(sizeof(*handle));
	MemoryContextSwitchTo(oldcxt);
	handle->rlocator = rlocator;
	handle->forknum = forknum;
	handle->entry_index = entry_index;
	*out_handle = handle;
}

static void
cluster_shared_fs_block_device_create(RelFileLocator rlocator, ForkNumber forknum, bool isRedo,
									  ClusterSharedFsHandle **out_handle)
{
	RawLayoutLock lock;
	ClusterRawDirEntry entry;
	uint32 entry_index;
	uint32 free_index = UINT32_MAX;

	(void)isRedo;
	if (!raw_layout_lock(&lock))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_SHARED_STORAGE_FAILED),
						errmsg("could not acquire raw layout lock for create")));

	PG_TRY();
	{
		if (!raw_find_dir_entry(rlocator, forknum, &entry_index, &entry, &free_index)) {
			uint32 data_extent;
			uint32 slot;

			if (free_index == UINT32_MAX)
				ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
								errmsg("raw block-device directory is full")));

			data_extent = raw_allocate_extent();
			slot = raw_allocate_slot(data_extent);

			memset(&entry, 0, sizeof(entry));
			entry.spcOid = (uint32)rlocator.spcOid;
			entry.dbOid = (uint32)rlocator.dbOid;
			entry.relNumber = (uint32)rlocator.relNumber;
			entry.forknum = (int16)forknum;
			entry.n_extents = 1;
			entry.logical_nblocks = 0;
			entry.first_extent = slot;
			entry.flags = CLUSTER_RAW_ENTRY_IN_USE;
			entry_index = free_index;
			raw_write_dir_entry(entry_index, &entry);
		}
		if (FileSync(cluster_raw_device_file, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not barrier-sync raw layout create: %m")));
	}
	PG_FINALLY();
	{
		raw_layout_unlock(&lock);
	}
	PG_END_TRY();

	cluster_shared_fs_block_device_open_existing(rlocator, forknum, out_handle);
}

static void
cluster_shared_fs_block_device_close(ClusterSharedFsHandle *handle)
{
	if (handle != NULL)
		pfree(handle);
}

static int
cluster_shared_fs_block_device_read(ClusterSharedFsHandle *handle, BlockNumber blocknum, char *buf)
{
	ClusterRawDirEntry entry;
	PGIOAlignedBlock io;
	int nbytes;

	raw_refresh_handle_entry(handle, &entry);
	if (blocknum >= entry.logical_nblocks)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED), errmsg("raw block-device read past logical EOF"),
				 errdetail("block=%u logical_nblocks=%u", blocknum, entry.logical_nblocks)));

	nbytes = FileRead(cluster_raw_device_file, io.data, BLCKSZ,
					  (off_t)raw_block_offset(&entry, blocknum), WAIT_EVENT_DATA_FILE_READ);
	if (nbytes < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not read raw relation block %u: %m", blocknum)));
	if (nbytes != BLCKSZ)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("short read of raw relation block %u", blocknum)));
	memcpy(buf, io.data, BLCKSZ);
	return nbytes;
}

static int
cluster_shared_fs_block_device_write(ClusterSharedFsHandle *handle, BlockNumber blocknum,
									 const char *buf)
{
	ClusterRawDirEntry entry;
	PGIOAlignedBlock io;
	int nbytes;

	raw_refresh_handle_entry(handle, &entry);
	if (blocknum >= entry.logical_nblocks)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED), errmsg("raw block-device write past logical EOF"),
				 errdetail("block=%u logical_nblocks=%u", blocknum, entry.logical_nblocks)));

	memcpy(io.data, buf, BLCKSZ);
	nbytes = FileWrite(cluster_raw_device_file, io.data, BLCKSZ,
					   (off_t)raw_block_offset(&entry, blocknum), WAIT_EVENT_DATA_FILE_WRITE);
	if (nbytes < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write raw relation block %u: %m", blocknum)));
	if (nbytes != BLCKSZ)
		ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
						errmsg("short write of raw relation block %u", blocknum)));
	return nbytes;
}

static void
cluster_shared_fs_block_device_extend(ClusterSharedFsHandle *handle, BlockNumber blocknum)
{
	RawLayoutLock lock;
	ClusterRawDirEntry entry;
	uint32 needed_extents;
	BlockNumber blk;
	BlockNumber old_logical;

	if (blocknum == InvalidBlockNumber)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("raw block-device cannot extend to InvalidBlockNumber")));

	if (!raw_layout_lock(&lock))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_SHARED_STORAGE_FAILED),
						errmsg("could not acquire raw layout lock for extend")));

	PG_TRY();
	{
		raw_refresh_handle_entry(handle, &entry);
		if (blocknum >= entry.logical_nblocks) {
			needed_extents = blocknum / CLUSTER_RAW_BLOCKS_PER_EXTENT + 1;
			while (entry.n_extents < needed_extents)
				raw_append_extent(&entry);

			old_logical = entry.logical_nblocks;
			for (blk = old_logical; blk <= blocknum; blk++)
				raw_zero_data_block(&entry, blk);
			entry.logical_nblocks = blocknum + 1;
			raw_write_dir_entry(handle->entry_index, &entry);
			if (FileSync(cluster_raw_device_file, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not barrier-sync raw layout extend: %m")));
		}
	}
	PG_FINALLY();
	{
		raw_layout_unlock(&lock);
	}
	PG_END_TRY();
}

static BlockNumber
cluster_shared_fs_block_device_nblocks(ClusterSharedFsHandle *handle)
{
	ClusterRawDirEntry entry;

	raw_refresh_handle_entry(handle, &entry);
	return entry.logical_nblocks;
}

static void
cluster_shared_fs_block_device_truncate(ClusterSharedFsHandle *handle, BlockNumber nblocks)
{
	RawLayoutLock lock;
	ClusterRawDirEntry entry;
	uint32 keep_extents;

	if (!raw_layout_lock(&lock))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_SHARED_STORAGE_FAILED),
						errmsg("could not acquire raw layout lock for truncate")));

	PG_TRY();
	{
		ClusterRawExtentSlot tail_slot;
		uint64 release_first = CLUSTER_RAW_INVALID_SLOT;
		uint64 tail = CLUSTER_RAW_INVALID_SLOT;

		raw_refresh_handle_entry(handle, &entry);
		if (nblocks > entry.logical_nblocks)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("raw block-device truncate cannot extend logical EOF"),
					 errdetail("requested=%u logical_nblocks=%u", nblocks, entry.logical_nblocks)));

		keep_extents = nblocks == 0 ? 1 : ((nblocks - 1) / CLUSTER_RAW_BLOCKS_PER_EXTENT + 1);
		if (keep_extents > entry.n_extents)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("raw truncate target exceeds mapped extents")));

		if (keep_extents > 0 && keep_extents < entry.n_extents) {
			tail = raw_slot_for_ordinal(&entry, keep_extents - 1, &tail_slot);
			release_first = tail_slot.next_slot == UINT32_MAX ? CLUSTER_RAW_INVALID_SLOT
															  : tail_slot.next_slot;
		}

		entry.n_extents = keep_extents;
		entry.logical_nblocks = nblocks;
		raw_write_dir_entry(handle->entry_index, &entry);

		if (release_first != CLUSTER_RAW_INVALID_SLOT) {
			tail_slot.next_slot = UINT32_MAX;
			raw_write_slot((uint32)tail, &tail_slot);
			raw_release_slot_chain(release_first);
		}

		if (FileSync(cluster_raw_device_file, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not barrier-sync raw layout truncate: %m")));
	}
	PG_FINALLY();
	{
		raw_layout_unlock(&lock);
	}
	PG_END_TRY();
}

static void
cluster_shared_fs_block_device_immedsync(ClusterSharedFsHandle *handle)
{
	(void)handle;
	if (FileSync(cluster_raw_device_file, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
		ereport(ERROR,
				(errcode_for_file_access(), errmsg("could not barrier-sync raw block device: %m")));
}

static void
cluster_shared_fs_block_device_unlink(RelFileLocator rlocator, ForkNumber forknum)
{
	RawLayoutLock lock;
	ClusterRawDirEntry entry;
	uint32 entry_index;

	if (!raw_layout_lock(&lock))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_SHARED_STORAGE_FAILED),
						errmsg("could not acquire raw layout lock for unlink")));

	PG_TRY();
	{
		if (raw_find_dir_entry(rlocator, forknum, &entry_index, &entry, NULL)) {
			uint64 first_slot = entry.first_extent;

			memset(&entry, 0, sizeof(entry));
			raw_write_dir_entry(entry_index, &entry);
			raw_release_slot_chain(first_slot);
			if (FileSync(cluster_raw_device_file, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0)
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not barrier-sync raw layout unlink: %m")));
		}
	}
	PG_FINALLY();
	{
		raw_layout_unlock(&lock);
	}
	PG_END_TRY();
}

static void
cluster_shared_fs_block_device_init(void)
{
	int flags = O_RDWR | PG_BINARY;

	if (cluster_block_device_path == NULL || cluster_block_device_path[0] == '\0')
		ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster.block_device_path must be set when "
							   "shared_storage_backend=block_device")));

	if (cluster_block_device_use_odirect) {
#if PG_O_DIRECT == 0
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_STORAGE_IO_ALIGNMENT),
						errmsg("PG_O_DIRECT is not supported on this platform")));
#else
		if (PG_IO_ALIGN_SIZE > BLCKSZ || BLCKSZ % PG_IO_ALIGN_SIZE != 0)
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_STORAGE_IO_ALIGNMENT),
							errmsg("BLCKSZ is not compatible with required direct-I/O alignment"),
							errdetail("BLCKSZ=%d PG_IO_ALIGN_SIZE=%d", BLCKSZ, PG_IO_ALIGN_SIZE)));
		flags |= PG_O_DIRECT;
#endif
	}

	if (cluster_storage_fence_driver == CLUSTER_STORAGE_FENCE_DRIVER_SCSI3_PR)
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_STORAGE_FENCE_UNAVAILABLE),
				 errmsg("SCSI-3 persistent reservation fencing is not available"),
				 errhint("Use cluster.storage_fence_driver=auto or disabled until a platform "
						 "SCSI-3 PR driver is installed.")));

	cluster_raw_device_file = PathNameOpenFile(cluster_block_device_path, flags);
	if (cluster_raw_device_file < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open raw block device \"%s\": %m", cluster_block_device_path)));

	raw_ensure_layout();
	elog(LOG, "cluster_shared_fs: raw block_device backend attached to \"%s\"",
		 cluster_block_device_path);
}

static void
cluster_shared_fs_block_device_shutdown(void)
{
	if (cluster_raw_device_file >= 0) {
		FileClose(cluster_raw_device_file);
		cluster_raw_device_file = -1;
	}
}

static int
cluster_shared_fs_block_device_barrier_sync(ClusterSharedFsHandle *handle)
{
	cluster_shared_fs_block_device_immedsync(handle);
	return 0;
}

static int
cluster_shared_fs_block_device_register_fence_key(int node_id)
{
	(void)node_id;
	if (cluster_storage_fence_driver == CLUSTER_STORAGE_FENCE_DRIVER_SCSI3_PR)
		return EOPNOTSUPP;
	return EOPNOTSUPP;
}

static ClusterFenceCapability
cluster_shared_fs_block_device_fence_capability(void)
{
	return CLUSTER_FENCE_CAP_NONE;
}

const ClusterSharedFsOps cluster_shared_fs_block_device_ops = {
	.name = "block_device",
	.id = CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE,
	.caps = &cluster_shared_fs_block_device_caps,

	.exists = cluster_shared_fs_block_device_exists,
	.open_existing = cluster_shared_fs_block_device_open_existing,
	.create = cluster_shared_fs_block_device_create,
	.close = cluster_shared_fs_block_device_close,
	.read = cluster_shared_fs_block_device_read,
	.write = cluster_shared_fs_block_device_write,
	.extend = cluster_shared_fs_block_device_extend,
	.nblocks = cluster_shared_fs_block_device_nblocks,
	.truncate = cluster_shared_fs_block_device_truncate,
	.immedsync = cluster_shared_fs_block_device_immedsync,
	.unlink = cluster_shared_fs_block_device_unlink,

	.init = cluster_shared_fs_block_device_init,
	.shutdown = cluster_shared_fs_block_device_shutdown,

	.barrier_sync = cluster_shared_fs_block_device_barrier_sync,
	.register_fence_key = cluster_shared_fs_block_device_register_fence_key,
	.fence_capability = cluster_shared_fs_block_device_fence_capability,
};

#endif /* USE_PGRAC_CLUSTER */
