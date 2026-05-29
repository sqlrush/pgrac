/*-------------------------------------------------------------------------
 *
 * cluster_undo_record.c
 *	  pgrac record-level undo allocator + reader (spec-3.7 D5).
 *
 *	  Stage 3 第 11 sub-spec — record-level API on top of spec-3.4b 已
 *	  ship segment-level allocator(`cluster_undo_alloc.c`).
 *
 *	  Public APIs(declared in cluster_undo_record_api.h):
 *	    - cluster_undo_record_alloc()  — write one undo record + durable flush
 *	    - cluster_undo_get_record()    — read one undo record by UBA
 *	    - cluster_undo_record_shmem_register/size/init
 *	    - cluster_undo_record_xact_reset()  — end-of-xact hook
 *	    - cluster_undo_record_is_touched()  — for D16 PREPARE guard
 *
 *	  Concurrency model(MVP):
 *	    - single per-instance LWLock(`cursor_lock`)protects active segment
 *	      cursor advance(active_segment_id / current_block / free_offset /
 *	      slot_count).多 backend 写同 instance → serialize on this lock.
 *	    - per-backend `cluster_undo_touched` static bool tracks D16 state;
 *	      reset on xact end.
 *
 *	  Block boundary contract(per spec-3.7 §3.2):record 不跨 block。
 *	  block 不够时 advance 到下一 block;segment 不够时 ereport 53R9D
 *	  fail-closed(caller 在 critical section 之前调用).
 *
 *	  Durable ordering(per spec-3.7 §3.4 W2 self-contained):
 *	  cluster_undo_record_alloc() 返回 non-InvalidUba 前必须 undo block
 *	  bytes 已 fsync 到 shared storage。
 *
 *	  File I/O 模式:复用 `cluster_undo_alloc.c` 既有 pattern
 *	  (BasicOpenFile + pg_pwrite + pg_fsync);spec-3.7 Step 5 D7 后续把
 *	  block-level I/O 抽象到 cluster_undo_smgr.c。
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4 +
 *       Hardening v1.0.1 H-1/H-2)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_record.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "miscadmin.h"
#include "access/xact.h"
#include "port/atomics.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_format.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_alloc.h"

#include "access/xlog.h" /* GetXLogWriteRecPtr */

/* Local helper to express InvalidUba sentinel (all-zero UBA). */
static const UBA InvalidUbaVal = InvalidUba_init;
#define InvalidUba (InvalidUbaVal)


/*
 * ClusterUndoRecordShared -- per-instance shmem state for record-level
 *	cursor advance + counters.
 *
 *	Layout(packed,~96B + LWLock):
 *	  - active_segment_id : 0 means no active segment yet (lazy claim)
 *	  - current_block     : data block index within active segment (block 0 is segment header, so data starts at 1)
 *	  - free_offset       : free byte offset within current_block
 *	  - slot_count        : record count in current_block
 *	  - block_first_scn   : SCN of first record in current_block
 *	  - block_dirty       : 1 if current_block has data not yet block_write_count'd
 *	  - 5 atomic counters
 *	  - cursor_lock       : LWLock protect cursor advance
 */
typedef struct ClusterUndoRecordShared {
	uint32 active_segment_id;
	uint32 current_block;
	uint32 free_offset;
	uint16 slot_count;
	uint16 block_dirty;
	SCN block_first_scn;
	uint32 _pad_align;

	pg_atomic_uint64 record_alloc_count;
	pg_atomic_uint64 segment_claim_count;
	pg_atomic_uint64 block_write_count;
	pg_atomic_uint64 block_flush_count;
	pg_atomic_uint64 reader_lookup_count;

	LWLockPadded cursor_lock;
} ClusterUndoRecordShared;


static ClusterUndoRecordShared *UndoRecordShared = NULL;

/* Per-backend state(D16 PREPARE guard support). */
static bool cluster_undo_touched_in_xact = false;

/*
 * Backend-local latest undo head per TT slot.
 *
 * spec-3.7 v0.4 says TTSlot.first_undo_block is logically the latest/head
 * undo UBA.  Existing spec-3.4b DML callers still pass the TT-only UBA as
 * `prev_uba` on every operation, so the record allocator must maintain the
 * true per-xact undo chain locally until later specs persist the TT head in
 * the undo segment header.  This state is reset at xact end together with
 * cluster_undo_touched_in_xact.
 */
typedef struct ClusterUndoLocalHead {
	uint16 tt_slot_segment_id;
	uint16 tt_slot_offset;
	UBA head;
} ClusterUndoLocalHead;

#define CLUSTER_UNDO_LOCAL_HEAD_MAX 1024
static ClusterUndoLocalHead cluster_undo_local_heads[CLUSTER_UNDO_LOCAL_HEAD_MAX];
static uint32 cluster_undo_local_head_count = 0;


static int
cluster_undo_local_head_find(uint16 tt_slot_segment_id, uint16 tt_slot_offset)
{
	uint32 i;

	for (i = 0; i < cluster_undo_local_head_count; i++) {
		if (cluster_undo_local_heads[i].tt_slot_segment_id == tt_slot_segment_id
			&& cluster_undo_local_heads[i].tt_slot_offset == tt_slot_offset)
			return (int)i;
	}
	return -1;
}


static bool
cluster_undo_local_head_ensure(uint16 tt_slot_segment_id, uint16 tt_slot_offset, int *out_idx)
{
	int idx = cluster_undo_local_head_find(tt_slot_segment_id, tt_slot_offset);

	if (idx >= 0) {
		*out_idx = idx;
		return true;
	}
	if (cluster_undo_local_head_count >= CLUSTER_UNDO_LOCAL_HEAD_MAX)
		return false;

	idx = (int)cluster_undo_local_head_count++;
	cluster_undo_local_heads[idx].tt_slot_segment_id = tt_slot_segment_id;
	cluster_undo_local_heads[idx].tt_slot_offset = tt_slot_offset;
	cluster_undo_local_heads[idx].head = InvalidUba;
	*out_idx = idx;
	return true;
}


/*
 * cluster_undo_record_shmem_size
 *	  Bytes required by the record-level cursor + counter shmem region.
 */
Size
cluster_undo_record_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterUndoRecordShared));
}


/*
 * cluster_undo_record_shmem_init
 *	  Postmaster-once shmem layout + initial values.  Called via the
 *	  ClusterShmemRegion.init_fn callback during postmaster shmem
 *	  attachment.
 */
void
cluster_undo_record_shmem_init(void)
{
	bool found;

	UndoRecordShared
		= ShmemInitStruct("ClusterUndoRecordShared", cluster_undo_record_shmem_size(), &found);

	if (!found) {
		int tranche_id = LWLockNewTrancheId();

		memset(UndoRecordShared, 0, sizeof(*UndoRecordShared));
		UndoRecordShared->active_segment_id = 0;
		UndoRecordShared->current_block = 0;
		UndoRecordShared->free_offset = 0;
		UndoRecordShared->slot_count = 0;
		UndoRecordShared->block_dirty = 0;
		UndoRecordShared->block_first_scn = InvalidScn;

		pg_atomic_init_u64(&UndoRecordShared->record_alloc_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->segment_claim_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->block_write_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->block_flush_count, 0);
		pg_atomic_init_u64(&UndoRecordShared->reader_lookup_count, 0);

		LWLockInitialize(&UndoRecordShared->cursor_lock.lock, tranche_id);
		LWLockRegisterTranche(tranche_id, "cluster_undo_record_cursor");
	}
}


/*
 * ClusterShmemRegion descriptor + registration entry.
 */
static const ClusterShmemRegion cluster_undo_record_region = {
	.name = "pgrac cluster undo record cursor",
	.size_fn = cluster_undo_record_shmem_size,
	.init_fn = cluster_undo_record_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_undo_record",
	.reserved_flags = 0,
};

void
cluster_undo_record_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_undo_record_region);
}


/* ---- Helper: compute record total length per record_type ---- */

static inline uint16
undo_record_total_length(uint8 record_type, uint16 payload_len)
{
	return (uint16)(sizeof(UndoRecordHeader) + payload_len);
}


/* ---- Helper: read/write undo block via BasicOpenFile ---- */

static int
open_segment_fd(uint32 segment_id, uint8 owner_instance)
{
	char path[MAXPGPATH];
	int ret;
	int fd;

	ret = cluster_undo_path_resolve(owner_instance, segment_id, path, sizeof(path));
	if (ret != 0)
		return -1;

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	return fd;
}


static bool
read_undo_block(uint32 segment_id, uint8 owner_instance, uint32 block_no, char *buf)
{
	int fd;
	off_t offset;
	ssize_t nread;
	bool ok;

	fd = open_segment_fd(segment_id, owner_instance);
	if (fd < 0)
		return false;

	offset = (off_t)block_no * BLCKSZ;
	nread = pg_pread(fd, buf, BLCKSZ, offset);
	ok = (nread == BLCKSZ);

	close(fd);
	return ok;
}


static bool
write_undo_block(uint32 segment_id, uint8 owner_instance, uint32 block_no, const char *buf,
				 bool do_fsync)
{
	int fd;
	off_t offset;
	ssize_t nwritten;
	bool ok = true;

	fd = open_segment_fd(segment_id, owner_instance);
	if (fd < 0)
		return false;

	offset = (off_t)block_no * BLCKSZ;
	nwritten = pg_pwrite(fd, buf, BLCKSZ, offset);
	if (nwritten != BLCKSZ)
		ok = false;

	if (ok && do_fsync) {
		if (pg_fsync(fd) != 0)
			ok = false;
	}

	close(fd);
	return ok;
}


/*
 * cluster_undo_record_alloc -- record-level allocator main API.
 *
 *	Implementation outline:
 *	  1. Claim active segment if none(via existing segment-level API)
 *	  2. Lock cursor
 *	  3. Compute record total length;reject 53R9D if > inline_max GUC
 *	  4. If current block doesn't fit:
 *	     a. Flush current block (already happens per-record via fsync)
 *	     b. Advance current_block += 1, reset cursor
 *	     c. If exceed segment data blocks → 53R9D segment exhaustion
 *	  5. Read current block from segment file(or zeroed if first record)
 *	  6. Construct UndoRecordHeader + copy payload
 *	  7. Place record at free_offset;append slot dir at end
 *	  8. Update UndoBlockHeader(slot_count++, free_offset += rec_len)
 *	  9. Write block back to file
 *	  10. fsync segment file (durable per W2)
 *	  11. Counter bumps
 *	  12. Unlock cursor
 *	  13. Mark backend touched
 *	  14. Encode UBA and return
 */
UBA
cluster_undo_record_alloc(uint8 record_type, const ClusterUndoRecordTarget *target,
						  uint16 tt_slot_segment_id, uint16 tt_slot_offset, const void *payload,
						  uint16 payload_len, UBA prev_uba)
{
	UBA result;
	uint16 record_length;
	uint16 inline_cap;
	uint8 owner_instance;
	uint32 segment_id;
	uint32 current_block;
	uint32 free_offset;
	uint16 slot_count;
	uint16 new_slot_idx;
	char block_buf[BLCKSZ];
	UndoBlockHeader *blkhdr;
	UndoRecordHeader *rechdr;
	UndoSlotDirEntry *slot;
	SCN current_scn;
	uint32 ensured_segment_id;
	int local_head_idx;
	UBA effective_prev_uba;
	bool first_in_tx;

	/* Input validation. */
	if (record_type == UNDO_RECORD_INVALID || record_type > UNDO_RECORD_ITL)
		return InvalidUba;
	if (target == NULL || payload == NULL)
		return InvalidUba;

	if (UndoRecordShared == NULL)
		return InvalidUba; /* shmem not initialized */

	record_length = undo_record_total_length(record_type, payload_len);

	/* Enforce inline cap GUC. */
	inline_cap = (uint16)cluster_undo_record_inline_max_bytes;
	if (payload_len > inline_cap)
		return InvalidUba; /* caller ereport 53R9D oversize */

	/* Hard cap (must fit in single block). */
	if (record_length > UNDO_RECORD_HARD_CAP_BYTES)
		return InvalidUba;

	/* Owner instance is current node_id + 1 (per cluster_undo_alloc.c
	 * convention: owner_instance is 1-indexed). */
	owner_instance = (uint8)(cluster_node_id + 1);

	/*
	 * Segment creation may ereport(ERROR), emit WAL, and perform filesystem I/O.
	 * Do it before taking the record cursor LWLock; otherwise an ERROR would
	 * leave the LWLock held and wedge all future undo writers in this backend.
	 */
	ensured_segment_id = cluster_undo_active_segment_for_node_or_create(cluster_node_id);
	if (ensured_segment_id == 0)
		return InvalidUba;

	if (!cluster_undo_local_head_ensure(tt_slot_segment_id, tt_slot_offset, &local_head_idx))
		return InvalidUba;
	effective_prev_uba = cluster_undo_local_heads[local_head_idx].head;
	if (UBA_is_invalid(effective_prev_uba) && !UBA_is_invalid(prev_uba)) {
		uint32 prev_segment;
		uint32 prev_block;
		uint16 prev_tt_off;
		uint16 prev_row_off;

		/* Accept caller-supplied prev_uba only if it points to an actual undo
		 * record.  TT-only UBAs from spec-3.4b have block_no == 0 and must not
		 * be written into the undo chain. */
		if (uba_decode(prev_uba, &prev_segment, &prev_block, &prev_tt_off, &prev_row_off)
			&& prev_block != 0)
			effective_prev_uba = prev_uba;
	}
	first_in_tx = UBA_is_invalid(effective_prev_uba);

	/* SCN stamp for write_scn (advance Lamport). */
	current_scn = cluster_scn_advance();

	LWLockAcquire(&UndoRecordShared->cursor_lock.lock, LW_EXCLUSIVE);

	/* Publish active segment into shared cursor state if this is the first writer. */
	segment_id = UndoRecordShared->active_segment_id;
	if (segment_id == 0) {
		segment_id = ensured_segment_id;
		UndoRecordShared->active_segment_id = segment_id;
		UndoRecordShared->current_block = 1; /* block 0 is segment header */
		UndoRecordShared->free_offset = sizeof(UndoBlockHeader);
		UndoRecordShared->slot_count = 0;
		UndoRecordShared->block_dirty = 0;
		UndoRecordShared->block_first_scn = current_scn;
		pg_atomic_fetch_add_u64(&UndoRecordShared->segment_claim_count, 1);
	}

	current_block = UndoRecordShared->current_block;
	free_offset = UndoRecordShared->free_offset;
	slot_count = UndoRecordShared->slot_count;

	/* Check block fit;  advance to next block if needed. */
	if (!cluster_undo_block_has_space(free_offset, slot_count, record_length)) {
		/* Advance to next block. */
		current_block++;
		if (current_block >= UNDO_BLOCKS_PER_SEGMENT) {
			/* Segment exhausted — fail-closed.  spec-3.8 lifecycle
			 * will autoextend; for now caller gets 53R9D. */
			LWLockRelease(&UndoRecordShared->cursor_lock.lock);
			return InvalidUba;
		}
		UndoRecordShared->current_block = current_block;
		UndoRecordShared->free_offset = sizeof(UndoBlockHeader);
		UndoRecordShared->slot_count = 0;
		UndoRecordShared->block_first_scn = current_scn;
		free_offset = sizeof(UndoBlockHeader);
		slot_count = 0;
	}

	/* Read current block (or zeroed if first write to this block). */
	if (slot_count == 0) {
		/* Fresh block — init zeroed buffer + header. */
		memset(block_buf, 0, BLCKSZ);
		blkhdr = (UndoBlockHeader *)block_buf;
		blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
		blkhdr->block_version = UNDO_BLOCK_VERSION_1;
		blkhdr->slot_count = 0;
		blkhdr->free_offset = sizeof(UndoBlockHeader);
		blkhdr->first_change_scn = current_scn;
		blkhdr->first_change_lsn = GetXLogWriteRecPtr();
		blkhdr->crc64 = 0;
	} else {
		if (!read_undo_block(segment_id, owner_instance, current_block, block_buf)) {
			LWLockRelease(&UndoRecordShared->cursor_lock.lock);
			return InvalidUba; /* I/O fail */
		}
		blkhdr = (UndoBlockHeader *)block_buf;
	}

	/* Construct UndoRecordHeader at free_offset. */
	rechdr = (UndoRecordHeader *)(block_buf + free_offset);
	memset(rechdr, 0, sizeof(UndoRecordHeader));
	rechdr->record_type = record_type;
	rechdr->flags = first_in_tx ? UNDO_REC_FLAG_FIRST_IN_TX : 0;
	rechdr->payload_length = payload_len;
	rechdr->xid = GetCurrentTransactionIdIfAny();
	rechdr->origin_node_id = (uint16)cluster_node_id;
	rechdr->tt_slot_segment_id = tt_slot_segment_id;
	rechdr->tt_slot_id = cluster_tt_slot_offset_to_id(tt_slot_offset);
	rechdr->write_scn = current_scn;
	rechdr->prev_uba = effective_prev_uba;
	rechdr->target_locator = target->locator;
	rechdr->target_fork = target->forknum;
	rechdr->target_block = target->blockno;
	rechdr->target_offset = target->offnum;

	/* Copy op-specific payload bytes after the header. */
	memcpy(block_buf + free_offset + sizeof(UndoRecordHeader), payload, payload_len);

	/* Append slot directory entry. */
	new_slot_idx = slot_count;
	slot = UNDO_SLOT_DIR_PTR(block_buf, new_slot_idx);
	slot->record_offset = free_offset;
	slot->record_length = record_length;
	slot->record_type = record_type;
	slot->flags = rechdr->flags;

	/* Update block header. */
	blkhdr->slot_count = (uint16)(slot_count + 1);
	blkhdr->free_offset = free_offset + record_length;

	/* Write block back to file. */
	if (!write_undo_block(segment_id, owner_instance, current_block, block_buf,
						  /* do_fsync = */ true)) {
		LWLockRelease(&UndoRecordShared->cursor_lock.lock);
		return InvalidUba; /* I/O fail */
	}

	pg_atomic_fetch_add_u64(&UndoRecordShared->block_write_count, 1);
	pg_atomic_fetch_add_u64(&UndoRecordShared->block_flush_count, 1);

	/* Advance shared cursor. */
	UndoRecordShared->free_offset = free_offset + record_length;
	UndoRecordShared->slot_count = (uint16)(slot_count + 1);
	UndoRecordShared->block_dirty = 0; /* just flushed */

	pg_atomic_fetch_add_u64(&UndoRecordShared->record_alloc_count, 1);

	LWLockRelease(&UndoRecordShared->cursor_lock.lock);

	/* Mark backend touched for D16 PREPARE guard. */
	cluster_undo_touched_in_xact = true;

	/* Encode UBA per spec-3.4b: (segment_id, block_no, tt_slot_offset, row_offset).
	 * For undo records, row_offset = slot-dir index within block. */
	result = uba_encode((uint32)segment_id, current_block, tt_slot_offset, new_slot_idx);
	cluster_undo_local_heads[local_head_idx].head = result;

	return result;
}


/*
 * cluster_undo_get_record -- sanity reader, own-instance only at spec-3.7.
 */
size_t
cluster_undo_get_record(UBA uba, void *out_buffer, size_t buffer_size)
{
	uint32 segment_id, block_no;
	uint16 tt_slot_offset, row_offset;
	char block_buf[BLCKSZ];
	const UndoBlockHeader *blkhdr;
	const UndoSlotDirEntry *slot;
	uint8 owner_instance;
	size_t copy_bytes;

	if (UndoRecordShared == NULL)
		return 0;

	if (!uba_decode(uba, &segment_id, &block_no, &tt_slot_offset, &row_offset))
		return 0;

	/* Map segment_id back to owner_instance.  Per spec-3.4b convention:
	 *   owner_instance = (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1 */
	owner_instance = (uint8)((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE + 1);

	/* Own-instance only at spec-3.7. */
	if (owner_instance != (uint8)(cluster_node_id + 1)) {
		ereport(WARNING,
				(errmsg("cluster_undo_get_record: cross-instance read not supported at spec-3.7"),
				 errhint("cross-instance undo read 推 spec-3.9 CR construction / Cache Fusion")));
		return 0;
	}

	if (!read_undo_block(segment_id, owner_instance, block_no, block_buf))
		return 0;

	blkhdr = (UndoBlockHeader *)block_buf;
	if (blkhdr->magic != PGRAC_UNDO_BLOCK_MAGIC)
		return 0;

	if (row_offset >= blkhdr->slot_count)
		return 0;

	slot = UNDO_SLOT_DIR_PTR(block_buf, row_offset);
	if (slot->record_offset == 0 || slot->record_length == 0)
		return 0;

	if (buffer_size < slot->record_length)
		return 0;

	copy_bytes = slot->record_length;
	memcpy(out_buffer, block_buf + slot->record_offset, copy_bytes);

	pg_atomic_fetch_add_u64(&UndoRecordShared->reader_lookup_count, 1);

	return copy_bytes;
}


/*
 * cluster_undo_record_xact_reset -- end-of-xact hook;  reset per-backend
 *	touched flag.  Called from xact.c CommitTransaction / AbortTransaction.
 */
void
cluster_undo_record_xact_reset(void)
{
	cluster_undo_touched_in_xact = false;
	memset(cluster_undo_local_heads, 0, sizeof(cluster_undo_local_heads));
	cluster_undo_local_head_count = 0;
}


/*
 * cluster_undo_record_is_touched -- D16 PREPARE TRANSACTION guard helper.
 */
bool
cluster_undo_record_is_touched(void)
{
	return cluster_undo_touched_in_xact;
}


/* ---- Counter accessors (for emit_row / TAP verification) ---- */

uint64
cluster_undo_record_alloc_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->record_alloc_count);
}

uint64
cluster_undo_segment_claim_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->segment_claim_count);
}

uint64
cluster_undo_block_write_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->block_write_count);
}

uint64
cluster_undo_block_flush_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->block_flush_count);
}

uint64
cluster_undo_reader_lookup_count(void)
{
	if (UndoRecordShared == NULL)
		return 0;
	return pg_atomic_read_u64(&UndoRecordShared->reader_lookup_count);
}
