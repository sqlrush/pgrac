/*-------------------------------------------------------------------------
 *
 * cluster_hw_snapshot.c
 *	  HW (relation extend) cluster authority -- durable snapshot envelope
 *	  (spec-5.7 D3, §3.1b R1/R10/R12/R13).
 *
 *	  Serialise / deserialise+validate the unified identity envelope that
 *	  captures a master's hw_htab at a checkpoint (R1) or when an online
 *	  remaster adopts a dead master's shard (R9).  One envelope, one parse
 *	  and validate path (R12).  The recovery rebuild merges the snapshot with
 *	  the HW_RESERVE WAL tail (§3.1b R3); this file owns only the on-disk
 *	  envelope and (with D3 S3) the torn-safe durable file I/O.
 *
 *	  The pure serialize / deserialize / identity / supersede helpers carry no
 *	  PG-backend dependency (only CRC32C from libpgport), so they are unit
 *	  tested standalone (test_cluster_hw_snapshot).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hw_snapshot.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D3, §3.1b R1/R10/R12/R13)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "cluster/cluster_guc.h" /* cluster_shared_data_dir */
#include "cluster/cluster_hw_snapshot.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"

/*
 * Per-master snapshot lives under the shared data dir, beside the shared
 * pg_control authority, named with its owner node id so a survivor can read a
 * dead master's file:  <shared>/global/pg_hw_snapshot.<owner_node_id>.
 */
#define CLUSTER_HW_SNAPSHOT_REL_DIR "global"
#define CLUSTER_HW_SNAPSHOT_BASENAME "pg_hw_snapshot"

/*
 * cluster_hw_snapshot_serialized_size -- header (48) + 20*n entries + crc (4).
 */
size_t
cluster_hw_snapshot_serialized_size(uint32 n_entries)
{
	return (size_t)CLUSTER_HW_SNAPSHOT_HEADER_SIZE
		   + (size_t)n_entries * CLUSTER_HW_SNAPSHOT_ENTRY_SIZE + CLUSTER_HW_SNAPSHOT_CRC_SIZE;
}

/*
 * cluster_hw_snapshot_serialize -- lay the header, then the n_entries 20-byte
 * (resid, next_hwm) pairs, then a trailing CRC32C over everything before it.
 *
 *	The header struct and the entry struct have no padding (StaticAssert'd
 *	48 / 20), so a fully-initialised input serialises to a deterministic byte
 *	image -- the CRC cannot depend on uninitialised padding.
 */
size_t
cluster_hw_snapshot_serialize(const ClusterHwSnapshotHeader *hdr,
							  const ClusterHwSnapshotEntry *entries, char *buf, size_t buflen)
{
	size_t required;
	size_t crc_offset;
	pg_crc32c crc;

	Assert(hdr != NULL && buf != NULL);
	Assert(hdr->n_entries == 0 || entries != NULL);

	required = cluster_hw_snapshot_serialized_size(hdr->n_entries);
	if (buflen < required)
		return 0;

	memcpy(buf, hdr, CLUSTER_HW_SNAPSHOT_HEADER_SIZE);
	if (hdr->n_entries > 0)
		memcpy(buf + CLUSTER_HW_SNAPSHOT_HEADER_SIZE, entries,
			   (size_t)hdr->n_entries * CLUSTER_HW_SNAPSHOT_ENTRY_SIZE);

	crc_offset = required - CLUSTER_HW_SNAPSHOT_CRC_SIZE;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, crc_offset);
	FIN_CRC32C(crc);
	memcpy(buf + crc_offset, &crc, CLUSTER_HW_SNAPSHOT_CRC_SIZE);

	return required;
}

/*
 * cluster_hw_snapshot_deserialize -- validate then load.
 *
 *	Validation order is structural before cryptographic: a buffer too short to
 *	hold even an empty envelope, or a wrong magic/version, is rejected before
 *	any CRC work (a foreign byte order shows up as a wrong magic).  A declared
 *	entry count larger than the caller's capacity fails closed (R6) rather than
 *	silently truncating the authority.  Only once the structure checks out is
 *	the CRC recomputed and compared.
 */
ClusterHwSnapshotValidity
cluster_hw_snapshot_deserialize(const char *buf, size_t len, ClusterHwSnapshotHeader *hdr_out,
								ClusterHwSnapshotEntry *entries_out, uint32 max_entries)
{
	ClusterHwSnapshotHeader hdr;
	size_t required;
	size_t crc_offset;
	pg_crc32c crc;
	pg_crc32c stored;

	Assert(hdr_out != NULL);

	/* Need at least an empty envelope: header + trailing CRC. */
	if (buf == NULL || len < (size_t)CLUSTER_HW_SNAPSHOT_HEADER_SIZE + CLUSTER_HW_SNAPSHOT_CRC_SIZE)
		return CLUSTER_HW_SNAPSHOT_INVALID_SHORT;

	memcpy(&hdr, buf, CLUSTER_HW_SNAPSHOT_HEADER_SIZE);

	if (hdr.magic != CLUSTER_HW_SNAPSHOT_MAGIC || hdr.version != CLUSTER_HW_SNAPSHOT_VERSION)
		return CLUSTER_HW_SNAPSHOT_INVALID_MAGIC;

	/* Fail closed if the caller cannot hold the declared entries (R6). */
	if (hdr.n_entries > max_entries)
		return CLUSTER_HW_SNAPSHOT_INVALID_SHORT;

	required = cluster_hw_snapshot_serialized_size(hdr.n_entries);
	if (len < required)
		return CLUSTER_HW_SNAPSHOT_INVALID_SHORT;

	crc_offset = required - CLUSTER_HW_SNAPSHOT_CRC_SIZE;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, buf, crc_offset);
	FIN_CRC32C(crc);
	memcpy(&stored, buf + crc_offset, CLUSTER_HW_SNAPSHOT_CRC_SIZE);
	if (!EQ_CRC32C(crc, stored))
		return CLUSTER_HW_SNAPSHOT_INVALID_CRC;

	*hdr_out = hdr;
	if (hdr.n_entries > 0 && entries_out != NULL)
		memcpy(entries_out, buf + CLUSTER_HW_SNAPSHOT_HEADER_SIZE,
			   (size_t)hdr.n_entries * CLUSTER_HW_SNAPSHOT_ENTRY_SIZE);

	return CLUSTER_HW_SNAPSHOT_VALID;
}

/*
 * cluster_hw_snapshot_identity_ok -- cluster + owner + shard gate (R12).  A
 * zero expected_sysid skips only the cluster-identity leg (bootstrap before the
 * identity is known); owner and shard are always checked.
 */
bool
cluster_hw_snapshot_identity_ok(const ClusterHwSnapshotHeader *hdr, uint64 expected_sysid,
								uint32 expected_owner, uint32 expected_shard)
{
	Assert(hdr != NULL);

	if (expected_sysid != 0 && hdr->system_id != expected_sysid)
		return false;
	if (hdr->owner_node_id != expected_owner)
		return false;
	if (hdr->shard_partition != expected_shard)
		return false;
	return true;
}

/*
 * cluster_hw_snapshot_supersedes -- R10 generation pick: cand replaces cur iff
 * it is the SAME shard authority (system_id + owner + shard) with a strictly
 * greater generation.  A foreign identity never supersedes (mis-bound image);
 * an equal/lower generation never supersedes (stale).
 */
bool
cluster_hw_snapshot_supersedes(const ClusterHwSnapshotHeader *cand,
							   const ClusterHwSnapshotHeader *cur)
{
	Assert(cand != NULL && cur != NULL);

	if (cand->system_id != cur->system_id || cand->owner_node_id != cur->owner_node_id
		|| cand->shard_partition != cur->shard_partition)
		return false;

	return cand->generation > cur->generation;
}

/*
 * cluster_hw_snapshot_rebuild_value -- R3/R11 rebuild merge: max(current,
 * candidate).  Monotone raise; a lower candidate is a conservative hole, never
 * a regression (see header).
 */
BlockNumber
cluster_hw_snapshot_rebuild_value(BlockNumber current, BlockNumber candidate)
{
	return candidate > current ? candidate : current;
}


/* ============================================================
 * Durable torn-safe snapshot file I/O (R1/R2; mirror spec-5.6
 * cluster_cf_authority_write / _read).
 * ============================================================ */

/*
 * hw_snapshot_build_path -- join the shared data dir with the per-master
 * snapshot name (optionally the .tmp staging name).  Returns false (empty dst)
 * when the shared root is unset so callers fail closed.
 */
static bool
hw_snapshot_build_path(uint32 owner_node_id, bool tmp, char *dst, size_t dstlen)
{
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0') {
		if (dstlen > 0)
			dst[0] = '\0';
		return false;
	}
	snprintf(dst, dstlen, "%s/%s/%s.%u%s", cluster_shared_data_dir, CLUSTER_HW_SNAPSHOT_REL_DIR,
			 CLUSTER_HW_SNAPSHOT_BASENAME, owner_node_id, tmp ? ".tmp" : "");
	return true;
}

bool
cluster_hw_snapshot_path(uint32 owner_node_id, char *dst, size_t dstlen)
{
	return hw_snapshot_build_path(owner_node_id, false, dst, dstlen);
}

/*
 * hw_snapshot_ensure_dir -- make sure <shared_data_dir>/global exists before a
 * snapshot write.  pgrac-init creates it in production (like $PGDATA/global), but
 * the snapshot write is reached from a checkpoint whenever the HW authority is
 * active, so it must be self-sufficient rather than PANIC the checkpoint when the
 * directory was not pre-created.  Idempotent: EEXIST is success.
 */
static void
hw_snapshot_ensure_dir(void)
{
	char dir[MAXPGPATH];

	snprintf(dir, sizeof(dir), "%s/%s", cluster_shared_data_dir, CLUSTER_HW_SNAPSHOT_REL_DIR);
	if (MakePGDirectory(dir) < 0 && errno != EEXIST)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not create cluster HW snapshot directory \"%s\": %m", dir)));
}

/*
 * hw_snapshot_write_durable -- write `len` bytes to `tmp`, fsync, then
 * durable_rename over `final` (which fsyncs the directory).  PANICs on any I/O
 * failure, mirroring the stock update_controlfile / cluster_cf_authority_write
 * contract -- a half-written snapshot must never be observed.
 */
static void
hw_snapshot_write_durable(const char *tmp, const char *final, const char *buf, size_t len)
{
	int fd;

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", tmp)));

	errno = 0;
	if (write(fd, buf, len) != (ssize_t)len) {
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
 * cluster_hw_snapshot_write -- serialise the local master's authority snapshot
 * and install it torn-safe (R1/R2).  See header.
 */
void
cluster_hw_snapshot_write(uint32 owner_node_id, uint32 shard_partition, uint32 generation,
						  XLogRecPtr snapshot_lsn, uint64 system_id, ClusterHwSnapshotKind kind,
						  const ClusterHwSnapshotEntry *entries, uint32 n_entries)
{
	ClusterHwSnapshotHeader hdr;
	char final[MAXPGPATH];
	char tmp[MAXPGPATH];
	char *buf;
	size_t len;

	if (!hw_snapshot_build_path(owner_node_id, false, final, sizeof(final))
		|| !hw_snapshot_build_path(owner_node_id, true, tmp, sizeof(tmp)))
		ereport(PANIC, (errmsg("cluster shared_data_dir is not configured")));

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = CLUSTER_HW_SNAPSHOT_MAGIC;
	hdr.version = CLUSTER_HW_SNAPSHOT_VERSION;
	hdr.system_id = system_id;
	hdr.owner_node_id = owner_node_id;
	hdr.shard_partition = shard_partition;
	hdr.snapshot_kind = (uint32)kind;
	hdr.generation = generation;
	hdr.snapshot_lsn = (uint64)snapshot_lsn;
	hdr.n_entries = n_entries;
	hdr.reserved = 0;

	len = cluster_hw_snapshot_serialized_size(n_entries);
	buf = (char *)palloc(len);
	if (cluster_hw_snapshot_serialize(&hdr, entries, buf, len) != len)
		ereport(PANIC, (errmsg("could not serialize HW authority snapshot")));

	hw_snapshot_ensure_dir();
	hw_snapshot_write_durable(tmp, final, buf, len);
	pfree(buf);
}

/*
 * cluster_hw_snapshot_read -- read + validate a per-master snapshot (R6).  See
 * header.  A missing file, a torn image, or a foreign/mis-bound identity all
 * return a non-VALID outcome so the caller fails closed and never rebuilds from
 * zero or FileSize.
 */
ClusterHwSnapshotValidity
cluster_hw_snapshot_read(uint32 owner_node_id, uint64 expected_sysid, uint32 expected_owner,
						 uint32 expected_shard, ClusterHwSnapshotHeader *out_hdr,
						 ClusterHwSnapshotEntry *entries_out, uint32 max_entries)
{
	char path[MAXPGPATH];
	char *buf;
	size_t bufsz;
	int fd;
	ssize_t r;
	ClusterHwSnapshotValidity v;

	Assert(out_hdr != NULL);

	if (!hw_snapshot_build_path(owner_node_id, false, path, sizeof(path)))
		return CLUSTER_HW_SNAPSHOT_INVALID_SHORT;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return CLUSTER_HW_SNAPSHOT_INVALID_SHORT; /* missing -> caller fails closed */

	bufsz = cluster_hw_snapshot_serialized_size(max_entries);
	buf = (char *)palloc(bufsz);
	r = read(fd, buf, bufsz);
	CloseTransientFile(fd);
	if (r < 0) {
		pfree(buf);
		return CLUSTER_HW_SNAPSHOT_INVALID_SHORT;
	}

	v = cluster_hw_snapshot_deserialize(buf, (size_t)r, out_hdr, entries_out, max_entries);
	pfree(buf);
	if (v != CLUSTER_HW_SNAPSHOT_VALID)
		return v;

	if (!cluster_hw_snapshot_identity_ok(out_hdr, expected_sysid, expected_owner, expected_shard))
		return CLUSTER_HW_SNAPSHOT_INVALID_IDENTITY;

	return CLUSTER_HW_SNAPSHOT_VALID;
}
