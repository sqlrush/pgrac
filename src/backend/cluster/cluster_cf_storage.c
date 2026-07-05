/*-------------------------------------------------------------------------
 *
 * cluster_cf_storage.c
 *	  Establish and verify the shared pg_control authority on a node and
 *	  on the underlying shared storage (spec-5.6).
 *
 *	  Covers migration of a per-node control file into the shared
 *	  authority plus a node-local symlink, classification of what a
 *	  node's local control path actually is, the Phase-1 local rename
 *	  probe, and the pure write-gate.  Phase-2 cross-node nonce+
 *	  ack verification (which needs the interconnect and a live peer) is
 *	  layered on by the bootstrap path and exercised by t/289.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cf_storage.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_phase2.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_recovery_anchor.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"

/* A node's local control path is always $PGDATA/global/pg_control. */
#define CLUSTER_CF_LOCAL_REL "global/pg_control"

/* Phase-1 probe nonce written, renamed, and read back to prove the storage
 * honours durable_rename locally. */
static const char cf_probe_nonce[] = "PGRAC_CF_RENAME_PROBE_v1";

/*
 * ClusterCfContractRecord -- on-disk layout of the per-node
 * $PGDATA/global/pgrac_cf_contract file.  It binds this node's storage
 * rename-contract verification to the shared-storage identity it was proven
 * against, so a storage swap (different uuid) silently invalidates the record
 * and forces re-verification (fail-closed).  CRC-protected; a torn or corrupt
 * record reads as UNVERIFIED.
 */
#define CLUSTER_CF_CONTRACT_MAGIC 0x43464354 /* 'CFCT' */
#define CLUSTER_CF_CONTRACT_VERSION 2

typedef struct ClusterCfContractRecord {
	uint32 magic;
	uint32 version;
	uint64 authority_system_identifier; /* v2: this node's bound authority sysid */
	char storage_uuid[CLUSTER_SHARED_UUID_LEN];
	char _pad[3];  /* keep `state` 4-byte aligned */
	uint32 state;  /* a ClusterCfContractState value */
	pg_crc32c crc; /* over [0, offsetof(crc)) */
} ClusterCfContractRecord;

/*
 * cluster_cf_startup_verdict -- pure startup-gate decision.
 *
 *	A node in authority mode must reach the shared authority through a
 *	symlink that points at it AND read a valid, identity-matched image;
 *	anything else fails closed with a specific reason.
 */
ClusterCfStartupVerdict
cluster_cf_startup_verdict(ClusterCfSymlinkStatus link_status, bool authority_readable,
						   bool identity_ok)
{
	switch (link_status) {
	case CLUSTER_CF_SYMLINK_OK:
		break;
	case CLUSTER_CF_SYMLINK_MISSING:
		return CLUSTER_CF_STARTUP_FATAL_MISSING;
	case CLUSTER_CF_SYMLINK_NOT_SYMLINK:
		return CLUSTER_CF_STARTUP_FATAL_NOT_SYMLINK;
	case CLUSTER_CF_SYMLINK_WRONG_TARGET:
		return CLUSTER_CF_STARTUP_FATAL_WRONG_TARGET;
	}

	if (!authority_readable)
		return CLUSTER_CF_STARTUP_FATAL_UNREADABLE;
	if (!identity_ok)
		return CLUSTER_CF_STARTUP_FATAL_IDENTITY;
	return CLUSTER_CF_STARTUP_OK;
}

/*
 * cluster_cf_storage_write_allowed -- pure write-gate.
 */
bool
cluster_cf_storage_write_allowed(ClusterCfContractState state, bool multi_node)
{
	if (!multi_node)
		return true;
	return state == CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
}

/*
 * cluster_cf_contract_resolve -- pure identity-bound resolution (see header).
 */
ClusterCfContractState
cluster_cf_contract_resolve(const char *persisted_uuid, const char *current_uuid,
							ClusterCfContractState persisted_state)
{
	/* No current storage identity -> cannot bind anything -> fail closed. */
	if (current_uuid == NULL || current_uuid[0] == '\0')
		return CLUSTER_CF_CONTRACT_UNVERIFIED;
	/* No persisted record (or it recorded no identity) -> never verified. */
	if (persisted_uuid == NULL || persisted_uuid[0] == '\0')
		return CLUSTER_CF_CONTRACT_UNVERIFIED;
	/* Bound to a different storage (swap/re-attach) -> re-verify. */
	if (strcmp(persisted_uuid, current_uuid) != 0)
		return CLUSTER_CF_CONTRACT_UNVERIFIED;
	/* Same storage: trust only a recorded CROSSNODE_VERIFIED, nothing weaker. */
	if (persisted_state == CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED)
		return CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
	return CLUSTER_CF_CONTRACT_UNVERIFIED;
}

/*
 * read_contract_record -- read + structurally validate the per-node anchor.
 * Returns true (with *crc_ok set) for a current-version record with the right
 * magic; false when the file is absent/short/foreign-format (no usable anchor).
 */
static bool
read_contract_record(const char *pgdata, ClusterCfContractRecord *rec, bool *crc_ok)
{
	char path[MAXPGPATH];
	pg_crc32c crc;
	int fd;
	int n;

	*crc_ok = false;
	snprintf(path, sizeof(path), "%s/%s", pgdata, CLUSTER_CF_CONTRACT_REL_PATH);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	n = read(fd, rec, sizeof(*rec));
	CloseTransientFile(fd);
	if (n != (int)sizeof(*rec) || rec->magic != CLUSTER_CF_CONTRACT_MAGIC
		|| rec->version != CLUSTER_CF_CONTRACT_VERSION)
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, rec, offsetof(ClusterCfContractRecord, crc));
	FIN_CRC32C(crc);
	*crc_ok = (crc == rec->crc);
	return true;
}

/*
 * write_contract_record -- (re)compute the CRC and atomically write the anchor
 * (write-tmp + fsync + durable_rename).  Returns false on any I/O failure.
 */
static bool
write_contract_record(const char *pgdata, ClusterCfContractRecord *rec)
{
	char path[MAXPGPATH];
	char tmp[MAXPGPATH];
	int fd;

	INIT_CRC32C(rec->crc);
	COMP_CRC32C(rec->crc, rec, offsetof(ClusterCfContractRecord, crc));
	FIN_CRC32C(rec->crc);

	snprintf(path, sizeof(path), "%s/%s", pgdata, CLUSTER_CF_CONTRACT_REL_PATH);
	snprintf(tmp, sizeof(tmp), "%s/%s.tmp", pgdata, CLUSTER_CF_CONTRACT_REL_PATH);

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		return false;
	if (write(fd, rec, sizeof(*rec)) != (int)sizeof(*rec) || pg_fsync(fd) != 0) {
		CloseTransientFile(fd);
		unlink(tmp);
		return false;
	}
	CloseTransientFile(fd);

	/* durable_rename (LOG elevel) fsyncs the dir and returns -1 on failure. */
	if (durable_rename(tmp, path, LOG) != 0) {
		unlink(tmp);
		return false;
	}
	return true;
}

/*
 * cluster_cf_contract_persist -- atomically record the contract + the node's
 * bound authority system_identifier (see header).
 */
bool
cluster_cf_contract_persist(const char *pgdata, ClusterCfContractState state,
							uint64 authority_sysid)
{
	ClusterCfContractRecord rec;

	memset(&rec, 0, sizeof(rec));
	rec.magic = CLUSTER_CF_CONTRACT_MAGIC;
	rec.version = CLUSTER_CF_CONTRACT_VERSION;
	rec.authority_system_identifier = authority_sysid;
	cluster_shared_fs_get_storage_uuid(rec.storage_uuid, sizeof(rec.storage_uuid));
	rec.state = (uint32)state;
	return write_contract_record(pgdata, &rec);
}

/*
 * cluster_cf_contract_update_state -- update ONLY the state of an existing
 * anchor, preserving the bound authority system_identifier and storage uuid
 * (see header).  Fails (false) when there is no valid anchor to update: the
 * identity is established at migration and must never be fabricated here.
 */
bool
cluster_cf_contract_update_state(const char *pgdata, ClusterCfContractState state)
{
	ClusterCfContractRecord rec;
	bool crc_ok;

	if (!read_contract_record(pgdata, &rec, &crc_ok) || !crc_ok)
		return false;
	rec.state = (uint32)state;
	return write_contract_record(pgdata, &rec);
}

/*
 * cluster_cf_contract_load -- read the anchor and resolve its STATE against the
 * current shared-storage identity (see header).  Missing/torn -> UNVERIFIED.
 */
ClusterCfContractState
cluster_cf_contract_load(const char *pgdata)
{
	ClusterCfContractRecord rec;
	char current_uuid[CLUSTER_SHARED_UUID_LEN];
	bool crc_ok;

	if (!read_contract_record(pgdata, &rec, &crc_ok) || !crc_ok)
		return CLUSTER_CF_CONTRACT_UNVERIFIED;
	cluster_shared_fs_get_storage_uuid(current_uuid, sizeof(current_uuid));
	return cluster_cf_contract_resolve(rec.storage_uuid, current_uuid,
									   (ClusterCfContractState)rec.state);
}

/*
 * cluster_cf_contract_identity_resolve -- pure per-node identity verdict (see
 * header).  Once a node was bound to a storage that advertised a uuid, the
 * current storage must still advertise the SAME one: an empty current uuid means
 * the shared-storage sentinel is missing/corrupt (or a sentinel-less snapshot
 * was mounted) and cannot confirm identity -> fail closed.  A node bound without
 * a uuid (no sentinel at migration) stays sysid-only.  The bound authority
 * system_identifier MUST always match the authority being read.
 */
ClusterCfIdentityVerdict
cluster_cf_contract_identity_resolve(bool present, bool crc_ok, const char *anchor_uuid,
									 uint64 anchor_sysid, const char *current_uuid,
									 uint64 shared_sysid)
{
	if (!present)
		return CLUSTER_CF_IDENTITY_FATAL_NO_ANCHOR;
	if (!crc_ok)
		return CLUSTER_CF_IDENTITY_FATAL_CRC;
	if (anchor_uuid != NULL && anchor_uuid[0] != '\0') {
		/* Bound to a uuid: the current storage must advertise the same one. */
		if (current_uuid == NULL || current_uuid[0] == '\0'
			|| strcmp(anchor_uuid, current_uuid) != 0)
			return CLUSTER_CF_IDENTITY_FATAL_STORAGE;
	}
	if (anchor_sysid != shared_sysid)
		return CLUSTER_CF_IDENTITY_FATAL_SYSID;
	return CLUSTER_CF_IDENTITY_OK;
}

/*
 * cluster_cf_contract_identity_check -- read this node's anchor and verify the
 * shared authority it is symlinked to still has the bound identity (see
 * header).  This is the real startup gate: with no independent local sysid
 * after migration, the anchor is the only proof a node is reading its own
 * cluster's control file and not a foreign one swapped in at the same path.
 */
ClusterCfIdentityVerdict
cluster_cf_contract_identity_check(const char *pgdata, uint64 shared_sysid)
{
	ClusterCfContractRecord rec;
	char current_uuid[CLUSTER_SHARED_UUID_LEN];
	bool present;
	bool crc_ok;

	present = read_contract_record(pgdata, &rec, &crc_ok);
	if (!present)
		return cluster_cf_contract_identity_resolve(false, false, NULL, 0, NULL, shared_sysid);
	cluster_shared_fs_get_storage_uuid(current_uuid, sizeof(current_uuid));
	return cluster_cf_contract_identity_resolve(true, crc_ok, rec.storage_uuid,
												rec.authority_system_identifier, current_uuid,
												shared_sysid);
}

/*
 * cluster_cf_contract_bind_decide -- pure decision (see header).
 */
ClusterCfBindAction
cluster_cf_contract_bind_decide(bool present, bool crc_ok, const char *anchor_uuid,
								const char *current_uuid)
{
	if (!present || !crc_ok)
		return CLUSTER_CF_BIND_SKIP_NO_ANCHOR;
	if (anchor_uuid != NULL && anchor_uuid[0] != '\0')
		return CLUSTER_CF_BIND_NOOP_ALREADY;
	if (current_uuid == NULL || current_uuid[0] == '\0')
		return CLUSTER_CF_BIND_SKIP_NO_UUID;
	return CLUSTER_CF_BIND_FILL;
}

/*
 * cluster_cf_contract_bind_storage_uuid -- fill the anchor's storage uuid once
 * (see header).  Only a FILL decision writes; every other case is a no-op so a
 * missing/torn anchor is never fabricated and a bound uuid is never overwritten.
 */
bool
cluster_cf_contract_bind_storage_uuid(const char *pgdata)
{
	ClusterCfContractRecord rec;
	char current_uuid[CLUSTER_SHARED_UUID_LEN];
	bool present;
	bool crc_ok = false;

	present = read_contract_record(pgdata, &rec, &crc_ok);
	cluster_shared_fs_get_storage_uuid(current_uuid, sizeof(current_uuid));
	if (cluster_cf_contract_bind_decide(present, crc_ok, present ? rec.storage_uuid : NULL,
										current_uuid)
		!= CLUSTER_CF_BIND_FILL)
		return false;

	/*
	 * Fill the uuid in place; write_contract_record recomputes the CRC.  The
	 * bound system_identifier and the contract state are left exactly as read,
	 * so the identity anchor and any cross-node verification are preserved.
	 */
	strlcpy(rec.storage_uuid, current_uuid, sizeof(rec.storage_uuid));
	return write_contract_record(pgdata, &rec);
}

/*
 * cluster_cf_bind_storage_uuid_once -- postmaster-once entry (see header).
 */
void
cluster_cf_bind_storage_uuid_once(const char *pgdata)
{
	if (!cluster_controlfile_shared_authority)
		return;
	if (cluster_cf_contract_bind_storage_uuid(pgdata)) {
		char uuid[CLUSTER_SHARED_UUID_LEN];

		cluster_shared_fs_get_storage_uuid(uuid, sizeof(uuid));
		ereport(LOG, (errmsg("cluster: bound control-file authority anchor to "
							 "shared-storage uuid %s",
							 uuid)));
	}
}

/*
 * cluster_cf_assess_liveness -- tri-state liveness (see header).
 */
ClusterCfLiveness
cluster_cf_assess_liveness(void)
{
	/*
	 * Order matters: require CSSD READY first.  A zero alive-peer
	 * count is only meaningful once CSSD shmem + membership are up; checked
	 * before that, the count helpers return 0 for "shmem absent", which would
	 * falsely read as "I am the only node".
	 */
	if (cluster_cssd_get_status() != CLUSTER_CSSD_READY)
		return CLUSTER_CF_LIVENESS_UNKNOWN;

	/*
	 * A demonstrably-alive peer is PEER_ALIVE regardless of quorum.  The
	 * join-read-only role this drives does NOT write the shared authority, so
	 * it needs no quorum; requiring quorum here would stall a healthy 2-node
	 * bring-up (the gate would read UNKNOWN until quorum settles even though
	 * the peer is plainly up).
	 */
	if (cluster_cssd_get_alive_peer_count() != 0)
		return CLUSTER_CF_LIVENESS_PEER_ALIVE;

	/*
	 * Claiming sole-liveness authorizes a single-node-authority WRITE, so it
	 * must rule out a partition in which a peer is alive but unreachable: that
	 * requires quorum.  Without quorum, a zero alive-peer count is
	 * not trustworthy -> UNKNOWN (fail-closed for the owner path).
	 */
	if (!cluster_qvotec_in_quorum())
		return CLUSTER_CF_LIVENESS_UNKNOWN;
	return CLUSTER_CF_LIVENESS_SOLE;
}

/*
 * cluster_cf_verify_sole_liveness_or_fail -- positive proof, the
 * boolean "== SOLE" view of the liveness assessment.
 */
bool
cluster_cf_verify_sole_liveness_or_fail(void)
{
	return cluster_cf_assess_liveness() == CLUSTER_CF_LIVENESS_SOLE;
}

/*
 * cluster_cf_bootstrap_role -- pure (config, liveness, contract) -> role.
 */
ClusterCfBootstrapRole
cluster_cf_bootstrap_role(bool multi_node, ClusterCfLiveness liveness,
						  ClusterCfContractState contract)
{
	/* A single-node cluster is trivially the sole authority owner. */
	if (!multi_node)
		return CLUSTER_CF_ROLE_OWNER;

	switch (liveness) {
	case CLUSTER_CF_LIVENESS_SOLE:

		/*
			 * Sole live node of a multi-node cluster: it may take authority and
			 * write during its own recovery, but only after the storage has
			 * been cross-node verified -- otherwise a peer that reattaches
			 * might not see these writes (split-brain hazard).
			 */
		return (contract == CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED) ? CLUSTER_CF_ROLE_OWNER
																	: CLUSTER_CF_ROLE_FAILCLOSED;

	case CLUSTER_CF_LIVENESS_PEER_ALIVE:

		/*
			 * A live peer owns the authority.  This node attaches read-only and
			 * must not write the shared authority during recovery -- but only if
			 * it has proven (cross-node verified) it can actually see the peer's
			 * authority writes; otherwise it cannot trust what it reads.
			 */
		return (contract == CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED) ? CLUSTER_CF_ROLE_JOIN_READONLY
																	: CLUSTER_CF_ROLE_FAILCLOSED;

	case CLUSTER_CF_LIVENESS_UNKNOWN:
	default:
		/* Cannot tell who is live -> no safe role -> fail closed. */
		return CLUSTER_CF_ROLE_FAILCLOSED;
	}
}

/*
 * cluster_cf_bootstrap_authority_gate -- write gate.
 */
bool
cluster_cf_bootstrap_authority_gate(bool multi_node, ClusterCfContractState contract)
{
	if (!cluster_cf_verify_sole_liveness_or_fail())
		return false;
	return cluster_cf_storage_write_allowed(contract, multi_node);
}

/*
 * cluster_cf_enter_bootstrap_window_or_fail -- bootstrap entry.
 */
void
cluster_cf_enter_bootstrap_window_or_fail(void)
{
	bool multi_node;

	if (!cluster_controlfile_shared_authority)
		return; /* authority off: stock per-node path */

	/*
	 * The shared authority lives on storage that must give a torn-safe,
	 * cross-node-visible rename; verify the rename primitive locally before
	 * trusting any write to it (Phase-1).  Phase-2 cross-node visibility is
	 * established separately and gates the multi-node path below.
	 */
	if (!cluster_cf_storage_probe_local())
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
						errmsg("shared storage under cluster.shared_data_dir does not honour the "
							   "durable rename contract"),
						errhint("Use a shared filesystem that provides atomic rename + directory "
								"fsync, or turn cluster.controlfile_shared_authority off.")));

	/*
	 * A single-node cluster (or cluster.enabled off) is trivially the sole
	 * authority: there is no peer that could concurrently write, so open the
	 * bootstrap window without needing CSSD to prove sole-liveness.
	 */
	multi_node = cluster_enabled && cluster_conf_node_count() > 1;
	if (!multi_node) {
		/* Single-node cluster: trivially the sole authority owner. */
		cluster_cf_counter_inc(CLUSTER_CF_SINGLE_NODE_AUTHORITY);
		cluster_cf_set_bootstrap_authority(true);
		return;
	}

	/*
	 * Multi-node bootstrap.  The membership service is a post-PM_RUN aux
	 * process, so during StartupXLOG it cannot prove this node is the sole live
	 * authority; a multi-node node therefore never takes single-node authority
	 * here.  Instead the Phase-2 rendezvous (run just above in StartupXLOG)
	 * proves, for this bootstrap, that a peer is alive and the shared storage is
	 * cross-node verified, which authorizes only JOIN_READONLY: read the shared
	 * authority, never write it during recovery.  Steady-state writes go through
	 * CF X once the node reaches PM_RUN.  No verified peer -> fail closed (there
	 * is no owner-write path during recovery yet).
	 *
	 * Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
	 */
	if (cluster_cf_phase2_peer_verified()) {
		/*
		 * Mark the node-wide shmem join flag (so the checkpointer's
		 * CreateCheckPoint skips CF X for the end-of-recovery checkpoint) AND
		 * the startup process's own process-local write-skip (so this process's
		 * remaining recovery-time control-file writes are skipped at the
		 * chokepoint).  The startup process exits after recovery, so its
		 * write-skip cannot leak into steady state.
		 */
		cluster_cf_set_join_readonly(true);
		cluster_cf_set_write_skip(true);
		return;
	}

	ereport(
		FATAL,
		(errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
		 errmsg("cannot establish bootstrap shared control-file authority on a multi-node cluster"),
		 errhint("Phase-2 cross-node rename verification with a live peer is required; "
				 "a sole-survivor authority write needs an early-fencing oracle (not yet "
				 "supported in this release) and fails closed.")));
}

/*
 * cluster_cf_symlink_status -- classify local_path against expected_target.
 */
ClusterCfSymlinkStatus
cluster_cf_symlink_status(const char *local_path, const char *expected_target)
{
	struct stat st;
	char linkbuf[MAXPGPATH];
	ssize_t n;

	if (lstat(local_path, &st) != 0)
		return CLUSTER_CF_SYMLINK_MISSING;

	if (!S_ISLNK(st.st_mode))
		return CLUSTER_CF_SYMLINK_NOT_SYMLINK;

	n = readlink(local_path, linkbuf, sizeof(linkbuf) - 1);
	if (n < 0)
		return CLUSTER_CF_SYMLINK_WRONG_TARGET;
	linkbuf[n] = '\0';

	if (expected_target != NULL && strcmp(linkbuf, expected_target) == 0)
		return CLUSTER_CF_SYMLINK_OK;
	return CLUSTER_CF_SYMLINK_WRONG_TARGET;
}

/*
 * cluster_cf_storage_probe_local -- Phase-1 local rename probe.
 *
 *	Writes a nonce, fsyncs, durable_renames it, then reopens and verifies
 *	the nonce survived.  durable_rename is asked to ereport only at LOG so
 *	a failure returns false (fail-closed) instead of throwing.  Best-effort
 *	cleanup of probe files; a leftover probe file is harmless.
 */
bool
cluster_cf_storage_probe_local(void)
{
	char tmp[MAXPGPATH];
	char final[MAXPGPATH];
	char readback[sizeof(cf_probe_nonce)];
	int fd;
	int pid = (int)getpid();

	if (cluster_shared_data_dir == NULL)
		return false;

	snprintf(tmp, sizeof(tmp), "%s/global/pgrac_cf_probe.%d.tmp", cluster_shared_data_dir, pid);
	snprintf(final, sizeof(final), "%s/global/pgrac_cf_probe.%d", cluster_shared_data_dir, pid);

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		return false;
	if (write(fd, cf_probe_nonce, sizeof(cf_probe_nonce)) != (int)sizeof(cf_probe_nonce)
		|| pg_fsync(fd) != 0) {
		CloseTransientFile(fd);
		unlink(tmp);
		return false;
	}
	CloseTransientFile(fd);

	if (durable_rename(tmp, final, LOG) != 0) {
		unlink(tmp);
		return false;
	}

	fd = OpenTransientFile(final, O_RDONLY | PG_BINARY);
	if (fd < 0) {
		unlink(final);
		return false;
	}
	if (read(fd, readback, sizeof(readback)) != (int)sizeof(readback)) {
		CloseTransientFile(fd);
		unlink(final);
		return false;
	}
	CloseTransientFile(fd);
	unlink(final);

	return memcmp(cf_probe_nonce, readback, sizeof(cf_probe_nonce)) == 0;
}

/*
 * read_local_controlfile -- raw read of a node-local pg_control image,
 * accepted only if structurally valid (CRC/byte order).  No identity check.
 */
static bool
read_local_controlfile(const char *path, ControlFileData *out)
{
	int fd;
	int r;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	r = read(fd, out, sizeof(ControlFileData));
	CloseTransientFile(fd);
	if (r != (int)sizeof(ControlFileData))
		return false;
	return cluster_cf_classify_buffer((char *)out, sizeof(ControlFileData), 0) == CLUSTER_CF_VALID;
}

/*
 * cluster_cf_migrate_and_link -- create the shared authority from this
 * node's local control file (if absent) and point the local path at it.
 */
bool
cluster_cf_migrate_and_link(const char *local_pgdata)
{
	char local_path[MAXPGPATH];
	const char *shared;
	ClusterCfSymlinkStatus st;
	ControlFileData local_cf;
	ControlFileData shared_cf;

	shared = cluster_cf_shared_path();
	if (shared == NULL)
		return false;

	snprintf(local_path, sizeof(local_path), "%s/%s", local_pgdata, CLUSTER_CF_LOCAL_REL);

	/* Already the correct symlink: the startup identity gate verifies it. */
	st = cluster_cf_symlink_status(local_path, shared);
	if (st == CLUSTER_CF_SYMLINK_OK)
		return true;

	if (st == CLUSTER_CF_SYMLINK_NOT_SYMLINK) {
		/*
		 * A genuine per-node regular control file: this is the FIRST migration
		 * (or the node that bootstraps the shared authority).  Bind the node's
		 * identity anchor to the authority's system_identifier BEFORE pointing
		 * the local path at it, so every later start can prove it is reading its
		 * own cluster's authority.
		 */
		if (!read_local_controlfile(local_path, &local_cf))
			return false;
		if (!cluster_cf_authority_read(&shared_cf)) {
			cluster_cf_authority_write(&local_cf);
			if (!cluster_cf_authority_read(&shared_cf))
				return false;
		}
		if (local_cf.system_identifier != shared_cf.system_identifier)
			return false; /* foreign authority -> fail-closed */

		/*
		 * spec-5.6a D4: seed this node's per-node recovery anchor from the
		 * pre-migration local control file -- at this moment it still holds
		 * this node's own checkpoint/state (its authority=off semantics).
		 * Skipped when a backup_label is present: a label-provisioned tree
		 * carries the SOURCE node's checkpoint fields in its control file,
		 * so a joiner's anchor is created by its first own checkpoint
		 * instead (a crash inside that label window fails closed, 53RB3,
		 * and is re-provisioned).  Written durably BEFORE the symlink flip
		 * below, so a crash between the two re-runs this arm (R7 ordering).
		 */
		{
			char label_path[MAXPGPATH];
			struct stat label_st;

			snprintf(label_path, sizeof(label_path), "%s/backup_label", local_pgdata);
			if (cluster_node_id >= 0 && stat(label_path, &label_st) != 0) {
				ClusterRecoveryAnchor ra;

				cluster_recovery_anchor_build_from_controlfile(&local_cf, &ra);
				cluster_recovery_anchor_write(&ra);
			}
		}

		if (!cluster_cf_contract_persist(local_pgdata, CLUSTER_CF_CONTRACT_LOCAL_PROBED,
										 shared_cf.system_identifier))
			return false; /* could not record the identity anchor -> fail-closed */
		if (unlink(local_path) != 0)
			return false;
		if (symlink(shared, local_path) != 0)
			return false;
		return true;
	}

	/*
	 * WRONG_TARGET or MISSING: an already-provisioned node whose symlink points
	 * elsewhere or is gone.  Re-link ONLY if the per-node anchor proves the
	 * current shared authority is this node's own cluster -- never blindly
	 * re-trust whatever authority now sits at the configured path.
	 */
	if (!cluster_cf_authority_read(&shared_cf))
		return false;
	if (cluster_cf_contract_identity_check(local_pgdata, shared_cf.system_identifier)
		!= CLUSTER_CF_IDENTITY_OK)
		return false;
	if (st != CLUSTER_CF_SYMLINK_MISSING) {
		if (unlink(local_path) != 0)
			return false;
	}
	if (symlink(shared, local_path) != 0)
		return false;
	return true;
}

/*
 * startup_verdict_detail -- human-readable reason for a fail-closed verdict.
 */
static const char *
startup_verdict_detail(ClusterCfStartupVerdict v)
{
	switch (v) {
	case CLUSTER_CF_STARTUP_OK:
		return "ok";
	case CLUSTER_CF_STARTUP_FATAL_MISSING:
		return "the local control path does not exist";
	case CLUSTER_CF_STARTUP_FATAL_NOT_SYMLINK:
		return "the local control path is a per-node regular file, "
			   "not a symlink to the shared authority";
	case CLUSTER_CF_STARTUP_FATAL_WRONG_TARGET:
		return "the local control symlink points at a different authority";
	case CLUSTER_CF_STARTUP_FATAL_UNREADABLE:
		return "the shared authority is unreadable or torn";
	case CLUSTER_CF_STARTUP_FATAL_IDENTITY:
		return "the shared authority has a foreign system identifier";
	}
	return "unknown";
}

/*
 * identity_verdict_detail -- human-readable reason for an identity-gate fail.
 */
static const char *
identity_verdict_detail(ClusterCfIdentityVerdict v)
{
	switch (v) {
	case CLUSTER_CF_IDENTITY_OK:
		return "ok";
	case CLUSTER_CF_IDENTITY_FATAL_NO_ANCHOR:
		return "this node has no identity anchor for the shared authority "
			   "(global/pgrac_cf_contract is missing or unrecognized)";
	case CLUSTER_CF_IDENTITY_FATAL_CRC:
		return "the node's identity anchor is torn or corrupt";
	case CLUSTER_CF_IDENTITY_FATAL_STORAGE:
		return "the shared storage identity changed since this node was bound to it";
	case CLUSTER_CF_IDENTITY_FATAL_SYSID:
		return "the shared authority has a different system identifier than the "
			   "one this node was bound to (foreign control file)";
	}
	return "unknown";
}

/*
 * cluster_cf_startup_prepare -- startup hook (see header).
 */
void
cluster_cf_startup_prepare(const char *pgdata)
{
	char local_path[MAXPGPATH];
	const char *shared;
	ClusterCfSymlinkStatus st;
	ControlFileData cf;
	bool readable;
	ClusterCfStartupVerdict v;
	ClusterCfIdentityVerdict idv;

	if (!cluster_controlfile_shared_authority)
		return; /* off: stock per-node pg_control */

	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
						errmsg("cluster.controlfile_shared_authority is on but "
							   "cluster.shared_data_dir is not set"),
						errhint("Set cluster.shared_data_dir to the shared mount, or turn "
								"cluster.controlfile_shared_authority off.")));

	/* Establish the shared authority + symlink for this node (idempotent). */
	if (!cluster_cf_migrate_and_link(pgdata))
		ereport(
			FATAL,
			(errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
			 errmsg("could not migrate global/pg_control into the shared authority under \"%s\"",
					cluster_shared_data_dir)));

	/* Verify and fail-closed on any mismatch. */
	shared = cluster_cf_shared_path();
	snprintf(local_path, sizeof(local_path), "%s/%s", pgdata, CLUSTER_CF_LOCAL_REL);
	st = cluster_cf_symlink_status(local_path, shared);
	readable = cluster_cf_authority_read(&cf);

	/*
	 * Structural verdict: the local path must be a symlink to the current
	 * shared authority and that authority must be readable.  Identity is the
	 * separate anchor check below, so identity_ok is passed true here.
	 */
	v = cluster_cf_startup_verdict(st, readable, true);
	if (v != CLUSTER_CF_STARTUP_OK)
		ereport(
			FATAL,
			(errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
			 errmsg("shared pg_control authority check failed at startup"),
			 errdetail("%s.", startup_verdict_detail(v)),
			 errhint("Every node must point at the same shared authority \"%s\"; "
					 "turn cluster.controlfile_shared_authority off for a single-node deployment.",
					 shared)));

	/*
	 * Identity gate.  After migration the local control file IS the symlink to
	 * the shared authority, so there is no independent local system_identifier
	 * to compare; the per-node anchor recorded at migration is the only proof
	 * this node is reading its own cluster's authority and not a foreign but
	 * valid one swapped in at the same shared path.  Fail closed on any
	 * mismatch -- a missing anchor included.
	 */
	idv = cluster_cf_contract_identity_check(pgdata, cf.system_identifier);
	if (idv != CLUSTER_CF_IDENTITY_OK)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
						errmsg("shared pg_control authority identity check failed at startup"),
						errdetail("%s.", identity_verdict_detail(idv)),
						errhint("This node is not bound to the shared authority at \"%s\"; it "
								"must only attach to the cluster it was provisioned with.",
								shared)));

	/*
	 * spec-5.6a D4 startup vet: a label-less boot under the authority needs
	 * this node's per-node recovery anchor to locate its own checkpoint in
	 * its own WAL thread -- the shared authority's checkpoint fields belong
	 * to the last permitted writer.  Fail here, before any further boot side
	 * effects, rather than let the startup process get anywhere near the
	 * foreign fields (StartupXLOG re-checks and stays the authoritative
	 * consumer; this is the early, actionable error).  A backup_label boot
	 * is exempt: the label is the one-shot provisioning authority and the
	 * first own checkpoint creates the anchor.  Scoped to the multi-node
	 * regime (cluster.enabled=on): the single-node-authority window admits
	 * exactly one writer, so the shared fields are this node's own there.
	 */
	if (cluster_enabled)
	{
		char label_path[MAXPGPATH];
		struct stat label_st;

		snprintf(label_path, sizeof(label_path), "%s/backup_label", pgdata);
		if (stat(label_path, &label_st) != 0) {
			ClusterRecoveryAnchor ra;

			if (!cluster_recovery_anchor_read(cf.system_identifier, &ra, NULL))
				ereport(FATAL,
						(errcode(ERRCODE_CLUSTER_RECOVERY_ANCHOR_UNAVAILABLE),
						 errmsg("per-node recovery anchor for node %d is missing or invalid "
								"under the shared control-file authority",
								cluster_node_id),
						 errdetail("A label-less restart cannot locate this node's own "
								   "checkpoint from the shared pg_control (its checkpoint "
								   "fields belong to the last writer node)."),
						 errhint("Re-provision this node from a base backup, or verify that "
								 "cluster.shared_data_dir (\"%s\") is correctly mounted.",
								 cluster_shared_data_dir)));
		}
	}
}
