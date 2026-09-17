/*-------------------------------------------------------------------------
 *
 * cluster_cf_phase2.c
 *	  Cross-node storage rename-contract verification (spec-5.6 Phase-2).
 *
 *	  Symmetric nonce+ack rendezvous over the shared storage (see the header
 *	  for the protocol).  The probe/ack files are written with the exact
 *	  tmp + fsync + durable_rename + dir-fsync sequence the shared pg_control
 *	  authority uses, so a successful rendezvous proves the storage gives the
 *	  cross-node durable_rename visibility the authority depends on.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cf_phase2.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_cf_phase2.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "utils/timestamp.h"

#define CLUSTER_CF_PHASE2_MAGIC 0x43465032 /* 'CFP2' */
#define CLUSTER_CF_PHASE2_VERSION 2

/* Poll interval while waiting for the peer's probe/ack to appear. */
#define CLUSTER_CF_PHASE2_POLL_US 100000 /* 100 ms */

/*
 * ensure_p2_dir -- create <shared_dir>/global/pgrac_cf_p2 if absent.  The
 * parent global/ already holds the shared authority, so only the leaf is made.
 */
static void
ensure_p2_dir(const char *shared_dir)
{
	char dir[MAXPGPATH];

	snprintf(dir, sizeof(dir), "%s/%s", shared_dir, CLUSTER_CF_PHASE2_DIR);
	if (mkdir(dir, pg_dir_create_mode) != 0 && errno != EEXIST)
		ereport(LOG, (errcode_for_file_access(),
					  errmsg("cluster cf phase-2: could not create \"%s\": %m", dir)));
}

/*
 * write_record -- torn-safe write of one record to <shared_dir>/<rel>, using
 * the same tmp + fsync + durable_rename + dir-fsync sequence as the authority.
 * Returns false on any I/O failure without throwing.
 */
static bool
cf_phase2_node_valid(int node_id)
{
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES && cluster_conf_lookup_node(node_id) != NULL;
}

static bool
cf_phase2_path(char path[MAXPGPATH], const char *shared_dir, const char *rel)
{
	int n = snprintf(path, MAXPGPATH, "%s/%s", shared_dir, rel);

	return n >= 0 && n < MAXPGPATH;
}

static bool
cf_phase2_probe_paths(char final_rel[MAXPGPATH], char tmp_rel[MAXPGPATH], int probe_owner,
					  uint64 nonce)
{
	int final_n;
	int tmp_n;

	final_n = snprintf(final_rel, MAXPGPATH, "%s/probe.%d", CLUSTER_CF_PHASE2_DIR, probe_owner);
	tmp_n = snprintf(tmp_rel, MAXPGPATH, "%s/probe.%d.tmp.%016" INT64_MODIFIER "x",
					 CLUSTER_CF_PHASE2_DIR, probe_owner, nonce);
	return final_n >= 0 && final_n < MAXPGPATH && tmp_n >= 0 && tmp_n < MAXPGPATH;
}

static bool
cf_phase2_ack_paths(char final_rel[MAXPGPATH], char tmp_rel[MAXPGPATH], int probe_owner,
					int responder, uint64 nonce)
{
	int final_n;
	int tmp_n;

	final_n = snprintf(final_rel, MAXPGPATH, "%s/ack.%d.%d.%016" INT64_MODIFIER "x",
					   CLUSTER_CF_PHASE2_DIR, probe_owner, responder, nonce);
	tmp_n = snprintf(tmp_rel, MAXPGPATH, "%s/ack.%d.%d.%016" INT64_MODIFIER "x.tmp",
					 CLUSTER_CF_PHASE2_DIR, probe_owner, responder, nonce);
	return final_n >= 0 && final_n < MAXPGPATH && tmp_n >= 0 && tmp_n < MAXPGPATH;
}

static void
cf_phase2_init_record(ClusterCfPhase2RecordV2 *rec, ClusterCfPhase2Kind kind, int probe_owner,
					  int responder, uint64 nonce)
{
	memset(rec, 0, sizeof(*rec));
	rec->magic = CLUSTER_CF_PHASE2_MAGIC;
	rec->version = CLUSTER_CF_PHASE2_VERSION;
	rec->kind = kind;
	rec->probe_owner_node = probe_owner;
	rec->responder_node = responder;
	rec->probe_nonce = nonce;
	INIT_CRC32C(rec->crc);
	COMP_CRC32C(rec->crc, rec, offsetof(ClusterCfPhase2RecordV2, crc));
	FIN_CRC32C(rec->crc);
}

/* Read one exact V2 tuple.  V1, trailing bytes and path/body drift fail shut. */
static bool
cf_phase2_read_record(const char *shared_dir, const char *rel, ClusterCfPhase2Kind expected_kind,
					  int expected_owner, int expected_responder, uint64 expected_nonce,
					  bool exact_nonce, ClusterCfPhase2RecordV2 *out)
{
	ClusterCfPhase2RecordV2 rec;
	char path[MAXPGPATH];
	char extra;
	pg_crc32c crc;
	bool exact_size = false;
	ssize_t n;
	int fd;

	if (!cf_phase2_path(path, shared_dir, rel))
		return false;
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	n = read(fd, &rec, sizeof(rec));
	if (n == (ssize_t)sizeof(rec)) {
		n = read(fd, &extra, 1);
		exact_size = (n == 0);
	}
	CloseTransientFile(fd);
	if (!exact_size || rec.magic != CLUSTER_CF_PHASE2_MAGIC
		|| rec.version != CLUSTER_CF_PHASE2_VERSION || rec.kind != expected_kind
		|| rec.reserved != 0 || rec.probe_owner_node != expected_owner
		|| rec.responder_node != expected_responder || rec.probe_nonce == 0
		|| (exact_nonce && rec.probe_nonce != expected_nonce)
		|| !cf_phase2_node_valid(rec.probe_owner_node))
		return false;
	if (rec.kind == CLUSTER_CF_P2_PROBE) {
		if (rec.responder_node != -1)
			return false;
	} else if (rec.kind == CLUSTER_CF_P2_ACK) {
		if (!cf_phase2_node_valid(rec.responder_node) || rec.responder_node == rec.probe_owner_node)
			return false;
	} else
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &rec, offsetof(ClusterCfPhase2RecordV2, crc));
	FIN_CRC32C(crc);
	if (crc != rec.crc)
		return false;

	if (out != NULL)
		*out = rec;
	return true;
}

/* Tuple-scoped staging prevents independent responders sharing a tmp file. */
static bool
cf_phase2_write_record(const char *shared_dir, const char *final_rel, const char *tmp_rel,
					   ClusterCfPhase2Kind kind, int probe_owner, int responder, uint64 nonce)
{
	ClusterCfPhase2RecordV2 rec;
	char final_path[MAXPGPATH];
	char tmp_path[MAXPGPATH];
	int fd;

	if (cf_phase2_read_record(shared_dir, final_rel, kind, probe_owner, responder, nonce, true,
							  NULL))
		return true;
	if (!cf_phase2_path(final_path, shared_dir, final_rel)
		|| !cf_phase2_path(tmp_path, shared_dir, tmp_rel))
		return false;

	cf_phase2_init_record(&rec, kind, probe_owner, responder, nonce);
	fd = OpenTransientFile(tmp_path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0) {
		if (errno != EEXIST
			|| !cf_phase2_read_record(shared_dir, tmp_rel, kind, probe_owner, responder, nonce,
									  true, NULL))
			return false;
	} else {
		if (write(fd, &rec, sizeof(rec)) != (ssize_t)sizeof(rec) || pg_fsync(fd) != 0) {
			CloseTransientFile(fd);
			unlink(tmp_path);
			return false;
		}
		if (CloseTransientFile(fd) != 0) {
			unlink(tmp_path);
			return false;
		}
	}

	if (durable_rename(tmp_path, final_path, LOG) != 0)
		return cf_phase2_read_record(shared_dir, final_rel, kind, probe_owner, responder, nonce,
									 true, NULL);
	return cf_phase2_read_record(shared_dir, final_rel, kind, probe_owner, responder, nonce, true,
								 NULL);
}

static bool
cf_phase2_parse_ack_name(const char *name, int probe_owner, int responder, uint64 *nonce)
{
	char prefix[64];
	const char *p;
	size_t len;
	uint64 value = 0;
	int i;

	if (snprintf(prefix, sizeof(prefix), "ack.%d.%d.", probe_owner, responder)
		>= (int)sizeof(prefix))
		return false;
	if (strncmp(name, prefix, strlen(prefix)) != 0)
		return false;
	p = name + strlen(prefix);
	len = strlen(p);
	if (len == 20 && strcmp(p + 16, ".tmp") == 0)
		len = 16;
	if (len != 16)
		return false;
	for (i = 0; i < 16; i++) {
		int digit;

		if (p[i] >= '0' && p[i] <= '9')
			digit = p[i] - '0';
		else if (p[i] >= 'a' && p[i] <= 'f')
			digit = p[i] - 'a' + 10;
		else
			return false;
		value = (value << 4) | (uint64)digit;
	}
	*nonce = value;
	return true;
}

#define CLUSTER_CF_PHASE2_GC_MAX 64
#define CLUSTER_CF_PHASE2_GC_NAME_MAX 64

typedef struct ClusterCfPhase2GcEntry {
	char name[CLUSTER_CF_PHASE2_GC_NAME_MAX];
	dev_t device;
	ino_t inode;
} ClusterCfPhase2GcEntry;

typedef struct ClusterCfPhase2GcSnapshot {
	int count;
	ClusterCfPhase2GcEntry entries[CLUSTER_CF_PHASE2_GC_MAX];
} ClusterCfPhase2GcSnapshot;

/*
 * Freeze a bounded set before publishing the caller's ACK.  A delayed old
 * writer therefore cannot discover a later writer's current-nonce final.
 */
static void
cf_phase2_capture_ack_pair(const char *shared_dir, int probe_owner, int responder,
						   uint64 incoming_nonce, ClusterCfPhase2GcSnapshot *snapshot)
{
	char dirpath[MAXPGPATH];
	DIR *dir;
	struct dirent *de;

	memset(snapshot, 0, sizeof(*snapshot));
	if (!cf_phase2_path(dirpath, shared_dir, CLUSTER_CF_PHASE2_DIR))
		return;
	dir = AllocateDir(dirpath);
	if (dir == NULL)
		return;
	while ((de = ReadDir(dir, dirpath)) != NULL) {
		ClusterCfPhase2GcEntry *entry;
		char rel[MAXPGPATH];
		char path[MAXPGPATH];
		struct stat st;
		uint64 nonce;

		if (!cf_phase2_parse_ack_name(de->d_name, probe_owner, responder, &nonce)
			|| nonce == incoming_nonce || snapshot->count >= CLUSTER_CF_PHASE2_GC_MAX
			|| strlen(de->d_name) >= CLUSTER_CF_PHASE2_GC_NAME_MAX)
			continue;
		if (snprintf(rel, sizeof(rel), "%s/%s", CLUSTER_CF_PHASE2_DIR, de->d_name)
			>= (int)sizeof(rel))
			continue;
		if (!cf_phase2_read_record(shared_dir, rel, CLUSTER_CF_P2_ACK, probe_owner, responder,
								   nonce, true, NULL))
			continue;
		if (!cf_phase2_path(path, shared_dir, rel) || lstat(path, &st) != 0 || !S_ISREG(st.st_mode))
			continue;
		entry = &snapshot->entries[snapshot->count++];
		strlcpy(entry->name, de->d_name, sizeof(entry->name));
		entry->device = st.st_dev;
		entry->inode = st.st_ino;
	}
	FreeDir(dir);
}

/* Delete only the pre-publication snapshot, never a later directory entry. */
static void
cf_phase2_cleanup_ack_snapshot(const char *shared_dir, int probe_owner, int responder,
							   uint64 current_nonce, const ClusterCfPhase2GcSnapshot *snapshot)
{
	int i;

	for (i = 0; i < snapshot->count; i++) {
		const ClusterCfPhase2GcEntry *entry = &snapshot->entries[i];
		ClusterCfPhase2RecordV2 probe;
		char rel[MAXPGPATH];
		char path[MAXPGPATH];
		struct stat st;
		uint64 nonce;

		if (!cluster_cf_phase2_read_probe(shared_dir, probe_owner, &probe)
			|| probe.probe_nonce != current_nonce)
			return;
		if (!cf_phase2_parse_ack_name(entry->name, probe_owner, responder, &nonce)
			|| nonce == current_nonce)
			continue;
		if (snprintf(rel, sizeof(rel), "%s/%s", CLUSTER_CF_PHASE2_DIR, entry->name)
				>= (int)sizeof(rel)
			|| !cf_phase2_read_record(shared_dir, rel, CLUSTER_CF_P2_ACK, probe_owner, responder,
									  nonce, true, NULL)
			|| !cf_phase2_path(path, shared_dir, rel) || lstat(path, &st) != 0
			|| st.st_dev != entry->device || st.st_ino != entry->inode)
			continue;
		(void)unlink(path);
	}
}

bool
cluster_cf_phase2_write_probe(const char *shared_dir, int probe_owner, uint64 probe_nonce)
{
	char final_rel[MAXPGPATH];
	char tmp_rel[MAXPGPATH];

	if (shared_dir == NULL || shared_dir[0] == '\0' || probe_nonce == 0
		|| !cf_phase2_node_valid(probe_owner))
		return false;
	ensure_p2_dir(shared_dir);
	if (!cf_phase2_probe_paths(final_rel, tmp_rel, probe_owner, probe_nonce))
		return false;
	return cf_phase2_write_record(shared_dir, final_rel, tmp_rel, CLUSTER_CF_P2_PROBE, probe_owner,
								  -1, probe_nonce);
}

bool
cluster_cf_phase2_read_probe(const char *shared_dir, int probe_owner, ClusterCfPhase2RecordV2 *out)
{
	char final_rel[MAXPGPATH];
	char tmp_rel[MAXPGPATH];

	if (shared_dir == NULL || out == NULL || !cf_phase2_node_valid(probe_owner)
		|| !cf_phase2_probe_paths(final_rel, tmp_rel, probe_owner, 1))
		return false;
	return cf_phase2_read_record(shared_dir, final_rel, CLUSTER_CF_P2_PROBE, probe_owner, -1, 0,
								 false, out);
}

bool
cluster_cf_phase2_write_ack(const char *shared_dir, int probe_owner, int responder,
							uint64 probe_nonce)
{
	ClusterCfPhase2GcSnapshot gc_snapshot;
	ClusterCfPhase2RecordV2 probe;
	char final_rel[MAXPGPATH];
	char tmp_rel[MAXPGPATH];

	if (shared_dir == NULL || shared_dir[0] == '\0' || probe_nonce == 0
		|| !cf_phase2_node_valid(probe_owner) || !cf_phase2_node_valid(responder)
		|| responder == probe_owner
		|| !cluster_cf_phase2_read_probe(shared_dir, probe_owner, &probe)
		|| probe.probe_nonce != probe_nonce
		|| !cf_phase2_ack_paths(final_rel, tmp_rel, probe_owner, responder, probe_nonce))
		return false;
	cf_phase2_capture_ack_pair(shared_dir, probe_owner, responder, probe_nonce, &gc_snapshot);
	if (!cf_phase2_write_record(shared_dir, final_rel, tmp_rel, CLUSTER_CF_P2_ACK, probe_owner,
								responder, probe_nonce))
		return false;

	/* A delayed old-nonce writer cannot leave a positive ACK after drift. */
	if (!cluster_cf_phase2_read_probe(shared_dir, probe_owner, &probe)
		|| probe.probe_nonce != probe_nonce) {
		char final_path[MAXPGPATH];

		if (cf_phase2_path(final_path, shared_dir, final_rel))
			(void)unlink(final_path);
		return false;
	}
	cf_phase2_cleanup_ack_snapshot(shared_dir, probe_owner, responder, probe_nonce, &gc_snapshot);
	return true;
}

bool
cluster_cf_phase2_read_exact_ack(const char *shared_dir, int probe_owner, int expected_responder,
								 uint64 expected_nonce)
{
	char final_rel[MAXPGPATH];
	char tmp_rel[MAXPGPATH];

	if (shared_dir == NULL || expected_nonce == 0 || !cf_phase2_node_valid(probe_owner)
		|| !cf_phase2_node_valid(expected_responder) || expected_responder == probe_owner
		|| !cf_phase2_ack_paths(final_rel, tmp_rel, probe_owner, expected_responder,
								expected_nonce))
		return false;
	return cf_phase2_read_record(shared_dir, final_rel, CLUSTER_CF_P2_ACK, probe_owner,
								 expected_responder, expected_nonce, true, NULL);
}

/*
 * cluster_cf_phase2_rendezvous -- symmetric nonce+ack handshake (see header).
 */
bool
cluster_cf_phase2_rendezvous(const char *shared_dir, int self_id, int peer_id, uint64 nonce,
							 int timeout_ms)
{
	TimestampTz deadline;

	if (shared_dir == NULL || shared_dir[0] == '\0' || !cf_phase2_node_valid(self_id)
		|| !cf_phase2_node_valid(peer_id) || self_id == peer_id || nonce == 0)
		return false;

	ensure_p2_dir(shared_dir);

	/* Publish my probe (durable_rename) so the peer can see it cross-node. */
	if (!cluster_cf_phase2_write_probe(shared_dir, self_id, nonce))
		return false;

	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), timeout_ms);

	for (;;) {
		int id;

		/* Every responder has its own pairwise ACK namespace. */
		for (id = 0; id < CLUSTER_MAX_NODES; id++) {
			ClusterCfPhase2RecordV2 peer_probe;

			if (id == self_id || !cf_phase2_node_valid(id))
				continue;
			if (cluster_cf_phase2_read_probe(shared_dir, id, &peer_probe))
				(void)cluster_cf_phase2_write_ack(shared_dir, id, self_id, peer_probe.probe_nonce);
		}

		/* Only the selected peer can complete this exact round trip. */
		if (cluster_cf_phase2_read_exact_ack(shared_dir, self_id, peer_id, nonce))
			return true;

		if (GetCurrentTimestamp() >= deadline)
			return false; /* no peer / no cross-node visibility */

		CHECK_FOR_INTERRUPTS();
		pg_usleep(CLUSTER_CF_PHASE2_POLL_US);
	}
}

/*
 * find_peer_node -- the first configured node id other than self.  The
 * rendezvous proves a property of the storage, so verifying against any one
 * peer establishes the contract; spec-5.6 scope is 2-node (one peer).
 */
static int
find_peer_node(void)
{
	int id;

	for (id = 0; id < CLUSTER_MAX_NODES; id++) {
		if (id == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(id) != NULL)
			return id;
	}
	return -1;
}

/*
 * Set true once this bootstrap's rendezvous proved a peer alive + the storage
 * cross-node verified.  Process-local: drives the multi-node role gate this
 * startup only (a fresh peer-ALIVE proof every boot, since the membership
 * service is not yet spawned during StartupXLOG).
 */
static bool cf_phase2_peer_verified = false;

bool
cluster_cf_phase2_peer_verified(void)
{
	return cf_phase2_peer_verified;
}

/*
 * cluster_cf_phase2_verify_or_fail -- backend entry (see header).
 */
void
cluster_cf_phase2_verify_or_fail(const char *pgdata)
{
	int peer_id;
	uint64 nonce;
	uint8 raw[8];

	if (!cluster_controlfile_shared_authority) {
		return;
	}
	if (!cluster_enabled || cluster_conf_node_count() <= 1)
		return; /* single-node: no cross-node contract needed */

	/*
	 * Always run a FRESH rendezvous on a multi-node bootstrap (do not short-
	 * circuit on a persisted CROSSNODE_VERIFIED): the role gate needs proof the
	 * peer is alive THIS run, not merely that the storage was verified once
	 * before.  A stale persisted contract must never authorize JOIN_READONLY
	 * against a peer that is actually down (it would skip a recovery the
	 * survivor should perform).
	 */
	peer_id = find_peer_node();
	if (peer_id < 0) {
		return; /* no peer configured -> gate fails closed */
	}

	if (!pg_strong_random(raw, sizeof(raw)))
		return; /* no fresh nonce -> leave unverified */
	memcpy(&nonce, raw, sizeof(nonce));

	/*
	 * Run the rendezvous bounded by cluster.cf_enqueue_timeout_ms.  The poll
	 * loop itself waits for the peer's probe to appear (the peer publishes it
	 * when it runs its own verify), so a peer that is merely slow to start is
	 * tolerated; a peer that never appears, or storage without cross-node
	 * rename visibility, times out and leaves the contract unverified (the
	 * role gate then fails closed -- never a false CROSSNODE_VERIFIED).
	 */
	if (cluster_cf_phase2_rendezvous(cluster_shared_data_dir, cluster_node_id, peer_id, nonce,
									 cluster_cf_enqueue_timeout_ms)) {
		/*
		 * Peer alive + storage cross-node verified this run.  Update only the
		 * contract STATE (preserving the identity anchor written at migration --
		 * never re-bind the authority sysid here) and flag peer-verified so the
		 * role gate grants JOIN_READONLY (read the authority, never write it
		 * during recovery; steady-state writes go through CF X after PM_RUN).
		 */
		cf_phase2_peer_verified = true;
		(void)cluster_cf_contract_update_state(pgdata, CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED);
		ereport(
			LOG,
			(errmsg("cluster cf phase-2: cross-node storage rename contract verified with node %d",
					peer_id)));
	}
}

/*
 * cluster_cf_phase2_respond_tick -- steady-state probe responder
 * (spec-5.6a D6 substrate repair).
 *
 *	The boot-time rendezvous above acks peer probes only while THIS node is
 *	itself inside its bootstrap loop, so a node crash-restarting into a live
 *	cluster could never get its fresh nonce acked: the live peer was
 *	steady-state and silent, and the rejoiner failed closed at the multi-node
 *	role gate ("cannot establish bootstrap shared control-file authority").
 *	This tick, run from the CSSD heartbeat cadence, acks any configured
 *	peer's probe whose nonce has not been acked yet, giving a rejoining peer
 *	its live-peer + cross-node-visibility proof (the same conclusion the
 *	concurrent-bootstrap rendezvous establishes; same files, same protocol).
 *
 *	Cheap and idempotent: one small read per configured peer per tick.  The
 *	exact writer returns immediately for an existing valid tuple, while still
 *	being able to restore an ephemeral ACK that disappeared.
 */
void
cluster_cf_phase2_respond_tick(void)
{
	int id;

	if (!cluster_controlfile_shared_authority)
		return;
	if (!cluster_enabled || cluster_conf_node_count() <= 1)
		return;
	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		return;

	for (id = 0; id < CLUSTER_MAX_NODES; id++) {
		uint64 nonce;

		if (id == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(id) == NULL)
			continue;
		{
			ClusterCfPhase2RecordV2 probe;

			if (!cluster_cf_phase2_read_probe(cluster_shared_data_dir, id, &probe))
				continue;
			nonce = probe.probe_nonce;
		}
		if (!cluster_cf_phase2_write_ack(cluster_shared_data_dir, id, cluster_node_id, nonce))
			continue;
	}
}
