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

/*
 * ClusterCfPhase2Record -- on-disk probe/ack file layout.  `nonce` carries the
 * prober's fresh nonce in a probe and the echoed nonce in an ack.
 */
#define CLUSTER_CF_PHASE2_MAGIC 0x43465032 /* 'CFP2' */
#define CLUSTER_CF_PHASE2_VERSION 1

typedef struct ClusterCfPhase2Record {
	uint32 magic;
	uint32 version;
	uint64 nonce;
	pg_crc32c crc; /* over [0, offsetof(crc)) */
} ClusterCfPhase2Record;

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
write_record(const char *shared_dir, const char *rel, uint64 nonce)
{
	ClusterCfPhase2Record rec;
	char path[MAXPGPATH];
	char tmp[MAXPGPATH];
	int fd;

	memset(&rec, 0, sizeof(rec));
	rec.magic = CLUSTER_CF_PHASE2_MAGIC;
	rec.version = CLUSTER_CF_PHASE2_VERSION;
	rec.nonce = nonce;
	INIT_CRC32C(rec.crc);
	COMP_CRC32C(rec.crc, &rec, offsetof(ClusterCfPhase2Record, crc));
	FIN_CRC32C(rec.crc);

	snprintf(path, sizeof(path), "%s/%s", shared_dir, rel);
	snprintf(tmp, sizeof(tmp), "%s/%s.tmp", shared_dir, rel);

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		return false;
	if (write(fd, &rec, sizeof(rec)) != (int)sizeof(rec) || pg_fsync(fd) != 0) {
		CloseTransientFile(fd);
		unlink(tmp);
		return false;
	}
	CloseTransientFile(fd);

	if (durable_rename(tmp, path, LOG) != 0) {
		unlink(tmp);
		return false;
	}
	return true;
}

/*
 * read_record -- read + validate one record from <shared_dir>/<rel>.  Returns
 * false (out untouched) when the file is absent/short/foreign/CRC-bad.
 */
static bool
read_record(const char *shared_dir, const char *rel, uint64 *out_nonce)
{
	ClusterCfPhase2Record rec;
	char path[MAXPGPATH];
	pg_crc32c crc;
	int fd;
	int n;

	snprintf(path, sizeof(path), "%s/%s", shared_dir, rel);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	n = read(fd, &rec, sizeof(rec));
	CloseTransientFile(fd);
	if (n != (int)sizeof(rec) || rec.magic != CLUSTER_CF_PHASE2_MAGIC
		|| rec.version != CLUSTER_CF_PHASE2_VERSION)
		return false;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &rec, offsetof(ClusterCfPhase2Record, crc));
	FIN_CRC32C(crc);
	if (crc != rec.crc)
		return false;

	*out_nonce = rec.nonce;
	return true;
}

bool
cluster_cf_phase2_write_probe(const char *shared_dir, int self_id, uint64 nonce)
{
	char rel[MAXPGPATH];

	snprintf(rel, sizeof(rel), "%s/probe.%d", CLUSTER_CF_PHASE2_DIR, self_id);
	return write_record(shared_dir, rel, nonce);
}

bool
cluster_cf_phase2_read_probe(const char *shared_dir, int peer_id, uint64 *out_nonce)
{
	char rel[MAXPGPATH];

	snprintf(rel, sizeof(rel), "%s/probe.%d", CLUSTER_CF_PHASE2_DIR, peer_id);
	return read_record(shared_dir, rel, out_nonce);
}

bool
cluster_cf_phase2_write_ack(const char *shared_dir, int peer_id, uint64 echo_nonce)
{
	char rel[MAXPGPATH];

	/* ack.<peer_id> = "this node's ack of peer_id's probe", echoing its nonce. */
	snprintf(rel, sizeof(rel), "%s/ack.%d", CLUSTER_CF_PHASE2_DIR, peer_id);
	return write_record(shared_dir, rel, echo_nonce);
}

bool
cluster_cf_phase2_read_ack(const char *shared_dir, int self_id, uint64 *out_echo)
{
	char rel[MAXPGPATH];

	/* ack.<self_id> = "the peer's ack of my probe"; out_echo must equal my nonce. */
	snprintf(rel, sizeof(rel), "%s/ack.%d", CLUSTER_CF_PHASE2_DIR, self_id);
	return read_record(shared_dir, rel, out_echo);
}

/*
 * cluster_cf_phase2_rendezvous -- symmetric nonce+ack handshake (see header).
 */
bool
cluster_cf_phase2_rendezvous(const char *shared_dir, int self_id, int peer_id, uint64 nonce,
							 int timeout_ms)
{
	TimestampTz deadline;
	bool acked_peer = false;
	bool my_ack_ok = false;

	if (shared_dir == NULL || shared_dir[0] == '\0')
		return false;

	ensure_p2_dir(shared_dir);

	/* Publish my probe (durable_rename) so the peer can see it cross-node. */
	if (!cluster_cf_phase2_write_probe(shared_dir, self_id, nonce))
		return false;

	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), timeout_ms);

	for (;;) {
		/* Direction peer->self: I can see the peer's probe -> ack it. */
		if (!acked_peer) {
			uint64 peer_nonce;

			if (cluster_cf_phase2_read_probe(shared_dir, peer_id, &peer_nonce)) {
				if (cluster_cf_phase2_write_ack(shared_dir, peer_id, peer_nonce))
					acked_peer = true;
			}
		}

		/* Direction self->peer: the peer acked my probe and I can see the ack. */
		if (!my_ack_ok) {
			uint64 echo;

			if (cluster_cf_phase2_read_ack(shared_dir, self_id, &echo) && echo == nonce)
				my_ack_ok = true;
		}

		if (acked_peer && my_ack_ok)
			return true; /* both directions verified */

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

	if (!cluster_controlfile_shared_authority)
		return;
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
	if (peer_id < 0)
		return; /* no peer configured -> gate fails closed */

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
 *	Cheap and idempotent: one small read per configured peer per tick, an
 *	ack write only when a fresh probe appears.  Acking a stale leftover
 *	probe is harmless -- no one waits on it, and a rebooting peer always
 *	publishes a fresh nonce first.
 */
void
cluster_cf_phase2_respond_tick(void)
{
	static uint64 last_acked_nonce[CLUSTER_MAX_NODES]; /* CSSD process-local */
	static bool last_acked_valid[CLUSTER_MAX_NODES];
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
		if (!cluster_cf_phase2_read_probe(cluster_shared_data_dir, id, &nonce))
			continue;
		if (last_acked_valid[id] && last_acked_nonce[id] == nonce)
			continue;
		if (cluster_cf_phase2_write_ack(cluster_shared_data_dir, id, nonce)) {
			last_acked_nonce[id] = nonce;
			last_acked_valid[id] = true;
		}
	}
}
