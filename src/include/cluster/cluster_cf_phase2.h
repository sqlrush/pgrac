/*-------------------------------------------------------------------------
 *
 * cluster_cf_phase2.h
 *	  Cross-node storage rename-contract verification (spec-5.6 Phase-2).
 *
 *	  The shared pg_control authority is written with durable_rename; that
 *	  is torn-safe for a same-node reader, but a peer only sees the new
 *	  image if the shared storage provides cross-node close-to-open /
 *	  metadata visibility for renames (L368: non-buffer-managed shared-file
 *	  coherence is a hard property, not a given).  Phase-2 PROVES that
 *	  property at bring-up via a symmetric nonce+ack rendezvous conducted
 *	  through the shared storage itself (the most direct test of the
 *	  property the authority depends on -- not the interconnect):
 *
 *	    1. each node writes probe.<self> = {fresh nonce} with the same
 *	       write-tmp + fsync + durable_rename + dir-fsync the authority uses;
 *	    2. each node reads the peer's probe.<peer> (proving it can see the
 *	       peer's durable_rename'd write) and writes ack.<peer> echoing the
 *	       peer's nonce;
 *	    3. each node reads ack.<self> and checks it echoes its own nonce
 *	       (proving the peer saw this node's write, and this node sees the
 *	       peer's write back).
 *
 *	  When both directions verify, the node persists CROSSNODE_VERIFIED
 *	  (bound to the storage uuid) so the multi-node bootstrap gate
 *	  may proceed.  On timeout (no peer, or storage that does not give
 *	  cross-node rename visibility) it returns false and the gate fails
 *	  closed.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cf_phase2.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CF_PHASE2_H
#define CLUSTER_CF_PHASE2_H

/* Subdirectory under the shared global/ dir holding the probe/ack files. */
#define CLUSTER_CF_PHASE2_DIR "global/pgrac_cf_p2"

/*
 * Single-step I/O primitives (exposed for unit testing both roles in one
 * process against a temp shared dir; the real cross-node proof is the 2-node
 * TAP).  All writes are torn-safe (tmp + fsync + durable_rename + dir-fsync);
 * all reads return false when the file is absent/short/CRC-bad.
 */
extern bool cluster_cf_phase2_write_probe(const char *shared_dir, int self_id, uint64 nonce);
extern bool cluster_cf_phase2_read_probe(const char *shared_dir, int peer_id, uint64 *out_nonce);
extern bool cluster_cf_phase2_write_ack(const char *shared_dir, int peer_id, uint64 echo_nonce);
extern bool cluster_cf_phase2_read_ack(const char *shared_dir, int self_id, uint64 *out_echo);

/*
 * Steady-state probe responder (spec-5.6a): acks any configured peer's
 * fresh probe so a node crash-restarting into a live cluster can complete
 * its bootstrap rendezvous.  Called from the CSSD heartbeat cadence; no-op
 * unless the shared authority is on and the cluster is multi-node.
 */
extern void cluster_cf_phase2_respond_tick(void);

/*
 * cluster_cf_phase2_rendezvous -- run the symmetric nonce+ack handshake from
 * this node against `peer_id` over `shared_dir`, bounded by timeout_ms.
 * Returns true iff BOTH directions verified (this node saw the peer's probe,
 * and the peer acked this node's probe with the matching nonce).  Does not
 * persist anything and does not ereport; the caller persists the contract and
 * decides the fail-closed action.  `nonce` is a caller-supplied fresh nonce
 * (so the loop body stays free of randomness for deterministic resume).
 */
extern bool cluster_cf_phase2_rendezvous(const char *shared_dir, int self_id, int peer_id,
										 uint64 nonce, int timeout_ms);

/*
 * cluster_cf_phase2_verify_or_fail -- backend entry called from StartupXLOG
 * before the bootstrap role gate, when this is a multi-node cluster whose
 * storage contract is not yet CROSSNODE_VERIFIED.  Waits (bounded) for a peer
 * to become alive (CSSD), runs the rendezvous against it, and on success
 * persists CROSSNODE_VERIFIED for this node (bound to the storage uuid).  A
 * no-op when the authority is off, the cluster is single-node, or the contract
 * is already verified.  Never throws: on failure it simply leaves the contract
 * unverified so the role gate fails closed.
 */
extern void cluster_cf_phase2_verify_or_fail(const char *pgdata);

/*
 * cluster_cf_phase2_peer_verified -- did this bootstrap's Phase-2 rendezvous
 * prove a peer ALIVE and the shared storage cross-node verified?  The
 * multi-node bootstrap role gate uses this (NOT CSSD, which is not yet spawned
 * during StartupXLOG -- it is a phase-4 / post-PM_RUN aux process) to authorize
 * the JOIN_READONLY role.  It is strictly a peer-ALIVE proof, never a
 * sole-liveness proof: a dead-peer survivor cannot use it to take owner
 * authority and write the shared control file -- with no live peer the
 * bootstrap fails closed (no owner-write path during recovery yet).
 * Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
 */
extern bool cluster_cf_phase2_peer_verified(void);

#endif /* CLUSTER_CF_PHASE2_H */
