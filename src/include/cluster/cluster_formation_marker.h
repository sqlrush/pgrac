/*-------------------------------------------------------------------------
 *
 * cluster_formation_marker.h
 *	  RF-ROOT P9 verification / cold-formation cold-formation ruling
 *	  (2026-08-19): durable quorum-majority COLD-FORMATION marker —
 *	  the 5.22 "observation window + lowest co-boot arbiter writes a
 *	  durable formation marker" mixed design (spec-5.22-online-join-
 *	  cold-formation.md §8 Q1 A+C, frozen).
 *
 *	  A 2-node (or N-node) cluster that cold-co-boots PAST the initial
 *	  epoch (clean shutdown + restart, or a full-outage crash co-boot)
 *	  has no live survivor and no coordinator.  The co-booting nodes
 *	  observe a quorum-stable window with no fresh slot past INITIAL
 *	  (slot epoch semantics: an UN-FORMED node publishes INITIAL — see
 *	  cluster_qvotec.c), pick the LOWEST co-boot node as arbiter, and
 *	  the arbiter writes a COMMITTED formation marker naming the exact
 *	  formation generation / epoch / arbiter identity / co-boot member
 *	  incarnations to a quorum-majority of voting disks (region 7,
 *	  per-member slots, coordinator-write pattern of the JCMK region).
 *	  Every co-boot member then publishes record_admitted(exact
 *	  incarnation) -> exact equality -> MEMBER.  D13 strictness is
 *	  preserved: no presented carve-out, floors come only from the
 *	  marker (quorum-majority durable commit evidence, INV-J7/J8).
 *
 *	  Arbiter takeover (5.22 F2, frozen): the marker carries the arbiter
 *	  identity + incarnation; a crashed mid-formation arbiter's slot ages
 *	  out of the window, the next-lowest co-boot node re-observes, writes
 *	  a HIGHER formation_generation takeover marker (monotonic, CAS-on-
 *	  generation); no two arbiters can be co-equal at the same generation.
 *	  A new postmaster NEVER inherits a previous marker's incarnation —
 *	  it raises the formation generation and commits its CURRENT boot
 *	  incarnation (per-member, read from the voting slots).
 *
 *	  Live-cluster never-seen / returning nodes are NOT admitted here —
 *	  they keep the ordinary JCMK join path; the root never seeds
 *	  membership.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_FORMATION_MARKER_H
#define CLUSTER_FORMATION_MARKER_H

#include "c.h"
#include "cluster/cluster_voting_disk_io.h" /* CLUSTER_VOTING_SLOT_BYTES */

/* 16 bytes — mirrors CLUSTER_RECONFIG_DEAD_BITMAP_BYTES (cluster_reconfig.h
 * cannot be included here: it includes this header for the mailbox type). */
#define CLUSTER_FORMATION_MARKER_BITMAP_BYTES 16

/*
 * Formation-marker slot layout (region 7 of the voting disk): one 512-byte
 * slot per node immediately after the epoch-ballot region
 * (offset = (6 * CLUSTER_MAX_NODES + 2 + node_id) * 512).  The arbiter
 * writes the SAME marker image into every co-boot member's slot (the
 * coordinator-write pattern of the JCMK region), so a marker reaches a
 * quorum-majority of DISKS per member slot, and a majority of MEMBER slots
 * carries the same commit identity (INV-J13 same-commit grouping).
 */
#define CLUSTER_FORMATION_MARKER_MAGIC UINT32_C(0x5047464D) /* "PGFM" */
#define CLUSTER_FORMATION_MARKER_VERSION UINT16_C(1)
#define CLUSTER_FORMATION_MARKER_PHASE_COMMITTED UINT8_C(2)

/* Compact per-member entry: uint8 node_id + uint64 incarnation. */
#define CLUSTER_FORMATION_MARKER_ENTRY_BYTES 9
/* Header bytes up to (and including) crc32c); entries follow at 72. */
#define CLUSTER_FORMATION_MARKER_HEADER_BYTES 72
/* Max co-boot members that fit the 512-byte slot. */
#define CLUSTER_FORMATION_MARKER_MAX_MEMBERS                                                       \
	((CLUSTER_VOTING_SLOT_BYTES - CLUSTER_FORMATION_MARKER_HEADER_BYTES)                           \
	 / CLUSTER_FORMATION_MARKER_ENTRY_BYTES)

typedef struct ClusterFormationCommitMarker {
	uint32 magic;	/* CLUSTER_FORMATION_MARKER_MAGIC */
	uint16 version; /* CLUSTER_FORMATION_MARKER_VERSION */
	uint8 phase;	/* CLUSTER_FORMATION_MARKER_PHASE_COMMITTED */
	uint8 _pad1;
	uint64 formation_generation; /* monotonic; takeover raises it */
	uint64 formation_epoch;		 /* the (possibly recovered) cluster epoch */
	uint64 arbiter_node;		 /* lowest co-boot node that wrote this */
	uint64 arbiter_incarnation;	 /* arbiter's CURRENT boot incarnation */
	uint64 commit_nonce;		 /* per-formation-attempt identity */
	uint8 admitted_nodes[CLUSTER_FORMATION_MARKER_BITMAP_BYTES]; /* co-boot bitmap */
	uint16 n_admitted;											 /* = popcount(admitted_nodes) */
	uint16 _pad2;
	uint32 crc32c; /* CRC32C over [magic .. _pad2] */
				   /* followed by n_admitted compact entries: uint8 node_id + uint64 incarnation */
} ClusterFormationCommitMarker;

extern void cluster_formation_marker_compute_crc(ClusterFormationCommitMarker *marker);
extern bool
cluster_formation_marker_encode(const ClusterFormationCommitMarker *marker,
								const uint64 *incarnation_by_node /* CLUSTER_MAX_NODES */,
								uint8 slot_bytes[CLUSTER_VOTING_SLOT_BYTES]);
extern bool
cluster_formation_marker_decode(const uint8 slot_bytes[CLUSTER_VOTING_SLOT_BYTES],
								ClusterFormationCommitMarker *marker,
								uint64 *incarnation_by_node /* CLUSTER_MAX_NODES, optional */);
extern bool cluster_formation_marker_validate(const uint8 slot_bytes[CLUSTER_VOTING_SLOT_BYTES],
											  ClusterFormationCommitMarker *out_decoded,
											  uint64 *out_incarnations);
extern uint64 cluster_formation_marker_incarnation_for(const uint64 *incarnation_by_node,
													   int32 node_id);

/*
 * qvotec formation-marker mailbox (mirrors the join-marker submit pattern):
 * the arbiter LMON stages a COMMITTED marker + the target member set; qvotec
 * writes the marker into every target member's region-7 slot on every disk,
 * requires the marker to land on a quorum-majority of disks per member slot,
 * then re-reads each target slot on every disk and ACKs only when a strict
 * majority of disks carries the EXACT image (majority readback).  A marker
 * that cannot reach majority on every target member fails closed.
 */
typedef struct ClusterFormationMarkerSubmitRequest {
	bool active;
	uint64 request_seq;
	uint64 completion_seq;
	uint32 result; /* bool success */
	uint8 marker_bytes[CLUSTER_VOTING_SLOT_BYTES];
	uint8 target_members[CLUSTER_FORMATION_MARKER_BITMAP_BYTES];
} ClusterFormationMarkerSubmitRequest;

#endif /* CLUSTER_FORMATION_MARKER_H */
