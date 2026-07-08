/*-------------------------------------------------------------------------
 *
 * cluster_undo_resid.h
 *	  Shared-undo block resource identity + owner-as-master routing
 *	  contract -- spec-5.22a D1.
 *
 *	  Names an undo block (including the segment TT header block) as a
 *	  first-class cluster resource: (owner_node, undo_segment, block_no,
 *	  generation) encoded into the 16-byte ClusterResId wire format.  Undo
 *	  is the first owner-as-master resid class: the resource master IS the
 *	  owning instance (cluster_undo_resid_master returns owner_node), never
 *	  a GRD shard-hash master.  Hash-routing an undo resid through
 *	  cluster_grd_lookup_master / cluster_gcs_lookup_master is a fail-closed
 *	  error: the undo authority lives at the owner and a hash-derived master
 *	  would bypass it.
 *
 *	  generation carries the segment reuse generation (the segment header
 *	  wrap_count): a recycled segment reuses (undo_segment, block_no) for
 *	  different content, so a generation mismatch means a stale reference
 *	  and the caller must fail closed.  The owner membership epoch (owner
 *	  incarnation) is deliberately NOT part of the tag: it is a grant-time
 *	  ownership-validity attribute negotiated on the authority path, not a
 *	  static identity field.
 *
 *	  This header declares the PURE layer only (no elog / shmem / lock;
 *	  standalone-linkable for cluster_unit).  The data plane that consumes
 *	  this identity (grant / PI / block serving / recovery materialization /
 *	  retention) lands with later deliverables.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_resid.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.22a-undo-block-resource-identity.md (D1, §2.2 / §3.1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_RESID_H
#define CLUSTER_UNDO_RESID_H

#include "cluster/cluster_cf_enqueue.h"	 /* CLUSTER_CF_RESID_TYPE (collision check) */
#include "cluster/cluster_dl.h"			 /* CLUSTER_DL_RESID_TYPE (collision check) */
#include "cluster/cluster_grd.h"		 /* ClusterResId */
#include "cluster/cluster_hw.h"			 /* CLUSTER_HW_RESID_TYPE (collision check) */
#include "cluster/cluster_ir.h"			 /* CLUSTER_IR_RESID_TYPE (collision check) */
#include "cluster/cluster_ko.h"			 /* CLUSTER_KO_RESID_TYPE (collision check) */
#include "cluster/cluster_oid_lease.h"	 /* CLUSTER_OID_RESID_TYPE (collision check) */
#include "cluster/cluster_relmap_lock.h" /* CLUSTER_RELMAP_RESID_TYPE (collision check) */
#include "cluster/cluster_sequence.h"	 /* CLUSTER_SQ_RESID_TYPE (collision check) */
#include "cluster/cluster_ts.h"			 /* CLUSTER_TT_RESID_TYPE (collision check) */
#include "storage/lock.h"				 /* LOCKTAG_LAST_TYPE, DEFAULT_LOCKMETHOD */

/*
 * CLUSTER_UNDO_RESID_TYPE -- undo-block resource-id namespace marker.
 * 0xF9 is the next free slot after the contiguous 0xF0-0xF8 run.  Must be
 * above every PG LockTagType and distinct from every existing resid class.
 *
 * NB: 0xF3 is double-booked today by CLUSTER_DL_RESID_TYPE (cluster_dl.h)
 * and the backend-local CLUSTER_RAW_LAYOUT_RESID_TYPE
 * (cluster_shared_fs_block_device.c), which this cross-assert net cannot
 * name.  0xF9 collides with neither; cleaning up the 0xF3 double-booking
 * is a registered follow-up outside this header.
 */
#define CLUSTER_UNDO_RESID_TYPE 0xF9

StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "undo resid namespace must not collide with any PG LockTagType");
StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE != CLUSTER_SQ_RESID_TYPE,
				 "undo and SQ resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE != CLUSTER_CF_RESID_TYPE,
				 "undo and CF resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE != CLUSTER_HW_RESID_TYPE,
				 "undo and HW resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE != CLUSTER_DL_RESID_TYPE,
				 "undo and DL resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE != CLUSTER_TT_RESID_TYPE,
				 "undo and TT (tablespace-DDL) resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE != CLUSTER_IR_RESID_TYPE,
				 "undo and IR resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE != CLUSTER_KO_RESID_TYPE,
				 "undo and KO resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE != CLUSTER_OID_RESID_TYPE,
				 "undo and OID-lease resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_UNDO_RESID_TYPE != CLUSTER_RELMAP_RESID_TYPE,
				 "undo and RELMAP resid namespaces must be distinct");

/*
 * ClusterResId field mapping for the undo class (16 bytes, NOT memcpy --
 * cluster_undo_resid_encode/decode are the wire-ABI boundary):
 *
 *	field1       = undo_segment (per-instance undo segment number)
 *	field2       = block_no     (block number within the segment; block 0
 *	                             is the segment TT header block -- DATA and
 *	                             TT blocks share this one class)
 *	field3       = generation   (segment reuse generation == the segment
 *	                             header wrap_count; anti-ABA guard against
 *	                             whole-segment recycling)
 *	field4       = owner_node   (owning instance node id; uint16 on the
 *	                             wire, valid range [0, SCN_MAX_VALID_NODE_ID])
 *	type         = CLUSTER_UNDO_RESID_TYPE
 *	lockmethodid = DEFAULT_LOCKMETHOD
 *
 * The owner membership epoch (owner incarnation) is NOT in the tag; it is
 * negotiated at grant/serve time on the authority path.
 */

/*
 * cluster_undo_resid_encode -- build the undo-block resource id.
 */
extern void cluster_undo_resid_encode(int32 owner_node, uint32 undo_segment, uint32 block_no,
									  uint32 generation, ClusterResId *dst);

/*
 * cluster_undo_resid_decode -- split an undo resid back into its fields.
 * Must only be called on a resid whose type is CLUSTER_UNDO_RESID_TYPE.
 */
extern void cluster_undo_resid_decode(const ClusterResId *rid, int32 *owner_node,
									  uint32 *undo_segment, uint32 *block_no, uint32 *generation);

/*
 * cluster_undo_resid_is_undo -- class discriminator (type == 0xF9).
 */
extern bool cluster_undo_resid_is_undo(const ClusterResId *rid);

/*
 * cluster_undo_resid_master -- owner-as-master routing: returns the
 * encoded owner_node directly, NEVER a hash-derived master.  Undo
 * resources route exclusively through this function; the GRD/GCS
 * hash-master lookups reject the undo class (fail closed).
 */
extern int32 cluster_undo_resid_master(const ClusterResId *rid);

/*
 * cluster_undo_resid_generation_matches -- anti-ABA check: false means the
 * reference is stale (the segment was recycled) and the caller MUST fail
 * closed; it must never be treated as a match.
 */
extern bool cluster_undo_resid_generation_matches(const ClusterResId *rid,
												  uint32 expected_generation);

#endif /* CLUSTER_UNDO_RESID_H */
