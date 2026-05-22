/*-------------------------------------------------------------------------
 *
 * cluster_visibility_inject.h
 *	  pgrac test-only visibility cluster path inject mechanism.
 *
 *	  spec-3.2 D5b (NEW;v0.3 N3 driver).
 *
 *	  Test fixtures need a way to push the visibility fork into its
 *	  cluster path despite spec-3.1 D4 ITL reader always returning
 *	  tt_slot_id=0 placeholder (real on-page origin/segment/slot lands
 *	  in spec-3.4).  D5b provides:
 *	    - test-only GUC cluster_test_force_visibility_cluster_path
 *	    - test-only SQL UDF cluster_test_inject_visibility_tt_ref(xid,
 *	      origin, segment, slot, epoch)
 *	    - shmem-resident inject table keyed by xid, value = ref
 *	    - lookup helper used by D5 fork entry
 *
 *	  ENABLE_INJECTION conditional:  production binary (no
 *	  --enable-injection-points configure) does not link this file's
 *	  body (all functions stub out;  GUC + UDF absent).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_visibility_inject.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_VISIBILITY_INJECT_H
#define CLUSTER_VISIBILITY_INJECT_H

#include "c.h"
#include "access/transam.h"
#include "cluster/cluster_tt_slot.h" /* ClusterUndoTTSlotRef */

/*
 * Lookup helper — called from D5 visibility fork entry when GUC
 * cluster_test_force_visibility_cluster_path is true.  Returns true +
 * fills *ref if xid is in the inject table.  In production builds
 * (no ENABLE_INJECTION) this is a no-op returning false.
 */
extern bool cluster_test_lookup_visibility_inject(TransactionId xid, ClusterUndoTTSlotRef *ref);

extern Size cluster_visibility_inject_shmem_size(void);
extern void cluster_visibility_inject_shmem_init(void);
extern void cluster_visibility_inject_shmem_register(void);

#endif /* CLUSTER_VISIBILITY_INJECT_H */
