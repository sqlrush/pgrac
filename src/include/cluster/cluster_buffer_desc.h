/*-------------------------------------------------------------------------
 *
 * cluster_buffer_desc.h
 *	  pgrac cluster buffer descriptor cluster fields typedefs +
 *	  invariants.
 *
 *	  Stage 1.6 ships only the enums + INVALID_BUFFER_ID / INVALID_NODE_ID
 *	  sentinels.  All cluster fields on every BufferDesc are written to
 *	  placeholder values at InitBufferPool / InitLocalBuffers time
 *	  (via ClusterInitBufferDescFields, defined in buf_internals.h
 *	  after the BufferDesc struct) and remain in those placeholder
 *	  states for the duration of stage 1.6; the actual PCM lock state
 *	  machine (AD-002), CR chain construction (AD-006), PI buffer
 *	  creation (#84), Cache Fusion protocol (#17), and GRD master
 *	  cache (#11/#12) all land at Stage 2-3 真值激活 spec.
 *
 *	  Each BufferDesc in pgrac 1.6+ contains ~76B of cluster fields:
 *	    - 12B hot tail in 64B BufferDesc segment 1 ([52, 64); buffer_type / pcm_state
 *	      / pi_flags / cluster_padding_1 / block_scn).
 *	    - 64B cold body in 64B BufferDesc segment 2 ([64, 128); cr_chain_head /
 *	      cr_chain_next / cr_scn / pi_buf_id / pi_lsn / grd_master_node
 *	      / grd_master_seq / cf_state / cf_owner_node / cf_request_count
 *	      / pcm_lock / pi_created_at).
 *	  packed into the BufferDescPadded slot whose padded size grew from
 *	  PG-vanilla 64B to pgrac 128B (BUFFERDESC_PAD_TO_SIZE 64 -> 128 in
 *	  USE_PGRAC_CLUSTER mode -- see buf_internals.h).  PG-original
 *	  fields occupy [0, 52) on PG 16.13 (BufferTag 20B + buf_id 4B +
 *	  state 4B + wait_backend 4B + freeNext 4B + content_lock 16B); the
 *	  cluster hot tail fits in the remaining 12B of 64B BufferDesc segment 1 so
 *	  existing PG hot paths still read 1 cache line.  block_scn is the
 *	  Stage 2-3 visibility hot field and stays in 64B BufferDesc segment 1 by
 *	  design (PIVOT B 2026-05-02 user approve); cr_chain_head is cold
 *	  (CR construction path only) and lives at the 64B BufferDesc segment 2
 *	  boundary.
 *
 *	  Critical layout invariant (Q2 + Q7 audit + PIVOT B):
 *	    PG bufmgr.c:AssertNotCatalogBufferLock reverse-derefs BufferDesc
 *	    from an LWLock pointer using
 *	      bufHdr = (BufferDesc *) ((char *) lock - offsetof(BufferDesc,
 *	                                                       content_lock))
 *	    which uses the offsetof() macro and therefore adapts to whatever
 *	    PG version's actual content_lock offset is (36 on PG 16.13).
 *	    The Q2 invariant is that cluster fields appear *after*
 *	    content_lock so the reverse-deref keeps yielding a valid
 *	    BufferDesc pointer.  StaticAssertDecls in buf_internals.h
 *	    enforce this semantically (block_scn stays in 64B BufferDesc segment 1;
 *	    cr_chain_head starts 64B BufferDesc segment 2; cluster fields follow
 *	    content_lock).  Any future reorder breaking these semantics
 *	    fails to compile.
 *
 *	  Critical local buffer note (Q1 §1.4 + Q7 audit):
 *	    PG localbuf.c:595 calls
 *	      LocalBufferDescriptors = (BufferDesc *) calloc(nbufs, sizeof(BufferDesc))
 *	    which zero-fills.  Most cluster fields' placeholder values are
 *	    legitimately 0 (PCM_STATE_N / BUF_TYPE_CURRENT / CF_STATE_NONE
 *	    / InvalidScn / etc.), but cr_chain_head / cr_chain_next /
 *	    pi_buf_id MUST be -1 (INVALID_BUFFER_ID); 0 is a *valid*
 *	    buffer_id and would mislead Stage 3 CR-path reads of local
 *	    buffers.  pcm_lock also MUST be LWLockInitialize'd explicitly --
 *	    calloc 0 is not a valid LWLock state.  This is why
 *	    ClusterInitBufferDescFields() exists and is called from BOTH
 *	    InitBufferPool() and InitLocalBuffers() (PGRAC MODIFICATIONS
 *	    14th and 16th respectively).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_buffer_desc.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.6-buffer-descriptor.md
 *	  Design: docs/buffer-pool-design.md v1.1 §4.3 (ClusterBufferDesc
 *	  field layout) + §12 (memory budget +0.78%).
 *	  AD-002 (PCM lock state machine; pcm_state field).
 *	  AD-005 (Cache Fusion full; cf_state / cf_owner_node fields).
 *	  AD-006 (CR block construction; cr_chain_head / cr_scn fields).
 *	  #84 (PI Read Fast-Path; pi_buf_id / pi_lsn fields).
 *	  #11 / #12 (GRD master cache; grd_master_node / grd_master_seq
 *	  fields).
 *
 *	  Header dependency note: the BufferDesc struct itself plus the
 *	  inline ClusterInitBufferDescFields helper live in
 *	  storage/buf_internals.h (because the helper accesses struct
 *	  fields and must follow the struct definition).  This file is
 *	  included by buf_internals.h, not the other way around, so we
 *	  keep this header free of buf_internals.h includes to avoid
 *	  circular dependency.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_BUFFER_DESC_H
#define CLUSTER_BUFFER_DESC_H

#include "c.h" /* uint8 / uint16 */


/*
 * BufferType -- buffer payload type (Oracle current / CR / PI / SCUR / XCUR taxonomy).
 *
 *	Stage 1.6 ships only the typedef; all buffers are zero-init'd to
 *	BUF_TYPE_CURRENT (= 0).  Stage 3 (AD-006 第五轮) implements the
 *	actual CR / PI buffer types when CR chain construction lands.
 *
 *	spec-2.31 D5 v0.4 extends the enum with SCUR (Shared current copy,
 *	PCM=S) and XCUR (eXclusive current copy, PCM=X).  Updated by the
 *	bufmgr LockBuffer hook on successful PCM acquire (HC66:  set only
 *	on the success path, monotone hint — last successful PCM ownership
 *	mode, not real-time PCM holding state).  pcm_state mirrors real-time
 *	PCM via cluster_pcm_lock_query; buffer_type stays through release.
 *	cluster_debug.c `buffer_type_count` surfaces 5 (was 3 in spec-1.6).
 */
typedef enum {
	BUF_TYPE_CURRENT = 0, /* current block copy (zero-init occupies) */
	BUF_TYPE_CR = 1,	  /* Consistent Read copy (read-only, on-demand) */
	BUF_TYPE_PI = 2,	  /* Past Image (kept after X-lock let-go) */
	BUF_TYPE_SCUR = 3,	  /* Shared current copy (PCM=S; spec-2.31 D5 v0.4) */
	BUF_TYPE_XCUR = 4	  /* eXclusive current copy (PCM=X; spec-2.31 D5 v0.4) */
} BufferType;


/*
 * PcmState -- Parallel Cache Management lock state (AD-002 N/S/X).
 *
 *	Stage 1.6 ships only the typedef; all buffers are zero-init'd to
 *	PCM_STATE_N (= 0; no PCM lock held -- PG-native semantics).
 *	Stage 2 (#15) implements the 9-rule N <-> S <-> X transition state
 *	machine.  has_pi orthogonal flag lives in pi_flags below.
 */
typedef enum {
	PCM_STATE_N = 0, /* Null - no PCM lock (zero-init occupies) */
	PCM_STATE_S = 1, /* Shared */
	PCM_STATE_X = 2, /* eXclusive */
	/*
	 * PGRAC: spec-5.2 §3.5 D11 (writer-transfer-revoke).
	 * Transient BufferDesc.pcm_state marker (NOT a PCM lock mode, never
	 * carried on the wire / GRD): this backend installed a remote X holder's
	 * READ-IMAGE in response to its OWN write (N->X) request, because the
	 * holder deferred the writer-transfer (it still has an uncommitted ITL
	 * slot).  The backend holds NO PCM lock — for ownership, covering, the
	 * re-declare scan, and eviction this state behaves exactly like
	 * PCM_STATE_N (cluster_pcm_mode_covers + the scan already treat any
	 * non-{S,X} value as "no lock", and the content-lock unlock hook clears
	 * it back to N).  Its ONLY effect: the cluster_itl forward-write path
	 * fails closed (53R9H, retryable) rather than mutate a non-owned copy
	 * (Rule 8.A).  The contended-row writer waits in the heap AM and
	 * re-acquires real X (overwriting this marker) before it ever writes;
	 * only a writer whose own target row was NOT contended reaches the guard
	 * with this marker still set.
	 */
	PCM_STATE_READ_IMAGE = 3
} PcmState;


/*
 * CacheFusionState -- Cache Fusion protocol per-buffer state
 *	(AD-005 + #17).
 *
 *	Stage 1.6 ships only the typedef; all buffers are zero-init'd to
 *	CF_STATE_NONE (= 0).  Stage 2 (#17) implements the actual transfer
 *	state machine (CONVERTING / TRANSFERRING / PI_PUBLISHING).
 */
typedef enum {
	CF_STATE_NONE = 0,		   /* static state (zero-init occupies) */
	CF_STATE_CONVERTING = 1,   /* PCM lock conversion in progress */
	CF_STATE_TRANSFERRING = 2, /* block byte transfer in progress */
	CF_STATE_PI_PUBLISHING = 3 /* PI being published on X-lock let-go */
} CacheFusionState;


/*
 * BufferFlags -- 1-byte bitfield for cluster buffer flags.
 *
 *	Stage 1.6 ships only BUF_FLAG_HAS_PI; more bits reserved for
 *	Stage 2-3 (BUF_FLAG_DIRTY_REMOTE / BUF_FLAG_PINNED_BY_CF / etc).
 */
typedef enum {
	BUF_FLAG_HAS_PI = 0x01 /* this current buffer has a PI buffer */
						   /* 0x02 .. 0x80 reserved for Stage 2-3 */
} BufferFlags;


/*
 * INVALID_BUFFER_ID -- the "no buffer" sentinel for cr_chain_head /
 *	cr_chain_next / pi_buf_id fields.
 *
 *	-1 matches PG's FREENEXT_END_OF_LIST convention (buf_internals.h).
 *	PG buffer indexes are non-negative (0 .. NBuffers-1) so -1
 *	unambiguously means "absent / no buffer linked".
 *
 *	CRITICAL: zero is a *valid* buffer_id (the first buffer slot), so
 *	a calloc-zero default would mislead Stage 3 CR-path code into
 *	reading buffer 0 as the chain head / PI buffer.  Callers MUST use
 *	ClusterInitBufferDescFields() (in buf_internals.h) to write -1
 *	explicitly -- never rely on calloc / MemSet zero-fill for these
 *	three fields.
 */
#define INVALID_BUFFER_ID ((int)-1)


/*
 * INVALID_NODE_ID -- the "no node assigned" sentinel for grd_master_node
 *	field (and Stage 2 cf_owner_node).
 *
 *	BufferDesc.grd_master_node is uint16 so the cluster_guc.c sentinel
 *	`int cluster_node_id = -1` (signed) cannot apply.  pgrac caps
 *	cluster node count at 16 (AD-012 例外 10) so real node ids are
 *	1..16; 0 is permanently reserved as "absent".  This makes
 *	INVALID_NODE_ID = 0 natural for zero-init occupancy (matches
 *	grd_master_seq=0, cf_owner_node=0, cf_request_count=0 placeholder
 *	pattern).
 *
 *	NOTE: this is *different* sentinel value from cluster_guc.c's
 *	`int cluster_node_id = -1` GUC default ("未配置").  The buffer-side
 *	uses uint16 = 0 = "未分配 master"; the GUC-side uses int = -1 =
 *	"未配置 node id".  Same conceptual "absent" but different sentinel
 *	value due to type width (signed vs unsigned).  Spec-1.6 §1.4
 *	example #4 originally claimed they shared the same sentinel value
 *	-- that was an inaccuracy caught at implementation time; both
 *	still mean "未" but the values differ.
 */
#define INVALID_NODE_ID ((uint16)0)


#endif /* CLUSTER_BUFFER_DESC_H */
