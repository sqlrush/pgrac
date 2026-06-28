/*-------------------------------------------------------------------------
 *
 * cluster_write_fence.h
 *	  pgrac internal cooperative write-fence -- a shared-storage write is
 *	  allowed only while this node proves it holds the current cluster
 *	  authority; a stale / superseded / lease-expired / self-fenced node
 *	  fails closed (spec-4.12, split-brain recovery guard).
 *
 *	  Scope (spec-4.12 §1, FROZEN v1.0): COOPERATIVE fence -- every
 *	  cooperative shared-storage write path (cluster_smgr write / extend /
 *	  zeroextend / truncate / unlink / create, the WAL write entry, the
 *	  4.10/4.11 recovery write-back) consults the local atomic write-fence
 *	  token before writing; a fenced node fails closed.  This is NOT
 *	  hardware fencing (SCSI-3 PR / STONITH / cloud) -- that is a separate
 *	  failure domain forwarded to 4.12b / spec-2.X (AD-013).
 *
 *	  The authoritative epoch lives in a durable fence marker on the voting
 *	  disk; qvotec (the sole voting-disk I/O owner) polls it, requires a
 *	  quorum-majority of disks to agree on the same marker tuple, orders by
 *	  fence_epoch (monotonic), and refreshes the local token + lease.  A
 *	  partitioned node cannot refresh -> its lease expires -> writes fail
 *	  closed.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_write_fence.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.12-cooperative-write-fence.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WRITE_FENCE_H
#define CLUSTER_WRITE_FENCE_H

/*
 * cluster_write_fence_decide -- the PURE cooperative write-fence judge (spec-4.12
 * D3).  A shared-storage write is allowed iff enforcement is on AND the local
 * write-fence token proves this node is authorized:
 *
 *	enforcement_on    cluster.write_fence_enforcement is on (off / dev -> escape
 *	                  hatch, always allowed).
 *	region_attached   the write-fence token shmem region is attached (L110: a
 *	                  detached region fails closed -- never fail-open).
 *	epoch_current     cluster_epoch_get_current().
 *	authorized_epoch  the fence_epoch the qvotec poll locked in from the durable
 *	                  voting-disk marker.  Compared with EXACT == (never >=): a
 *	                  stale node's worst risk is "thinking its epoch is current".
 *	now_us            current time; lease_expire_us the qvotec-refreshed lease.
 *	                  now >= expire means this node failed to refresh (partition)
 *	                  -> fail closed.
 *	self_fenced       this node is in the marker's fenced_dead_bitmap.
 *
 * PURE so every branch is unit-pinned; the .c wrapper reads the live runtime +
 * shmem token and calls this.
 */
static inline bool
cluster_write_fence_decide(bool enforcement_on, bool region_attached, uint64 epoch_current,
						   uint64 authorized_epoch, uint64 now_us, uint64 lease_expire_us,
						   bool self_fenced)
{
	if (!enforcement_on)
		return true; /* escape hatch: off / dev */
	if (!region_attached)
		return false; /* L110 sentinel: fail-closed, never fail-open */
	if (self_fenced)
		return false; /* this node is in the marker's fenced_dead_bitmap */
	if (epoch_current != authorized_epoch)
		return false; /* exact == (P0b / user 修正 3): a stale node must not write */
	if (now_us >= lease_expire_us)
		return false; /* lease expired: failed to refresh (partition) */
	return true;
}

/*
 * cluster_write_fence_grace_before_engage -- spec-4.12b D3 (Q3=A) bring-up grace.
 *	Returns true when the HOT write gate should short-circuit to ALLOWED because
 *	the cluster is still in the boot -> first-authority window: enforcement is on,
 *	the token region is attached, but no authority has ever latched (fence_engaged
 *	== false).  This is not an unguarded fail-open window -- the qvotec quorum
 *	backend gate (spec-2.6) already blocks writes when there is no quorum, so the
 *	write-fence need not add a second hard-block before its first poll.  Once an
 *	authority (or a durable self-fence) latches fence_engaged, the grace ends and
 *	the hot gate enforces strictly via cluster_write_fence_decide.
 *
 *	A DETACHED region (region_attached == false) gets NO grace: it falls through
 *	to the pure judge, which fails closed (L110).  PURE so the latch transition is
 *	unit-pinned; ONLY cluster_write_fence_allowed() (the hot gate) consults this --
 *	the direct durable checks (verify_durable / startup_self_check) never do (P1).
 *
 *	spec-4.12b Hardening v1.0.2 (self-fence grace race, defense-in-depth): a node
 *	whose token already says self_fenced gets NO grace either -- it falls through to
 *	the pure judge, which fails closed (self_fenced).  This is the SECOND layer behind
 *	the engage-first publish order in cluster_write_fence_refresh_from_marker(): the
 *	primary fix engages the latch before publishing a self-fencing token, so whenever
 *	self_fenced is observable fence_engaged is already set; this !self_fenced guard
 *	survives a future reorder regression of that publish path.
 */
static inline bool
cluster_write_fence_grace_before_engage(bool enforcement_on, bool region_attached,
										bool fence_engaged, bool self_fenced)
{
	return enforcement_on && region_attached && !fence_engaged && !self_fenced;
}

/*
 * ClusterFenceMarker -- the durable fence marker (spec-4.12 D1).  Embedded in the
 * voting slot's _reserved1[368] (offset 128; the slot CRC over bytes 0..507 already
 * protects it, so no on-disk ABI / torn-write change).  Fixed 64 bytes
 * (CLUSTER_FENCE_MARKER_BYTES) -- pinned by static asserts in cluster_qvotec.c.
 *
 *	magic == 0 (or != CLUSTER_FENCE_MARKER_MAGIC) means NO marker (no fence).
 *	fence_epoch is the AUTHORITATIVE ordering key (monotonic -- epoch only
 *	increases); fence_generation tie-breaks the same epoch.  fence_event_id is the
 *	ReconfigEvent siphash -- IDENTITY ONLY, never used for max()/sort (it is a hash,
 *	not monotonic; spec-4.12 P0b).  fenced_dead_bitmap carries the full multi-dead
 *	set so a 3-node reconfig declaring 2 dead is complete (user 修正 4).
 */
#define CLUSTER_FENCE_MARKER_MAGIC 0x46454E43U /* 'FENC' */
#define CLUSTER_FENCE_MARKER_VERSION 1U
#define CLUSTER_FENCE_MARKER_BYTES 64
#define CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES 16

/*
 * spec-4.12b D2/P1: the pristine (never-reconfigured) baseline's issuer.  A fixed
 * sentinel (not a real node id) so EVERY node's pristine baseline is the SAME tuple
 * -> trivially reaches quorum-majority.  For a reconfigured membership the baseline
 * issuer is last_applied.coordinator_node_id (the SAME issuer the fence marker used,
 * cluster_reconfig.c:607) so the baseline republish is tuple-identical to the fence
 * marker -- one authoritative tuple per membership epoch, never a per-issuer split.
 */
#define CLUSTER_FENCE_BASELINE_INITIAL_ISSUER (-1)

/*
 * spec-4.12b D1: marker kind -- OBSERVABILITY ONLY.  It is NEVER a priority/sort
 * key and NEVER part of tuple identity (cluster_fence_marker_tuple_equal); the
 * authority ordering stays (fence_epoch, fence_generation) and the quorum-majority
 * equality stays the full 4.12 identity (P0-1).  It occupies one of the marker's
 * pad bytes, so the marker stays CLUSTER_FENCE_MARKER_BYTES.  An old (4.12) reader
 * sees the byte as pad==0 == FENCE -- the safe default: it treats every marker as
 * an authoritative membership marker regardless of kind.
 */
typedef enum ClusterFenceMarkerKind {
	CLUSTER_FENCE_MARKER_KIND_FENCE = 0,	/* reconfig-issued fence (4.12 default) */
	CLUSTER_FENCE_MARKER_KIND_BASELINE = 1, /* spec-4.12b steady-state membership republish */
	CLUSTER_FENCE_MARKER_KIND_NODE_REMOVED
	= 2, /* spec-5.18: permanent-removal fence (observability) */
} ClusterFenceMarkerKind;

typedef struct ClusterFenceMarker {
	uint32 magic;			 /* CLUSTER_FENCE_MARKER_MAGIC, or 0 = no marker */
	uint32 version;			 /* CLUSTER_FENCE_MARKER_VERSION */
	uint64 fence_epoch;		 /* AUTHORITATIVE ordering key (monotonic) */
	uint64 fence_event_id;	 /* ReconfigEvent siphash -- identity only (P0b) */
	uint64 fence_generation; /* cssd_dead_generation -- same-epoch tie-break */
	int32 issuer_node_id;	 /* coordinator that issued this fence */
	uint8 fenced_dead_bitmap[CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES];
	uint8 marker_kind; /* ClusterFenceMarkerKind (spec-4.12b D1); was _pad[0].
							  * OBSERVABILITY ONLY -- not compared, not ordered (P0-1). */
	uint8 _pad[11];	   /* pad to exactly CLUSTER_FENCE_MARKER_BYTES */
} ClusterFenceMarker;

/*
 * cluster_fence_marker_pack -- write a marker into a voting slot's _reserved1
 *	bytes (the first CLUSTER_FENCE_MARKER_BYTES); the remaining reserved bytes are
 *	left untouched (other future reserved uses coexist).
 * cluster_fence_marker_unpack -- read a marker from _reserved1; returns false when
 *	there is no valid marker (magic mismatch) and leaves *out zeroed.
 */
static inline void
cluster_fence_marker_pack(uint8 *reserved1, const ClusterFenceMarker *m)
{
	memcpy(reserved1, m, sizeof(*m)); /* occupies _reserved1[0..63]; rest untouched */
}

static inline bool
cluster_fence_marker_unpack(const uint8 *reserved1, ClusterFenceMarker *out)
{
	memcpy(out, reserved1, sizeof(*out));
	if (out->magic != CLUSTER_FENCE_MARKER_MAGIC) {
		memset(out, 0, sizeof(*out)); /* no valid marker -> leave *out zeroed */
		return false;
	}
	return true;
}

/*
 * cluster_fence_marker_node_is_fenced -- is node_id set in a marker's
 *	fenced_dead_bitmap?  Used to set self_fenced (U4 multi-dead).  Out-of-range
 *	node ids never read past the bitmap (return false).
 */
static inline bool
cluster_fence_marker_node_is_fenced(const uint8 *fenced_dead_bitmap, int node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES * 8)
		return false;
	return (fenced_dead_bitmap[node_id / 8] & (1u << (node_id % 8))) != 0;
}

/*
 * cluster_fence_marker_tuple_equal -- two markers name the SAME fence iff every
 *	identity field matches (epoch + generation + event_id + issuer + dead bitmap).
 *	magic / version / pad are layout, not identity.  Used for the quorum-majority
 *	count (P0a): a disk only "agrees" when it carries the identical tuple.
 */
static inline bool
cluster_fence_marker_tuple_equal(const ClusterFenceMarker *a, const ClusterFenceMarker *b)
{
	return a->fence_epoch == b->fence_epoch && a->fence_generation == b->fence_generation
		   && a->fence_event_id == b->fence_event_id && a->issuer_node_id == b->issuer_node_id
		   && memcmp(a->fenced_dead_bitmap, b->fenced_dead_bitmap,
					 CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES)
				  == 0;
}

/*
 * ClusterFenceAuthority -- the result of cluster_fence_authority_decide: did the
 *	per-disk markers reach a quorum-majority agreement on a single fence tuple?
 */
typedef struct ClusterFenceAuthority {
	bool has_authority;		   /* a single tuple reached >= majority disks */
	bool minority_seen;		   /* >=1 marker present but none reached majority */
	int agree_disk_count;	   /* disks carrying the authoritative tuple */
	ClusterFenceMarker marker; /* the authoritative tuple (valid iff has_authority) */
} ClusterFenceAuthority;

/*
 * cluster_fence_authority_decide -- the PURE marker-authority selector (spec-4.12
 *	D2, P0a + P0b).  disk_markers[i] is the best marker qvotec read on disk i (only
 *	meaningful when disk_has_marker[i]).  A fence tuple is AUTHORITATIVE only when an
 *	identical tuple appears on >= quorum-majority (n_disks/2+1) DISTINCT disks (P0a:
 *	a CRC-ok minority / partial marker is NOT authority).  Among the tuples that DO
 *	reach majority, the one with the highest fence_epoch wins, fence_generation
 *	tie-breaks (P0b: order by epoch -- monotonic -- never by event_id, which is a
 *	siphash).  No majority tuple -> no authority; minority_seen fires the counter.
 */
static inline ClusterFenceAuthority
cluster_fence_authority_decide(const ClusterFenceMarker *disk_markers, const bool *disk_has_marker,
							   int n_disks)
{
	ClusterFenceAuthority res;
	int majority = n_disks / 2 + 1;
	int best_i = -1;
	int any_marker = 0;
	int i, j;

	memset(&res, 0, sizeof(res));

	for (i = 0; i < n_disks; i++) {
		int count = 0;

		if (!disk_has_marker[i])
			continue;
		any_marker = 1;

		for (j = 0; j < n_disks; j++)
			if (disk_has_marker[j]
				&& cluster_fence_marker_tuple_equal(&disk_markers[i], &disk_markers[j]))
				count++;

		if (count < majority)
			continue; /* P0a: this tuple is a minority -> not authority */

		/* Among majority-reaching tuples pick the highest epoch (gen tie-break). */
		if (best_i < 0 || disk_markers[i].fence_epoch > disk_markers[best_i].fence_epoch
			|| (disk_markers[i].fence_epoch == disk_markers[best_i].fence_epoch
				&& disk_markers[i].fence_generation > disk_markers[best_i].fence_generation)) {
			best_i = i;
			res.agree_disk_count = count;
		}
	}

	if (best_i >= 0) {
		res.has_authority = true;
		res.marker = disk_markers[best_i];
	} else {
		res.has_authority = false;
		res.minority_seen = (any_marker != 0);
	}
	return res;
}

/*
 * cluster_fence_marker_preserve_per_disk -- carry a marker forward from a node's
 *	prior own-slot on ONE disk into the freshly-rebuilt self_slot about to be
 *	written to that SAME disk (R13).  The input is a single disk's prior reserved
 *	bytes, so a marker on disk i can never reach disk j: the per-disk signature is
 *	the anti-amplification guarantee (a 1-of-N minority marker stays 1-of-N across
 *	heartbeats, never promoted to quorum-majority = P0a revival).  No valid marker
 *	on this disk -> leave new_reserved1 untouched (no marker carried).
 */
static inline void
cluster_fence_marker_preserve_per_disk(uint8 *new_reserved1, const uint8 *prior_reserved1_same_disk)
{
	ClusterFenceMarker m;

	if (cluster_fence_marker_unpack(prior_reserved1_same_disk, &m))
		cluster_fence_marker_pack(new_reserved1, &m);
}

/*
 * cluster_fence_dead_superset -- does dead set a cover (is a superset of) dead set
 *	b?  True iff every bit set in b is also set in a.  Used by the D5 refresh guard
 *	(spec-4.12b P0-2): a new authority must never RELEASE an already-fenced node.
 */
static inline bool
cluster_fence_dead_superset(const uint8 *a, const uint8 *b)
{
	int i;

	for (i = 0; i < CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES; i++)
		if ((b[i] & ~a[i]) != 0)
			return false; /* a bit set in b is missing from a -> not a superset */
	return true;
}

/*
 * cluster_write_fence_authority_advances -- the PURE D5 double-monotonic guard
 *	(spec-4.12b, P0-2).  A new authority marker may refresh the local token ONLY when
 *	it does not roll membership backward:
 *	  (1) epoch monotonic:   new_epoch >= latched_epoch, AND
 *	  (2) dead-set superset: new_dead covers latched_dead (a same/higher-epoch marker
 *	      with a SHRUNK dead set is rejected -- it would re-release a fenced node).
 *	In Stage 4 the dead set only grows (Q5=C, no online rejoin); a shrink is an
 *	unsupported uncontrolled rejoin -> rejected fail-closed (the token ages out and
 *	writes fail closed rather than releasing a fenced node = 8.A).  The first
 *	authority (latched_epoch==0, empty latched_dead) always advances.
 */
static inline bool
cluster_write_fence_authority_advances(uint64 new_epoch, const uint8 *new_dead,
									   uint64 latched_epoch, const uint8 *latched_dead)
{
	if (new_epoch < latched_epoch)
		return false; /* epoch must not roll back */
	return cluster_fence_dead_superset(new_dead, latched_dead);
}

/*
 * cluster_fence_marker_build_baseline -- the PURE baseline builder (spec-4.12b D2).
 *	Fill *out as a BASELINE-kind membership marker from the APPLIED membership tuple,
 *	which the caller MUST read atomically from cluster_reconfig_get_last_event() (NOT
 *	raw cluster_epoch_get_current() + a separately-read dead set; P0-3: the reconfig
 *	coordinator bumps epoch before publishing the event, so current_epoch can be
 *	ahead of the applied membership).
 *
 *	issuer_node_id is the STABLE membership issuer (P1), NOT the authoring node:
 *	  - reconfigured membership -> last_applied.coordinator_node_id (same id the
 *	    fence marker used -> the baseline republish is tuple-identical to the fence
 *	    marker, so there is exactly one authoritative tuple per epoch; a leader
 *	    handover never produces a second, competing tuple).
 *	  - pristine membership (last_applied.event_id == 0) -> the fixed sentinel
 *	    CLUSTER_FENCE_BASELINE_INITIAL_ISSUER, identical on every node.
 *	"who authored it" is observability only (baseline_author_is_self), never identity.
 */
static inline void
cluster_fence_marker_build_baseline(ClusterFenceMarker *out, uint64 applied_epoch,
									const uint8 *applied_dead, uint64 applied_generation,
									uint64 applied_event_id, int32 issuer_node_id)
{
	memset(out, 0, sizeof(*out));
	out->magic = CLUSTER_FENCE_MARKER_MAGIC;
	out->version = CLUSTER_FENCE_MARKER_VERSION;
	out->fence_epoch = applied_epoch;
	out->fence_event_id = applied_event_id;
	out->fence_generation = applied_generation;
	out->issuer_node_id = issuer_node_id;
	memcpy(out->fenced_dead_bitmap, applied_dead, CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES);
	out->marker_kind = CLUSTER_FENCE_MARKER_KIND_BASELINE;
}

/*
 * cluster_write_fence_lowest_live_node -- the PURE deterministic baseline-author
 *	leader selector (spec-4.12b D2, U5).  Returns the lowest node_id whose bit is
 *	set in alive_bitmap, or -1 if none is live.  The lowest live node is the single
 *	steady-state baseline author (Q1=A: no election); this is the SAME rule the
 *	reconfig coordinator uses (cluster_reconfig.c dead_bitmap_lowest_bit_set over
 *	the alive set), so the baseline author and the fence coordinator are the same
 *	node and the baseline republish is tuple-identical to its fence marker (P1).
 *	Bit convention matches cluster_fence_marker_node_is_fenced: node N -> byte N/8,
 *	bit N%8.  alive_bitmap must be CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES wide (the
 *	same 16-byte/128-node layout as the quorum decision's alive_bitmap).
 */
static inline int32
cluster_write_fence_lowest_live_node(const uint8 *alive_bitmap)
{
	int i, j;

	for (i = 0; i < CLUSTER_FENCE_MARKER_DEAD_BITMAP_BYTES; i++) {
		if (alive_bitmap[i] == 0)
			continue;
		for (j = 0; j < 8; j++)
			if (alive_bitmap[i] & (uint8)(1u << j))
				return (int32)(i * 8 + j);
	}
	return -1; /* no live node -> no baseline author this cycle */
}

#ifndef FRONTEND

#include "port/atomics.h"
#include "cluster/cluster_reconfig.h" /* CLUSTER_RECONFIG_DEAD_BITMAP_BYTES */

/*
 * cluster.write_fence_enforcement (spec-4.12 §2.4, Q7).  OFF / DEV are the escape
 * hatch (hot gate is a no-op); ON enforces.  Default ON for multi-node + shared-fs
 * is wired by the D7 GUC registration; the storage global defaults OFF until then
 * so the partially-wired mechanism (before D2 refreshes the lease) never fences.
 */
typedef enum ClusterWriteFenceEnforcement {
	CLUSTER_WRITE_FENCE_ENFORCE_OFF = 0,
	CLUSTER_WRITE_FENCE_ENFORCE_ON,
	CLUSTER_WRITE_FENCE_ENFORCE_DEV,
} ClusterWriteFenceEnforcement;

/* GUC storage (registered in cluster_guc.c, D7; storage defined in cluster_write_fence.c). */
extern int cluster_write_fence_enforcement;
extern int cluster_write_fence_lease_ms;

/*
 * ClusterFenceMarkerSubmitResult -- outcome of cluster_write_fence_submit_marker
 *	(D4).  ACK means qvotec wrote + fdatasync'd the marker to >= quorum-majority
 *	voting disks; anything else is fail-closed (the coordinator must NOT publish the
 *	reconfig event / start recovery).
 */
typedef enum ClusterFenceMarkerSubmitResult {
	CLUSTER_FENCE_MARKER_SUBMIT_ACK = 0, /* >= majority disks durable */
	CLUSTER_FENCE_MARKER_SUBMIT_FAILED,	 /* < majority durable, or region/qvotec absent */
	CLUSTER_FENCE_MARKER_SUBMIT_TIMEOUT, /* qvotec did not complete within the bound */
} ClusterFenceMarkerSubmitResult;

/*
 * Local write-fence token (the "pgrac cluster write fence" shmem region, spec-4.12
 * D3).  qvotec refreshes it every poll from the durable voting-disk marker (D2);
 * the hot write paths read it via cluster_write_fence_allowed().
 */
typedef struct ClusterWriteFenceShmem {
	pg_atomic_uint64 authorized_epoch;	 /* fence_epoch from the durable majority marker */
	pg_atomic_uint64 lease_expire_at_us; /* qvotec-refreshed lease deadline */
	pg_atomic_uint32 self_fenced;		 /* this node is in fenced_dead_bitmap */
	pg_atomic_uint32 fence_engaged;		 /* spec-4.12b D3: one-way bring-up latch -- set on
										  * the first authority refresh OR durable self-fence.
										  * Until set, the HOT gate grants a bring-up grace
										  * (the qvotec quorum gate covers no-quorum); after
										  * set, the hot gate is strictly fail-closed.  Scope
										  * is the hot gate ONLY -- verify_durable /
										  * startup_self_check never consult it (P1). */
	uint8 fenced_dead_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	pg_atomic_uint64 fence_event_id;			/* identity of the active fence (not ordered) */
	pg_atomic_uint64 hot_gate_blocked;			/* counter: hot-path fail-closed */
	pg_atomic_uint64 durable_check_blocked;		/* counter: recovery/rejoin direct-check fail */
	pg_atomic_uint64 minority_marker_ignored;	/* counter: partial/minority marker rejected */
	pg_atomic_uint64 baseline_stale_rejected;	/* spec-4.12b D5: refresh rejected (epoch
											   * rollback or dead-set shrink, P0-2) */
	pg_atomic_uint64 baseline_published;		/* spec-4.12b D6: poll cycles in which THIS
											   * node authored a baseline (it is the leader) */
	pg_atomic_uint32 baseline_author_is_self;	/* spec-4.12b D6: this node is currently the
											   * lowest-live baseline-author leader (0/1) */
	pg_atomic_uint64 last_authority_refresh_us; /* spec-4.12b D6: wall-clock of the last
												 * successful token refresh (authority age) */

	/*
	 * D4 LMON->qvotec fence-marker submit mailbox.  Single producer = the
	 * coordinator LMON (reconfig is serialized in the LMON tick + the submit is
	 * synchronous), single consumer = qvotec (the sole voting-disk writer).  The
	 * request_seq / completion_seq pair is the handshake; pending_marker is written
	 * before request_seq is bumped (write barrier between).
	 */
	struct Latch *qvotec_latch;				/* qvotec publishes MyLatch (D4 latch-wake) */
	pg_atomic_uint64 marker_request_seq;	/* LMON bumps to submit a request */
	pg_atomic_uint64 marker_completion_seq; /* qvotec sets = request_seq when done */
	pg_atomic_uint32 marker_result;			/* ClusterFenceMarkerSubmitResult */
	pg_atomic_uint64 marker_write_failed;	/* counter: a submit did not reach majority */
	ClusterFenceMarker pending_marker;		/* LMON stages here before bumping request_seq */
} ClusterWriteFenceShmem;

/* shmem region plumbing (ClusterShmemRegion 5-step). */
extern void cluster_write_fence_shmem_register(void);

/*
 * cluster_write_fence_refresh_from_marker -- qvotec poll refresh (spec-4.12 D2,
 *	ONLY qvotec calls it).  Publishes the authoritative fence tuple into the local
 *	token: authorized_epoch + self_fenced (this node in fenced_dead_bitmap) +
 *	event_id, then the lease LAST behind a write barrier.  The lease is invalidated
 *	first so a concurrent hot-path reader that sees a stale epoch also sees an
 *	expired lease (fail-closed); see the read-side order in cluster_write_fence.c.
 * cluster_write_fence_note_minority_marker -- a CRC-ok marker was present but did
 *	not reach quorum-majority (P0a) -> ignored; bump the observability counter.  The
 *	token is left untouched so its lease ages out (fail-closed) if no authority
 *	returns.
 */
extern void cluster_write_fence_refresh_from_marker(const ClusterFenceMarker *m,
													uint64 lease_expire_us);
extern void cluster_write_fence_note_minority_marker(void);
/* spec-4.12b D5: a baseline that would roll membership backward was rejected
 * (refresh-side double-monotonic guard, or qvotec author-side P1-1 epoch regress). */
extern void cluster_write_fence_note_baseline_stale(void);

/*
 * D4 cross-process fence-marker submit (core 8.A order).
 *
 * cluster_write_fence_submit_marker -- LMON (coordinator) side: stage the marker,
 *	wake qvotec, and synchronously wait (bounded) for qvotec to write + fdatasync it
 *	to >= quorum-majority disks.  Returns ACK only when the marker is durable on a
 *	majority; the coordinator publishes the reconfig event ONLY on ACK.
 * cluster_write_fence_publish_qvotec_latch -- qvotec startup: publish MyLatch so
 *	LMON can wake it; auto-cleared at proc_exit.
 * cluster_write_fence_qvotec_poll_pending -- qvotec poll: returns true + the marker
 *	when a new submit is pending (and marks it in-flight).
 * cluster_write_fence_qvotec_complete -- qvotec poll: publish the in-flight result
 *	(acked = reached majority) back to the waiting LMON.
 */
extern ClusterFenceMarkerSubmitResult
cluster_write_fence_submit_marker(const ClusterFenceMarker *m);
extern void cluster_write_fence_publish_qvotec_latch(struct Latch *latch);
extern bool cluster_write_fence_qvotec_poll_pending(ClusterFenceMarker *out);
extern void cluster_write_fence_qvotec_complete(bool acked);

/*
 * cluster_write_fence_enforcing -- spec-4.12b D4: is the fence ACTIVELY enforced
 * (GUC == on AND voting disks configured)?  default-ON auto-degrades to a no-op on
 * a single node / no-voting-disk deployment; the enforcement consumers gate on this
 * (not the raw GUC) so single-node recovery is never blocked.
 */
extern bool cluster_write_fence_enforcing(void);

/*
 * cluster_write_fence_allowed -- the hot-path wrapper: read the live epoch + the
 * shmem token and apply cluster_write_fence_decide.  PURE-read, no-throw,
 * critical-section-safe (no LWLock, no durable I/O); callers decide the
 * fail-closed action (ERROR / PANIC / BLOCKED) by context (spec-4.12 §3, Q1).
 */
extern bool cluster_write_fence_allowed(void);

/*
 * cluster_write_fence_reject_if_fenced -- spec-4.12 D5 hot-path gate.  Allowed ->
 *	return; fenced -> bump hot_gate_blocked and fail closed (CritSectionCount>0 ->
 *	PANIC, else ERROR 53R51).  Every cooperative shared-storage write entry calls
 *	this ONE helper so the CritSectionCount-aware gate is identical everywhere (L240).
 *	op names the operation for the message ("write" / "extend" / ...).
 */
extern void cluster_write_fence_reject_if_fenced(const char *op);

/*
 * cluster_write_fence_verify_durable -- spec-4.12 D6 (Option B, Oracle-aligned).
 *	Direct durable read of the voting-disk fence markers (bypassing the cache token):
 *	true iff a quorum-majority of the CONFIGURED disks carry an identical marker with
 *	fence_epoch == required_epoch.  Gates authority publication on the recovery /
 *	rejoin / startup paths (a superseded episode is rejected, 8.A).  Does NOT bound
 *	replay -- full redo is applied (Oracle-aligned); the non-cooperating zombie's
 *	over-apply is AD-013 hardware-fence scope.  Enforcement off -> true.
 */
extern bool cluster_write_fence_verify_durable(uint64 required_epoch);

/*
 * cluster_write_fence_startup_self_check -- spec-4.12 D6 rejoin/startup gate (Q5=C).
 *	True iff a durable quorum-majority marker lists THIS node as fenced; arms the
 *	local self_fenced token so D5 rejects all shared writes immediately.  The caller
 *	enters a non-serving, NON-FATAL terminal (recover only via controlled rejoin /
 *	cold-admin; no online rejoin in Stage 4).  Enforcement off -> false.
 */
extern bool cluster_write_fence_startup_self_check(void);
/* spec-5.16 (3-node rejoin) — a self-fenced node un-fences when its own durable
 * quorum-majority COMMITTED join marker (admitted_epoch) is newer than the fence
 * it is under (RC-5 supersede for the write-fence; coordinator-authored, safe). */
extern void cluster_write_fence_supersede_by_admit(uint64 admitted_epoch);

/* D7 observability counter accessors (cluster_debug 'write_fence' category). */
extern uint64 cluster_write_fence_get_hot_gate_blocked(void);
extern uint64 cluster_write_fence_get_durable_check_blocked(void);
extern uint64 cluster_write_fence_get_minority_marker_ignored(void);
extern uint64 cluster_write_fence_get_marker_write_failed(void);
extern uint64 cluster_write_fence_get_baseline_stale_rejected(void);
extern bool cluster_write_fence_get_fence_engaged(void); /* spec-4.12b D3 latch state */
/* spec-4.12b D6 baseline observability. */
extern uint64 cluster_write_fence_get_baseline_published(void);
extern bool cluster_write_fence_get_baseline_author_is_self(void);
extern uint64 cluster_write_fence_get_baseline_authority_age_us(void);
/* spec-4.12b D6: qvotec records, on each poll, whether this node is the baseline
 * leader (is_leader -> baseline_author_is_self) and whether it actually authored a
 * baseline this cycle (published -> bumps baseline_published). */
extern void cluster_write_fence_note_baseline_published(bool is_leader, bool published);

#endif /* !FRONTEND */

#endif /* CLUSTER_WRITE_FENCE_H */
