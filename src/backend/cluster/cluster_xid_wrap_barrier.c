/*-------------------------------------------------------------------------
 *
 * cluster_xid_wrap_barrier.c
 *	  pgrac xid epoch-rollover barrier (GCS-race round-3 P0-1).
 *
 *	  Protocol (header holds the why):
 *
 *	   coordinator (any node whose margin trips; typically all of them,
 *	   herding keeps the counters within slack x 64):
 *	    (1) durable NATIVE_RAW_REUSED stamp (idempotent both-copies)
 *	    (2) own coverage latch disabled (one-way)
 *	    (3) XID_NATIVE_DISABLE fanout to alive members lacking an ack bit
 *	    (4) all bits in -> wrap_barrier_done (opens the allocation gate)
 *
 *	   member: on DISABLE, disable own latch (one-way) + ACK.  Trusting a
 *	   spurious DISABLE is safe: disabling only degrades below-hw recycled
 *	   refs to fail-closed 53R97, never yields a wrong verdict.
 *
 *	  Soundness of the ack round: an ACK proves the member's latch was off
 *	  at ack time; the latch cannot re-arm within that boot (one-way), and
 *	  any later boot sees the durable stamp (written BEFORE the first
 *	  DISABLE went out) and never latches again -- so a filled bitmap is a
 *	  permanent global "every latch off" fact, and the boot shortcut may
 *	  trust an already-wrapped counter (the first carry was gated by this
 *	  same barrier).  Membership trust mirrors the KO flush barrier: the
 *	  alive-peer mask is authoritative because a non-ALIVE peer is fenced /
 *	  evicted and can only return through a reboot (boot gate) or rejoin
 *	  reconfig.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_xid_wrap_barrier.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_sinval.h"
#include "cluster/cluster_xid_authority.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_xid_wrap_barrier.h"
#include "port/pg_crc32c.h"

/* per-boot LOG-once latches (LMON is the only writer of all of these) */
static bool wb_logged_started = false;
static bool wb_logged_done = false;
static uint64 wb_logged_nocap_mask = 0;

static pg_crc32c
wb_payload_crc(const ClusterXidNativeDisablePayload *p)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, p, offsetof(ClusterXidNativeDisablePayload, crc));
	FIN_CRC32C(crc);
	return crc;
}

static bool
wb_payload_valid(const ClusterXidNativeDisablePayload *p)
{
	if (p->magic != CLUSTER_XID_WRAP_BARRIER_IC_MAGIC)
		return false;
	if (p->version != CLUSTER_XID_WRAP_BARRIER_IC_VERSION)
		return false;
	if (p->reserved != 0)
		return false;
	return p->crc == wb_payload_crc(p);
}

static void
wb_build_payload(ClusterXidNativeDisablePayload *p)
{
	memset(p, 0, sizeof(*p));
	p->magic = CLUSTER_XID_WRAP_BARRIER_IC_MAGIC;
	p->version = CLUSTER_XID_WRAP_BARRIER_IC_VERSION;
	p->node_id = cluster_node_id;
	p->crc = wb_payload_crc(p);
}

/*
 * Margin predicate: the cluster's forward-most xid view (own nextFullXid or
 * the herding-mirrored peer hwm max) is within MARGIN of the 2^32 boundary,
 * or already past it.  The test-only force GUC drives the whole real path
 * in TAP (a genuine 2^32 approach is not drivable in a test).
 */
static bool
wb_margin_reached(void)
{
	uint64 next;
	uint64 hi;

	if (cluster_xid_wrap_barrier_force)
		return true;

	next = U64FromFullTransactionId(ReadNextFullTransactionId());
	hi = cluster_xid_stripe_cluster_max_hwm();
	if (next > hi)
		hi = next;
	return hi + CLUSTER_XID_WRAP_BARRIER_MARGIN >= (UINT64CONST(1) << 32);
}

/* member side: coordinator says raw reuse is imminent -- latch off + ACK. */
static void
wb_disable_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterXidNativeDisablePayload *p = (const ClusterXidNativeDisablePayload *)payload;
	ClusterXidNativeDisablePayload ack;

	if (!wb_payload_valid(p))
		return;
	/* F6 discipline: the body's claimed sender must be the wire sender. */
	if (p->node_id != (int32)env->source_node_id)
		return;

	cluster_cr_native_prehistory_disable();

	wb_build_payload(&ack);
	(void)cluster_ic_send_envelope(PGRAC_IC_MSG_XID_NATIVE_DISABLE_ACK, (int32)env->source_node_id,
								   &ack, (uint32)sizeof(ack));
}

/* coordinator side: a member's latch is provably off -- set its bit. */
static void
wb_ack_handler(const ClusterICEnvelope *env, const void *payload)
{
	const ClusterXidNativeDisablePayload *p = (const ClusterXidNativeDisablePayload *)payload;

	if (!wb_payload_valid(p))
		return;
	if (p->node_id != (int32)env->source_node_id)
		return;

	cluster_xid_wrap_barrier_note_ack(p->node_id);
}

void
cluster_xid_wrap_barrier_register_ic_msg_types(void)
{
	const ClusterICMsgTypeInfo disable_info = {
		.msg_type = PGRAC_IC_MSG_XID_NATIVE_DISABLE,
		.name = "xid_native_disable",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false,
		.handler = wb_disable_handler,
	};
	const ClusterICMsgTypeInfo ack_info = {
		.msg_type = PGRAC_IC_MSG_XID_NATIVE_DISABLE_ACK,
		.name = "xid_native_disable_ack",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.broadcast_ok = false,
		.handler = wb_ack_handler,
	};

	cluster_ic_register_msg_type(&disable_info);
	cluster_ic_register_msg_type(&ack_info);
}

/*
 * StartupXLOG-tail init (before the coverage verify, before backends).
 *
 * Mirrors a durably stamped flag into shmem so the LMON tick skips the
 * stamp step, disables the local latch outright (a boot on a stamped
 * authority must never route natively -- the verify's own boot gate is the
 * belt, this is the braces), and takes the boot shortcut on an
 * already-wrapped counter: the first carry was gated by the barrier, so
 * "every latch off forever" already holds globally and this boot may
 * allocate epoch>=1 without a fresh ack round.
 *
 * An unreadable authority leaves everything unset: the allocation gate
 * stays closed for epoch>=1 candidates (fail-closed, retryable) and the
 * LMON tick keeps retrying the read at 1Hz.
 */
void
cluster_xid_wrap_barrier_startup_init(void)
{
	ClusterXidAuthorityHeader auth;

	if (!cluster_enabled || !cluster_shared_catalog)
		return;
	if (!cluster_xid_authority_read(&auth))
		return;
	if ((auth.flags & CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED) == 0)
		return;

	cluster_cr_native_prehistory_disable();
	cluster_xid_wrap_barrier_set_marked();

	if (U64FromFullTransactionId(ReadNextFullTransactionId()) > (uint64)PG_UINT32_MAX) {
		cluster_xid_wrap_barrier_set_done();
		ereport(LOG, (errmsg("cluster xid wrap barrier: counter already past the native era; "
							 "epoch allocation gate open (boot shortcut)")));
	}
}

/*
 * LMON tick (1Hz).  Zero cost before the margin and after the gate opens.
 */
void
cluster_xid_wrap_barrier_lmon_tick(void)
{
	ClusterXidAuthorityHeader auth;
	uint32 alive_mask;
	uint64 pending;
	int32 n;

	if (!cluster_enabled || !cluster_shared_catalog)
		return;
	if (cluster_xid_wrap_barrier_passed())
		return;

	if (!cluster_xid_wrap_barrier_marked()) {
		if (!wb_margin_reached())
			return;

		/*
		 * Precheck read keeps the tick fail-soft on a transiently
		 * unreadable authority (retry next tick); the stamp itself PANICs
		 * only on damage that appears between the two reads.
		 */
		if (!cluster_xid_authority_read(&auth)) {
			if (!wb_logged_started)
				ereport(LOG, (errmsg("cluster xid wrap barrier: shared XID authority unreadable; "
									 "retrying before the native-raw-reused stamp")));
			wb_logged_started = true;
			return;
		}
		cluster_xid_authority_mark_native_raw_reused();
		cluster_xid_wrap_barrier_set_marked();
		ereport(LOG, (errmsg("cluster xid wrap barrier: native-raw-reused stamped durably; "
							 "disabling native prehistory routing cluster-wide")));
	}

	/* self is a member too (idempotent) */
	cluster_cr_native_prehistory_disable();

	alive_mask = cluster_sinval_compute_alive_peer_mask();
	pending = (uint64)alive_mask & ~cluster_xid_wrap_barrier_ack_bitmap();

	if (pending == 0) {
		cluster_xid_wrap_barrier_set_done();
		if (!wb_logged_done)
			ereport(LOG, (errmsg("cluster xid wrap barrier complete: every member disabled native "
								 "prehistory routing; epoch allocation gate open")));
		wb_logged_done = true;
		return;
	}

	/* the alive mask is 32 bits wide (cluster_sinval_compute_alive_peer_mask);
	 * pending inherits that bound */
	for (n = 0; n < 32; n++) {
		ClusterXidNativeDisablePayload p;

		if ((pending & (UINT64CONST(1) << n)) == 0)
			continue;
		if (!cluster_sf_peer_supports_xid_native_disable(n)) {
			/*
			 * A member that cannot speak the barrier keeps the gate closed
			 * (fail-closed beats a wrong LOCAL verdict).  LOG once per
			 * peer; the operator resolves by upgrading or removing it.
			 */
			if ((wb_logged_nocap_mask & (UINT64CONST(1) << n)) == 0)
				ereport(LOG, (errmsg("cluster xid wrap barrier: node %d does not advertise the "
									 "barrier capability; epoch allocation stays gated until it "
									 "is upgraded or removed",
									 n)));
			wb_logged_nocap_mask |= UINT64CONST(1) << n;
			continue;
		}
		wb_build_payload(&p);
		(void)cluster_ic_send_envelope(PGRAC_IC_MSG_XID_NATIVE_DISABLE, n, &p, (uint32)sizeof(p));
	}
}
