/*-------------------------------------------------------------------------
 *
 * cluster_ic.c
 *	  pgrac cluster internal IPC abstraction layer (Stage 0.18 stub
 *	  implementation).
 *
 *	  Stage 0.18 ships exactly one interconnect tier vtable -- the stub
 *	  -- whose send_bytes returns true for target == self and ereports
 *	  ERRCODE_FEATURE_NOT_SUPPORTED for cross-node, and whose
 *	  recv_bytes always returns false (no messages).  Tier1 (TCP)
 *	  lands in Stage 2 and replaces the vtable at startup based on the
 *	  cluster.interconnect_tier GUC; Tier2/3 (RDMA) land in Stage 6+.
 *
 *	  The high-level protocol layer (cluster_msg_*, cluster_rpc_*) is
 *	  fully wired here and forwards through the vtable: at stage 0.18
 *	  the only legal target is self, which exercises header
 *	  construction and CRC computation but produces no actual wire
 *	  traffic.  Stage 2 reuses these protocol-layer functions
 *	  unchanged once the vtable is swapped to TCP.
 *
 *	  See:
 *	    docs/cluster-ic-design.md         - design rationale, wire
 *	                                        format, Stage evolution
 *	    specs/spec-0.18-ic-framework.md   - this stage's spec
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ic.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All exported symbols use the cluster_ic_ / cluster_msg_ /
 *	  cluster_rpc_ / ClusterICOps prefix.  Excluded from disable-cluster
 *	  builds via cluster/Makefile (spec-0.3 symbol contract).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "utils/elog.h"

#include "cluster/cluster_guc.h" /* cluster_node_id, cluster_interconnect_tier */
#include "cluster/cluster_ic.h"


/*
 * Compile-time anchor for the wire format.  Mirrors the constant in
 * cluster_ic.h; if anyone changes ClusterMsgHeader without updating
 * the constant the build fails here.
 */
StaticAssertDecl(sizeof(ClusterMsgHeader) == PGRAC_IC_HEADER_BYTES,
				 "ClusterMsgHeader must be exactly PGRAC_IC_HEADER_BYTES");


/*
 * Active vtable.  Initialised to point at ClusterICOps_Stub by
 * cluster_ic_init (see below) so that the dereference path is always
 * valid; postmaster startup ordering guarantees cluster_ic_init runs
 * before any backend forks.
 */
const ClusterICOps *ClusterICOps_Active = NULL;


/* ============================================================
 * Stub vtable (Stage 0.18 default).
 * ============================================================ */

static bool
stub_send_bytes(int32 target_node_id, const void *buf pg_attribute_unused(),
				size_t len pg_attribute_unused())
{
	if (target_node_id == cluster_node_id) {
		/*
		 * Self-targeted send is a no-op success.  Stage 0 unit / single
		 * -node deployments need this path so callers can exercise the
		 * upper API without a listener; Stage 2+ TCP vtable will short-
		 * circuit self-target via local memory copy.
		 */
		return true;
	}

	if (cluster_node_id == -1) {
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster.node_id is unconfigured (-1)"),
						errhint("Set cluster.node_id in postgresql.conf and restart.")));
	}

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cross-node interconnect is not available in stub tier"),
					errhint("Set cluster.interconnect_tier to tier1 (Stage 2+) for "
							"real cross-node IPC.")));

	return false; /* unreachable */
}

static bool
stub_recv_bytes(int32 *out_sender_node_id pg_attribute_unused(), void *buf pg_attribute_unused(),
				size_t bufsize pg_attribute_unused(),
				size_t *out_received_len pg_attribute_unused())
{
	/* No listener exists in stub mode; nothing is ever received. */
	return false;
}

static void
stub_tier_init(void)
{
	/* Stub: no resources to allocate. */
}

static void
stub_tier_shutdown(void)
{
	/* Stub: no resources to release. */
}

const ClusterICOps ClusterICOps_Stub = {
	.send_bytes = stub_send_bytes,
	.recv_bytes = stub_recv_bytes,
	.tier_init = stub_tier_init,
	.tier_shutdown = stub_tier_shutdown,
	.tier_name = "stub",
};


/* ============================================================
 * Tier selection (cluster_ic_init / shutdown).
 * ============================================================ */

void
cluster_ic_init(void)
{
	switch ((ClusterICTier)cluster_interconnect_tier) {
	case CLUSTER_IC_TIER_STUB:
		ClusterICOps_Active = &ClusterICOps_Stub;
		break;

	case CLUSTER_IC_TIER_1:
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster.interconnect_tier=tier1 is not implemented"),
						errhint("tier1 (TCP) lands in Stage 2; stay on stub for now.")));
		break;

	case CLUSTER_IC_TIER_2:
	case CLUSTER_IC_TIER_3:
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster.interconnect_tier=tier2/tier3 is not implemented"),
						errhint("tier2/tier3 (RDMA) land in Stage 6+; stay on stub for now.")));
		break;

	default:
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid cluster.interconnect_tier value: %d", cluster_interconnect_tier)));
		break;
	}

	Assert(ClusterICOps_Active != NULL);
	ClusterICOps_Active->tier_init();
}

void
cluster_ic_shutdown(void)
{
	if (ClusterICOps_Active != NULL) {
		ClusterICOps_Active->tier_shutdown();
		ClusterICOps_Active = NULL;
	}
}


/* ============================================================
 * Low-level byte stream API.  Pure forwarders to the active vtable;
 * upper layers should always dereference through ClusterICOps_Active
 * (not the stub instance directly) so Stage 2 swap takes effect.
 * ============================================================ */

bool
cluster_ic_send_bytes(int32 target_node_id, const void *buf, size_t len)
{
	Assert(ClusterICOps_Active != NULL);
	return ClusterICOps_Active->send_bytes(target_node_id, buf, len);
}

bool
cluster_ic_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize,
					  size_t *out_received_len)
{
	Assert(ClusterICOps_Active != NULL);
	return ClusterICOps_Active->recv_bytes(out_sender_node_id, buf, bufsize, out_received_len);
}


/* ============================================================
 * High-level protocol API.  Adds the ClusterMsgHeader plus CRC over
 * (header excl crc) + payload.  Stub mode still computes the CRC so
 * the protocol layer is exercised end-to-end during single-node
 * smoke testing -- this catches header-shape regressions early even
 * before Stage 2 ships real wire traffic.
 *
 * seq_no is a single global atomic counter for now.  Stage 2+ may
 * partition per target node for back-pressure / dedupe.
 * ============================================================ */

static uint32 ic_global_seq_no = 0;

static pg_crc32c
compute_msg_crc(const ClusterMsgHeader *hdr, const void *payload, uint32 payload_len)
{
	pg_crc32c crc;
	const uint8 *hdr_bytes = (const uint8 *)hdr;
	const size_t crc_offset = offsetof(ClusterMsgHeader, crc32);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, hdr_bytes, crc_offset);
	if (payload_len > 0)
		COMP_CRC32C(crc, payload, payload_len);
	FIN_CRC32C(crc);
	return crc;
}

bool
cluster_msg_send(int32 target_node_id, uint16 msg_type, const void *payload, uint32 payload_len)
{
	ClusterMsgHeader hdr;

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = PGRAC_IC_MAGIC;
	hdr.protocol_version = PGRAC_IC_PROTOCOL_VERSION_V1;
	hdr.msg_type = msg_type;
	hdr.sender_node_id = (int16)cluster_node_id;
	hdr.seq_no = ++ic_global_seq_no;
	hdr.payload_len = payload_len;
	hdr.crc32 = compute_msg_crc(&hdr, payload, payload_len);

	if (!cluster_ic_send_bytes(target_node_id, &hdr, sizeof(hdr)))
		return false;

	if (payload_len > 0)
		return cluster_ic_send_bytes(target_node_id, payload, payload_len);

	return true;
}

bool
cluster_msg_recv(ClusterMsgHeader *out_hdr, void *payload_buf, uint32 payload_buf_size)
{
	int32 sender_node_id;
	size_t hdr_received;
	size_t payload_received;
	pg_crc32c expected_crc;

	if (!cluster_ic_recv_bytes(&sender_node_id, out_hdr, sizeof(*out_hdr), &hdr_received))
		return false;

	if (hdr_received != sizeof(*out_hdr)) {
		ereport(WARNING, (errmsg("cluster_msg_recv: short header read (%zu of %zu bytes)",
								 hdr_received, sizeof(*out_hdr))));
		return false;
	}

	if (out_hdr->magic != PGRAC_IC_MAGIC) {
		ereport(WARNING, (errmsg("cluster_msg_recv: bad magic 0x%08x (expected 0x%08x)",
								 out_hdr->magic, PGRAC_IC_MAGIC)));
		return false;
	}

	if (out_hdr->protocol_version != PGRAC_IC_PROTOCOL_VERSION_V1) {
		ereport(WARNING, (errmsg("cluster_msg_recv: unsupported protocol version %u",
								 out_hdr->protocol_version)));
		return false;
	}

	if (out_hdr->payload_len > payload_buf_size) {
		ereport(WARNING, (errmsg("cluster_msg_recv: payload %u exceeds buffer %u",
								 out_hdr->payload_len, payload_buf_size)));
		return false;
	}

	if (out_hdr->payload_len > 0) {
		if (!cluster_ic_recv_bytes(&sender_node_id, payload_buf, payload_buf_size,
								   &payload_received))
			return false;

		if (payload_received != out_hdr->payload_len) {
			ereport(WARNING, (errmsg("cluster_msg_recv: short payload read (%zu of %u bytes)",
									 payload_received, out_hdr->payload_len)));
			return false;
		}
	}

	expected_crc = compute_msg_crc(out_hdr, payload_buf, out_hdr->payload_len);
	if (!EQ_CRC32C(expected_crc, out_hdr->crc32)) {
		ereport(WARNING, (errmsg("cluster_msg_recv: CRC mismatch (got 0x%08x expected 0x%08x)",
								 out_hdr->crc32, expected_crc)));
		return false;
	}

	return true;
}

bool
cluster_rpc_call(int32 target_node_id, uint16 msg_type, const void *req, uint32 req_len,
				 void *resp_buf, uint32 resp_buf_size, uint32 *out_resp_len, int timeout_ms)
{
	ClusterMsgHeader hdr;
	uint32 sent_seq;
	int elapsed_ms = 0;
	const int poll_step_ms = 1;

	/*
	 * Capture the seq_no we are about to send so the receive loop can
	 * match the reply.  Stage 0.18 stub never produces a reply, so
	 * this loop simply runs until the timeout.
	 */
	sent_seq = ic_global_seq_no + 1;
	if (!cluster_msg_send(target_node_id, msg_type, req, req_len))
		return false;

	while (elapsed_ms < timeout_ms) {
		if (cluster_msg_recv(&hdr, resp_buf, resp_buf_size)) {
			if (hdr.seq_no == sent_seq) {
				if (out_resp_len != NULL)
					*out_resp_len = hdr.payload_len;
				return true;
			}
			/* Otherwise drop and keep waiting; Stage 2+ may queue. */
		}

		pg_usleep(poll_step_ms * 1000L);
		elapsed_ms += poll_step_ms;
	}

	return false; /* timeout */
}
