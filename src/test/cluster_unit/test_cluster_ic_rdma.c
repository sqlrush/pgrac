/*-------------------------------------------------------------------------
 *
 * test_cluster_ic_rdma.c
 *	  Header-level invariants for the spec-6.1 RDMA transport surface.
 *
 *	  These tests deliberately do not link cluster_ic_rdma.o or libibverbs.
 *	  They pin the PG-free public ABI that other cluster subsystems compile
 *	  against: enum values stored by GUCs/views, SGE release callback shape,
 *	  provider vtable layout, and the dedicated RDMA stats LWLock tranche.
 *	  Runtime verbs behavior is covered by cluster_tap
 *	  t/334_ic_rdma_soft_roce.pl on Linux soft-RoCE when enabled.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ic_rdma.c
 *
 * NOTES
 *	  This is a pgrac-original file.  It is header-only by design so
 *	  cluster_unit never depends on RDMA libraries or backend-only
 *	  symbols.  Do not add calls to cluster_ic_rdma.c functions here
 *	  unless the Makefile rule is updated with explicit stubs.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ic_rdma.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


UT_DEFINE_GLOBALS();


static bool
rdma_unit_device_open(ClusterICRdmaCtx *ctx)
{
	return ctx == NULL;
}

static bool
rdma_unit_reg_region(ClusterICRdmaCtx *ctx, void *base, size_t len, ClusterICMr *out)
{
	UT_ASSERT_NULL(ctx);
	UT_ASSERT_NOT_NULL(base);
	UT_ASSERT_EQ((int)len, 8192);
	UT_ASSERT_NOT_NULL(out);
	out->base = base;
	out->len = len;
	out->lkey = 0x11112222U;
	out->rkey = 0;
	return true;
}

static bool
rdma_unit_qp_create(ClusterICRdmaCtx *ctx, int32 peer, ClusterICQp *out)
{
	UT_ASSERT_NULL(ctx);
	UT_ASSERT_EQ(peer, 7);
	UT_ASSERT_NULL(out);
	return true;
}

static ClusterICSendResult
rdma_unit_post_send(ClusterICQp *qp, const ClusterICSge *sge, int n_sge, bool signaled,
					bool inline_send, uint32 imm)
{
	UT_ASSERT_NULL(qp);
	UT_ASSERT_NOT_NULL(sge);
	UT_ASSERT_EQ(n_sge, 1);
	UT_ASSERT_EQ((int)signaled, 1);
	UT_ASSERT_EQ((int)inline_send, 1);
	UT_ASSERT_EQ((int)imm, 0);
	return CLUSTER_IC_SEND_DONE;
}

static bool
rdma_unit_post_recv(ClusterICQp *qp, ClusterICSge *sge)
{
	UT_ASSERT_NULL(qp);
	UT_ASSERT_NOT_NULL(sge);
	return true;
}

static int
rdma_unit_poll_cq(ClusterICRdmaCtx *ctx, ClusterICWc *out, int max)
{
	UT_ASSERT_NULL(ctx);
	UT_ASSERT_NULL(out);
	UT_ASSERT_EQ(max, 0);
	return 0;
}

static void
rdma_unit_device_close(ClusterICRdmaCtx *ctx)
{
	UT_ASSERT_NULL(ctx);
}

static void
rdma_unit_release(void *arg)
{
	int *counter = (int *)arg;

	(*counter)++;
}


UT_TEST(test_rdma_guc_enum_values_are_stable)
{
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_FALLBACK_AUTO, 0);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_FALLBACK_OFF, 1);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_PROVIDER_AUTO, 0);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_PROVIDER_VERBS, 1);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_PROVIDER_MLX5, 2);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_COMPLETION_EVENT, 0);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_COMPLETION_BUSYPOLL, 1);
}

UT_TEST(test_rdma_view_transport_and_state_values_are_stable)
{
	UT_ASSERT_EQ((int)CLUSTER_IC_PEER_TRANSPORT_TCP, 0);
	UT_ASSERT_EQ((int)CLUSTER_IC_PEER_TRANSPORT_RDMA, 1);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_PEER_DISABLED, 0);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_PEER_FALLBACK_TCP, 1);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_PEER_CONNECTING, 2);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_PEER_CONNECTED, 3);
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_PEER_ERROR, 4);
}

UT_TEST(test_rdma_sge_release_contract)
{
	char buf[BLCKSZ];
	int releases = 0;
	ClusterICSge sge;

	memset(&sge, 0, sizeof(sge));
	sge.addr = buf;
	sge.len = sizeof(buf);
	sge.lkey = 0xabcdef01U;
	sge.release_cb = rdma_unit_release;
	sge.release_arg = &releases;

	UT_ASSERT_EQ((int)sge.len, BLCKSZ);
	UT_ASSERT_EQ((int)sge.lkey, (int)0xabcdef01U);
	UT_ASSERT_NOT_NULL(sge.release_cb);
	sge.release_cb(sge.release_arg);
	UT_ASSERT_EQ(releases, 1);
}

UT_TEST(test_rdma_provider_vtable_shape)
{
	char page[BLCKSZ];
	ClusterICMr mr;
	ClusterICSge sge;
	ClusterICRdmaProvider provider;

	memset(&mr, 0, sizeof(mr));
	memset(&sge, 0, sizeof(sge));
	memset(&provider, 0, sizeof(provider));

	provider.name = "unit-rdma-provider";
	provider.device_open = rdma_unit_device_open;
	provider.reg_region = rdma_unit_reg_region;
	provider.qp_create = rdma_unit_qp_create;
	provider.post_send = rdma_unit_post_send;
	provider.post_recv = rdma_unit_post_recv;
	provider.poll_cq = rdma_unit_poll_cq;
	provider.device_close = rdma_unit_device_close;

	UT_ASSERT_STR_EQ(provider.name, "unit-rdma-provider");
	UT_ASSERT(provider.device_open(NULL));
	UT_ASSERT(provider.reg_region(NULL, page, sizeof(page), &mr));
	UT_ASSERT_EQ((int)mr.len, BLCKSZ);
	UT_ASSERT_EQ((int)mr.lkey, (int)0x11112222U);
	UT_ASSERT_EQ((int)mr.rkey, 0);
	UT_ASSERT(provider.qp_create(NULL, 7, NULL));

	sge.addr = page;
	sge.len = sizeof(page);
	sge.lkey = mr.lkey;
	UT_ASSERT_EQ((int)provider.post_send(NULL, &sge, 1, true, true, 0), (int)CLUSTER_IC_SEND_DONE);
	UT_ASSERT(provider.post_recv(NULL, &sge));
	UT_ASSERT_EQ(provider.poll_cq(NULL, NULL, 0), 0);
	provider.device_close(NULL);
}

UT_TEST(test_rdma_lwlock_tranche_is_after_stage5_cluster_tranches)
{
	UT_ASSERT_EQ((int)LWTRANCHE_CLUSTER_IC_RDMA, (int)LWTRANCHE_CLUSTER_HW + 1);
	UT_ASSERT((int)LWTRANCHE_CLUSTER_IC_RDMA < (int)LWTRANCHE_FIRST_USER_DEFINED);
}

UT_TEST(test_rdma_wait_events_are_contiguous)
{
	UT_ASSERT_EQ((int)WAIT_EVENT_INTERCONNECT_RDMA_RECV,
				 (int)WAIT_EVENT_INTERCONNECT_RDMA_SEND + 1);
	UT_ASSERT_EQ((int)WAIT_EVENT_CLUSTER_IC_RDMA_POLL, (int)WAIT_EVENT_INTERCONNECT_RDMA_RECV + 1);
	UT_ASSERT_EQ((int)WAIT_EVENT_INTERCONNECT_RDMA_BUSYPOLL,
				 (int)WAIT_EVENT_CLUSTER_IC_RDMA_POLL + 1);
	UT_ASSERT_EQ((int)WAIT_EVENT_INTERCONNECT_RDMA_INLINE_SEND,
				 (int)WAIT_EVENT_INTERCONNECT_RDMA_BUSYPOLL + 1);
	UT_ASSERT_EQ((int)WAIT_EVENT_CLUSTER_IC_RDMA_CONNECT,
				 (int)WAIT_EVENT_INTERCONNECT_RDMA_INLINE_SEND + 1);
	UT_ASSERT_EQ((int)WAIT_EVENT_INTERCONNECT_TCP_FALLBACK,
				 (int)WAIT_EVENT_CLUSTER_IC_RDMA_CONNECT + 1);
}

UT_TEST(test_rdma_spec613_header_policies)
{
	UT_ASSERT_EQ(cluster_ic_rdma_signal_batch_k(16), 8);
	UT_ASSERT_EQ(cluster_ic_rdma_signal_batch_k(4096), CLUSTER_IC_RDMA_SIGNAL_BATCH_CAP);
	UT_ASSERT(cluster_ic_rdma_payload_inline_eligible(256, 256));
	UT_ASSERT(!cluster_ic_rdma_payload_inline_eligible(257, 256));
	UT_ASSERT_EQ(CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES, 84);
	UT_ASSERT_EQ(CLUSTER_IC_RDMA_DIRECT_LAND_REPLY_BYTES, 84 + BLCKSZ);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_rdma_guc_enum_values_are_stable);
	UT_RUN(test_rdma_view_transport_and_state_values_are_stable);
	UT_RUN(test_rdma_sge_release_contract);
	UT_RUN(test_rdma_provider_vtable_shape);
	UT_RUN(test_rdma_lwlock_tranche_is_after_stage5_cluster_tranches);
	UT_RUN(test_rdma_wait_events_are_contiguous);
	UT_RUN(test_rdma_spec613_header_policies);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
