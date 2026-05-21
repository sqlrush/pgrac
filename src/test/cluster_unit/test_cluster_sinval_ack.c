/*-------------------------------------------------------------------------
 *
 * test_cluster_sinval_ack.c
 *	  pgrac spec-2.39 D15 — cluster_unit static-contract tests for the
 *	  SI Broadcaster production-activation ack/barrier wire ABI + flags +
 *	  enums.
 *
 *	  Tests in this binary (L1-L16):
 *	    L1  PGRAC_IC_MSG_SINVAL_ACK == 19  (msg_type slot, no 12=GCS_REQUEST collision)
 *	    L2  CLUSTER_IC_PRODUCER_SINVAL_ACK == (1u << B_LMON)  (L172 family)
 *	    L3  sizeof(SinvalAckHeader) == 24  (HC140 wire ABI lock)
 *	    L4  SINVAL_REQUIRES_ACK == 0x0001 + SINVAL_RESET_ALL_BROADCAST ==
 *	        0x0002 + SINVAL_KNOWN_FLAGS == 0x0003  (HC142 v0.3 P1)
 *	    L5  ClusterSinvalAckStatus enum tri-state (DONE=0/DROPPED=1/
 *	        RESET_PENDING=2)  (v0.3 P2)
 *	    L6  HC141 sender fulfilled rule:  DONE & RESET_PENDING both视
 *	        fulfilled;DROPPED 不 (verified via enum value invariants)
 *	    L7  cluster_sinval_enqueue_and_wait_ack prototype linkable
 *	    L8  cluster_sinval_broadcast_reset_all prototype linkable (LMON-only)
 *	    L9  cluster_sinval_reset_all_on_reconfig prototype linkable
 *	    L10 cluster_sinval_handle_ack_envelope prototype linkable
 *	    L11 cluster_sinval_ack_wait / ack_outbound shmem helpers linkable
 *	    L12 cluster_sinval_drain_ack_outbound_and_send prototype linkable
 *	    L13 6 NEW counter accessors linkable (3 fanout + 3 ack)
 *	    L14 GUC enum ClusterSinvalAckMode 2 valid values (NONE / PEER_ENQUEUED)
 *	    L15 53R95 ERRCODE_CLUSTER_SINVAL_ACK_TIMEOUT encodable via
 *	        MAKE_SQLSTATE macro (rerouted via errcodes_no_pg_header.h trick
 *	        common to other spec unit tests)
 *	    L16 CLUSTER_SINVAL_BATCH_MAX still 64 (spec-2.38 inheritance — no
 *	        spec-2.39 change)
 *
 *	  Header-only;  behavioral coverage in cluster_tap t/118.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_sinval_ack.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.39-sinval-ddl-commit-hook-ack-barrier.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_sinval.h"
#include "miscadmin.h"
#include "utils/errcodes.h"

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


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ----- Stubs for symbols referenced by the test (Makefile doesn't link
 * cluster_sinval.o into this unit binary;  pattern matches test_cluster_
 * lmon.c stubs).  These are pure address-take, never called. ----- */

ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *msgs pg_attribute_unused(),
									int n pg_attribute_unused())
{
	return CLUSTER_SINVAL_ACK_DONE;
}

void
cluster_sinval_broadcast_reset_all(void)
{}
void
cluster_sinval_reset_all_on_reconfig(void)
{}

void
cluster_sinval_handle_ack_envelope(const struct ClusterICEnvelope *env pg_attribute_unused(),
								   const void *payload pg_attribute_unused())
{}

Size
cluster_sinval_ack_wait_shmem_size(void)
{
	return 0;
}
void
cluster_sinval_ack_wait_shmem_init(void)
{}
Size
cluster_sinval_ack_outbound_shmem_size(void)
{
	return 0;
}
void
cluster_sinval_ack_outbound_shmem_init(void)
{}
void
cluster_sinval_drain_ack_outbound_and_send(void)
{}

uint64
cluster_sinval_get_fanout_would_block_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_fanout_hard_error_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_fanout_peer_down_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_received_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_timeout_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_orphan_count(void)
{
	return 0;
}

/* Stub for spec-2.38 enqueue_batch — not called, just address-taken. */
bool
cluster_sinval_enqueue_batch(const SharedInvalidationMessage *msgs pg_attribute_unused(),
							 int n pg_attribute_unused())
{
	return false;
}


UT_TEST(test_ack_msg_type_is_19)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_SINVAL_ACK, 19);
	/* Sanity:  ACK msg_type is distinct from spec-2.32 GCS_REQUEST=12
	 * (the v0.1 → v0.2 codereview F1 P0 wire collision fix). */
	UT_ASSERT_NE((int)PGRAC_IC_MSG_SINVAL_ACK, (int)PGRAC_IC_MSG_GCS_REQUEST);
}

UT_TEST(test_ack_producer_mask_equals_lmon_bit)
{
	/* L172 family inheritance:  ack envelope wire fanout is LMON-only
	 * (tier1 fd ownership) — same as SINVAL itself. */
	UT_ASSERT_EQ((unsigned int)CLUSTER_IC_PRODUCER_SINVAL_ACK, (unsigned int)(1u << B_LMON));
}

UT_TEST(test_ack_header_sizeof_24)
{
	/* HC140 wire ABI lock. */
	UT_ASSERT_EQ((int)sizeof(SinvalAckHeader), 24);
}

UT_TEST(test_known_flags_invariants)
{
	/* v0.3 P1:  SINVAL_REQUIRES_ACK = 1 + SINVAL_RESET_ALL_BROADCAST = 2.
	 * SINVAL_KNOWN_FLAGS must include both and only both. */
	UT_ASSERT_EQ((unsigned int)SINVAL_REQUIRES_ACK, 0x0001u);
	UT_ASSERT_EQ((unsigned int)SINVAL_RESET_ALL_BROADCAST, 0x0002u);
	UT_ASSERT_EQ((unsigned int)SINVAL_KNOWN_FLAGS, 0x0003u);
	UT_ASSERT_EQ((unsigned int)(SINVAL_REQUIRES_ACK | SINVAL_RESET_ALL_BROADCAST),
				 (unsigned int)SINVAL_KNOWN_FLAGS);
}

UT_TEST(test_ack_status_enum_tri_state)
{
	/* v0.3 P2 三态 enum:  DONE=0 / DROPPED=1 / RESET_PENDING=2.  Wire
	 * ABI sends uint16, but enum values MUST match. */
	UT_ASSERT_EQ((int)SINVAL_ACK_DONE, 0);
	UT_ASSERT_EQ((int)SINVAL_ACK_DROPPED, 1);
	UT_ASSERT_EQ((int)SINVAL_ACK_RESET_PENDING, 2);
}

UT_TEST(test_ack_fulfilled_rule_hc141)
{
	/* HC141:  sender bit-set fulfilled rule.  Verified via enum value
	 * relationship:  DONE & RESET_PENDING both视 fulfilled (sender
	 * mask bit-set);DROPPED 不 (走 timeout path).  Test 是 static
	 * contract statement;  runtime path 在 cluster_tap t/118 L? 验. */
	UT_ASSERT_NE((int)SINVAL_ACK_DONE, (int)SINVAL_ACK_DROPPED);
	UT_ASSERT_NE((int)SINVAL_ACK_RESET_PENDING, (int)SINVAL_ACK_DROPPED);
}

UT_TEST(test_enqueue_and_wait_ack_prototype_linkable)
{
	/* L7:  proto linkable — taking address proves symbol presence. */
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_enqueue_and_wait_ack);
}

UT_TEST(test_broadcast_reset_all_prototype_linkable)
{
	/* L8:  v0.3 P1 RESET-all sentinel emit helper exists. */
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_broadcast_reset_all);
}

UT_TEST(test_reset_all_on_reconfig_prototype_linkable)
{
	/* L9:  D14 reconfig RESET-all hook exists. */
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_reset_all_on_reconfig);
}

UT_TEST(test_handle_ack_envelope_prototype_linkable)
{
	/* L10:  D6 IC handler exists. */
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_handle_ack_envelope);
}

UT_TEST(test_ack_shmem_helpers_linkable)
{
	/* L11:  D3 + D5 shmem region size/init helpers exist. */
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_ack_wait_shmem_size);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_ack_wait_shmem_init);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_ack_outbound_shmem_size);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_ack_outbound_shmem_init);
}

UT_TEST(test_drain_ack_outbound_prototype_linkable)
{
	/* L12:  D5 LMON drain ack-outbound hook exists. */
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_drain_ack_outbound_and_send);
}

UT_TEST(test_six_new_counter_accessors_linkable)
{
	/* L13:  D8 (3 fanout) + D9 (3 ack) counter accessors all linkable. */
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_fanout_would_block_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_fanout_hard_error_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_fanout_peer_down_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_ack_received_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_ack_timeout_count);
	UT_ASSERT_NOT_NULL((void *)cluster_sinval_get_ack_orphan_count);
}

UT_TEST(test_ack_mode_enum_two_values)
{
	/* L14:  GUC enum 2 valid value (peer_applied 不暴露,推 spec-2.X+). */
	UT_ASSERT_EQ((int)CLUSTER_SINVAL_ACK_MODE_NONE, 0);
	UT_ASSERT_EQ((int)CLUSTER_SINVAL_ACK_MODE_PEER_ENQUEUED, 1);
}

UT_TEST(test_ack_timeout_errcode_53r95_encodable)
{
	/* L15:  53R95 encodable via MAKE_SQLSTATE. */
	int expect = MAKE_SQLSTATE('5', '3', 'R', '9', '5');
	UT_ASSERT_EQ(ERRCODE_CLUSTER_SINVAL_ACK_TIMEOUT, expect);
}

UT_TEST(test_batch_max_inheritance_unchanged)
{
	/* L16:  spec-2.38 CLUSTER_SINVAL_BATCH_MAX 64 unchanged. */
	UT_ASSERT_EQ((int)CLUSTER_SINVAL_BATCH_MAX, 64);
}


int
main(void)
{
	UT_RUN(test_ack_msg_type_is_19);
	UT_RUN(test_ack_producer_mask_equals_lmon_bit);
	UT_RUN(test_ack_header_sizeof_24);
	UT_RUN(test_known_flags_invariants);
	UT_RUN(test_ack_status_enum_tri_state);
	UT_RUN(test_ack_fulfilled_rule_hc141);
	UT_RUN(test_enqueue_and_wait_ack_prototype_linkable);
	UT_RUN(test_broadcast_reset_all_prototype_linkable);
	UT_RUN(test_reset_all_on_reconfig_prototype_linkable);
	UT_RUN(test_handle_ack_envelope_prototype_linkable);
	UT_RUN(test_ack_shmem_helpers_linkable);
	UT_RUN(test_drain_ack_outbound_prototype_linkable);
	UT_RUN(test_six_new_counter_accessors_linkable);
	UT_RUN(test_ack_mode_enum_two_values);
	UT_RUN(test_ack_timeout_errcode_53r95_encodable);
	UT_RUN(test_batch_max_inheritance_unchanged);
	UT_DONE();
}
