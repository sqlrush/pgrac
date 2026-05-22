/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_status_hint.c
 *	  pgrac spec-3.2 D9 — cluster_unit static-contract tests for
 *	  cross-node TT status hint wire ABI + msg_type slot + counters.
 *
 *	  18+ static tests (v0.3 N5 + M3 enhanced):
 *	    T1   sizeof(ClusterTTStatusHintMsg) == 32 (HC184)
 *	    T2   offsetof(ClusterTTStatusHintMsg.key) == 8
 *	    T3   CLUSTER_TT_STATUS_HINT_V1 == 1 (msg_version stable)
 *	    T4   PGRAC_IC_MSG_TT_STATUS_HINT == 20 (slot 20)
 *	    T5   distinct from sinval_ack 19 + sinval 7 (no collision)
 *	    T6   CLUSTER_IC_PRODUCER_TT_STATUS_HINT == (1u << B_LMON)
 *	    T7   emit/handle_envelope/drain_outbound prototypes linkable
 *	    T8-T13  6 counter getters linkable (v0.3 N5 +drop_unknown_version)
 *	    T14  GUC enum 2 values (disabled=0, all_status=1) — v0.3 删 commit_only
 *	    T15  53R97 ERRCODE_CLUSTER_TT_STATUS_UNKNOWN encodable via MAKE_SQLSTATE
 *	    T16  ClusterTTStatusHintMsg field offsets stable
 *	         (msg_version=0, status=2, flags=4, _reserved16=6, key=8)
 *	    T17  embedded ClusterTTStatusKey 24B preserved
 *	    T18  L176 banned list amend: PGRAC_IC_MSG_TT_STATUS_HINT positive
 *	         presence in ic_envelope.h (symbol exists post-amend)
 *
 *	  Header-only;  behavioral coverage in cluster_tap t/204.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tt_status_hint.c
 *
 * Spec: spec-3.2-mvcc-cluster-path-tt-status-wire.md (v1.0 FROZEN 2026-05-22)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h"
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


/* Stubs — cluster_unit binary doesn't link cluster_tt_status_hint.o. */
int cluster_tt_status_hint_outbound_capacity = 256;
int cluster_tt_status_hint_emit_mode = CLUSTER_TT_STATUS_HINT_EMIT_ALL_STATUS;

void
cluster_tt_status_hint_emit(const ClusterTTStatusKey *key pg_attribute_unused(),
							ClusterTTStatus status pg_attribute_unused())
{}
void
cluster_tt_status_hint_handle_envelope(const ClusterICEnvelope *env pg_attribute_unused(),
									   const void *payload pg_attribute_unused())
{}
void
cluster_tt_status_hint_drain_outbound(void)
{}
void
cluster_tt_status_hint_register_msg_type(void)
{}

#define HINT_GETTER_STUB(name)                                                                     \
	uint64 cluster_tt_status_hint_get_##name(void)                                                 \
	{                                                                                              \
		return 0;                                                                                  \
	}

HINT_GETTER_STUB(emit_count)
HINT_GETTER_STUB(receive_count)
HINT_GETTER_STUB(drop_invalid_count)
HINT_GETTER_STUB(drop_stale_epoch_count)
HINT_GETTER_STUB(drop_unknown_version_count)
HINT_GETTER_STUB(install_count)

Size
cluster_tt_status_hint_shmem_size(void)
{
	return 0;
}
void
cluster_tt_status_hint_shmem_init(void)
{}
void
cluster_tt_status_hint_shmem_register(void)
{}


/* ===== T1: 32B wire ABI ===== */
UT_TEST(test_t1_hint_msg_sizeof_32)
{
	UT_ASSERT_EQ((int)sizeof(ClusterTTStatusHintMsg), 32);
}

/* ===== T2: embedded key at offset 8 ===== */
UT_TEST(test_t2_key_offset_8)
{
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsg, key), 8);
}

/* ===== T3: msg_version V1 ===== */
UT_TEST(test_t3_msg_version_v1)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_HINT_V1, 1);
}

/* ===== T4: msg_type 20 ===== */
UT_TEST(test_t4_msg_type_20)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_TT_STATUS_HINT, 20);
}

/* ===== T5: no collision with other msg_types ===== */
UT_TEST(test_t5_no_collision)
{
	UT_ASSERT_NE((int)PGRAC_IC_MSG_TT_STATUS_HINT, (int)PGRAC_IC_MSG_SINVAL);
	UT_ASSERT_NE((int)PGRAC_IC_MSG_TT_STATUS_HINT, (int)PGRAC_IC_MSG_SINVAL_ACK);
	UT_ASSERT_NE((int)PGRAC_IC_MSG_TT_STATUS_HINT, (int)PGRAC_IC_MSG_GCS_REQUEST);
}

/* ===== T6: producer mask LMON only ===== */
UT_TEST(test_t6_producer_mask_lmon)
{
	UT_ASSERT_EQ((unsigned int)CLUSTER_IC_PRODUCER_TT_STATUS_HINT, (unsigned int)(1u << B_LMON));
}

/* ===== T7: API prototypes linkable ===== */
UT_TEST(test_t7_api_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_hint_emit, NULL);
	UT_ASSERT_NE((void *)cluster_tt_status_hint_handle_envelope, NULL);
	UT_ASSERT_NE((void *)cluster_tt_status_hint_drain_outbound, NULL);
	UT_ASSERT_NE((void *)cluster_tt_status_hint_register_msg_type, NULL);
}

/* ===== T8-T13: 6 counter getters ===== */
UT_TEST(test_t8_emit_count_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_hint_get_emit_count, NULL);
}
UT_TEST(test_t9_receive_count_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_hint_get_receive_count, NULL);
}
UT_TEST(test_t10_drop_invalid_count_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_hint_get_drop_invalid_count, NULL);
}
UT_TEST(test_t11_drop_stale_epoch_count_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_hint_get_drop_stale_epoch_count, NULL);
}
UT_TEST(test_t12_drop_unknown_version_count_linkable)
{
	/* v0.3 N5 — 6th counter for forward-compat reject. */
	UT_ASSERT_NE((void *)cluster_tt_status_hint_get_drop_unknown_version_count, NULL);
}
UT_TEST(test_t13_install_count_linkable)
{
	UT_ASSERT_NE((void *)cluster_tt_status_hint_get_install_count, NULL);
}

/* ===== T14: GUC enum 2 values (v0.3 删 commit_only) ===== */
UT_TEST(test_t14_emit_mode_enum_two_values)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_HINT_EMIT_DISABLED, 0);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_HINT_EMIT_ALL_STATUS, 1);
}

/* ===== T15: 53R97 encodable ===== */
UT_TEST(test_t15_errcode_53r97_encodable)
{
	int sqlstate = MAKE_SQLSTATE('5', '3', 'R', '9', '7');
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_TT_STATUS_UNKNOWN, sqlstate);
}

/* ===== T16: field offsets stable ===== */
UT_TEST(test_t16_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsg, msg_version), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsg, status), 2);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsg, flags), 4);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsg, _reserved16), 6);
	UT_ASSERT_EQ((int)offsetof(ClusterTTStatusHintMsg, key), 8);
}

/* ===== T17: embedded ClusterTTStatusKey 24B preserved ===== */
UT_TEST(test_t17_embedded_key_24)
{
	UT_ASSERT_EQ((int)sizeof(((ClusterTTStatusHintMsg *)0)->key), 24);
}

/* ===== T18: L176 v1.66 amend positive presence ===== */
UT_TEST(test_t18_l176_amend_symbol_present)
{
	/* Compile-time success means symbol exists in cluster_ic_envelope.h.
	 * L176 lint amend allows this identifier; lint script no longer bans
	 * PGRAC_IC_MSG_TT_STATUS_HINT (v1.66). */
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_TT_STATUS_HINT > 0, 1);
}


int
main(void)
{
	UT_RUN(test_t1_hint_msg_sizeof_32);
	UT_RUN(test_t2_key_offset_8);
	UT_RUN(test_t3_msg_version_v1);
	UT_RUN(test_t4_msg_type_20);
	UT_RUN(test_t5_no_collision);
	UT_RUN(test_t6_producer_mask_lmon);
	UT_RUN(test_t7_api_linkable);
	UT_RUN(test_t8_emit_count_linkable);
	UT_RUN(test_t9_receive_count_linkable);
	UT_RUN(test_t10_drop_invalid_count_linkable);
	UT_RUN(test_t11_drop_stale_epoch_count_linkable);
	UT_RUN(test_t12_drop_unknown_version_count_linkable);
	UT_RUN(test_t13_install_count_linkable);
	UT_RUN(test_t14_emit_mode_enum_two_values);
	UT_RUN(test_t15_errcode_53r97_encodable);
	UT_RUN(test_t16_field_offsets);
	UT_RUN(test_t17_embedded_key_24);
	UT_RUN(test_t18_l176_amend_symbol_present);
	UT_DONE();
}
