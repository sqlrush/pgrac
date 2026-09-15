/*-------------------------------------------------------------------------
 *
 * test_cluster_grd_outbound.c
 *    Standalone regression tests for reliable GES cleanup staging.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_grd_outbound.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "cluster/cluster_clean_leave.h"
#include "../../backend/cluster/cluster_grd_work_queue.c"
#include "../../backend/cluster/cluster_grd_outbound.c"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

ProcessingMode Mode = NormalProcessing;
int cluster_lms_workers = 1;
int cluster_lmon_main_loop_interval = 1000;
int MaxBackends = 200;
int cluster_node_id = 0;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

static uint64 ut_log_count;

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel == LOG)
		ut_log_count++;
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* RF-ROOT P6: cluster_grd_outbound.o's send-refusal requeue path logs via
 * errmsg; the standalone fixture swallows it like the other err stubs. */
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

static const ClusterShmemRegion *ut_region;
static LWLockPadded ut_lock;
static LWLockPadded ut_work_lock;
static LWLock *ut_held_lock;
static LWLockMode ut_last_mode;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	void *p = malloc(size);

	UT_ASSERT(p != NULL);
	memset(p, 0, size);
	*found = false;
	return p;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	ut_region = region;
}

LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name)
{
	return strcmp(tranche_name, "ClusterGrdWorkQueue") == 0 ? &ut_work_lock : &ut_lock;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	Assert(ut_held_lock == NULL);
	ut_held_lock = lock;
	ut_last_mode = mode;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	Assert(ut_held_lock == lock);
	ut_held_lock = NULL;
}

bool
LWLockHeldByMe(LWLock *lock)
{
	return ut_held_lock == lock;
}

static uint64 ut_cleanup_deferred;
static uint64 ut_reply_deferred;
static uint64 ut_reply_dropped;
static uint64 ut_lmon_wakeup;

void
cluster_grd_inc_ges_cleanup_deferred(void)
{
	ut_cleanup_deferred++;
}

void
cluster_grd_inc_ges_reply_deferred(void)
{
	ut_reply_deferred++;
}

void
cluster_grd_inc_ges_reply_dropped(void)
{
	ut_reply_dropped++;
}

void
cluster_lmon_duty_mark_dirty(ClusterLmonDuty duty pg_attribute_unused())
{}

void
cluster_lmon_wakeup(void)
{
	Assert(ut_held_lock == NULL);
	ut_lmon_wakeup++;
}

const ClusterICMsgTypeInfo *
cluster_ic_get_msg_type_info(uint8 msg_type pg_attribute_unused())
{
	return NULL;
}

int
cluster_gcs_block_payload_shard(uint8 msg_type pg_attribute_unused(),
								const void *payload pg_attribute_unused(),
								uint16 payload_len pg_attribute_unused(),
								int nworkers pg_attribute_unused())
{
	return -1;
}

bool
cluster_lms_outbound_enqueue(int worker_id pg_attribute_unused(),
							 uint8 msg_type pg_attribute_unused(),
							 uint32 dest_node_id pg_attribute_unused(),
							 const void *payload pg_attribute_unused(),
							 uint16 payload_len pg_attribute_unused())
{
	return false;
}

void
cluster_gcs_block_lmon_prepare_outbound_request(GcsBlockRequestPayload *req pg_attribute_unused(),
												int32 dest_node pg_attribute_unused())
{}

static ClusterICSendResult ut_send_result = CLUSTER_IC_SEND_DONE;
static uint64 ut_send_count;
static uint64 ut_release_seen[2048];
static int ut_release_seen_count;

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type pg_attribute_unused(),
						 int32 dest_node_id pg_attribute_unused(), const void *payload,
						 uint32 payload_len)
{
	ut_send_count++;
	if (payload != NULL && payload_len == sizeof(GesRequestPayload)
		&& ((const GesRequestPayload *)payload)->opcode == GES_REQ_OPCODE_RELEASE
		&& ut_release_seen_count < (int)lengthof(ut_release_seen)) {
		const GesRequestPayload *rel = (const GesRequestPayload *)payload;

		ut_release_seen[ut_release_seen_count++]
			= ((uint64)rel->holder_request_id_lo) | (((uint64)rel->holder_request_id_hi) << 32);
	}
	return ut_send_result;
}

static void
ut_reset_state(void)
{
	UT_ASSERT(ut_region != NULL);
	ut_region->init_fn();
	cluster_grd_work_queue_shmem_init();
	ut_send_result = CLUSTER_IC_SEND_DONE;
	ut_send_count = 0;
	ut_release_seen_count = 0;
	ut_log_count = 0;
	memset(ut_release_seen, 0, sizeof(ut_release_seen));
}

static GesRequestPayload
ut_release(uint64 request_id)
{
	GesRequestPayload rel;

	memset(&rel, 0, sizeof(rel));
	rel.opcode = GES_REQ_OPCODE_RELEASE;
	rel.holder_node_id = 0;
	rel.holder_procno = 17;
	rel.holder_request_id_lo = (uint32)(request_id & UINT64CONST(0xffffffff));
	rel.holder_request_id_hi = (uint32)(request_id >> 32);
	return rel;
}

static void
ut_fill_main_ring(void)
{
	uint8 payload = 0xA5;
	int i;

	for (i = 0; i < PGRAC_GES_OUTBOUND_RING_CAPACITY; i++)
		cluster_grd_outbound_enqueue_lmon_reply(1, &payload, sizeof(payload));
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(), (uint32)PGRAC_GES_OUTBOUND_RING_CAPACITY);
}

UT_TEST(test_cleanup_retry_queue_never_overwrites_oldest)
{
	int i;

	ut_reset_state();
	ut_fill_main_ring();

	for (i = 1; i <= 65; i++) {
		GesRequestPayload rel = ut_release((uint64)i);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}

	/* Pre-fix cleanup_dirty overwrote request_id=1 at the old 64-slot limit. */
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_dirty_depth(), (uint32)65);

	/* Drain the filler, then the deferred releases. */
	while (cluster_grd_outbound_ring_depth() > 0 || cluster_grd_outbound_cleanup_dirty_depth() > 0)
		(void)cluster_grd_outbound_lmon_drain_send();

	UT_ASSERT_EQ(ut_release_seen_count, 65);
	for (i = 0; i < 65; i++)
		UT_ASSERT_EQ(ut_release_seen[i], (uint64)(i + 1));
}

UT_TEST(test_cleanup_hard_error_is_deferred_for_retry)
{
	GesRequestPayload rel;

	ut_reset_state();
	rel = ut_release(UINT64CONST(0xABCDEF));
	cluster_grd_outbound_enqueue_cleanup_release(3, &rel, sizeof(rel));

	ut_send_result = CLUSTER_IC_SEND_HARD_ERROR;
	(void)cluster_grd_outbound_lmon_drain_send();
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth() + cluster_grd_outbound_cleanup_dirty_depth(),
				 (uint32)1);

	ut_send_result = CLUSTER_IC_SEND_DONE;
	(void)cluster_grd_outbound_lmon_drain_send();
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth() + cluster_grd_outbound_cleanup_dirty_depth(),
				 (uint32)0);
	UT_ASSERT_EQ(ut_release_seen_count, 2);
	UT_ASSERT_EQ(ut_release_seen[0], UINT64CONST(0xABCDEF));
	UT_ASSERT_EQ(ut_release_seen[1], UINT64CONST(0xABCDEF));
}

UT_TEST(test_cleanup_retry_pressure_logs_once_per_postmaster_lifetime)
{
	int i;

	ut_reset_state();
	ut_fill_main_ring();

	for (i = 1; i < PGRAC_GES_CLEANUP_DIRTY_WARN50_DEPTH; i++) {
		GesRequestPayload rel = ut_release((uint64)i);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn50_count(), UINT64CONST(0));
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn90_count(), UINT64CONST(0));
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(0));

	{
		GesRequestPayload rel = ut_release((uint64)PGRAC_GES_CLEANUP_DIRTY_WARN50_DEPTH);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn50_count(), UINT64CONST(1));
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn90_count(), UINT64CONST(0));
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(1));

	for (i = PGRAC_GES_CLEANUP_DIRTY_WARN50_DEPTH + 1;
		 i <= PGRAC_GES_CLEANUP_DIRTY_WARN90_DEPTH + 1; i++) {
		GesRequestPayload rel = ut_release((uint64)i);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn50_count(), UINT64CONST(1));
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn90_count(), UINT64CONST(1));
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(2));

	/* Draining below both thresholds does not re-arm lifetime LOG-once. */
	while (cluster_grd_outbound_ring_depth() > 0 || cluster_grd_outbound_cleanup_dirty_depth() > 0)
		(void)cluster_grd_outbound_lmon_drain_send();
	ut_fill_main_ring();
	for (i = 1; i <= PGRAC_GES_CLEANUP_DIRTY_WARN90_DEPTH; i++) {
		GesRequestPayload rel = ut_release((uint64)i);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn50_count(), UINT64CONST(1));
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn90_count(), UINT64CONST(1));
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(2));
}

UT_TEST(test_local_cleanup_reaches_work_owner_and_retains_on_full)
{
	GesRequestPayload rel;
	ClusterGrdWorkItem item;
	uint32 slot;
	const char *reason;

	ut_reset_state();
	rel = ut_release(201);
	cluster_grd_outbound_enqueue_cleanup_release(0, &rel, sizeof(rel));
	for (int i = 0; i < PGRAC_GES_WORK_QUEUE_CAPACITY; i++)
		UT_ASSERT(cluster_grd_work_queue_enqueue(0, &rel, sizeof(rel)));
	UT_ASSERT_EQ(cluster_grd_outbound_lmon_drain_send(), 0);
	UT_ASSERT_EQ(ut_send_count, 0); /* IC self-send is a no-op, not ownership. */
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth() + cluster_grd_outbound_cleanup_dirty_depth(), 1);
	UT_ASSERT_EQ(cluster_grd_work_queue_depth(), PGRAC_GES_WORK_QUEUE_CAPACITY);
	for (int i = 0; i < PGRAC_GES_WORK_QUEUE_CAPACITY; i++)
		UT_ASSERT(cluster_grd_work_queue_dequeue(&item));
	UT_ASSERT_EQ(cluster_grd_outbound_lmon_drain_send(), 1);
	UT_ASSERT_EQ(ut_send_count, 0);
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth() + cluster_grd_outbound_cleanup_dirty_depth(), 0);
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_grd_work_queue_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(cluster_grd_work_queue_dequeue(&item));
	UT_ASSERT_EQ(item.source_node_id, 0);
	UT_ASSERT_EQ(item.payload_len, sizeof(rel));
	UT_ASSERT(memcmp(&rel, item.payload, sizeof(rel)) == 0);
	UT_ASSERT_EQ(cluster_grd_work_queue_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_normal_stop_required_queues_uninitialized)
{
	uint32 slot;
	const char *reason;
	UT_ASSERT_EQ(cluster_grd_work_queue_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_normal_stop_all_three_outbound_queues)
{
	uint32 slot;
	const char *reason;
	ClusterGrdOutboundSlot item;
	GesRequestPayload rel = ut_release(73);
	ut_reset_state();
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_READY);
	ut_fill_main_ring();
	cluster_grd_outbound_enqueue_lmon_reply(1, &rel, sizeof(rel));
	cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	for (int i = 0; i < PGRAC_GES_OUTBOUND_RING_CAPACITY; i++)
		UT_ASSERT(cluster_grd_outbound_dequeue(&item));
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(), 0);
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(reason, "GRD_REPLY_DIRTY") == 0);
	UT_ASSERT_EQ(ut_last_mode, LW_SHARED);
	cluster_grd_outbound_state->cleanup_dirty[cluster_grd_outbound_state->cleanup_dirty_tail].origin
		= 0;
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_dirty_depth(), 1);
	cluster_grd_outbound_state->cleanup_dirty[cluster_grd_outbound_state->cleanup_dirty_tail].origin
		= CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE;
	UT_ASSERT_EQ(cluster_grd_outbound_lmon_drain_send(), 2);
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_READY);
	/* Nonzero cursors and warning counters are not new responsibility. */
	UT_ASSERT(cluster_grd_outbound_state->ring_head != 0);
	cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	ut_send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	UT_ASSERT_EQ(cluster_grd_outbound_lmon_drain_send(), 0);
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	ut_send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	UT_ASSERT_EQ(cluster_grd_outbound_lmon_drain_send(), 1);
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_READY);
	/* IC fixture took ownership; this is only the ring proof. */
}

UT_TEST(test_normal_stop_work_queue_exact_shape_and_lock)
{
	uint32 slot;
	const char *reason;
	GesRequestPayload rel = ut_release(17);
	ClusterGrdWorkItem item;
	ut_reset_state();
	UT_ASSERT_EQ(cluster_grd_work_queue_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(cluster_grd_work_queue_enqueue(3, &rel, sizeof(rel)));
	UT_ASSERT_EQ(cluster_grd_work_queue_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(slot, 0);
	UT_ASSERT_EQ(ut_last_mode, LW_SHARED);
	cluster_grd_work_queue_state->items[0].source_node_id = CLUSTER_MAX_NODES;
	UT_ASSERT_EQ(cluster_grd_work_queue_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_grd_work_queue_state->items[0].source_node_id, CLUSTER_MAX_NODES);
	cluster_grd_work_queue_state->items[0].source_node_id = 3;
	cluster_grd_work_queue_state->head = PGRAC_GES_WORK_QUEUE_CAPACITY;
	UT_ASSERT_EQ(cluster_grd_work_queue_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	cluster_grd_work_queue_state->head = 1;
	UT_ASSERT(cluster_grd_work_queue_dequeue(&item));
	UT_ASSERT_EQ(cluster_grd_work_queue_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	LWLockAcquire(cluster_grd_work_queue_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_grd_work_queue_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	LWLockRelease(cluster_grd_work_queue_lock);
}

UT_TEST(test_normal_stop_outbound_geometry_cannot_fake_empty)
{
	uint32 slot;
	const char *reason;
	ut_reset_state();
	cluster_grd_outbound_state->cleanup_dirty_head = 1;
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	cluster_grd_outbound_state->cleanup_dirty_head = 0;
	cluster_grd_outbound_state->reply_dirty_count = PGRAC_GES_REPLY_DIRTY_BUDGET + 1;
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	cluster_grd_outbound_state->reply_dirty_count = 0;
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason), CLUSTER_NORMAL_STOP_READY);
	LWLockAcquire(cluster_grd_outbound_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_grd_outbound_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	LWLockRelease(cluster_grd_outbound_lock);
}

int
main(void)
{
	cluster_grd_outbound_shmem_register();
	UT_PLAN(8);

	UT_RUN(test_normal_stop_required_queues_uninitialized);
	UT_RUN(test_cleanup_retry_queue_never_overwrites_oldest);
	UT_RUN(test_cleanup_hard_error_is_deferred_for_retry);
	UT_RUN(test_cleanup_retry_pressure_logs_once_per_postmaster_lifetime);
	UT_RUN(test_local_cleanup_reaches_work_owner_and_retains_on_full);
	UT_RUN(test_normal_stop_all_three_outbound_queues);
	UT_RUN(test_normal_stop_work_queue_exact_shape_and_lock);
	UT_RUN(test_normal_stop_outbound_geometry_cannot_fake_empty);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
