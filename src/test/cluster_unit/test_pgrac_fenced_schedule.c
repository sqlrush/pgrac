/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_schedule.c
 *    Per-target join/FIFO and global bounded-capacity tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgrac_fenced_schedule.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static void
identity(uint8 target[16], uint8 binding[32], uint8 seed)
{
	memset(target, seed, 16);
	memset(binding, (uint8)(seed + 64), 32);
}

UT_TEST(test_same_binding_joins_only_live_joinable_operation)
{
	PgracFencedScheduleV1 schedule;
	PgracFencedScheduleTicketV1 owner;
	PgracFencedScheduleTicketV1 joiner;
	PgracFencedScheduleTicketV1 queued;
	PgracFencedScheduleSnapshotV1 snapshot;
	uint8 target[16];
	uint8 binding[32];

	identity(target, binding, 1);
	UT_ASSERT(pgrac_fenced_schedule_init(&schedule));
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 10, target, binding, 1000, &owner),
				 PGRAC_FENCED_SCHEDULE_START);
	UT_ASSERT(pgrac_fenced_schedule_set_state(&schedule, &owner, PGRAC_FENCED_STATE_ACTUATING));
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 11, target, binding, 900, &joiner),
				 PGRAC_FENCED_SCHEDULE_JOIN);
	UT_ASSERT_EQ(owner.slot, joiner.slot);
	UT_ASSERT_EQ(owner.serial, joiner.serial);
	UT_ASSERT(pgrac_fenced_schedule_snapshot(&schedule, &owner, &snapshot));
	UT_ASSERT_EQ(snapshot.client_count, 2);
	/* A joiner's shorter deadline never steals the operation owner's deadline. */
	UT_ASSERT_EQ(snapshot.deadline_mono_ns, 1000);

	UT_ASSERT(
		pgrac_fenced_schedule_set_state(&schedule, &owner, PGRAC_FENCED_STATE_PROVEN_DURABLE));
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 12, target, binding, 1200, &queued),
				 PGRAC_FENCED_SCHEDULE_QUEUE);
	UT_ASSERT_NE(owner.serial, queued.serial);
	UT_ASSERT_EQ(pgrac_fenced_schedule_operation_count(&schedule), 2);
}

UT_TEST(test_same_target_is_strict_fifo_and_different_target_starts)
{
	PgracFencedScheduleV1 schedule;
	PgracFencedScheduleTicketV1 first;
	PgracFencedScheduleTicketV1 second;
	PgracFencedScheduleTicketV1 third;
	PgracFencedScheduleTicketV1 other;
	PgracFencedScheduleTicketV1 started;
	PgracFencedScheduleSnapshotV1 snapshot;
	uint8 target[16];
	uint8 other_target[16];
	uint8 binding[32];
	uint8 second_binding[32];
	uint8 third_binding[32];
	uint8 other_binding[32];

	identity(target, binding, 2);
	identity(other_target, other_binding, 3);
	memset(second_binding, 0x91, sizeof(second_binding));
	memset(third_binding, 0x92, sizeof(third_binding));
	UT_ASSERT(pgrac_fenced_schedule_init(&schedule));
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 20, target, binding, 1000, &first),
				 PGRAC_FENCED_SCHEDULE_START);
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 21, target, second_binding, 1100, &second),
				 PGRAC_FENCED_SCHEDULE_QUEUE);
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 22, target, third_binding, 1200, &third),
				 PGRAC_FENCED_SCHEDULE_QUEUE);
	UT_ASSERT_EQ(
		pgrac_fenced_schedule_submit(&schedule, 23, other_target, other_binding, 1300, &other),
		PGRAC_FENCED_SCHEDULE_START);
	UT_ASSERT_EQ(pgrac_fenced_schedule_active_count(&schedule), 2);

	UT_ASSERT(pgrac_fenced_schedule_release(&schedule, &first, &started));
	UT_ASSERT_EQ(started.serial, second.serial);
	UT_ASSERT(pgrac_fenced_schedule_snapshot(&schedule, &started, &snapshot));
	UT_ASSERT_EQ(snapshot.state, PGRAC_FENCED_STATE_RESOLVING);
	UT_ASSERT_EQ(pgrac_fenced_schedule_active_count(&schedule), 2);
	UT_ASSERT(pgrac_fenced_schedule_release(&schedule, &started, &started));
	UT_ASSERT_EQ(started.serial, third.serial);
	UT_ASSERT_EQ(pgrac_fenced_schedule_active_count(&schedule), 2);
	UT_ASSERT(pgrac_fenced_schedule_release(&schedule, &started, &started));
	UT_ASSERT_EQ(started.serial, 0);
	UT_ASSERT_EQ(pgrac_fenced_schedule_active_count(&schedule), 1);
}

UT_TEST(test_cancel_queued_preserves_fifo_and_stale_ticket_is_rejected)
{
	PgracFencedScheduleV1 schedule;
	PgracFencedScheduleTicketV1 first;
	PgracFencedScheduleTicketV1 second;
	PgracFencedScheduleTicketV1 third;
	PgracFencedScheduleTicketV1 started;
	uint8 target[16];
	uint8 binding[32];
	uint8 second_binding[32];
	uint8 third_binding[32];

	identity(target, binding, 4);
	memset(second_binding, 0xa1, sizeof(second_binding));
	memset(third_binding, 0xa2, sizeof(third_binding));
	UT_ASSERT(pgrac_fenced_schedule_init(&schedule));
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 30, target, binding, 1000, &first),
				 PGRAC_FENCED_SCHEDULE_START);
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 31, target, second_binding, 1100, &second),
				 PGRAC_FENCED_SCHEDULE_QUEUE);
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 32, target, third_binding, 1200, &third),
				 PGRAC_FENCED_SCHEDULE_QUEUE);
	UT_ASSERT(pgrac_fenced_schedule_cancel_client(&schedule, 31, &started));
	UT_ASSERT_EQ(started.serial, 0);
	UT_ASSERT_EQ(pgrac_fenced_schedule_operation_count(&schedule), 2);
	UT_ASSERT(pgrac_fenced_schedule_release(&schedule, &first, &started));
	UT_ASSERT_EQ(started.serial, third.serial);
	UT_ASSERT(!pgrac_fenced_schedule_release(&schedule, &first, &second));
}

UT_TEST(test_capacity_129_denies_without_mutating_existing_operations)
{
	PgracFencedScheduleV1 schedule;
	PgracFencedScheduleTicketV1 ticket;
	uint8 target[16];
	uint8 binding[32];
	uint32 i;

	UT_ASSERT(pgrac_fenced_schedule_init(&schedule));
	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++) {
		memset(target, 0, sizeof(target));
		memset(binding, 0, sizeof(binding));
		target[0] = (uint8)(i + 1);
		binding[0] = (uint8)(i + 1);
		UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, (int)i, target, binding,
												  UINT64_C(1000) + i, &ticket),
					 PGRAC_FENCED_SCHEDULE_START);
	}
	UT_ASSERT_EQ(pgrac_fenced_schedule_operation_count(&schedule), PGRAC_FENCED_MAX_OPERATIONS);
	UT_ASSERT_EQ(pgrac_fenced_schedule_active_count(&schedule), PGRAC_FENCED_MAX_OPERATIONS);
	memset(target, 0xee, sizeof(target));
	memset(binding, 0xef, sizeof(binding));
	UT_ASSERT_EQ(pgrac_fenced_schedule_submit(&schedule, 200, target, binding, 2000, &ticket),
				 PGRAC_FENCED_SCHEDULE_DENY);
	UT_ASSERT_EQ(pgrac_fenced_schedule_operation_count(&schedule), PGRAC_FENCED_MAX_OPERATIONS);
	UT_ASSERT_EQ(pgrac_fenced_schedule_active_count(&schedule), PGRAC_FENCED_MAX_OPERATIONS);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_same_binding_joins_only_live_joinable_operation);
	UT_RUN(test_same_target_is_strict_fifo_and_different_target_starts);
	UT_RUN(test_cancel_queued_preserves_fifo_and_stale_ticket_is_rejected);
	UT_RUN(test_capacity_129_denies_without_mutating_existing_operations);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
