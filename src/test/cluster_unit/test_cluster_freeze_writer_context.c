/*-------------------------------------------------------------------------
 * test_cluster_freeze_writer_context.c
 *    Original backend-local writer context with controlled pin/owner leaves.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_gcs_block.h"
#include "storage/bufmgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
void
ExceptionalCondition(const char *condition pg_attribute_unused(),
	const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}
int cluster_node_id = 0;
static BufferDesc fixture_buf;
static bool pinned, exclusive, terminal_current, object_current;
static uint64 terminal_generation;
static int terminal_reads, descriptor_reads;

static BufferDesc *
fixture_descriptor(int id)
{
	UT_ASSERT_EQ(id, 0);
	descriptor_reads++;
	return &fixture_buf;
}
#define GetBufferDescriptor(id) fixture_descriptor(id)

bool
ClusterBufferHasSharedPin(Buffer buffer)
{
	return buffer == 1 && pinned;
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&fixture_buf));
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	return exclusive;
}

bool cluster_ko_peer_delivery_current_v3(uint64 revision);
bool
cluster_ko_peer_delivery_current_v3(uint64 revision)
{
	UT_ASSERT_EQ(revision, 7);
	return object_current;
}

bool cluster_bufmgr_terminal_stamp_authority(Buffer, const BufferTag *, uint64 *, uint64 *, uint8 *);
bool
cluster_bufmgr_terminal_stamp_authority(Buffer buffer, const BufferTag *tag,
	uint64 *generation, uint64 *epoch, uint8 *state)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(BufferTagsEqual(tag, &fixture_buf.tag));
	UT_ASSERT(pinned && exclusive);
	terminal_reads++;
	*generation = terminal_generation;
	*epoch = 9;
	*state = PCM_STATE_X;
	return terminal_current;
}
#include "test_cluster_freeze_writer_context.inc"

static void
setup(void)
{
	ClusterPcmXWriterLedgerEntry *entry = &cluster_bufmgr_pcm_x_writer_ledger[0];
	memset(&fixture_buf, 0, sizeof(fixture_buf));
	memset(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	fixture_buf.buf_id = 0;
	InitBufferTag(&fixture_buf.tag, &((RelFileLocator){1663, 5, 99}), MAIN_FORKNUM, 7);
	entry->phase = PCM_X_WRITER_LEDGER_ACTIVE;
	entry->buffer_id = 0;
	entry->content_lock = BufferDescriptorGetContentLock(&fixture_buf);
	entry->authority.ref.assertion.resource = fixture_buf.tag;
	entry->authority.ref.assertion.requester_node = 0;
	entry->authority.ref.formation = 3;
	entry->authority.ref.acquisition_generation = 4;
	entry->authority.r4_record_generation = 5;
	entry->authority.buffer_ownership_generation = 6;
	entry->granted.tag = fixture_buf.tag;
	entry->granted.generation = terminal_generation = 6;
	entry->granted.pcm_state = PCM_STATE_X;
	entry->object_checked = true;
	entry->object_revision = 7;
	pinned = exclusive = terminal_current = object_current = true;
	terminal_reads = descriptor_reads = 0;
}

static void
refused_without_output(Buffer buffer)
{
	ResourceXWriterUseContext out, before;
	memset(&out, 0xa5, sizeof(out));
	before = out;
	UT_ASSERT(!ClusterBufferCaptureMutationContext(buffer, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
}

UT_TEST(reject_pin_before_descriptor)
{
	setup(); pinned = false; refused_without_output(1);
	UT_ASSERT_EQ(descriptor_reads + terminal_reads, 0);
	setup(); refused_without_output(0); refused_without_output(-1); refused_without_output(2);
	UT_ASSERT_EQ(descriptor_reads + terminal_reads, 0);
}
UT_TEST(reject_share_or_local_x_without_writer)
{
	setup(); exclusive = false; refused_without_output(1);
	UT_ASSERT_EQ(terminal_reads, 0);
	setup(); cluster_bufmgr_pcm_x_writer_ledger[0].phase = PCM_X_WRITER_LEDGER_UNUSED;
	refused_without_output(1); UT_ASSERT_EQ(terminal_reads, 0);
}
UT_TEST(reject_inactive_or_changed_ledger)
{
	for (int phase = PCM_X_WRITER_LEDGER_HANDOFF; phase <= PCM_X_WRITER_LEDGER_RELEASING; phase++)
	{
		if (phase == PCM_X_WRITER_LEDGER_ACTIVE) continue;
		setup(); cluster_bufmgr_pcm_x_writer_ledger[0].phase = phase;
		refused_without_output(1); UT_ASSERT_EQ(terminal_reads, 0);
	}
	setup(); cluster_bufmgr_pcm_x_writer_ledger[0].authority.ref.assertion.resource.blockNum++;
	refused_without_output(1); UT_ASSERT_EQ(terminal_reads, 0);
}
UT_TEST(reject_stale_original_object_or_terminal)
{
	setup(); object_current = false; refused_without_output(1);
	UT_ASSERT_EQ(terminal_reads, 0);
	setup(); terminal_current = false; refused_without_output(1);
	setup(); terminal_generation++; refused_without_output(1);
}
UT_TEST(capture_and_match_original_active_writer)
{
	ResourceXWriterUseContext out;
	setup();
	UT_ASSERT(ClusterBufferCaptureMutationContext(1, &out));
	UT_ASSERT_EQ(out.ref.acquisition_generation, 4);
	UT_ASSERT_EQ(out.r4_record_generation, 5);
	UT_ASSERT_EQ(out.buffer_ownership_generation, 6);
	UT_ASSERT(ClusterBufferMutationContextMatches(1, &out));
}
UT_TEST(reject_every_changed_logical_context_field)
{
	ResourceXWriterUseContext out;
	for (int field = 0; field < 9; field++)
	{
		setup(); UT_ASSERT(ClusterBufferCaptureMutationContext(1, &out));
		switch (field)
		{
			case 0: out.ref.assertion.resource.blockNum++; break;
			case 1: out.ref.assertion.requester_node++; break;
			case 2: out.ref.formation++; break;
			case 3: out.ref.acquisition_generation++; break;
			case 4: out.r4_record_generation++; break;
			case 5: out.buffer_ownership_generation++; break;
			case 6: out.writer_activation_token++; break;
			case 7: out.resource_x_activation_generation++; break;
			case 8: terminal_current = false; break;
		}
		UT_ASSERT(!ClusterBufferMutationContextMatches(1, &out));
	}
}
UT_TEST(reject_null_output_and_expectation)
{
	setup(); UT_ASSERT(!ClusterBufferCaptureMutationContext(1, NULL));
	UT_ASSERT(!ClusterBufferMutationContextMatches(1, NULL));
	UT_ASSERT_EQ(descriptor_reads + terminal_reads, 0);
}
int
main(void)
{
	UT_PLAN(7);
	UT_RUN(reject_pin_before_descriptor);
	UT_RUN(reject_share_or_local_x_without_writer);
	UT_RUN(reject_inactive_or_changed_ledger);
	UT_RUN(reject_stale_original_object_or_terminal);
	UT_RUN(capture_and_match_original_active_writer);
	UT_RUN(reject_every_changed_logical_context_field);
	UT_RUN(reject_null_output_and_expectation);
	UT_DONE(); return ut_failed_count != 0;
}
