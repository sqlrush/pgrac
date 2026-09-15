/* Author: SqlRush <sqlrush@gmail.com> */
/* Real chunk receive/reset and envelope codec; allocator/dispatch boundaries
 * are fixtures. No wire geometry substitution and no socket/cluster fixture. */
#include "postgres.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_scn.h"
#include "../../backend/cluster/cluster_ic_chunk.c"
#include "../../backend/cluster/cluster_ic_envelope.c"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool IsUnderPostmaster = true;
AuxProcType MyAuxProcType = LmonProcess;
int cluster_node_id = 0;
int cluster_interconnect_payload_max_bytes = PGRAC_IC_PAYLOAD_MAX_DEFAULT;
int cluster_interconnect_chunk_reassembly_timeout_ms = 10000;
static MemoryContextData top_context;
MemoryContext TopMemoryContext = &top_context;
MemoryContext CurrentMemoryContext = &top_context;

typedef struct TestContext {
	MemoryContextData header;
	void *allocation;
} TestContext;
static int live_contexts;
static int dispatched_count;
static uint32 diagnostic_active[CLUSTER_MAX_NODES];
static bool dispatch_saw_pending;
static bool dispatch_bytes_valid;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size minsize, Size initial,
							  Size maximum)
{
	TestContext *ctx = calloc(1, sizeof(*ctx));
	Assert(ctx != NULL);
	ctx->header.type = T_AllocSetContext;
	ctx->header.parent = parent;
	ctx->header.name = name;
	live_contexts++;
	return &ctx->header;
}

void *
palloc(Size size)
{
	TestContext *ctx = (TestContext *)CurrentMemoryContext;
	Assert(CurrentMemoryContext != TopMemoryContext && ctx->allocation == NULL);
	ctx->allocation = malloc(size);
	Assert(ctx->allocation != NULL);
	return ctx->allocation;
}

void
MemoryContextDelete(MemoryContext context)
{
	TestContext *ctx = (TestContext *)context;
	free(ctx->allocation);
	free(ctx);
	live_contexts--;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return INT64CONST(1000000);
}
uint64
cluster_epoch_get_current(void)
{
	return 42;
}
SCN
cluster_scn_current(void)
{
	return 99;
}
void
cluster_ic_tier1_set_chunk_reassembly_active(int32 peer, uint32 active)
{
	diagnostic_active[peer] = active;
}

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload, int fd)
{
	int peer = -1;
	uint32 sequence = 0;
	const char *reason = NULL;
	const uint8 *bytes = payload;
	dispatched_count++;
	dispatch_saw_pending = cluster_ic_chunk_normal_stop_poll(&peer, &sequence, &reason)
							   == CLUSTER_NORMAL_STOP_PENDING
						   && sequence == 2;
	dispatch_bytes_valid = env->payload_length == PGRAC_IC_CHUNK_BYTES + 1
						   && env->source_node_id == 3 && env->dest_node_id == 0
						   && env->payload_crc32c == cluster_ic_envelope_compute_crc(env, payload)
						   && bytes[0] == 0x5a && bytes[PGRAC_IC_CHUNK_BYTES - 1] == 0x5a
						   && bytes[PGRAC_IC_CHUNK_BYTES] == 0xa5;
	return true;
}

static bool
receive_chunk(int peer, uint32 sequence)
{
	ClusterICChunkHeader hdr = { 0 };
	ClusterICEnvelope env = { 0 };
	size_t length = sequence == 0 ? PGRAC_IC_CHUNK_BYTES : 1;
	uint8 *frame = malloc(sizeof(hdr) + length);
	bool result;
	Assert(frame != NULL);
	hdr.chunk_seq = sequence;
	hdr.chunk_total = 2;
	hdr.total_payload_len = PGRAC_IC_CHUNK_BYTES + 1;
	hdr.inner_msg_type = 31;
	memcpy(frame, &hdr, sizeof(hdr));
	memset(frame + sizeof(hdr), sequence == 0 ? 0x5a : 0xa5, length);
	env.payload_length = sizeof(hdr) + length;
	env.source_node_id = peer;
	result = cluster_ic_chunk_dispatch_frame(&env, frame, peer);
	free(frame);
	return result;
}

static ClusterNormalStopPollResult
poll_chunk(int *peer, uint32 *sequence)
{
	const char *reason = NULL;
	return cluster_ic_chunk_normal_stop_poll(peer, sequence, &reason);
}

static void
reset_test(void)
{
	for (int peer = 0; peer < CLUSTER_MAX_NODES; peer++)
		cluster_ic_chunk_reset_peer(peer);
	Assert(live_contexts == 0);
	MyAuxProcType = LmonProcess;
	IsUnderPostmaster = true;
	dispatched_count = 0;
	dispatch_saw_pending = dispatch_bytes_valid = false;
}

UT_TEST(test_only_owning_transport_process_can_poll)
{
	int peer = 17;
	uint32 sequence = 17;
	const char *reason = NULL;
	reset_test();
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(peer, -1);
	UT_ASSERT_EQ(sequence, 0);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_INVALID);
	MyAuxProcType = LmsProcess;
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_READY);
	MyAuxProcType = LmsWorker7Process;
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_READY);
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(cluster_ic_chunk_normal_stop_poll(NULL, &sequence, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_actual_two_chunk_ownership_through_dispatch)
{
	int peer;
	uint32 sequence;
	reset_test();
	UT_ASSERT(receive_chunk(3, 0));
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(peer, 3);
	UT_ASSERT_EQ(sequence, 1);
	UT_ASSERT_EQ(live_contexts, 1);
	/* A debug counter is not the reassembly owner. */
	diagnostic_active[3] = 0;
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(receive_chunk(3, 1));
	UT_ASSERT_EQ(dispatched_count, 1);
	UT_ASSERT(dispatch_saw_pending);
	UT_ASSERT(dispatch_bytes_valid);
	UT_ASSERT_EQ(live_contexts, 0);
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_later_malformed_peer_overrides_pending_without_clearing)
{
	int peer;
	uint32 sequence;
	ChunkReassemblyState before;
	reset_test();
	UT_ASSERT(receive_chunk(3, 0));
	UT_ASSERT(receive_chunk(7, 0));
	cluster_chunk_reassembly_state[7].seq_next = 3;
	before = cluster_chunk_reassembly_state[7];
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(peer, 7);
	UT_ASSERT_EQ(memcmp(&before, &cluster_chunk_reassembly_state[7], sizeof(before)), 0);
	UT_ASSERT_EQ(live_contexts, 2);
	cluster_ic_chunk_reset_peer(7);
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(peer, 3);
	UT_ASSERT(receive_chunk(3, 1));
}

UT_TEST(test_context_and_state_must_agree)
{
	int peer;
	uint32 sequence;
	reset_test();
	cluster_chunk_reassembly_state[8].started_at = 1;
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(peer, 8);
	cluster_ic_chunk_reset_peer(8);
	UT_ASSERT(receive_chunk(8, 0));
	cluster_chunk_reassembly_state[8].buf = NULL;
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(live_contexts, 1);
	cluster_ic_chunk_reset_peer(8);
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_real_sequence_reject_is_not_completion)
{
	int peer;
	uint32 sequence;
	reset_test();
	UT_ASSERT(!receive_chunk(3, 1));
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(receive_chunk(3, 0));
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(!receive_chunk(3, 0));
	UT_ASSERT_EQ(dispatched_count, 0);
	UT_ASSERT_EQ(live_contexts, 0);
	/* The original protocol rejection did clear its context. The owner
	 * must retain that failure; an empty poll is never an error waiver. */
	UT_ASSERT_EQ(poll_chunk(&peer, &sequence), CLUSTER_NORMAL_STOP_READY);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_only_owning_transport_process_can_poll);
	UT_RUN(test_actual_two_chunk_ownership_through_dispatch);
	UT_RUN(test_later_malformed_peer_overrides_pending_without_clearing);
	UT_RUN(test_context_and_state_must_agree);
	UT_RUN(test_real_sequence_reject_is_not_completion);
	reset_test();
	UT_DONE();
	return ut_failed_count != 0;
}
