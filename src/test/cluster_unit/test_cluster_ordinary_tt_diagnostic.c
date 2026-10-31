/* Original ordinary TT reserve/exchange/release, with transport and clock leaves.
 * A stage return is a local API observation, never proof of a wire send. */
#include "postgres.h"

#include "miscadmin.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_gcs_reqid.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_sf_dep.h"
#include "port/pg_crc32c.h"
#include "storage/backendid.h"
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "utils/errcodes.h"
#include "utils/timestamp.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND CLUSTER_GCS_BLOCK_MAX_OUTSTANDING_PER_BACKEND
#define CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE 0
#include "test_cluster_ordinary_tt_types.inc"

/* These private arrays use the original slot types, not shared PG storage. */
static ClusterGcsBlockBackendBlock blocks[3], before[3];
static struct {
	pg_atomic_uint64 block_checksum_fail_count;
} counts, *ClusterGcsBlock = &counts;
static ClusterGcsUndoVerdictPathCounters gcs_undo_verdict_path_counters;
int cluster_node_id = 2, MaxBackends = 3, MyProcPid = 41954;
int cluster_gcs_reply_timeout_ms = 10;
BackendId MyBackendId = 2;
TimestampTz MyStartTimestamp = 17000;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

typedef enum TestAction {
	STAGE_REFUSED,
	STAGE_ERROR,
	NO_REPLY,
	SLEEP_CANCEL,
	EXACT_REPLY,
	WRONG_STATUS,
	NO_TRAILER,
	BAD_CHECKSUM,
	BEFORE_STAGE_ERROR
} TestAction;
static TestAction action;
static int locks, level, sqlstate, hooks, stages, prepares, cancels, sleeps, broadcasts,
	block_lookups;
static unsigned log_count;
static TimestampTz now;
static char message[4096], logs[65536];
static GcsBlockForwardPayload observed;
static ClusterGcsBlockOutstandingSlot *selected;
static GcsBlockReplyHeader output_hdr;
static ClusterGcsUndoVerdictPage output_page;
static uint64 output_generation, output_scn;

bool
errstart(int l, const char *domain pg_attribute_unused())
{
	level = l;
	return l == LOG || l >= ERROR;
}
bool
errstart_cold(int l, const char *domain)
{
	return errstart(l, domain);
}
int
errcode(int code)
{
	sqlstate = code;
	return 0;
}
static int
format_message(const char *fmt, va_list args)
{
	return vsnprintf(message, sizeof(message), fmt, args);
}
int
errmsg_internal(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	format_message(fmt, args);
	va_end(args);
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	format_message(fmt, args);
	va_end(args);
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	UT_ASSERT_EQ(locks, 0);
	if (level >= ERROR) {
		if (PG_exception_stack != NULL)
			siglongjmp(*PG_exception_stack, 1);
		abort();
	}
	/* Refusal diagnostics run after the original slot release. */
	UT_ASSERT(selected == NULL || !selected->in_use);
	log_count++;
	strncat(logs, message, sizeof(logs) - strlen(logs) - 2);
	strncat(logs, "\n", sizeof(logs) - strlen(logs) - 1);
}
void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
ExceptionalCondition(const char *c pg_attribute_unused(), const char *f pg_attribute_unused(),
					 int line pg_attribute_unused())
{
	abort();
}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	locks++;
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	locks--;
}
void
ConditionVariableBroadcast(ConditionVariable *cv)
{
	UT_ASSERT(cv == &blocks[1].capacity_cv);
	broadcasts++;
}
void
ConditionVariablePrepareToSleep(ConditionVariable *cv)
{
	UT_ASSERT(selected != NULL && cv == &selected->reply_cv);
	prepares++;
}
bool
ConditionVariableCancelSleep(void)
{
	cancels++;
	return true;
}
bool
ConditionVariableTimedSleep(ConditionVariable *cv, long timeout, uint32 wait_event)
{
	UT_ASSERT(selected != NULL && cv == &selected->reply_cv);
	UT_ASSERT_EQ(wait_event, WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
	UT_ASSERT_EQ(timeout, sleeps == 0 ? 10 : 6);
	UT_ASSERT_EQ(locks, 0);
	sleeps++;
	if (action == SLEEP_CANCEL)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("fixture cancel")));
	UT_ASSERT(sleeps <= 2);
	now += sleeps == 1 ? 4000 : 6000;
	return true;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return now;
}
uint64
cluster_epoch_get_current(void)
{
	return 70;
}
void
cluster_pcm_lock_check_shutdown_wait(void)
{
	UT_ASSERT(false);
}
void
cluster_gcs_block_dedup_register_backend_exit_hook(void)
{
	hooks++;
}
static ClusterGcsBlockBackendBlock *
gcs_block_my_block(void)
{
	UT_ASSERT_EQ(MyBackendId, 2);
	if (++block_lookups == 2 && action == BEFORE_STAGE_ERROR)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("fixture before stage")));
	return &blocks[1];
}
static void gcs_block_clear_slot_locked(ClusterGcsBlockOutstandingSlot *slot);
static uint32 gcs_block_compute_checksum(const char *data);
static bool
gcs_block_stage_forward_wait(ClusterGcsBlockOutstandingSlot *slot, int32 dest,
							 const GcsBlockForwardPayload *fwd, void *expected, void *retry)
{
	UT_ASSERT_EQ(locks, 0);
	UT_ASSERT_EQ(dest, 1);
	UT_ASSERT(expected == NULL && retry == NULL);
	UT_ASSERT(slot == &blocks[1].slots[6] && slot->in_use);
	selected = slot;
	observed = *fwd;
	stages++;
	if (action == STAGE_ERROR)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("fixture stage error")));
	if (action == STAGE_REFUSED)
		return false;
	if (action == NO_REPLY || action == SLEEP_CANCEL)
		return true;
	slot->reply_received = true;
	slot->reply_header.request_id = fwd->request_id;
	slot->reply_header.epoch = fwd->epoch;
	slot->reply_header.requester_backend_id = fwd->requester_backend_id;
	slot->reply_header.status = action == WRONG_STATUS ? GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT
													   : GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT;
	slot->reply_undo_trailer_valid = action != NO_TRAILER;
	slot->reply_undo_tt_generation = 77;
	slot->reply_undo_authority_scn = 9002;
	memset(slot->reply_block_data, 0x51, sizeof(slot->reply_block_data));
	slot->reply_header.checksum = gcs_block_compute_checksum(slot->reply_block_data);
	if (action == BAD_CHECKSUM)
		slot->reply_header.checksum ^= 1;
	return true;
}

#include "test_cluster_ordinary_tt_diagnostic.inc"

static BufferTag
fixture(TestAction next)
{
	BufferTag tag = GcsBlockUndoFetchTagMake(1025, 6);
	unsigned backend, index;

	action = next;
	memset(blocks, 0, sizeof(blocks));
	memset(&counts, 0, sizeof(counts));
	memset(&gcs_undo_verdict_path_counters, 0, sizeof(gcs_undo_verdict_path_counters));
	memset(&observed, 0, sizeof(observed));
	memset(&output_hdr, 0xa5, sizeof(output_hdr));
	memset(&output_page, 0xa5, sizeof(output_page));
	output_generation = output_scn = 0xa5a5;
	for (backend = 0; backend < lengthof(blocks); backend++) {
		blocks[backend].next_request_id = 51;
		for (index = 0; index < lengthof(blocks[backend].slots); index++) {
			ClusterGcsBlockOutstandingSlot *slot = &blocks[backend].slots[index];
			slot->in_use = true;
			slot->request_id = 1000 + backend * 10 + index;
			slot->tag = tag;
			slot->tag.blockNum += index + 100;
		}
	}
	blocks[1].slots[6].in_use = false;
	memcpy(before, blocks, sizeof(blocks));
	selected = NULL;
	locks = level = sqlstate = hooks = stages = prepares = cancels = sleeps = broadcasts = 0;
	block_lookups = 0;
	log_count = 0;
	logs[0] = '\0';
	now = 12500;
	return tag;
}

static bool
exchange(BufferTag tag, bool authoritative, bool authority_kind, SCN pair_scn, bool *caught)
{
	volatile bool fetched = false;

	*caught = false;
	PG_TRY();
	{
		fetched = gcs_block_undo_verdict_wire_exchange_trace_impl(
			1, tag, 70, 798, pair_scn, authoritative, authority_kind, &output_hdr, &output_page,
			&output_generation, &output_scn);
	}
	PG_CATCH();
	{
		*caught = true;
	}
	PG_END_TRY();
	return fetched;
}

static void
unchanged_peers_and_released_owner(void)
{
	unsigned backend, index;

	for (backend = 0; backend < lengthof(blocks); backend++)
		for (index = 0; index < lengthof(blocks[backend].slots); index++)
			if (backend != 1 || index != 6)
				UT_ASSERT_EQ(memcmp(&blocks[backend].slots[index], &before[backend].slots[index],
									sizeof(blocks[backend].slots[index])),
							 0);
	UT_ASSERT(!blocks[1].slots[6].in_use);
	UT_ASSERT_EQ(blocks[1].slots[6].request_id, 0);
	UT_ASSERT_EQ(blocks[1].slots[6].request_epoch, 0);
	UT_ASSERT_EQ(blocks[1].next_request_id, 52);
	UT_ASSERT_EQ(blocks[0].next_request_id, 51);
	UT_ASSERT_EQ(blocks[2].next_request_id, 51);
	UT_ASSERT_EQ(broadcasts, 1);
	UT_ASSERT_EQ(locks, 0);
}

static void
ordinary_log_identity(void)
{
	char key[256];

	snprintf(key, sizeof(key), "node=2 origin=1 request=" UINT64_FORMAT " xid=798 epoch=70",
			 gcs_reqid_requester(2, 1, 51));
	UT_ASSERT(strstr(logs, key) != NULL);
	snprintf(key, sizeof(key), " tag=%u/%u/%u/%u/%u ", observed.tag.spcOid, observed.tag.dbOid,
			 observed.tag.relNumber, observed.tag.forkNum, observed.tag.blockNum);
	UT_ASSERT(strstr(logs, key) != NULL);
	UT_ASSERT(strstr(logs, " backend=2 stage_started=1 ") != NULL);
	UT_ASSERT_EQ(observed.request_id, gcs_reqid_requester(2, 1, 51));
	UT_ASSERT_EQ(observed.original_requester_node, 2);
	UT_ASSERT_EQ(observed.requester_backend_id, 2);
	UT_ASSERT_EQ(log_count, 1);
}

UT_TEST(stage_false_keeps_transport_error_and_original_identity_after_release)
{
	bool caught;
	BufferTag tag = fixture(STAGE_REFUSED);

	UT_ASSERT(!exchange(tag, false, false, InvalidScn, &caught));
	UT_ASSERT(caught);
	UT_ASSERT_EQ(sqlstate, ERRCODE_CONNECTION_FAILURE);
	ordinary_log_identity();
	UT_ASSERT(strstr(logs, "stage_result=0") != NULL);
	UT_ASSERT(strstr(logs, "phase=EXCHANGE_EXCEPTION received=0 status=-1 trailer=0 checksum_ok=-1")
			  != NULL);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.calls, 1);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.request_enqueues, 0);
	UT_ASSERT_EQ(prepares | cancels | sleeps, 0);
	UT_ASSERT_EQ(stages, 1);
	unchanged_peers_and_released_owner();
}

UT_TEST(stage_exception_keeps_unknown_result_and_original_cancel)
{
	bool caught;
	BufferTag tag = fixture(STAGE_ERROR);

	UT_ASSERT(!exchange(tag, false, false, InvalidScn, &caught));
	UT_ASSERT(caught);
	UT_ASSERT_EQ(sqlstate, ERRCODE_QUERY_CANCELED);
	ordinary_log_identity();
	UT_ASSERT(strstr(logs, "stage_result=-1") != NULL);
	UT_ASSERT_EQ(stages, 1);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.request_enqueues, 0);
	unchanged_peers_and_released_owner();
}

UT_TEST(stage_true_without_reply_keeps_original_deadline_and_false_result)
{
	bool caught;
	BufferTag tag = fixture(NO_REPLY);

	UT_ASSERT(!exchange(tag, false, false, InvalidScn, &caught));
	UT_ASSERT(!caught);
	ordinary_log_identity();
	UT_ASSERT(strstr(logs, "stage_result=1") != NULL);
	UT_ASSERT(strstr(logs, "phase=WIRE_RESULT received=0 status=-1 trailer=0 checksum_ok=-1")
			  != NULL);
	UT_ASSERT_EQ(now, 22500);
	UT_ASSERT_EQ(stages, 1);
	UT_ASSERT_EQ(prepares, 1);
	UT_ASSERT_EQ(cancels, 1);
	UT_ASSERT_EQ(sleeps, 2);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.request_enqueues, 1);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.sync_exchanges, 1);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.matched_replies, 0);
	UT_ASSERT_EQ(output_generation, 0xa5a5);
	unchanged_peers_and_released_owner();
}

UT_TEST(cancel_after_stage_does_not_relabel_stage_or_resubmit)
{
	bool caught;
	BufferTag tag = fixture(SLEEP_CANCEL);

	UT_ASSERT(!exchange(tag, false, false, InvalidScn, &caught));
	UT_ASSERT(caught);
	UT_ASSERT_EQ(sqlstate, ERRCODE_QUERY_CANCELED);
	ordinary_log_identity();
	UT_ASSERT(strstr(logs, "stage_result=1") != NULL);
	UT_ASSERT_EQ(stages, 1);
	UT_ASSERT_EQ(sleeps, 1);
	/* The original catch releases the slot; CV teardown belongs to PG error cleanup. */
	UT_ASSERT_EQ(cancels, 0);
	unchanged_peers_and_released_owner();
}

UT_TEST(invalid_reply_keeps_status_trailer_and_checksum_observations_separate)
{
	static const struct {
		TestAction action;
		bool trailer;
		int checksum;
	} cases[]
		= { { WRONG_STATUS, true, -1 }, { NO_TRAILER, false, -1 }, { BAD_CHECKSUM, true, 0 } };
	unsigned i;

	for (i = 0; i < lengthof(cases); i++) {
		bool caught;
		char expected[128];
		BufferTag tag = fixture(cases[i].action);

		UT_ASSERT(!exchange(tag, false, false, InvalidScn, &caught));
		UT_ASSERT(!caught);
		ordinary_log_identity();
		UT_ASSERT(strstr(logs, "stage_result=1") != NULL);
		snprintf(expected, sizeof(expected), "received=1 status=%d trailer=%d checksum_ok=%d",
				 cases[i].action == WRONG_STATUS ? GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT
												 : GCS_BLOCK_REPLY_UNDO_VERDICT_RESULT,
				 cases[i].trailer, cases[i].checksum);
		UT_ASSERT(strstr(logs, expected) != NULL);
		UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.matched_replies, 1);
		UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.verified_replies, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&counts.block_checksum_fail_count),
					 cases[i].action == BAD_CHECKSUM);
		UT_ASSERT_EQ(output_generation, 0xa5a5);
		unchanged_peers_and_released_owner();
	}
}

UT_TEST(success_and_other_families_do_not_emit_ordinary_refusals)
{
	unsigned family;

	for (family = 0; family < 4; family++) {
		bool caught;
		BufferTag tag = fixture(EXACT_REPLY);

		UT_ASSERT(exchange(tag, family == 1 || family == 3, family == 2,
						   family == 3 ? 9000 : InvalidScn, &caught));
		UT_ASSERT(!caught);
		UT_ASSERT_EQ(log_count, 0);
		UT_ASSERT_EQ(output_generation, 77);
		UT_ASSERT_EQ(output_scn, 9002);
		UT_ASSERT_EQ(((unsigned char *)&output_page)[0], 0x51);
		UT_ASSERT_EQ(stages, 1);
		unchanged_peers_and_released_owner();
		if (family != 0) {
			tag = fixture(STAGE_REFUSED);
			UT_ASSERT(!exchange(tag, family == 1 || family == 3, family == 2,
								family == 3 ? 9000 : InvalidScn, &caught));
			UT_ASSERT(caught);
			UT_ASSERT_EQ(sqlstate, ERRCODE_CONNECTION_FAILURE);
			UT_ASSERT(strstr(logs, "ordinary TT") == NULL);
			UT_ASSERT_EQ(log_count, family == 3 ? 1 : 0);
			unchanged_peers_and_released_owner();
		}
	}
}

UT_TEST(missing_exchange_sample_never_invents_stage_or_backend)
{
	BufferTag tag = fixture(NO_REPLY);

	gcs_ordinary_verdict_refusal("PAGE_BINDING", 1, tag, 798, 70, 51, NULL, &output_page);
	UT_ASSERT(strstr(logs, "backend=-1 stage_started=-1 stage_result=-1") != NULL);
	UT_ASSERT_EQ(stages | hooks | prepares | sleeps | broadcasts, 0);
	UT_ASSERT_EQ(memcmp(blocks, before, sizeof(blocks)), 0);
}

UT_TEST(exception_before_stage_preserves_not_entered_observation)
{
	bool caught;
	BufferTag tag = fixture(BEFORE_STAGE_ERROR);

	UT_ASSERT(!exchange(tag, false, false, InvalidScn, &caught));
	UT_ASSERT(caught);
	UT_ASSERT_EQ(sqlstate, ERRCODE_INTERNAL_ERROR);
	UT_ASSERT(strstr(logs, "backend=2 stage_started=0 stage_result=-1") != NULL);
	UT_ASSERT(strstr(logs, "phase=EXCHANGE_EXCEPTION received=0 status=-1 trailer=0 checksum_ok=-1")
			  != NULL);
	UT_ASSERT_EQ(stages | prepares | sleeps | cancels, 0);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.calls, 1);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.request_enqueues, 0);
	unchanged_peers_and_released_owner();
}

UT_TEST(detail_budget_does_not_suppress_exchange_or_cleanup_counters)
{
	unsigned i, emitted = 0;

	/* This is deliberately last: the production logger owns its static budget. */
	for (i = 0; i < 70; i++) {
		bool caught;
		BufferTag tag = fixture(STAGE_REFUSED);

		UT_ASSERT(!exchange(tag, false, false, InvalidScn, &caught));
		UT_ASSERT(caught);
		emitted += log_count;
		UT_ASSERT_EQ(stages, 1);
		UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.calls, 1);
		UT_ASSERT_EQ(gcs_undo_verdict_path_counters.owner_verdict.request_enqueues, 0);
		unchanged_peers_and_released_owner();
	}
	/* Eight preceding ordinary failures and the NULL sample consumed nine. */
	UT_ASSERT_EQ(emitted, 55);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(stage_false_keeps_transport_error_and_original_identity_after_release);
	UT_RUN(stage_exception_keeps_unknown_result_and_original_cancel);
	UT_RUN(stage_true_without_reply_keeps_original_deadline_and_false_result);
	UT_RUN(cancel_after_stage_does_not_relabel_stage_or_resubmit);
	UT_RUN(invalid_reply_keeps_status_trailer_and_checksum_observations_separate);
	UT_RUN(success_and_other_families_do_not_emit_ordinary_refusals);
	UT_RUN(missing_exchange_sample_never_invents_stage_or_backend);
	UT_RUN(exception_before_stage_preserves_not_entered_observation);
	UT_RUN(detail_budget_does_not_suppress_exchange_or_cleanup_counters);
	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
