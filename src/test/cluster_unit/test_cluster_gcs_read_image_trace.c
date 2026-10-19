/* Exercise the real read-image waiter with controlled transport and clock
 * boundaries.  The observer is diagnostic only; no authority service is
 * replaced with a fabricated successful decision in product code. */
#include "postgres.h"

#include <setjmp.h>
#include <unistd.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_xnode_lever.h"
#include "storage/backendid.h"
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Only the private fields touched by the extracted waiter are needed here.
 * Wire headers, authority snapshots and BufferDesc use production types. */
typedef struct ClusterGcsBlockOutstandingSlot {
	bool in_use, reply_received, reply_sf_dep_valid, stale;
	uint8 reply_sf_flags;
	uint64 request_id, request_epoch;
	int32 expected_master_node;
	GcsBlockReplyHeader reply_header;
	char reply_block_data[GCS_BLOCK_DATA_SIZE];
	ClusterSfDepVec reply_sf_dep_vec;
	ConditionVariable reply_cv;
} ClusterGcsBlockOutstandingSlot;

typedef struct ClusterGcsBlockBackendBlock {
	LWLockPadded lock;
	ClusterGcsBlockOutstandingSlot slot;
} ClusterGcsBlockBackendBlock;

static struct {
	pg_atomic_uint64 retransmit_attempt_count, retransmit_send_count;
	pg_atomic_uint64 block_forward_sent_count, block_checksum_fail_count;
	pg_atomic_uint64 block_master_not_holder_count;
} fixture_shared, *ClusterGcsBlock = &fixture_shared;

int cluster_node_id = 0;
int cluster_gcs_reply_timeout_ms = 3;
int cluster_gcs_block_retransmit_max_retries = 0;
bool cluster_read_scache = false;
bool cluster_trace_normal_stop = false;
volatile uint32 CritSectionCount = 0;
BackendId MyBackendId = 3;
PGPROC *MyProc = NULL;
Latch *MyLatch = NULL;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

static ClusterGcsBlockBackendBlock fixture_backend;
static BufferDesc fixture_buffer;
static PcmAuthoritySnapshot fixture_authority;
static TimestampTz fixture_now;
static int scenario, sends, releases, installs, cv_waits, lock_depth;
static int log_level, error_code, injected_log_failures;
static bool caught_error, result, retry_denied;
static char log_line[4096], logs[65536], error_detail[1024];
static GcsBlockForwardPayload last_forward;

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	log_level = elevel;
	return elevel >= ERROR || elevel == LOG;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errmsg_internal(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(log_line, sizeof(log_line), fmt, args);
	va_end(args);
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(error_detail, sizeof(error_detail), fmt, args);
	va_end(args);
	return 0;
}

int
errcode(int code)
{
	error_code = code;
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (log_level >= ERROR)
		siglongjmp(*PG_exception_stack, 1);
	if (scenario == 10 && injected_log_failures++ == 0) {
		error_code = ERRCODE_OUT_OF_MEMORY;
		siglongjmp(*PG_exception_stack, 1);
	}
	if (lock_depth != 0)
		abort();
	strncat(logs, log_line, sizeof(logs) - strlen(logs) - 2);
	strncat(logs, "\n", sizeof(logs) - strlen(logs) - 1);
}

void
pg_re_throw(void)
{
	siglongjmp(*PG_exception_stack, 1);
}

void ExceptionalCondition(const char *condition pg_attribute_unused(),
						  const char *file pg_attribute_unused(), int line pg_attribute_unused())
{ abort(); }

TimestampTz GetCurrentTimestamp(void) { return fixture_now; }
static uint64 gcs_block_pcm_x_monotonic_us(void) { return (uint64)fixture_now; }
uint64 cluster_epoch_get_current(void) { return 71; }
bool cluster_pcm_lock_authority_matches(BufferTag tag pg_attribute_unused(),
										const PcmAuthoritySnapshot *a pg_attribute_unused())
{ return scenario != 7; }
bool cluster_gcs_block_resource_x_local_s_barrier_active(BufferTag tag pg_attribute_unused())
{ return false; }
void cluster_gcs_block_dedup_register_backend_exit_hook(void) {}
static ClusterGcsBlockBackendBlock *gcs_block_my_block(void) { return &fixture_backend; }
static ClusterGcsBlockOutstandingSlot *
gcs_block_reserve_slot(BufferTag tag pg_attribute_unused(), uint8 transition pg_attribute_unused(),
					   int32 master pg_attribute_unused(), uint64 *request_id)
{
	fixture_backend.slot.in_use = true;
	fixture_backend.slot.request_id = *request_id = 51;
	return &fixture_backend.slot;
}
static void gcs_block_release_slot(ClusterGcsBlockOutstandingSlot *slot)
{ releases++; slot->in_use = false; }
static long gcs_block_backoff_ms_for_retry(int attempt pg_attribute_unused()) { return 1; }
static bool gcs_block_pcm_x_next_request_id(uint64 *id) { ++*id; return true; }
SCN cluster_pcm_lock_pi_watermark_scn_query(BufferTag tag pg_attribute_unused()) { return 99; }
static uint32 gcs_block_compute_checksum(const char *data pg_attribute_unused()) { return 123; }
static void
gcs_block_install_reply_block(BufferDesc *buf pg_attribute_unused(),
							  const char *data pg_attribute_unused(),
							  XLogRecPtr lsn pg_attribute_unused(),
							  ClusterGcsBlockOutstandingSlot *slot pg_attribute_unused())
{
	if (scenario == 6)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("controlled install refusal")));
	installs++;
}
static void gcs_block_stamp_touched(int32 a pg_attribute_unused(), int32 b pg_attribute_unused()) {}
bool cluster_pcm_lock_apply_gcs_transition(BufferTag tag pg_attribute_unused(),
										 PcmLockTransition transition pg_attribute_unused(),
										 int32 node pg_attribute_unused()) { return true; }
void cluster_lever_a_note_remote_ack_degraded(void) {}

bool LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{ lock_depth++; return true; }
void LWLockRelease(LWLock *lock pg_attribute_unused()) { lock_depth--; }
int WaitLatch(Latch *latch pg_attribute_unused(), int events pg_attribute_unused(),
			  long timeout, uint32 event pg_attribute_unused())
{ fixture_now += timeout * 1000; return WL_TIMEOUT; }
void ResetLatch(Latch *latch pg_attribute_unused()) {}
void ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused()) {}
bool ConditionVariableCancelSleep(void) { return true; }
bool ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(), long timeout,
								uint32 event pg_attribute_unused())
{ cv_waits++; fixture_now += timeout * 1000; return true; }

static bool
gcs_block_stage_forward_wait(ClusterGcsBlockOutstandingSlot *slot, int32 holder pg_attribute_unused(),
							 const GcsBlockForwardPayload *fwd,
							 const PcmAuthoritySnapshot *authority pg_attribute_unused(),
							 bool *denied)
{
	sends++;
	last_forward = *fwd;
	if (scenario == 4 || scenario == 5) {
		*denied = scenario == 4;
		return false;
	}
	if (scenario != 0) {
		slot->reply_received = true;
		slot->reply_header.request_id = fwd->request_id;
		slot->reply_header.epoch = 71;
		slot->reply_header.sender_node = 1;
		slot->reply_header.status = scenario == 3 ? GCS_BLOCK_REPLY_DENIED_PENDING_X
			: scenario == 8 ? GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE
			: scenario == 9 && sends == 1 ? GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER
			: GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
		slot->reply_header.checksum = scenario == 2 ? 124 : 123;
		slot->reply_header.page_lsn = 3456;
		GcsBlockReplyHeaderSetForwardingMasterNode(&slot->reply_header, 0);
	}
	return true;
}

#include "test_cluster_gcs_read_image_trace.inc"

static void
run_scenario(int selected, bool trace)
{
	sigjmp_buf outer;
	scenario = selected;
	memset(&fixture_backend, 0, sizeof(fixture_backend));
	memset(&fixture_buffer, 0, sizeof(fixture_buffer));
	memset(&fixture_authority, 0, sizeof(fixture_authority));
	fixture_buffer.tag.spcOid = 1663;
	fixture_buffer.tag.dbOid = 5;
	fixture_buffer.tag.relNumber = 16388;
	fixture_buffer.tag.forkNum = MAIN_FORKNUM;
	fixture_buffer.tag.blockNum = 137;
	fixture_authority.state = PCM_STATE_X;
	fixture_authority.x_holder_node = 1;
	fixture_authority.master_holder.node_id = 1;
	fixture_authority.master_holder.procno = 29;
	fixture_authority.master_holder.cluster_epoch = 70;
	fixture_authority.master_holder.request_id = 456;
	fixture_authority.transition_count = 18;
	fixture_authority.pending_x_since_lsn = 19;
	fixture_now = 10000;
	sends = releases = installs = cv_waits = lock_depth = 0;
	error_code = injected_log_failures = 0;
	logs[0] = error_detail[0] = '\0';
	caught_error = result = retry_denied = false;
	cluster_trace_normal_stop = trace;
	cluster_read_scache = selected == 8;
	cluster_gcs_block_retransmit_max_retries = selected == 9 ? 1 : 0;
	PG_exception_stack = &outer;
	if (sigsetjmp(outer, 0) == 0)
		result = cluster_gcs_local_master_read_image_and_wait(
			&fixture_buffer, &fixture_authority, false, &retry_denied);
	else
		caught_error = true;
	PG_exception_stack = NULL;
}

UT_TEST(default_off_retains_timeout_and_silence)
{
	run_scenario(0, false);
	UT_ASSERT(caught_error && !retry_denied);
	UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT(strstr(error_detail, "last_status=-1") != NULL);
	UT_ASSERT_EQ(sends, 1); UT_ASSERT_EQ(releases, 1); UT_ASSERT_EQ(cv_waits, 1);
	UT_ASSERT_EQ(strlen(logs), 0);
}

UT_TEST(timeout_is_observer_absence_not_server_verdict)
{
	run_scenario(0, true);
	UT_ASSERT(caught_error && !retry_denied && releases == 1 && installs == 0);
	UT_ASSERT(strstr(logs, "event=wait.timeout") != NULL);
	UT_ASSERT(strstr(logs, "reply_known=0 status=-1") != NULL);
	UT_ASSERT(strstr(logs, "status_source=requester_observation") != NULL);
	UT_ASSERT(strstr(logs, "event=slot.released") != NULL);
	UT_ASSERT(strstr(logs, "request_id=51") != NULL);
	UT_ASSERT(strstr(logs, "request_epoch=71") != NULL);
}

UT_TEST(success_preserves_one_shot_and_exact_identity)
{
	run_scenario(1, true);
	UT_ASSERT(!caught_error && !result && !retry_denied && installs == 1 && releases == 1);
	UT_ASSERT(strstr(logs, "event=install.end") != NULL);
	UT_ASSERT(strstr(logs, "event=request.finish") != NULL);
	UT_ASSERT(strstr(logs, "spc=1663 db=5 rel=16388 fork=0 block=137") != NULL);
	UT_ASSERT(strstr(logs, "holder_node=1 holder_procno=29 holder_epoch=70 holder_request_id=456") != NULL);
	UT_ASSERT(strstr(logs, "transition_count=18 pending_x_since_lsn=19") != NULL);
	UT_ASSERT(strstr(logs, "authoritative=0") != NULL);
	UT_ASSERT(strstr(logs, "process_incarnation_known=0") != NULL);
	UT_ASSERT(strstr(logs, "checksum_expected=123 checksum_actual=123") != NULL);
}

UT_TEST(checksum_failure_preserves_refusal_and_values)
{
	run_scenario(2, true);
	UT_ASSERT(caught_error && installs == 0 && releases == 1);
	UT_ASSERT(strstr(logs, "event=checksum.reject") != NULL);
	UT_ASSERT(strstr(logs, "checksum_expected=124 checksum_actual=123") != NULL);
}

UT_TEST(pending_denial_returns_original_rearm_disposition)
{
	run_scenario(3, true);
	UT_ASSERT(!caught_error && !result && retry_denied && installs == 0 && releases == 1);
	UT_ASSERT(strstr(logs, "event=wait.reply") != NULL);
	UT_ASSERT(strstr(logs, "retry_denied=1") != NULL);
}

UT_TEST(enqueue_refusal_and_error_cleanup_are_distinct)
{
	run_scenario(4, true);
	UT_ASSERT(!caught_error && retry_denied && releases == 1);
	UT_ASSERT(strstr(logs, "event=forward.refused") != NULL);
	run_scenario(5, true);
	UT_ASSERT(caught_error && error_code == ERRCODE_CONNECTION_FAILURE && releases == 1);
	UT_ASSERT(strstr(logs, "event=slot.released.error") != NULL);
}

UT_TEST(install_error_releases_once_without_claiming_install)
{
	run_scenario(6, true);
	UT_ASSERT(caught_error && error_code == ERRCODE_DATA_CORRUPTED && releases == 1);
	UT_ASSERT(strstr(logs, "event=install.begin") != NULL);
	UT_ASSERT(strstr(logs, "event=install.end") == NULL);
	UT_ASSERT(strstr(logs, "event=slot.released.error") != NULL);
}

UT_TEST(displaced_authority_never_reserves_or_sends)
{
	run_scenario(7, true);
	UT_ASSERT(!caught_error && retry_denied && sends == 0 && releases == 0);
	UT_ASSERT(strstr(logs, "event=authority.displaced") != NULL);
	UT_ASSERT(strstr(logs, "request_id=0") != NULL);
}

UT_TEST(durable_downgrade_result_is_unchanged)
{
	run_scenario(8, true);
	UT_ASSERT(!caught_error && result && installs == 1 && releases == 1);
	UT_ASSERT(strstr(logs, "durable_s=1") != NULL);
}

UT_TEST(retry_uses_new_request_and_same_operation_parent)
{
	run_scenario(9, true);
	UT_ASSERT(!caught_error && !result && installs == 1 && releases == 1 && sends == 2);
	UT_ASSERT(strstr(logs, "request_id=51") != NULL);
	UT_ASSERT(strstr(logs, "request_id=52") != NULL);
	UT_ASSERT(strstr(logs, "operation_request_id=51") != NULL);
}

UT_TEST(critical_trace_gap_is_disclosed_after_safe_reentry)
{
	const char *skipped;
	CritSectionCount = 1;
	run_scenario(1, true);
	UT_ASSERT(!caught_error && installs == 1 && releases == 1 && strlen(logs) == 0);
	CritSectionCount = 0;
	run_scenario(1, true);
	skipped = strstr(logs, "trace_critical_skipped=");
	UT_ASSERT(skipped != NULL);
	if (skipped != NULL)
		UT_ASSERT(strtoull(skipped + strlen("trace_critical_skipped="), NULL, 10) > 0);
}

UT_TEST(log_error_after_reservation_keeps_original_slot_cleanup)
{
	run_scenario(10, true);
	UT_ASSERT(caught_error && error_code == ERRCODE_OUT_OF_MEMORY);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(sends, 0);
	UT_ASSERT(!fixture_backend.slot.in_use);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(default_off_retains_timeout_and_silence);
	UT_RUN(timeout_is_observer_absence_not_server_verdict);
	UT_RUN(success_preserves_one_shot_and_exact_identity);
	UT_RUN(checksum_failure_preserves_refusal_and_values);
	UT_RUN(pending_denial_returns_original_rearm_disposition);
	UT_RUN(enqueue_refusal_and_error_cleanup_are_distinct);
	UT_RUN(install_error_releases_once_without_claiming_install);
	UT_RUN(displaced_authority_never_reserves_or_sends);
	UT_RUN(durable_downgrade_result_is_unchanged);
	UT_RUN(retry_uses_new_request_and_same_operation_parent);
	UT_RUN(critical_trace_gap_is_disclosed_after_safe_reentry);
	UT_RUN(log_error_after_reservation_keeps_original_slot_cleanup);
	UT_DONE();
	return ut_failed_count > 0 ? 1 : 0;
}
