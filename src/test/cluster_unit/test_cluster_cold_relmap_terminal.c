/* Real retained WAL/terminal/CLOG through the cross-stage participant.
 * Start, declared relmap SIDE witnesses and installed-writer leaves remain
 * controlled. Author: SqlRush <sqlrush@gmail.com> */
int terminal_retained_fixture_main(int argc, char **argv);
#define TEST_RELMAP_RETAINED_MAIN terminal_retained_fixture_main
#include "test_cluster_relmap_retained_wal.c"
#include "cluster/cluster_cold_recovery_plan_internal.h"
#include "cluster/cluster_recovery_plan.h"
#include "test_cluster_cold_relmap_terminal_namespace.h"
#include "cluster/cluster_relmap_terminal_internal.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_ir.h"
#ifdef TEST_TERMINAL_REAL_START
#include "test_cluster_relmap_original_start.h"
static bool terminal_real_start;
#endif

static ClusterClogNativeBaseHandleV1 *terminal_last_native;
bool cluster_clog_native_terminal_seal_v1(ClusterClogNativeBaseHandleV1 *held)
{ terminal_last_native = held; return test_relmap_native_clog_seal(held); }
bool cluster_clog_native_terminal_current_v1(ClusterClogNativeBaseHandleV1 *held)
{ return test_relmap_native_clog_current(held); }

int terminal_holdoff;
bool terminal_lock_acquire(LWLock *lock, LWLockMode mode);
void terminal_lock_release(LWLock *lock);
bool terminal_lock_acquire(LWLock *lock, LWLockMode mode)
{ (void)lock; (void)mode; UT_ASSERT(false); return false; }
void terminal_lock_release(LWLock *lock)
{ (void)lock; UT_ASSERT(false); }

bool terminal_checkpoint(const ClusterColdTypedV1 *typed, uint32 p,
	ClusterControlRootSnapshot *root, ClusterWalRecoveryCheckpointV1 *checkpoint);
bool terminal_checkpoint(const ClusterColdTypedV1 *typed, uint32 p,
	ClusterControlRootSnapshot *root, ClusterWalRecoveryCheckpointV1 *checkpoint)
{
	(void)typed;
	if (p != 0) return false;
	*root = prefix_base_expected.checkpoint;
	memset(checkpoint, 0, sizeof(*checkpoint));
	checkpoint->source = prefix_base_expected.source;
	checkpoint->checkpoint_start = prefix_base_expected.checkpoint_start;
	checkpoint->checkpoint = prefix_base_expected.native_checkpoint;
	memcpy(checkpoint->native_window, prefix_base_expected.native_window, sizeof(checkpoint->native_window));
	return true;
}

static ClusterColdTypedV1 terminal_typed;
static ClusterColdPlanV1 terminal_plan;
static ColdRelmapInput terminal_input;
static ClusterRelmapRecoveryPins terminal_pins;
static ClusterRecoveryCheckpointOwnerV1 terminal_owner;
static ClusterRecoverySerialGuard terminal_serial;
static bool terminal_episode_complete, terminal_episode_drift, terminal_episode_error;
static bool terminal_current, terminal_revoked, terminal_throw;
static bool terminal_capture_throw_after_detach;
static unsigned terminal_writer_calls, terminal_current_calls;
static const struct ClusterRecoveryStartV1 *terminal_start;
static struct ClusterRecoveryFencePlan *terminal_fence;

bool terminal_output_distinct(const struct ClusterRecoveryStartV1 *start,
	const void *out, Size bytes);
bool terminal_output_distinct(const struct ClusterRecoveryStartV1 *start,
	const void *out, Size bytes)
{
#ifdef TEST_TERMINAL_REAL_START
	if (terminal_real_start) return test_relmap_start_distinct(start, out, bytes);
#endif
	return start == terminal_start && out != NULL && bytes != 0
		&& !cold_relmap_storage_overlap(out, bytes, start, sizeof(terminal_owner))
		&& cold_relmap_output_distinct(&terminal_typed, out, bytes);
}

/* These optional fixture modes are deliberately unselected. Refuse if a
 * future fixture change reaches them; do not link their independent owners. */
TestRelmapNativeOwnerResult test_relmap_native_owner_capture(const ClusterWalSourceRef *source,
	uint64 top, uint64 epoch, unsigned fault, ClusterRelmapOwnerSampleV2 *out)
{
	TestRelmapNativeOwnerResult result = {0};
	(void)source; (void)top; (void)epoch; (void)fault; (void)out;
	UT_ASSERT(false); return result;
}
ClusterWalRetentionPin *test_relmap_native_pin_setup(const ClusterWalRetentionPinThreadRequest *r,
	ClusterRecoverySerialGuard *s, TestRelmapPinRootCurrent root,
	TestRelmapPinSerialCurrent serial, sigjmp_buf *stack)
{
	(void)r; (void)s; (void)root; (void)serial; (void)stack;
	UT_ASSERT(false); return NULL;
}
ClusterWalPinResult test_relmap_native_pin_revalidate(ClusterWalRetentionPin *pin, sigjmp_buf *stack)
{
	(void)pin; (void)stack; UT_ASSERT(false); return CLUSTER_WAL_PIN_UNAVAILABLE;
}
void test_relmap_native_pin_fault(unsigned fault) { (void)fault; UT_ASSERT(false); }
TestRelmapNativePinStats test_relmap_native_pin_stats(void)
{
	TestRelmapNativePinStats result = {0}; UT_ASSERT(false); return result;
}

bool terminal_start_current(const struct ClusterRecoveryStartV1 *start);
void terminal_start_fail(const struct ClusterRecoveryStartV1 *start);
const ClusterColdTypedV1 *terminal_start_inputs(const struct ClusterRecoveryStartV1 *start);
bool terminal_start_owner(const struct ClusterRecoveryStartV1 *start, ClusterRecoveryCheckpointOwnerV1 *out);
bool terminal_installed_writer(ClusterWalWriterToken *writer, uint8 operation[16], uint64 *generation);
bool terminal_sources_current(ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence);
bool test_terminal_saved_native(const ClusterColdPlanV1 *plan);
bool terminal_side_input(const RfSideOnlinePlanV1 *side, const ClusterColdRelmapInputV1 *input);
uint32 terminal_side_count(const RfSideOnlinePlanV1 *side, const ClusterWalSourceRef *source);
void terminal_held_locks(void (*callback)(LWLock *, LWLockMode, void *), void *arg);

bool terminal_start_current(const struct ClusterRecoveryStartV1 *start)
{
#ifdef TEST_TERMINAL_REAL_START
	if (terminal_real_start) return test_relmap_start_current(start);
#endif
	terminal_current_calls++;
	if (terminal_throw) siglongjmp(*PG_exception_stack, 1);
	return start == terminal_start && terminal_current && !terminal_revoked
		&& !inputs_native_recovery && MyBackendType == B_STARTUP;
}
void terminal_start_fail(const struct ClusterRecoveryStartV1 *start)
{
#ifdef TEST_TERMINAL_REAL_START
	if (terminal_real_start) { test_relmap_start_fail(start); return; }
#endif
	if (start == terminal_start) terminal_revoked = true;
}
const ClusterColdTypedV1 *terminal_start_inputs(const struct ClusterRecoveryStartV1 *start)
{
#ifdef TEST_TERMINAL_REAL_START
	if (terminal_real_start) return test_relmap_start_inputs(start);
#endif
	return terminal_start_current(start) ? &terminal_typed : NULL;
}
bool terminal_start_owner(const struct ClusterRecoveryStartV1 *start, ClusterRecoveryCheckpointOwnerV1 *out)
{
#ifdef TEST_TERMINAL_REAL_START
	if (terminal_real_start) return test_relmap_start_owner(start, out);
#endif
	if (!terminal_start_current(start)) return false;
	*out = terminal_owner; return true;
}
bool terminal_installed_writer(ClusterWalWriterToken *writer, uint8 operation[16], uint64 *generation)
{
#ifdef TEST_TERMINAL_REAL_START
	if (terminal_real_start) return test_relmap_start_writer(writer, operation, generation);
#endif
	terminal_writer_calls++;
	if (inputs_native_recovery) return false; /* actual original xlog role gate */
	*writer = terminal_owner.writer;
	memcpy(operation, terminal_owner.operation_uuid, 16);
	*generation = terminal_owner.install_generation;
	return terminal_current;
}
bool terminal_sources_current(ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence)
{
	if (terminal_capture_throw_after_detach && terminal_last_native != NULL
		&& test_terminal_saved_native(&terminal_plan))
		siglongjmp(*PG_exception_stack, 1);
	return typed == &terminal_typed && fence == terminal_fence && terminal_current
		&& memcmp(&typed->sources[0], &prefix_base_expected.source, sizeof(ClusterWalSourceRef)) == 0;
}
bool terminal_side_input(const RfSideOnlinePlanV1 *side, const ClusterColdRelmapInputV1 *input)
{
	if (side != cold_original_plan) return false;
	for (uint32 i = 0; i < terminal_plan.relmap_count; ++i)
		if (memcmp(input, &terminal_plan.relmap_inputs[i].input, sizeof(*input)) == 0)
			return true;
	return false;
}
uint32 terminal_side_count(const RfSideOnlinePlanV1 *side, const ClusterWalSourceRef *source)
{
	return side == cold_original_plan && memcmp(source, &terminal_typed.sources[0], sizeof(*source)) == 0
		? terminal_plan.relmap_count : UINT32_MAX;
}
void terminal_held_locks(void (*callback)(LWLock *, LWLockMode, void *), void *arg)
{
	(void)callback; (void)arg;
}

bool terminal_episode_authority(ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint32, ClusterThreadRecoveryAuthorityV1 *);
bool terminal_episode_visit(ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *, uint32, ClusterUndoRecoveryCheckpointVisitorV1, void *);

/* The original typed/fence authority leaf is controlled; the underlying
 * ROOT/token visitor, scanner, SIDE, CLOG and SLRU remain the real bodies. */
bool terminal_episode_authority(ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence,
	uint32 participant, ClusterThreadRecoveryAuthorityV1 *out)
{
	if (terminal_episode_error) siglongjmp(*PG_exception_stack, 1);
	if (terminal_episode_drift && test_relmap_native_clog_stats().calls != 0)
		terminal_serial.lock_request.request_id++;
	if (!terminal_episode_complete || participant != 0 || !terminal_sources_current(typed, fence)
		|| !terminal_serial.held || terminal_serial.release_uncertain)
		return false;
	*out = cold_original_authority;
	out->serial_guard = &terminal_serial;
	return true;
}
typedef struct TerminalEpisodeVisit { ClusterUndoRecoveryCheckpointVisitorV1 visitor; void *arg; } TerminalEpisodeVisit;
static bool terminal_episode_record(XLogReaderState *reader, void *arg)
{
	TerminalEpisodeVisit *work = arg;
	ClusterWalRecoveryCheckpointV1 cp;
	ClusterControlRootSnapshot root;
	if (!terminal_checkpoint(&terminal_typed, 0, &root, &cp)) return false;
	return work->visitor(&cp, &root, terminal_serial.lock_request.request_id, reader, work->arg);
}
bool terminal_episode_visit(ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence,
	uint32 participant, ClusterUndoRecoveryCheckpointVisitorV1 visitor, void *arg)
{
	ClusterThreadRecoveryAuthorityV1 authority;
	ClusterWalTailObservation observed = {0};
	TerminalEpisodeVisit work = {visitor, arg};
	return terminal_episode_authority(typed, fence, participant, &authority)
		&& cluster_control_root_recovery_visit(&execution_root, &execution_token,
			terminal_episode_record, &work, &observed) == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& observed.complete_end == execution_root.validated_tail_lsn_exclusive
		&& terminal_episode_authority(typed, fence, participant, &authority);
}
bool cluster_clog_native_episode_read_v1(int origin, TransactionId xid, ClusterClogNativeBaseV1 *out,
	ClusterClogNativeBaseHandleV1 **held, const ClusterRelmapTerminalEpisodeV1 *episode)
{
	prefix_base_reads++;
	return test_relmap_native_clog_episode_read(origin, xid, out, PG_exception_stack, held, episode);
}
bool cluster_clog_native_episode_seal_v1(ClusterClogNativeBaseHandleV1 *held,
	const ClusterRelmapTerminalEpisodeV1 *episode)
{
	terminal_last_native = held;
	return test_relmap_native_clog_episode_seal(held, episode);
}

static void terminal_setup(bool aborted)
{
	ClusterColdRelmapInputV1 *input = &terminal_input.input;
#ifdef TEST_TERMINAL_REAL_START
	terminal_real_start = false;
	MyProcPid = getpid();
#endif
	cold_native_real_scope = cold_native_real_clog = true;
	cold_native_setup(false, aborted, false);
	memset(&terminal_typed, 0, sizeof(terminal_typed));
	memset(&terminal_plan, 0, sizeof(terminal_plan));
	memset(&terminal_input, 0, sizeof(terminal_input));
	memset(&terminal_pins, 0, sizeof(terminal_pins));
	memset(&terminal_owner, 0, sizeof(terminal_owner));
	terminal_plan.magic = CLUSTER_COLD_PLAN_MAGIC;
	terminal_plan.phase = COLD_PHASE_SEALED;
	terminal_plan.participant_count = 1;
	terminal_plan.memory_budget = 4 * 1024 * 1024;
	terminal_plan.relmap_count = 1;
	terminal_plan.relmap_inputs = &terminal_input;
	terminal_plan.relmap_recovery = &terminal_pins;
	terminal_input.captured = terminal_input.attempted = terminal_input.applied = true;
	input->source = prefix_base_expected.source;
	input->cut.thread_id = input->source.claim.identity.origin_thread_id;
	input->cut.timeline = input->source.timeline;
	input->cut.owner_incarnation = input->source.claim.identity.origin_owner_incarnation;
	input->cut.physical_lower = execution_root.checkpoint_lower_lsn;
	input->cut.native_redo = prefix_base_expected.native_redo;
	input->cut.tail_end = execution_root.validated_tail_lsn_exclusive;
	input->staged = custody_image;
	input->frame.end_rec_ptr = custody_image.pending_owner.wal_end;
	input->frame.read_rec_ptr = input->frame.end_rec_ptr - 1;
	input->frame.rmid = RM_RELMAP_ID;
	input->frame.info = XLOG_RELMAP_UPDATE;
	terminal_pins.state = CLUSTER_RELMAP_RETENTION_BUILDING;
	terminal_pins.count = terminal_pins.expected = 1;
	terminal_pins.restored = true;
	terminal_pins.attempted[0] = terminal_pins.staged[0] = true;
	terminal_pins.images[0] = custody_image;
	terminal_typed.plan = &terminal_plan;
	terminal_typed.participant_count = terminal_typed.replay_count = 1;
	terminal_typed.system_identifier = custody_image.identity.system_identifier;
	terminal_typed.sources[0] = input->source;
	terminal_typed.participants[0] = input->cut;
	terminal_typed.side_plans[0] = cold_original_plan;
	terminal_owner.writer = inputs_native_writer;
	terminal_owner.operation_uuid[0] = 7;
	terminal_owner.install_generation = 11;
	terminal_owner.start_generation = 12;
	terminal_owner.start_serial = 1;
	terminal_owner.owner_pid = MyProcPid;
	terminal_owner.owner_boot = 9;
	terminal_start = (const struct ClusterRecoveryStartV1 *)&terminal_owner;
	terminal_fence = (struct ClusterRecoveryFencePlan *)&terminal_plan;
	terminal_current = true; terminal_revoked = terminal_throw = false;
	terminal_last_native = NULL; terminal_capture_throw_after_detach = false;
	terminal_writer_calls = terminal_current_calls = 0;
	memset(&terminal_serial, 0, sizeof(terminal_serial));
	terminal_serial.held = true;
	terminal_serial.mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
	terminal_serial.lock_request.request_id = 23;
	terminal_episode_complete = true;
	terminal_episode_drift = terminal_episode_error = false;
}

static void terminal_leave(void)
{
	cluster_undo_recovery_scope_leave_v1(&cold_original_scope);
	/* Original replay handoff moves this exact fence only after scope leave. */
	terminal_typed.tx_fence = terminal_fence;
	inputs_native_recovery = false;
	UT_ASSERT_EQ(cold_original_id(), 0);
}
static void terminal_done(void)
{
#ifdef TEST_TERMINAL_REAL_START
	if (terminal_real_start) UT_ASSERT(test_relmap_start_done());
	terminal_real_start = false;
#endif
	/* Original context cleanup only; never publish or transfer test pins. */
	cold_relmap_terminal_cleanup(&terminal_plan);
	inputs_native_recovery = true;
	cold_native_done(NULL);
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_terminal_capture_handoff_real_commit_abort)
{
	for (unsigned aborted = 0; aborted < 2; ++aborted) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		unsigned reads, syncs;
		bool captured;
		terminal_setup(aborted != 0);
		captured = cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence);
		UT_ASSERT(captured);
		UT_ASSERT_EQ(terminal_writer_calls, 0);
		if (captured) {
			UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
			terminal_leave(); terminal_owner.writer.epoch++; reads = prefix_base_reads;
			syncs = test_relmap_native_clog_stats().file_syncs;
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT_EQ(out->count, 1);
			UT_ASSERT_EQ(out->entries[0].status, aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT_EQ(out->entries[0].top_full_xid, custody_image.pending_owner.top_full_xid);
			UT_ASSERT_EQ(memcmp(&out->entries[0].staged, &custody_image, sizeof(custody_image)), 0);
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT_EQ(prefix_base_reads, reads);
			UT_ASSERT_EQ(test_relmap_native_clog_stats().file_syncs, syncs);
			UT_ASSERT(!terminal_revoked && !terminal_pins.transferred);
		}
		free(out); terminal_done();
	}
}

UT_TEST(test_terminal_missing_and_wrong_stage_refuse)
{
	ClusterColdRelmapTerminalCheckV1 *out = malloc(sizeof(*out));
	terminal_setup(false); terminal_leave();
	memset(out, 0xa5, sizeof(*out));
	UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
	UT_ASSERT_EQ(out->count, 0);
	UT_ASSERT(terminal_revoked);
	UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 0);
	free(out); terminal_done();
}

UT_TEST(test_terminal_drift_and_late_error_keep_original_debt)
{
	for (unsigned fault = 0; fault < 11; ++fault) {
		ClusterColdRelmapTerminalCheckV1 *out = malloc(sizeof(*out));
		bool captured; volatile bool threw = false;
		terminal_setup(false);
		captured = cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence);
		UT_ASSERT(captured);
		if (captured) {
			terminal_leave();
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			if (fault == 0) terminal_typed.sources[0].timeline++;
			if (fault == 1) terminal_input.input.staged.pending_generation++;
			if (fault == 2) terminal_owner.install_generation++;
			if (fault == 3) terminal_current = false;
			if (fault == 4) terminal_throw = true;
			if (fault == 5) terminal_owner.operation_uuid[1]++;
			if (fault == 6) terminal_owner.writer.epoch++;
			if (fault == 7) terminal_owner.start_generation++;
			if (fault == 8) terminal_owner.owner_boot++;
			if (fault == 9) terminal_typed.tx_fence = (struct ClusterRecoveryFencePlan *)out;
			if (fault == 10) terminal_owner.owner_pid++;
			memset(out, 0xa5, sizeof(*out));
			PG_TRY(); { UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out)); }
			PG_CATCH(); { threw = true; } PG_END_TRY();
			terminal_throw = false;
			UT_ASSERT_EQ(threw, fault == 4);
			UT_ASSERT_EQ(out->count, 0);
			UT_ASSERT(terminal_revoked);
			UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 1);
			UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
			UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		}
		free(out); terminal_done();
	}
}

UT_TEST(test_terminal_same_bytes_mutation_and_active_writer_refuse)
{
	for (unsigned active = 0; active < 2; ++active) {
		ClusterColdRelmapTerminalCheckV1 *out = malloc(sizeof(*out));
		terminal_setup(false);
		UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_leave();
		test_relmap_native_clog_mutate(true);
		if (!active) test_relmap_native_clog_mutate(false);
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT(out->count == 0 && terminal_revoked && !terminal_pins.transferred);
		if (active) test_relmap_native_clog_mutate(false);
		free(out); terminal_done();
	}
}

UT_TEST(test_terminal_alias_and_one_shot_budget)
{
	unsigned reads;
	terminal_setup(false);
	UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	reads = prefix_base_reads;
	UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	UT_ASSERT_EQ(prefix_base_reads, reads);
	terminal_leave();
	UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start,
		(ClusterColdRelmapTerminalCheckV1 *)&terminal_typed));
	UT_ASSERT(terminal_typed.plan == &terminal_plan && !terminal_revoked);
	terminal_done();
	terminal_setup(false); terminal_plan.memory_budget = 1;
	UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 0);
	terminal_plan.memory_budget = 4 * 1024 * 1024;
	UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 0);
	terminal_done();
}

UT_TEST(test_terminal_native_reader_refusal_and_error_keep_debt)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		volatile bool threw = false;
		terminal_setup(false);
		/* The original reader still refuses a short file or later
		 * file drift, and its own native ERROR crosses the new wrapper. */
		if (fault == 0) test_relmap_native_clog_fault(6, 2);
		if (fault == 1) test_relmap_native_clog_fault(5, 2);
		if (fault == 2) test_relmap_native_clog_fault(2, 1);
		PG_TRY(); {
			UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		} PG_CATCH(); { threw = true; } PG_END_TRY();
		UT_ASSERT_EQ(threw, fault == 1);
		UT_ASSERT(terminal_plan.relmap_failed && !terminal_pins.transferred);
		UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_done();
	}
}

UT_TEST(test_terminal_unchanged_reopen_accepts)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
	terminal_setup(false);
	UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	terminal_leave();
	test_relmap_native_clog_reopen_same_fd(terminal_last_native, false);
	UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
	UT_ASSERT_EQ(out->count, 1);
	/* An unchanged allocation identity is not part of the contract. */
	test_relmap_native_clog_reopen_same_fd(terminal_last_native, true);
	free(out); terminal_done();
}

UT_TEST(test_terminal_start_extent_alias_before_providers)
{
	unsigned calls;
	char *storage = calloc(1, sizeof(ClusterColdRelmapTerminalCheckV1) + sizeof(terminal_owner));
	terminal_setup(false);
	/* A Start has an opaque extent. Point inside it; enough backing storage
	 * keeps the original broken implementation's writes inside this test. */
	terminal_start = (const struct ClusterRecoveryStartV1 *)storage;
	calls = terminal_current_calls;
	UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start,
		(ClusterColdRelmapTerminalCheckV1 *)(storage + 8)));
	UT_ASSERT_EQ(terminal_current_calls, calls);
	free(storage); terminal_done();
}


UT_TEST(test_terminal_reopen_changes_refuse)
{
	const unsigned faults[] = {0, 7, 2};
	for (unsigned i = 0; i < lengthof(faults); ++i) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		terminal_setup(false);
		UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_leave();
		test_relmap_native_clog_reopen_fault(terminal_last_native, faults[i]);
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT(out->count == 0 && terminal_revoked && !terminal_pins.transferred);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		test_relmap_native_clog_reopen_same_fd(terminal_last_native, true);
		free(out); terminal_done();
	}
}

UT_TEST(test_terminal_failed_or_missing_receipt_aliases_are_untouched)
{
	for (unsigned captured = 0; captured < 2; ++captured) {
		void *aliases[5]; unsigned calls;
		terminal_setup(false);
		if (captured)
			UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_leave();
		terminal_current = false; terminal_throw = true;
		aliases[0] = &terminal_typed; aliases[1] = &terminal_plan;
		aliases[2] = &terminal_input; aliases[3] = &terminal_pins;
		aliases[4] = terminal_plan.relmap_terminal;
		calls = terminal_current_calls;
		for (unsigned i = 0; i < lengthof(aliases); ++i) {
			unsigned char before[16];
			if (aliases[i] == NULL) continue;
			memcpy(before, aliases[i], sizeof(before));
			UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, aliases[i]));
			UT_ASSERT_EQ(memcmp(before, aliases[i], sizeof(before)), 0);
		}
		UT_ASSERT_EQ(terminal_current_calls, calls);
		UT_ASSERT(!terminal_revoked && !terminal_pins.transferred);
		terminal_throw = false; terminal_done();
	}
}

UT_TEST(test_terminal_duplicate_and_partial_error_are_one_shot)
{
	ColdRelmapInput batch[2]; volatile bool threw = false;
	terminal_setup(false);
	batch[0] = batch[1] = terminal_input;
	terminal_plan.relmap_inputs = batch; terminal_plan.relmap_count = 2;
	UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 0);
	UT_ASSERT(terminal_plan.relmap_failed && !terminal_pins.transferred);
	terminal_done();
	terminal_setup(false);
	/* Two distinct declared namespaces; the SIDE declaration witness remains
	 * controlled. ERROR after the first real moved FD must retain both debts. */
	batch[0] = batch[1] = terminal_input;
	batch[1].input.staged.identity.shared = true;
	batch[1].input.staged.identity.dbid = InvalidOid;
	terminal_plan.relmap_inputs = batch; terminal_plan.relmap_count = 2;
	terminal_pins.count = terminal_pins.expected = 2;
	terminal_pins.images[1] = batch[1].input.staged;
	{
		int fd = open(test_root, O_RDONLY | O_DIRECTORY);
		UT_ASSERT(fd >= 0);
		relmap_census_file(fd, cold_native_paths[0], &batch[1].input.staged);
		relmap_census_file(fd, cold_native_paths[1], &batch[1].input.staged);
		UT_ASSERT_EQ(close(fd), 0);
	}
	terminal_capture_throw_after_detach = true;
	PG_TRY(); {
		UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	} PG_CATCH(); { threw = true; } PG_END_TRY();
	terminal_capture_throw_after_detach = false;
	UT_ASSERT(threw && terminal_last_native != NULL);
	UT_ASSERT(terminal_plan.relmap_failed && !terminal_pins.transferred);
	UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
	UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	terminal_done();
}


#ifdef TEST_TERMINAL_REAL_START
static bool terminal_bind_original_start(void)
{
	terminal_leave();
	terminal_real_start = true;
	terminal_start = test_relmap_start_begin(&terminal_typed, MyProcPid);
	return terminal_start != NULL;
}

UT_TEST(test_terminal_original_start_commit_abort_and_repeat)
{
	for (unsigned aborted = 0; aborted < 2; ++aborted) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		unsigned reads, syncs;
		terminal_setup(aborted != 0);
		UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		if (terminal_bind_original_start()) {
			reads = prefix_base_reads; syncs = test_relmap_native_clog_stats().file_syncs;
			UT_ASSERT(test_relmap_start_inputs(terminal_start) == &terminal_typed);
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT(out->count == 1 && out->entries[0].status ==
				(aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED));
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT_EQ(prefix_base_reads, reads);
			UT_ASSERT_EQ(test_relmap_native_clog_stats().file_syncs, syncs);
			UT_ASSERT(test_relmap_start_pending() && !terminal_pins.transferred);
		} else UT_ASSERT(false);
		free(out); terminal_done();
	}
}

UT_TEST(test_terminal_original_start_drift_keeps_debt)
{
	for (unsigned fault = 0; fault < 5; ++fault) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		terminal_setup(false);
		UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		if (terminal_bind_original_start()) {
			UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			test_relmap_start_fault(fault);
			UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
			UT_ASSERT(out->count == 0 && !test_relmap_start_current(terminal_start));
			UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 1);
			UT_ASSERT_EQ(test_relmap_native_clog_stats().open_fds, 1);
		} else UT_ASSERT(false);
		free(out); terminal_done();
	}
}

UT_TEST(test_terminal_original_start_full_alias_guard)
{
	terminal_setup(false);
	UT_ASSERT(cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	if (terminal_bind_original_start()) {
		const void *aliases[] = {terminal_start, (const char *)terminal_start + 8,
			&terminal_typed, &terminal_plan, &terminal_input, &terminal_pins,
			terminal_plan.relmap_terminal};
		for (unsigned i = 0; i < lengthof(aliases); ++i) {
			unsigned char before[16];
			memcpy(before, aliases[i], sizeof(before));
			UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start,
				(ClusterColdRelmapTerminalCheckV1 *)aliases[i]));
			UT_ASSERT_EQ(memcmp(before, aliases[i], sizeof(before)), 0);
			UT_ASSERT(test_relmap_start_current(terminal_start));
		}
		UT_ASSERT(test_relmap_start_pending() && !terminal_pins.transferred);
	} else UT_ASSERT(false);
	terminal_done();
}

UT_TEST(test_terminal_original_start_missing_capture_refuses)
{
	ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
	terminal_setup(false);
	if (terminal_bind_original_start()) {
		UT_ASSERT(!cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT(out->count == 0 && !test_relmap_start_current(terminal_start));
		UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 1);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 0);
	} else UT_ASSERT(false);
	free(out); terminal_done();
}
#endif

#ifdef TEST_TERMINAL_REAL_START
#include "test_cluster_cold_relmap_terminal_batch.inc"
#include "test_cluster_cold_relmap_staging_terminal.inc"
#endif

/* The complete replay episode still owns IR/WALR after the last record's
 * native scope has left. Ordinary scanner permission must remain closed. */
static void terminal_capture_after_record_scope(bool aborted)
{
	ClusterRelmapRecoveryHandle *old = NULL;
	ClusterRelmapV2View view = {0};
	bool captured;
	terminal_setup(aborted);
	view.committed = view.pending_committed = custody_image.committed;
	view.committed_generation = view.pending_committed_generation = custody_image.committed_generation;
	view.pending = custody_image.pending;
	view.pending_generation = custody_image.pending_generation;
	view.pending_owner = custody_image.pending_owner;
	view.pending_complete = view.has_pending = true;
	UT_ASSERT(cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &old));
	UT_ASSERT(old != NULL);
	cluster_relmap_recovery_handle_release_v1(&old);
	cluster_undo_recovery_scope_leave_v1(&cold_original_scope);
	UT_ASSERT_EQ(cold_original_id(), 0);
	UT_ASSERT(!cluster_relmap_recovery_prepare_cold_v1(&custody_image.identity, &view, &old));
	UT_ASSERT(old == NULL);
	captured = cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence);
	UT_ASSERT(captured);
	if (captured) {
		ClusterColdRelmapTerminalCheckV1 *out = calloc(1, sizeof(*out));
		terminal_leave();
		UT_ASSERT(cluster_cold_relmap_terminal_recheck_v1(terminal_start, out));
		UT_ASSERT_EQ(out->count, 1);
		UT_ASSERT_EQ(out->entries[0].status, aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
		free(out);
	}
	UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 1);
	UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
	terminal_done();
}
UT_TEST(test_terminal_episode_after_scope_commit) { terminal_capture_after_record_scope(false); }
UT_TEST(test_terminal_episode_after_scope_abort) { terminal_capture_after_record_scope(true); }

UT_TEST(test_terminal_episode_requires_completed_current_ir)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		terminal_setup(false);
		cluster_undo_recovery_scope_leave_v1(&cold_original_scope);
		if (fault == 0) terminal_episode_complete = false;
		if (fault == 1) terminal_serial.held = false;
		if (fault == 2) terminal_serial.release_uncertain = true;
		UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		UT_ASSERT(terminal_plan.relmap_failed && !terminal_pins.transferred && terminal_pins.count == 1);
		UT_ASSERT_EQ(test_relmap_native_clog_stats().calls, 0);
		UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_done();
	}
}
UT_TEST(test_terminal_episode_late_ir_or_error_keeps_debt)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		bool threw = false;
		terminal_setup(false);
		cluster_undo_recovery_scope_leave_v1(&cold_original_scope);
		terminal_episode_drift = fault == 0; terminal_episode_error = fault == 1;
		PG_TRY();
		{ UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence)); }
		PG_CATCH();
		{ threw = true; }
		PG_END_TRY();
		UT_ASSERT_EQ(threw, fault == 1);
		UT_ASSERT(terminal_plan.relmap_failed && terminal_plan.relmap_terminal_attempted);
		UT_ASSERT(!terminal_pins.transferred && terminal_pins.count == 1);
		UT_ASSERT(!cluster_cold_relmap_terminal_capture_v1(&terminal_typed, terminal_fence));
		terminal_done();
	}
}

#include "test_cluster_cold_relmap_pending_terminal.inc"

int main(int argc, char **argv)
{
	test_program_path = argv[0]; setup_fixture();
#ifdef TEST_TERMINAL_REAL_START
	UT_PLAN(16);
	UT_RUN(test_terminal_episode_after_scope_commit);
	UT_RUN(test_terminal_episode_after_scope_abort);
	UT_RUN(test_terminal_episode_requires_completed_current_ir);
	UT_RUN(test_terminal_episode_late_ir_or_error_keeps_debt);
	UT_RUN(test_terminal_original_start_commit_abort_and_repeat);
	UT_RUN(test_terminal_original_start_drift_keeps_debt);
	UT_RUN(test_terminal_original_start_full_alias_guard);
	UT_RUN(test_terminal_original_start_missing_capture_refuses);
	UT_RUN(test_terminal_batch_both_namespaces_commit_abort_and_order);
	UT_RUN(test_terminal_batch_second_fd_loss_withdraws_whole_output);
	UT_RUN(test_terminal_batch_second_saved_image_drift_keeps_all_debt);
	UT_RUN(test_terminal_batch_partial_capture_error_never_enters_start);
	UT_RUN(test_start_staging_and_terminal_both_required_for_complete_batch);
	UT_RUN(test_start_staging_refusal_does_not_borrow_valid_terminal);
	UT_RUN(test_start_terminal_refusal_does_not_borrow_valid_staging);
	UT_RUN(test_start_staging_repeat_rereads_the_second_copy);
	UT_DONE(); return ut_failed_count != 0;
#endif
	if (argc > 1 && strcmp(argv[1], "handoff") == 0) {
		UT_PLAN(2);
		UT_RUN(test_terminal_capture_handoff_real_commit_abort);
		UT_RUN(test_terminal_start_extent_alias_before_providers);
		UT_DONE(); return ut_failed_count != 0;
	}
	UT_PLAN(22);
	UT_RUN(test_terminal_episode_after_scope_commit);
	UT_RUN(test_terminal_episode_after_scope_abort);
	UT_RUN(test_terminal_episode_requires_completed_current_ir);
	UT_RUN(test_terminal_episode_late_ir_or_error_keeps_debt);
	UT_RUN(test_terminal_capture_handoff_real_commit_abort);
	UT_RUN(test_terminal_missing_and_wrong_stage_refuse);
	UT_RUN(test_terminal_drift_and_late_error_keep_original_debt);
	UT_RUN(test_terminal_same_bytes_mutation_and_active_writer_refuse);
	UT_RUN(test_terminal_alias_and_one_shot_budget);
	UT_RUN(test_terminal_native_reader_refusal_and_error_keep_debt);
	UT_RUN(test_terminal_unchanged_reopen_accepts);
	UT_RUN(test_terminal_reopen_changes_refuse);
	UT_RUN(test_terminal_failed_or_missing_receipt_aliases_are_untouched);
	UT_RUN(test_terminal_duplicate_and_partial_error_are_one_shot);
	UT_RUN(test_terminal_start_extent_alias_before_providers);
	UT_RUN(test_pending_terminal_mixed_commit_abort_and_repeat);
	UT_RUN(test_pending_terminal_does_not_open_legacy_mixed_gates);
	UT_RUN(test_pending_terminal_empty_does_not_manufacture_qualification);
	UT_RUN(test_pending_terminal_source_writer_and_pin_drift_stay_failed);
	UT_RUN(test_pending_terminal_mutation_and_active_writer_keep_fd_debt);
	UT_RUN(test_pending_terminal_capture_and_start_errors_keep_debt);
	UT_RUN(test_pending_terminal_aliases_refuse_before_owner_or_output);
	UT_DONE(); return ut_failed_count != 0;
}
