/* Original Startup handoff, with process-exit and allocation owners bounded. */
#include "postgres.h"
#include <setjmp.h>
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_plan.h"
#include "storage/ipc.h"
#include "utils/resowner.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterColdTypedV1 attempts[2];
static ClusterColdTypedV1 *cluster_recovery_cold_inputs;
static struct ClusterRecoveryStartV1 *cluster_recovery_cold_start;
static pg_on_exit_callback exit_callback;
static Datum exit_arg;
static unsigned registrations, cancellations, destructions;
static bool fatal_armed;
static jmp_buf fatal_jump;

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

bool
errstart(int elevel, const char *domain)
{
	(void)domain;
	UT_ASSERT_EQ(elevel, FATAL);
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errmsg(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

void
errfinish(const char *filename, int lineno, const char *funcname)
{
	(void)filename;
	(void)lineno;
	(void)funcname;
	if (!fatal_armed)
		abort();
	longjmp(fatal_jump, 1);
}

void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	UT_ASSERT(exit_callback == NULL);
	exit_callback = function;
	exit_arg = arg;
	registrations++;
}

void
cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	UT_ASSERT(exit_callback == function && exit_arg == arg);
	exit_callback = NULL;
	cancellations++;
}

void
cluster_cold_typed_destroy_v1(ClusterColdTypedV1 **typed)
{
	if (*typed != NULL) {
		UT_ASSERT(*typed == &attempts[0] || *typed == &attempts[1]);
		UT_ASSERT((*typed)->inputs == NULL);
		destructions++;
		*typed = NULL;
	}
}

/* This fixture owns only pre-TX retained inputs. The separate cold-TX
 * fixture executes the original checkpoint register/handoff/revoke bodies. */
void cluster_recovery_checkpoint_revoke_v1(const struct ClusterRecoveryStartV1 *start)
{
	(void)start;
	UT_ASSERT(false);
}

bool cluster_recovery_checkpoint_handoff_v1(const struct ClusterRecoveryStartV1 *start)
{
	(void)start;
	UT_ASSERT(false);
	return false;
}

void pfree(void *pointer)
{
	(void)pointer;
	UT_ASSERT(false);
}

#include "test_cluster_cold_eor_inputs.inc"

static void
reset(void)
{
	UT_ASSERT(cluster_recovery_cold_inputs == NULL && exit_callback == NULL);
	MemSet(attempts, 0, sizeof(attempts));
	registrations = cancellations = destructions = 0;
	fatal_armed = false;
}

UT_TEST(test_original_inputs_survive_replay_and_transfer_read_only)
{
	ClusterColdTypedV1 *input = &attempts[0];
	EndOfWalRecoveryInfo result = {0};
	const ClusterColdTypedV1 *borrowed;

	reset();
	input->xid_next_after_redo = UINT64CONST(0x100000020);
	cluster_recovery_cold_inputs_retain(&input);
	UT_ASSERT(input == NULL);
	UT_ASSERT_EQ(registrations, 1);
	UT_ASSERT_EQ(destructions, 0);
	cluster_recovery_cold_inputs_handoff(&result);
	borrowed = result.cluster_cold_inputs;
	UT_ASSERT(borrowed == &attempts[0]);
	UT_ASSERT_EQ(borrowed->xid_next_after_redo, UINT64CONST(0x100000020));
	UT_ASSERT(borrowed->tt_base == NULL && borrowed->tt_after_redo == NULL);
	UT_ASSERT_EQ(destructions, 0);
	ClusterReleaseColdRecoveryInputs(&result);
	UT_ASSERT(result.cluster_cold_inputs == NULL);
	UT_ASSERT_EQ(destructions, 1);
	UT_ASSERT_EQ(cancellations, 1);
	ClusterReleaseColdRecoveryInputs(&result);
	UT_ASSERT_EQ(destructions, 1);
	UT_ASSERT_EQ(cancellations, 1);
}

UT_TEST(test_exit_before_eor_releases_retained_attempt)
{
	ClusterColdTypedV1 *input = &attempts[0];
	pg_on_exit_callback cleanup;

	reset();
	cluster_recovery_cold_inputs_retain(&input);
	cleanup = exit_callback;
	exit_callback = NULL; /* Native exit pops the registered callback first. */
	cleanup(1, exit_arg);
	UT_ASSERT(cluster_recovery_cold_inputs == NULL);
	UT_ASSERT_EQ(destructions, 1);
	UT_ASSERT_EQ(cancellations, 0);
}

UT_TEST(test_second_attempt_cannot_replace_original_inputs)
{
	ClusterColdTypedV1 *first = &attempts[0];
	ClusterColdTypedV1 *second = &attempts[1];
	EndOfWalRecoveryInfo result = {0};

	reset();
	cluster_recovery_cold_inputs_retain(&first);
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) {
		cluster_recovery_cold_inputs_retain(&second);
		UT_ASSERT(false);
	}
	fatal_armed = false;
	UT_ASSERT(second == &attempts[1]);
	UT_ASSERT_EQ(registrations, 1);
	UT_ASSERT_EQ(destructions, 0);
	cluster_recovery_cold_inputs_handoff(&result);
	UT_ASSERT(result.cluster_cold_inputs == &attempts[0]);
	ClusterReleaseColdRecoveryInputs(&result);
}

UT_TEST(test_live_read_scope_cannot_cross_resource_owner_lifetime)
{
	ClusterColdTypedV1 *input = &attempts[0];

	reset();
	input->inputs = (struct ClusterWalInputsV1 *)&attempts[1];
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) {
		cluster_recovery_cold_inputs_retain(&input);
		UT_ASSERT(false);
	}
	fatal_armed = false;
	UT_ASSERT(input == &attempts[0]);
	UT_ASSERT(cluster_recovery_cold_inputs == NULL);
	UT_ASSERT_EQ(registrations, 0);
	UT_ASSERT_EQ(destructions, 0);
	input->inputs = NULL;
}

UT_TEST(test_wrong_result_cannot_release_another_attempt)
{
	ClusterColdTypedV1 *input = &attempts[0];
	EndOfWalRecoveryInfo result = { 0 };

	reset();
	cluster_recovery_cold_inputs_retain(&input);
	result.cluster_cold_inputs = &attempts[1];
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) {
		ClusterReleaseColdRecoveryInputs(&result);
		UT_ASSERT(false);
	}
	fatal_armed = false;
	UT_ASSERT_EQ(destructions, 0);
	UT_ASSERT_EQ(cancellations, 0);
	cluster_recovery_cold_inputs_handoff(&result);
	ClusterReleaseColdRecoveryInputs(&result);
}

UT_TEST(test_non_cold_recovery_has_no_retained_owner)
{
	EndOfWalRecoveryInfo result = {0};

	reset();
	MemSet(&result, 0x5a, sizeof(result));
	cluster_recovery_cold_inputs_handoff(&result);
	UT_ASSERT(result.cluster_cold_inputs == NULL);
	ClusterReleaseColdRecoveryInputs(&result);
	UT_ASSERT_EQ(registrations, 0);
	UT_ASSERT_EQ(destructions, 0);
	UT_ASSERT_EQ(cancellations, 0);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_original_inputs_survive_replay_and_transfer_read_only);
	UT_RUN(test_exit_before_eor_releases_retained_attempt);
	UT_RUN(test_second_attempt_cannot_replace_original_inputs);
	UT_RUN(test_live_read_scope_cannot_cross_resource_owner_lifetime);
	UT_RUN(test_wrong_result_cannot_release_another_attempt);
	UT_RUN(test_non_cold_recovery_has_no_retained_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
