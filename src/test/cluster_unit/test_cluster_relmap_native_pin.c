/* Original WAL retention pin beside the original cold recovery owner.
 * The existing fixture supplies controlled lower WALR/fence facilities.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_relmap_native_pin_namespace.h"
int d3_pin_existing_main(int argc, char **argv);
#define main d3_pin_existing_main
#include "test_cluster_wal_retention.c"
#undef main
#include "test_cluster_relmap_native_pin.h"

static ClusterWalRetentionPin *relmap_original_pin;
static unsigned relmap_pin_checks;
static ClusterWalPinResult relmap_pin_setup_result;
static TestRelmapPinRootCurrent relmap_pin_root_current;
static TestRelmapPinSerialCurrent relmap_pin_serial_current;

static ClusterControlRootResult
relmap_pin_root_revalidate(const ClusterControlRootReadToken *token,
						   const ClusterControlRootIdentity *identity,
						   ClusterControlRootSnapshot *snapshot)
{
	return relmap_pin_root_current(token, identity, snapshot);
}

static ClusterRecoverySerialRevalidateResult
relmap_pin_serial_revalidate(ClusterRecoverySerialGuard *serial)
{
	return relmap_pin_serial_current(serial);
}

#undef cluster_control_root_revalidate
#define cluster_control_root_revalidate relmap_pin_root_revalidate
#undef cluster_recovery_serial_revalidate
#define cluster_recovery_serial_revalidate relmap_pin_serial_revalidate
#include "../../backend/cluster/cluster_wal_retention.c"
#undef cluster_control_root_revalidate
#undef cluster_recovery_serial_revalidate

TestRelmapNativePinStats
test_relmap_native_pin_stats(void)
{
	TestRelmapNativePinStats out = {
		fake_acquire_call_count, relmap_pin_checks, fake_release_call_count,
		relmap_original_pin != NULL && active_pin == relmap_original_pin,
		relmap_original_pin != NULL && relmap_original_pin->state == CLUSTER_WAL_PIN_STATE_BOUND_ONE,
		relmap_original_pin != NULL && relmap_original_pin->poisoned,
		relmap_pin_setup_result, ut_current_failed
	};
	return out;
}

ClusterWalRetentionPin *
test_relmap_native_pin_setup(const ClusterWalRetentionPinThreadRequest *request,
	ClusterRecoverySerialGuard *serial, TestRelmapPinRootCurrent root_current,
	TestRelmapPinSerialCurrent serial_current, sigjmp_buf *outer_stack)
{
	reset_pin_fakes();
	ut_current_failed = 0;
	relmap_pin_checks = 0;
	UT_ASSERT(relmap_original_pin == NULL && active_pin == NULL);
	UT_ASSERT(root_current != NULL && serial_current != NULL);
	relmap_pin_root_current = root_current;
	relmap_pin_serial_current = serial_current;
	PG_exception_stack = outer_stack;
	cluster_shared_config = true;
	relmap_pin_setup_result = cluster_wal_retention_pin_acquire(request, 1, &relmap_original_pin);
	if (relmap_original_pin != NULL)
		UT_ASSERT_EQ(cluster_wal_retention_pin_bind_one(relmap_original_pin, serial),
					 CLUSTER_WAL_PIN_OK);
	return relmap_original_pin;
}

ClusterWalPinResult
test_relmap_native_pin_revalidate(ClusterWalRetentionPin *pin, sigjmp_buf *outer_stack)
{
	relmap_pin_checks++;
	PG_exception_stack = outer_stack;
	return cluster_wal_retention_pin_revalidate(pin);
}

void
test_relmap_native_pin_fault(unsigned fault)
{
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)(fault == 1 ? 2 : 1);
	MyProcPid = fault == 2 ? 43 : 42;
	fake_control_owner_current = fault != 3;
}

TestRelmapNativePinStats
test_relmap_native_pin_done(void)
{
	TestRelmapNativePinStats result;
	test_relmap_native_pin_fault(0);
	if (relmap_original_pin != NULL)
		UT_ASSERT_EQ(cluster_wal_retention_pin_release(&relmap_original_pin),
					 CLUSTER_WALR_RELEASE_CONFIRMED);
	result = test_relmap_native_pin_stats();
	UT_ASSERT(!result.active);
	result.assertion_failed = ut_current_failed;
	cluster_shared_config = false;
	PG_exception_stack = NULL;
	relmap_pin_root_current = NULL;
	relmap_pin_serial_current = NULL;
	return result;
}
