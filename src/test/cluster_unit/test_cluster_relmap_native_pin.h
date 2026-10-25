/* Test-only original WAL retention pin composition.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_NATIVE_PIN_H
#define TEST_CLUSTER_RELMAP_NATIVE_PIN_H
#include "cluster/cluster_thread_recovery_authority.h"

typedef ClusterControlRootResult (*TestRelmapPinRootCurrent)(const ClusterControlRootReadToken *,
	const ClusterControlRootIdentity *, ClusterControlRootSnapshot *);
typedef ClusterRecoverySerialRevalidateResult (*TestRelmapPinSerialCurrent)(
	ClusterRecoverySerialGuard *);
typedef struct TestRelmapNativePinStats {
	unsigned acquires, checks, releases;
	bool active, bound, poisoned;
	ClusterWalPinResult setup_result;
	int assertion_failed;
} TestRelmapNativePinStats;

extern ClusterWalRetentionPin *test_relmap_native_pin_setup(
	const ClusterWalRetentionPinThreadRequest *request, ClusterRecoverySerialGuard *serial,
	TestRelmapPinRootCurrent root_current, TestRelmapPinSerialCurrent serial_current,
	sigjmp_buf *outer_stack);
extern ClusterWalPinResult test_relmap_native_pin_revalidate(ClusterWalRetentionPin *pin,
	sigjmp_buf *outer_stack);
extern void test_relmap_native_pin_fault(unsigned fault);
extern TestRelmapNativePinStats test_relmap_native_pin_stats(void);
extern TestRelmapNativePinStats test_relmap_native_pin_done(void);

extern bool test_relmap_thread_pin_request(uint16 dead_tid, const ClusterRecoveryDutyKey *duty,
	const ClusterControlRootSnapshot *root_snapshot, const ClusterControlRootReadToken *root_token,
	const ClusterFormationWitnessV1 *formation, const PgracExternalFenceNeedSetV1 *fence_need_set,
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set,
	ClusterWalRetentionInterval *out_interval, ClusterWalRetentionPinThreadRequest *out_request);
#endif
