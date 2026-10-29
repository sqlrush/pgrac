/* Real scope types, original role/current/release guards and new storage
 * bodies. All non-ONLINE/cold/legacy cleanup continuations abort on entry;
 * this fixture cannot claim successful admission or resource cleanup there. */
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/cluster_wal_cut.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster_control_root_private.h"
#include "test_cluster_wal_inputs_job_scope_types.inc"

/* External enabled GUC input; it confers no ROOT/input proof. */
bool cluster_enabled = true;

bool
RecoveryInProgress(void)
{
	abort();
}
bool
ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work)
{
	(void)start; (void)work; abort();
}
bool
ClusterColdRecoveryTxCheckpointOwnerV1(const struct ClusterRecoveryStartV1 *start,
	ClusterRecoveryCheckpointOwnerV1 *owner)
{
	(void)start; (void)owner; abort();
}
const ClusterColdTypedV1 *
ClusterColdRecoveryTxInputs(const struct ClusterRecoveryStartV1 *start)
{
	(void)start; abort();
}
bool
ClusterColdRecoveryTxSubjectCurrent(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdTopTxCensusEntryV1 *subject, uint64 *generation, uint64 *serial)
{
	(void)start; (void)subject; (void)generation; (void)serial; abort();
}
static void inputs_tx_serial_release(ClusterWalInputsV1 *scope) { (void)scope; abort(); }
static void inputs_namespace_close(ClusterWalInputsV1 *scope) { (void)scope; abort(); }
void
cluster_wal_cut_release_v1(ClusterWalCutV1 **cut)
{
	(void)cut; abort();
}

/* Process identity is an explicit fixture input, consistently with MyProc. */
static bool scope_expect_error;
static bool
scope_fixture_error(int level, const char *domain)
{
	if (scope_expect_error && level == ERROR)
		pg_re_throw();
	return errstart_cold(level, domain);
}
#define getpid() MyProcPid
#define errstart_cold scope_fixture_error
#include "test_cluster_wal_inputs_job_scope_bodies.inc"
#undef errstart_cold
#undef getpid
