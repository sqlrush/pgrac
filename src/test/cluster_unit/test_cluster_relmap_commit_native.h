/* Controlled native leaf beside the original runtime/producer fixtures.
 * This supplies no native durability, source or lifetime qualification.
 * Static names stay within each existing test translation unit.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef TEST_CLUSTER_RELMAP_COMMIT_NATIVE_H
#define TEST_CLUSTER_RELMAP_COMMIT_NATIVE_H
#include "access/clog.h"
#include "cluster/cluster_relmap_recovery.h"

static unsigned test_relmap_native_commit_calls;
static bool (*test_relmap_native_commit_hook)(const ClusterRelmapOwnerSampleV2 *,
	ClusterRelmapRecoveryNativeTerminal *);

static inline bool
test_relmap_native_commit_default(const ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapRecoveryNativeTerminal *out)
{
	memset(out, 0, sizeof(*out));
	out->bounds.system_identifier = sample->writer.ref.claim.identity.system_identifier;
	out->bounds.database_incarnation = sample->writer.ref.claim.database_incarnation;
	out->bounds.origin = sample->owner.node;
	out->bounds.membership_epoch = sample->owner.membership_epoch;
	out->bounds.admitted_incarnation = sample->owner.admitted_incarnation;
	out->bounds.boot_incarnation = sample->owner.boot_incarnation;
	out->bounds.native_floor = sample->owner.top_full_xid - 16;
	out->bounds.last_issued = sample->owner.top_full_xid + 16;
	out->top_full_xid = sample->owner.top_full_xid;
	out->status = TRANSACTION_STATUS_COMMITTED;
	return true;
}

static inline bool
test_relmap_native_commit_sync(const ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapRecoveryNativeTerminal *out)
{
	test_relmap_native_commit_calls++;
	return test_relmap_native_commit_hook != NULL
		? test_relmap_native_commit_hook(sample, out)
		: test_relmap_native_commit_default(sample, out);
}
#endif
