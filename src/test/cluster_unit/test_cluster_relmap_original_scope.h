/* Test-only declarations for original cold scope composition.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_ORIGINAL_SCOPE_H
#define TEST_CLUSTER_RELMAP_ORIGINAL_SCOPE_H
#include "cluster/cluster_undo_recovery.h"
#include "cluster/storage/cluster_shared_fs.h"

extern bool test_relmap_scope_source_matches(const ClusterWalSourceRef *source,
											 const ClusterControlRootSnapshot *checkpoint,
											 XLogRecPtr redo);
extern uint64 test_relmap_scope_id(const ClusterWalSourceRef *source,
								   const ClusterControlRootSnapshot *checkpoint, XLogRecPtr redo);
extern bool test_relmap_scope_single_source(const ClusterWalSourceRef *source,
											const ClusterControlRootSnapshot *checkpoint,
											XLogRecPtr redo, uint64 serial);
extern bool test_relmap_scope_checkpoint_visit(int origin,
											   ClusterUndoRecoveryCheckpointVisitorV1 visitor,
											   void *arg);
extern bool test_relmap_scope_terminal_matches(const ClusterWalSourceRef *source,
											   const ClusterControlRootSnapshot *checkpoint,
											   XLogRecPtr redo, uint64 serial,
											   const XLogReaderState *reader);
extern uint32 test_relmap_scope_terminal_count(const ClusterWalSourceRef *source,
											   const ClusterControlRootSnapshot *checkpoint,
											   XLogRecPtr redo, uint64 serial);
extern ClusterThreadRecoveryAuthorityResultV1
test_relmap_scope_authority(const ClusterThreadRecoveryAuthorityV1 *authority);
extern bool test_relmap_scope_storage(ClusterProtectedSetIdentityV1 *out);
extern void test_relmap_scope_cache_reset(void);
/* Controlled lower facilities, never external-fence/retention certification. */
extern ClusterRecoverySerialRevalidateResult
test_relmap_authority_serial(ClusterRecoverySerialGuard *guard);
extern ClusterFormationWitnessResult
test_relmap_authority_formation(const ClusterFormationWitnessV1 *formation);
extern bool test_relmap_authority_needs(const PgracExternalFenceNeedSetV1 *needs,
	const ClusterFormationWitnessV1 *formation, PgracExternalFenceDenyReason *reason);
extern bool test_relmap_authority_admissions(const PgracExternalFenceAdmissionSetV1 *admissions,
	const PgracExternalFenceNeedSetV1 *needs, const ClusterFormationWitnessV1 *formation,
	PgracExternalFenceDenyReason *reason);
extern ClusterWalPinResult test_relmap_authority_pin(ClusterWalRetentionPin *pin);
#endif
