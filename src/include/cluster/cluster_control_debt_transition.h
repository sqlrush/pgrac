/*-------------------------------------------------------------------------
 * cluster_control_debt_transition.h
 *     Borrowed original-owner observations of a protective ROOT transition.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/include/cluster/cluster_control_debt_transition.h
 * NOTES
 *     Local observations only; no wire format or publication authority.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONTROL_DEBT_TRANSITION_H
#define CLUSTER_CONTROL_DEBT_TRANSITION_H
#include "cluster/cluster_control_file_identity.h"
struct ClusterRecoveryStartV1;
typedef struct ClusterControlDebtTransitionV2 ClusterControlDebtTransitionV2;
typedef struct ClusterControlDebtBasisViewV2 {
	uint64 episode, prepare;
	const uint8 *basis;
	Size basis_bytes;
	uint8 basis_reference[128];
} ClusterControlDebtBasisViewV2;
typedef struct ClusterControlDebtTransitionViewV2 {
	const struct ClusterRecoveryStartV1 *start;
	const ClusterControlRelmapSelectionV2 *selection;
	uint64 cf_sequence;
	const uint8 *before_root, *after_root;
	Size root_pair_bytes;
	const uint8 *basis;
	Size basis_bytes;
	uint8 basis_reference[128];
	const ClusterControlDebtBasisViewV2 *batches;
	uint64 batch_count;
	const uint8 *registry, *pages;
	Size registry_bytes, page_bytes;
	uint8 prepared_reference[128];
} ClusterControlDebtTransitionViewV2;
/* The original control owner alone constructs and destroys the opaque receipt.
 * It is address-, Start-, selection-, ResourceOwner-, process- and CF-bound.
 * Borrowed bytes remain valid only until that owner destroys the receipt;
 * never retain a view after destruction or treat a copied view as authority.
 * This getter performs no allocation, I/O, lock acquisition or publication.
 * False preserves output, including aliases of receipt-owned storage and the original
 * Start/selection storage.
 * Consumers must conjoin the original replay-input authorization and complete
 * physical ROOT/readback checks. Neither candidate bytes nor this view prove
 * PREPARED durability, OPEN, completion, protection transfer or retirement.
 * No live prepare producer is exposed until those consumers are connected. */
extern bool cluster_control_root_debt_transition_get_v2(
	const ClusterControlDebtTransitionV2 *, ClusterControlDebtTransitionViewV2 *out);
/* Independently reread the exact physical after-pair and every immutable debt
 * dependency through the original owner's borrowed global-directory FD.
 * The same CF-X, Start/selection, ResourceOwner and process must remain held.
 * No LWLock or critical section may enclose this I/O. The caller retains the
 * original storage/cohort custody; the FD is never adopted or closed here.
 * True is a complete byte observation only: it neither updates any token nor
 * proves replay authorization, protection transfer, completion or OPEN.
 * No create, sync, repair, fallback, ROOT publication or success flag is used.
 * False/ERROR preserves the original receipt, borrowed FD and protection debt.
 */
extern bool cluster_control_root_debt_transition_readback_v2(
	const ClusterControlDebtTransitionV2 *, int global_directory, Size byte_budget);
#endif
