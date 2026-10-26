/*-------------------------------------------------------------------------
 *
 * cluster_page_stable_base_private.h
 *     Private stable-base proof layout shared by purpose-specific consumers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/backend/cluster/cluster_page_stable_base_private.h
 *
 * NOTES
 *     A proof owns only its private snapshots. Original scope, IR, WALR and
 *     PAGE owners retain their existing lifetimes and release ordering.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_STABLE_BASE_PRIVATE_H
#define CLUSTER_PAGE_STABLE_BASE_PRIVATE_H

#include "cluster/cluster_page_stable_base.h"

struct ClusterRecoveryCtrcSubjectAuthorityV1;
struct ClusterRecoveryCtrcReplaySetSourceV1;
struct RfPageStableCtrcStorageV1;

#define RF_PAGE_STABLE_PROOF_MAGIC UINT32_C(0x52505350)

struct RfPageStableBaseProofV1 {
	uint32 magic;
	uint32 participant_count;
	RfPageIdentityV1 page_identity;
	RfPageVersionV1 expected_result;
	RfPageStableSelectionV1 selection;
	const ClusterRecoveryDutyKey *duties;
	const ClusterControlRootReadToken *root_tokens;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *fence_need_set;
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set;
	ClusterWalRetentionPin *retention_pin;
	const RfPagePinnedSourceV1 *source;
	const RfContributorVectorV1 *contributors;
	const struct ClusterThreadRecoveryAuthorityV1 *source_authorities;
	struct ClusterThreadRecoveryAuthorityV1 *source_owners;
	/* Native CTRC purpose; private copies own no original scope or grants. */
	const struct ClusterRecoveryCtrcSubjectAuthorityV1 *ctrc_authorities;
	struct ClusterRecoveryCtrcSubjectAuthorityV1 *ctrc_owners;
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *ctrc_replay_authorities;
	struct ClusterRecoveryCtrcReplaySetSourceV1 *ctrc_replay_owners;
	struct RfPageStableCtrcStorageV1 *ctrc_storage;
	uint32 *ctrc_scratch;
};

#endif /* CLUSTER_PAGE_STABLE_BASE_PRIVATE_H */
