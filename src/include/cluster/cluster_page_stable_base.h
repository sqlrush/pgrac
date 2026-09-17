/*-------------------------------------------------------------------------
 *
 * cluster_page_stable_base.h
 *    STOP-06 exact PageVersion graph and stable-base selector.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_STABLE_BASE_H
#define CLUSTER_PAGE_STABLE_BASE_H

#include "access/xlogdefs.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_control_root.h"
#include "storage/relfilelocator.h"

#define CLUSTER_PAGE_STABLE_BASE_INTERFACE_V1 1
#define RF_PAGE_STABLE_MAX_PARTICIPANTS 128
#define RF_PAGE_STABLE_MAX_EDGES 65536
#define RF_PAGE_STABLE_MAX_COMPONENTS 33

typedef struct RfPageIdentityV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	RelFileLocator locator;
	uint32 forknum;
	BlockNumber blockno;
	uint32 reserved_zero;
} RfPageIdentityV1;

StaticAssertDecl(sizeof(RfPageIdentityV1) == 48, "RfPageIdentityV1 ABI drift");
StaticAssertDecl(offsetof(RfPageIdentityV1, storage_uuid) == 8,
				 "RfPageIdentityV1 storage UUID offset drift");
StaticAssertDecl(offsetof(RfPageIdentityV1, locator) == 24,
				 "RfPageIdentityV1 locator offset drift");
StaticAssertDecl(offsetof(RfPageIdentityV1, forknum) == 36, "RfPageIdentityV1 fork offset drift");
StaticAssertDecl(offsetof(RfPageIdentityV1, blockno) == 40, "RfPageIdentityV1 block offset drift");

#define RF_CONTRIBUTOR_CUT_COMPLETE UINT16_C(0x0001)
#define RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY UINT16_C(0x0002)
#define RF_CONTRIBUTOR_CUT_KNOWN_MASK UINT16_C(0x0003)

typedef struct RfContributorStreamCutV1 {
	uint16 failed_thread;
	uint16 flags;
	TimeLineID timeline_id;
	XLogRecPtr scan_begin_inclusive;
	XLogRecPtr scan_end_exclusive;
	uint32 contributor_count;
	uint32 component_count;
} RfContributorStreamCutV1;

StaticAssertDecl(sizeof(RfContributorStreamCutV1) == 32, "RfContributorStreamCutV1 ABI drift");
StaticAssertDecl(offsetof(RfContributorStreamCutV1, timeline_id) == 4,
				 "RfContributorStreamCutV1 timeline offset drift");
StaticAssertDecl(offsetof(RfContributorStreamCutV1, scan_begin_inclusive) == 8,
				 "RfContributorStreamCutV1 begin offset drift");
StaticAssertDecl(offsetof(RfContributorStreamCutV1, contributor_count) == 24,
				 "RfContributorStreamCutV1 count offset drift");

/* Exact immutable identity of one decoded foreign WAL record. */
typedef struct RfPageReplayRecordIdentityV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint16 origin_thread;
	uint16 reserved_zero;
	TimeLineID timeline_id;
	XLogRecPtr read_rec_ptr;
	XLogRecPtr end_rec_ptr;
	uint32 record_crc;
	uint8 rmid;
	uint8 info;
	uint16 reserved_zero2;
} RfPageReplayRecordIdentityV1;

StaticAssertDecl(sizeof(RfPageReplayRecordIdentityV1) == 56,
				 "RfPageReplayRecordIdentityV1 ABI drift");
StaticAssertDecl(offsetof(RfPageReplayRecordIdentityV1, timeline_id) == 28,
				 "RfPageReplayRecordIdentityV1 timeline offset drift");
StaticAssertDecl(offsetof(RfPageReplayRecordIdentityV1, read_rec_ptr) == 32,
				 "RfPageReplayRecordIdentityV1 read LSN offset drift");
StaticAssertDecl(offsetof(RfPageReplayRecordIdentityV1, record_crc) == 48,
				 "RfPageReplayRecordIdentityV1 CRC offset drift");

typedef enum RfPageProofDetailV1 {
	RF_PAGE_PROOF_DETAIL_OK = 0,
	RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT = 1,
	RF_PAGE_PROOF_DETAIL_ROOT_STALE = 2,
	RF_PAGE_PROOF_DETAIL_DUTY_STALE = 3,
	RF_PAGE_PROOF_DETAIL_FENCE_STALE = 4,
	RF_PAGE_PROOF_DETAIL_RETENTION_STALE = 5,
	RF_PAGE_PROOF_DETAIL_SOURCE_GAP = 6,
	RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING = 7,
	RF_PAGE_PROOF_DETAIL_EDGE_GAP = 8,
	RF_PAGE_PROOF_DETAIL_EDGE_BRANCH = 9,
	RF_PAGE_PROOF_DETAIL_EDGE_CYCLE = 10,
	RF_PAGE_PROOF_DETAIL_TERMINAL_AMBIGUOUS = 11,
	RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING = 12,
	RF_PAGE_PROOF_DETAIL_ANCHOR_AMBIGUOUS = 13,
	RF_PAGE_PROOF_DETAIL_IMAGE_DECODE_FAILED = 14,
	RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED = 15,
	RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED = 16,
	RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN = 17,
	RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH = 18,
	RF_PAGE_PROOF_DETAIL_INCARNATION_MISMATCH = 19,
	RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH = 20,
	RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE = 21,
	RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE = 22,
	RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION = 23,
	RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED = 24,
	RF_PAGE_PROOF_DETAIL_CAPACITY = 25,
	RF_PAGE_PROOF_DETAIL_WOULD_BLOCK = 26,
	RF_PAGE_PROOF_DETAIL_CANCELLED = 27,
	RF_PAGE_PROOF_DETAIL_OOM = 28,
	RF_PAGE_PROOF_DETAIL_INTERNAL = 29
} RfPageProofDetailV1;

/* Immutable decoder output projected onto one ordinary target page. */
typedef struct RfPageStableEdgeInputV1 {
	RfPageIdentityV1 page_identity;
	RfPageVersionEdgeEntryV1 edge;
	uint64 result_token;
	RfPageReplayRecordIdentityV1 record_identity;
	uint16 participant_index;
	uint16 component_count;
	uint8 anchor_digest[32];
	bool record_complete;
	bool opcode_supported;
	bool side_complete;
	bool image_integrity_ok;
} RfPageStableEdgeInputV1;

/* Caller-owned RF-SIDE view.  It owns neither WAL nor authority. */
typedef struct RfContributorVectorV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint32 participant_count;
	uint32 edge_count;
	const RfContributorStreamCutV1 *cuts;
	const RfPageStableEdgeInputV1 *edges;
} RfContributorVectorV1;

typedef struct RfPagePinnedSourceV1 {
	RfPageIdentityV1 page_identity;
	RfPageVersionV1 source_version;
	uint64 binding_cookie;
	uint64 current_binding_cookie;
	bool identity_verified;
	bool integrity_verified;
} RfPagePinnedSourceV1;

typedef struct RfPageStableGraphRequestV1 {
	RfPageIdentityV1 page_identity;
	RfPageVersionV1 expected_result;
	const RfContributorVectorV1 *contributors;
	const RfPagePinnedSourceV1 *source;
	uint32 participant_count;
	uint32 flags;
	uint64 retention_binding_cookie;
	uint64 current_retention_binding_cookie;
	bool root_current;
	bool duty_current;
	bool fence_current;
	bool retention_current;
} RfPageStableGraphRequestV1;

typedef struct RfPageStableSelectionV1 {
	RfPageVersionV1 terminal_version;
	uint32 anchor_edge_index;
	uint32 chain_length;
	bool result_already_present;
} RfPageStableSelectionV1;

typedef struct PgracExternalFenceAdmissionSetV1 PgracExternalFenceAdmissionSetV1;
typedef struct PgracExternalFenceNeedSetV1 PgracExternalFenceNeedSetV1;
typedef struct ClusterFormationWitnessV1 ClusterFormationWitnessV1;
typedef struct ClusterWalRetentionPin ClusterWalRetentionPin;
typedef struct RfPageStableBaseProofV1 RfPageStableBaseProofV1;

typedef struct RfPageStableBaseProofRequestV1 {
	const RfPageStableGraphRequestV1 *graph;
	const ClusterRecoveryDutyKey *duties;
	const ClusterControlRootReadToken *root_tokens;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *fence_need_set;
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set;
	ClusterWalRetentionPin *retention_pin;
	uint32 flags;
} RfPageStableBaseProofRequestV1;

extern bool rf_page_identity_valid_v1(const RfPageIdentityV1 *identity);
extern bool rf_page_identity_equal_v1(const RfPageIdentityV1 *left, const RfPageIdentityV1 *right);
extern bool rf_page_version_present_v1(const RfPageVersionV1 *version);

/*
 * Select the unique exact chain and nearest valid anchor.  chain_indices is
 * caller-owned workspace and receives anchor-to-terminal edge indices only on
 * success.  Every failure leaves both output objects untouched.
 */
extern RfPageProofDetailV1 rf_page_stable_base_select_v1(const RfPageStableGraphRequestV1 *request,
														 uint32 *chain_indices,
														 uint32 chain_capacity,
														 RfPageStableSelectionV1 *selection);
extern RfPageProofDetailV1
rf_page_stable_base_proof_build_wait_v1(const RfPageStableBaseProofRequestV1 *request,
										uint32 *chain_indices, uint32 chain_capacity,
										int timeout_ms, RfPageStableBaseProofV1 **out_proof);
extern RfPageProofDetailV1
rf_page_stable_base_proof_build_bound_v1(const RfPageStableBaseProofRequestV1 *request,
										 uint32 *chain_indices, uint32 chain_capacity,
										 RfPageStableBaseProofV1 **out_proof);
extern bool rf_page_stable_base_proof_matches_v1(
	const RfPageStableBaseProofV1 *proof, const RfPageIdentityV1 *page_identity,
	const RfPageVersionV1 *expected_result, const ClusterRecoveryDutyKey *duties,
	const ClusterControlRootReadToken *root_tokens, const ClusterFormationWitnessV1 *formation,
	const PgracExternalFenceNeedSetV1 *fence_need_set,
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set,
	ClusterWalRetentionPin *retention_pin, const RfPagePinnedSourceV1 *source,
	const RfContributorVectorV1 *contributors, uint32 participant_count);
extern void rf_page_stable_base_proof_destroy_v1(RfPageStableBaseProofV1 **proof);

#ifdef USE_CLUSTER_UNIT

typedef enum RfPageInstallTargetStateV1 {
	RF_PAGE_INSTALL_TARGET_EXPECTED = 1,
	RF_PAGE_INSTALL_TARGET_RESULT = 2,
	RF_PAGE_INSTALL_TARGET_TORN = 3,
	RF_PAGE_INSTALL_TARGET_UNRELATED = 4
} RfPageInstallTargetStateV1;

typedef struct RfPageInstallComponentV1 {
	RfPageIdentityV1 page_identity;
	RfPageVersionV1 expected_before;
	RfPageVersionV1 expected_result;
	const char *canonical_page;
	uint8 target_state;
	bool route_preflight_ok;
	bool side_preflight_ok;
	bool scratch_ready;
	bool identity_authority_ok;
	bool canonical_layout_ok;
	bool checksums_enabled;
} RfPageInstallComponentV1;

typedef struct RfPageInstallOpsV1 {
	void *arg;
	bool (*canonicalize)(void *arg, uint32 index, bool checksums_enabled, char page[BLCKSZ]);
	bool (*promote)(void *arg);
	bool (*write)(void *arg, uint32 index, const char page[BLCKSZ]);
	bool (*sync)(void *arg, uint32 index);
	bool (*postread)(void *arg, uint32 index, char page[BLCKSZ]);
	bool (*publish)(void *arg);
	bool (*release)(void *arg);
} RfPageInstallOpsV1;

typedef struct RfPageInstallRequestV1 {
	const RfPageInstallComponentV1 *components;
	uint32 component_count;
	char *prepared_pages;
	Size prepared_capacity;
	const RfPageInstallOpsV1 *ops;
	bool global_preflight_ok;
} RfPageInstallRequestV1;

typedef struct RfPageInstallProofV1 {
	uint32 component_count;
	bool durability_complete;
	bool postread_complete;
	bool proof_published;
	bool authority_released;
} RfPageInstallProofV1;

extern RfPageProofDetailV1 rf_page_stable_install_test_v1(const RfPageInstallRequestV1 *request,
														  RfPageInstallProofV1 *proof);

#endif /* USE_CLUSTER_UNIT */

#endif /* CLUSTER_PAGE_STABLE_BASE_H */
