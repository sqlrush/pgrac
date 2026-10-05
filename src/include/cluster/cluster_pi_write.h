/*-------------------------------------------------------------------------
 * cluster_pi_write.h
 *    Exact master/current-holder DATA completion.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PI_WRITE_H
#define CLUSTER_PI_WRITE_H

#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_page_data.h"
#include "cluster/cluster_page_wal.h"

typedef struct ClusterWalInputsV1 ClusterWalInputsV1;

#ifdef USE_PGRAC_CLUSTER
/* Process-local projection, never a grant or a persistent record. */
typedef struct ClusterPcmPiWriteCutV1 {
	ResourceXMasterSnapshot holder;
	uint64 binding_generation;
	uint64 transition_count;
	uint64 master_generation;
	int32 master_node;
	uint32 pi_holders_bitmap;
} ClusterPcmPiWriteCutV1;

StaticAssertDecl(sizeof(ClusterPcmPiWriteCutV1) == 128,
				 "PI write cut must remain a bounded value projection");

/* No current X exists. Capture residency and any not-yet-granted storage
 * waiter independently of the current-holder write certificate. */
typedef struct ClusterPcmPiStorageCutV1 {
	PcmAuthoritySnapshot authority;
	ResourceXMasterSnapshot waiting;
	BufferTag resource;
	uint32 pi_holders_bitmap;
	uint64 binding_generation;
	uint64 resource_formation;
	uint64 authority_generation;
	uint64 master_generation;
	uint64 master_session_incarnation;
	int32 master_node;
	uint32 reserved;
} ClusterPcmPiStorageCutV1;

StaticAssertDecl(sizeof(ClusterPcmPiStorageCutV1) == 232,
				 "PI storage cut must remain a bounded transient projection");

static inline bool
cluster_pcm_pi_storage_cut_valid_v1(const ClusterPcmPiStorageCutV1 *cut)
{
	static const ResourceXMasterSnapshot no_waiter = { 0 };
	const PcmAuthoritySnapshot *a;
	if (cut == NULL)
		return false;
	a = &cut->authority;
	return cut->reserved == 0 && cut->binding_generation != 0
		   && cut->binding_generation != UINT64_MAX && cut->resource_formation != 0
		   && cut->resource_formation != UINT64_MAX && cut->authority_generation != 0
		   && cut->authority_generation != UINT64_MAX && cut->master_generation != 0
		   && cut->master_generation != UINT64_MAX && cut->master_session_incarnation != 0
		   && cut->master_session_incarnation != UINT64_MAX && cut->master_node >= 0
		   && cut->master_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		   && (cut->resource.forkNum == MAIN_FORKNUM
			   || cut->resource.forkNum == VISIBILITYMAP_FORKNUM
			   || (cut->resource.forkNum == SPACE_FORKNUM && cut->resource.blockNum <= 1))
		   && a->reserved[0] == 0 && a->reserved[1] == 0 && a->transition_count != 0
		   && a->transition_count != UINT64_MAX && a->x_holder_node == -1
		   && ((a->state == PCM_STATE_N && a->s_holders_bitmap == 0
				&& a->master_holder.node_id == UINT32_MAX)
			   || (a->state == PCM_STATE_S && a->master_holder.node_id < 32
				   && (a->s_holders_bitmap & ((uint32)1u << a->master_holder.node_id)) != 0))
		   && ((a->pending_x_requester_node == -1 && a->pending_x_since_lsn == 0)
			   || (a->pending_x_requester_node >= 0 && a->pending_x_requester_node < 32
				   && a->pending_x_since_lsn != 0))
		   && (memcmp(&cut->waiting, &no_waiter, sizeof(no_waiter)) == 0
			   || (a->state == PCM_STATE_N && cut->waiting.phase == RESOURCE_X_MASTER_WAIT_PROOF
				   && cut->waiting.resource_formation == cut->resource_formation
				   && cut->waiting.master_session_incarnation == cut->master_session_incarnation
				   && BufferTagsEqual(&cut->waiting.assertion.resource, &cut->resource)));
}

/* Master-local, read-only. An X grant or any exact cut change invalidates
 * the observation. A WAIT_PROOF head may coexist with N + unpaid PI. */
extern bool cluster_pcm_lock_pi_storage_snapshot_v1(BufferTag tag, ClusterPcmPiStorageCutV1 *out);
extern bool cluster_pcm_lock_pi_storage_matches_v1(const ClusterPcmPiStorageCutV1 *cut);

/* Read/fsync/read storage under the original SPACE0 identity owner. The
 * requested version must be terminal in the sealed full-source plan.
 * No DATA write or X acquisition. Output/ERROR ownership matches DATA. */
extern bool cluster_bufmgr_observe_pi_storage_v1(const ClusterPageDataTargetV1 *target,
												 const ClusterPcmPiStorageCutV1 *cut,
												 const RfPageOnlinePlanV1 *plan,
												 const ClusterWalSourceRef *sources,
												 uint32 source_count,
												 ClusterPageDataReceiptV1 **out);
/* Typed SPACE terminal selected from the complete retained input. SPACE0
 * is its own identity: exact master N/S cut plus typed read/fsync/read needs
 * no resident current image. SPACE1 retains the original SPACE0 identity-S.
 * Neither path writes DATA or acquires X. Plan lifetime follows bind_plan. */
extern bool cluster_bufmgr_observe_pi_space_storage_v1(const ClusterSpaceIdentityKey *key,
	const ClusterPcmPiStorageCutV1 *cut, const struct ClusterThreadRecoveryFabricPlanV1 *plan,
	const ClusterWalSourceRef *sources, uint32 source_count, ClusterPageDataReceiptV1 **out);
extern bool cluster_page_data_pi_storage_proof_v1(const ClusterPageDataReceiptV1 *receipt,
												  const RfPageOnlinePlanV1 *plan,
												  const ClusterWalSourceRef *sources,
												  uint32 source_count,
												  ClusterPcmPiStorageCutV1 *out);

typedef enum ClusterPiPhysicalResultV1 {
	CLUSTER_PI_PHYSICAL_RETRY = 0,
	CLUSTER_PI_PHYSICAL_ABSENT,
	CLUSTER_PI_PHYSICAL_REPLACED,
	CLUSTER_PI_PHYSICAL_DISCARDED
} ClusterPiPhysicalResultV1;

/* Local physical consumer only. Drops a strictly unpinned, unfenced PI
 * iff its exact original WAL/version is an ancestor of actual DATA in the
 * same sealed input. Current/S and later PI are never discarded. This result
 * is neither a master-clear grant nor proof that another boot has retired;
 * the background owner must qualify every node/session and recheck its cut. */
extern ClusterPiPhysicalResultV1
cluster_bufmgr_discard_pi_at_data_v1(const ClusterPageDataReceiptV1 *receipt,
									 const RfPageOnlinePlanV1 *plan,
									 const ClusterWalSourceRef *sources, uint32 source_count);

typedef struct ClusterPiPhysicalAckV1 ClusterPiPhysicalAckV1;

typedef struct ClusterPageStructuralReceiptV2 ClusterPageStructuralReceiptV2;
typedef struct ClusterPiStructuralAckV2 ClusterPiStructuralAckV2;
struct ClusterPiWritebackFactV2;
/* Original master only: join an already handed-off native KO result to this
 * exact MAIN/VM target and its whole old-incarnation PAGE chain in a sealed
 * fabric. The caller retains that immutable plan until receipt release.
 * No buffer/storage I/O or PI retirement occurs; actual consumption still
 * requires the original input pins, physical acknowledgements and exact CAS. */
extern bool cluster_page_structural_from_ko_v2(uint32 slot, uint64 serial,
	const RfPageIdentityV1 *page, const struct ClusterThreadRecoveryFabricPlanV1 *plan,
	ClusterPageStructuralReceiptV2 **out);
/* Process/ResourceOwner-local. Each read rechecks the original shared owner
 * and master cut. Exactly one output cut is populated; refusal preserves
 * both. Export is only for a real original peer, never raw flags as proof. */
extern bool cluster_page_structural_pi_proof_v2(const ClusterPageStructuralReceiptV2 *receipt,
	ClusterPcmPiWriteCutV1 *write_cut, ClusterPcmPiStorageCutV1 *storage_cut);
extern bool cluster_page_structural_pi_fact_v2(const ClusterPageStructuralReceiptV2 *receipt,
	int32 peer, struct ClusterPiWritebackFactV2 *out);
extern void cluster_page_structural_receipt_free_v2(ClusterPageStructuralReceiptV2 **receipt);

/* Local physical consumption of the original committed structural result.
 * The caller retains its sealed plan/input scope. Strictly unpinned old PI
 * only; ABSENT/REPLACED require the same complete old ancestry. This is not
 * a logical retirement, master-clear grant, remote ACK or WAL reuse proof. */
extern ClusterPiPhysicalResultV1
cluster_bufmgr_discard_pi_at_structure_v2(const ClusterPageStructuralReceiptV2 *receipt);

/* Local departed-writer responsibility. Binding identity and revision fence
 * an exact snapshot across DATA/physical I/O; neither counter orders pages.
 * Empty is a qualified directory observation, not proof of a retired boot. */
typedef struct ClusterPcmLocalPiSnapshotV1 {
	BufferTag resource;
	uint64 binding_generation, revision;
	ClusterPageWalBindingV1 first, last;
} ClusterPcmLocalPiSnapshotV1;

/* Original handoff owner only, before publishing its source completion.
 * This can add responsibility, never grant or retire it. Failed retain
 * leaves the previous responsibility intact and the handoff must retry. */
extern bool cluster_pcm_local_pi_record_v1(BufferTag tag, const ClusterPageWalBindingV1 *binding);
/* D S09 R-A22: the producer also hands over the first own record since the
 * page was clean (NULL = binding); the responsibility keeps the earliest. */
extern bool cluster_pcm_local_pi_record_first_v1(BufferTag tag,
												 const ClusterPageWalBindingV1 *first,
												 const ClusterPageWalBindingV1 *binding);
extern bool cluster_pcm_local_pi_snapshot_v1(BufferTag tag, ClusterPcmLocalPiSnapshotV1 *out);

/* Local PI responsibilities as seen by one WAL source. A responsibility is
 * bounded when its first binding is the source's record; unbounded when
 * only its latest binding is (an earlier own record may lie in between). */
typedef struct ClusterPcmLocalPiFloorV1 {
	XLogRecPtr floor; /* least bounded first record_start, or Invalid */
	uint64 bounded;
	uint64 unbounded;
	uint64 foreign;	 /* neither binding is the source's */
	uint64 examined; /* live directory entries examined */
	uint64 pending;	 /* PENDING holder-pair first records of the source (R-A22) */
} ClusterPcmLocalPiFloorV1;

/* Read-only scan of the local directory, one entry lock at a time; takes no
 * pin. PENDING holder-pair first records count as bounded (and pending).
 * Counts every responsibility unretired when the call starts and still
 * unretired when its entry is examined; one first recorded after the call
 * starts may be missed and must be bounded by the caller. False (output
 * zeroed) when the directory is not ready or an entry is inconsistent. */
extern bool cluster_pcm_local_pi_floor_v1(const ClusterWalSourceRef *source,
										  ClusterPcmLocalPiFloorV1 *out);
extern bool cluster_page_data_covers_local_pi_v1(const ClusterPageDataReceiptV1 *receipt,
												 const RfPageOnlinePlanV1 *plan,
												 const ClusterWalSourceRef *sources,
												 uint32 source_count,
												 const ClusterPcmLocalPiSnapshotV1 *local);
extern bool cluster_pcm_local_pi_retire_v1(const ClusterPcmLocalPiSnapshotV1 *local,
										   const ClusterPageDataReceiptV1 *receipt,
										   const RfPageOnlinePlanV1 *plan,
										   const ClusterWalSourceRef *sources, uint32 source_count,
										   const ClusterPiPhysicalAckV1 *ack);

extern bool cluster_page_structural_covers_local_pi_v2(const ClusterPageStructuralReceiptV2 *receipt,
	const ClusterPcmLocalPiSnapshotV1 *local);
extern bool cluster_pcm_local_pi_retire_structural_v2(const ClusterPcmLocalPiSnapshotV1 *local,
	const ClusterPageStructuralReceiptV2 *receipt, const ClusterPiStructuralAckV2 *ack);
/* Exact local boot only, under the original complete retained input scope.
 * The caller keeps both inputs and receipt/plan alive through ACK consumption.
 * Every logical endpoint must be an ancestor, followed by physical disposal
 * and the original local-entry revision CAS. No remote or master retirement. */
extern bool cluster_bufmgr_ack_pi_at_structure_v2(const ClusterPageStructuralReceiptV2 *receipt,
	ClusterWalInputsV1 *inputs, ClusterPiStructuralAckV2 **out);
extern bool cluster_page_structural_pi_ack_read_v2(const ClusterPiStructuralAckV2 *ack,
	const ClusterPageStructuralReceiptV2 *receipt, int32 *out_node);
extern void cluster_page_structural_pi_ack_free_v2(ClusterPiStructuralAckV2 **ack);

/* Actual local physical completion, qualified for the original writer/boot.
 * It covers this instance, including older boots only when the original
 * retirement owner qualifies them in the supplied complete input scope.
 * NULL inputs is sufficient only when every local source has this boot.
 * Remote node ids,
 * caller bitmaps and physical-result enums cannot construct an acknowledgement.
 * The caller retains the complete input scope throughout this background job.
 * Output must start NULL; failure leaves it unchanged. The acknowledgement is
 * process/ResourceOwner local, never a wire object or ROOT/GC permission. */
extern bool cluster_bufmgr_ack_pi_at_data_v1(const ClusterPageDataReceiptV1 *receipt,
											 const RfPageOnlinePlanV1 *plan,
											 const ClusterWalSourceRef *sources,
											 uint32 source_count, ClusterWalInputsV1 *inputs,
											 ClusterPiPhysicalAckV1 **out);
/* Master-local alternative for a durably recovered DEAD executor, not a
 * physical observation at a remote instance. Requires the complete selected
 * input set and terminal DATA ancestry for this page. The acknowledgement
 * borrows inputs, which must stay alive until the acknowledgement is freed;
 * suspension, ROOT/membership change or a different owner prevents use.
 * It cannot be exported as a physical acknowledgement or authorize WAL GC. */
extern bool cluster_bufmgr_ack_recovered_pi_at_data_v1(
	const ClusterPageDataReceiptV1 *receipt, const RfPageOnlinePlanV1 *plan,
	const ClusterWalSourceRef *sources, uint32 source_count, ClusterWalInputsV1 *inputs,
	int32 node, ClusterPiPhysicalAckV1 **out);
extern bool cluster_page_data_pi_ack_read_v1(const ClusterPiPhysicalAckV1 *ack,
											 const ClusterPageDataReceiptV1 *receipt,
											 int32 *out_node);
extern void cluster_page_data_pi_ack_free_v1(ClusterPiPhysicalAckV1 **ack);

static inline bool
cluster_pcm_pi_write_cut_valid_v1(const ClusterPcmPiWriteCutV1 *cut)
{
	const ResourceXMasterSnapshot *h;
	if (cut == NULL)
		return false;
	h = &cut->holder;
	return cut->binding_generation != 0 && cut->binding_generation != UINT64_MAX
		   && cut->transition_count != 0 && cut->transition_count != UINT64_MAX
		   && cut->master_generation != 0 && cut->master_generation != UINT64_MAX
		   && cut->master_node >= 0 && cut->master_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		   && h->assertion.requester_node >= 0
		   && h->assertion.requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		   && (h->assertion.resource.forkNum == MAIN_FORKNUM
			   || h->assertion.resource.forkNum == VISIBILITYMAP_FORKNUM
			   || (h->assertion.resource.forkNum == SPACE_FORKNUM
				   && h->assertion.resource.blockNum < 2))
		   && h->phase == RESOURCE_X_MASTER_SETTLED && h->is_head == 0 && h->resource_formation != 0
		   && h->resource_formation != UINT64_MAX && h->master_session_incarnation != 0
		   && h->master_session_incarnation != UINT64_MAX && h->assertion_sequence != 0
		   && h->assertion_sequence != UINT64_MAX && h->base_authority_generation != 0
		   && h->final_authority_generation > h->base_authority_generation
		   && h->final_authority_generation != UINT64_MAX && h->requester_target_generation != 0
		   && h->requester_target_generation != UINT64_MAX
		   && h->blocked_holders_bitmap == h->incompatible_holders_bitmap;
}

/* Read only; requires the local master and an exactly settled current X.
 * A zero PI set has no work. False leaves out unchanged and creates no entry. */
extern bool cluster_pcm_lock_pi_write_snapshot_v1(BufferTag tag, ClusterPcmPiWriteCutV1 *out);

/* Read-only bounded registry walk for the existing background writer. Tags
 * are candidates only; the consumer must capture/recheck an exact cut. */
extern uint32 cluster_pcm_lock_pi_candidates_v1(uint32 *cursor, uint32 probe_budget,
												BufferTag *tags, uint32 capacity);

/* Read-only, bounded relation observation for the original completed KO
 * owner. MORE/PENDING preserve its prefix, INVALID grants no progress.
 * EMPTY covers only local MAIN/VM responsibility. The caller must independently
 * exclude new old-incarnation producers with its exact structural/barrier cut;
 * this observation never clears PI or authorizes WAL/file reclamation. */
typedef enum ClusterPcmPiRelationScanV2 {
	CLUSTER_PCM_PI_RELATION_INVALID,
	CLUSTER_PCM_PI_RELATION_PENDING,
	CLUSTER_PCM_PI_RELATION_MORE,
	CLUSTER_PCM_PI_RELATION_EMPTY
} ClusterPcmPiRelationScanV2;
extern ClusterPcmPiRelationScanV2 cluster_pcm_lock_pi_relation_scan_v2(RelFileLocator locator,
																	   uint64 formation,
																	   uint32 budget,
																	   uint32 *cursor);

/* Actual holder DATA write. The cut is captured before this call; it must
 * match the installed current-X buffer generation. No ownership acquisition.
 * A receipt from the ordinary endpoint cannot retire master PI obligations. */
extern bool cluster_bufmgr_write_page_data_at_cut_v1(const ClusterPageDataTargetV1 *target,
													 const ClusterPcmPiWriteCutV1 *cut,
													 ClusterPageDataReceiptV1 **out);

/* Background current-block variant: the requested incarnation must match,
 * while the actual version is sampled under the installed holder's content
 * lock. Later local mutations do not turn owned checkpoint work into errors. */
extern bool cluster_bufmgr_write_current_data_at_cut_v1(const ClusterPageDataTargetV1 *target,
														const ClusterPcmPiWriteCutV1 *cut,
														ClusterPageDataReceiptV1 **out);

/* Master write requests know the namespace/tag and installed X generation,
 * not the holder's current version. Sample incarnation from locked SPACE0
 * and version from the actual current binding. Original background I/O only;
 * this neither creates an identity nor acquires X. */
extern bool cluster_bufmgr_write_tag_data_at_cut_v1(const ClusterSpaceIdentityKey *key,
													const ClusterPcmPiWriteCutV1 *cut,
													ClusterPageDataReceiptV1 **out);

/* Copy actual DATA facts for the original background completion transport.
 * This projection is not an import/receipt constructor or retirement grant.
 * A raw decoded binding/cut cannot create an opaque DATA receipt. */
extern bool cluster_page_data_pi_export_v1(const ClusterPageDataReceiptV1 *receipt,
										   ClusterPageWalBindingV1 *binding,
										   ClusterPcmPiWriteCutV1 *cut);

/* Validate ancestry through the actual receipt in a sealed full-source PAGE
 * plan. Later page versions remain uncovered in its contribution prefix.
 * This projection grants neither checkpoint publication nor WAL reuse. */
extern bool cluster_page_data_pi_proof_v1(const ClusterPageDataReceiptV1 *receipt,
										  const RfPageOnlinePlanV1 *plan,
										  const ClusterWalSourceRef *sources, uint32 source_count,
										  ClusterPcmPiWriteCutV1 *out);

/* Clear only the unchanged master cut after exact DATA/ancestry proof AND
 * qualified physical or original recovery acknowledgements for every holder. Missing,
 * duplicate or foreign-cut acknowledgements never retire a subset. A retry
 * against an unchanged already-cleared cut is idempotent. holders_out reports
 * the completed set, never a queue of notifications still owed. The retained
 * input scope must remain valid; these endpoints do not authorize ROOT/GC.
 * False clears holders_out but no shared directory state. */
extern bool cluster_pcm_lock_pi_write_complete_v1(const ClusterPageDataReceiptV1 *receipt,
												  const RfPageOnlinePlanV1 *plan,
												  const ClusterWalSourceRef *sources,
												  uint32 source_count,
												  const ClusterPiPhysicalAckV1 *const *acks,
												  uint32 ack_count, uint32 *holders_out);
extern bool cluster_pcm_lock_pi_storage_complete_v1(const ClusterPageDataReceiptV1 *receipt,
													const RfPageOnlinePlanV1 *plan,
													const ClusterWalSourceRef *sources,
													uint32 source_count,
													const ClusterPiPhysicalAckV1 *const *acks,
													uint32 ack_count, uint32 *holders_out);
/* Original structural owner, never DATA/ROOT/GC authority. Qualify every
 * holder before taking directory locks, then compare the entire X or N/S
 * master cut under entry X. Refusal zeroes holders_out without changing
 * the directory. The caller retains the receipt and all ACK/input scopes. */
extern bool cluster_pcm_lock_pi_structural_complete_v2(const ClusterPageStructuralReceiptV2 *receipt,
	const ClusterPiStructuralAckV2 *const *acks, uint32 ack_count, uint32 *holders_out);
#endif
#endif /* CLUSTER_PI_WRITE_H */
