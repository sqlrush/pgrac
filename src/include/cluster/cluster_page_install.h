/*-------------------------------------------------------------------------
 *
 * cluster_page_install.h
 *    STOP-06 storage install, durability and canonical post-read.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_INSTALL_H
#define CLUSTER_PAGE_INSTALL_H

#include "cluster/cluster_page_stable_base.h"

#define CLUSTER_PAGE_INSTALL_INTERFACE_V1 1

typedef struct RfPageStorageInstallComponentV1 {
	RfPageIdentityV1 page_identity;
	uint8 before_kind;
	uint8 reserved_zero[7];
	RfPageVersionV1 expected_before;
	RfPageVersionV1 expected_result;
	const char *canonical_page;
} RfPageStorageInstallComponentV1;

typedef struct RfPageInstallStorageOpsV1 {
	void *arg;
	bool checksums_enabled;
	bool (*read)(void *arg, uint32 index, const RfPageIdentityV1 *identity, char page[BLCKSZ],
				 bool *exists);
	bool (*write)(void *arg, uint32 index, const RfPageIdentityV1 *identity,
				  const char page[BLCKSZ], bool extend);
	bool (*sync)(void *arg, uint32 index, const RfPageIdentityV1 *identity);
	uint16 (*checksum)(void *arg, const char page[BLCKSZ], BlockNumber blockno);
} RfPageInstallStorageOpsV1;

typedef struct RfPageInstallAuthorityOpsV1 {
	void *arg;
	bool (*validate_identity)(void *arg, const RfPageIdentityV1 *identity,
							  const uint8 incarnation[16]);
	bool (*promote)(void *arg);
	bool (*publish)(void *arg);
	bool (*release)(void *arg);
	/* Optional: exact membership in the protected, complete contributor chain.
	 * Called only after promotion; absence preserves the strict before/result gate. */
	bool (*covers_version)(void *arg, const RfPageIdentityV1 *identity,
						   const RfPageVersionV1 *version, const RfPageVersionV1 *result);
} RfPageInstallAuthorityOpsV1;

typedef struct RfPageStorageInstallRequestV1 {
	const RfPageStorageInstallComponentV1 *components;
	uint32 component_count;
	char *prepared_pages;
	Size prepared_capacity;
	char *io_pages;
	Size io_capacity;
	const RfPageInstallStorageOpsV1 *storage;
	const RfPageInstallAuthorityOpsV1 *authority;
	bool global_preflight_ok;
} RfPageStorageInstallRequestV1;

typedef struct RfPageStorageInstallProofV1 {
	uint32 component_count;
	uint32 write_count;
	uint32 result_skip_count;
	bool durability_complete;
	bool postread_complete;
	bool proof_published;
	bool authority_released;
} RfPageStorageInstallProofV1;

extern RfPageProofDetailV1
rf_page_storage_install_execute_v1(const RfPageStorageInstallRequestV1 *request,
								   RfPageStorageInstallProofV1 *proof);

typedef struct RfPageSmgrPreopenV1 RfPageSmgrPreopenV1;

extern RfPageProofDetailV1
rf_page_storage_smgr_preopen_v1(const RfPageStorageInstallRequestV1 *request,
								RfPageSmgrPreopenV1 **out_preopen);
extern RfPageProofDetailV1
rf_page_storage_install_smgr_preopened_v1(const RfPageStorageInstallRequestV1 *request,
										  RfPageSmgrPreopenV1 *preopen,
										  RfPageStorageInstallProofV1 *proof);
extern void rf_page_storage_smgr_preopen_destroy_v1(RfPageSmgrPreopenV1 **preopen);
extern RfPageProofDetailV1
rf_page_storage_install_smgr_v1(const RfPageStorageInstallRequestV1 *request,
								RfPageStorageInstallProofV1 *proof);

#ifdef USE_PGRAC_CLUSTER
#include "access/generic_xlog.h"

struct ClusterRecoveryFpiInsertV1;
struct ClusterRecoverySerialGuard;
struct ClusterWalRetentionPin;
struct ClusterRecoveryStartV1;
struct ClusterColdCtrcWorkV1;
struct ClusterColdTtSlotInputV1;
struct RfPageAuthorityGuardV1;
struct ClusterSpaceIdentity;
/* Native E1 recovered Startup only. Consume the original pending FPI and
 * still-held PAGE/serial/pin; install exact before/result, sync and postread.
 * Retain the original image, guard and INSERTED window on every path. Success
 * sets only process-local target_synced; no discharge, DURABLE or clear.
 * Failure marks the exact window failed where still owned; ERROR rethrows. */
extern bool rf_page_recovery_fpi_install_smgr_v1(struct ClusterRecoveryFpiInsertV1 *inserted,
	struct ClusterRecoverySerialGuard *serial, struct ClusterWalRetentionPin *pin);
/* Same original Finish contract, then the installer above. Caller supplies a
 * qualified private frame and fresh, distinct output; never reuse live output.
 * Finish prepares hash/capacity before nowait PAGE promotion. Only its success
 * reaches target I/O. False/ERROR retains its original guard/window/WAL debt;
 * success means only target_synced, with no discharge, DURABLE or clear. */
extern bool rf_page_recovery_fpi_apply_smgr_v1(const GenericXLogRecoveryImageV1 *image,
	const struct ClusterRecoveryStampOwnerV1 *owner, struct RfPageAuthorityPreflightV1 *preflight,
	struct ClusterRecoverySerialGuard *serial, struct ClusterWalRetentionPin *pin,
	struct ClusterRecoveryFpiInsertV1 *out);
/* Provisional native MAIN predecessor input only, under the original
 * RECOVERED/native-DONE Start/work and promoted PAGE/serial/pin. Verify exact
 * LIVE SPACE identity/reservation, page version/checksum and unchanged SPACE
 * rereads. Retain every borrowed owner; no install, WAL or release proof.
 * Caller supplies distinct output storage, disjoint from all borrowed state.
 * Refusal/ERROR leaves output unchanged; native ERROR propagates. */
extern bool rf_page_recovery_ctrc_read_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, const struct RfPageAuthorityGuardV1 *guard,
	struct ClusterRecoverySerialGuard *serial, struct ClusterWalRetentionPin *pin,
	const struct ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, PGAlignedBlock *out);

typedef struct RfPageRecoveryCtrcItlSelectionV1 {
	uint32 subject_index;
	uint16 itl_index;
	uint16 expected_itl_wrap;
	uint8 itl_class;
} RfPageRecoveryCtrcItlSelectionV1;

typedef struct RfPageRecoveryCtrcItlImageV1 {
	PGAlignedBlock image;
	bool rewritten;
} RfPageRecoveryCtrcItlImageV1;

/* Read the same protected native predecessor and calculate only the selected
 * original work entries' ITL projections. All selections must succeed before
 * output changes; no output may overlap borrowed state. Original source,
 * terminal/history qualification and complete coverage remain the caller's
 * responsibility. The private candidate retains predecessor header/version
 * fields; it is not a WAL image, installed page or release/completion proof.
 * Native ERROR propagates without changing output or retiring any owner. */
extern bool rf_page_recovery_ctrc_itl_image_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, const struct RfPageAuthorityGuardV1 *guard,
	struct ClusterRecoverySerialGuard *serial, struct ClusterWalRetentionPin *pin,
	const struct ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, const RfPageRecoveryCtrcItlSelectionV1 *selections,
	Size count, RfPageRecoveryCtrcItlImageV1 *out);

typedef struct RfPageRecoveryCtrcItlFpiV1 {
	GenericXLogRecoveryImageV1 frame;
	bool rewritten;
} RfPageRecoveryCtrcItlFpiV1;

/* Prepare the original Generic versioned frame from that same protected
 * predecessor and selected ITL calculation. No change returns rewritten=false
 * and a zero frame without minting a token. Refusal/ERROR preserves all output
 * bytes and borrowed owners. A frame is only a private candidate: complete
 * source/history/coverage proof, later hash/capacity preparation outside PAGE,
 * exact F4/WAL/installation and responsibility discharge remain required. */
extern bool rf_page_recovery_ctrc_itl_fpi_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, const struct RfPageAuthorityGuardV1 *guard,
	struct ClusterRecoverySerialGuard *serial, struct ClusterWalRetentionPin *pin,
	const struct ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, const RfPageRecoveryCtrcItlSelectionV1 *selections,
	Size count, RfPageRecoveryCtrcItlFpiV1 *out);

/* Calculate all native page ITLs against the complete original work array.
 * Reserve scratch for at least work->counts.subject_count entries before PAGE
 * promotion; it must be distinct from output and every borrowed object. This
 * call performs no scratch allocation and may change it on refusal/ERROR.
 * The full original input is copied before I/O and rechecked before publishing
 * a private frame. All source/history/coverage and later F4/WAL/install/release
 * obligations of the selected-frame API remain. No change returns a zero frame
 * without a new token. Output and borrowed owners are retained on every failure. */
extern bool rf_page_recovery_ctrc_itl_page_fpi_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, const struct RfPageAuthorityGuardV1 *guard,
	struct ClusterRecoverySerialGuard *serial, struct ClusterWalRetentionPin *pin,
	const struct ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, struct ClusterColdTtSlotInputV1 *scratch,
	Size capacity, RfPageRecoveryCtrcItlFpiV1 *out);
/* Post-C0 variant using the same retained pre-C0 work. The caller holds a
 * complete immutable physical TT observation array in original work order,
 * under the original isolation/namespace/full-census premises. Require exact
 * original accepted TX value plus each canonical C0/terminal successor.
 * Reserve scratch for twice subject_count entries before PAGE promotion:
 * original inputs then terminal views. Recheck all inputs and that same TX
 * value across the original reader/token producer; no scratch allocation.
 * All backing storage must be distinct; scratch may change on refusal/ERROR,
 * output and original owners/debts stay unchanged. This only prepares a
 * private frame; graph/history/source proof, F4/WAL/install/discharge and
 * full-service completion remain separate requirements. */
struct ClusterRecoveryUndoEntry;
extern bool rf_page_recovery_ctrc_itl_c0_page_fpi_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work, const struct RfPageAuthorityGuardV1 *guard,
	struct ClusterRecoverySerialGuard *serial, struct ClusterWalRetentionPin *pin,
	const struct ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, const struct ClusterRecoveryUndoEntry *physical,
	Size physical_count, struct ClusterColdTtSlotInputV1 *scratch, Size capacity,
	RfPageRecoveryCtrcItlFpiV1 *out);
#endif

#endif /* CLUSTER_PAGE_INSTALL_H */
