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
struct ClusterRecoveryFpiInsertV1;
struct ClusterRecoverySerialGuard;
struct ClusterWalRetentionPin;
struct ClusterRecoveryStartV1;
struct ClusterColdCtrcWorkV1;
struct RfPageAuthorityGuardV1;
struct ClusterSpaceIdentity;
/* Native E1 recovered Startup only. Consume the original pending FPI and
 * still-held PAGE/serial/pin; install exact before/result, sync and postread.
 * Retain the original image, guard and INSERTED window on every path. Success
 * sets only process-local target_synced; no discharge, DURABLE or clear.
 * Failure marks the exact window failed where still owned; ERROR rethrows. */
extern bool rf_page_recovery_fpi_install_smgr_v1(struct ClusterRecoveryFpiInsertV1 *inserted,
	struct ClusterRecoverySerialGuard *serial, struct ClusterWalRetentionPin *pin);
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
#endif

#endif /* CLUSTER_PAGE_INSTALL_H */
