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

#endif /* CLUSTER_PAGE_INSTALL_H */
