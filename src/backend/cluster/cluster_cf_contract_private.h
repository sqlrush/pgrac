/* Existing local storage-contract image, shared with the original creator.
 * This records identity, not database creation or serving permission.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CF_CONTRACT_PRIVATE_H
#define CLUSTER_CF_CONTRACT_PRIVATE_H

#include "cluster/cluster_cf_storage.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "port/pg_crc32c.h"

#define CLUSTER_CF_CONTRACT_MAGIC 0x43464354
#define CLUSTER_CF_CONTRACT_VERSION 2

typedef struct ClusterCfContractRecord {
	uint32 magic;
	uint32 version;
	uint64 authority_system_identifier;
	char storage_uuid[CLUSTER_SHARED_UUID_LEN];
	char _pad[3];
	uint32 state;
	pg_crc32c crc;
} ClusterCfContractRecord;

#endif
