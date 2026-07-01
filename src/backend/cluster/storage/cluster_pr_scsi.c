/*-------------------------------------------------------------------------
 *
 * cluster_pr_scsi.c
 *	  SCSI-3 Persistent Reservation probe/register helpers.
 *
 *	  The raw block_device backend uses this file to detect whether the
 *	  attached device accepts SCSI-3 PR commands and to register this node's
 *	  own key.  Cross-node preempt/evict remains outside spec-6.0a.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_pr_scsi.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *	  Spec: spec-6.0a-production-shared-storage-backend-matrix.md
 *	  (FROZEN, SCSI-3 PR capability probe and own-key registration).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>

#ifdef __linux__
#include <scsi/sg.h>
#include <sys/ioctl.h>
#endif

#include "cluster/storage/cluster_pr_scsi.h"

#ifdef USE_PGRAC_CLUSTER

#define CLUSTER_PR_SCSI_TIMEOUT_MS 5000
#define CLUSTER_PR_SCSI_PARAM_REGISTER_LEN 24
#define CLUSTER_PR_SCSI_READ_KEYS_LEN 32
#define CLUSTER_PR_SCSI_KEY_PREFIX UINT64CONST(0x5047524143000000) /* "PGRAC" */

#ifdef __linux__
static void
cluster_pr_scsi_store_be64(unsigned char *dst, uint64 value)
{
	int i;

	for (i = 7; i >= 0; i--) {
		dst[i] = (unsigned char)(value & 0xff);
		value >>= 8;
	}
}

static int
cluster_pr_scsi_sgio(int fd, unsigned char *cdb, unsigned char cdb_len, void *data,
					 unsigned int data_len, int dxfer_direction)
{
	sg_io_hdr_t hdr;
	unsigned char sense[32];

	memset(&hdr, 0, sizeof(hdr));
	memset(sense, 0, sizeof(sense));

	hdr.interface_id = 'S';
	hdr.cmdp = cdb;
	hdr.cmd_len = cdb_len;
	hdr.sbp = sense;
	hdr.mx_sb_len = sizeof(sense);
	hdr.dxferp = data;
	hdr.dxfer_len = data_len;
	hdr.dxfer_direction = dxfer_direction;
	hdr.timeout = CLUSTER_PR_SCSI_TIMEOUT_MS;

	if (ioctl(fd, SG_IO, &hdr) < 0)
		return errno == 0 ? EIO : errno;
	if ((hdr.info & SG_INFO_OK_MASK) != SG_INFO_OK)
		return EIO;
	return 0;
}
#endif

uint64
cluster_pr_scsi_key_for_node(int node_id)
{
	uint64 node;

	if (node_id < 0)
		return 0;

	node = (uint64)((uint32)node_id + 1);
	return CLUSTER_PR_SCSI_KEY_PREFIX | (node & UINT64CONST(0x000000000000ffff));
}

ClusterFenceCapability
cluster_pr_scsi_probe(int fd)
{
#ifdef __linux__
	unsigned char cdb[10];
	unsigned char data[CLUSTER_PR_SCSI_READ_KEYS_LEN];

	if (fd < 0)
		return CLUSTER_FENCE_CAP_NONE;

	memset(cdb, 0, sizeof(cdb));
	memset(data, 0, sizeof(data));

	cdb[0] = 0x5e; /* PERSISTENT RESERVE IN */
	cdb[1] = 0x00; /* READ KEYS */
	cdb[7] = (unsigned char)(sizeof(data) >> 8);
	cdb[8] = (unsigned char)(sizeof(data) & 0xff);

	if (cluster_pr_scsi_sgio(fd, cdb, sizeof(cdb), data, sizeof(data), SG_DXFER_FROM_DEV) == 0)
		return CLUSTER_FENCE_CAP_SCSI3_PR;
#else
	(void)fd;
#endif
	return CLUSTER_FENCE_CAP_NONE;
}

int
cluster_pr_scsi_register_key(int fd, int node_id)
{
#ifdef __linux__
	unsigned char cdb[10];
	unsigned char data[CLUSTER_PR_SCSI_PARAM_REGISTER_LEN];
	uint64 key = cluster_pr_scsi_key_for_node(node_id);

	if (fd < 0 || key == 0)
		return EINVAL;

	memset(cdb, 0, sizeof(cdb));
	memset(data, 0, sizeof(data));

	cluster_pr_scsi_store_be64(data + 8, key);

	cdb[0] = 0x5f; /* PERSISTENT RESERVE OUT */
	cdb[1] = 0x00; /* REGISTER */
	cdb[7] = (unsigned char)(sizeof(data) >> 8);
	cdb[8] = (unsigned char)(sizeof(data) & 0xff);

	return cluster_pr_scsi_sgio(fd, cdb, sizeof(cdb), data, sizeof(data), SG_DXFER_TO_DEV);
#else
	(void)fd;
	(void)node_id;
	return EOPNOTSUPP;
#endif
}

#endif /* USE_PGRAC_CLUSTER */
