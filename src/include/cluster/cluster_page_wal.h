/* Exact last-mutation WAL source for a versioned resident page.
 * In-memory values only; neither WAL durability nor writer authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PAGE_WAL_H
#define CLUSTER_PAGE_WAL_H

#include "cluster/cluster_page_stable_base.h"
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_wal_source.h"
#include "storage/buf.h"

typedef struct ClusterPageWalBindingV1 {
	ClusterWalSourceRef source;
	RfPageIdentityV1 identity;
	RfPageVersionV1 version;
	XLogRecPtr record_start;
	XLogRecPtr record_end;
	uint32 record_crc;
	uint8 rmid;
	uint8 info;
	uint16 reserved_zero;
} ClusterPageWalBindingV1;

extern Size cluster_page_wal_shmem_size(void);
extern void cluster_page_wal_shmem_init(void);
extern void cluster_page_wal_shmem_register(void);

/* Sole native WAL insertion owner after successful insertion, while its
 * registered buffer is still pinned and content-X. No allocation, I/O or
 * lock upgrade. Caller must reject a failure before exposing the mutation.
 * End/start/CRC belong to the actual inserted record, not its retry candidate. */
extern bool cluster_page_wal_capture_native_v1(Buffer buffer, const RfPageVersionEdgeEntryV1 *edge,
											   uint64 result_token, XLogRecPtr start,
											   XLogRecPtr end, uint32 crc, uint8 rmid, uint8 info);

/* Caller already pins and content-locks this descriptor and supplies the
 * lifecycle-qualified SPACE identity. Does not acquire any page or grant
 * write/flush authority. Failure leaves output untouched. */
extern bool cluster_page_wal_read_v1(Buffer buffer, const ClusterSpaceIdentity *identity,
									 ClusterPageWalBindingV1 *out);

#endif
