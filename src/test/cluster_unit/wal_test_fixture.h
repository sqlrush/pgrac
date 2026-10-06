/* PGRAC: record endpoints and deliberately obsolete sidefile fixtures.
 * No production codec or promise is supplied by these bytes.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef WAL_TEST_FIXTURE_H
#define WAL_TEST_FIXTURE_H
#include "cluster/cluster_wal_source.h"
#include "port/pg_crc32c.h"
typedef struct WalTestRecord {
	uint64 sequence;
	XLogRecPtr exclusive_end, record_start;
	pg_crc32c record_crc;
} WalTestRecord;
#define WAL_TEST_OBSOLETE_BYTES 256
static inline void
wal_test_obsolete_bytes(const ClusterWalSourceRef *source, const WalTestRecord *record,
						uint8 bytes[WAL_TEST_OBSOLETE_BYTES])
{
	memset(bytes, 0x6d, WAL_TEST_OBSOLETE_BYTES);
	memcpy(bytes, "PGWP", 4);
	memcpy(bytes + 112, &source->timeline, sizeof(source->timeline));
	memcpy(bytes + 128, record, sizeof(*record));
}
#endif
