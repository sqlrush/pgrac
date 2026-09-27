/*-------------------------------------------------------------------------
 *
 * cluster_native_startup.h
 *    Read-only native startup page predicates shared by origin inspection.
 *
 * These functions validate bytes, not recovery ownership or completion.
 * Native modules assert the geometry against their own on-disk definitions.
 *
 * Portions Copyright (c) 2026, PGRAC contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_native_startup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_NATIVE_STARTUP_H
#define CLUSTER_NATIVE_STARTUP_H

#include "access/transam.h"

#define CLUSTER_NATIVE_CLOG_PER_PAGE (BLCKSZ * 4)
#define CLUSTER_NATIVE_SUBTRANS_PER_PAGE (BLCKSZ / sizeof(TransactionId))
#define CLUSTER_NATIVE_MX_OFFSETS_PER_PAGE (BLCKSZ / sizeof(uint32))
#define CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE ((BLCKSZ / 20) * 4)
#define CLUSTER_NATIVE_COMMIT_TS_PER_PAGE (BLCKSZ / 10)

static inline bool
cluster_native_clog_suffix_unused(const char *page, TransactionId next)
{
	uint32 byte = (next % CLUSTER_NATIVE_CLOG_PER_PAGE) / 4;
	unsigned shift = (next % 4) * 2;

	if (page == NULL || !TransactionIdIsNormal(next))
		return false;
	if (((unsigned char)page[byte] & ~((1 << shift) - 1)) != 0)
		return false;
	for (uint32 i = byte + 1; i < BLCKSZ; ++i)
		if (page[i] != 0)
			return false;
	return true;
}

static inline bool
cluster_native_subtrans_suffix_unused(const char *page, TransactionId next)
{
	if (page == NULL || !TransactionIdIsNormal(next))
		return false;
	for (uint32 i = next % CLUSTER_NATIVE_SUBTRANS_PER_PAGE; i < CLUSTER_NATIVE_SUBTRANS_PER_PAGE;
		 ++i) {
		TransactionId parent;
		memcpy(&parent, page + i * sizeof(parent), sizeof(parent));
		if (TransactionIdIsValid(parent))
			return false;
	}
	return true;
}

#endif /* CLUSTER_NATIVE_STARTUP_H */
