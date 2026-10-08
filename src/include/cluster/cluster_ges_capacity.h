/*-------------------------------------------------------------------------
 * cluster_ges_capacity.h
 *   Startup-only bounded queue capacity for the configured GES cohort.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GES_CAPACITY_H
#define CLUSTER_GES_CAPACITY_H

#include "access/twophase.h"
#include "cluster/cluster_conf.h"
#include "miscadmin.h"
#include "storage/shmem.h"

/* One resource may have all eight PG modes for each configured owner. Queues
 * reserve complete bursts at startup. This is a finite capacity, not permission
 * to drop cleanup or a guarantee against an unbounded stalled peer. */
static inline uint32
cluster_ges_configured_capacity(uint32 minimum, unsigned waves)
{
	Size owners = mul_size((Size)Max(1, cluster_conf_declared_node_count_early()),
						   add_size((Size)Max(1, MaxBackends), (Size)Max(0, max_prepared_xacts)));
	Size slots = mul_size(mul_size(owners, 8), waves);

	if (slots > INT_MAX / 4)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("configured GES message capacity is too large")));
	return Max(minimum, (uint32)slots);
}
#endif
