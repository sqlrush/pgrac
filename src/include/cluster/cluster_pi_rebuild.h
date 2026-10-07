/* PGRAC: reconstruct retained writer obligations before a new master serves.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PI_REBUILD_H
#define CLUSTER_PI_REBUILD_H

#include "cluster/cluster_grd.h"
#include "cluster/cluster_scn.h"
#include "storage/buf_internals.h"

typedef enum ClusterPiRebuildProgressV1 {
	CLUSTER_PI_REBUILD_IDLE = 0,
	CLUSTER_PI_REBUILD_WAIT = 1,
	/* Bounded work made progress. Run the next batch without a timer sleep. */
	CLUSTER_PI_REBUILD_MORE = 2
} ClusterPiRebuildProgressV1;

extern ClusterPiRebuildProgressV1 cluster_pi_rebuild_bgwriter_tick_v1(void);
/* Local target-master admission/progression only. Control cleanup and remote
 * survivor declarations remain independent of this DATA authority gate. */
extern bool cluster_grd_pi_rebuild_blocked_v1(BufferTag tag);
/* Additive only, under the exact still-frozen cut. No S/X, DATA or retirement
 * authority can be created by this consumer. */
extern bool cluster_pcm_rebuild_pi_contributors_v1(const ClusterGrdPiRebuildCutV1 *cut,
												   BufferTag tag, uint32 holders,
												   XLogRecPtr watermark_lsn, SCN watermark_scn);

#endif
