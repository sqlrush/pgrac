/* Shared heap's conservative native freeze boundary.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_HEAP_HORIZON_H
#define CLUSTER_HEAP_HORIZON_H

#include "access/transam.h"

/* Before heap mutation, cap a local proposal at the immutable activation
 * floor: another slot can still issue an XID below this node's proposal.
 * Not a complete cluster freeze/reclamation horizon. Refusal preserves out.
 * The caller excludes temporary relations. Nonshared mode is native. */
extern bool cluster_heap_freeze_cutoff_v1(TransactionId proposed, TransactionId *out);

#endif
