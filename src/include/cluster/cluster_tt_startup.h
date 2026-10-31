/* Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_TT_STARTUP_H
#define CLUSTER_TT_STARTUP_H

#include "c.h"

/* Minted only by StartupXLOG's private legacy-sweep call. No constructor is
 * exported. A borrowed pointer is valid only during that lexical call. This
 * is local startup authority, never a shared recovery/admission proof. */
typedef struct ClusterTTStartupLocalContext ClusterTTStartupLocalContext;
extern const ClusterTTStartupLocalContext *cluster_tt_startup_local_context_current(void);
extern bool cluster_tt_startup_local_context_valid(const ClusterTTStartupLocalContext *context,
												 uint8 owner_instance);

#endif
