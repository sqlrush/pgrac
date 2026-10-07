/* Original initdb's new shared DATA producer; no startup admission.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_INITDB_BASE_H
#define CLUSTER_INITDB_BASE_H

/* Called only by ShutdownXLOG before the original final checkpoint. A missing
 * creator context is a no-op; an invalid or failed creation is fatal. */
extern void cluster_initdb_base_create(int exit_code);

#endif
