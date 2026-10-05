/* Read-only PRE2 lifecycle observations; never admission or exit authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CONTROL_OBSERVE_H
#define CLUSTER_CONTROL_OBSERVE_H

/* palloc-owned JSON, or NULL when the current native writer/R4 cut is unavailable. */
extern char *cluster_control_observe_writer_json(void);
extern void ClusterControlObserveMain(int argc, char **argv) pg_attribute_noreturn();

#endif
