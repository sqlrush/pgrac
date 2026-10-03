/* Original initdb child, before ordinary startup dispatch.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_INITDB_COHORT_H
#define CLUSTER_INITDB_COHORT_H

extern void ClusterInitdbCohortMain(int argc, char **argv) pg_attribute_noreturn();

#endif
