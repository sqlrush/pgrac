/* Ordinary writer fixtures have no authenticated creation owner. Metadata
 * selection must stay UNKNOWN; accidentally consuming a request/table fails. */
#ifndef TEST_CLUSTER_CLOG_NO_CREATION_H
#define TEST_CLUSTER_CLOG_NO_CREATION_H
#if defined(USE_PGRAC_CLUSTER) && !defined(CLOG_FIXTURE_CREATION_PROVIDED)
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_wal_thread.h"
#ifndef CLOG_FIXTURE_HAVE_UNDER_POSTMASTER
bool IsUnderPostmaster;
#endif
bool IsPostmasterEnvironment;
BackendType MyBackendType;
#ifndef CLOG_FIXTURE_NATIVE_MODE_PROVIDED
ClusterControlNativeModeV2
cluster_control_native_files_mode_v2(void)
{ return CLUSTER_NATIVE_FILES_UNKNOWN_V2; }
#endif
const PgracInitdbRecoveryContext *
cluster_wal_thread_initdb_recovery_context(void)
{ abort(); }
bool
cluster_control_native_files_info_v2(ClusterControlNativeTableInfoV2 *out)
{ (void) out; abort(); }
#endif
#endif
