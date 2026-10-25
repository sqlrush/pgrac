/*-------------------------------------------------------------------------
 * cluster_unit_port_stubs.c
 *   Standalone error boundary for the real backend portability library.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * ARM's runtime CRC selector logs its chosen implementation. Tests that
 * otherwise need no backend error machinery still link the real selector.
 * Existing per-fixture error handlers take precedence over these weak
 * defaults. Only the selector's DEBUG1 message is ignored; any unexpected
 * severity aborts the test, never becoming a successful operation.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_visibility_trace_filter.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_update_trace.h"
#include "cluster/cluster_tx_identity_trace.h"
#include "cluster/cluster_native_itl_trace.h"
#include "cluster/cluster_wait_trace.h"
#include "cluster/cluster_xnode_profile.h"
#include "cluster/cluster_storage_quorum.h"

/* Profiling-aware product objects can link without a diagnostic backend.
 * Dedicated trace tests override these defaults with the real collector. */
bool cluster_update_trace_enabled __attribute__((weak)) = false;
bool cluster_trace_tx_identity __attribute__((weak)) = false;
void __attribute__((weak))
cluster_native_itl_trace_tt(ClusterNativeItlTraceKind kind pg_attribute_unused(),
						  const ClusterCtrcTxnKeyV1 *key pg_attribute_unused(),
						  const TTSlot *slot pg_attribute_unused(),
						  XLogRecPtr lsn pg_attribute_unused())
{
	if (cluster_trace_tx_identity)
		abort();
}
void __attribute__((weak))
cluster_native_itl_trace_selected(Buffer buffer pg_attribute_unused(),
								 TransactionId xid pg_attribute_unused(),
								 uint8 selected pg_attribute_unused(),
								 bool succeeded pg_attribute_unused())
{
	if (cluster_trace_tx_identity)
		abort();
}
void __attribute__((weak))
cluster_native_itl_trace_terminal(ClusterNativeItlTraceKind kind pg_attribute_unused(),
	uint8 owner pg_attribute_unused(), uint32 segment pg_attribute_unused(),
	uint32 generation pg_attribute_unused(), uint16 offset pg_attribute_unused(),
	const TTSlot *successor pg_attribute_unused(), XLogRecPtr lsn pg_attribute_unused(),
	const ClusterCanonicalTxnBinding *binding pg_attribute_unused())
{
	if (cluster_trace_tx_identity)
		abort();
}
void __attribute__((weak))
cluster_tx_identity_trace_emit(ClusterTxIdentityTraceKind kind pg_attribute_unused(),
								const ClusterTxIdentityTrace *row pg_attribute_unused())
{
	abort();
}
uint64 __attribute__((weak))
cluster_wait_trace_begin(ClusterWaitTraceKind kind pg_attribute_unused())
{
	if (cluster_trace_tx_identity)
		abort();
	return 0;
}
void __attribute__((weak))
cluster_wait_trace_emit(ClusterWaitTraceKind kind pg_attribute_unused(),
						const ClusterWaitTrace *row pg_attribute_unused())
{
	if (cluster_trace_tx_identity)
		abort();
}
void __attribute__((weak))
cluster_buffer_trace_tx_wait(ClusterWaitTraceSite site pg_attribute_unused(),
							uint64 wait_seq pg_attribute_unused(),
							uint32 tx_xid pg_attribute_unused(),
							uint64 request_seq pg_attribute_unused())
{
	if (cluster_trace_tx_identity)
		abort();
}
bool cluster_trace_visibility_enabled __attribute__((weak)) = false;
ClusterVisibilityTraceFilter cluster_trace_visibility_filter __attribute__((weak)) = { 0 };
NodeId __attribute__((weak))
uba_origin_node_id(UBA uba pg_attribute_unused())
{
	abort();
}
bool cluster_xnode_profile_enabled __attribute__((weak)) = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl __attribute__((weak)) = NULL;

/* PageSetLSN reads the static shared-storage profile. Legacy standalone
 * fixtures default to off; shared-profile tests supply their own value. */
bool cluster_shared_config __attribute__((weak)) = false;
int cluster_node_id __attribute__((weak)) = -1;

/* Legacy fixtures run the native profile. A shared voting-I/O fixture must
 * provide its actual eligibility boundary, never silently grant authority. */
bool __attribute__((weak))
cluster_storage_quorum_allows_node(int node_id pg_attribute_unused())
{
	abort();
}

void __attribute__((weak))
cluster_update_trace_event_at(const ClusterUpdateTraceEvent *event pg_attribute_unused(),
							  uint64 now pg_attribute_unused())
{
	abort();
}

void __attribute__((weak))
cluster_update_trace_interval_enter_at(ClusterUpdateTraceInterval *interval pg_attribute_unused(),
									   const ClusterUpdateTraceEvent *event pg_attribute_unused(),
									   uint64 now pg_attribute_unused())
{
	abort();
}

void __attribute__((weak))
cluster_update_trace_interval_leave_at(ClusterUpdateTraceInterval *interval pg_attribute_unused(),
									   bool finished pg_attribute_unused(),
									   uint64 now pg_attribute_unused())
{
	abort();
}

uint64 __attribute__((weak))
cluster_update_trace_phase_begin_at(int bucket pg_attribute_unused(),
									uint64 now pg_attribute_unused())
{
	abort();
}

void __attribute__((weak))
cluster_update_trace_phase_end_at(uint64 token pg_attribute_unused(),
								  uint64 now pg_attribute_unused())
{
	abort();
}

bool __attribute__((weak))
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel != DEBUG1)
		abort();
	return false;
}

bool __attribute__((weak))
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int __attribute__((weak))
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}

/* Native page helpers can report corruption. Standalone tests must provide
 * an explicit boundary to exercise it, never turn it into a warning/pass. */
int __attribute__((weak))
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}

int __attribute__((weak))
errcode(int sqlerrcode pg_attribute_unused())
{
	abort();
}

void __attribute__((weak))
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort();
}

/* Old default-non-SELF fixtures cannot manufacture command ownership.
 * The dedicated FULL/SELF test links the real CID module and publication edge. */
#include "access/htup.h"
#include "cluster/cluster_tt_local.h"
__attribute__((weak)) bool
cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	abort();
}
__attribute__((weak)) bool
HeapTupleHeaderCommandIdIsAvailable(HeapTupleHeader tuple)
{
	abort();
}
