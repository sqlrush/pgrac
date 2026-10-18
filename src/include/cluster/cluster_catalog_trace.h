/* Relation-scoped, passive catalog diagnostics. Zero disables tracing. */
#ifndef CLUSTER_CATALOG_TRACE_H
#define CLUSTER_CATALOG_TRACE_H
#include "access/htup.h"
#include "utils/snapshot.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_itl_slot.h"
#include "utils/rel.h"
extern uint64 cluster_catalog_trace_enter(HeapTuple tuple);
extern void cluster_catalog_trace_begin(HeapTuple tuple, Snapshot snapshot);
extern void cluster_catalog_trace_leave(uint64 previous);
extern void cluster_catalog_trace_header(const char *stage, HeapTupleHeader tuple,
										 Snapshot snapshot);
extern void cluster_catalog_trace_slot(const char *stage, const ClusterItlSlotData *slot);
extern void cluster_catalog_trace_event(const char *stage, const char *branch, int64 value);
extern int cluster_catalog_trace_result(const char *function, int line, int value);
extern CommandId cluster_catalog_trace_cid(const char *kind, CommandId value);
extern void cluster_catalog_trace_catalog(const char *stage, Oid relation, HeapTuple tuple,
										  CommandId curcid);
extern void cluster_catalog_trace_descriptor(const char *stage, Relation relation, int attnum,
											 CommandId curcid);
extern void cluster_catalog_trace_type(Oid relation, Oid target, int32 typmod, Oid source,
									   CommandId curcid);
#define CLUSTER_CATALOG_TRACE_RETURN(value)                                                        \
	return (unlikely(cluster_catalog_trace_relation != 0)                                          \
				? cluster_catalog_trace_result(__func__, __LINE__, (value))                        \
				: (value))
#define CLUSTER_CATALOG_TRACE_CID(kind, value)                                                     \
	(unlikely(cluster_catalog_trace_relation != 0) ? cluster_catalog_trace_cid((kind), (value))    \
												   : (value))
#else
#define CLUSTER_CATALOG_TRACE_RETURN(value) return (value)
#define CLUSTER_CATALOG_TRACE_CID(kind, value) (value)
#endif
#endif
