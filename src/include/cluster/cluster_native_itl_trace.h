/* Native TT/ITL observations. Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_NATIVE_ITL_TRACE_H
#define CLUSTER_NATIVE_ITL_TRACE_H

#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_local.h"
#include "storage/buf_internals.h"

#define CLUSTER_NATIVE_ITL_TRACE_CAPACITY 4096

typedef enum ClusterNativeItlTraceKind {
	CLNITL_TT_ADMITTED = 1,
	CLNITL_TT_ACTIVE_PUBLISHED,
	CLNITL_SLOT_SELECTED,
	CLNITL_PAGE_REREAD,
	CLNITL_TT_COMMITTED,
	CLNITL_TT_ABORTED
} ClusterNativeItlTraceKind;

typedef struct ClusterNativeItlTraceEvent {
	uint64 sequence;
	int32 node, pid, procno, backend_id;
	int64 backend_start;
	uint64 process_incarnation;
	uint32 kind;
	bool key_known, page_known, selection_succeeded;
	ClusterCtrcTxnKeyV1 key;
	TTSlot tt_slot;
	XLogRecPtr bind_lsn;
	XLogRecPtr terminal_lsn;
	uint64 active_sequence;
	bool native_binding_known;
	ClusterCanonicalTxnBinding native_binding;
	BufferTag tag;
	XLogRecPtr page_lsn;
	SCN page_scn, recycle_watermark;
	TransactionId caller_xid;
	uint8 selected_slot;
	ClusterItlSlotData slots[CLUSTER_ITL_INITRANS_DEFAULT];
} ClusterNativeItlTraceEvent;

typedef struct ClusterNativeItlTracePrefix {
	bool available, enabled, overflow, complete;
	uint64 before, after, published, dropped;
} ClusterNativeItlTracePrefix;

extern void cluster_native_itl_trace_shmem_register(void);
extern bool cluster_native_itl_trace_append(const ClusterNativeItlTraceEvent *row,
										  uint64 *sequence);
extern bool cluster_native_itl_trace_read(uint64 index, ClusterNativeItlTraceEvent *out);
extern void cluster_native_itl_trace_prefix(ClusterNativeItlTracePrefix *out);
extern bool cluster_native_itl_trace_capture_page(const BufferTag *tag, Page page,
												ClusterNativeItlTraceEvent *out);
#ifndef USE_CLUSTER_UNIT
extern void cluster_native_itl_trace_tt(ClusterNativeItlTraceKind kind,
	const ClusterCtrcTxnKeyV1 *key, const TTSlot *slot, XLogRecPtr bind_lsn);
extern void cluster_native_itl_trace_selected(Buffer buffer, TransactionId xid,
	uint8 selected, bool succeeded);
extern void cluster_native_itl_trace_terminal(ClusterNativeItlTraceKind kind,
	uint8 owner, uint32 segment, uint32 generation, uint16 offset,
	const TTSlot *successor, XLogRecPtr terminal_lsn,
	const ClusterCanonicalTxnBinding *binding);
#else
/* Standalone authority fixtures have no diagnostic shared-memory region. */
static inline void cluster_native_itl_trace_tt(ClusterNativeItlTraceKind kind,
	const ClusterCtrcTxnKeyV1 *key, const TTSlot *slot, XLogRecPtr lsn)
{ (void)kind; (void)key; (void)slot; (void)lsn; }
static inline void cluster_native_itl_trace_selected(Buffer b, TransactionId xid, uint8 s, bool ok)
{ (void)b; (void)xid; (void)s; (void)ok; }
static inline void cluster_native_itl_trace_terminal(ClusterNativeItlTraceKind kind,
	uint8 owner, uint32 segment, uint32 generation, uint16 offset,
	const TTSlot *slot, XLogRecPtr lsn, const ClusterCanonicalTxnBinding *binding)
{ (void)kind; (void)owner; (void)segment; (void)generation; (void)offset;
  (void)slot; (void)lsn; (void)binding; }
#endif

#endif
