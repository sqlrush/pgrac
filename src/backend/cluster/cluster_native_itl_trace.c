/* Native TT/ITL observations. Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "cluster/cluster_native_itl_trace.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/shmem.h"

typedef struct NativeItlTraceRecord {
	pg_atomic_uint32 published;
	ClusterNativeItlTraceEvent row;
} NativeItlTraceRecord;
typedef struct NativeItlTraceShared {
	pg_atomic_uint64 next, dropped;
	pg_atomic_uint32 overflow;
	NativeItlTraceRecord records[CLUSTER_NATIVE_ITL_TRACE_CAPACITY];
} NativeItlTraceShared;
static NativeItlTraceShared *native_itl_trace;

static Size native_itl_trace_size(void)
{ return IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0 ? 0 : MAXALIGN(sizeof(NativeItlTraceShared)); }
static void native_itl_trace_init(void)
{
	bool found; Size size = native_itl_trace_size();
	if (!size) return;
	native_itl_trace = ShmemInitStruct("pgrac native itl trace", size, &found);
	if (!found) {
		memset(native_itl_trace, 0, size);
		pg_atomic_init_u64(&native_itl_trace->next, 0);
		pg_atomic_init_u64(&native_itl_trace->dropped, 0);
		pg_atomic_init_u32(&native_itl_trace->overflow, 0);
		for (unsigned i = 0; i < CLUSTER_NATIVE_ITL_TRACE_CAPACITY; i++)
			pg_atomic_init_u32(&native_itl_trace->records[i].published, 0);
	}
}
void cluster_native_itl_trace_shmem_register(void)
{
	static const ClusterShmemRegion region = {
		.name = "pgrac native itl trace", .size_fn = native_itl_trace_size,
		.init_fn = native_itl_trace_init, .owner_subsys = "native_itl_trace"
	};
	cluster_shmem_register_region(&region);
}
static void
native_itl_trace_drop(void)
{
	/* overflow stays set even if the diagnostic dropped counter wraps. */
	pg_atomic_write_u32(&native_itl_trace->overflow, 1);
	pg_atomic_fetch_add_u64(&native_itl_trace->dropped, 1);
}

bool
cluster_native_itl_trace_append(const ClusterNativeItlTraceEvent *row, uint64 *sequence)
{
	uint64 index;
	NativeItlTraceRecord *record;

	if (!cluster_trace_tx_identity || native_itl_trace == NULL || row == NULL)
		return false;
	if (MyProc == NULL || MyProcPid <= 0 || MyStartTimestamp <= 0 ||
		MyProc->cluster_grd_generation == 0 ||
		pg_atomic_read_u64(&native_itl_trace->next) >= CLUSTER_NATIVE_ITL_TRACE_CAPACITY) {
		native_itl_trace_drop();
		return false;
	}
	/* Once full, later calls do not advance next; in-flight reservations can
	 * exceed capacity only by the bounded number of concurrent producers. */
	index = pg_atomic_fetch_add_u64(&native_itl_trace->next, 1);
	if (index >= CLUSTER_NATIVE_ITL_TRACE_CAPACITY) {
		native_itl_trace_drop();
		return false;
	}
	record = &native_itl_trace->records[index];
	record->row = *row;
	record->row.sequence = index + 1;
	record->row.node = cluster_node_id;
	record->row.pid = MyProcPid;
	record->row.procno = MyProc->pgprocno;
	record->row.backend_id = MyProc->backendId;
	record->row.backend_start = MyStartTimestamp;
	record->row.process_incarnation = MyProc->cluster_grd_generation;
	pg_write_barrier();
	pg_atomic_write_u32(&record->published, 1);
	if (sequence != NULL)
		*sequence = index + 1;
	return true;
}

bool
cluster_native_itl_trace_read(uint64 index, ClusterNativeItlTraceEvent *out)
{
	NativeItlTraceRecord *record;
	if (native_itl_trace == NULL || out == NULL || index >= CLUSTER_NATIVE_ITL_TRACE_CAPACITY)
		return false;
	record = &native_itl_trace->records[index];
	if (pg_atomic_read_u32(&record->published) != 1)
		return false;
	pg_read_barrier();
	*out = record->row;
	return true;
}

void
cluster_native_itl_trace_prefix(ClusterNativeItlTracePrefix *out)
{
	uint64 limit;
	memset(out, 0, sizeof(*out));
	out->enabled = cluster_trace_tx_identity;
	if (native_itl_trace == NULL)
		return;
	out->available = true;
	out->before = pg_atomic_read_u64(&native_itl_trace->next);
	limit = Min(out->before, (uint64)CLUSTER_NATIVE_ITL_TRACE_CAPACITY);
	for (uint64 i = 0; i < limit; i++)
		out->published += pg_atomic_read_u32(&native_itl_trace->records[i].published) == 1;
	out->dropped = pg_atomic_read_u64(&native_itl_trace->dropped);
	out->overflow = pg_atomic_read_u32(&native_itl_trace->overflow) != 0;
	out->after = pg_atomic_read_u64(&native_itl_trace->next);
	out->complete = out->enabled && !out->overflow && out->before == out->after &&
		out->published == out->before && out->dropped == 0;
}

bool
cluster_native_itl_trace_capture_page(const BufferTag *tag, Page page, ClusterNativeItlTraceEvent *out)
{
	const PageHeaderData *header = (const PageHeaderData *)page;
	if (out == NULL)
		return false;
	out->page_known = false;
	memset(&out->tag, 0, sizeof(out->tag));
	memset(out->slots, 0, sizeof(out->slots));
	out->page_lsn = out->page_scn = out->recycle_watermark = 0;
	/* No throwing decoder, allocation, TT lookup, or new lock on the owned
	 * page. Raw UBA and ITL wrap stay distinct from the TT key's slot wrap. */
	if (tag == NULL || page == NULL || header->pd_lower < SizeOfPageHeaderData ||
		header->pd_lower > header->pd_upper || header->pd_upper > header->pd_special ||
		header->pd_special > BLCKSZ || !PageHasItl(page) ||
		PageGetSpecialSize(page) < CLUSTER_ITL_SPECIAL_SIZE)
		return false;
	out->tag = *tag;
	out->page_lsn = PageGetLSN(page);
	out->page_scn = header->pd_block_scn;
	out->recycle_watermark = ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn;
	memcpy(out->slots, ClusterPageGetItlSlots(page), sizeof(out->slots));
	out->page_known = true;
	return true;
}

void
cluster_native_itl_trace_tt(ClusterNativeItlTraceKind kind,
	const ClusterCtrcTxnKeyV1 *key, const TTSlot *slot, XLogRecPtr lsn)
{
	ClusterNativeItlTraceEvent row = {0};
	if (!cluster_trace_tx_identity)
		return;
	row.kind = kind;
	if (key != NULL && slot != NULL) {
		row.key_known = true;
		row.key = *key;
		row.tt_slot = *slot;
		row.bind_lsn = lsn;
	}
	(void)cluster_native_itl_trace_append(&row, NULL);
}

void
cluster_native_itl_trace_selected(Buffer buffer, TransactionId xid, uint8 selected, bool ok)
{
	ClusterNativeItlTraceEvent row = {0};
	if (!cluster_trace_tx_identity)
		return;
	row.kind = CLNITL_SLOT_SELECTED;
	row.caller_xid = xid;
	row.selected_slot = ok ? selected : UINT8_MAX;
	row.selection_succeeded = ok;
	if (buffer > 0)
		(void)cluster_native_itl_trace_capture_page(
			&GetBufferDescriptor(buffer - 1)->tag, BufferGetPage(buffer), &row);
	(void)cluster_native_itl_trace_append(&row, NULL);
}
