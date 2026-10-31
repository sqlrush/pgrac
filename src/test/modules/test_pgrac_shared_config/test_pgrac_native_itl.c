/* Opt-in native TT/ITL copies. Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "fmgr.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_native_itl_trace.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_native_itl_trace);
PG_FUNCTION_INFO_V1(test_pgrac_native_itl_page);

static void check_reader(void)
{
	if (!superuser())
		ereport(ERROR, (errmsg("native ITL diagnostics require superuser")));
#ifndef USE_PGRAC_CLUSTER
	ereport(ERROR, (errmsg("native ITL diagnostics require --enable-cluster")));
#endif
}

#ifdef USE_PGRAC_CLUSTER
static void append_hex(StringInfo out, const void *data, Size size)
{
	const uint8 *bytes = data;
	static const char digits[] = "0123456789abcdef";
	for (Size i = 0; i < size; i++) {
		appendStringInfoChar(out, digits[bytes[i] >> 4]);
		appendStringInfoChar(out, digits[bytes[i] & 15]);
	}
}

static void append_event(StringInfo out, const ClusterNativeItlTraceEvent *r)
{
	const ClusterCtrcTxnKeyV1 *k = &r->key;
	appendStringInfo(out,
		"{\"sequence\":" UINT64_FORMAT ",\"kind\":%u,\"node\":%d,\"pid\":%d,"
		"\"procno\":%d,\"backend_id\":%d,\"backend_start\":\"" INT64_FORMAT "\","
		"\"process_incarnation\":\"" UINT64_FORMAT "\",\"authoritative\":false,"
		"\"key_known\":%s,\"key\":{\"format_version\":%u,\"owner_instance\":%u,"
		"\"origin_node_id\":%u,\"segment_id\":%u,\"segment_generation\":%u,"
		"\"slot_offset\":%u,\"slot_wrap\":%u,\"xid\":%u,\"cluster_epoch\":%u,"
		"\"system_identifier\":\"" UINT64_FORMAT "\",\"origin_boot_incarnation\":\"" UINT64_FORMAT "\","
		"\"formation_epoch\":\"" UINT64_FORMAT "\",\"admission_record_generation\":\"" UINT64_FORMAT "\","
		"\"root_descriptor_incarnation\":\"" UINT64_FORMAT "\",\"root_id\":\"" UINT64_FORMAT "\","
		"\"root_generation\":\"" UINT64_FORMAT "\"},\"key_bytes\":\"",
		r->sequence, r->kind, r->node, r->pid, r->procno, r->backend_id,
		r->backend_start, r->process_incarnation, r->key_known ? "true" : "false",
		k->format_version, k->owner_instance, k->origin_node_id, k->segment_id,
		k->segment_generation, k->slot_offset, k->slot_wrap, k->xid, k->cluster_epoch,
		k->system_identifier, k->origin_boot_incarnation, k->formation_epoch,
		k->admission_record_generation, k->root_descriptor_incarnation, k->root_id, k->root_generation);
	append_hex(out, k, sizeof(*k));
	appendStringInfo(out,
		"\",\"terminal_lsn\":\"" UINT64_FORMAT "\",\"active_sequence\":" UINT64_FORMAT
		",\"native_binding_known\":%s,\"native_binding\":{\"segment_id\":%u,"
		"\"segment_generation\":%u,\"xid\":%u,\"slot_offset\":%u,\"slot_wrap\":%u,"
		"\"origin_instance\":%u,\"publish_state\":%u,\"reserved16\":%u,"
		"\"active_lsn\":\"" UINT64_FORMAT "\"}",
		(uint64)r->terminal_lsn, r->active_sequence, r->native_binding_known ? "true" : "false",
		r->native_binding.segment_id, r->native_binding.segment_generation,
		r->native_binding.xid, r->native_binding.slot_offset, r->native_binding.slot_wrap,
		r->native_binding.origin_instance, r->native_binding.publish_state,
		r->native_binding.reserved16, (uint64)r->native_binding.active_lsn);
	appendStringInfo(out,
		",\"bind_lsn\":\"" UINT64_FORMAT "\",\"tt_slot\":{\"xid\":%u,\"wrap\":%u,"
		"\"status\":%u,\"flags\":%u,\"commit_scn\":\"" UINT64_FORMAT "\",\"first_undo\":\"",
		(uint64)r->bind_lsn, r->tt_slot.xid, r->tt_slot.wrap, r->tt_slot.status,
		r->tt_slot.flags, (uint64)r->tt_slot.commit_scn);
	append_hex(out, &r->tt_slot.first_undo_block, sizeof(UBA));
	appendStringInfo(out,
		"\"},\"page_known\":%s,\"tag\":[%u,%u,%u,%d,%u],"
		"\"page_lsn\":\"" UINT64_FORMAT "\",\"page_scn\":\"" UINT64_FORMAT "\","
		"\"recycle_watermark\":\"" UINT64_FORMAT "\",\"caller_xid\":%u,"
		"\"selected_slot\":%u,\"selection_succeeded\":%s,\"slots\":[",
		r->page_known ? "true" : "false", r->tag.spcOid, r->tag.dbOid, r->tag.relNumber,
		(int)r->tag.forkNum, r->tag.blockNum, (uint64)r->page_lsn, (uint64)r->page_scn,
		(uint64)r->recycle_watermark, r->caller_xid, r->selected_slot,
		r->selection_succeeded ? "true" : "false");
	for (unsigned i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		const ClusterItlSlotData *s = &r->slots[i];
		if (i) appendStringInfoChar(out, ',');
		appendStringInfo(out,
			"{\"index\":%u,\"xid\":%u,\"itl_wrap\":%u,\"flags\":%u,\"lock_count\":%u,"
			"\"commit_scn\":\"" UINT64_FORMAT "\",\"write_scn\":\"" UINT64_FORMAT "\","
			"\"first_change_lsn\":\"" UINT64_FORMAT "\",\"uba_bytes\":\"",
			i, s->xid, s->wrap, s->flags, s->lock_count, (uint64)s->commit_scn,
			(uint64)s->write_scn, (uint64)s->first_change_lsn);
		append_hex(out, &s->undo_segment_head, sizeof(UBA));
		appendStringInfoString(out, "\",\"tt_wrap_known\":false}");
	}
	appendStringInfo(out,
		"],\"update_retry\":{\"observed\":%s,\"phase\":%u,\"old_vm\":%d,\"new_vm\":%d,"
		"\"wait_deadline_us\":\"" UINT64_FORMAT "\",\"receipt_present\":%s,"
		"\"receipt_projection_only\":true,\"ready_known\":false,"
		"\"shared_retirement_known\":false,\"vm_reference_counts_known\":false,"
		"\"magic\":%u,\"record_type\":%u,\"owner_instance\":%u,\"payload_capacity\":%u,"
		"\"segment_id\":%u,\"slot_offset\":%u,\"actual_segment_id\":%u,"
		"\"reservation_sequence\":\"" UINT64_FORMAT "\",\"prepare_deadline_us\":\"" UINT64_FORMAT "\","
		"\"attempt_generation\":%u,\"pending_mask\":%u,\"prepared_mask\":%u,"
		"\"applied_mask\":%u,\"reuse_mask\":%u}}",
		r->retry.observed ? "true" : "false", r->retry.phase,
		r->retry.old_vm, r->retry.new_vm, r->retry.wait_deadline_us,
		r->retry.receipt_present ? "true" : "false", r->retry.magic, r->retry.record_type,
		r->retry.owner_instance, r->retry.payload_capacity, r->retry.segment_id,
		r->retry.slot_offset, r->retry.actual_segment_id, r->retry.reservation_sequence,
		r->retry.prepare_deadline_us, r->retry.attempt_generation, r->retry.pending_mask,
		r->retry.prepared_mask, r->retry.applied_mask, r->retry.reuse_mask);
}
#endif

Datum test_pgrac_native_itl_trace(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	StringInfoData out;
	ClusterNativeItlTracePrefix first, last;
	ClusterNativeItlTraceEvent row;
	bool comma = false;
	uint64 exported = 0;
	uint64 first_sequence = 0, last_sequence = 0;
#endif
	check_reader();
#ifdef USE_PGRAC_CLUSTER
	cluster_native_itl_trace_prefix(&first);
	initStringInfo(&out);
	appendStringInfoString(&out, "{\"version\":1,\"sequence_domain\":\"NATIVE_ITL\",\"atomic_graph\":false,\"events\":[");
	for (uint64 i = 0; i < Min(first.before, (uint64)CLUSTER_NATIVE_ITL_TRACE_CAPACITY); i++) {
		if (!cluster_native_itl_trace_read(i, &row)) continue;
		if (comma) appendStringInfoChar(&out, ',');
		if (exported == 0) first_sequence = row.sequence;
		last_sequence = row.sequence;
		append_event(&out, &row); comma = true; exported++;
	}
	cluster_native_itl_trace_prefix(&last);
	appendStringInfo(&out,
		"],\"available\":%s,\"enabled\":%s,\"capacity\":%u,\"first_seq\":" UINT64_FORMAT ","
		"\"last_seq\":" UINT64_FORMAT ",\"reserved_before\":" UINT64_FORMAT
		",\"reserved_after\":" UINT64_FORMAT ",\"exported\":" UINT64_FORMAT
		",\"dropped\":" UINT64_FORMAT ",\"overflow\":%s,\"export_complete\":%s}",
		last.available ? "true" : "false", last.enabled ? "true" : "false",
		CLUSTER_NATIVE_ITL_TRACE_CAPACITY, first_sequence, last_sequence,
		first.before, last.after, exported, last.dropped, last.overflow ? "true" : "false",
		first.complete && last.complete && first.before == last.after && exported == first.before
			? "true" : "false");
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
#else
	PG_RETURN_NULL();
#endif
}

Datum test_pgrac_native_itl_page(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	Oid relid = PG_GETARG_OID(0);
	int32 block = PG_GETARG_INT32(1);
	Relation relation;
	Buffer buffer;
	BufferTag tag;
	PGAlignedBlock image;
	ClusterNativeItlTraceEvent row = {0};
	StringInfoData out;
	uint64 sequence = 0;
	bool stored;
#endif
	check_reader();
#ifdef USE_PGRAC_CLUSTER
	if (!cluster_trace_tx_identity || block < 0)
		ereport(ERROR, (errmsg("native ITL page requires trace_tx_identity and a nonnegative block")));
	relation = table_open(relid, AccessShareLock);
	if (relation->rd_rel->relkind != RELKIND_RELATION ||
		relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR, (errmsg("native ITL page requires a permanent heap")));
	buffer = ReadBuffer(relation, (BlockNumber)block);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	tag = GetBufferDescriptor(buffer - 1)->tag;
	memcpy(image.data, BufferGetPage(buffer), BLCKSZ);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
	table_close(relation, AccessShareLock);
	row.kind = CLNITL_PAGE_REREAD;
	(void)cluster_native_itl_trace_capture_page(&tag, image.data, &row);
	stored = cluster_native_itl_trace_append(&row, &sequence);
	initStringInfo(&out);
	appendStringInfo(&out, "{\"stored\":%s,\"sequence\":" UINT64_FORMAT
		",\"page_known\":%s,\"image_scope\":\"native_buffer_owned_copy\",\"page_bytes\":\"",
		stored ? "true" : "false", sequence, row.page_known ? "true" : "false");
	append_hex(&out, image.data, BLCKSZ);
	appendStringInfoString(&out, "\"}");
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
#else
	PG_RETURN_NULL();
#endif
}
