/* Opt-in native diagnostic copies. Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_content_trace.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_wait_graph.h"
#include "storage/proc.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_wait_graph);
PG_FUNCTION_INFO_V1(test_pgrac_wait_graph_prepare);
PG_FUNCTION_INFO_V1(test_pgrac_wait_graph_enqueue);
PG_FUNCTION_INFO_V1(test_pgrac_wait_graph_block);

static void
check_reader(void)
{
	if (!superuser())
		ereport(ERROR, (errmsg("native wait graph requires superuser")));
#ifndef USE_PGRAC_CLUSTER
	ereport(ERROR, (errmsg("native wait graph requires --enable-cluster")));
#endif
}

#ifdef USE_PGRAC_CLUSTER
static void
json_tag(StringInfo out, const BufferTag *tag)
{
	appendStringInfo(out, "[%u,%u,%u,%d,%u]", tag->spcOid, tag->dbOid,
		tag->relNumber, tag->forkNum, tag->blockNum);
}

static void
json_lock(StringInfo out, const ClusterContentTraceLock *lock)
{
	appendStringInfo(out, "{\"buffer\":%d,\"mode\":%d,\"tag\":", lock->buffer, lock->mode);
	if (lock->tag_known) json_tag(out, &lock->tag);
	else appendStringInfoString(out, "null");
	appendStringInfoChar(out, '}');
}

static void
json_wait(StringInfo out, const ClusterWaitTrace *row)
{
	appendStringInfoString(out, "{\"tag\":");
	if (row->tag_known) json_tag(out, &row->tag);
	else appendStringInfoString(out, "null");
#define WG_U(f) appendStringInfo(out, ",\"" #f "\":" UINT64_FORMAT, (uint64)row->f)
#define WG_I(f) appendStringInfo(out, ",\"" #f "\":%d", row->f)
#define WG_B(f) appendStringInfo(out, ",\"" #f "\":%s", row->f ? "true" : "false")
	WG_U(wait_seq); WG_U(request_seq); WG_U(binding); WG_U(formation);
	WG_U(master_session); WG_U(r4); WG_U(acquisition); WG_U(base); WG_U(authority);
	WG_U(pending_cookie); WG_U(progress);
	WG_U(snapshot); WG_U(own_generation); WG_U(reservation);
	WG_U(writer_activation); WG_U(rx_activation); WG_U(master_request); WG_U(master_head_request);
	WG_U(head); WG_U(enqueue_order); WG_U(requester_connection); WG_U(ingress_connection);
	WG_U(wire_sender_connection); WG_U(holder_node); WG_U(holder_proc);
	WG_U(holder_epoch); WG_U(holder_request); WG_U(s_holders); WG_U(pi_holders);
	WG_U(incompatible); WG_U(blocked); WG_U(round_request); WG_U(round_formation);
	WG_U(round_session); WG_U(round_r4); WG_U(round_base); WG_U(head_request);
	WG_U(head_formation); WG_U(head_session); WG_U(head_base); WG_U(head_change);
	WG_U(round_requester_connection); WG_U(round_ingress_connection);
	WG_U(head_requester_connection); WG_U(head_ingress_connection); WG_U(wait_head_change);
	WG_U(entry_formation); WG_U(entry_acquisition); WG_U(entry_base);
	WG_U(continuation_pending); WG_U(continuation_expected_x); WG_U(continuation_reservation);
	WG_U(continuation_claim_pending); WG_U(continuation_claim_reservation); WG_U(refusal_token);
	WG_U(buffer_state); WG_U(own_flags); WG_U(refusal_flags); WG_U(refusal_pins); WG_U(tx_xid);
	WG_U(pins_seen); WG_U(ledger_seen); WG_U(local_pins); WG_U(rows); WG_U(omitted);
	WG_I(site); WG_I(mode); WG_I(expected_master); WG_I(requester); WG_I(source);
	WG_I(queue_kind); WG_I(state); WG_I(x_holder); WG_I(pending_requester);
	WG_I(phase); WG_I(round_phase); WG_I(owner_phase); WG_I(round_requester);
	WG_I(head_requester); WG_I(round_master); WG_I(guard); WG_I(guard_result);
	WG_I(refusal_reason); WG_I(blocking_instance); WG_I(blocking_session); WG_I(blocking_pid);
	WG_I(final_blocking_instance); WG_I(final_blocking_session); WG_I(final_blocking_pid);
	WG_I(master_holder_instance); WG_I(master_holder_session); WG_I(master_holder_pid);
	WG_I(buffer); WG_I(private_pins); WG_I(shared_pins); WG_I(ledger_phase); WG_I(own_state); WG_I(buffer_type);
	WG_B(entry_known); WG_B(request_known); WG_B(master_view);
	WG_B(content_held); WG_B(refusal_io); WG_B(transport_known); WG_B(blocking_known);
	WG_B(final_blocking_known); WG_B(master_holder_known); WG_B(master_head_known); WG_B(master_request_head_known);
	WG_B(round_request_known); WG_B(head_request_known);
	WG_B(caller_matches_round); WG_B(caller_matches_head); WG_B(continuation_known);
#undef WG_U
#undef WG_I
#undef WG_B
	appendStringInfoChar(out, '}');
}

typedef struct WaitGraphOwnerWriter {
	StringInfo out;
	uint32 count;
} WaitGraphOwnerWriter;

static void
json_owner(const ClusterContentTraceOwner *row, void *context)
{
	WaitGraphOwnerWriter *writer = context;
	StringInfo out = writer->out;
	if (writer->count++ != 0) appendStringInfoChar(out, ',');
	appendStringInfo(out, "{\"node\":%d,\"pid\":%d,\"procno\":%u,\"backend_id\":%d,"
		"\"backend_start\":" INT64_FORMAT ",\"revision\":" UINT64_FORMAT
		",\"omitted_locks\":%u,\"locks\":[", cluster_node_id, row->pid, row->procno,
		row->backend_id, row->backend_start, row->revision, row->omitted_locks);
	for (uint32 i = 0; i < row->nlocks; i++) {
		if (i != 0) appendStringInfoChar(out, ',');
		json_lock(out, &row->locks[i]);
	}
	appendStringInfoString(out, "],\"native_wait\":");
	if (row->native_waiting) json_lock(out, &row->native_wait);
	else appendStringInfoString(out, "null");
	appendStringInfoString(out, ",\"enqueue\":");
	if (row->enqueue_waiting) {
		const ClusterContentTraceEnqueue *wait = &row->enqueue;
		appendStringInfo(out, "{\"resid\":[%u,%u,%u,%u,%u,%u],\"mode\":%d,"
			"\"current_mode\":%d,\"convert\":%s,\"holder\":[%u,%u," UINT64_FORMAT
			"," UINT64_FORMAT "],\"request\":" UINT64_FORMAT ",\"epoch\":" UINT64_FORMAT
			",\"wait_seq\":" UINT64_FORMAT "}", wait->field1, wait->field2, wait->field3,
			wait->field4, wait->type, wait->lockmethodid, wait->mode, wait->current_mode,
			wait->convert ? "true" : "false", wait->holder_node, wait->holder_procno,
			wait->holder_epoch, wait->holder_request, wait->request, wait->epoch, wait->wait_seq);
	} else appendStringInfoString(out, "null");
	appendStringInfoString(out, ",\"pcm_wait\":");
	if (row->wait_mask & (1U << CLCONTENT_PCM)) json_wait(out, &row->waits[CLCONTENT_PCM]);
	else appendStringInfoString(out, "null");
	appendStringInfoString(out, ",\"tx_wait\":");
	if (row->wait_mask & (1U << CLCONTENT_TX)) json_wait(out, &row->waits[CLCONTENT_TX]);
	else appendStringInfoString(out, "null");
	appendStringInfoChar(out, '}');
}

static void
json_ko(StringInfo out, const ClusterWaitGraphKoSnapshot *snapshot)
{
	appendStringInfo(out, "{\"status\":%d,\"omitted\":%u,\"rows\":[",
		snapshot->status, snapshot->omitted);
	for (uint32 i = 0; i < snapshot->nrows; i++) {
		const ClusterWaitGraphKoRow *row = &snapshot->rows[i];
		if (i != 0) appendStringInfoChar(out, ',');
		appendStringInfo(out, "{\"kind\":%u,\"slot\":%u,\"pid\":%d,\"executor\":%d,"
			"\"serial\":" UINT64_FORMAT ",\"epoch\":" UINT64_FORMAT ",\"locator\":[%u,%u,%u],"
			"\"fork\":%d,\"failed\":%s,\"complete\":%s,\"observed\":%s,\"reserved\":%s,"
			"\"invalid\":%s,\"raw_copy\":%s,\"buffer_copy\":%s,\"cursor\":%u,"
			"\"first\":%u,\"count\":%u,\"lsn\":" UINT64_FORMAT ",\"sent\":%u,"
			"\"done\":%u,\"uncertain\":%u,\"failed_peers\":%u}", row->kind, row->slot,
			row->pid, row->executor, row->serial, row->epoch, row->locator.spcOid,
			row->locator.dbOid, row->locator.relNumber, row->fork, row->failed ? "true" : "false",
			row->complete ? "true" : "false", row->observed ? "true" : "false",
			row->reserved ? "true" : "false", row->invalid ? "true" : "false",
			row->raw_copy ? "true" : "false", row->buffer_copy ? "true" : "false",
			row->cursor, row->first, row->count, row->lsn, row->sent, row->done,
			row->uncertain, row->failed_peers);
	}
	appendStringInfoString(out, "]}");
}

static uint32
key_argument(FunctionCallInfo fcinfo, int index, uint64 maximum)
{
	int64 value = PG_GETARG_INT64(index);
	if (value < 0 || (uint64)value > maximum)
		ereport(ERROR, (errmsg("wait graph resource key is out of range")));
	return (uint32)value;
}
#endif

Datum
test_pgrac_wait_graph(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	StringInfoData out;
	WaitGraphOwnerWriter writer = {.out = &out};
	ClusterContentTraceWalkStats stats;
	ClusterWaitGraphKoSnapshot *ko;
	uint64 epoch;
	bool first = true;
	check_reader();
	ko = palloc0(sizeof(*ko));
	epoch = cluster_epoch_get_current();
	initStringInfo(&out);
	appendStringInfo(&out, "{\"version\":1,\"node\":%d,\"epoch_begin\":" UINT64_FORMAT
		",\"atomic_graph\":false,\"observed_at\":" INT64_FORMAT ",\"owners\":[",
		cluster_node_id, epoch, GetCurrentTimestamp());
	cluster_content_trace_walk(json_owner, &writer, &stats);
	appendStringInfo(&out, "],\"stats\":{\"available\":%s,\"scanned\":%u,\"busy\":%u,"
		"\"changed\":%u,\"dead\":%u,\"owners\":%u,\"omitted_locks\":%u},\"procs\":[",
		stats.available ? "true" : "false", stats.scanned, stats.busy, stats.changed,
		stats.dead, stats.owners, stats.omitted_locks);
	if (ProcGlobal != NULL) {
		for (uint32 i = 0; i < ProcGlobal->allProcCount; i++) {
			const PGPROC *proc = &ProcGlobal->allProcs[i];
			int pid = proc->pid;
			BackendId backend = proc->backendId;
			uint32 wait = *(volatile const uint32 *)&proc->wait_event_info;
			bool stable;
			const char *wait_name = pgstat_get_wait_event(wait);
			if (pid <= 0) continue;
			pg_read_barrier();
			stable = pid == proc->pid && backend == proc->backendId;
			if (!first) appendStringInfoChar(&out, ',');
			first = false;
			appendStringInfo(&out, "{\"procno\":%u,\"pid\":%d,\"backend_id\":%d,"
				"\"wait_event_info\":%u,\"wait_name\":", i, pid, backend, wait);
			escape_json(&out, wait_name != NULL ? wait_name : "");
			appendStringInfo(&out, ",\"stable\":%s}", stable ? "true" : "false");
		}
	}
	cluster_ko_wait_graph_snapshot(ko);
	appendStringInfoString(&out, "],\"ko\":"); json_ko(&out, ko);
	appendStringInfo(&out, ",\"epoch_end\":" UINT64_FORMAT "}", cluster_epoch_get_current());
	pfree(ko);
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
#else
	check_reader();
	PG_RETURN_NULL();
#endif
}

Datum
test_pgrac_wait_graph_enqueue(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	StringInfoData out;
	ClusterResId key;
	ClusterGrdQueueSnapshot snapshot;
	ClusterGrdQueueMemberSnapshot *members;
	ClusterGrdQueueObservationStatus status;
	check_reader();
	members = palloc0(1024 * sizeof(*members));
	key.field1 = key_argument(fcinfo, 0, UINT32_MAX);
	key.field2 = key_argument(fcinfo, 1, UINT32_MAX);
	key.field3 = key_argument(fcinfo, 2, UINT32_MAX);
	key.field4 = key_argument(fcinfo, 3, UINT16_MAX);
	key.type = key_argument(fcinfo, 4, UINT8_MAX);
	key.lockmethodid = key_argument(fcinfo, 5, UINT8_MAX);
	status = cluster_grd_queue_try_snapshot(&key, &snapshot, members, 1024);
	initStringInfo(&out);
	appendStringInfo(&out, "{\"kind\":\"enqueue\",\"node\":%d,\"epoch\":" UINT64_FORMAT
		",\"resid\":[%u,%u,%u,%u,%u,%u],\"status\":%d,\"generation\":" UINT64_FORMAT
		",\"last_modified_scn\":" UINT64_FORMAT
		",\"copied\":%d,\"required\":%d,\"members\":[", cluster_node_id,
		cluster_epoch_get_current(), key.field1, key.field2, key.field3, key.field4,
		key.type, key.lockmethodid, status, snapshot.generation, snapshot.last_modified_scn, snapshot.copied, snapshot.required);
	for (int i = 0; i < snapshot.copied; i++) {
		const ClusterGrdQueueMemberSnapshot *row = &members[i];
		if (i != 0) appendStringInfoChar(&out, ',');
		appendStringInfo(&out, "{\"kind\":%d,\"ordinal\":%d,\"id\":[%u,%u,"
			UINT64_FORMAT "," UINT64_FORMAT "],\"mode\":%d,\"current_mode\":%d,"
			"\"source_node\":%d,\"lock_group\":%u,\"wait_seq\":" UINT64_FORMAT
			",\"master_generation\":" UINT64_FORMAT ",\"opcode\":%u,\"fair_queue_seq\":" UINT64_FORMAT "}",
			row->kind, row->ordinal, row->id.node_id, row->id.procno, row->id.cluster_epoch,
			row->id.request_id, row->mode, row->current_mode, row->source_node_id,
			row->lock_group_procno_plus_one, row->wait_seq, row->shard_master_generation,
			row->request_opcode, row->fair_queue_seq);
	}
	appendStringInfoString(&out, "]}"); pfree(members);
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
#else
	check_reader();
	PG_RETURN_NULL();
#endif
}

Datum
test_pgrac_wait_graph_block(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	StringInfoData out;
	BufferTag tag;
	ClusterWaitGraphPcmSnapshot *snapshot;
	check_reader();
	snapshot = palloc0(sizeof(*snapshot));
	tag.spcOid = key_argument(fcinfo, 0, UINT32_MAX);
	tag.dbOid = key_argument(fcinfo, 1, UINT32_MAX);
	tag.relNumber = key_argument(fcinfo, 2, UINT32_MAX);
	tag.forkNum = key_argument(fcinfo, 3, MAX_FORKNUM);
	tag.blockNum = key_argument(fcinfo, 4, MaxBlockNumber);
	cluster_pcm_wait_graph_snapshot(&tag, snapshot);
	initStringInfo(&out);
	appendStringInfo(&out, "{\"kind\":\"pcm\",\"node\":%d,\"epoch\":" UINT64_FORMAT
		",\"status\":%d,\"view\":", cluster_node_id, cluster_epoch_get_current(), snapshot->status);
	json_wait(&out, &snapshot->view);
	appendStringInfoString(&out, ",\"queue\":[");
	for (uint32 i = 0; i < snapshot->nqueued; i++) {
		if (i != 0) appendStringInfoChar(&out, ',');
		json_wait(&out, &snapshot->queue[i]);
	}
	appendStringInfoString(&out, "]}"); pfree(snapshot);
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
#else
	check_reader();
	PG_RETURN_NULL();
#endif
}

/* Pre-workload only: attach this backend's native GRD DSA mapping. The hang
 * readers themselves refuse an unprepared mapping rather than attach/wait. */
Datum
test_pgrac_wait_graph_prepare(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ClusterResId key = {0};
	ClusterGrdQueueSnapshot snapshot;
	ClusterGrdQueueObservationStatus status;
	check_reader();
	key.type = UINT8_MAX;
	key.lockmethodid = UINT8_MAX;
	status = cluster_grd_queue_snapshot(&key, &snapshot, NULL, 0);
	PG_RETURN_BOOL(status != CLUSTER_GRD_QUEUE_UNAVAILABLE && status != CLUSTER_GRD_QUEUE_INVALID);
#else
	check_reader();
	PG_RETURN_NULL();
#endif
}
