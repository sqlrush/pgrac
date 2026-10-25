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
#include "cluster/cluster_conf.h"
#include "cluster/cluster_content_trace.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_wait_graph.h"
#include "common/hashfn.h"
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
typedef struct WaitGraphRoute {
	ClusterConfDeclaredNodes topology;
	uint64 epoch;
	uint64 membership_generation;
	uint64 dead_generation;
	int32 master;
	bool known;
} WaitGraphRoute;

/* Diagnostic healthy static-route subset only. No normal lookup/counter or
 * cache mutation, no recovery routing, no admission and no blocking wait. */
static void
wait_graph_route_snapshot(const BufferTag *tag, WaitGraphRoute *out)
{
	WaitGraphRoute cut = {0};
	ClusterConfDeclaredNodes after;
	bool busy;
	bool self_declared = false;

	memset(out, 0, sizeof(*out));
	out->master = -1;
	if (tag == NULL || !cluster_shared_config ||
		cluster_cssd_get_status_nowait(&busy) != CLUSTER_CSSD_READY || busy)
		return;
	cut.epoch = cluster_epoch_get_current();
	cut.membership_generation = cluster_membership_cut_generation();
	cut.dead_generation = cluster_cssd_get_dead_generation();
	if (cut.epoch == 0 || cut.membership_generation == 0 ||
		(cut.membership_generation & 1) != 0 ||
		!cluster_conf_declared_nodes_observe(&cut.topology) || !cut.topology.known ||
		cut.topology.count == 0 || cut.topology.count > CLUSTER_MAX_NODES)
		return;
	for (uint32 i = 0; i < cut.topology.count; i++) {
		int32 node = cut.topology.nodes[i];
		if (node < 0 || node >= CLUSTER_MAX_NODES ||
			(i != 0 && node <= cut.topology.nodes[i - 1]) ||
			cluster_cssd_get_peer_state(node) != CLUSTER_CSSD_PEER_ALIVE ||
			!cluster_membership_is_member(node))
			return;
		self_declared |= node == cluster_node_id;
	}
	if (cut.topology.count == 1 && !self_declared)
		return;
	cut.master = cut.topology.count == 1 ? cluster_node_id :
		cut.topology.nodes[hash_bytes((const unsigned char *)tag, sizeof(*tag)) % cut.topology.count];
	for (uint32 i = 0; i < cut.topology.count; i++)
		if (cluster_cssd_get_peer_state(cut.topology.nodes[i]) != CLUSTER_CSSD_PEER_ALIVE ||
			!cluster_membership_is_member(cut.topology.nodes[i]))
			return;
	if (!cluster_conf_declared_nodes_observe(&after) || !after.known ||
		after.count != cut.topology.count ||
		memcmp(after.nodes, cut.topology.nodes, after.count * sizeof(after.nodes[0])) != 0 ||
		!cluster_membership_cut_generation_current(cut.membership_generation) ||
		cluster_cssd_get_dead_generation() != cut.dead_generation ||
		cluster_epoch_get_current() != cut.epoch ||
		cluster_cssd_get_status_nowait(&busy) != CLUSTER_CSSD_READY || busy)
		return;
	cut.known = true;
	*out = cut;
}

typedef struct WaitGraphContext {
	ClusterSemanticWaitGraphSnapshot semantic;
	ResourceXGateSnapshot gate;
	ClusterSfPeerCap peers[CLUSTER_MAX_NODES];
	ClusterICTerminalPeerSessions streams[CLUSTER_MAX_NODES];
	bool stream_known[CLUSTER_MAX_NODES];
	uint64 stream_epoch;
	uint32 local_capability;
	int stream_channels;
	bool streams_known;
	bool gate_known;
	bool peers_known, peers_busy;
} WaitGraphContext;

static void
wait_graph_context_snapshot(WaitGraphContext *out)
{
	ClusterSfPeerCap after[CLUSTER_MAX_NODES];
	bool busy;

	memset(out, 0, sizeof(*out));
	cluster_semantic_activation_wait_graph_snapshot(&out->semantic);
	out->gate_known = cluster_pcm_lock_resource_x_gate_snapshot(&out->gate);
	out->peers_known = cluster_sf_peer_capability_table_try_snapshot(
		out->peers, CLUSTER_MAX_NODES, &out->peers_busy);
	out->stream_epoch = cluster_epoch_get_current();
	out->stream_channels = cluster_lms_workers;
	out->local_capability = cluster_ic_local_capability_word();
	if (!out->peers_known || out->peers_busy || out->stream_epoch == 0 ||
		out->stream_epoch == UINT64_MAX || out->stream_channels < 1 ||
		out->stream_channels > CLUSTER_IC_TIER1_DATA_CHANNELS)
		return;
	/* Reuse the native bounded, read-only CONTROL/all-DATA cut. A current
	 * stream is not a receipt witness for an earlier BLOCK frame. */
	for (uint32 i = 0; i < CLUSTER_MAX_NODES; i++)
		if (i != cluster_node_id && out->peers[i].valid &&
			(out->peers[i].bits & PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1) != 0)
			out->stream_known[i] = cluster_ic_tier1_terminal_peer_sessions(
				i, out->stream_epoch, out->peers[i].generation,
				out->stream_channels, &out->streams[i]);
	if (!cluster_sf_peer_capability_table_try_snapshot(after, CLUSTER_MAX_NODES, &busy) || busy ||
		cluster_epoch_get_current() != out->stream_epoch || cluster_lms_workers != out->stream_channels ||
		cluster_ic_local_capability_word() != out->local_capability)
		return;
	for (uint32 i = 0; i < CLUSTER_MAX_NODES; i++)
		if (after[i].bits != out->peers[i].bits || after[i].generation != out->peers[i].generation ||
			after[i].valid != out->peers[i].valid)
			return;
	out->streams_known = true;
}

static void
json_context(StringInfo out, const char *side, const WaitGraphContext *cut)
{
	const ClusterSemanticWaitGraphSnapshot *s = &cut->semantic;
	appendStringInfo(out, ",\"semantic_%s\":{\"known\":%s,\"seq\":" UINT64_FORMAT
		",\"active_bits\":" UINT64_FORMAT ",\"record_generation\":" UINT64_FORMAT
		",\"formation_epoch\":" UINT64_FORMAT ",\"transition_closed\":%s}",
		side, s->known ? "true" : "false", s->seq, s->active_bits,
		s->record_generation, s->formation_epoch, s->transition_closed ? "true" : "false");
	appendStringInfo(out, ",\"pcm_gate_%s\":{\"known\":%s,\"phase\":%u,\"formation\":" UINT64_FORMAT
		",\"freeze\":" UINT64_FORMAT "}", side, cut->gate_known ? "true" : "false",
		cut->gate.phase, cut->gate.formation, cut->gate.freeze_generation);
	appendStringInfo(out, ",\"peer_%s\":{\"known\":%s,\"busy\":%s,\"rows\":[", side,
		cut->peers_known ? "true" : "false", cut->peers_busy ? "true" : "false");
	for (uint32 i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i != 0) appendStringInfoChar(out, ',');
		appendStringInfo(out, "{\"node\":%u,\"bits\":%u,\"generation\":%u,\"valid\":%s}",
			i, cut->peers[i].bits, cut->peers[i].generation, cut->peers[i].valid ? "true" : "false");
	}
	appendStringInfoString(out, "]}");
	appendStringInfo(out, ",\"streams_%s\":{\"known\":%s,\"epoch\":" UINT64_FORMAT
		",\"channels\":%d,\"local_bits\":%u,\"rows\":[", side, cut->streams_known ? "true" : "false",
		cut->stream_epoch, cut->stream_channels, cut->local_capability);
	for (uint32 i = 0; i < CLUSTER_MAX_NODES; i++) {
		const ClusterICTerminalPeerSessions *stream = &cut->streams[i];
		if (i != 0) appendStringInfoChar(out, ',');
		appendStringInfo(out, "{\"node\":%u,\"known\":%s,\"control\":" UINT64_FORMAT ",\"data\":[",
			i, cut->stream_known[i] ? "true" : "false", stream->control_stream_generation);
		for (uint32 ch = 0; ch < stream->data_channels && ch < CLUSTER_IC_TIER1_DATA_CHANNELS; ch++) {
			if (ch != 0) appendStringInfoChar(out, ',');
			appendStringInfo(out, UINT64_FORMAT, stream->data_stream_generation[ch]);
		}
		appendStringInfoString(out, "]}");
	}
	appendStringInfoString(out, "]}");
}

static void
json_route(StringInfo out, const WaitGraphRoute *cut)
{
	appendStringInfo(out, "{\"known\":%s,\"master\":%d,\"epoch\":" UINT64_FORMAT
		",\"membership_generation\":" UINT64_FORMAT ",\"dead_generation\":" UINT64_FORMAT
		",\"declared\":[", cut->known ? "true" : "false", cut->master,
		cut->epoch, cut->membership_generation, cut->dead_generation);
	for (uint32 i = 0; i < cut->topology.count; i++) {
		if (i != 0) appendStringInfoChar(out, ',');
		appendStringInfo(out, "%d", cut->topology.nodes[i]);
	}
	appendStringInfoString(out, "]}");
}

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
json_pcm_admission(StringInfo out, const ClusterWaitPcmAdmission *row)
{
	appendStringInfoString(out, "{\"entry_checks_only\":true");
#define WG_U(f) appendStringInfo(out, ",\"" #f "\":" UINT64_FORMAT, (uint64)row->f)
#define WG_I(f) appendStringInfo(out, ",\"" #f "\":%d", row->f)
#define WG_B(f) appendStringInfo(out, ",\"" #f "\":%s", row->f ? "true" : "false")
	WG_U(feature_bit); WG_U(record_generation); WG_U(formation_epoch);
	WG_U(gate_formation); WG_U(gate_freeze); WG_U(master_session);
	WG_U(gate_phase); WG_U(peer_capability_generation);
	WG_I(origin); WG_I(master_node); WG_I(side);
	WG_B(supplied); WG_B(entered); WG_B(known);
#undef WG_U
#undef WG_I
#undef WG_B
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
	WG_U(ordered_lane); WG_U(semantic_crc32c); WG_U(source_candidate);
	WG_U(retain_pi_if_dirty); WG_U(common_flags); WG_U(common_outcome);
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
	appendStringInfoString(out, ",\"service_admission\":");
	json_pcm_admission(out, &row->service_admission);
	appendStringInfoChar(out, '}');
}

static void
json_pcm_owner(StringInfo out, const ClusterWaitGraphPcmOwner *owner)
{
	const ResourceXLocalOwnerHandle *h = &owner->handle;
	appendStringInfo(out, "{\"valid\":%s,\"successor_known\":%s,\"state\":%u,"
		"\"highest_generation\":" UINT64_FORMAT ",\"held_buffer_id_plus_one\":%u,"
		"\"claim\":{\"pid\":%d,\"proc_generation\":" UINT64_FORMAT
		",\"owner_generation\":" UINT64_FORMAT "},\"handle\":{\"tag\":",
		owner->valid ? "true" : "false", owner->successor_known ? "true" : "false",
		owner->state, owner->highest_generation, owner->held_buffer_id_plus_one,
		owner->claim.pid, owner->claim.proc_generation, owner->claim.owner_generation);
	json_tag(out, &h->ref.assertion.resource);
	appendStringInfo(out, ",\"requester\":%d,\"formation\":" UINT64_FORMAT
		",\"acquisition\":" UINT64_FORMAT ",\"master_session\":" UINT64_FORMAT
		",\"r4\":" UINT64_FORMAT ",\"ownership\":" UINT64_FORMAT ",\"reservation\":" UINT64_FORMAT
		",\"generation\":" UINT64_FORMAT ",\"deadline\":" UINT64_FORMAT ",\"procno\":%d},\"successor\":",
		h->ref.assertion.requester_node, h->ref.formation, h->ref.acquisition_generation,
		h->master_session_incarnation, h->r4_record_generation, h->buffer_ownership_generation,
		h->reservation_token, h->owner_generation, h->absolute_deadline_us, h->owner_procno);
	json_wait(out, &owner->successor);
	appendStringInfoChar(out, '}');
}

static void
json_pcm_block(StringInfo out, const ClusterWaitGraphPcmBlock *block)
{
	const ResourceXIntentSlot *slot = &block->slot;
	appendStringInfo(out, "{\"holder\":%d,\"decoded\":%s,\"reject\":%d,"
		"\"slot_matches\":%s,\"neutral_sender\":%s,\"slot\":{\"state\":%u,"
		"\"kind\":%u,\"destination\":%u,\"payload_bytes\":%u,\"logical_generation\":" UINT64_FORMAT
		",\"authority_generation\":" UINT64_FORMAT ",\"first_armed_us\":" UINT64_FORMAT
		",\"last_attempt_us\":" UINT64_FORMAT ",\"send_episode\":" UINT64_FORMAT
		",\"last_send_us\":" UINT64_FORMAT ",\"owner_node\":%u,\"owner_kind\":%u,"
		"\"owner_index\":%u,\"owner_generation\":" UINT64_FORMAT ",\"requester\":%d,\"tag\":",
		block->holder, block->decoded ? "true" : "false", block->reject,
		block->slot_matches ? "true" : "false", block->neutral_sender ? "true" : "false",
		slot->state, slot->kind, slot->destination_node, slot->payload_bytes,
		slot->logical_generation, slot->authority_generation, slot->first_armed_us,
		slot->last_attempt_us, slot->send_episode, slot->last_send_us,
		slot->body.owner_node, slot->body.owner_kind, slot->body.owner_index,
		slot->body.owner_generation, slot->body.assertion.requester_node);
	json_tag(out, &slot->body.assertion.resource);
	appendStringInfoString(out, "},\"payload_hex\":\"");
	for (uint32 i = 0; i < sizeof(block->payload); i++)
		appendStringInfo(out, "%02x", block->payload[i]);
	appendStringInfoString(out, "\",\"request\":");
	if (block->decoded) json_wait(out, &block->request);
	else appendStringInfoString(out, "null");
	appendStringInfoChar(out, '}');
}

typedef struct WaitGraphOwnerWriter {
	StringInfo out;
	uint32 count;
} WaitGraphOwnerWriter;

static void
json_native_proc(StringInfo out, const ClusterContentTraceNativeProc *proc)
{
	appendStringInfo(out, "{\"available\":%s,\"procno\":%u,\"pid\":%d,"
		"\"backend_id\":%d,\"generation\":" UINT64_FORMAT ",\"wait_event_info\":%u}",
		proc->available ? "true" : "false", proc->procno, proc->pid,
		proc->backend_id, proc->generation, proc->wait_event_info);
}

static void
json_rejection(const ClusterContentTraceRejection *row, void *context)
{
	WaitGraphOwnerWriter *writer = context;
	StringInfo out = writer->out;
	if (writer->count++ != 0) appendStringInfoChar(out, ',');
	appendStringInfo(out, "{\"slot\":%u,\"reason\":%d,\"payload_copied\":%s,"
		"\"payload\":{\"pid\":%d,\"procno\":%u,\"backend_id\":%d,"
		"\"backend_start\":" INT64_FORMAT ",\"generation\":" UINT64_FORMAT
		",\"revision\":" UINT64_FORMAT ",\"nlocks\":%u,\"ngrants\":%u,"
		"\"wait_mask\":%u,\"native_waiting\":%s,\"enqueue_waiting\":%s,\"pcm_serving\":%s},"
		"\"slot_revision\":" UINT64_FORMAT ",\"final_revision\":" UINT64_FORMAT
		",\"native_before\":", row->slot, row->reason, row->payload_copied ? "true" : "false",
		row->payload_pid, row->payload_procno, row->payload_backend_id,
		row->payload_start, row->payload_generation, row->payload_revision,
		row->payload_nlocks, row->payload_ngrants, row->payload_wait_mask,
		row->payload_native_waiting ? "true" : "false",
		row->payload_enqueue_waiting ? "true" : "false", row->payload_pcm_serving ? "true" : "false",
		row->slot_revision, row->final_revision);
	json_native_proc(out, &row->native_before);
	appendStringInfoString(out, ",\"native_after\":");
	json_native_proc(out, &row->native_after);
	appendStringInfoChar(out, '}');
}

static void
json_owner(const ClusterContentTraceOwner *row, void *context)
{
	WaitGraphOwnerWriter *writer = context;
	StringInfo out = writer->out;
	if (writer->count++ != 0) appendStringInfoChar(out, ',');
	appendStringInfo(out, "{\"node\":%d,\"pid\":%d,\"procno\":%u,\"backend_id\":%d,"
		"\"backend_start\":" INT64_FORMAT ",\"proc_generation\":" UINT64_FORMAT ",\"revision\":" UINT64_FORMAT
		",\"omitted_locks\":%u,\"locks\":[", cluster_node_id, row->pid, row->procno,
		row->backend_id, row->backend_start, row->proc_generation, row->revision, row->omitted_locks);
	for (uint32 i = 0; i < row->nlocks; i++) {
		if (i != 0) appendStringInfoChar(out, ',');
		json_lock(out, &row->locks[i]);
	}
	appendStringInfo(out, "],\"omitted_grants\":%u,\"grants\":[", row->omitted_grants);
	for (uint32 i = 0; i < row->ngrants; i++) {
		const ClusterContentTraceGrant *grant = &row->grants[i];
		if (i != 0) appendStringInfoChar(out, ',');
		appendStringInfo(out, "{\"resid\":[%u,%u,%u,%u,%u,%u],\"holder\":[%u,%u,"
			UINT64_FORMAT "," UINT64_FORMAT "],\"mode\":%d}", grant->field1, grant->field2,
			grant->field3, grant->field4, grant->type, grant->lockmethodid, grant->holder_node,
			grant->holder_procno, grant->holder_epoch, grant->holder_request, grant->mode);
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
	appendStringInfoString(out, ",\"pcm_service\":");
	if (row->pcm_serving) json_wait(out, &row->pcm_service);
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
	StringInfoData rejections;
	WaitGraphOwnerWriter writer = {.out = &out};
	WaitGraphOwnerWriter rejected_writer = {.out = &rejections};
	ClusterContentTraceWalkStats stats;
	ClusterWaitGraphKoSnapshot *ko;
	WaitGraphContext begin, end;
	uint64 epoch;
	bool first = true;
	check_reader();
	ko = palloc0(sizeof(*ko));
	epoch = cluster_epoch_get_current();
	wait_graph_context_snapshot(&begin);
	initStringInfo(&out);
	initStringInfo(&rejections);
	appendStringInfo(&out, "{\"version\":2,\"node\":%d,\"epoch_begin\":" UINT64_FORMAT
		",\"atomic_graph\":false,\"observed_at\":" INT64_FORMAT ",\"owners\":[",
		cluster_node_id, epoch, GetCurrentTimestamp());
	cluster_content_trace_walk_rejections(json_owner, &writer,
		json_rejection, &rejected_writer, &stats);
	appendStringInfo(&out, "],\"rejected_slots\":[%s],\"stats\":{\"available\":%s,\"scanned\":%u,\"busy\":%u,"
		"\"changed\":%u,\"dead\":%u,\"owners\":%u,\"omitted_locks\":%u},\"procs\":[",
		rejections.data, stats.available ? "true" : "false", stats.scanned, stats.busy, stats.changed,
		stats.dead, stats.owners, stats.omitted_locks);
	pfree(rejections.data);
	if (ProcGlobal != NULL) {
		for (uint32 i = 0; i < ProcGlobal->allProcCount; i++) {
			const PGPROC *proc = &ProcGlobal->allProcs[i];
			int pid = proc->pid;
			BackendId backend = proc->backendId;
			uint64 generation = *(volatile const uint64 *)&proc->cluster_grd_generation;
			uint32 wait = *(volatile const uint32 *)&proc->wait_event_info;
			bool stable;
			const char *wait_name = pgstat_get_wait_event(wait);
			if (pid <= 0) continue;
			pg_read_barrier();
			stable = pid == proc->pid && backend == proc->backendId &&
				generation == *(volatile const uint64 *)&proc->cluster_grd_generation;
			if (!first) appendStringInfoChar(&out, ',');
			first = false;
			appendStringInfo(&out, "{\"procno\":%u,\"pid\":%d,\"backend_id\":%d,"
				"\"proc_generation\":" UINT64_FORMAT ",\"wait_event_info\":%u,\"wait_name\":",
				i, pid, backend, generation, wait);
			escape_json(&out, wait_name != NULL ? wait_name : "");
			appendStringInfo(&out, ",\"stable\":%s}", stable ? "true" : "false");
		}
	}
	cluster_ko_wait_graph_snapshot(ko);
	appendStringInfoString(&out, "],\"ko\":"); json_ko(&out, ko);
	wait_graph_context_snapshot(&end);
	json_context(&out, "begin", &begin);
	json_context(&out, "end", &end);
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
		",\"master\":%d,\"master_generation\":%u,\"shard_phase\":%u,\"route_known\":%s"
		",\"copied\":%d,\"required\":%d,\"members\":[", cluster_node_id,
		cluster_epoch_get_current(), key.field1, key.field2, key.field3, key.field4,
		key.type, key.lockmethodid, status, snapshot.generation, snapshot.last_modified_scn,
		snapshot.master_node, snapshot.master_generation, snapshot.shard_phase,
		snapshot.route_known ? "true" : "false", snapshot.copied, snapshot.required);
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
	BufferTag tag = {0};
	ClusterWaitGraphPcmSnapshot *snapshot;
	WaitGraphRoute route_begin, route_end;
	WaitGraphContext begin, end;
	uint64 epoch;
	check_reader();
	snapshot = palloc0(sizeof(*snapshot));
	tag.spcOid = key_argument(fcinfo, 0, UINT32_MAX);
	tag.dbOid = key_argument(fcinfo, 1, UINT32_MAX);
	tag.relNumber = key_argument(fcinfo, 2, UINT32_MAX);
	tag.forkNum = key_argument(fcinfo, 3, MAX_FORKNUM);
	tag.blockNum = key_argument(fcinfo, 4, MaxBlockNumber);
	epoch = cluster_epoch_get_current();
	wait_graph_route_snapshot(&tag, &route_begin);
	wait_graph_context_snapshot(&begin);
	cluster_pcm_wait_graph_snapshot(&tag, snapshot);
	wait_graph_context_snapshot(&end);
	wait_graph_route_snapshot(&tag, &route_end);
	initStringInfo(&out);
	appendStringInfo(&out, "{\"kind\":\"pcm\",\"node\":%d,\"epoch\":" UINT64_FORMAT
		",\"status\":%d,\"tag\":", cluster_node_id, epoch, snapshot->status);
	json_tag(&out, &tag);
	appendStringInfoString(&out, ",\"route_begin\":"); json_route(&out, &route_begin);
	appendStringInfoString(&out, ",\"route_end\":"); json_route(&out, &route_end);
	json_context(&out, "begin", &begin);
	json_context(&out, "end", &end);
	appendStringInfoString(&out, ",\"view\":");
	json_wait(&out, &snapshot->view);
	appendStringInfoString(&out, ",\"local_owner\":"); json_pcm_owner(&out, &snapshot->local_owner);
	appendStringInfoString(&out, ",\"queue\":[");
	for (uint32 i = 0; i < snapshot->nqueued; i++) {
		if (i != 0) appendStringInfoChar(&out, ',');
		json_wait(&out, &snapshot->queue[i]);
	}
	appendStringInfo(&out, "],\"omitted_blocks\":%u,\"blocks\":[", snapshot->omitted_blocks);
	for (uint32 i = 0; i < snapshot->nblocks; i++) {
		if (i != 0) appendStringInfoChar(&out, ',');
		json_pcm_block(&out, &snapshot->blocks[i]);
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
	/* The cut itself must never initialize the process-local conf index. */
	(void) cluster_conf_lookup_node(0);
	status = cluster_grd_queue_snapshot(&key, &snapshot, NULL, 0);
	PG_RETURN_BOOL(status != CLUSTER_GRD_QUEUE_UNAVAILABLE && status != CLUSTER_GRD_QUEUE_INVALID);
#else
	check_reader();
	PG_RETURN_NULL();
#endif
}
