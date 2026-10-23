/*-------------------------------------------------------------------------
 * cluster_tx_identity_trace.c
 *   Finite process-local identity logs. No authority lookups or shared state.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_tx_identity_trace.h"
#include "cluster/cluster_wait_trace.h"
#include "miscadmin.h"
#include "storage/proc.h"

#ifdef USE_PGRAC_CLUSTER
#define ORIGIN_DEDUP_SIZE 128
static uint32 counts[CLTXID_NKINDS];
static uint64 sequence;
static ClusterTxIdentityTrace origin_seen[ORIGIN_DEDUP_SIZE];
static uint32 origin_next;
static uint32 origin_count;
static uint32 wait_counts[CLWAIT_NKINDS];
static uint64 wait_snapshot;

uint64
cluster_wait_trace_begin(ClusterWaitTraceKind kind)
{
	if (!cluster_trace_tx_identity || kind < 0 || kind >= CLWAIT_NKINDS
		|| wait_counts[kind] > CLUSTER_WAIT_TRACE_LIMIT || wait_snapshot == UINT64_MAX)
		return 0;
	return ++wait_snapshot;
}

void
cluster_wait_trace_send_gate_emit(const BufferTag *tag, const ClusterWaitSendGate *gate)
{
	uint32 *count = &wait_counts[CLWAIT_PCM_SEND_GATE];
	uint64 snapshot;

	if (!cluster_trace_tx_identity || tag == NULL || gate == NULL || !gate->known)
		return;
	snapshot = cluster_wait_trace_begin(CLWAIT_PCM_SEND_GATE);
	if (snapshot == 0)
		return;
	/* No ereport/ERROR: even a rejected stage may have transport ownership
	 * elsewhere in its caller. Stderr failure must not unwind that caller. */
	if ((*count)++ == CLUSTER_WAIT_TRACE_LIMIT) {
		write_stderr("WAIT_RESOURCE event=LIMIT family=PCM_SEND_GATE actor_node=%d "
			"pid=%d limit=%u evidence_complete=0\n",
			cluster_node_id, MyProcPid, CLUSTER_WAIT_TRACE_LIMIT);
		return;
	}
	write_stderr("WAIT_RESOURCE event=PCM_SEND_GATE actor_node=%d pid=%d proc=%u "
		"snapshot=" UINT64_FORMAT " master_view=0 tag_known=1 spc=%u db=%u rel=%u fork=%d block=%u "
		"send_gate_known=1 send_owner=%d send_state=%d send_reason=%d "
		"send_owner_node=%d send_owner_index=%d send_requester=%d send_kind=%d send_destination=%d send_result=%d send_site=%d "
		"send_owner_generation=" UINT64_FORMAT " send_logical_generation=" UINT64_FORMAT " "
		"send_authority_generation=" UINT64_FORMAT " "
		"send_observed_us=" UINT64_FORMAT " send_last_attempt_us=" UINT64_FORMAT " "
		"send_last_send_us=" UINT64_FORMAT " send_episode=" UINT64_FORMAT " send_busy=%d "
		"send_stream_generation=" UINT64_FORMAT " send_busy_stream_generation=" UINT64_FORMAT " "
		"send_capability_generation=%u send_busy_capability_generation=%u\n",
		cluster_node_id, MyProcPid, MyProc != NULL ? MyProc->pgprocno : UINT32_MAX,
		snapshot, tag->spcOid, tag->dbOid, tag->relNumber, tag->forkNum, tag->blockNum,
		gate->owner, gate->state, gate->reason, gate->owner_node, gate->owner_index,
		gate->requester, gate->kind, gate->destination, gate->result, gate->site, gate->owner_generation,
		gate->logical_generation, gate->authority_generation, gate->observed_us, gate->last_attempt_us,
		gate->last_send_us, gate->episode, gate->busy, gate->stream_generation,
		gate->busy_stream_generation, gate->capability_generation,
		gate->busy_capability_generation);
}

void
cluster_wait_trace_emit(ClusterWaitTraceKind kind, const ClusterWaitTrace *row)
{
	static const char *const names[CLWAIT_NKINDS] = {
		"PCM_WAIT", "PCM_QUEUE", "PCM_ADMISSION", "TX_BUFFER", "TX_LEDGER", "TX_SUMMARY", "PCM_GUARD",
		"PCM_SEND_GATE"
	};
	uint32 *count;

	if (!cluster_trace_tx_identity || row == NULL || kind < 0 || kind >= CLWAIT_NKINDS
		|| kind == CLWAIT_PCM_SEND_GATE)
		return;
	count = &wait_counts[kind];
	if (*count > CLUSTER_WAIT_TRACE_LIMIT)
		return;
	if ((*count)++ == CLUSTER_WAIT_TRACE_LIMIT) {
		ereport(LOG, (errmsg_internal("WAIT_RESOURCE event=LIMIT family=%s actor_node=%d "
			"pid=%d limit=%u evidence_complete=0", names[kind], cluster_node_id,
			MyProcPid, CLUSTER_WAIT_TRACE_LIMIT)));
		return;
	}
	ereport(LOG, (errmsg_internal(
		"WAIT_RESOURCE event=%s actor_node=%d pid=%d proc=%u snapshot=" UINT64_FORMAT " site=%d "
		"tag_known=%d spc=%u db=%u rel=%u fork=%d block=%u "
		"wait_seq=" UINT64_FORMAT " tx_xid=%u request_known=%d request_seq=" UINT64_FORMAT " "
		"mode=%d master_view=%d expected_master=%d requester=%d source=%d entry_known=%d "
		"state=%d x_holder=%d s_holders=%08x pi_holders=%08x pending_requester=%d "
		"pending_cookie=" UINT64_FORMAT " binding=" UINT64_FORMAT " formation=" UINT64_FORMAT " "
		"master_session=" UINT64_FORMAT " r4=" UINT64_FORMAT " acquisition=" UINT64_FORMAT " "
		"base=" UINT64_FORMAT " authority=" UINT64_FORMAT " head=" UINT64_FORMAT " "
		"enqueue_order=" UINT64_FORMAT " requester_conn=" UINT64_FORMAT " ingress_conn=" UINT64_FORMAT " "
		"transport_known=%d wire_sender_conn=" UINT64_FORMAT " "
		"queue_kind=%d phase=%d round_phase=%d owner_phase=%d progress=%08x incompatible=%08x blocked=%08x "
		"round_request=" UINT64_FORMAT " round_requester=%d head_request=" UINT64_FORMAT " head_requester=%d "
		"holder_node=%u holder_proc=%u holder_epoch=" UINT64_FORMAT " holder_request=" UINT64_FORMAT " "
		"blocking_instance=%d blocking_session=%d blocking_pid=%d blocking_known=%d "
		"final_blocking_instance=%d final_blocking_session=%d final_blocking_pid=%d final_blocking_known=%d "
		"master_holder_instance=%d master_holder_session=%d master_holder_pid=%d master_holder_known=%d "
		"master_request=" UINT64_FORMAT " master_head_request=" UINT64_FORMAT " master_head_known=%d "
		"master_request_head_known=%d "
		"admission_bridge_known=%d admission_r4=" UINT64_FORMAT " admitted_base=" UINT64_FORMAT " "
		"admitted_head=" UINT64_FORMAT " admitted_request=" UINT64_FORMAT " "
		"admitted_order=" UINT64_FORMAT " admitted_requester_conn=" UINT64_FORMAT " "
		"admitted_ingress_conn=" UINT64_FORMAT " admitted_phase=%d admitted_source=%d "
		"buffer=%d private_pins=%d shared_pins=%d content_held=%d ledger_phase=%d "
		"buffer_state=%08x own_state=%d buffer_type=%d own_flags=%08x "
		"own_generation=" UINT64_FORMAT " reservation=" UINT64_FORMAT " "
		"writer_activation=" UINT64_FORMAT " rx_activation=" UINT64_FORMAT " "
		"entry_formation=" UINT64_FORMAT " entry_acquisition=" UINT64_FORMAT " entry_base=" UINT64_FORMAT " "
		"round_known=%d round_formation=" UINT64_FORMAT " round_session=" UINT64_FORMAT " "
		"round_r4=" UINT64_FORMAT " round_base=" UINT64_FORMAT " round_master=%d "
		"round_requester_conn=" UINT64_FORMAT " round_ingress_conn=" UINT64_FORMAT " caller_matches_round=%d "
		"head_known=%d head_formation=" UINT64_FORMAT " head_session=" UINT64_FORMAT " head_base=" UINT64_FORMAT " "
		"head_requester_conn=" UINT64_FORMAT " head_ingress_conn=" UINT64_FORMAT " caller_matches_head=%d "
		"wait_head_change=" UINT64_FORMAT " head_change=" UINT64_FORMAT " continuation_known=%d "
		"continuation_pending=" UINT64_FORMAT " continuation_expected_x=" UINT64_FORMAT " "
		"continuation_reservation=" UINT64_FORMAT " continuation_claim_pending=" UINT64_FORMAT " "
		"continuation_claim_reservation=" UINT64_FORMAT " guard=%d guard_result=%d refusal_reason=%d "
		"refusal_token=" UINT64_FORMAT " refusal_flags=%08x refusal_pins=%u refusal_io=%d "
		"pins_seen=%u ledger_seen=%u local_pins=%u rows=%u omitted=%u atomic_graph=0",
		names[kind], cluster_node_id, MyProcPid,
		MyProc == NULL ? UINT32_MAX : (uint32)MyProc->pgprocno, row->snapshot, row->site,
		row->tag_known, row->tag.spcOid, row->tag.dbOid, row->tag.relNumber,
		(int)row->tag.forkNum, row->tag.blockNum, row->wait_seq, row->tx_xid, row->request_known, row->request_seq,
		row->mode, row->master_view, row->expected_master, row->requester, row->source, row->entry_known,
		row->state, row->x_holder, row->s_holders, row->pi_holders, row->pending_requester,
		row->pending_cookie, row->binding, row->formation, row->master_session, row->r4,
		row->acquisition, row->base, row->authority, row->head, row->enqueue_order,
		row->requester_connection, row->ingress_connection, row->transport_known,
		row->wire_sender_connection, row->queue_kind, row->phase, row->round_phase,
		row->owner_phase, row->progress, row->incompatible, row->blocked,
		row->round_request, row->round_requester, row->head_request, row->head_requester,
		row->holder_node, row->holder_proc, row->holder_epoch, row->holder_request,
		row->blocking_known ? row->blocking_instance : -1,
		row->blocking_known ? row->blocking_session : -1,
		row->blocking_known ? row->blocking_pid : -1, row->blocking_known,
		row->final_blocking_known ? row->final_blocking_instance : -1,
		row->final_blocking_known ? row->final_blocking_session : -1,
		row->final_blocking_known ? row->final_blocking_pid : -1,
		row->final_blocking_known,
		row->master_holder_known ? row->master_holder_instance : -1,
		row->master_holder_known ? row->master_holder_session : -1,
		row->master_holder_known ? row->master_holder_pid : -1,
		row->master_holder_known, row->master_holder_known ? row->master_request : 0,
		row->master_head_known ? row->master_head_request : 0,
		row->master_head_known, row->master_request_head_known,
		row->admission.known, row->admission.known ? row->admission.r4 : 0,
		row->admission.known ? row->admission.base : 0,
		row->admission.known ? row->admission.head : 0,
		row->admission.known ? row->admission.request_seq : 0,
		row->admission.known ? row->admission.enqueue_order : 0,
		row->admission.known ? row->admission.requester_connection : 0,
		row->admission.known ? row->admission.ingress_connection : 0,
		row->admission.known ? row->admission.phase : -1,
		row->admission.known ? row->admission.source : -1,
		row->buffer, row->private_pins, row->shared_pins, row->content_held, row->ledger_phase,
		row->buffer_state, row->own_state, row->buffer_type, row->own_flags,
		row->own_generation, row->reservation, row->writer_activation, row->rx_activation,
		row->entry_formation, row->entry_acquisition, row->entry_base,
		row->round_request_known, row->round_formation, row->round_session,
		row->round_r4, row->round_base, row->round_master,
		row->round_requester_connection, row->round_ingress_connection, row->caller_matches_round,
		row->head_request_known, row->head_formation, row->head_session, row->head_base,
		row->head_requester_connection, row->head_ingress_connection, row->caller_matches_head,
		row->wait_head_change, row->head_change, row->continuation_known,
		row->continuation_pending, row->continuation_expected_x, row->continuation_reservation,
		row->continuation_claim_pending, row->continuation_claim_reservation,
		row->guard, row->guard_result, row->refusal_reason, row->refusal_token,
		row->refusal_flags, row->refusal_pins, row->refusal_io,
		row->pins_seen, row->ledger_seen, row->local_pins, row->rows, row->omitted)));
}

void
cluster_tx_identity_trace_emit(ClusterTxIdentityTraceKind kind, const ClusterTxIdentityTrace *row)
{
	static const char *const names[CLTXID_NKINDS] = {
		"RESERVE", "ACTIVE", "RETRY", "ALIAS", "LOCAL_TERMINAL", "TERMINAL",
		"WAIT_SOURCE", "WAIT_TARGET", "WAIT_END", "ORIGIN_MAP", "WAKE", "WAKE_SUMMARY"
	};
	uint32 *count;

	if (!cluster_trace_tx_identity || row == NULL || kind < 0 || kind >= CLTXID_NKINDS)
		return;
	count = &counts[kind];
	if (*count > CLUSTER_TX_IDENTITY_TRACE_LIMIT)
		return;
	if (*count == CLUSTER_TX_IDENTITY_TRACE_LIMIT) {
		(*count)++;
		ereport(LOG, (errmsg_internal("TX_IDENTITY event=LIMIT family=%s node=%d pid=%d "
			"limit=%u evidence_complete=0", names[kind], cluster_node_id, MyProcPid,
			CLUSTER_TX_IDENTITY_TRACE_LIMIT)));
		return;
	}
	/* Diagnostic-only deduplication. A changed physical key or outcome is
	 * retained. Eviction may repeat a map, never supplies an authority hit. */
	if (kind == CLTXID_ORIGIN_MAP) {
		for (uint32 i = 0; i < origin_count; i++)
			if (memcmp(&origin_seen[i], row, sizeof(*row)) == 0)
				return;
		origin_seen[origin_next] = *row;
		origin_next = (origin_next + 1) % ORIGIN_DEDUP_SIZE;
		if (origin_count < ORIGIN_DEDUP_SIZE)
			origin_count++;
	}
	(*count)++;
	sequence++;
	ereport(LOG, (errmsg_internal(
		"TX_IDENTITY event=%s seq=" UINT64_FORMAT " actor_node=%d pid=%d proc=%u "
		"node=%d xid=%u epoch=" UINT64_FORMAT " "
		"tt_known=%d tt_segment=%u tt_slot=%u tt_wrap=%u generation_known=%d tt_generation=%u "
		"source_known=%d source_segment=%u source_slot_id=%u target_known=%d uba_known=%d "
		"uba0=%016" INT64_MODIFIER "x uba1=%016" INT64_MODIFIER "x "
		"itl_wrap=%u itl_kind=%u itl_slot=%u wait_seq=" UINT64_FORMAT " "
		"status=%d peer_proc=%u count=%u skipped_targets=%u omitted=%u",
		names[kind], sequence, cluster_node_id, MyProcPid,
		MyProc == NULL ? UINT32_MAX : (uint32)MyProc->pgprocno,
		row->node, row->xid, row->epoch, row->tt_known, row->tt_segment, row->tt_slot,
		row->tt_wrap, row->generation_known, row->tt_generation, row->source_known,
		row->source_segment, row->source_slot_id, row->target_known, row->uba_known, row->uba0, row->uba1,
		row->itl_wrap, row->itl_kind, row->itl_slot, row->wait_seq, row->status,
		row->peer_procno, row->count, row->skipped_targets, row->omitted)));
}
#endif
