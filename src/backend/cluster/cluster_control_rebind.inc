/* Same-OPEN CONTROL collection, owned by the existing LMON.
 * No serving consumer or live dispatch enters this staged body yet.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef enum SemanticControlRebindCheck {
	SEMANTIC_CONTROL_REBIND_BAD,
	SEMANTIC_CONTROL_REBIND_WAIT,
	SEMANTIC_CONTROL_REBIND_SAME
} SemanticControlRebindCheck;

/* Original durable bytes and common formation only; never local stream numbers. */
static bool
semantic_control_rebind_binding(uint32 collector, uint64 nonce, uint8 out[32])
{
	const SemanticControlRebindBaseline *base = &SemanticControlRebindState->baseline;
	const ClusterSemanticActivationAckTableV1 *table = &base->table;
	uint8 bytes[1280] = { 0 };
	Size at = 32;
	const char domain[] = "PGRAC_CONTROL_OPEN_REBIND_V1";

	if (collector >= 4 || nonce == 0 || nonce == UINT64_MAX)
		return false;
	memcpy(bytes, domain, sizeof(domain));
	semantic_activation_write_u64_le(bytes + at, base->cut.system_identifier);
	at += 8;
	memcpy(bytes + at, base->pgsa, 512);
	at += 512;
	memcpy(bytes + at, base->root, 512);
	at += 512;
	semantic_activation_write_u64_le(bytes + at, table->transition_epoch);
	at += 8;
	semantic_activation_write_u64_le(bytes + at, table->expected_members_lo);
	at += 8;
	semantic_activation_write_u64_le(bytes + at, table->expected_members_hi);
	at += 8;
	for (unsigned node = 0; node < 4; node++) {
		semantic_activation_write_u64_le(bytes + at, table->expected[node].boot_id);
		at += 8;
		semantic_activation_write_u64_le(bytes + at, table->expected[node].admitted_incarnation);
		at += 8;
	}
	semantic_activation_write_u64_le(bytes + at, table->source_feature_bitmap);
	at += 8;
	semantic_activation_write_u64_le(bytes + at, table->target_feature_bitmap);
	at += 8;
	semantic_activation_write_u64_le(bytes + at, table->rollback_feature_bitmap);
	at += 8;
	semantic_activation_write_u64_le(bytes + at, table->capability_sample_digest);
	at += 8;
	memcpy(bytes + at, table->restart_binding, 32);
	at += 32;
	semantic_activation_write_u32_le(bytes + at, collector);
	at += 4;
	semantic_activation_write_u64_le(bytes + at, table->expected[collector].boot_id);
	at += 8;
	semantic_activation_write_u64_le(bytes + at, nonce);
	at += 8;
	return normal_start_sha256(bytes, at, out);
}

/* Do not call serving-ready here: replying must not depend on this round's
 * COMPLETE. All non-transport evidence comes from the original successful K. */
static SemanticControlRebindCheck
semantic_control_rebind_memory(SemanticControlRebindCut *cut,
							   ClusterSemanticActivationAckTableV1 *table, bool *changed)
{
	const SemanticControlRebindBaseline *base;
	SemanticActivationAdmissionSnapshot gate, after;
	ClusterFormationSnapshotV1 formation;
	ClusterSemanticActivationRecord open;
	uint8 root[512];
	uint64 seq, membership;
	bool wait = false;

	memset(cut, 0, sizeof(*cut));
	*changed = false;
	if (!IsUnderPostmaster || !AmLmonProcess() || MyProc == NULL || MyProcPid <= 0
		|| MyProc->pgprocno < 0 || MyProc->cluster_grd_generation == 0
		|| MyProc->cluster_grd_generation == UINT64_MAX || SemanticControlRebindState == NULL
		|| !cluster_shared_config || cluster_node_id < 0 || cluster_node_id >= 4
		|| cluster_normal_stop_requested() || !cluster_qvotec_in_quorum()
		|| !cluster_write_fence_allowed() || cluster_reconfig_has_pending_prebump_stage())
		return SEMANTIC_CONTROL_REBIND_BAD;
	seq = pg_atomic_read_u64(&SemanticControlRebindState->baseline_seq);
	if (seq == 0 || (seq & 1) != 0 || seq == UINT64_MAX)
		return SEMANTIC_CONTROL_REBIND_WAIT;
	pg_read_barrier();
	base = &SemanticControlRebindState->baseline;
	if (!base->valid || !cluster_semantic_activation_record_decode(base->pgsa, &open, NULL)
		|| !semantic_activation_snapshot(&gate))
		return SEMANTIC_CONTROL_REBIND_BAD;
	if (gate.transition_closed || gate.active_bits != base->table.target_feature_bitmap
		|| gate.record_generation != base->table.record_generation
		|| gate.formation_epoch != base->cut.epoch || cluster_epoch_get_current() != base->cut.epoch
		|| GetSystemIdentifier() != base->cut.system_identifier
		|| base->table.flags
			   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| base->table.expected_members_lo != 15 || base->table.expected_members_hi != 0
		|| !normal_start_service_binding_current(&open, base->root, &base->table))
		return SEMANTIC_CONTROL_REBIND_BAD;
	membership = cluster_membership_cut_generation();
	if (membership == 0
		|| !cluster_reconfig_capture_formation_snapshot_v1(cluster_node_id + 1, &formation)
		|| !semantic_activation_pgrd_snapshot_copy(root))
		return SEMANTIC_CONTROL_REBIND_WAIT;
	if (formation.local_epoch != base->cut.epoch
		|| formation.startup_formation_generation != base->cut.formation
		|| formation.self_join_admitted != 1 || formation.self_join_failed
		|| formation.prebump_sync_active
		|| !semantic_activation_bytes_are_zero(formation.reserved, sizeof(formation.reserved))
		|| !semantic_activation_bytes_are_zero(formation.pending_join_bitmap,
											   sizeof(formation.pending_join_bitmap))
		|| !semantic_activation_bytes_are_zero(formation.excluded_bitmap,
											   sizeof(formation.excluded_bitmap))
		|| !semantic_activation_bytes_are_zero(formation.clean_departed_bitmap,
											   sizeof(formation.clean_departed_bitmap))
		|| !semantic_activation_bytes_are_zero(formation.removed_bitmap,
											   sizeof(formation.removed_bitmap))
		|| memcmp(root, base->root, 512) != 0
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&cut->identity.native)
		|| memcmp(&cut->identity.native, &base->cut.native, sizeof(base->cut.native)) != 0)
		return SEMANTIC_CONTROL_REBIND_BAD;
	*table = base->table;
	table->flags = CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID;
	table->observed_members_lo = table->observed_members_hi = 0;
	memset(table->observed, 0, sizeof(table->observed));
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; node++) {
		if ((node < 4) != (formation.membership.membership_state[node] == CLUSTER_MEMBER_MEMBER))
			return SEMANTIC_CONTROL_REBIND_BAD;
		if (node >= 4)
			continue;
		if (formation.membership.last_admitted_incarnation[node] != base->cut.members[node]
			|| cluster_membership_get_last_admitted_incarnation(node) != base->cut.members[node]
			|| cluster_membership_get_state(node) != CLUSTER_MEMBER_MEMBER)
			return SEMANTIC_CONTROL_REBIND_BAD;
		cut->identity.members[node] = base->cut.members[node];
		if (node == (unsigned)cluster_node_id) {
			if (!semantic_activation_ack_self_tuple(node, cluster_ic_local_capability_word(),
													gate.formation_epoch, gate.record_generation,
													&table->expected[node])
				|| !semantic_activation_ack_matches(&table->expected[node],
													&base->table.expected[node]))
				return SEMANTIC_CONTROL_REBIND_BAD;
		} else {
			ClusterSfPeerCap capability;
			if (!cluster_sf_peer_capability_record_snapshot(node, &capability)
				|| !capability.valid) {
				wait = true;
				continue;
			}
			if (capability.bits != base->table.expected[node].capability_word
				|| capability.generation == 0 || capability.generation == UINT32_MAX
				|| capability.generation < base->table.expected[node].capability_generation)
				return SEMANTIC_CONTROL_REBIND_BAD;
			if (!cluster_ic_tier1_stream_capture(node, &cut->streams[node])) {
				wait = true;
				continue;
			}
			if (capability.generation != base->table.expected[node].capability_generation) {
				/* A changed store alone cannot prove that the old stream closed. */
				if (cluster_ic_tier1_stream_current(
						&SemanticControlRebindState->original_cut.streams[node]))
					return SEMANTIC_CONTROL_REBIND_BAD;
				*changed = true;
			} else if (memcmp(&cut->streams[node],
							  &SemanticControlRebindState->original_cut.streams[node],
							  sizeof(cut->streams[node]))
					   != 0) {
				wait = true; /* new stream has not published its authenticated HELLO yet */
			}
			table->expected[node].capability_generation = capability.generation;
			table->expected[node].control_connection_generation = capability.generation;
		}
	}
	cut->identity.admission_seq = gate.seq;
	cut->identity.ack_seq = base->cut.ack_seq;
	cut->identity.pgrd_seq = pg_atomic_read_u64(&SemanticActivationPgrdSnapshot->publication_seq);
	cut->identity.membership_cut = membership;
	cut->identity.epoch = gate.formation_epoch;
	cut->identity.formation = formation.startup_formation_generation;
	cut->identity.system_identifier = base->cut.system_identifier;
	cut->owner_pid = MyProcPid;
	cut->owner_procno = MyProc->pgprocno;
	cut->owner_generation = MyProc->cluster_grd_generation;
	cut->base_seq = seq;
	if (cut->owner_pid != SemanticControlRebindState->original_cut.owner_pid
		|| cut->owner_procno != SemanticControlRebindState->original_cut.owner_procno
		|| cut->owner_generation != SemanticControlRebindState->original_cut.owner_generation)
		*changed = true;
	pg_read_barrier();
	if (pg_atomic_read_u64(&SemanticControlRebindState->baseline_seq) != seq
		|| !semantic_activation_snapshot(&after) || after.seq != gate.seq
		|| !cluster_membership_cut_generation_current(membership)
		|| (cut->identity.pgrd_seq & 1) != 0)
		return SEMANTIC_CONTROL_REBIND_WAIT;
	return wait ? SEMANTIC_CONTROL_REBIND_WAIT : SEMANTIC_CONTROL_REBIND_SAME;
}

static bool
semantic_control_rebind_frame_current(const SemanticControlRebindIngress *item)
{
	ClusterSfPeerCap cap;
	int peer = item->stream.peer;
	return item->valid && peer >= 0 && peer < 4 && cluster_ic_tier1_stream_current(&item->stream)
		   && cluster_sf_peer_capability_record_snapshot(peer, &cap) && cap.valid
		   && cap.bits == item->capability.bits && cap.generation == item->capability.generation;
}

/* A bounded copy of the original dispatch provenance. Never query a newer
 * global capability record to convert an old frame into a new-stream ACK. */
static bool
semantic_control_rebind_ingress(const ClusterICEnvelope *env, const void *payload)
{
	SemanticControlRebindIngress item = { 0 };
	SemanticControlRebindIngress *slot;
	ClusterICControlIngress ingress;
	ClusterSemanticControlRebindWireV3 *m = &item.message;
	unsigned peer;

	if (!IsUnderPostmaster || !AmLmonProcess() || env == NULL || payload == NULL
		|| env->msg_type != PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1 || env->payload_length != 152
		|| cluster_node_id < 0 || cluster_node_id >= 4 || env->source_node_id >= 4
		|| env->source_node_id == (uint32)cluster_node_id
		|| env->dest_node_id != (uint32)cluster_node_id || env->epoch != cluster_epoch_get_current()
		|| !cluster_semantic_control_rebind_wire_decode(payload, m)
		|| m->transition_epoch != env->epoch || !cluster_ic_dispatch_control_ingress(env, &ingress))
		return false;
	peer = env->source_node_id;
	if (ingress.carrier != CLUSTER_IC_CONTROL_TCP || ingress.source_node != (int32)peer
		|| ingress.epoch != env->epoch || ingress.owner_pid != MyProcPid || ingress.serial == 0
		|| !cluster_ic_tier1_stream_capture(peer, &item.stream)
		|| item.stream.plane != CLUSTER_IC_PLANE_CONTROL || item.stream.channel != -1
		|| item.stream.serial != ingress.serial || item.stream.epoch != ingress.epoch
		|| item.stream.owner_pid != ingress.owner_pid
		|| !cluster_sf_peer_capability_record_snapshot(peer, &item.capability)
		|| !item.capability.valid || item.capability.generation == 0
		|| item.capability.generation == UINT32_MAX || m->capability_word != item.capability.bits)
		return false;
	if (m->kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST) {
		if (m->coordinator_node != peer || m->member_node != (uint32)cluster_node_id)
			return false;
		slot = &semantic_control_rebind.requests[peer];
		/* Preserve one peer's newer request against late frames. The real
		 * collector nonce persists across LMON generations in shared memory. */
		if (slot->message.round_nonce > m->round_nonce)
			return false;
	} else {
		if (m->member_node != peer || m->coordinator_node != (uint32)cluster_node_id
			|| semantic_control_rebind.phase != SEMANTIC_CONTROL_REBIND_COLLECTING
			|| m->round_nonce != semantic_control_rebind.nonce)
			return false;
		slot = &semantic_control_rebind.acks[peer];
	}
	item.valid = true;
	if (!semantic_control_rebind_frame_current(&item))
		return false;
	*slot = item;
	return true;
}

static bool
semantic_control_rebind_message_matches(const SemanticControlRebindIngress *item)
{
	const ClusterSemanticActivationAckTableV1 *base = &SemanticControlRebindState->baseline.table;
	const ClusterSemanticControlRebindWireV3 *m = &item->message;
	uint8 binding[32];
	unsigned source = item->stream.peer;

	return semantic_control_rebind_frame_current(item)
		   && m->transition_epoch == base->transition_epoch
		   && m->record_generation == base->record_generation
		   && m->source_feature_bitmap == base->source_feature_bitmap
		   && m->target_feature_bitmap == base->target_feature_bitmap
		   && m->rollback_feature_bitmap == base->rollback_feature_bitmap
		   && m->admitted_members_lo == base->expected_members_lo && m->admitted_members_hi == 0
		   && m->capability_sample_digest == base->capability_sample_digest
		   && m->boot_id == base->expected[source].boot_id
		   && m->admitted_incarnation == base->expected[source].admitted_incarnation
		   && m->capability_word == base->expected[source].capability_word
		   && semantic_control_rebind_binding(m->coordinator_node, m->round_nonce, binding)
		   && memcmp(binding, m->proof_binding, 32) == 0;
}

static void
semantic_control_rebind_clear_completion(void)
{
	uint64 seq = pg_atomic_read_u64(&SemanticControlRebindState->completion_seq);
	if (SemanticControlRebindState->completion.nonce == 0 || (seq & 1) != 0 || seq > UINT64_MAX - 2)
		return;
	pg_atomic_write_u64(&SemanticControlRebindState->completion_seq, seq + 1);
	pg_write_barrier();
	memset(&SemanticControlRebindState->completion, 0,
		   sizeof(SemanticControlRebindState->completion));
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticControlRebindState->completion_seq, seq + 2);
}

static bool
semantic_control_rebind_cut_current(void)
{
	SemanticControlRebindCut cut;
	ClusterSemanticActivationAckTableV1 table;
	bool changed;
	return semantic_control_rebind_memory(&cut, &table, &changed) == SEMANTIC_CONTROL_REBIND_SAME
		   && memcmp(&cut, &semantic_control_rebind.cut, sizeof(cut)) == 0
		   && memcmp(table.expected, semantic_control_rebind.table.expected, sizeof(table.expected))
				  == 0;
}

static bool
semantic_control_rebind_begin(const SemanticControlRebindCut *cut,
							  const ClusterSemanticActivationAckTableV1 *table)
{
	SemanticControlRebindRuntime *r = &semantic_control_rebind;
	uint64 nonce = pg_atomic_read_u64(&SemanticControlRebindState->nonce);

	if (r->read_seq != 0 || pg_atomic_read_u64(&SemanticControlRebindState->read_seq) != 0
		|| nonce >= UINT64_MAX - 1)
		return false;
	pg_atomic_write_u64(&SemanticControlRebindState->nonce, nonce + 1);
	r->nonce = nonce + 1;
	r->cut = *cut;
	r->table = *table;
	r->table.observed[cluster_node_id] = r->table.expected[cluster_node_id];
	r->table.observed_members_lo = UINT64_C(1) << cluster_node_id;
	memset(r->last_send, 0, sizeof(r->last_send));
	memset(r->acks, 0, sizeof(r->acks));
	r->read_verified = false;
	r->phase = SEMANTIC_CONTROL_REBIND_READING;
	return semantic_control_rebind_binding(cluster_node_id, r->nonce, r->proof_binding);
}

static ClusterSemanticControlRebindWireV3
semantic_control_rebind_request(unsigned peer)
{
	SemanticControlRebindRuntime *r = &semantic_control_rebind;
	ClusterSemanticControlRebindWireV3 m = { 0 };
	const SemanticActivationAckTuple *self = &r->table.expected[cluster_node_id];
	m.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST;
	m.stage = CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED;
	m.coordinator_node = cluster_node_id;
	m.member_node = peer;
	m.transition_epoch = r->table.transition_epoch;
	m.record_generation = r->table.record_generation;
	m.round_nonce = r->nonce;
	m.source_feature_bitmap = r->table.source_feature_bitmap;
	m.target_feature_bitmap = r->table.target_feature_bitmap;
	m.rollback_feature_bitmap = r->table.rollback_feature_bitmap;
	m.admitted_members_lo = 15;
	m.capability_sample_digest = r->table.capability_sample_digest;
	m.boot_id = self->boot_id;
	m.admitted_incarnation = self->admitted_incarnation;
	m.capability_word = self->capability_word;
	memcpy(m.proof_binding, r->proof_binding, 32);
	return m;
}

/* Keep native transport acceptance distinct from all-member confirmation. */
static bool
semantic_control_rebind_send(unsigned peer, const ClusterSemanticControlRebindWireV3 *message)
{
	uint8 bytes[152];
	ClusterICSendResult result;
	if (!semantic_control_rebind_cut_current()
		|| !cluster_semantic_control_rebind_wire_encode(message, bytes))
		return false;
	result = cluster_ic_send_envelope(PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1, peer, bytes,
									  sizeof(bytes));
	if (result == CLUSTER_IC_SEND_HARD_ERROR)
		cluster_ic_tier1_close_peer(peer, "CONTROL rebind send failed");
	return result == CLUSTER_IC_SEND_DONE || result == CLUSTER_IC_SEND_WOULD_BLOCK;
}

static bool
semantic_control_rebind_publish(void)
{
	SemanticControlRebindRuntime *r = &semantic_control_rebind;
	SemanticControlRebindCompletion *out = &SemanticControlRebindState->completion;
	uint64 seq = pg_atomic_read_u64(&SemanticControlRebindState->completion_seq);
	uint8 live[4 * 24] = { 0 }, digest[32];

	if ((seq & 1) != 0 || seq > UINT64_MAX - 2 || !r->read_verified || r->read_seq != 0
		|| !semantic_control_rebind_cut_current() || r->table.observed_members_lo != 15
		|| !semantic_activation_full_ack_table_matches(r->table.observed, 15, 0, r->table.expected,
													   15, 0))
		return false;
	for (unsigned node = 0; node < 4; node++) {
		if (node != (unsigned)cluster_node_id
			&& (!semantic_control_rebind_message_matches(&r->acks[node])
				|| memcmp(&r->acks[node].stream, &r->cut.streams[node],
						  sizeof(r->cut.streams[node]))
					   != 0))
			return false;
		semantic_activation_write_u64_le(live + node * 24, r->table.expected[node].boot_id);
		semantic_activation_write_u64_le(live + node * 24 + 8,
										 r->table.expected[node].capability_word);
		semantic_activation_write_u64_le(live + node * 24 + 16,
										 r->table.expected[node].capability_generation);
	}
	if (!normal_start_sha256(live, sizeof(live), digest) || !semantic_control_rebind_cut_current())
		return false;
	pg_atomic_write_u64(&SemanticControlRebindState->completion_seq, seq + 1);
	pg_write_barrier();
	out->cut = r->cut;
	out->nonce = r->nonce;
	out->table = r->table;
	out->table.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
	memcpy(out->proof_binding, r->proof_binding, 32);
	memcpy(out->live_digest, digest, 32);
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticControlRebindState->completion_seq, seq + 2);
	r->phase = SEMANTIC_CONTROL_REBIND_COMPLETE;
	return true;
}

/* Nonblocking, fixed four-peer work. True retains the original shared
 * QVOTEC owner, including an unconsumed result quarantined after owner loss. */
static bool
semantic_control_rebind_tick(TimestampTz now)
{
	SemanticControlRebindRuntime *r = &semantic_control_rebind;
	SemanticControlRebindCut cut;
	ClusterSemanticActivationAckTableV1 table;
	ClusterSemanticActivationReadCompletion completion;
	SemanticControlRebindCheck check;
	bool changed, incoming = false;
	uint64 shared_read_seq;

	if (!IsUnderPostmaster || !AmLmonProcess() || SemanticControlRebindState == NULL)
		return false;
	check = semantic_control_rebind_memory(&cut, &table, &changed);
	shared_read_seq = pg_atomic_read_u64(&SemanticControlRebindState->read_seq);
	pg_read_barrier();
	if (shared_read_seq != 0
		&& (r->read_seq != shared_read_seq || MyProc == NULL
			|| SemanticControlRebindState->read_owner_pid != MyProcPid
			|| SemanticControlRebindState->read_owner_procno != MyProc->pgprocno
			|| SemanticControlRebindState->read_owner_generation
				   != MyProc->cluster_grd_generation)) {
		/* A successor cannot consume an earlier creator's result. Keep the
		 * original shared owner visible, including completed-but-unconsumed. */
		semantic_control_rebind_clear_completion();
		r->phase = SEMANTIC_CONTROL_REBIND_INVALID;
		return true;
	}
	if (r->read_seq != 0) {
		/* No replacement request may recycle the completed but unconsumed
		 * old result. Its original creator consumes it before losing seq. */
		if (!semantic_activation_record_read_mailbox_poll_completion(r->read_seq, &completion))
			return true;
		pg_atomic_write_u64(&SemanticControlRebindState->read_seq, 0);
		r->read_seq = 0;
		r->read_verified
			= check == SEMANTIC_CONTROL_REBIND_SAME && r->read_nonce == r->nonce
			  && SemanticControlRebindState->read_nonce == r->read_nonce
			  && memcmp(&cut, &r->cut, sizeof(cut)) == 0
			  && memcmp(table.expected, r->table.expected, sizeof(table.expected)) == 0
			  && completion.result == CLUSTER_SEMANTIC_ACTIVATION_OK && !completion.implicit_open
			  && memcmp(completion.selected_bytes, SemanticControlRebindState->baseline.pgsa, 512)
					 == 0;
		if (!r->read_verified && check == SEMANTIC_CONTROL_REBIND_SAME
			&& memcmp(&cut, &r->cut, sizeof(cut)) == 0) {
			r->phase = SEMANTIC_CONTROL_REBIND_INVALID;
			semantic_control_rebind_clear_completion();
			return false;
		}
	}
	if (check != SEMANTIC_CONTROL_REBIND_SAME) {
		semantic_control_rebind_clear_completion();
		memset(r->acks, 0, sizeof(r->acks));
		memset(r->requests, 0, sizeof(r->requests));
		r->read_verified = false;
		r->phase = check == SEMANTIC_CONTROL_REBIND_BAD ? SEMANTIC_CONTROL_REBIND_INVALID
														: SEMANTIC_CONTROL_REBIND_IDLE;
		return false;
	}
	for (unsigned peer = 0; peer < 4; peer++)
		incoming |= r->requests[peer].valid;
	if (r->phase != SEMANTIC_CONTROL_REBIND_IDLE && r->cut.base_seq != 0
		&& (memcmp(&cut, &r->cut, sizeof(cut)) != 0
			|| memcmp(table.expected, r->table.expected, sizeof(table.expected)) != 0)) {
		semantic_control_rebind_clear_completion();
		r->phase = SEMANTIC_CONTROL_REBIND_IDLE;
		r->read_verified = false;
	}
	if (r->phase == SEMANTIC_CONTROL_REBIND_INVALID)
		return false;
	if (r->phase == SEMANTIC_CONTROL_REBIND_IDLE) {
		if (!changed && !incoming)
			return false;
		if (!semantic_control_rebind_begin(&cut, &table)) {
			r->phase = SEMANTIC_CONTROL_REBIND_INVALID;
			return false;
		}
	}
	if (!r->read_verified) {
		uint32 utility
			= pg_atomic_read_u32(&SemanticActivationUtilityMailbox->utility_mailbox_state);
		if (utility != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_WRITING
			&& utility != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
			&& semantic_activation_authority_mailbox_owner(true) < 0
			&& semantic_activation_record_read_mailbox_submit(&r->read_seq)) {
			r->read_nonce = r->nonce;
			SemanticControlRebindState->read_nonce = r->nonce;
			SemanticControlRebindState->read_owner_pid = r->cut.owner_pid;
			SemanticControlRebindState->read_owner_procno = r->cut.owner_procno;
			SemanticControlRebindState->read_owner_generation = r->cut.owner_generation;
			pg_write_barrier();
			pg_atomic_write_u64(&SemanticControlRebindState->read_seq, r->read_seq);
		}
		return r->read_seq != 0;
	}
	/* Replies are independent of our own collection; no wait-for cycle. */
	for (unsigned peer = 0; peer < 4; peer++) {
		SemanticControlRebindIngress *request = &r->requests[peer];
		ClusterSemanticControlRebindWireV3 reply;
		if (peer == (unsigned)cluster_node_id || !request->valid)
			continue;
		if (!semantic_control_rebind_message_matches(request)) {
			memset(request, 0, sizeof(*request));
			continue;
		}
		reply = request->message;
		reply.kind = CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK;
		reply.result = CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK;
		reply.boot_id = r->table.expected[cluster_node_id].boot_id;
		reply.admitted_incarnation = r->table.expected[cluster_node_id].admitted_incarnation;
		reply.capability_word = r->table.expected[cluster_node_id].capability_word;
		if (semantic_control_rebind_send(peer, &reply))
			request->valid = false;
	}
	if (!changed || r->phase == SEMANTIC_CONTROL_REBIND_COMPLETE)
		return false;
	r->phase = SEMANTIC_CONTROL_REBIND_COLLECTING;
	for (unsigned peer = 0; peer < 4; peer++) {
		SemanticControlRebindIngress *ack = &r->acks[peer];
		if (peer == (unsigned)cluster_node_id)
			continue;
		if ((r->table.observed_members_lo & (UINT64_C(1) << peer)) == 0
			&& semantic_control_rebind_message_matches(ack)
			&& memcmp(&ack->stream, &r->cut.streams[peer], sizeof(ack->stream)) == 0
			&& ack->message.round_nonce == r->nonce
			&& ack->capability.generation == r->table.expected[peer].capability_generation) {
			r->table.observed[peer] = r->table.expected[peer];
			r->table.observed_members_lo |= UINT64_C(1) << peer;
		}
		if ((r->table.observed_members_lo & (UINT64_C(1) << peer)) == 0 && now > 0
			&& cluster_interconnect_heartbeat_interval_ms > 0
			&& (r->last_send[peer] == 0
				|| (now >= r->last_send[peer]
					&& now - r->last_send[peer]
						   >= (int64)cluster_interconnect_heartbeat_interval_ms * 1000))) {
			ClusterSemanticControlRebindWireV3 request = semantic_control_rebind_request(peer);
			r->last_send[peer] = now;
			(void)semantic_control_rebind_send(peer, &request);
		}
	}
	(void)semantic_control_rebind_publish();
	return false;
}
