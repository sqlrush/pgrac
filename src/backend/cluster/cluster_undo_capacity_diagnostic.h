/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Process-local, passive first-refusal capture. No authority or shared state.
 */
#ifndef CLUSTER_UNDO_CAPACITY_DIAGNOSTIC_INTERNAL_H
#define CLUSTER_UNDO_CAPACITY_DIAGNOSTIC_INTERNAL_H

#include <stdarg.h>
#include "cluster_undo_capacity_trace.h"

enum UndoCapacityDiagnosticValue {
	D_ENABLED, D_STORAGE, D_CONTEXT, D_HANDLED, D_PROOF, D_PRODUCER,
	D_STATUS, D_HAVE_FIRST, D_PROTECTOR, D_ADVANCE, D_CURRENT,
	D_FLOOR, D_SUPPLIED, D_OWNER_CHANGED, D_WORKER, D_REASON, D_COUNT
};
#define D_BIT(value) (UINT32_C(1) << (value))

/* Pointers are only borrowed synchronously at an actual refusal. */
typedef struct UndoCapacityRefusalInput {
	const char *site;
	uint32 segment;
	const ClusterCtrcTxnKeyV1 *key;
	const ClusterCapacityObservationV1 *observation;
	const ClusterCapacityHistoryPass *pass;
	const ClusterHeapFreezeCohortObservation *history;
	const ClusterHeapFreezeCohortObservation *first;
	const ClusterUndoHorizonFloor *floor;
	const uint64 *owners;
	uint64 first_change;
	uint64 probe_epoch, route_epoch;
	bool probe_epoch_known, route_epoch_known;
	uint64 coordinator_generation;
	uint64 worker_generation;
	uint32 known;
	int values[D_COUNT];
} UndoCapacityRefusalInput;

typedef struct UndoCapacityRefusalRecord {
	bool valid;
	bool trace_enabled;
	int pid, node;
	UndoCapacityRefusalInput input;
	bool key_known, observation_known, pass_known, history_known, first_known, floor_known;
	bool owners_known;
	ClusterCtrcTxnKeyV1 key;
	ClusterCapacityObservationV1 observation;
	ClusterCapacityHistoryPass pass;
	uint64 horizon, epoch, formation, record_generation, system_identifier;
	uint64 publication_sequence, membership_generation, local_boot;
	uint64 admitted_lo, admitted_hi, local_report_sequence, first_report_sequence;
	uint64 expected_coordinator, expected_worker;
	size_t history_text_bytes;
	UndoCapacityRouteTrace trace;
} UndoCapacityRefusalRecord;

static char undo_capacity_refusal_text[16384];
static size_t undo_capacity_refusal_text_limit = sizeof(undo_capacity_refusal_text);
/* Two disjoint history locals plus one nested CURRENT/pool candidate row. */
StaticAssertDecl(sizeof(UndoCapacityRefusalRecord) + 3 * sizeof(UndoCapacityTraceRow)
				 + sizeof(undo_capacity_refusal_text_limit) <= 16384,
				 "capacity refusal snapshot must remain bounded");
static UndoCapacityRefusalRecord undo_capacity_refusal;
StaticAssertDecl(sizeof(undo_capacity_refusal_text) <= 16384,
				 "capacity refusal output must remain bounded");
static void undo_capacity_refusal_history_sources(const ClusterHeapFreezeCohortObservation *history);

void
cluster_undo_cleaner_capacity_trace_reset(void)
{
	undo_capacity_refusal.trace_enabled = cluster_trace_undo_capacity_refusal;
	if (undo_capacity_refusal.trace_enabled)
		MemSet(&undo_capacity_refusal.trace, 0, sizeof(undo_capacity_refusal.trace));
}

void
cluster_undo_cleaner_capacity_trace_row(const UndoCapacityTraceRow *row)
{
	UndoCapacityRouteTrace *trace = &undo_capacity_refusal.trace;
	UndoCapacityTraceRow *to = NULL;

	if (!cluster_trace_undo_capacity_refusal || !undo_capacity_refusal.trace_enabled || row == NULL)
		return;
	if (row->kind == CAP_TRACE_CURRENT && row->position < lengthof(trace->current)) {
		to = &trace->current[row->position];
		if ((to->known & CAP_TRACE_PRESENT) == 0) trace->current_count++;
	} else if (row->kind == CAP_TRACE_POOL && trace->pool_count < lengthof(trace->pool))
		to = &trace->pool[trace->pool_count++];
	else if (row->kind == CAP_TRACE_FALLBACK)
		to = &trace->fallback;
	else if (row->kind == CAP_TRACE_HISTORY)
		to = &trace->history;
	if (to == NULL || (to->known & CAP_TRACE_PRESENT) != 0) {
		if (trace->dropped != UINT32_MAX) trace->dropped++;
		return;
	}
	*to = *row;
	to->known |= CAP_TRACE_PRESENT;
}

void
cluster_undo_cleaner_capacity_trace_counts(unsigned current_gets, unsigned pool_gets, bool captured)
{
	if (cluster_trace_undo_capacity_refusal && undo_capacity_refusal.trace_enabled) {
		undo_capacity_refusal.trace.counters_known = true;
		undo_capacity_refusal.trace.current_gets = current_gets;
		undo_capacity_refusal.trace.pool_gets = pool_gets;
		undo_capacity_refusal.trace.current_captured = captured;
	}
}

void
cluster_undo_cleaner_capacity_trace_final_read(bool ready)
{
	UndoCapacityRouteTrace *trace = &undo_capacity_refusal.trace;

	if (!cluster_trace_undo_capacity_refusal || !undo_capacity_refusal.trace_enabled)
		return;
	trace->final_read_known = true;
	trace->final_read_ready = ready;
	trace->final_snapshot_known = trace->final_snapshot_equal = false;
	trace->final_field = ready ? NULL : "snapshot_read";
	trace->final_slot = -1;
	trace->final_before = trace->final_after = 0;
}

void
cluster_undo_cleaner_capacity_trace_final(bool equal,
	const ClusterTTSlotCurrentSnapshot *before, const ClusterTTSlotCurrentSnapshot *after)
{
	UndoCapacityRouteTrace *trace = &undo_capacity_refusal.trace;

	if (!cluster_trace_undo_capacity_refusal || !undo_capacity_refusal.trace_enabled)
		return;
	trace->final_snapshot_known = true;
	trace->final_snapshot_equal = equal;
	trace->final_slot = -1;
	trace->final_field = NULL;
	trace->final_before = trace->final_after = 0;
	if (equal)
		return;
#define FINAL_FIELD(name, left, right) \
	do { if ((left) != (right)) { \
		trace->final_field = (name); trace->final_before = (left); trace->final_after = (right); \
		return; } } while (0)
	FINAL_FIELD("binding", before->binding_generation, after->binding_generation);
	FINAL_FIELD("segment", before->segment_id, after->segment_id);
	FINAL_FIELD("reserved", before->reserved32, after->reserved32);
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const ClusterTTSlotCurrentOwner *a = &before->slots[i], *b = &after->slots[i];

		trace->final_slot = i;
		FINAL_FIELD("owner.segment", a->segment_id, b->segment_id);
		FINAL_FIELD("owner.xid", a->xid, b->xid);
		FINAL_FIELD("owner.scn", a->commit_scn, b->commit_scn);
		FINAL_FIELD("owner.slot", a->slot_offset, b->slot_offset);
		FINAL_FIELD("owner.wrap", a->wrap, b->wrap);
		FINAL_FIELD("owner.status", a->status, b->status);
		for (unsigned j = 0; j < sizeof(a->reserved8); j++)
			FINAL_FIELD("owner.reserved", a->reserved8[j], b->reserved8[j]);
	}
	/* A padding-only difference is not assigned a semantic field. */
	trace->final_slot = -1;
	trace->final_field = "padding_unknown";
#undef FINAL_FIELD
}

static void
undo_capacity_refusal_capture(const UndoCapacityRefusalInput *input)
{
	UndoCapacityRefusalRecord *record = &undo_capacity_refusal;

	if (!cluster_trace_undo_capacity_refusal || record->valid)
		return;
	/* Preserve the invocation's diagnostic arm and rows across final capture. */
	MemSet(&record->pid, 0, offsetof(UndoCapacityRefusalRecord, trace)
		- offsetof(UndoCapacityRefusalRecord, pid));
	record->pid = MyProcPid;
	record->node = cluster_node_id;
	record->input = *input;
	for (unsigned i = 0; i < D_COUNT; i++)
		if ((input->known & D_BIT(i)) == 0)
			record->input.values[i] = -1;
	if ((record->key_known = input->key != NULL))
		record->key = *input->key;
	if ((record->observation_known = input->observation != NULL))
		record->observation = *input->observation;
	if ((record->pass_known = input->pass != NULL))
		record->pass = *input->pass;
	if ((record->history_known = input->history != NULL)) {
		const ClusterHeapFreezeCohortObservation *history = input->history;

		record->horizon = history->horizon_scn;
		record->formation = history->formation_epoch;
		record->record_generation = history->record_generation;
		record->system_identifier = history->admission_fence.system_identifier;
		record->publication_sequence = history->admission_fence.admission_publication_seq;
		record->membership_generation = history->admission_fence.membership_cut_generation;
		record->local_boot = history->admission_fence.local_boot_incarnation;
		record->admitted_lo = history->admitted_members_lo;
		record->admitted_hi = history->admitted_members_hi;
		record->local_report_sequence = history->source_local_report_sequence;
	}
	if ((record->first_known = input->first != NULL))
		record->first_report_sequence = input->first->source_local_report_sequence;
	if ((record->floor_known = input->floor != NULL)) {
		record->horizon = input->floor->scn;
		record->epoch = input->floor->epoch;
	}
	if (input->owners != NULL && (input->known & D_BIT(D_WORKER)) != 0
		&& input->values[D_WORKER] >= 0
		&& input->values[D_WORKER] < CLUSTER_UNDO_CLEANER_WORKER_TYPES) {
		record->owners_known = true;
		record->expected_coordinator = input->owners[0];
		record->expected_worker = input->owners[input->values[D_WORKER]];
	}
	/* No caller stack pointer survives the capture. */
	record->input.key = NULL;
	record->input.observation = NULL;
	record->input.pass = NULL;
	record->input.history = record->input.first = NULL;
	record->input.floor = NULL;
	record->input.owners = NULL;
	undo_capacity_refusal_history_sources(input->history);
	record->valid = true;
}

static void pg_attribute_printf(2, 3)
undo_capacity_refusal_append(size_t *used, const char *format, ...)
{
	va_list args;
	int n;
	size_t remaining = undo_capacity_refusal_text_limit - *used;

	if (remaining <= 1)
		return;
	va_start(args, format);
	n = vsnprintf(undo_capacity_refusal_text + *used, remaining, format, args);
	va_end(args);
	if (n < 0 || (size_t)n >= remaining) {
		static const char marker[] = " truncated=1";

		memcpy(undo_capacity_refusal_text + undo_capacity_refusal_text_limit - sizeof(marker),
			   marker, sizeof(marker));
		*used = undo_capacity_refusal_text_limit - 1;
	} else
		*used += n;
}

/* Render only the existing value copy while it is borrowed. Keep this section
 * in the original output buffer; no second cohort or text buffer is retained. */
static void
undo_capacity_refusal_history_sources(const ClusterHeapFreezeCohortObservation *history)
{
	size_t used = 0;
	unsigned matched = 0, emitted = 0;

	undo_capacity_refusal_text_limit = 4096 - 96; /* reserve the count footer */
	undo_capacity_refusal_append(&used, " h_sources_v1={known=%d", history != NULL);
	if (history != NULL)
		for (int peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
			uint64 members = peer < 64 ? history->admitted_members_lo : history->admitted_members_hi;
			bool local = peer == undo_capacity_refusal.node;
			const ClusterHeapFreezeHistorySourceObservationV1 *source;
			uint64 horizon, sequence;
			int bytes;

			if ((members & (UINT64CONST(1) << (peer % 64))) == 0)
				continue;
			horizon = local ? history->source_local_horizon_scn : history->source_peer_horizon_scn[peer];
			if (scn_local(horizon) != scn_local(history->horizon_scn))
				continue;
			source = local ? &history->source_local_history : &history->source_peer_history[peer];
			sequence = local ? history->source_local_report_sequence : history->source_peer_report_sequence[peer];
			matched++;
#define H_SOURCE_FORMAT ",source={node=%d,boot=" UINT64_FORMAT ",h=" UINT64_FORMAT \
			",clock=" UINT64_FORMAT ",kind=%u,token=" UINT64_FORMAT ",report_sequence=" UINT64_FORMAT "}"
#define H_SOURCE_VALUES peer, history->admission_fence.admitted_incarnation[peer], horizon, \
			source->sample_clock_scn, (unsigned)source->contributor_kind, source->contributor_token, sequence
			bytes = snprintf(NULL, 0, H_SOURCE_FORMAT, H_SOURCE_VALUES);
			if (bytes < 0 || (size_t)bytes >= undo_capacity_refusal_text_limit - used)
				continue;
			undo_capacity_refusal_append(&used, H_SOURCE_FORMAT, H_SOURCE_VALUES);
			emitted++;
#undef H_SOURCE_FORMAT
#undef H_SOURCE_VALUES
		}
	undo_capacity_refusal_text_limit = 4096;
	if (history != NULL)
		undo_capacity_refusal_append(&used, ",matched=%u,emitted=%u,dropped=%u,truncated=%d}",
			matched, emitted, matched - emitted, matched != emitted);
	else
		undo_capacity_refusal_append(&used, "}");
	undo_capacity_refusal.history_text_bytes = used;
	undo_capacity_refusal_text_limit = sizeof(undo_capacity_refusal_text);
}

static void
undo_capacity_refusal_key(size_t *used, const char *name, const ClusterCtrcTxnKeyV1 *key)
{
	undo_capacity_refusal_append(used,
		" %s.segment=%u %s={version=%u,owner=%u,origin=%u,generation=%u,slot=%u,wrap=%u,"
		"xid=%u,epoch=%u,system=" UINT64_FORMAT ",boot=" UINT64_FORMAT
		",formation=" UINT64_FORMAT ",record_generation=" UINT64_FORMAT
		",descriptor=" UINT64_FORMAT ",root=" UINT64_FORMAT ",root_generation=" UINT64_FORMAT
		",reserved=",
		name, key->segment_id, name, key->format_version, key->owner_instance,
		key->origin_node_id, key->segment_generation, key->slot_offset, key->slot_wrap,
		key->xid, key->cluster_epoch, key->system_identifier, key->origin_boot_incarnation,
		key->formation_epoch, key->admission_record_generation, key->root_descriptor_incarnation,
		key->root_id, key->root_generation);
	for (unsigned i = 0; i < sizeof(key->reserved); i++)
		undo_capacity_refusal_append(used, "%02x", (unsigned)key->reserved[i]);
	undo_capacity_refusal_append(used, "}");
}

static void
undo_capacity_refusal_identity(size_t *used, const char *name,
							  const ClusterCapacitySegmentIdentityV1 *identity)
{
	undo_capacity_refusal_append(used,
		" %s={owner=%u,segment=%u,generation=%u,current=%u,binding=" UINT64_FORMAT
		",epoch=" UINT64_FORMAT ",system=" UINT64_FORMAT ",boot=" UINT64_FORMAT
		",formation=" UINT64_FORMAT ",record_generation=" UINT64_FORMAT
		",root=" UINT64_FORMAT ",root_generation=" UINT64_FORMAT "}",
		name, identity->owner_instance, identity->segment_id, identity->segment_generation,
		identity->current_segment, identity->current_binding_generation, identity->epoch,
		identity->system_identifier, identity->origin_boot_incarnation, identity->formation_epoch,
		identity->admission_record_generation, identity->root_id, identity->root_generation);
}

/* Compact field order is fixed for offline decoding. known/native_known are
 * mandatory: a zero from an unexecuted branch is never evidence. */
static void
undo_capacity_refusal_route_row(size_t *used, const UndoCapacityTraceRow *row)
{
	if ((row->known & CAP_TRACE_PRESENT) == 0)
		return;
	undo_capacity_refusal_append(used,
		" row={kind=%u,pos=%u,branch=%u,result=%d,gate=%s,known=%x,capacity_mode=%d,native=%x/%x,sample_slot=%u",
		row->kind, row->position, row->branch, row->result,
		row->gate != NULL ? row->gate : "NONE", row->known,
		(row->known & CAP_TRACE_SAMPLE) != 0 ? (row->known & CAP_TRACE_CAPACITY_MODE) != 0 : -1,
		row->native_values, row->native_known, row->sample_slot);
	if ((row->known & CAP_TRACE_KEY) != 0) {
		const ClusterCtrcTxnKeyV1 *key = &row->key;

		undo_capacity_refusal_append(used,
			",key=[%u,%u,%u,%u,%u,%u,%u,%u,%u," UINT64_FORMAT "," UINT64_FORMAT
			"," UINT64_FORMAT "," UINT64_FORMAT "," UINT64_FORMAT "," UINT64_FORMAT "," UINT64_FORMAT ",",
			key->format_version, key->owner_instance, key->origin_node_id, key->segment_id,
			key->segment_generation, key->slot_offset, key->slot_wrap, key->xid, key->cluster_epoch,
			key->system_identifier, key->origin_boot_incarnation, key->formation_epoch,
			key->admission_record_generation, key->root_descriptor_incarnation,
			key->root_id, key->root_generation);
		for (unsigned i = 0; i < sizeof(key->reserved); i++)
			undo_capacity_refusal_append(used, "%02x", (unsigned)key->reserved[i]);
		undo_capacity_refusal_append(used, "]");
	}
	if ((row->known & CAP_TRACE_OWNER) != 0)
		undo_capacity_refusal_append(used,
			",owner=[%u,%u,%u,%u,%u," UINT64_FORMAT "," UINT64_FORMAT ",%02x%02x%02x]",
			row->owner_data.owner.segment_id, row->owner_data.owner.slot_offset, row->owner_data.owner.xid, row->owner_data.owner.wrap,
			row->owner_data.owner.status, row->owner_data.owner.commit_scn, row->binding,
			(unsigned)row->owner_data.owner.reserved8[0], (unsigned)row->owner_data.owner.reserved8[1],
			(unsigned)row->owner_data.owner.reserved8[2]);
	if ((row->known & CAP_TRACE_CLEANER) != 0)
		undo_capacity_refusal_append(used, ",cleaner=[" UINT64_FORMAT "," UINT64_FORMAT "]",
			row->owner_data.cleaner.coordinator, row->owner_data.cleaner.worker);
	if ((row->known & CAP_TRACE_ADMISSION) != 0)
		undo_capacity_refusal_append(used, ",admission=[" UINT64_FORMAT "," UINT64_FORMAT "]",
			row->formation, row->record_generation);
	if ((row->known & CAP_TRACE_ROOT) != 0)
		undo_capacity_refusal_append(used, ",root=[" UINT64_FORMAT "," UINT64_FORMAT "]",
			row->root, row->root_generation);
	if ((row->known & CAP_TRACE_GENERATION) != 0)
		undo_capacity_refusal_append(used, ",generation=[%u," UINT64_FORMAT "]",
			row->generation_result, row->generation);
	if ((row->known & CAP_TRACE_SLOT) != 0) {
		undo_capacity_refusal_append(used, ",slot=[%u,%u,%u,%u," UINT64_FORMAT ",",
			row->slot.xid, row->slot.wrap, row->slot.status, row->slot.flags, row->slot.commit_scn);
		for (unsigned i = 0; i < sizeof(row->slot.first_undo_block); i++)
			undo_capacity_refusal_append(used, "%02x", ((const uint8 *)&row->slot.first_undo_block)[i]);
		undo_capacity_refusal_append(used, "]");
	}
	undo_capacity_refusal_append(used, ",acquire=%u,release=%u,failure=%u}",
		row->acquire, row->release, row->failure);
}

/* One-shot log-only detail for the original error, after original CV cleanup. */
const char *
cluster_undo_cleaner_capacity_refusal_detail(uint32 segment, TransactionId top_xid,
										   bool active_slot_pending)
{
	const UndoCapacityRefusalRecord *record = &undo_capacity_refusal;
	const UndoCapacityRefusalInput *input = &record->input;
	size_t used = 0;
	size_t history_bytes = record->history_text_bytes;
	bool valid = record->valid;

	undo_capacity_refusal.valid = false;
	if (!cluster_trace_undo_capacity_refusal || !valid || input->segment != segment)
		return NULL;
	if (history_bytes >= 4096)
		return NULL;
	/* Protect the captured section while formatting the original header. */
	memmove(undo_capacity_refusal_text + sizeof(undo_capacity_refusal_text) - history_bytes,
			undo_capacity_refusal_text, history_bytes);
	undo_capacity_refusal_text_limit = sizeof(undo_capacity_refusal_text) - history_bytes;
#define D_VALUE(value) input->values[value]
	undo_capacity_refusal_append(&used,
		"undo_capacity_first_refusal_v1 site=%s pid=%d node=%d segment=%u "
		"top_xid=%u active_slot_pending=%d enabled=%d storage=%d context=%d "
		"history_handled=%d proof=%d producer=%d status=%d have_first=%d protector=%d "
		"reports_advance=%d change_current=%d floor_ready=%d worker=%d supplied=%d owner_changed=%d "
		"probe_reason=%d key_known=%d observation_known=%d pass_known=%d history_known=%d first_known=%d floor_known=%d "
		"owners_known=%d actual_owners_known=%d probe_epoch_known=%d probe_epoch=" UINT64_FORMAT " route_epoch_known=%d route_epoch=" UINT64_FORMAT
		" horizon=" UINT64_FORMAT " epoch=" UINT64_FORMAT " formation=" UINT64_FORMAT
		" record_generation=" UINT64_FORMAT " system=" UINT64_FORMAT
		" publication_sequence=" UINT64_FORMAT " membership_generation=" UINT64_FORMAT
		" local_boot=" UINT64_FORMAT " admitted_lo=" UINT64_FORMAT " admitted_hi=" UINT64_FORMAT
		" local_report_sequence=" UINT64_FORMAT " first_report_sequence=" UINT64_FORMAT
		" first_change=" UINT64_FORMAT " expected_coordinator=" UINT64_FORMAT
		" expected_worker=" UINT64_FORMAT " actual_coordinator=" UINT64_FORMAT
		" actual_worker=" UINT64_FORMAT,
		input->site, record->pid, record->node, segment, top_xid, active_slot_pending,
		D_VALUE(D_ENABLED), D_VALUE(D_STORAGE), D_VALUE(D_CONTEXT), D_VALUE(D_HANDLED),
		D_VALUE(D_PROOF), D_VALUE(D_PRODUCER), D_VALUE(D_STATUS), D_VALUE(D_HAVE_FIRST),
		D_VALUE(D_PROTECTOR), D_VALUE(D_ADVANCE), D_VALUE(D_CURRENT), D_VALUE(D_FLOOR),
		D_VALUE(D_WORKER), D_VALUE(D_SUPPLIED), D_VALUE(D_OWNER_CHANGED), D_VALUE(D_REASON), record->key_known,
		record->observation_known, record->pass_known, record->history_known,
		record->first_known, record->floor_known, record->owners_known,
		(input->known & D_BIT(D_SUPPLIED)) != 0, input->probe_epoch_known, input->probe_epoch,
		input->route_epoch_known, input->route_epoch, record->horizon, record->epoch, record->formation,
		record->record_generation, record->system_identifier, record->publication_sequence,
		record->membership_generation, record->local_boot, record->admitted_lo, record->admitted_hi,
		record->local_report_sequence, record->first_report_sequence, input->first_change,
		record->expected_coordinator, record->expected_worker,
		input->coordinator_generation, input->worker_generation);
#undef D_VALUE
	memmove(undo_capacity_refusal_text + used,
			undo_capacity_refusal_text + sizeof(undo_capacity_refusal_text) - history_bytes, history_bytes);
	used += history_bytes;
	undo_capacity_refusal_text[used] = '\0';
	undo_capacity_refusal_text_limit = sizeof(undo_capacity_refusal_text);
	if (record->key_known)
		undo_capacity_refusal_key(&used, "input_key", &record->key);
	if (record->observation_known) {
		const ClusterCapacityObservationV1 *observation = &record->observation;

		undo_capacity_refusal_append(&used,
			" observation={reason=%d,scope=%d,phase=%d,worker=%u,owner_generation=" UINT64_FORMAT
			",coordinator_generation=" UINT64_FORMAT ",change_sequence=" UINT64_FORMAT
			",pool_positions=" UINT64_FORMAT ",unproven=%u,retain=%u}",
			observation->reason, observation->scope, observation->phase, observation->owner_worker,
			observation->owner_generation, observation->coordinator_generation,
			observation->observed_change_sequence, observation->pool_positions,
			observation->unproven_reasons, observation->retain_reasons);
		undo_capacity_refusal_key(&used, "observed_key", &observation->key);
		undo_capacity_refusal_identity(&used, "observed_segment", &observation->segment);
	}
	if (record->pass_known) {
		const ClusterCapacityHistoryPass *pass = &record->pass;

		undo_capacity_refusal_append(&used,
			" pass.complete=%d pass={started=%d,incomplete=%d,progress=%d,history_only=%d,"
			"current_proven=%d,saw_history=%d,horizon=" UINT64_FORMAT ",change_sequence=" UINT64_FORMAT
			",next=%u,covered=%u,allocated=%u,first_absent=%u,record_segment=%u,configured_cap=%u}",
			pass->complete, pass->started, pass->incomplete, pass->progress, pass->history_only,
			pass->current_proven, pass->saw_history, pass->horizon, pass->change_sequence,
			pass->next_position, pass->covered_positions, pass->allocated_count,
			pass->first_absent_position, pass->record_segment, pass->configured_cap);
		undo_capacity_refusal_identity(&used, "pass_identity", &pass->identity);
	}
	if (!record->trace_enabled) {
		undo_capacity_refusal_append(&used, " route_v2={enabled=0}");
		return undo_capacity_refusal_text;
	}
	undo_capacity_refusal_append(&used,
		" route_v2={current=%u,pool=%u,dropped=%u,counters_known=%d,current_gets=%u,pool_gets=%u,"
		"current_captured=%d,final_read_known=%d,final_read_ready=%d,"
		"final_snapshot_known=%d,final_snapshot_equal=%d,final_slot=%d,"
		"final_field=%s,final_before=" UINT64_FORMAT ",final_after=" UINT64_FORMAT "}",
		record->trace.current_count, record->trace.pool_count, record->trace.dropped,
		record->trace.counters_known, record->trace.current_gets, record->trace.pool_gets,
		record->trace.current_captured, record->trace.final_read_known, record->trace.final_read_ready,
		record->trace.final_snapshot_known,
		record->trace.final_snapshot_equal, record->trace.final_slot,
		record->trace.final_field != NULL ? record->trace.final_field : "UNKNOWN",
		record->trace.final_before, record->trace.final_after);
	for (unsigned i = 0; i < lengthof(record->trace.current); i++)
		undo_capacity_refusal_route_row(&used, &record->trace.current[i]);
	undo_capacity_refusal_route_row(&used, &record->trace.fallback);
	undo_capacity_refusal_route_row(&used, &record->trace.history);
	for (unsigned i = 0; i < record->trace.pool_count; i++)
		undo_capacity_refusal_route_row(&used, &record->trace.pool[i]);
	return undo_capacity_refusal_text;
}
#endif
