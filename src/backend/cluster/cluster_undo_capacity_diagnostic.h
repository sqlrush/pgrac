/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Process-local, passive first-refusal capture. No authority or shared state.
 */
#ifndef CLUSTER_UNDO_CAPACITY_DIAGNOSTIC_INTERNAL_H
#define CLUSTER_UNDO_CAPACITY_DIAGNOSTIC_INTERNAL_H

#include <stdarg.h>

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
} UndoCapacityRefusalRecord;

StaticAssertDecl(sizeof(UndoCapacityRefusalRecord) <= 1024,
				 "capacity refusal snapshot must remain bounded");
static UndoCapacityRefusalRecord undo_capacity_refusal;
static char undo_capacity_refusal_text[4096];

static void
undo_capacity_refusal_capture(const UndoCapacityRefusalInput *input)
{
	UndoCapacityRefusalRecord *record = &undo_capacity_refusal;

	if (!cluster_trace_undo_capacity_refusal || record->valid)
		return;
	MemSet(record, 0, sizeof(*record));
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
	record->valid = true;
}

static void pg_attribute_printf(2, 3)
undo_capacity_refusal_append(size_t *used, const char *format, ...)
{
	va_list args;
	int n;
	size_t remaining = sizeof(undo_capacity_refusal_text) - *used;

	if (remaining <= 1)
		return;
	va_start(args, format);
	n = vsnprintf(undo_capacity_refusal_text + *used, remaining, format, args);
	va_end(args);
	if (n < 0 || (size_t)n >= remaining) {
		static const char marker[] = " truncated=1";

		memcpy(undo_capacity_refusal_text + sizeof(undo_capacity_refusal_text) - sizeof(marker),
			   marker, sizeof(marker));
		*used = sizeof(undo_capacity_refusal_text) - 1;
	} else
		*used += n;
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

/* One-shot log-only detail for the original error, after original CV cleanup. */
const char *
cluster_undo_cleaner_capacity_refusal_detail(uint32 segment, TransactionId top_xid,
										   bool active_slot_pending)
{
	const UndoCapacityRefusalRecord *record = &undo_capacity_refusal;
	const UndoCapacityRefusalInput *input = &record->input;
	size_t used = 0;
	bool valid = record->valid;

	undo_capacity_refusal.valid = false;
	if (!cluster_trace_undo_capacity_refusal || !valid || input->segment != segment)
		return NULL;
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
	return undo_capacity_refusal_text;
}
#endif
