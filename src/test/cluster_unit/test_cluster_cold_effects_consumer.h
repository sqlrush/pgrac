/*-------------------------------------------------------------------------
 * test_cluster_cold_effects_consumer.h
 *    Original Startup consumers with real decoding and replay sequencing.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_cold_effects_consumer.h
 * NOTES
 *    PGRAC-original. Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *-------------------------------------------------------------------------
 */

/* The decoder, sealed plan, sequencer, effect proof and two Startup callback
 * bodies are product code. Native/page I/O, foreign isolation and SIDE owners
 * are fault-injectable boundaries. No SPACE record is supplied here. This
 * proves dispatch and failure propagation, not durable ordinary effects. */
typedef enum ConsumerFault {
	CONSUMER_OK,
	CONSUMER_CONTROL_REFUSE,
	CONSUMER_SIDE_REFUSE,
	CONSUMER_GATE_INTERRUPT,
	CONSUMER_NATIVE_INTERRUPT,
	CONSUMER_PAGE_INTERRUPT,
	CONSUMER_PAYLOAD_CHANGED,
	CONSUMER_ACTION_CHANGED
} ConsumerFault;

typedef struct ConsumerFixture {
	/* Must be first: the sequencer and the original callbacks share arg. */
	ClusterColdTypedReplay replay;
	ClusterColdTypedV1 typed;
	ClusterColdParticipantV1 cuts[2];
	struct ClusterRecoveryFencePlan *fence;
	EffectRecord *current;
	ClusterColdReplayResultV1 result;
	uint32 next;
	uint32 count;
	bool page;
	bool skip;
	bool gate_passed;
	ConsumerFault fault;
	char trace[16];
	uint32 events;
	uint32 mismatches;
	uint32 interruptions;
	sigjmp_buf escape;
} ConsumerFixture;

static ConsumerFixture *consumer_active;

static void
consumer_event(char event)
{
	ConsumerFixture *h = consumer_active;

	if (h == NULL || h->events + 1 >= sizeof(h->trace))
		abort();
	h->trace[h->events++] = event;
	h->trace[h->events] = '\0';
}

static void
pg_attribute_noreturn()
	cluster_typed_replay_mismatch(ClusterColdTypedReplay *rep, uint32 participant, XLogRecPtr ptr,
								  const char *reason)
{
	ConsumerFixture *h = consumer_active;

	UT_ASSERT(rep == &h->replay && participant == 1 && reason != NULL);
	UT_ASSERT_EQ(ptr, h->current->reader.ReadRecPtr);
	h->mismatches++;
	siglongjmp(h->escape, 1);
}

static void
cluster_typed_replay_foreign_gate(ClusterColdTypedReplay *rep)
{
	ConsumerFixture *h = consumer_active;

	UT_ASSERT(rep == &h->replay && h->typed.own_participant != 1);
	consumer_event('G');
	if (h->fault == CONSUMER_GATE_INTERRUPT) {
		h->interruptions++;
		siglongjmp(h->escape, 1);
	}
	h->gate_passed = true;
}

static void
cluster_typed_replay_apply(XLogReaderState *r, void *arg)
{
	ConsumerFixture *h = consumer_active;
	ClusterColdTypedReplay *rep = arg;
	bool foreign = h->typed.own_participant != 1;

	UT_ASSERT(rep == &h->replay && r == &h->current->reader);
	UT_ASSERT_EQ(rep->apply_foreign, foreign);
	UT_ASSERT(!foreign || h->gate_passed);
	consumer_event('N');
	if (h->fault == CONSUMER_NATIVE_INTERRUPT) {
		h->interruptions++;
		siglongjmp(h->escape, 1);
	}
}

static void
cluster_typed_replay_space_step(ClusterColdTypedReplay *rep pg_attribute_unused(),
								uint32 participant pg_attribute_unused(),
								const ClusterColdStepV1 *step pg_attribute_unused(),
								XLogReaderState *r pg_attribute_unused())
{
	UT_ASSERT(false); /* These cuts have no SPACE inputs. */
	abort();
}

void
AdvanceNextFullTransactionIdPastXid(TransactionId xid)
{
	UT_ASSERT_EQ(xid, 100);
	UT_ASSERT_EQ(consumer_active->typed.own_participant, 1);
	consumer_event('X');
}

void
cluster_vis_bump_merged_records_applied(void)
{
	UT_ASSERT(false); /* Foreign SIDE and PAGE never use unscheduled native redo. */
}

bool
cluster_cold_typed_side_record_v1(ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence,
								  uint32 participant, const ClusterColdRecordV1 *record)
{
	ConsumerFixture *h = consumer_active;
	bool control = record->rmid == RM_XLOG_ID;

	UT_ASSERT(typed == &h->typed && fence == h->fence);
	UT_ASSERT_EQ(participant, 1);
	UT_ASSERT(h->typed.own_participant != 1);
	UT_ASSERT_EQ(record->read_rec_ptr, h->current->reader.ReadRecPtr);
	if (control) {
		UT_ASSERT_EQ(record->record_flags, CLUSTER_COLD_RECORD_FOREIGN_CONTROL);
		UT_ASSERT(!h->gate_passed);
	} else {
		UT_ASSERT_EQ(record->record_flags, CLUSTER_COLD_RECORD_FOREIGN_SIDE);
		UT_ASSERT(h->gate_passed);
	}
	consumer_event(control ? 'C' : 'S');
	return h->fault != (control ? CONSUMER_CONTROL_REFUSE : CONSUMER_SIDE_REFUSE);
}

void
cluster_cold_apply_step_v1(const ClusterColdStepV1 *step, XLogReaderState *record,
						   ClusterColdApplyRecordV1 apply, void *arg)
{
	ConsumerFixture *h = consumer_active;

	UT_ASSERT(h->page && !h->skip && !step->all_skip);
	UT_ASSERT(record == &h->current->reader);
	UT_ASSERT(h->typed.own_participant == 1 || h->gate_passed);
	consumer_event('H');
	if (h->fault == CONSUMER_PAGE_INTERRUPT) {
		h->interruptions++;
		siglongjmp(h->escape, 1);
	}
	apply(record, arg);
}

#ifndef CLUSTER_COLD_REDO_HOOK_CONSUMER_V1
#define CLUSTER_COLD_REDO_HOOK_CONSUMER_V1 1
#endif
#include "test_cluster_cold_effects_consumer.inc"

static void
consumer_record(ConsumerFixture *h, uint32 index)
{
	EffectRecord *r = h->current;

	if (h->page) {
		RfPageVersionEdgeEntryV1 *edge;

		record_init(r, RM_XLOG_ID, XLOG_FPI);
		r->decoded.max_block_id = 0;
		r->decoded.blocks[0].in_use = true;
		r->decoded.blocks[0].rlocator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 5, 123 };
		r->decoded.blocks[0].forknum = MAIN_FORKNUM;
		r->decoded.has_page_version_edge = true;
		r->decoded.page_version_edge.entry_count = 1;
		r->decoded.page_version_edge.result_token = 9;
		edge = &r->decoded.page_version_edge.entries[0];
		edge->page_class = RF_PAGE_CLASS_ORDINARY;
		edge->before_kind = RF_PAGE_STATE_PRESENT;
		edge->result_kind = RF_PAGE_STATE_PRESENT;
		edge->before.mutation_token = 8;
		memset(edge->before.segment_incarnation, 1, 16);
		memset(edge->result_incarnation, 1, 16);
	} else if (index == 0) {
		Oid next_oid = 18000;

		record_init(r, RM_XLOG_ID, XLOG_NEXTOID);
		r->decoded.header.xl_xid = InvalidTransactionId;
		append(r, &next_oid, sizeof(next_oid));
	} else
		completion(r, true, XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT);
	r->reader.ReadRecPtr = ABORT_REPLAY_LSN(index);
	r->reader.EndRecPtr = ABORT_REPLAY_LSN(index + 1);
	r->reader.system_identifier = 99;
	r->decoded.lsn = r->reader.ReadRecPtr;
	r->decoded.next_lsn = r->reader.EndRecPtr;
	r->decoded.header.xl_prev = index ? ABORT_REPLAY_LSN(index - 1) : 0xfc0;
	r->decoded.header.xl_crc = 200 + index;
}

static bool
consumer_observe(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	ConsumerFixture *h = arg;

	UT_ASSERT(h->page && page->locator.relNumber == 123);
	memset(out, 0, sizeof(*out));
	out->kind = CLUSTER_COLD_DATA_PRESENT;
	out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
	memset(out->version.segment_incarnation, 1, 16);
	out->version.mutation_token = h->skip ? 9 : 8;
	return true;
}

static ConsumerFixture *
consumer_create(bool foreign, bool page, bool skip)
{
	ConsumerFixture *h = calloc(1, sizeof(*h));
	ClusterColdDiagV1 diag;

	if (h == NULL)
		abort();
	h->current = calloc(1, sizeof(*h->current) + sizeof(DecodedBkpBlock));
	if (h->current == NULL)
		abort();
	h->page = page;
	h->skip = skip;
	h->count = page ? 1 : 2;
	h->cuts[0] = (ClusterColdParticipantV1){ .thread_id = 2,
											 .timeline = 1,
											 .owner_incarnation = 11,
											 .physical_lower = 0x1000,
											 .native_redo = 0x1000,
											 .tail_end = 0x1000 };
	h->cuts[1] = h->cuts[0];
	h->cuts[1].thread_id = 1;
	h->cuts[1].owner_incarnation = 12;
	h->cuts[1].tail_end = ABORT_REPLAY_LSN(h->count);
	h->typed.participant_count = 2;
	h->typed.own_participant = foreign ? 0 : 1;
	h->typed.system_identifier = 99;
	memcpy(h->typed.sources[1].claim.identity.storage_uuid, uuid, sizeof(uuid));
	h->fence = (struct ClusterRecoveryFencePlan *)&dependency_fence;
	h->replay.typed = &h->typed;
	h->replay.fence_plan = &h->fence;
	h->replay.current[1] = &h->current->reader;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(h->cuts, 2, 1024 * 1024, &h->typed.plan),
				 CLUSTER_COLD_OK);
	for (uint32 i = 0; i < h->count; i++) {
		ClusterColdDecodedV1 decoded = { 0 };

		consumer_record(h, i);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&h->current->reader, 99, uuid, false, foreign,
													 &decoded),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_plan_feed_v1(h->typed.plan, 1, &decoded.record), CLUSTER_COLD_OK);
		cluster_cold_decoded_release_v1(&decoded);
	}
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(h->typed.plan, consumer_observe, h, &diag),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(h->typed.plan), page ? 1 : 0);
	cluster_cold_effects_proof_init_v1(&h->replay.effects);
	consumer_active = h;
	return h;
}

static void
consumer_destroy(ConsumerFixture *h)
{
	cluster_cold_decoded_release_v1(&h->replay.decoded);
	cluster_cold_plan_destroy_v1(&h->typed.plan);
	free(h->current);
	free(h);
	consumer_active = NULL;
}

static bool
consumer_next(void *arg, uint32 participant, ClusterColdReplayRecordV1 *out)
{
	ConsumerFixture *h = arg;
	EffectRecord *r = h->current;

	if (participant != 1 || h->next == h->count)
		return false;
	consumer_record(h, h->next++);
	*out = (ClusterColdReplayRecordV1){ .read_rec_ptr = r->reader.ReadRecPtr,
										.end_rec_ptr = r->reader.EndRecPtr,
										.record_crc = r->decoded.header.xl_crc,
										.rmid = r->decoded.header.xl_rmid,
										.info = r->decoded.header.xl_info };
	if (h->fault == CONSUMER_PAYLOAD_CHANGED)
		r->decoded.toplevel_xid = 101;
	return true;
}

static bool
consumer_scheduled(void *arg, uint32 participant, const ClusterColdStepV1 *step,
				   ClusterColdPageActionV1 action)
{
	ConsumerFixture *h = arg;

	if (h->fault == CONSUMER_ACTION_CHANGED)
		action
			= action == CLUSTER_COLD_PAGE_APPLY ? CLUSTER_COLD_PAGE_SKIP : CLUSTER_COLD_PAGE_APPLY;
	return cluster_typed_replay_scheduled(arg, participant, step, action);
}

static int
consumer_run(ConsumerFixture *h)
{
	const ClusterColdReplayOpsV1 ops = { .next = consumer_next,
										 .unscheduled = cluster_typed_replay_unscheduled,
										 .scheduled = consumer_scheduled };

	/* Arm every invocation, including positive cases: a new product refusal
	 * must produce a failed assertion, never jump through an unarmed buffer.
	 * -1 is a fixture interruption, not a manufactured sequencer result. */
	if (sigsetjmp(h->escape, 0) != 0)
		return -1;
	return cluster_cold_replay_run_v1(h->typed.plan, h->cuts, 2, h->typed.own_participant, &ops, h,
									  &h->result);
}

static bool
consumer_finished(ConsumerFixture *h)
{
	return cluster_cold_effects_proof_finish_v1(&h->replay.effects, h->typed.plan, &h->result, 2);
}

UT_TEST(test_startup_consumers_complete_native_and_foreign_records)
{
	for (int foreign = 0; foreign <= 1; foreign++) {
		ConsumerFixture *h = consumer_create(foreign, false, false);

		UT_ASSERT_EQ(consumer_run(h), CLUSTER_COLD_REPLAY_OK);
		UT_ASSERT(strcmp(h->trace, foreign ? "CGS" : "NN") == 0);
		UT_ASSERT_EQ(h->replay.effects.records_seen[1], 2);
		UT_ASSERT_EQ(h->replay.effects.records_routed, 2);
		UT_ASSERT(consumer_finished(h));
		consumer_destroy(h);
	}
}

UT_TEST(test_startup_side_refusal_cannot_finish_counted_effects)
{
	ConsumerFixture *h = consumer_create(true, false, false);

	h->fault = CONSUMER_SIDE_REFUSE;
	UT_ASSERT_EQ(consumer_run(h), CLUSTER_COLD_REPLAY_CALLBACK);
	UT_ASSERT(strcmp(h->trace, "CGS") == 0);
	UT_ASSERT_EQ(h->result.last_end[1], h->cuts[1].tail_end);
	UT_ASSERT_EQ(h->replay.effects.records_seen[1], 2);
	UT_ASSERT_EQ(h->replay.effects.records_routed, 2);
	UT_ASSERT_EQ(h->replay.effects.required_union, h->replay.effects.routed_union);
	UT_ASSERT(!consumer_finished(h));
	consumer_destroy(h);
}

UT_TEST(test_startup_page_apply_and_skip_use_original_consumers)
{
	for (int foreign = 0; foreign <= 1; foreign++)
		for (int skip = 0; skip <= 1; skip++) {
			ConsumerFixture *h = consumer_create(foreign, true, skip);

			UT_ASSERT_EQ(consumer_run(h), CLUSTER_COLD_REPLAY_OK);
			UT_ASSERT(strcmp(h->trace, skip ? (foreign ? "" : "X") : (foreign ? "GHN" : "HN"))
					  == 0);
			UT_ASSERT_EQ(h->result.steps_done, 1);
			UT_ASSERT_EQ(h->result.pages_applied, skip ? 0 : 1);
			UT_ASSERT_EQ(h->result.pages_skipped, skip ? 1 : 0);
			UT_ASSERT(consumer_finished(h));
			consumer_destroy(h);
		}
}

UT_TEST(test_startup_consumer_interruption_stops_before_completion)
{
	const ConsumerFault faults[]
		= { CONSUMER_GATE_INTERRUPT, CONSUMER_PAGE_INTERRUPT, CONSUMER_NATIVE_INTERRUPT };
	const char *traces[] = { "G", "GH", "GHN" };

	for (unsigned i = 0; i < lengthof(faults); i++) {
		ConsumerFixture *h = consumer_create(true, true, false);

		h->fault = faults[i];
		/* Model non-returning owner failure, not a caught production FATAL
		 * that is allowed to continue recovery or publish completion. */
		UT_ASSERT_EQ(consumer_run(h), -1);
		UT_ASSERT_EQ(h->interruptions, 1);
		UT_ASSERT_EQ(h->mismatches, 0);
		UT_ASSERT(strcmp(h->trace, traces[i]) == 0);
		UT_ASSERT_EQ(h->replay.effects.records_routed, 1);
		UT_ASSERT_EQ(h->result.steps_done, 0);
		UT_ASSERT(!consumer_finished(h));
		consumer_destroy(h);
	}
}

UT_TEST(test_startup_scheduled_rechecks_payload_and_action)
{
	const ConsumerFault faults[] = { CONSUMER_PAYLOAD_CHANGED, CONSUMER_ACTION_CHANGED };

	for (unsigned i = 0; i < lengthof(faults); i++)
		for (int skip = 0; skip <= 1; skip++) {
			ConsumerFixture *h = consumer_create(true, true, skip);

			h->fault = faults[i];
			UT_ASSERT_EQ(consumer_run(h), -1);
			UT_ASSERT_EQ(h->mismatches, 1);
			UT_ASSERT_EQ(h->interruptions, 0);
			UT_ASSERT_EQ(h->events, 0);
			UT_ASSERT_EQ(h->result.steps_done, 0);
			UT_ASSERT(!consumer_finished(h));
			consumer_destroy(h);
		}
}

UT_TEST(test_startup_foreign_control_refusal_stops_source_drain)
{
	ConsumerFixture *h = consumer_create(true, false, false);

	h->fault = CONSUMER_CONTROL_REFUSE;
	UT_ASSERT_EQ(consumer_run(h), CLUSTER_COLD_REPLAY_CALLBACK);
	UT_ASSERT(strcmp(h->trace, "C") == 0);
	UT_ASSERT_EQ(h->next, 1);
	UT_ASSERT_EQ(h->replay.effects.records_routed, 1);
	UT_ASSERT(!consumer_finished(h));
	consumer_destroy(h);
}
