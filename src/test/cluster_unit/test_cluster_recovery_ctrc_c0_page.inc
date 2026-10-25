/* Real result slot, terminal-view checker, page reader and frame producer.
 * Original Start/proof/physical observations remain controlled fixture leaves;
 * this does not execute the C0 writer or establish a complete service graph. */
static PGAlignedBlock c0_page_census_storage;
static char c0_page_plan_object;
#define c0_page_plan ((ClusterColdPlanV1 *)&c0_page_plan_object)
static ClusterRecoveryUndoEntry c0_page_physical[CLUSTER_ITL_INITRANS_DEFAULT + 1];
static ClusterColdTtSlotInputV1 c0_page_scratch[2 * (CLUSTER_ITL_INITRANS_DEFAULT + 1)];
static bool c0_page_proof, c0_page_effects;
static unsigned c0_page_fault, c0_page_fault_at;
#define c0_page_census ((ClusterColdTopTxCensusV1 *)c0_page_census_storage.data)

/* The opaque guard/pin fixture boundary owns full aligned storage. The real
 * owner extent functions are covered in their original dedicated fixtures. */
bool
rf_page_authority_guard_output_distinct_v1(const RfPageAuthorityGuardV1 *guard,
	const void *out, Size bytes)
{
	uintptr_t a = (uintptr_t)out, b = (uintptr_t)guard;
	return guard == (const RfPageAuthorityGuardV1 *)&fpi_guard_object && out != NULL && bytes != 0
		&& (a <= b ? b - a >= bytes : a - b >= sizeof(fpi_guard_backing));
}

bool
cluster_wal_retention_pin_output_distinct_v1(const ClusterWalRetentionPin *pin,
	const void *out, Size bytes)
{
	uintptr_t a = (uintptr_t)out, b = (uintptr_t)pin;
	return pin == (const ClusterWalRetentionPin *)&fpi_pin_object && out != NULL && bytes != 0
		&& (a <= b ? b - a >= bytes : a - b >= sizeof(fpi_pin_backing));
}

bool
ClusterColdRecoveryStartOutputDistinct(const struct ClusterRecoveryStartV1 *start,
	const void *out, Size bytes)
{
	uintptr_t a = (uintptr_t)out, b = (uintptr_t)start;
	return start == input_start && bytes != 0
		&& (a <= b ? b - a >= bytes : a - b >= sizeof(input_start_object));
}

bool
ClusterColdRecoveryTxTopProofCurrent(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdTopTxCensusEntryV1 *subject)
{
	return input_current && start == input_start && c0_page_proof
		&& subject >= c0_page_census->entries
		&& subject < c0_page_census->entries + c0_page_census->count;
}

bool
ClusterColdRecoveryTxEffectsCurrent(const struct ClusterRecoveryStartV1 *start)
{
	return input_current && start == input_start && c0_page_effects;
}

bool
ClusterColdRecoveryTxC0Candidate(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdTopTxCensusEntryV1 *subject, ClusterRecoveryTTAbortDerivationV1 *out)
{
	ClusterRecoveryTTAbortV1 shape = {0};
	const ClusterColdTtSlotKeyV1 *key = &subject->input.key;

	if (!ClusterColdRecoveryTxTopProofCurrent(start, subject)
		|| !ClusterColdRecoveryTxEffectsCurrent(start))
		return false;
	shape.version = CLUSTER_RECOVERY_TT_ABORT_VERSION;
	shape.length = CLUSTER_RECOVERY_TT_ABORT_BYTES;
	shape.flags = CLUSTER_RECOVERY_TT_ABORT_TOP_KNOWN;
	shape.target_thread = key->owner_instance; shape.writer_thread = 1;
	shape.target_incarnation = 12; shape.writer_incarnation = 20;
	shape.recovery_serial = 30; shape.target_epoch = 1;
	memset(shape.target_claim_sha256, 1, sizeof(shape.target_claim_sha256));
	shape.exact.segment_id = key->segment_id;
	shape.exact.segment_generation = key->segment_generation;
	shape.exact.xid = key->xid; shape.exact.slot_offset = key->slot_offset;
	shape.exact.wrap = key->slot_wrap; shape.exact.instance = key->owner_instance;
	shape.exact.format_version = CLUSTER_UNDO_TT_ABORT_EXACT_VERSION;
	shape.before = subject->input.slot;
	return cluster_recovery_tt_abort_derive_fields_v1(&shape, 10000, true, false, 0, 1, out);
}

static void
c0_page_setup(unsigned count, bool mixed)
{
	reset_itl_input(true, count);
	input_token_fault = 0;
	terminal_frame_fault = NULL;
	terminal_owner_fault = NULL;
	c0_page_proof = c0_page_effects = true;
	c0_page_fault = c0_page_fault_at = 0;
	memset(&c0_page_census_storage, 0, sizeof(c0_page_census_storage));
	memset(c0_page_physical, 0, sizeof(c0_page_physical));
	input_typed.plan = c0_page_plan;
	input_work->plan = c0_page_plan;
	input_typed.top_tx_census = c0_page_census;
	input_work->top_tx_census = c0_page_census;
	c0_page_census->plan = c0_page_plan;
	input_typed.census_root_captured = true;
	input_typed.census_root.system_identifier = input_owner.writer.ref.claim.identity.system_identifier;
	memset(input_typed.census_root.image_sha256, 4, 32);
	for (unsigned i = 0; i < count; i++) {
		ClusterColdTtSlotInputV1 *subject = &input_work->entries[i];
		ClusterRecoveryUndoEntry *physical = &c0_page_physical[i];
		if (!mixed || i % 2 == 0) {
			subject->slot.status = TT_SLOT_ACTIVE;
			subject->slot.commit_scn = InvalidScn;
			c0_page_census->entries[c0_page_census->count].input = *subject;
			c0_page_census->entries[c0_page_census->count++].loser_proven = true;
		}
		physical->owner_instance = subject->key.owner_instance;
		physical->segment_id = subject->key.segment_id;
		physical->segment_generation = subject->key.segment_generation;
		physical->slot_offset = subject->key.slot_offset;
		physical->slot = subject->slot;
		if (physical->slot.status == TT_SLOT_ACTIVE)
			physical->slot.status = TT_SLOT_ABORTED; /* independent literal C0 successor */
	}
	c0_page_census->proven_count = c0_page_census->count;
	checkpoint_registered_start = NULL;
	memset(&checkpoint_registered_owner, 0, sizeof(checkpoint_registered_owner));
	UT_ASSERT(cluster_recovery_checkpoint_register_v1(input_start));
	/* The real TX result state machine consumes controlled proven ordinals.
	 * C0 execution and actual LMON registration are separate owner-fixture
	 * tests; this fixture supplies their acceptance boundary explicitly. */
	UT_ASSERT(cluster_recovery_tx_result_begin_v1(input_start));
	for (uint32 i = 0; i < c0_page_census->count; i++)
		UT_ASSERT(cluster_recovery_tx_result_step_v1(input_start, i));
	UT_ASSERT(cluster_recovery_tx_result_finish_v1(input_start));
	region.checkpoint_owner.tx_accepted_generation = region.checkpoint_owner.tx_result.result_generation;
}

static bool
c0_page_read(ClusterColdTtSlotInputV1 *scratch, Size capacity, RfPageRecoveryCtrcItlFpiV1 *out)
{
	bool ok;
	reading_input = preparing_input_frame = true;
	ok = rf_page_recovery_ctrc_itl_c0_page_fpi_smgr_v1(input_start, input_work,
		(RfPageAuthorityGuardV1 *)&fpi_guard_object, &fpi_serial,
		(ClusterWalRetentionPin *)&fpi_pin_object, &input_identity, 7, &input_version,
		c0_page_physical, input_work->counts.subject_count, scratch, capacity, out);
	reading_input = preparing_input_frame = false;
	return ok;
}

static void
c0_page_drift(bool token)
{
	if ((c0_page_fault_at == 6) != token
		|| (!token && c0_page_fault_at != input_reads))
		return;
	switch (c0_page_fault) {
	case 0: region.checkpoint_owner.tx_result.result_generation++;
		region.checkpoint_owner.tx_accepted_generation++; break;
	case 1: region.checkpoint_owner.generation++; break;
	case 2: c0_page_physical[input_work->counts.subject_count - 1].slot.flags ^= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN; break;
	case 3: c0_page_proof = false; break;
	case 4: c0_page_effects = false; break;
	case 5: c0_page_scratch[input_work->counts.subject_count].slot.flags ^= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN; break;
	case 6: input_work->entries[input_work->counts.subject_count - 1].slot.first_undo_block.raw[0]++; break;
	}
}

static void
c0_page_refused(void)
{
	RfPageRecoveryCtrcItlFpiV1 out, saved;
	memset(&out, 0xa5, sizeof(out)); saved = out;
	UT_ASSERT(!c0_page_read(c0_page_scratch, lengthof(c0_page_scratch), &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	itl_input_debt_unchanged();
	terminal_frame_fault = NULL;
}

UT_TEST(ctrc_c0_page_projects_original_active_work_without_mutating_it)
{
	for (unsigned count = 1; count <= CLUSTER_ITL_INITRANS_DEFAULT; count++) {
		RfPageRecoveryCtrcItlFpiV1 out;
		PGAlignedBlock original_work, original_page, disk;
		ClusterRecoveryTxResultValueV1 original_result;
		c0_page_setup(count, false);
		original_work = input_work_storage;
		original_result = region.checkpoint_owner.tx_result;
		UT_ASSERT_EQ(pread(install_fd, original_page.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(c0_page_read(c0_page_scratch, lengthof(c0_page_scratch), &out));
		UT_ASSERT(out.rewritten); UT_ASSERT_EQ(input_reads, 5);
		for (unsigned i = 0; i < count; i++) {
			ClusterItlSlotData *slot = &ClusterPageGetItlSlots((Page)out.frame.image.data)[i];
			UT_ASSERT_EQ(slot->flags, i % 2 == 0 ? ITL_FLAG_LOCK_ONLY_ABORTED : ITL_FLAG_ABORTED);
			UT_ASSERT_EQ(slot->commit_scn, InvalidScn);
			UT_ASSERT_EQ(c0_page_scratch[i].slot.status, TT_SLOT_ACTIVE);
			UT_ASSERT_EQ(c0_page_scratch[count + i].slot.status, TT_SLOT_ABORTED);
		}
		UT_ASSERT(memcmp(&original_work, &input_work_storage, sizeof(original_work)) == 0);
		UT_ASSERT(memcmp(&original_result, &region.checkpoint_owner.tx_result, sizeof(original_result)) == 0);
		UT_ASSERT_EQ(pread(install_fd, disk.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(&original_page, &disk, sizeof(disk)) == 0);
		itl_input_debt_unchanged();
	}
}

UT_TEST(ctrc_c0_page_handles_mixed_and_noop_inputs)
{
	for (unsigned count = 0; count <= 8; count += 8) {
		RfPageRecoveryCtrcItlFpiV1 out, empty = {0};
		c0_page_setup(count, true);
		UT_ASSERT(c0_page_read(count ? c0_page_scratch : NULL, 2 * count, &out));
		if (!count) UT_ASSERT(memcmp(&out, &empty, sizeof(out)) == 0);
		else UT_ASSERT(out.rewritten);
		itl_input_debt_unchanged();
	}
}

UT_TEST(ctrc_c0_page_requires_original_complete_accepted_value_before_io)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		c0_page_setup(2, false);
		switch (fault) {
		case 0: region.checkpoint_owner.tx_result.state = CLUSTER_RECOVERY_TX_RESULT_PENDING; break;
		case 1: region.checkpoint_owner.tx_accepted_generation--; break;
		case 2: region.checkpoint_owner.tx_result.input_root_sha256[7]++; break;
		case 3: region.checkpoint_owner.tx_result.subject_count++; break;
		case 4: region.checkpoint_owner.tx_result.processed_count--; break;
		case 5: region.checkpoint_owner.tx_result.unresolved_count++; break;
		case 6: c0_page_physical[1].slot.status = TT_SLOT_ACTIVE; break;
		case 7: c0_page_physical[1].segment_generation++; break;
		case 8: c0_page_proof = false; break;
		case 9: c0_page_effects = false; break;
		}
		c0_page_refused(); UT_ASSERT_EQ(input_reads, 0); UT_ASSERT_EQ(opens, 0);
	}
}

UT_TEST(ctrc_c0_page_rechecks_receipt_physical_proofs_and_full_work)
{
	for (unsigned at = 1; at <= 6; at++)
		for (unsigned fault = 0; fault < 7; fault++) {
			c0_page_setup(8, false);
			c0_page_fault_at = at; c0_page_fault = fault;
			terminal_frame_fault = c0_page_drift;
			c0_page_refused();
		}
}

UT_TEST(ctrc_c0_page_refuses_scratch_and_borrowed_aliases_before_io)
{
	for (unsigned fault = 0; fault < 16; fault++) {
		ClusterColdTtSlotInputV1 *scratch = c0_page_scratch;
		RfPageRecoveryCtrcItlFpiV1 out, saved, *output = &out;
		PGAlignedBlock saved_guard, saved_pin;
		Size capacity = lengthof(c0_page_scratch);
		c0_page_setup(2, false);
		memset(&out, 0xa5, sizeof(out)); saved = out;
		saved_guard = fpi_guard_backing; saved_pin = fpi_pin_backing;
		switch (fault) {
		case 0: capacity = 3; break;
		case 1: capacity = (Size)-1; break;
		case 2: scratch = (void *)c0_page_physical; break;
		case 3: output = (void *)c0_page_physical; break;
		case 4: scratch = (void *)c0_page_census; break;
		case 5: output = (void *)c0_page_census; break;
		case 6: scratch = input_work->entries; break;
		case 7: output = (void *)c0_page_scratch; break;
		case 8: scratch = (void *)&region.checkpoint_owner.tx_result; break;
		case 9: output = (void *)&region.checkpoint_owner.tx_result; break;
		case 10: scratch = (void *)input_start; break;
		case 11: output = (void *)input_start; break;
		case 12: scratch = (void *)((char *)&fpi_guard_object + sizeof(void *)); break;
		case 13: output = (void *)((char *)&fpi_guard_object + sizeof(void *)); break;
		case 14: scratch = (void *)((char *)&fpi_pin_object + sizeof(void *)); break;
		case 15: output = (void *)((char *)&fpi_pin_object + sizeof(void *)); break;
		}
		UT_ASSERT(!c0_page_read(scratch, capacity, output));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		UT_ASSERT(memcmp(&fpi_guard_backing, &saved_guard, sizeof(saved_guard)) == 0);
		UT_ASSERT(memcmp(&fpi_pin_backing, &saved_pin, sizeof(saved_pin)) == 0);
		UT_ASSERT_EQ(input_reads, 0); UT_ASSERT_EQ(opens, 0);
		itl_input_debt_unchanged();
	}
}

UT_TEST(ctrc_c0_page_errors_rethrow_without_publishing_or_retiring)
{
	for (unsigned at = 1; at <= 6; at++) {
		RfPageRecoveryCtrcItlFpiV1 out, saved;
		volatile bool caught = false;
		c0_page_setup(2, false);
		input_throw_at = at <= 5 ? at : 0;
		input_token_fault = at == 6 ? 13 : 0;
		memset(&out, 0xa5, sizeof(out)); saved = out;
		PG_TRY(); { (void)c0_page_read(c0_page_scratch, lengthof(c0_page_scratch), &out); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		reading_input = preparing_input_frame = false;
		UT_ASSERT(caught && raised_level == ERROR);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		itl_input_debt_unchanged();
	}
	input_token_fault = 0;
}

static void
c0_page_shrink_on_owner_read(void)
{
	input_work->counts.subject_count--;
	terminal_owner_fault = NULL;
}

UT_TEST(ctrc_c0_page_freezes_counts_before_original_owner_observation)
{
	RfPageRecoveryCtrcItlFpiV1 out, saved;
	c0_page_setup(2, false);
	/* Prime both terminal values so an unchecked later shrink cannot borrow
	 * an old scratch tail as part of the new complete physical observation. */
	UT_ASSERT(c0_page_read(c0_page_scratch, lengthof(c0_page_scratch), &out));
	input_reads = opens = 0;
	memset(&out, 0xa5, sizeof(out)); saved = out;
	terminal_owner_fault = c0_page_shrink_on_owner_read;
	UT_ASSERT(!c0_page_read(c0_page_scratch, lengthof(c0_page_scratch), &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT_EQ(input_reads, 0); UT_ASSERT_EQ(opens, 0);
	terminal_owner_fault = NULL;
	itl_input_debt_unchanged();
}
