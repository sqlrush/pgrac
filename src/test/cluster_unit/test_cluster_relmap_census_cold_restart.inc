/* Actual fresh ROOT, physical pairs and census; original checkpoint/native
 * scope leaves are controlled. No complete native lifecycle qualification.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_relmap_terminal_scan.h"
static unsigned census_restart_fault;

bool
cluster_relmap_recovery_retention_build_start_cold_v1(uint16 origin,
													  ClusterRelmapRetentionDirectory *out)
{
	ClusterRelmapRetentionDirectory zero = { 0 };
	CensusFixture *f = census_native_fixture;

	UT_ASSERT_EQ(memcmp(out, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(origin, 3);
	if (census_restart_fault == 1)
		return false;
	if (census_restart_fault == 2)
		siglongjmp(*PG_exception_stack, 1);
	if (cluster_relmap_retention_build_start(out, 817, 91, origin, 73, UINT64CONST(0x200000003),
											 UINT64CONST(0x200000400))
		!= CLUSTER_RELMAP_RETENTION_OK)
		return false;
	if (census_restart_fault == 3)
		out->native_floor++;
	if (census_restart_fault == 4)
		out->last_issued++;
	if (census_restart_fault == 5)
		out->origin++;
	if (census_restart_fault == 6)
		out->system_identifier++;
	if (census_restart_fault == 7)
		out->database_incarnation++;
	if (census_restart_fault == 8)
		out->generation++;
	if (census_restart_fault == 9)
		out->state = CLUSTER_RELMAP_RETENTION_COMPLETE;
	if (census_restart_fault == 10)
		f->origin[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
	if (census_restart_fault == 11)
		census_cold_selected.scope_serial++;
	if (census_restart_fault == 12)
		census_namespace_x = false;
	if (census_restart_fault == 13)
		siglongjmp(*PG_exception_stack, 1);
	if (census_restart_fault == 14)
		/* After producer postcheck and physical pre/post checks, ERROR
		 * during the final owner check of the allocated NEW census. */
		census_cold_check_error_after = census_cold_checks + 4;
	return true;
}

static bool
census_restart_ready(CensusFixture *f, Oid dbid)
{
	census_restart_fault = 0;
	census_cold_reset();
	census_fixture(f, true);
	census_pair(f->fd, InvalidOid, 3, false, true, false);
	census_native_reset(f);
	cluster_relmap_retention_invalidate(&f->origin[1]); /* Not a supplied origin. */
	f->directories[0] = &f->origin[0];
	f->roots = begin_roots(f->fd, 4 * 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(f->roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_relmap_retention_census_begin(f->roots, 817, 91, root_uuid, f->directories,
													   1, CENSUS_BUDGET, &f->census),
				 CLUSTER_RELMAP_CENSUS_MORE);
	if (f->census == NULL)
		return false;
	UT_ASSERT_EQ(census_drain(f), CLUSTER_RELMAP_CENSUS_DRAINED);
	census_batch_consuming = true;
	census_native_after_arbitration = census_cold_clear_pair_bytes;
	UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(f->census, dbid),
				 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	return true;
}

static void
census_restart_late_change(void *buffer, size_t length, off_t offset)
{
	(void)buffer;
	(void)length;
	(void)offset;
	namespace_read_transform = NULL;
	if (census_restart_fault == 20)
		census_native_fixture->origin[0].pins[0].wal_end++;
	if (census_restart_fault == 21)
		census_cold_selected.checkpoint.root_publish_seq++;
	if (census_restart_fault == 22)
		census_cold_selected.native_redo++;
	if (census_restart_fault == 23)
		census_merger_held = false;
	if (census_restart_fault == 24)
		siglongjmp(*PG_exception_stack, 1);
	if (census_restart_fault == 25)
		census_cold_fault = 8;
}

UT_TEST(test_census_restart_new_physical_census_restores_remaining_pin)
{
	for (unsigned shared = 0; shared < 2; shared++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *fresh = NULL, *again = (void *)1;
		ClusterRelmapRetentionDirectory saved[2];
		ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
		ClusterRelmapCensusRecovery input = { 0 };
		unsigned images = 0;
		Oid cleared = shared ? InvalidOid : 5, remaining = shared ? 5 : InvalidOid;
		if (census_restart_ready(&f, cleared)) {
			memcpy(saved, f.origin, sizeof(saved));
			UT_ASSERT(cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024,
																   CENSUS_BUDGET, &fresh));
			UT_ASSERT(fresh != NULL && fresh != f.census);
			for (unsigned i = 0; fresh != NULL && i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE;
				 i++) {
				ClusterRelmapCensusCommitted image;
				result = cluster_relmap_retention_census_next_committed(fresh, &image);
				if (image.present) {
					images++;
					UT_ASSERT(image.identity.dbid == InvalidOid || image.identity.dbid == 5);
					UT_ASSERT_EQ(image.map.mappings[0].mapfilenumber, 40000);
				}
			}
			UT_ASSERT_EQ(result, CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT_EQ(images, 2);
			/* Actual prepare selects the newly observed remaining pair and
			 * requires its pin in the NEW directory, not the old pin store. */
			census_native_prepares = 0;
			UT_ASSERT(
				cluster_relmap_retention_census_recovery_prepare_cold(fresh, remaining, &input));
			UT_ASSERT(input.handle != NULL && input.identity.dbid == remaining);
			UT_ASSERT_EQ(input.pending.pending_owner.node, 3);
			if (input.handle != NULL)
				cluster_relmap_recovery_handle_release_v1(&input.handle);
			UT_ASSERT(!cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024,
																	CENSUS_BUDGET, &again));
			UT_ASSERT(again == NULL);
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			UT_ASSERT(census_pin(&f.origin[0], cleared) != NULL);
			census_refused(&f);
		}
		cluster_relmap_retention_census_destroy(fresh);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_restart_initial_gates_and_multi_origin_preserve_follow_on)
{
	for (unsigned gate = 0; gate < 4; gate++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *fresh = (void *)1;
		ClusterRecoveryRootSet *observed = NULL;
		bool ready = gate == 3 ? census_owned_ready(&f, 5, NULL) : census_restart_ready(&f, 5);
		if (ready) {
			cluster_shared_catalog = gate != 0;
			census_merger_held = gate != 1;
			census_namespace_x = gate != 2;
			UT_ASSERT(!cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024,
																	CENSUS_BUDGET, &fresh));
			UT_ASSERT(fresh == NULL);
			cluster_shared_catalog = census_merger_held = census_namespace_x = true;
			UT_ASSERT(
				cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &observed));
		}
		cluster_recovery_root_set_destroy(observed);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_restart_checkpoint_range_serial_and_builder_error_refuse_once)
{
	for (unsigned fault = 1; fault <= 14; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *volatile fresh = (void *)1;
		ClusterRelmapRetentionDirectory saved[2];
		ClusterRecoveryRootSet *again = (void *)1;
		volatile bool caught = false;
		if (census_restart_ready(&f, 5)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_restart_fault = fault;
			PG_TRY();
			{
				UT_ASSERT(!cluster_relmap_retention_census_restart_cold(
					f.census, 1024 * 1024, CENSUS_BUDGET, (ClusterRelmapRetentionCensus **)&fresh));
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 2 || fault == 13 || fault == 14);
			UT_ASSERT(fresh == NULL);
			if (fault == 10)
				saved[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			census_namespace_x = true;
			UT_ASSERT(
				!cluster_relmap_retention_census_reobserve_cold(f.census, 1024 * 1024, &again));
			UT_ASSERT(again == NULL);
			census_refused(&f);
		}
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_restart_post_physical_io_checks_and_error_keep_old_debt)
{
	for (unsigned fault = 20; fault <= 25; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *volatile fresh = (void *)1;
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool caught = false;
		if (census_restart_ready(&f, 5)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_restart_fault = fault;
			namespace_read_transform = census_restart_late_change;
			PG_TRY();
			{
				UT_ASSERT(!cluster_relmap_retention_census_restart_cold(
					f.census, 1024 * 1024, CENSUS_BUDGET, (ClusterRelmapRetentionCensus **)&fresh));
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault >= 24);
			UT_ASSERT(fresh == NULL && namespace_read_transform == NULL);
			if (fault == 20)
				saved[0].pins[0].wal_end++;
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			census_refused(&f);
		}
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_restart_budget_read_and_missing_path_refuse_without_retry)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *fresh = (void *)1;
		ClusterRelmapRetentionDirectory saved[2];
		if (census_restart_ready(&f, 5)) {
			memcpy(saved, f.origin, sizeof(saved));
			if (fault == 2 || fault == 3)
				inject_pread_fault = fault - 1;
			if (fault == 4)
				UT_ASSERT_EQ(unlinkat(f.fd, "unrelated", 0), 0);
			UT_ASSERT(!cluster_relmap_retention_census_restart_cold(
				f.census, fault == 0 ? 1 : 1024 * 1024, fault == 1 ? 1 : CENSUS_BUDGET, &fresh));
			UT_ASSERT(fresh == NULL);
			inject_pread_fault = 0;
			UT_ASSERT(!cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024,
																	CENSUS_BUDGET, &fresh));
			UT_ASSERT(fresh == NULL);
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			census_refused(&f);
		}
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_restart_alias_and_null_output_preserve_original_storage)
{
	CensusFixture f;
	ClusterRelmapRetentionCensus *fresh = (void *)1;
	UT_ASSERT(!cluster_relmap_retention_census_restart_cold(NULL, 0, 0, &fresh));
	UT_ASSERT(fresh == NULL);
	if (census_restart_ready(&f, 5)) {
		ClusterRelmapRetentionDirectory saved[2];
		memcpy(saved, f.origin, sizeof(saved));
		UT_ASSERT(!cluster_relmap_retention_census_restart_cold(f.census, 0, 0, NULL));
		UT_ASSERT(!cluster_relmap_retention_census_restart_cold(
			f.census, 0, 0, (ClusterRelmapRetentionCensus **)&f.origin[0]));
		UT_ASSERT(!cluster_relmap_retention_census_restart_cold(
			f.census, 0, 0, (ClusterRelmapRetentionCensus **)f.census));
		UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
		fresh = NULL;
		UT_ASSERT(cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024, CENSUS_BUDGET,
															   &fresh));
		cluster_relmap_retention_census_destroy(fresh);
	}
	census_cold_consume_cleanup(&f);
}

UT_TEST(test_census_restart_interrupted_new_cursor_destroys_only_local_storage)
{
	for (unsigned changed = 0; changed < 2; changed++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *fresh = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		if (census_restart_ready(&f, 5)) {
			memcpy(saved, f.origin, sizeof(saved));
			UT_ASSERT(cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024,
																   CENSUS_BUDGET, &fresh));
			if (fresh != NULL && changed) {
				ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;
				root_file(f.fd, "unrelated", "warm", 4);
				for (unsigned i = 0; i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++)
					result = cluster_relmap_retention_census_next(fresh);
				UT_ASSERT(result > CLUSTER_RELMAP_CENSUS_DRAINED);
			}
			cluster_relmap_retention_census_destroy(fresh);
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
			census_refused(&f);
		}
		census_cold_consume_cleanup(&f);
	}
}

static ClusterRelmapCensusResult
census_restart_drain(ClusterRelmapRetentionCensus *census)
{
	ClusterRelmapCensusResult result = CLUSTER_RELMAP_CENSUS_MORE;

	for (unsigned i = 0; i < 20000 && result == CLUSTER_RELMAP_CENSUS_MORE; i++)
		result = cluster_relmap_retention_census_next(census);
	return result;
}

UT_TEST(test_census_restart_original_source_is_required_after_publication)
{
	for (unsigned prepared = 0; prepared < 2; prepared++)
		for (unsigned fault = 0; fault < (prepared ? 5 : 8); fault++) {
			CensusFixture f;
			ClusterRelmapRetentionCensus *fresh = NULL;
			ClusterRelmapRetentionDirectory saved[2];
			ClusterRelmapRecoverySourceObservationV1 source;
			ClusterRelmapCensusRecovery input = { 0 };
			if (census_restart_ready(&f, 5)) {
				memcpy(saved, f.origin, sizeof(saved));
				source = census_cold_selected;
				UT_ASSERT(cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024,
																	   CENSUS_BUDGET, &fresh));
				if (prepared)
					UT_ASSERT_EQ(census_restart_drain(fresh), CLUSTER_RELMAP_CENSUS_DRAINED);
				if (fault == 0)
					census_cold_selected.scope_serial++;
				if (fault == 1)
					census_cold_selected.checkpoint.root_publish_seq++;
				if (fault == 2)
					census_cold_selected.native_redo++;
				if (fault == 3)
					census_cold_selected.source.claim.identity.storage_uuid[0] ^= 1;
				if (fault == 4)
					census_native_scope_current = false;
				if (fault == 5)
					cluster_shared_catalog = false;
				if (fault == 6)
					census_merger_held = false;
				if (fault == 7)
					census_namespace_x = false;
				if (prepared) {
					unsigned before = census_native_prepares;
					UT_ASSERT(!cluster_relmap_retention_census_recovery_prepare_cold(
						fresh, InvalidOid, &input));
					UT_ASSERT(census_recovery_zero(&input));
					UT_ASSERT_EQ(census_native_prepares, before);
				} else
					UT_ASSERT_EQ(census_restart_drain(fresh), CLUSTER_RELMAP_CENSUS_CHANGED);
				census_cold_selected = source;
				census_native_scope_current = true;
				cluster_shared_catalog = census_merger_held = census_namespace_x = true;
				UT_ASSERT_EQ(cluster_relmap_retention_census_next(fresh),
							 prepared ? CLUSTER_RELMAP_CENSUS_UNAVAILABLE
									  : CLUSTER_RELMAP_CENSUS_CHANGED);
				UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
				if (input.handle != NULL)
					cluster_relmap_recovery_handle_release_v1(&input.handle);
			}
			cluster_relmap_retention_census_destroy(fresh);
			census_cold_consume_cleanup(&f);
		}
}

UT_TEST(test_census_restart_original_pin_and_metadata_checks_continue_until_native)
{
	for (unsigned prepared = 0; prepared < 2; prepared++)
		for (unsigned fault = 0; fault < 9; fault++) {
			CensusFixture f;
			ClusterRelmapRetentionCensus *fresh = NULL;
			ClusterRelmapRetentionDirectory saved[2];
			ClusterRelmapCensusRecovery input = { 0 };
			if (census_restart_ready(&f, 5)) {
				UT_ASSERT(cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024,
																	   CENSUS_BUDGET, &fresh));
				if (prepared)
					UT_ASSERT_EQ(census_restart_drain(fresh), CLUSTER_RELMAP_CENSUS_DRAINED);
				if (fault == 0)
					f.origin[0].pins[0].wal_end++;
				if (fault == 1)
					f.origin[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
				if (fault == 2)
					f.origin[0].generation++;
				if (fault == 3)
					f.origin[0].native_floor++;
				if (fault == 4)
					f.origin[0].last_issued++;
				if (fault == 5)
					f.origin[0].state = CLUSTER_RELMAP_RETENTION_BUILDING;
				if (fault == 6)
					f.origin[0].system_identifier++;
				if (fault == 7)
					f.origin[0].database_incarnation++;
				if (fault == 8)
					f.origin[0].origin++;
				memcpy(saved, f.origin, sizeof(saved));
				if (prepared) {
					unsigned before = census_native_prepares;
					UT_ASSERT(!cluster_relmap_retention_census_recovery_prepare_cold(
						fresh, InvalidOid, &input));
					UT_ASSERT(census_recovery_zero(&input));
					UT_ASSERT_EQ(census_native_prepares, before);
				} else
					UT_ASSERT_EQ(census_restart_drain(fresh), CLUSTER_RELMAP_CENSUS_CHANGED);
				UT_ASSERT_EQ(cluster_relmap_retention_census_next(fresh),
							 prepared ? CLUSTER_RELMAP_CENSUS_UNAVAILABLE
									  : CLUSTER_RELMAP_CENSUS_CHANGED);
				/* Even a changed original pin/metadata byte is never repaired,
				 * released or rewritten by the new observation's refusal. */
				UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
				if (input.handle != NULL)
					cluster_relmap_recovery_handle_release_v1(&input.handle);
			}
			cluster_relmap_retention_census_destroy(fresh);
			census_cold_consume_cleanup(&f);
		}
}

UT_TEST(test_census_restart_later_source_error_revokes_only_new_observation)
{
	for (unsigned prepared = 0; prepared < 2; prepared++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *fresh = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		ClusterRelmapCensusRecovery input = { 0 };
		volatile bool caught = false;
		if (census_restart_ready(&f, 5)) {
			memcpy(saved, f.origin, sizeof(saved));
			UT_ASSERT(cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024,
																   CENSUS_BUDGET, &fresh));
			if (prepared)
				UT_ASSERT_EQ(census_restart_drain(fresh), CLUSTER_RELMAP_CENSUS_DRAINED);
			census_cold_fault = 8;
			PG_TRY();
			{
				if (prepared)
					(void)cluster_relmap_retention_census_recovery_prepare_cold(fresh, InvalidOid,
																				&input);
				else
					(void)census_restart_drain(fresh);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT(caught && census_recovery_zero(&input));
			census_cold_fault = 0;
			UT_ASSERT_EQ(cluster_relmap_retention_census_next(fresh),
						 prepared ? CLUSTER_RELMAP_CENSUS_UNAVAILABLE
								  : CLUSTER_RELMAP_CENSUS_CHANGED);
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		}
		cluster_relmap_retention_census_destroy(fresh);
		census_cold_consume_cleanup(&f);
	}
}

UT_TEST(test_census_repeated_restart_keeps_earliest_original_protection)
{
	for (unsigned changed = 0; changed < 2; changed++) {
		CensusFixture f;
		ClusterRelmapRetentionCensus *first = NULL, *second = NULL;
		ClusterRelmapRetentionDirectory saved[2];
		if (census_restart_ready(&f, 5)) {
			UT_ASSERT(cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024,
																   CENSUS_BUDGET, &first));
			UT_ASSERT_EQ(census_restart_drain(first), CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(first, InvalidOid),
						 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
			UT_ASSERT(cluster_relmap_retention_census_restart_cold(first, 1024 * 1024,
																   CENSUS_BUDGET, &second));
			if (changed)
				f.origin[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
			memcpy(saved, f.origin, sizeof(saved));
			UT_ASSERT_EQ(census_restart_drain(second),
						 changed ? CLUSTER_RELMAP_CENSUS_CHANGED : CLUSTER_RELMAP_CENSUS_DRAINED);
			UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			UT_ASSERT(fcntl(f.fd, F_GETFD) >= 0);
		}
		cluster_relmap_retention_census_destroy(second);
		cluster_relmap_retention_census_destroy(first);
		census_cold_consume_cleanup(&f);
	}
}

static void
census_restart_ancestor_after_arbitration(void)
{
	census_cold_clear_pair_bytes();
	census_native_fixture->origin[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
}

UT_TEST(test_census_restart_native_return_checks_original_ancestor_pins)
{
	CensusFixture f;
	ClusterRelmapRetentionCensus *fresh = NULL, *again = (void *)1;
	ClusterRelmapRetentionDirectory saved[2];
	if (census_restart_ready(&f, 5)) {
		UT_ASSERT(cluster_relmap_retention_census_restart_cold(f.census, 1024 * 1024, CENSUS_BUDGET,
															   &fresh));
		UT_ASSERT_EQ(census_restart_drain(fresh), CLUSTER_RELMAP_CENSUS_DRAINED);
		memcpy(saved, f.origin, sizeof(saved));
		census_native_after_arbitration = census_restart_ancestor_after_arbitration;
		UT_ASSERT_EQ(cluster_relmap_retention_census_recovery_arbitrate_cold(fresh, InvalidOid),
					 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		saved[0].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
		UT_ASSERT_EQ(memcmp(saved, f.origin, sizeof(saved)), 0);
		UT_ASSERT_EQ(cluster_relmap_retention_census_next(fresh), CLUSTER_RELMAP_CENSUS_CHANGED);
		UT_ASSERT(!cluster_relmap_retention_census_restart_cold(fresh, 1024 * 1024, CENSUS_BUDGET,
																&again));
		UT_ASSERT(again == NULL);
	}
	cluster_relmap_retention_census_destroy(fresh);
	census_cold_consume_cleanup(&f);
}
