/* Actual physical census/ROOT/pair files; execution custody/current owner are
 * controlled providers. No native terminal or whole-cold completion claim.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_undo_recovery.h"

static ClusterRelmapRecoverySourceObservationV1 census_cold_selected;
static unsigned census_cold_fault, census_cold_reads, census_cold_checks;
static unsigned census_cold_getter_fault_after = 1;
static unsigned census_cold_check_error_after;

static void
census_cold_late_pin(void *buffer, size_t length, off_t offset)
{
	ClusterRelmapRetentionPin *pin
		= (ClusterRelmapRetentionPin *)census_pin(&census_native_fixture->origin[1], InvalidOid);
	(void)buffer;
	(void)length;
	(void)offset;
	UT_ASSERT(pin != NULL);
	if (pin != NULL)
		pin->wal_end++;
	namespace_read_transform = NULL;
}

bool
cluster_relmap_recovery_prepare_cold_v1(const ClusterRelmapV2Namespace *identity,
										const ClusterRelmapV2View *pending,
										ClusterRelmapRecoveryHandle **out)
{
	CensusFixture *f = census_native_fixture;
	if (!cluster_relmap_recovery_prepare_v1((void *)&census_inputs_cookie, identity, pending, out))
		return false;
	memset(&census_cold_selected, 0, sizeof(census_cold_selected));
	census_cold_selected.source = census_native_sources[0].source;
	census_cold_selected.source.claim.identity.origin_node_id = pending->pending_owner.node;
	census_cold_selected.checkpoint.identity = census_cold_selected.source.claim.identity;
	census_cold_selected.checkpoint.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	census_cold_selected.checkpoint.root_publish_seq = 8;
	census_cold_selected.native_redo = 512;
	census_cold_selected.scope_serial = 73;
	if (census_cold_fault == 9)
		((ClusterRelmapRetentionPin *)census_pin(&f->origin[1], InvalidOid))->wal_end++;
	if (census_cold_fault == 10)
		f->origin[1].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
	if (census_cold_fault == 11) {
		UT_ASSERT_EQ(mkdirat(f->fd, "base/8", 0700), 0);
		census_pair(f->fd, 8, 0, false, false, false);
	}
	if (census_cold_fault == 12) {
		uint8 bytes[CLUSTER_RELMAP_V2_FILE_SIZE];
		census_wire(bytes, 5, 3, true, false);
		root_file(f->fd, "replacement", bytes, sizeof(bytes));
		UT_ASSERT_EQ(renameat(f->fd, "replacement", f->fd, "base/5/pgrac_relmap_authority.bak"), 0);
	}
	if (census_cold_fault == 13)
		root_file(f->fd, "unrelated", "warm", 4);
	if (census_cold_fault == 14)
		cluster_shared_catalog = false;
	if (census_cold_fault == 15)
		census_merger_held = false;
	if (census_cold_fault == 16)
		census_namespace_x = false;
	if (census_cold_fault == 17)
		f->origin[1].last_issued++;
	if (census_cold_fault == 19)
		namespace_read_transform = census_cold_late_pin;
	return true;
}

bool
cluster_relmap_recovery_handle_source_v1(ClusterRelmapRecoveryHandle *handle,
										 ClusterRelmapRecoverySourceObservationV1 *out)
{
	unsigned fault;
	UT_ASSERT(handle == (void *)&census_native_handles[0] && census_native_handles[0].live);
	UT_ASSERT(out != NULL);
	memset(out, 0, sizeof(*out));
	census_cold_reads++;
	fault = census_cold_reads >= census_cold_getter_fault_after ? census_cold_fault : 0;
	if (fault == 1)
		return false;
	if (fault == 2)
		siglongjmp(*PG_exception_stack, 1);
	*out = census_cold_selected;
	if (fault == 3)
		out->source.claim.identity.system_identifier++;
	if (fault == 4)
		out->source.claim.database_incarnation++;
	if (fault == 5)
		out->source.claim.identity.storage_uuid[0] ^= 1;
	if (fault == 6)
		out->source.claim.identity.origin_node_id++;
	if (fault == 7)
		census_native_scope_current = false;
	if (fault == 18 && census_cold_reads == 2)
		out->scope_serial++;
	return true;
}

bool
cluster_undo_recovery_cold_single_source_v1(const ClusterWalSourceRef *source,
											const ClusterControlRootSnapshot *checkpoint,
											XLogRecPtr redo, uint64 serial)
{
	census_cold_checks++;
	if (census_cold_fault == 8
		|| (census_cold_check_error_after != 0
			&& census_cold_checks >= census_cold_check_error_after))
		siglongjmp(*PG_exception_stack, 1);
	return census_native_scope_current
		   && memcmp(source, &census_cold_selected.source, sizeof(*source)) == 0
		   && memcmp(checkpoint, &census_cold_selected.checkpoint, sizeof(*checkpoint)) == 0
		   && redo == census_cold_selected.native_redo
		   && serial == census_cold_selected.scope_serial;
}

static void
census_cold_reset(void)
{
	census_cold_check_error_after = 0;
	census_cold_fault = census_cold_reads = census_cold_checks = 0;
	census_cold_getter_fault_after = 1;
	namespace_read_transform = NULL;
}

static bool
census_cold_prepare(CensusFixture *f, Oid dbid, ClusterRelmapCensusRecovery *out)
{
	return cluster_relmap_retention_census_recovery_prepare_cold(f->census, dbid, out);
}

static void
census_cold_cleanup(CensusFixture *f)
{
	census_cold_reset();
	census_merger_held = census_namespace_x = cluster_shared_catalog = true;
	census_cleanup(f);
}

UT_TEST(test_census_cold_requires_drained_and_original_ownership)
{
	CensusFixture f;
	ClusterRelmapCensusRecovery out = { 0 };
	census_cold_reset();
	if (census_native_begin(&f, 5, 2, false)) {
		UT_ASSERT(!census_cold_prepare(&f, 5, &out));
		UT_ASSERT(census_recovery_zero(&out) && census_native_prepares == 0);
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		for (unsigned gate = 0; gate < 3; gate++) {
			uint8 digest[32];
			cluster_shared_catalog = gate != 0;
			census_merger_held = gate != 1;
			census_namespace_x = gate != 2;
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT(!census_cold_prepare(&f, 5, &out));
			UT_ASSERT(census_recovery_zero(&out) && census_native_prepares == 0);
			UT_ASSERT(cluster_recovery_root_set_digest(f.roots, digest));
			UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		}
		cluster_shared_catalog = census_merger_held = census_namespace_x = true;
		memset(&out, 0, sizeof(out));
		UT_ASSERT(census_cold_prepare(&f, 5, &out));
		if (out.handle != NULL)
			cluster_relmap_recovery_handle_release_v1(&out.handle);
	}
	census_cold_cleanup(&f);
}

UT_TEST(test_census_cold_selects_original_full_physical_pair_and_retains_all_pins)
{
	for (unsigned shared = 0; shared < 2; shared++)
		for (unsigned cut = 0; cut < 3; cut++) {
			CensusFixture f;
			ClusterRelmapCensusRecovery out = { 0 };
			ClusterRelmapRetentionDirectory saved[2];
			Oid dbid = shared ? InvalidOid : 5;
			census_cold_reset();
			if (census_native_begin(&f, dbid, cut, true)) {
				memcpy(saved, f.origin, sizeof(saved));
				UT_ASSERT(census_cold_prepare(&f, dbid, &out));
				UT_ASSERT(out.handle != NULL && out.pending.has_pending
						  && out.pending.pending_complete);
				UT_ASSERT_EQ(out.identity.dbid, dbid);
				UT_ASSERT_EQ(out.identity.shared, shared);
				UT_ASSERT_EQ(out.pending.pending_committed_generation, 1);
				UT_ASSERT_EQ(out.pending.committed_generation, cut == 1 ? 2 : 1);
				UT_ASSERT_EQ(memcmp(&out.pending, &census_native_pending, sizeof(out.pending)), 0);
				UT_ASSERT(census_cold_reads == 2 && census_cold_checks >= 2);
				UT_ASSERT_EQ(census_arbitrations, 0);
				census_batch_pins_same(&f, saved);
				UT_ASSERT_EQ(f.origin[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
				if (out.handle != NULL)
					cluster_relmap_recovery_handle_release_v1(&out.handle);
				census_batch_pins_same(&f, saved);
			}
			census_cold_cleanup(&f);
		}
}

UT_TEST(test_census_cold_source_scope_refusal_and_error_release_only_local_custody)
{
	for (unsigned fault = 1; fault <= 8; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out = { 0 };
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool threw = false;
		census_cold_reset();
		if (census_batch_begin(&f, 2, 2, false)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_cold_fault = fault;
			memset(&out, 0xa5, sizeof(out));
			PG_TRY();
			{
				UT_ASSERT(!census_cold_prepare(&f, 5, &out));
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(threw, fault == 2 || fault == 8);
			UT_ASSERT(census_recovery_zero(&out));
			UT_ASSERT_EQ(census_native_releases, 1);
			UT_ASSERT(!census_native_handles[0].live && census_arbitrations == 0);
			census_batch_pins_same(&f, saved);
			census_refused(&f);
		}
		census_cold_cleanup(&f);
	}
}

UT_TEST(test_census_cold_rechecks_new_namespace_same_bytes_inode_and_unrelated_root)
{
	for (unsigned fault = 11; fault <= 13; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out = { 0 };
		ClusterRelmapRetentionDirectory saved[2];
		census_cold_reset();
		if (census_batch_begin(&f, 2, 2, false)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_cold_fault = fault;
			UT_ASSERT(!census_cold_prepare(&f, 5, &out));
			UT_ASSERT(census_recovery_zero(&out) && census_native_releases == 1);
			census_batch_pins_same(&f, saved);
			census_refused(&f);
		}
		census_cold_cleanup(&f);
	}
}

UT_TEST(test_census_cold_rechecks_all_original_pins_including_late_root_io)
{
	const unsigned faults[] = { 9, 10, 19 };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out = { 0 };
		ClusterRelmapRetentionDirectory saved[2];
		census_cold_reset();
		if (census_batch_begin(&f, 2, 2, false)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_cold_fault = faults[i];
			UT_ASSERT(!census_cold_prepare(&f, 5, &out));
			UT_ASSERT(census_recovery_zero(&out) && census_native_releases == 1);
			if (faults[i] == 10)
				saved[1].pins[CLUSTER_RELMAP_RETENTION_CAPACITY - 1].wal_end++;
			else
				((ClusterRelmapRetentionPin *)census_pin(&saved[1], InvalidOid))->wal_end++;
			census_batch_pins_same(&f, saved);
			UT_ASSERT(namespace_read_transform == NULL);
			census_refused(&f);
		}
		census_cold_cleanup(&f);
	}
}

UT_TEST(test_census_cold_rechecks_original_ownership_metadata_and_source_after_io)
{
	for (unsigned fault = 14; fault <= 18; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out = { 0 };
		ClusterRelmapRetentionDirectory saved[2];
		census_cold_reset();
		if (census_batch_begin(&f, 2, 2, false)) {
			memcpy(saved, f.origin, sizeof(saved));
			census_cold_fault = fault;
			UT_ASSERT(!census_cold_prepare(&f, 5, &out));
			UT_ASSERT(census_recovery_zero(&out) && census_native_releases == 1);
			if (fault == 18)
				UT_ASSERT_EQ(census_cold_reads, 2);
			census_batch_pins_same(&f, saved);
			census_refused(&f);
		}
		census_cold_cleanup(&f);
	}
}

UT_TEST(test_census_cold_missing_original_pending_pin_and_native_prepare_failure)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out = { 0 };
		ClusterRelmapRetentionDirectory saved[2];
		volatile bool threw = false;
		census_cold_reset();
		if (census_native_begin(&f, 5, 2, true)) {
			if (fault == 1)
				((ClusterRelmapRetentionPin *)census_pin(&f.origin[0], 5))->may_be_staged = false;
			if (fault == 2)
				census_pair(f.fd, 5, 3, true, false, false);
			if (fault >= 3)
				census_native_fault = fault - 2;
			memcpy(saved, f.origin, sizeof(saved));
			PG_TRY();
			{
				UT_ASSERT(!census_cold_prepare(&f, fault == 0 ? InvalidOid : 5, &out));
			}
			PG_CATCH();
			{
				threw = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(threw, fault == 4);
			UT_ASSERT(census_recovery_zero(&out) && census_native_releases == 0);
			UT_ASSERT_EQ(census_native_prepares, fault >= 3 ? 1 : 0);
			census_batch_pins_same(&f, saved);
			census_refused(&f);
		}
		census_cold_cleanup(&f);
	}
}

UT_TEST(test_census_cold_known_output_overlap_never_changes_borrowed_inputs)
{
	CensusFixture f;
	ClusterRelmapCensusRecovery before, out = { 0 };
	census_cold_reset();
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_relmap_retention_census_recovery_prepare_cold(NULL, 5, &out));
	UT_ASSERT(census_recovery_zero(&out));
	if (census_native_begin(&f, 5, 2, true)) {
		void *aliases[] = { f.census, &f.origin[0], &f.origin[1].pins[2], f.roots };
		for (unsigned i = 0; i < lengthof(aliases); i++) {
			memcpy(&before, aliases[i], sizeof(before));
			UT_ASSERT(!census_cold_prepare(&f, 5, aliases[i]));
			UT_ASSERT_EQ(memcmp(&before, aliases[i], sizeof(before)), 0);
		}
		UT_ASSERT(!census_cold_prepare(&f, 5, NULL));
		UT_ASSERT_EQ(census_native_prepares, 0);
		memset(&out, 0, sizeof(out));
		UT_ASSERT(census_cold_prepare(&f, 5, &out));
		if (out.handle != NULL)
			cluster_relmap_recovery_handle_release_v1(&out.handle);
	}
	census_cold_cleanup(&f);
}
