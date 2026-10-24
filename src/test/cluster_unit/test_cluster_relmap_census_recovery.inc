/* Original ROOT/authority-file census -> native custody preparation.
 * The native scope is a controlled provider; ROOT readers and files are real.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-11-pre2-p0-m5-e-relmap-retention.md, sections3/4. */
#include "cluster/cluster_wal_inputs.h"

static unsigned char census_native_cookie, census_inputs_cookie;
static unsigned census_native_prepares, census_native_releases;
static unsigned census_native_fault;
static CensusFixture *census_native_fixture;
static ClusterRelmapV2Namespace census_native_identity;
static ClusterRelmapV2View census_native_pending;
static ClusterWalInputV1 census_native_sources[2];
static uint32 census_native_source_count;
static bool census_native_scope_current;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

ClusterControlRootResult
cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs)
{
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&census_inputs_cookie);
	return census_native_scope_current ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

uint32
cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs)
{
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&census_inputs_cookie);
	return census_native_source_count;
}

const ClusterWalInputV1 *
cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs, uint32 index)
{
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&census_inputs_cookie);
	UT_ASSERT(index < lengthof(census_native_sources));
	return index < lengthof(census_native_sources) ? &census_native_sources[index] : NULL;
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
cluster_relmap_recovery_prepare_v1(struct ClusterWalInputsV1 *inputs,
								   const ClusterRelmapV2Namespace *ns,
								   const ClusterRelmapV2View *view,
								   ClusterRelmapRecoveryHandle **out)
{
	census_native_prepares++;
	UT_ASSERT(inputs == (struct ClusterWalInputsV1 *)&census_inputs_cookie);
	UT_ASSERT(ns != NULL && view != NULL && out != NULL);
	*out = NULL;
	if (census_native_fault == 1)
		return false;
	if (census_native_fault == 2)
		siglongjmp(*PG_exception_stack, 1);
	census_native_identity = *ns;
	census_native_pending = *view;
	*out = (ClusterRelmapRecoveryHandle *)&census_native_cookie;
	if (census_native_fault == 3)
		census_native_fixture->origin[1].last_issued++;
	if (census_native_fault == 4)
		((ClusterRelmapRetentionPin *)census_pin(&census_native_fixture->origin[0], ns->dbid))
			->wal_end++;
	if (census_native_fault == 5)
		cluster_recovery_root_set_invalidate(census_native_fixture->roots);
	if (census_native_fault == 6)
		census_native_sources[1].source.claim.identity.storage_uuid[0] ^= 1;
	return true;
}

void
cluster_relmap_recovery_handle_release_v1(ClusterRelmapRecoveryHandle **handle)
{
	UT_ASSERT(handle != NULL && *handle == (ClusterRelmapRecoveryHandle *)&census_native_cookie);
	census_native_releases++;
	*handle = NULL;
}

static bool
census_native_begin(CensusFixture *f, Oid dbid, unsigned cut, bool drain)
{
	census_fixture(f, true);
	if (dbid == InvalidOid) {
		census_pair(f->fd, 5, 0, false, false, false);
		census_pair(f->fd, InvalidOid, 3, cut == 0, cut != 0, cut == 1);
	} else
		census_pair(f->fd, 5, 3, cut == 0, cut != 0, cut == 1);
	census_native_fixture = f;
	census_native_prepares = census_native_releases = census_native_fault = 0;
	memset(census_native_sources, 0, sizeof(census_native_sources));
	for (size_t i = 0; i < lengthof(census_native_sources); i++) {
		census_native_sources[i].source.claim.identity.system_identifier = 817;
		census_native_sources[i].source.claim.database_incarnation = 91;
		memcpy(census_native_sources[i].source.claim.identity.storage_uuid, root_uuid, 16);
	}
	census_native_source_count = lengthof(census_native_sources);
	census_native_scope_current = true;
	return census_observe_begin(f) && (!drain || census_drain(f) == CLUSTER_RELMAP_CENSUS_DRAINED);
}

static bool
census_native_prepare(CensusFixture *f, Oid dbid, ClusterRelmapCensusRecovery *out)
{
	return cluster_relmap_retention_census_recovery_prepare(
		f->census, (struct ClusterWalInputsV1 *)&census_inputs_cookie, dbid, out);
}

static bool
census_recovery_zero(const ClusterRelmapCensusRecovery *out)
{
	ClusterRelmapCensusRecovery zero = { 0 };

	return memcmp(out, &zero, sizeof(zero)) == 0;
}

UT_TEST(test_census_recovery_prepares_only_after_whole_root_drained)
{
	CensusFixture f;
	ClusterRelmapCensusRecovery out;
	ClusterRelmapRetentionPin before;

	if (census_native_begin(&f, 5, 2, false)) {
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!census_native_prepare(&f, 5, &out));
		UT_ASSERT(census_recovery_zero(&out));
		UT_ASSERT_EQ(census_native_prepares, 0);
		UT_ASSERT_EQ(census_drain(&f), CLUSTER_RELMAP_CENSUS_DRAINED);
		before = *census_pin(&f.origin[0], 5);
		UT_ASSERT(census_native_prepare(&f, 5, &out));
		UT_ASSERT(out.handle != NULL);
		UT_ASSERT_EQ(memcmp(&before, census_pin(&f.origin[0], 5), sizeof(before)), 0);
		UT_ASSERT_EQ(census_native_prepares, 1);
		cluster_relmap_recovery_handle_release_v1(&out.handle);
	}
	census_cleanup(&f);
}

UT_TEST(test_census_recovery_exact_namespace_and_partial_publish_base)
{
	for (unsigned shared = 0; shared <= 1; shared++)
		for (unsigned cut = 0; cut < 3; cut++) {
			CensusFixture f;
			Oid dbid = shared ? InvalidOid : 5;
			ClusterRelmapCensusRecovery out;
			ClusterRelmapRetentionDirectory before[2];

			if (census_native_begin(&f, dbid, cut, true)) {
				memcpy(before, f.origin, sizeof(before));
				UT_ASSERT(census_native_prepare(&f, dbid, &out));
				UT_ASSERT(out.handle != NULL);
				UT_ASSERT_EQ(out.identity.dbid, dbid);
				UT_ASSERT_EQ(out.identity.shared, shared);
				UT_ASSERT_EQ(out.identity.system_identifier, 817);
				UT_ASSERT_EQ(out.identity.database_incarnation, 91);
				UT_ASSERT_EQ(out.pending.committed_generation, cut == 1 ? 2 : 1);
				UT_ASSERT_EQ(out.pending.pending_committed_generation, 1);
				UT_ASSERT_EQ(out.pending.pending_committed.mappings[0].mapfilenumber, 40000);
				UT_ASSERT_EQ(out.pending.pending_owner.top_full_xid, CENSUS_FULL);
				UT_ASSERT_EQ(out.pending.pending_owner.node, 3);
				UT_ASSERT_EQ(memcmp(&out.pending, &census_native_pending, sizeof(out.pending)), 0);
				UT_ASSERT_EQ(memcmp(before, f.origin, sizeof(before)), 0);
				UT_ASSERT_EQ(cluster_relmap_retention_census_next(f.census),
							 CLUSTER_RELMAP_CENSUS_DRAINED);
				cluster_relmap_recovery_handle_release_v1(&out.handle);
			}
			census_cleanup(&f);
		}
}

UT_TEST(test_census_recovery_requires_exact_already_restored_pin)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out;

		if (census_native_begin(&f, 5, 2, true)) {
			ClusterRelmapRetentionPin *pin
				= (ClusterRelmapRetentionPin *)census_pin(&f.origin[0], 5);
			ClusterRelmapRetentionPin before;

			if (fault == 0)
				pin->occupied = false;
			if (fault == 1)
				pin->wal_end++;
			if (fault == 2)
				pin->key.boot_incarnation++;
			if (fault == 3)
				pin->may_be_staged = false;
			if (fault == 4) {
				for (size_t i = 0; i < lengthof(f.origin[0].pins); i++)
					if (!f.origin[0].pins[i].occupied) {
						f.origin[0].pins[i] = *pin;
						break;
					}
			}
			before = *pin;
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT(!census_native_prepare(&f, 5, &out));
			UT_ASSERT(census_recovery_zero(&out));
			UT_ASSERT_EQ(memcmp(&before, pin, sizeof(before)), 0);
			UT_ASSERT_EQ(census_native_prepares, 0);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_census_recovery_native_refusal_and_error_retain_original_pins)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out;
		ClusterRelmapRetentionPin before;
		volatile bool caught = false;

		if (census_native_begin(&f, 5, 2, true)) {
			before = *census_pin(&f.origin[0], 5);
			census_native_fault = fault;
			memset(&out, 0xa5, sizeof(out));
			PG_TRY();
			{
				UT_ASSERT(!census_native_prepare(&f, 5, &out));
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 2);
			UT_ASSERT(census_recovery_zero(&out));
			UT_ASSERT_EQ(memcmp(&before, census_pin(&f.origin[0], 5), sizeof(before)), 0);
			UT_ASSERT_EQ(census_native_releases, 0);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_census_recovery_post_native_borrowed_drift_discards_local_handle)
{
	for (unsigned fault = 3; fault <= 6; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out;

		if (census_native_begin(&f, 5, 2, true)) {
			census_native_fault = fault;
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT(!census_native_prepare(&f, 5, &out));
			UT_ASSERT(census_recovery_zero(&out));
			UT_ASSERT_EQ(census_native_prepares, 1);
			UT_ASSERT_EQ(census_native_releases, 1);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

UT_TEST(test_census_recovery_changed_pair_and_absent_pending_refuse)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out;

		if (census_native_begin(&f, 5, 2, true)) {
			if (fault == 0)
				census_pair(f.fd, 5, 3, true, true, false);
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT(!census_native_prepare(&f,
											 fault == 1	  ? InvalidOid
											 : fault == 2 ? 99
														  : 5,
											 &out));
			UT_ASSERT(census_recovery_zero(&out));
			UT_ASSERT_EQ(census_native_prepares, 0);
			UT_ASSERT(census_pin(&f.origin[0], 5) != NULL);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}

static void
census_native_valid_crc_replacement(void *buffer, size_t length, off_t offset)
{
	ClusterRelmapV2Namespace ns = { 817, 91, 5, false };
	ClusterRelmapV2Image image;

	if (length != CLUSTER_RELMAP_V2_FILE_SIZE || offset != 0)
		return;
	UT_ASSERT_EQ(cluster_relmap_v2_decode(buffer, length, &ns, &image), CLUSTER_RELMAP_V2_VALID);
	image.committed = census_map(40002);
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, buffer, length), CLUSTER_RELMAP_V2_VALID);
}

UT_TEST(test_census_recovery_full_sha_rejects_valid_crc_read_substitution)
{
	CensusFixture f;
	ClusterRelmapCensusRecovery out;
	ClusterRelmapRetentionPin before;

	if (census_native_begin(&f, 5, 2, true)) {
		before = *census_pin(&f.origin[0], 5);
		/* Same real inode/stat and unchanged on-disk bytes. Only this read's
		 * returned image changes, with valid native map and file CRCs. */
		namespace_read_transform = census_native_valid_crc_replacement;
		UT_ASSERT(!census_native_prepare(&f, 5, &out));
		namespace_read_transform = NULL;
		UT_ASSERT(census_recovery_zero(&out));
		UT_ASSERT_EQ(census_native_prepares, 0);
		UT_ASSERT_EQ(memcmp(&before, census_pin(&f.origin[0], 5), sizeof(before)), 0);
		census_refused(&f);
	}
	census_cleanup(&f);
}

UT_TEST(test_census_recovery_requires_original_wal_storage_scope)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		CensusFixture f;
		ClusterRelmapCensusRecovery out;
		ClusterRelmapRetentionPin before;

		if (census_native_begin(&f, 5, 2, true)) {
			before = *census_pin(&f.origin[0], 5);
			if (fault == 0)
				census_native_sources[1].source.claim.identity.system_identifier++;
			if (fault == 1)
				census_native_sources[1].source.claim.database_incarnation++;
			if (fault == 2)
				census_native_sources[1].source.claim.identity.storage_uuid[15] ^= 1;
			if (fault == 3)
				census_native_source_count = 0;
			if (fault == 4)
				census_native_source_count = CLUSTER_WAL_INPUTS_MAX + 1;
			if (fault == 5)
				census_native_scope_current = false;
			UT_ASSERT(!census_native_prepare(&f, 5, &out));
			UT_ASSERT(census_recovery_zero(&out));
			UT_ASSERT_EQ(census_native_prepares, 0);
			UT_ASSERT_EQ(memcmp(&before, census_pin(&f.origin[0], 5), sizeof(before)), 0);
			census_refused(&f);
		}
		census_cleanup(&f);
	}
}
