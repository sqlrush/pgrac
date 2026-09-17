/*-------------------------------------------------------------------------
 *
 * test_cluster_page_install.c
 *    STOP-06 real storage-install engine contract.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_page_install.h")
#include "cluster/cluster_page_install.h"
#include "storage/bufpage.h"
#define TEST_HAVE_CLUSTER_PAGE_INSTALL 1
#endif
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#ifndef TEST_HAVE_CLUSTER_PAGE_INSTALL

UT_TEST(test_page_install_capability_red)
{
	printf("# JIT_SEMANTIC_RED:D6-6-SMGR-DURABILITY-POSTREAD\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_page_install_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

#define TEST_INSTALL_BATCH_TARGETS 40

typedef struct InstallFixture {
	PGAlignedBlock disk[TEST_INSTALL_BATCH_TARGETS];
	bool exists[TEST_INSTALL_BATCH_TARGETS];
	bool read_ok[TEST_INSTALL_BATCH_TARGETS];
	bool write_ok[TEST_INSTALL_BATCH_TARGETS];
	bool sync_ok[TEST_INSTALL_BATCH_TARGETS];
	bool identity_ok;
	bool promote_ok;
	bool publish_ok;
	bool release_ok;
	bool checksums_enabled;
	int read_calls;
	int write_calls;
	int sync_calls;
	int identity_calls;
	int promote_calls;
	int publish_calls;
	int release_calls;
	int promote_step;
	int first_initial_read_step;
	int first_write_step;
	int last_initial_read_step;
	int step;
	bool last_extend;
	bool zero_postread;
	uint32 initial_read_count;
} InstallFixture;

typedef struct InstallCase {
	InstallFixture fixture;
	RfPageStorageInstallComponentV1 components[TEST_INSTALL_BATCH_TARGETS];
	PGAlignedBlock canonical[TEST_INSTALL_BATCH_TARGETS];
	PGAlignedBlock prepared[TEST_INSTALL_BATCH_TARGETS];
	PGAlignedBlock io[TEST_INSTALL_BATCH_TARGETS];
	RfPageInstallStorageOpsV1 storage;
	RfPageInstallAuthorityOpsV1 authority;
	RfPageStorageInstallRequestV1 request;
} InstallCase;

bool
rf_page_identity_valid_v1(const RfPageIdentityV1 *identity)
{
	return identity != NULL && identity->system_identifier != 0
		   && identity->locator.spcOid != InvalidOid && identity->locator.dbOid != InvalidOid
		   && identity->locator.relNumber != InvalidRelFileNumber
		   && identity->blockno != InvalidBlockNumber && identity->reserved_zero == 0;
}

bool
rf_page_version_present_v1(const RfPageVersionV1 *version)
{
	uint8 value = 0;
	int i;

	if (version == NULL || version->mutation_token == 0)
		return false;
	for (i = 0; i < 16; i++)
		value |= version->segment_incarnation[i];
	return value != 0;
}

static void
set_incarnation(uint8 incarnation[16], uint8 value)
{
	memset(incarnation, value, 16);
}

static bool
storage_read(void *arg, uint32 index, const RfPageIdentityV1 *identity, char page[BLCKSZ],
			 bool *exists)
{
	InstallFixture *fixture = (InstallFixture *)arg;

	fixture->step++;
	fixture->read_calls++;
	if (fixture->read_calls <= (int)fixture->initial_read_count) {
		if (fixture->first_initial_read_step == 0)
			fixture->first_initial_read_step = fixture->step;
		fixture->last_initial_read_step = fixture->step;
	}
	if (!fixture->read_ok[index])
		return false;
	*exists = fixture->exists[index];
	if (fixture->zero_postread && fixture->read_calls > (int)fixture->initial_read_count) {
		*exists = true;
		memset(page, 0, BLCKSZ);
		return true;
	}
	if (*exists)
		memcpy(page, fixture->disk[index].data, BLCKSZ);
	else
		memset(page, 0, BLCKSZ);
	return true;
}

static bool
storage_write(void *arg, uint32 index, const RfPageIdentityV1 *identity, const char page[BLCKSZ],
			  bool extend)
{
	InstallFixture *fixture = (InstallFixture *)arg;

	fixture->step++;
	if (fixture->first_write_step == 0)
		fixture->first_write_step = fixture->step;
	fixture->write_calls++;
	fixture->last_extend = extend;
	if (!fixture->write_ok[index])
		return false;
	memcpy(fixture->disk[index].data, page, BLCKSZ);
	fixture->exists[index] = true;
	return true;
}

static bool
storage_sync(void *arg, uint32 index, const RfPageIdentityV1 *identity)
{
	InstallFixture *fixture = (InstallFixture *)arg;

	fixture->step++;
	fixture->sync_calls++;
	return fixture->sync_ok[index];
}

static uint16
storage_checksum(void *arg, const char page[BLCKSZ], BlockNumber blockno)
{
	const uint8 *bytes = (const uint8 *)page;
	uint32 sum = blockno + 1;
	int i;

	for (i = 0; i < BLCKSZ; i++)
		if (i < (int)offsetof(PageHeaderData, pd_checksum)
			|| i >= (int)(offsetof(PageHeaderData, pd_checksum) + sizeof(uint16)))
			sum = (sum * 33) ^ bytes[i];
	return (uint16)((sum % 65535) + 1);
}

static bool
authority_identity(void *arg, const RfPageIdentityV1 *identity, const uint8 incarnation[16])
{
	InstallFixture *fixture = (InstallFixture *)arg;

	fixture->identity_calls++;
	return fixture->identity_ok;
}

static bool
authority_promote(void *arg)
{
	InstallFixture *fixture = (InstallFixture *)arg;

	fixture->step++;
	fixture->promote_calls++;
	fixture->promote_step = fixture->step;
	return fixture->promote_ok;
}

static bool
authority_publish(void *arg)
{
	InstallFixture *fixture = (InstallFixture *)arg;

	fixture->step++;
	fixture->publish_calls++;
	return fixture->publish_ok;
}

static bool
authority_release(void *arg)
{
	InstallFixture *fixture = (InstallFixture *)arg;

	fixture->step++;
	fixture->release_calls++;
	return fixture->release_ok;
}

static void
init_page(char page[BLCKSZ], uint64 token, uint8 payload)
{
	PageHeader header;

	memset(page, payload, BLCKSZ);
	header = (PageHeader)page;
	memset(header, 0, SizeOfPageHeaderData);
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = BLCKSZ;
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = (BLCKSZ & 0xFF00) | PG_PAGE_LAYOUT_VERSION;
	header->pd_block_scn = token;
}

static void
init_case(InstallCase *test_case, uint32 count)
{
	uint32 i;

	memset(test_case, 0, sizeof(*test_case));
	test_case->fixture.identity_ok = true;
	test_case->fixture.promote_ok = true;
	test_case->fixture.publish_ok = true;
	test_case->fixture.release_ok = true;
	test_case->fixture.initial_read_count = count;
	for (i = 0; i < TEST_INSTALL_BATCH_TARGETS; i++) {
		test_case->fixture.read_ok[i] = true;
		test_case->fixture.write_ok[i] = true;
		test_case->fixture.sync_ok[i] = true;
	}
	for (i = 0; i < count; i++) {
		RfPageStorageInstallComponentV1 *component = &test_case->components[i];

		component->page_identity.system_identifier = 99;
		memset(component->page_identity.storage_uuid, 3, 16);
		component->page_identity.locator.spcOid = 1;
		component->page_identity.locator.dbOid = 2;
		component->page_identity.locator.relNumber = 100 + i;
		component->page_identity.forknum = MAIN_FORKNUM;
		component->page_identity.blockno = i;
		component->before_kind = RF_PAGE_STATE_PRESENT;
		set_incarnation(component->expected_before.segment_incarnation, 7);
		component->expected_before.mutation_token = 10;
		set_incarnation(component->expected_result.segment_incarnation, 7);
		component->expected_result.mutation_token = 11;
		component->canonical_page = test_case->canonical[i].data;
		init_page(test_case->canonical[i].data, 11, (uint8)(0x40 + i));
		init_page(test_case->fixture.disk[i].data, 10, (uint8)(0x20 + i));
		test_case->fixture.exists[i] = true;
	}
	test_case->storage.arg = &test_case->fixture;
	test_case->storage.read = storage_read;
	test_case->storage.write = storage_write;
	test_case->storage.sync = storage_sync;
	test_case->storage.checksum = storage_checksum;
	test_case->storage.checksums_enabled = test_case->fixture.checksums_enabled;
	test_case->authority.arg = &test_case->fixture;
	test_case->authority.validate_identity = authority_identity;
	test_case->authority.promote = authority_promote;
	test_case->authority.publish = authority_publish;
	test_case->authority.release = authority_release;
	test_case->request.components = test_case->components;
	test_case->request.component_count = count;
	test_case->request.prepared_pages = test_case->prepared[0].data;
	test_case->request.prepared_capacity = sizeof(test_case->prepared);
	test_case->request.io_pages = test_case->io[0].data;
	test_case->request.io_capacity = sizeof(test_case->io);
	test_case->request.storage = &test_case->storage;
	test_case->request.authority = &test_case->authority;
	test_case->request.global_preflight_ok = true;
}

UT_TEST(test_expected_target_write_sync_postread_publish_release)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.sync_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 2);
	UT_ASSERT_EQ(test_case.fixture.publish_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.release_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.identity_calls, 2);
	UT_ASSERT(proof.durability_complete && proof.postread_complete);
}

UT_TEST(test_result_target_skips_write_but_proves_durability)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	memcpy(test_case.fixture.disk[0].data, test_case.canonical[0].data, BLCKSZ);
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.sync_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 2);
	UT_ASSERT_EQ(proof.result_skip_count, 1);
}

UT_TEST(test_noncanonical_result_target_blocks_whole_batch_before_promote)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 2);
	init_page(test_case.fixture.disk[0].data, 11, 0x7F);
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED);
	UT_ASSERT_EQ(test_case.fixture.promote_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.release_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
}

UT_TEST(test_absent_target_extends_only_for_absent_edge)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	test_case.fixture.exists[0] = false;
	test_case.components[0].before_kind = RF_PAGE_STATE_ABSENT;
	memset(&test_case.components[0].expected_before, 0,
		   sizeof(test_case.components[0].expected_before));
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 1);
	UT_ASSERT(test_case.fixture.last_extend);
}

UT_TEST(test_absent_target_with_present_edge_is_zero_mutation)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	test_case.fixture.exists[0] = false;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH);
	UT_ASSERT_EQ(test_case.fixture.promote_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.release_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
}

UT_TEST(test_unrelated_valid_token_blocks_whole_batch)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 2);
	((PageHeader)test_case.fixture.disk[1].data)->pd_block_scn = 99;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH);
	UT_ASSERT_EQ(test_case.fixture.promote_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.release_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
}

UT_TEST(test_torn_target_requires_identity_authority)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	memset(test_case.fixture.disk[0].data, 0, BLCKSZ);
	test_case.fixture.identity_ok = false;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
}

UT_TEST(test_postread_zero_page_fails_and_releases_without_publish)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	test_case.fixture.zero_postread = true;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED);
	UT_ASSERT_EQ(test_case.fixture.publish_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.release_calls, 1);
}

UT_TEST(test_sync_failure_releases_without_proof)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	test_case.fixture.sync_ok[0] = false;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED);
	UT_ASSERT_EQ(test_case.fixture.publish_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.release_calls, 1);
}

UT_TEST(test_checksum_modes_canonicalize_exactly)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	test_case.fixture.checksums_enabled = true;
	test_case.storage.checksums_enabled = true;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_NE(((PageHeader)test_case.fixture.disk[0].data)->pd_checksum, 0);

	init_case(&test_case, 1);
	((PageHeader)test_case.canonical[0].data)->pd_checksum = 0xCAFE;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(((PageHeader)test_case.fixture.disk[0].data)->pd_checksum, 0);
}

UT_TEST(test_whole_batch_reads_precede_first_write)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, TEST_INSTALL_BATCH_TARGETS);
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(test_case.fixture.write_calls, TEST_INSTALL_BATCH_TARGETS);
	UT_ASSERT(test_case.fixture.first_initial_read_step > test_case.fixture.promote_step);
	UT_ASSERT(test_case.fixture.first_write_step > test_case.fixture.last_initial_read_step);
}

UT_TEST(test_promote_failure_performs_no_target_io)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	test_case.fixture.promote_ok = false;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT_EQ(test_case.fixture.promote_calls, 1);
	UT_ASSERT_EQ(test_case.fixture.read_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.write_calls, 0);
	UT_ASSERT_EQ(test_case.fixture.release_calls, 0);
}

UT_TEST(test_reserved_and_nonordinary_fork_are_rejected)
{
	InstallCase test_case;
	RfPageStorageInstallProofV1 proof;

	init_case(&test_case, 1);
	test_case.components[0].reserved_zero[0] = 1;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(test_case.fixture.promote_calls, 0);

	init_case(&test_case, 1);
	test_case.components[0].page_identity.forknum = FSM_FORKNUM;
	UT_ASSERT_EQ(rf_page_storage_install_execute_v1(&test_case.request, &proof),
				 RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(test_case.fixture.promote_calls, 0);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_expected_target_write_sync_postread_publish_release);
	UT_RUN(test_result_target_skips_write_but_proves_durability);
	UT_RUN(test_noncanonical_result_target_blocks_whole_batch_before_promote);
	UT_RUN(test_absent_target_extends_only_for_absent_edge);
	UT_RUN(test_absent_target_with_present_edge_is_zero_mutation);
	UT_RUN(test_unrelated_valid_token_blocks_whole_batch);
	UT_RUN(test_torn_target_requires_identity_authority);
	UT_RUN(test_postread_zero_page_fails_and_releases_without_publish);
	UT_RUN(test_sync_failure_releases_without_proof);
	UT_RUN(test_checksum_modes_canonicalize_exactly);
	UT_RUN(test_whole_batch_reads_precede_first_write);
	UT_RUN(test_promote_failure_performs_no_target_io);
	UT_RUN(test_reserved_and_nonordinary_fork_are_rejected);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
