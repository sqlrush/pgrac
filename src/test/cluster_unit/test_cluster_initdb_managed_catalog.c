/* Original catalog publication from complete independently selected CLOG0. */
#define main managed_catalog_original_main
int managed_catalog_original_main(void);
#include "test_cluster_initdb_catalog.c"
#undef main
#include "access/clog_file.h"

#ifdef CATALOG_RAW_PARENT
#define cluster_initdb_catalog_read_managed_clog_v2(d, e, n, b, l) \
	cluster_initdb_catalog_read_clog(d, n, b, l)
#endif

static ClusterClogFileBindingV2 selected;
static uint8 managed_image[CLUSTER_CLOG_FILE_HEADER_BYTES + CLUSTER_CLOG_FRAME_BYTES];

static void
managed_prepare(uint16 origin)
{
	ClusterClogPageIdentityV2 page = {0};
	prepare();
	memset(&selected, 0, sizeof(selected));
	selected.identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	selected.identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	selected.identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	selected.identity.system_identifier = control.system_identifier;
	selected.identity.database_incarnation = 1;
	selected.identity.storage_uuid[0] = 0x23;
	selected.identity.redo_branch_uuid[0] = 0x45;
	selected.identity.redo_branch_generation = 1;
	selected.identity.redo_timeline = 1;
	selected.identity.creation_generation = 1;
	selected.identity.registration_generation = 1;
	selected.identity.origin = origin;
	page.identity.birth_observed = true;
	page.identity.birth_full_xid = FullTransactionIdFromU64(3);
	page.identity.page_generation = 3;
	page.identity.page_lsn = 1234;
	page.image_lsn = 2345;
	UT_ASSERT(cluster_clog_file_header_encode_v2(&selected, managed_image,
		CLUSTER_CLOG_FILE_HEADER_BYTES));
	UT_ASSERT(cluster_clog_file_page_encode_v2(&selected, &page, clog_bytes, BLCKSZ,
		managed_image + CLUSTER_CLOG_FILE_HEADER_BYTES, CLUSTER_CLOG_FRAME_BYTES));
	UT_ASSERT(unlinkat(clogdir, "0000", 0) == 0);
	put(clogdir, "0000", managed_image, sizeof(managed_image));
}

static bool
managed_read(uint64 next, uint8 **bytes, Size *length)
{
	return cluster_initdb_catalog_read_managed_clog_v2(clogdir,
		&selected.identity, next, bytes, length);
}

UT_TEST(managed_payload_populates_original_catalog_for_each_origin)
{
	const uint16 origins[] = {0, 1, 127};
	for (unsigned i = 0; i < lengthof(origins); ++i) {
		uint8 *bytes = NULL;
		Size length = 0;
		managed_prepare(origins[i]);
		UT_ASSERT(managed_read(750, &bytes, &length));
		UT_ASSERT(length == BLCKSZ && memcmp(bytes, clog_bytes, BLCKSZ) == 0);
		input.native_clog = bytes;
		input.native_clog_length = length;
		UT_ASSERT(cluster_initdb_catalog_create(global, &input));
		free(bytes);
		cleanup();
	}
}

UT_TEST(managed_expected_identity_is_independent_of_file)
{
	for (unsigned fault = 0; fault < 7; ++fault) {
		uint8 *bytes = NULL;
		Size length = 99;
		managed_prepare(1);
		if (fault == 0) selected.identity.origin = 0;
		if (fault == 1) selected.identity.system_identifier++;
		if (fault == 2) selected.identity.database_incarnation++;
		if (fault == 3) selected.identity.creation_generation++;
		if (fault == 4) selected.identity.redo_branch_uuid[0]++;
		if (fault == 5) selected.identity.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
		if (fault == 6) selected.identity.object_id = 1;
		UT_ASSERT(!managed_read(750, &bytes, &length));
		UT_ASSERT(bytes == NULL && length == 0);
		absent();
		cleanup();
	}
}

UT_TEST(managed_bad_birth_crc_or_partial_file_cannot_supply_history)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		uint8 *bytes = NULL;
		Size length = 99;
		int fd;
		managed_prepare(1);
		fd = openat(clogdir, "0000", O_RDWR);
		UT_ASSERT(fd >= 0);
		if (fault < 2) {
			ClusterClogPageIdentityV2 page = {0};
			page.identity.birth_observed = true;
			page.identity.birth_full_xid = FullTransactionIdFromU64(fault ? (UINT64CONST(1)<<32)+3 : 3);
			page.identity.page_generation = fault ? 3 : 4;
			UT_ASSERT(cluster_clog_file_page_encode_v2(&selected, &page, clog_bytes, BLCKSZ,
				managed_image + CLUSTER_CLOG_FILE_HEADER_BYTES, CLUSTER_CLOG_FRAME_BYTES));
		}
		if (fault == 2) managed_image[64] ^= 1;
		if (fault == 3) managed_image[sizeof(managed_image)-1] ^= 1;
		UT_ASSERT(pwrite(fd, managed_image, sizeof(managed_image), 0) == sizeof(managed_image));
		if (fault == 4) UT_ASSERT(ftruncate(fd, sizeof(managed_image)-1) == 0);
		if (fault == 5) UT_ASSERT(ftruncate(fd, sizeof(managed_image)+CLUSTER_CLOG_FRAME_BYTES) == 0);
		UT_ASSERT(close(fd) == 0);
		UT_ASSERT(!managed_read(750, &bytes, &length));
		UT_ASSERT(bytes == NULL && length == 0);
		absent();
		cleanup();
	}
}

UT_TEST(managed_read_error_or_late_source_change_preserves_failure)
{
	for (unsigned fault = 1; fault <= 3; ++fault) {
		uint8 *bytes = NULL;
		Size length = 99;
		managed_prepare(1);
		io_fault = fault;
		UT_ASSERT(!managed_read(750, &bytes, &length));
		UT_ASSERT(bytes == NULL && length == 0);
		absent();
		cleanup();
	}
}

UT_TEST(managed_initial_inventory_cannot_be_expanded_from_counter)
{
	const uint64 nexts[] = {2, BLCKSZ*4+1, CLUSTER_XID_PREHISTORY_MAX_XID+1};
	managed_prepare(1);
	for (unsigned i = 0; i < lengthof(nexts); ++i) {
		uint8 *bytes = NULL;
		Size length = 99;
		UT_ASSERT(!managed_read(nexts[i], &bytes, &length));
		UT_ASSERT(bytes == NULL && length == 0);
	}
	cleanup();
}

UT_TEST(managed_unsafe_path_never_falls_back_to_raw)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		uint8 *bytes = NULL;
		Size length = 99;
		managed_prepare(1);
		UT_ASSERT(renameat(clogdir, "0000", clogdir, "old") == 0);
		if (fault == 0) UT_ASSERT(symlinkat("old", clogdir, "0000") == 0);
		if (fault == 1) UT_ASSERT(linkat(clogdir, "old", clogdir, "0000", 0) == 0);
		if (fault == 2) UT_ASSERT(mkfifoat(clogdir, "0000", 0600) == 0);
		if (fault == 3) put(clogdir, "0000", clog_bytes, BLCKSZ);
		UT_ASSERT(!managed_read(750, &bytes, &length));
		UT_ASSERT(bytes == NULL && length == 0);
		cleanup();
	}
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(managed_payload_populates_original_catalog_for_each_origin);
	UT_RUN(managed_expected_identity_is_independent_of_file);
	UT_RUN(managed_bad_birth_crc_or_partial_file_cannot_supply_history);
	UT_RUN(managed_read_error_or_late_source_change_preserves_failure);
	UT_RUN(managed_initial_inventory_cannot_be_expanded_from_counter);
	UT_RUN(managed_unsafe_path_never_falls_back_to_raw);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
