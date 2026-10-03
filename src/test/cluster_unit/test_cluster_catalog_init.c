/* Original catalog images through the existing consumer codecs.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/transam.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_catalog_init.h"
#include "cluster/cluster_catalog_migrate.h"
#include "cluster/cluster_oid_lease.h"
#include "cluster/cluster_xid_authority.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
static ControlFileData control;
static ClusterCatalogInitialInput input;
static uint8 clog[BLCKSZ];
static uint8 output[BLCKSZ + sizeof(ClusterXidPrehistoryHeader)];
static const char *native_directory;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

static void
crc_control(void)
{
	INIT_CRC32C(control.crc);
	COMP_CRC32C(control.crc, &control, offsetof(ControlFileData, crc));
	FIN_CRC32C(control.crc);
}
static void
reset(void)
{
	memset(&control, 0, sizeof(control));
	memset(&input, 0, sizeof(input));
	memset(clog, 0, sizeof(clog));
	memset(output, 0, sizeof(output));
	control.system_identifier = UINT64CONST(7584383251700000001);
	control.pg_control_version = PG_CONTROL_VERSION;
	control.catalog_version_no = CATALOG_VERSION_NO;
	control.state = DB_SHUTDOWNED;
	control.checkPoint = control.checkPointCopy.redo = 0x2000028;
	control.checkPointCopy.ThisTimeLineID = control.checkPointCopy.PrevTimeLineID = 1;
	control.checkPointCopy.nextXid = FullTransactionIdFromU64(750);
	control.checkPointCopy.nextOid = FirstNormalObjectId;
	control.checkPointCopy.oldestXid = 3;
	control.checkPointCopy.oldestXidDB = 1;
	control.checkPointCopy.nextMulti = control.checkPointCopy.oldestMulti = 1;
	control.checkPointCopy.oldestMultiDB = 1;
	control.blcksz = BLCKSZ;
	crc_control();
	memset(input.identity.authority_uuid, 0xab, 16);
	memset(input.identity.storage_uuid, 0xcd, 16);
	input.identity.system_identifier = control.system_identifier;
	input.identity.database_incarnation = 1;
	input.native_control = &control;
	input.native_control_length = sizeof(control);
	input.native_clog = clog;
	input.native_clog_length = sizeof(clog);
	/* xid 3 committed, xid 4 aborted; preserve every original byte. */
	clog[0] = 1 << 6;
	clog[1] = 2;
}
static void
reject(void)
{
	uint8 saved[sizeof(output)];
	memset(output, 0xa5, sizeof(output));
	memcpy(saved, output, sizeof(output));
	UT_ASSERT_EQ(cluster_catalog_initial_image_size(CLUSTER_CATALOG_INITIAL_OID, &input), 0);
	UT_ASSERT(!cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input, output,
											 sizeof(ClusterOidAuthorityHeader)));
	UT_ASSERT(memcmp(output, saved, sizeof(output)) == 0);
}

UT_TEST(original_oid_consumer)
{
	ClusterOidAuthorityHeader h;
	reset();
	UT_ASSERT_EQ(cluster_catalog_initial_image_size(CLUSTER_CATALOG_INITIAL_OID, &input),
				 sizeof(h));
	UT_ASSERT(
		cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input, output, sizeof(h)));
	UT_ASSERT_EQ(cluster_oid_authority_classify((char *)output, sizeof(h)),
				 CLUSTER_OID_AUTHORITY_VALID);
	memcpy(&h, output, sizeof(h));
	UT_ASSERT_EQ(h.next_oid, control.checkPointCopy.nextOid);
}
UT_TEST(original_marker)
{
	ClusterCatalogAuthorityMarker h;
	pg_crc32c crc;
	reset();
	UT_ASSERT(
		cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_MARKER, &input, output, sizeof(h)));
	memcpy(&h, output, sizeof(h));
	UT_ASSERT_EQ(h.magic, CLUSTER_CATALOG_AUTHORITY_MAGIC);
	UT_ASSERT_EQ(h.version, CLUSTER_CATALOG_AUTHORITY_VERSION);
	UT_ASSERT_EQ(h.system_identifier, input.identity.system_identifier);
	UT_ASSERT_EQ(h.catalog_version_no, CATALOG_VERSION_NO);
	UT_ASSERT_EQ(h.flags, 0);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &h, offsetof(ClusterCatalogAuthorityMarker, crc));
	FIN_CRC32C(crc);
	UT_ASSERT_EQ(h.crc, crc);
}
UT_TEST(native_bootstrap_oid_floor)
{
	ClusterOidAuthorityHeader h;
	reset();
	control.checkPointCopy.nextOid = FirstUnpinnedObjectId + 1253;
	crc_control();
	UT_ASSERT(
		cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input, output, sizeof(h)));
	memcpy(&h, output, sizeof(h));
	UT_ASSERT_EQ(h.next_oid, FirstNormalObjectId);
}
UT_TEST(original_xid_consumer)
{
	ClusterXidAuthorityHeader h;
	reset();
	UT_ASSERT(
		cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_XID, &input, output, sizeof(h)));
	UT_ASSERT_EQ(cluster_xid_authority_classify((char *)output, sizeof(h)),
				 CLUSTER_XID_AUTHORITY_VALID);
	memcpy(&h, output, sizeof(h));
	UT_ASSERT_EQ(h.native_hw_full, 750);
	UT_ASSERT_EQ(h.next_multi, 1);
	UT_ASSERT_EQ(h.flags,
				 CLUSTER_XID_AUTHORITY_FLAG_SEALED | CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA);
}
UT_TEST(original_prehistory_consumer)
{
	reset();
	UT_ASSERT_EQ(cluster_catalog_initial_image_size(CLUSTER_CATALOG_INITIAL_PREHISTORY, &input),
				 sizeof(output));
	UT_ASSERT(cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_PREHISTORY, &input, output,
											sizeof(output)));
	UT_ASSERT_EQ(cluster_xid_prehistory_classify((char *)output, sizeof(output)),
				 CLUSTER_XID_AUTHORITY_VALID);
	UT_ASSERT(memcmp(output + sizeof(ClusterXidPrehistoryHeader), clog, sizeof(clog)) == 0);
}
UT_TEST(namespace_and_control_integrity)
{
	reset();
	input.identity.system_identifier++;
	reject();
	reset();
	memset(input.identity.authority_uuid, 0, 16);
	reject();
	reset();
	memset(input.identity.storage_uuid, 0, 16);
	reject();
	reset();
	input.identity.database_incarnation = 0;
	reject();
	reset();
	control.crc ^= 1;
	reject();
	reset();
	control.pg_control_version++;
	crc_control();
	reject();
	reset();
	control.catalog_version_no--;
	crc_control();
	reject();
	reset();
	input.native_control_length--;
	reject();
	reset();
	input.native_control_length++;
	reject();
}
UT_TEST(noninitial_control_is_refused)
{
	reset();
	control.state = DB_IN_PRODUCTION;
	crc_control();
	reject();
	reset();
	control.checkPointCopy.redo++;
	crc_control();
	reject();
	reset();
	control.checkPointCopy.ThisTimeLineID = 2;
	crc_control();
	reject();
	reset();
	control.minRecoveryPoint = 1;
	crc_control();
	reject();
	reset();
	control.backupEndRequired = true;
	crc_control();
	reject();
	reset();
	control.max_prepared_xacts = 1;
	crc_control();
	reject();
	reset();
	control.track_commit_timestamp = true;
	crc_control();
	reject();
}
UT_TEST(native_allocator_bounds)
{
	reset();
	control.checkPointCopy.nextOid = FirstGenbkiObjectId - 1;
	crc_control();
	reject();
	reset();
	control.checkPointCopy.nextXid = FullTransactionIdFromU64(2);
	crc_control();
	reject();
	reset();
	control.checkPointCopy.nextXid = FullTransactionIdFromU64(UINT64CONST(4294967300));
	crc_control();
	reject();
	reset();
	control.checkPointCopy.nextXid = FullTransactionIdFromU64(CLUSTER_XID_PREHISTORY_MAX_XID + 1);
	crc_control();
	reject();
	reset();
	control.checkPointCopy.oldestXid = 751;
	crc_control();
	reject();
	reset();
	control.checkPointCopy.nextMulti = 2;
	crc_control();
	reject();
	reset();
	control.checkPointCopy.nextMultiOffset = 1;
	crc_control();
	reject();
}
UT_TEST(clog_range_and_unresolved_child)
{
	reset();
	input.native_clog_length--;
	reject();
	reset();
	input.native_clog_length++;
	reject();
	reset();
	clog[1] = 3;
	reject();
	reset();
	input.native_clog = NULL;
	reject();
}
UT_TEST(output_and_alias_are_never_overwritten)
{
	uint8 saved[sizeof(output)];
	ClusterCatalogInitialInput saved_input;
	ControlFileData saved_control;
	reset();
	output[0] = 1;
	memcpy(saved, output, sizeof(output));
	UT_ASSERT(!cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input, output,
											 sizeof(ClusterOidAuthorityHeader)));
	UT_ASSERT(memcmp(output, saved, sizeof(output)) == 0);
	reset();
	saved_input = input;
	saved_control = control;
	UT_ASSERT(!cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input, &control,
											 sizeof(ClusterOidAuthorityHeader)));
	UT_ASSERT(memcmp(&control, &saved_control, sizeof(control)) == 0);
	UT_ASSERT(!cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input, &input,
											 sizeof(ClusterOidAuthorityHeader)));
	UT_ASSERT(memcmp(&input, &saved_input, sizeof(input)) == 0);
	UT_ASSERT(!cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input, clog + 4,
											 sizeof(ClusterOidAuthorityHeader)));
	UT_ASSERT_EQ(clog[4], 0);
}
UT_TEST(invalid_kind_length_and_null)
{
	reset();
	UT_ASSERT_EQ(cluster_catalog_initial_image_size(99, &input), 0);
	UT_ASSERT_EQ(cluster_catalog_initial_image_size(CLUSTER_CATALOG_INITIAL_OID, NULL), 0);
	UT_ASSERT(
		!cluster_catalog_initial_image(99, &input, output, sizeof(ClusterOidAuthorityHeader)));
	UT_ASSERT(!cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input, output,
											 sizeof(ClusterOidAuthorityHeader) - 1));
	UT_ASSERT(!cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input, NULL,
											 sizeof(ClusterOidAuthorityHeader)));
	input.native_control = NULL;
	reject();
}
UT_TEST(real_native_files)
{
	char path[MAXPGPATH];
	FILE *file;

	reset();
	snprintf(path, sizeof(path), "%s/global/pg_control", native_directory);
	file = fopen(path, "rb");
	UT_ASSERT(file != NULL);
	if (file == NULL)
		return;
	UT_ASSERT(fread(&control, 1, sizeof(control), file) == sizeof(control));
	UT_ASSERT(fclose(file) == 0);
	input.identity.system_identifier = control.system_identifier;
	snprintf(path, sizeof(path), "%s/pg_xact/0000", native_directory);
	file = fopen(path, "rb");
	UT_ASSERT(file != NULL);
	if (file == NULL)
		return;
	UT_ASSERT(fread(clog, 1, sizeof(clog), file) == sizeof(clog));
	UT_ASSERT(fclose(file) == 0);
	for (int kind = CLUSTER_CATALOG_INITIAL_OID; kind <= CLUSTER_CATALOG_INITIAL_PREHISTORY;
		 ++kind) {
		size_t length = cluster_catalog_initial_image_size(kind, &input);
		UT_ASSERT(length > 0 && length <= sizeof(output));
		memset(output, 0, sizeof(output));
		UT_ASSERT(cluster_catalog_initial_image(kind, &input, output, length));
		if (kind == CLUSTER_CATALOG_INITIAL_OID)
			UT_ASSERT_EQ(cluster_oid_authority_classify((char *)output, length),
						 CLUSTER_OID_AUTHORITY_VALID);
		if (kind == CLUSTER_CATALOG_INITIAL_XID)
			UT_ASSERT_EQ(cluster_xid_authority_classify((char *)output, length),
						 CLUSTER_XID_AUTHORITY_VALID);
		if (kind == CLUSTER_CATALOG_INITIAL_PREHISTORY)
			UT_ASSERT_EQ(cluster_xid_prehistory_classify((char *)output, length),
						 CLUSTER_XID_AUTHORITY_VALID);
	}
}

int
main(int argc, char **argv)
{
	if (argc == 3 && strcmp(argv[1], "--native") == 0) {
		native_directory = argv[2];
		UT_PLAN(1);
		UT_RUN(real_native_files);
		UT_DONE();
		return ut_failed_count ? 1 : 0;
	}
	if (argc != 1)
		return 2;
	UT_PLAN(11);
	UT_RUN(original_oid_consumer);
	UT_RUN(native_bootstrap_oid_floor);
	UT_RUN(original_marker);
	UT_RUN(original_xid_consumer);
	UT_RUN(original_prehistory_consumer);
	UT_RUN(namespace_and_control_integrity);
	UT_RUN(noninitial_control_is_refused);
	UT_RUN(native_allocator_bounds);
	UT_RUN(clog_range_and_unresolved_child);
	UT_RUN(output_and_alias_are_never_overwritten);
	UT_RUN(invalid_kind_length_and_null);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
