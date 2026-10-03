/* Verify shared catalog startup with actual codecs and the original entry body.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <setjmp.h>
#include "access/transam.h"
#include "access/xlog.h"
#include "catalog/catversion.h"
#include "cluster/cluster_catalog_bootstrap.h"
#include "cluster/cluster_catalog_migrate.h"
#include "cluster/cluster_catalog_startup.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_oid_lease.h"
#include "cluster/cluster_xid_authority.h"
#include "miscadmin.h"
#include "unit_test.h"
#include "../../backend/cluster/cluster_initdb_common_private.h"

UT_DEFINE_GLOBALS();
bool IsUnderPostmaster;
bool cluster_shared_catalog = true;
bool cluster_shared_config = true;
int cluster_sinval_ack_mode = CLUSTER_SINVAL_ACK_MODE_PEER_ENQUEUED;
int wal_level = WAL_LEVEL_REPLICA;
char *DataDir = "/not-used";
char *cluster_shared_data_dir = "/not-used";
static ControlFileData control;
static uint8 clog[BLCKSZ];
static ClusterOidAuthorityHeader oid_image;
static ClusterCatalogAuthorityMarker marker;
static ClusterXidAuthorityHeader xid_image, xid_backup;
static uint8 prehistory[BLCKSZ + sizeof(ClusterXidPrehistoryHeader)];
static ClusterInitdbCommon publication;
static ClusterCatalogStartupInput input;
static bool readable, current;
static int reads, currents, releases, legacy_reads, legacy_writes, off_checks, fatals;
static sigjmp_buf fatal_jump;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}
bool
cluster_cf_authority_read(ControlFileData *out)
{
	legacy_reads++;
	*out = control;
	return true;
}
bool
cluster_oid_authority_read(Oid *out)
{
	legacy_reads++;
	*out = FirstNormalObjectId;
	return true;
}
bool
cluster_oid_authority_present(void)
{
	legacy_reads++;
	return false;
}
bool
cluster_oid_authority_seed_if_absent(Oid initial)
{
	legacy_writes++;
	return false;
}
ClusterCatalogMigrateResult
cluster_catalog_migrate_tree(const char *dir, uint64 sysid)
{
	legacy_writes++;
	return CLUSTER_CATALOG_MIGRATE_ADOPTED;
}
void
cluster_catalog_vet_off_mode(void)
{
	off_checks++;
}
static void
cluster_catalog_vet_xid_striping_for_shared_catalog(void)
{}
static void
cluster_catalog_prepare_xid_authority(const ControlFileData *cf, ClusterCatalogMigrateResult result)
{
	legacy_writes++;
}

#undef ereport
#define ereport(level, rest)                                                                       \
	do {                                                                                           \
		fatals++;                                                                                  \
		siglongjmp(fatal_jump, 1);                                                                 \
	} while (0)
#undef elog
#define elog(...) ((void)0)
#include "test_cluster_catalog_startup_entry.inc"

static bool
read_input(ClusterCatalogStartupInput *out, void *arg)
{
	reads++;
	*out = input;
	return readable;
}
static bool
current_input(void *arg)
{
	currents++;
	return current;
}
static void
release_input(void *arg)
{
	releases++;
}
static const ClusterCatalogStartupSource source
	= { read_input, current_input, release_input, NULL };

static void
reset(void)
{
	ClusterSharedConfigRef config = { 0 };
	const ControlFileData *sources[CLUSTER_CONTROL_ROOT_RECORD_COUNT] = { 0 };
	memset(&control, 0, sizeof(control));
	memset(clog, 0, sizeof(clog));
	memset(&input, 0, sizeof(input));
	memset(&oid_image, 0, sizeof(oid_image));
	memset(&marker, 0, sizeof(marker));
	memset(&xid_image, 0, sizeof(xid_image));
	memset(prehistory, 0, sizeof(prehistory));
	control.system_identifier = UINT64CONST(7584383251700000001);
	control.pg_control_version = PG_CONTROL_VERSION;
	control.catalog_version_no = CATALOG_VERSION_NO;
	control.state = DB_SHUTDOWNED;
	control.checkPoint = control.checkPointCopy.redo = 0x2000028;
	control.checkPointCopy.ThisTimeLineID = control.checkPointCopy.PrevTimeLineID = 1;
	control.checkPointCopy.nextXid = FullTransactionIdFromU64(750);
	control.checkPointCopy.nextOid = 13253;
	control.checkPointCopy.oldestXid = 3;
	control.checkPointCopy.oldestXidDB = 1;
	control.checkPointCopy.nextMulti = control.checkPointCopy.oldestMulti = 1;
	control.checkPointCopy.oldestMultiDB = 1;
	control.blcksz = BLCKSZ;
	INIT_CRC32C(control.crc);
	COMP_CRC32C(control.crc, &control, offsetof(ControlFileData, crc));
	FIN_CRC32C(control.crc);
	config.identity.system_identifier = control.system_identifier;
	config.identity.database_incarnation = config.identity.generation = 1;
	config.identity.configured[0] = 1;
	memset(config.identity.authority_uuid, 0xab, 16);
	memset(config.identity.storage_uuid, 0xcd, 16);
	sources[0] = &control;
	UT_ASSERT(cluster_initdb_common_build(&config, sources, &publication));
	memcpy(input.original.identity.authority_uuid, config.identity.authority_uuid, 16);
	memcpy(input.original.identity.storage_uuid, config.identity.storage_uuid, 16);
	input.original.identity.system_identifier = control.system_identifier;
	input.original.identity.database_incarnation = 1;
	input.original.native_control = &control;
	input.original.native_control_length = sizeof(control);
	input.original.native_clog = clog;
	input.original.native_clog_length = sizeof(clog);
	input.manifest = (ClusterCatalogInputImage){ publication.catalog, publication.catalog_length };
	input.catalog_generation = 1;
	memcpy(input.catalog_sha256, publication.catalog_sha256, 32);
	UT_ASSERT(cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_OID, &input.original,
											&oid_image, sizeof(oid_image)));
	UT_ASSERT(cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_MARKER, &input.original,
											&marker, sizeof(marker)));
	UT_ASSERT(cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_XID, &input.original,
											&xid_image, sizeof(xid_image)));
	UT_ASSERT(cluster_catalog_initial_image(CLUSTER_CATALOG_INITIAL_PREHISTORY, &input.original,
											prehistory, sizeof(prehistory)));
	xid_backup = xid_image;
	input.oid = (ClusterCatalogInputImage){ &oid_image, sizeof(oid_image) };
	input.marker = (ClusterCatalogInputImage){ &marker, sizeof(marker) };
	input.xid = (ClusterCatalogInputImage){ &xid_image, sizeof(xid_image) };
	input.xid_backup = (ClusterCatalogInputImage){ &xid_backup, sizeof(xid_backup) };
	input.prehistory = (ClusterCatalogInputImage){ prehistory, sizeof(prehistory) };
	readable = current = true;
	reads = currents = releases = legacy_reads = legacy_writes = off_checks = fatals = 0;
	IsUnderPostmaster = false;
	cluster_shared_config = cluster_shared_catalog = true;
	cluster_sinval_ack_mode = CLUSTER_SINVAL_ACK_MODE_PEER_ENQUEUED;
	wal_level = WAL_LEVEL_REPLICA;
	UT_ASSERT(cluster_catalog_startup_set_source(&source));
}
static bool
startup(void)
{
	if (sigsetjmp(fatal_jump, 0) == 0) {
		cluster_catalog_startup_prepare();
		return true;
	}
	return false;
}
static void
no_legacy(void)
{
	UT_ASSERT_EQ(legacy_reads, 0);
	UT_ASSERT_EQ(legacy_writes, 0);
}
static void
oid_crc(void)
{
	INIT_CRC32C(oid_image.crc);
	COMP_CRC32C(oid_image.crc, &oid_image, offsetof(ClusterOidAuthorityHeader, crc));
	FIN_CRC32C(oid_image.crc);
}
static void
xid_crc(ClusterXidAuthorityHeader *h)
{
	INIT_CRC32C(h->crc);
	COMP_CRC32C(h->crc, h, offsetof(ClusterXidAuthorityHeader, crc));
	FIN_CRC32C(h->crc);
}

UT_TEST(original_images_pass)
{
	reset();
	UT_ASSERT(cluster_catalog_startup_validate(&input));
}
UT_TEST(manifest_identity_generation_hash)
{
	reset();
	input.original.identity.storage_uuid[0] ^= 1;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	input.catalog_generation++;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	input.catalog_sha256[0] ^= 1;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
}
UT_TEST(missing_and_short_images)
{
	reset();
	input.oid.bytes = NULL;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	input.marker.length--;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	input.xid.length++;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	input.xid_backup.bytes = NULL;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	input.prehistory.length--;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
}
UT_TEST(corrupt_original_inputs_and_objects)
{
	reset();
	control.crc ^= 1;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	oid_image.crc ^= 1;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	marker.system_identifier++;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	xid_image.crc ^= 1;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	prehistory[sizeof(ClusterXidPrehistoryHeader)] ^= 1;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
}
UT_TEST(oid_advance_and_wrap_are_not_reseeded)
{
	reset();
	oid_image.next_oid += 1000;
	oid_crc();
	UT_ASSERT(cluster_catalog_startup_validate(&input));
	oid_image.next_oid = 0;
	oid_crc();
	UT_ASSERT(cluster_catalog_startup_validate(&input));
	oid_image.next_oid = 1;
	oid_crc();
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
}
UT_TEST(xid_native_identity_and_monotonic_flags)
{
	reset();
	xid_image.native_hw_full++;
	xid_crc(&xid_image);
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	xid_image.flags &= ~CLUSTER_XID_AUTHORITY_FLAG_CLUSTER_ERA;
	xid_crc(&xid_image);
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	xid_image.flags |= 16;
	xid_crc(&xid_image);
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	xid_image.flags |= CLUSTER_XID_AUTHORITY_FLAG_EPOCH_GATE_ADMITTED;
	xid_crc(&xid_image);
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	reset();
	xid_image.flags |= CLUSTER_XID_AUTHORITY_FLAG_NATIVE_RAW_REUSED;
	xid_image.magic = CLUSTER_XID_AUTHORITY_MAGIC_RAW_REUSED;
	xid_crc(&xid_image);
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	xid_backup = xid_image;
	UT_ASSERT(cluster_catalog_startup_validate(&input));
	xid_image.flags |= CLUSTER_XID_AUTHORITY_FLAG_EPOCH_GATE_ADMITTED;
	xid_crc(&xid_image);
	xid_backup = xid_image;
	UT_ASSERT(cluster_catalog_startup_validate(&input));
}
UT_TEST(pure_validation_is_readonly)
{
	uint8 before[sizeof(prehistory)];
	ClusterOidAuthorityHeader oid_before;
	reset();
	memcpy(before, prehistory, sizeof(before));
	oid_before = oid_image;
	UT_ASSERT(cluster_catalog_startup_validate(&input));
	input.catalog_generation++;
	UT_ASSERT(!cluster_catalog_startup_validate(&input));
	UT_ASSERT(memcmp(before, prehistory, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&oid_before, &oid_image, sizeof(oid_image)) == 0);
}
UT_TEST(repeated_startup_only_verifies)
{
	reset();
	UT_ASSERT(startup());
	UT_ASSERT(startup());
	no_legacy();
	UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(currents, 2);
	UT_ASSERT_EQ(releases, 2);
}
UT_TEST(unregistered_source_is_fatal)
{
	reset();
	cluster_catalog_startup_set_source(NULL);
	UT_ASSERT(!startup());
	no_legacy();
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(fatals, 1);
}
UT_TEST(read_failure_releases_and_refuses)
{
	reset();
	readable = false;
	UT_ASSERT(!startup());
	no_legacy();
	UT_ASSERT_EQ(currents, 0);
	UT_ASSERT_EQ(releases, 1);
}
UT_TEST(validation_failure_releases_and_refuses)
{
	reset();
	input.marker.length = 0;
	UT_ASSERT(!startup());
	no_legacy();
	UT_ASSERT_EQ(currents, 0);
	UT_ASSERT_EQ(releases, 1);
}
UT_TEST(root_change_after_read_is_fatal)
{
	reset();
	current = false;
	UT_ASSERT(!startup());
	no_legacy();
	UT_ASSERT_EQ(currents, 1);
	UT_ASSERT_EQ(releases, 1);
}
UT_TEST(no_cached_positive_receipt)
{
	reset();
	UT_ASSERT(startup());
	readable = false;
	UT_ASSERT(!startup());
	no_legacy();
	UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(releases, 2);
}
UT_TEST(invalid_registration_clears_old_source)
{
	ClusterCatalogStartupSource bad = source;
	reset();
	bad.current = NULL;
	UT_ASSERT(!cluster_catalog_startup_set_source(&bad));
	UT_ASSERT(!startup());
	no_legacy();
	UT_ASSERT_EQ(reads, 0);
}
UT_TEST(nonshared_off_path_unchanged)
{
	reset();
	cluster_shared_config = cluster_shared_catalog = false;
	UT_ASSERT(startup());
	UT_ASSERT_EQ(off_checks, 1);
	UT_ASSERT_EQ(reads, 0);
	no_legacy();
}
UT_TEST(catalog_only_and_unsafe_gucs_refuse)
{
	reset();
	cluster_shared_config = false;
	UT_ASSERT(!startup());
	no_legacy();
	reset();
	cluster_shared_catalog = false;
	UT_ASSERT(!startup());
	no_legacy();
	reset();
	wal_level = WAL_LEVEL_MINIMAL;
	UT_ASSERT(!startup());
	no_legacy();
	reset();
	cluster_sinval_ack_mode = CLUSTER_SINVAL_ACK_MODE_NONE;
	UT_ASSERT(!startup());
	no_legacy();
}
UT_TEST(child_cannot_register_or_bootstrap)
{
	reset();
	IsUnderPostmaster = true;
	UT_ASSERT(!cluster_catalog_startup_set_source(&source));
	UT_ASSERT(startup());
	no_legacy();
	UT_ASSERT_EQ(reads, 0);
}
int
main(void)
{
	UT_PLAN(17);
	UT_RUN(original_images_pass);
	UT_RUN(manifest_identity_generation_hash);
	UT_RUN(missing_and_short_images);
	UT_RUN(corrupt_original_inputs_and_objects);
	UT_RUN(oid_advance_and_wrap_are_not_reseeded);
	UT_RUN(xid_native_identity_and_monotonic_flags);
	UT_RUN(pure_validation_is_readonly);
	UT_RUN(repeated_startup_only_verifies);
	UT_RUN(unregistered_source_is_fatal);
	UT_RUN(read_failure_releases_and_refuses);
	UT_RUN(validation_failure_releases_and_refuses);
	UT_RUN(root_change_after_read_is_fatal);
	UT_RUN(no_cached_positive_receipt);
	UT_RUN(invalid_registration_clears_old_source);
	UT_RUN(nonshared_off_path_unchanged);
	UT_RUN(catalog_only_and_unsafe_gucs_refuse);
	UT_RUN(child_cannot_register_or_bootstrap);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
