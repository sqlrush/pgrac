/*-------------------------------------------------------------------------
 *
 * test_cluster_space_recovery_route.c
 *    Exercise the actual merged-recovery classifier for SPACE WAL.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_recovery_route.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogreader.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/storage/cluster_smgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static bool shared;
static unsigned routes;
static RelFileLocator locator = { 1663, 5, 16384 };

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

int
cluster_smgr_which_for(RelFileLocator tag, BackendId backend)
{
	if (!RelFileLocatorEquals(tag, locator) || backend != InvalidBackendId)
		abort();
	routes++;
	return shared ? 1 : 0;
}

bool
XLogRecGetBlockTagExtended(XLogReaderState *record, uint8 id, RelFileLocator *tag,
						   ForkNumber *forknum, BlockNumber *block, Buffer *prefetch)
{
	(void)record;
	(void)id;
	(void)forknum;
	(void)block;
	(void)prefetch;
	*tag = locator;
	return true;
}

/* Generated directly from the named production function, not a copy of its
 * classification logic. Only the external storage-routing input is faked. */
#include "test_cluster_space_recovery_route.inc"

static void
record_init(XLogReaderState *reader, DecodedXLogRecord *decoded, uint8 *bytes)
{
	ClusterSpaceWalChange change;
	ClusterSpaceStructureChange pair = {0};

	memset(&change, 0, sizeof(change));
	change.action = CLUSTER_SPACE_WAL_CREATE;
	change.nblocks = InvalidBlockNumber;
	change.result_token = 17;
	change.result.key.system_identifier = 1;
	change.result.key.database_incarnation = 2;
	change.result.key.storage_uuid[0] = 3;
	change.result.key.locator = locator;
	change.result.incarnation[0] = 4;
	change.result.sequence = 1;
	change.result.operation = 17;
	change.result.state = CLUSTER_SPACE_IDENTITY_LIVE;
	pair.identity = change;
	pair.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
	pair.reservation.result.identity = change.result;
	pair.reservation.result_token = change.result_token;
	if (!cluster_space_structure_wal_encode(&pair, bytes, CLUSTER_SPACE_STRUCTURE_WAL_BYTES))
		abort();
	memset(reader, 0, sizeof(*reader));
	memset(decoded, 0, sizeof(*decoded));
	decoded->header.xl_rmid = RM_SMGR_ID;
	decoded->header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
	decoded->max_block_id = -1;
	decoded->main_data = (char *)bytes;
	decoded->main_data_len = CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
	reader->record = decoded;
	shared = true;
	routes = 0;
}

UT_TEST(test_typed_locator_routes_foreign_wal)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	uint8 bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	record_init(&reader, &decoded, bytes);
	UT_ASSERT_EQ(cluster_record_apply_class(&reader), CLUSTER_RECMERGE_SHARED);
	UT_ASSERT_EQ(routes, 1);
	shared = false;
	UT_ASSERT_EQ(cluster_record_apply_class(&reader), CLUSTER_RECMERGE_LOCAL);
	UT_ASSERT_EQ(routes, 2);
}

UT_TEST(test_invalid_typed_record_is_not_local_or_shared)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	uint8 bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	record_init(&reader, &decoded, bytes);
	decoded.main_data_len--;
	UT_ASSERT_EQ(cluster_record_apply_class(&reader), CLUSTER_RECMERGE_UNCLASSIFIABLE);
	decoded.main_data_len++;
	bytes[240] ^= 1;
	UT_ASSERT_EQ(cluster_record_apply_class(&reader), CLUSTER_RECMERGE_UNCLASSIFIABLE);
	UT_ASSERT_EQ(routes, 0);
	record_init(&reader, &decoded, bytes);
	decoded.main_data_len = CLUSTER_SPACE_WAL_BYTES;
	UT_ASSERT_EQ(cluster_record_apply_class(&reader), CLUSTER_RECMERGE_UNCLASSIFIABLE);
	UT_ASSERT_EQ(routes, 0);
	record_init(&reader, &decoded, bytes);
	bytes[CLUSTER_SPACE_WAL_BYTES + 208 + 16] ^= 1;
	UT_ASSERT_EQ(cluster_record_apply_class(&reader), CLUSTER_RECMERGE_UNCLASSIFIABLE);
	UT_ASSERT_EQ(routes, 0);
}

UT_TEST(test_block_reference_cannot_bypass_typed_validation)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	uint8 bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	record_init(&reader, &decoded, bytes);
	decoded.max_block_id = 0;
	UT_ASSERT_EQ(cluster_record_apply_class(&reader), CLUSTER_RECMERGE_UNCLASSIFIABLE);
	UT_ASSERT_EQ(routes, 0);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_typed_locator_routes_foreign_wal);
	UT_RUN(test_invalid_typed_record_is_not_local_or_shared);
	UT_RUN(test_block_reference_cannot_bypass_typed_validation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
