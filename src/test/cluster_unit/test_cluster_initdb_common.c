/* Original common representation: real CRC/hash/codecs, no publication stub.
 * Actual native input/control reading is exercised by initdb TAP 006.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "catalog/catversion.h"
#include "cluster/cluster_cf_authority.h"
#include "storage/bufpage.h"
#include "unit_test.h"
#include "../../backend/cluster/cluster_initdb_common_private.h"
UT_DEFINE_GLOBALS();

static ControlFileData controls[4];
static const ControlFileData *sources[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
static ClusterSharedConfigRef config;
static ClusterInitdbCommon output;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

static void
crc(ControlFileData *cf)
{
	INIT_CRC32C(cf->crc);
	COMP_CRC32C(cf->crc, cf, offsetof(ControlFileData, crc));
	FIN_CRC32C(cf->crc);
}

static void
prepare(void)
{
	memset(controls, 0, sizeof(controls));
	memset(sources, 0, sizeof(sources));
	memset(&config, 0, sizeof(config));
	config.identity.system_identifier = UINT64CONST(7584383251700000001);
	config.identity.database_incarnation = config.identity.generation = 1;
	config.identity.configured[0] = 15;
	memset(config.identity.authority_uuid, 0x23, 16);
	memset(config.identity.storage_uuid, 0x45, 16);
	controls[0].system_identifier = config.identity.system_identifier;
	controls[0].pg_control_version = PG_CONTROL_VERSION;
	controls[0].catalog_version_no = CATALOG_VERSION_NO;
	controls[0].state = DB_SHUTDOWNED;
	controls[0].time = 100;
	controls[0].checkPoint = controls[0].checkPointCopy.redo = 0x2000028;
	controls[0].checkPointCopy.ThisTimeLineID = controls[0].checkPointCopy.PrevTimeLineID = 1;
	controls[0].checkPointCopy.nextXid = FullTransactionIdFromU64(750);
	controls[0].checkPointCopy.oldestXid = 3;
	controls[0].checkPointCopy.oldestXidDB = 1;
	controls[0].checkPointCopy.nextOid = 16000;
	controls[0].checkPointCopy.nextMulti = controls[0].checkPointCopy.oldestMulti = 1;
	controls[0].checkPointCopy.oldestMultiDB = 1;
	controls[0].MaxConnections = 100;
	controls[0].max_locks_per_xact = 64;
	controls[0].blcksz = BLCKSZ;
	controls[0].data_checksum_version = PG_DATA_CHECKSUM_VERSION;
	memset(controls[0].mock_authentication_nonce, 'a', MOCK_AUTH_NONCE_LEN);
	for (int i = 0; i < 4; i++) {
		controls[i] = controls[0];
		controls[i].checkPoint += i * 8192;
		controls[i].checkPointCopy.redo = controls[i].checkPoint;
		controls[i].time += i;
		controls[i].checkPointCopy.time = controls[i].time;
		memset(controls[i].mock_authentication_nonce, 'a' + i, MOCK_AUTH_NONCE_LEN);
		crc(&controls[i]);
		sources[i] = &controls[i];
	}
}

static void
refusal(void)
{
	ClusterInitdbCommon zero = { 0 };
	memset(&output, 0xa5, sizeof(output));
	UT_ASSERT(!cluster_initdb_common_build(&config, sources, &output));
	UT_ASSERT(memcmp(&output, &zero, sizeof(output)) == 0);
}

UT_TEST(complete_cohort)
{
	ControlFileData decoded;
	uint8 expected[PG_CONTROL_FILE_SIZE];
	prepare();
	UT_ASSERT(cluster_initdb_common_build(&config, sources, &output));
	memcpy(&decoded, output.control, sizeof(decoded));
	UT_ASSERT(decoded.checkPoint == controls[0].checkPoint);
	UT_ASSERT(FullTransactionIdEquals(decoded.checkPointCopy.nextXid,
									  controls[0].checkPointCopy.nextXid));
	UT_ASSERT(memcmp(decoded.mock_authentication_nonce, controls[0].mock_authentication_nonce,
					 MOCK_AUTH_NONCE_LEN)
			  == 0);
	UT_ASSERT(cluster_cf_control_image_encode(&controls[0], expected)
			  == CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(expected, output.control, sizeof(expected)) == 0);
	UT_ASSERT(strstr((char *)output.catalog, "\"entries\":[],\"generation\":\"1\",\"version\":1}\n")
			  != NULL);
	UT_ASSERT(output.catalog_length == strlen((char *)output.catalog));
}
UT_TEST(missing_or_extra_origin)
{
	prepare();
	sources[3] = NULL;
	refusal();
	prepare();
	sources[127] = sources[3];
	refusal();
}
UT_TEST(common_xid_never_uses_max)
{
	prepare();
	controls[3].checkPointCopy.nextXid = FullTransactionIdFromU64(900);
	crc(&controls[3]);
	refusal();
}
UT_TEST(common_oid_and_mx_never_use_max)
{
	prepare();
	controls[2].checkPointCopy.nextOid++;
	crc(&controls[2]);
	refusal();
	prepare();
	controls[2].checkPointCopy.nextMulti++;
	crc(&controls[2]);
	refusal();
	prepare();
	controls[2].checkPointCopy.nextMultiOffset++;
	crc(&controls[2]);
	refusal();
}
UT_TEST(common_frozen_horizon_must_agree)
{
	prepare();
	controls[1].checkPointCopy.oldestXidDB++;
	crc(&controls[1]);
	refusal();
	prepare();
	controls[1].checkPointCopy.oldestMulti++;
	crc(&controls[1]);
	refusal();
}
UT_TEST(configuration_and_layout_must_agree)
{
	prepare();
	controls[1].MaxConnections++;
	crc(&controls[1]);
	refusal();
	prepare();
	controls[1].blcksz *= 2;
	crc(&controls[1]);
	refusal();
}
UT_TEST(foreign_or_corrupt_input)
{
	prepare();
	controls[1].system_identifier++;
	crc(&controls[1]);
	refusal();
	prepare();
	controls[1].crc++;
	refusal();
}
UT_TEST(noninitial_inputs)
{
	prepare();
	controls[1].state = DB_IN_PRODUCTION;
	crc(&controls[1]);
	refusal();
	prepare();
	controls[1].backupStartPoint = 123;
	crc(&controls[1]);
	refusal();
	prepare();
	controls[1].minRecoveryPoint = 123;
	crc(&controls[1]);
	refusal();
	prepare();
	controls[1].max_prepared_xacts = 1;
	crc(&controls[1]);
	refusal();
}
UT_TEST(namespace_and_generation)
{
	prepare();
	config.identity.generation = 2;
	refusal();
	prepare();
	memset(config.identity.authority_uuid, 0, 16);
	refusal();
	prepare();
	config.identity.configured[0] &= ~UINT64CONST(1);
	refusal();
}
UT_TEST(overlapping_output_is_unchanged)
{
	ClusterInitdbCommon saved;
	prepare();
	memset(&output, 0xa5, sizeof(output));
	saved = output;
	sources[0] = (ControlFileData *)output.control;
	UT_ASSERT(!cluster_initdb_common_build(&config, sources, &output));
	UT_ASSERT(memcmp(&output, &saved, sizeof(output)) == 0);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(complete_cohort);
	UT_RUN(missing_or_extra_origin);
	UT_RUN(common_xid_never_uses_max);
	UT_RUN(common_oid_and_mx_never_use_max);
	UT_RUN(common_frozen_horizon_must_agree);
	UT_RUN(configuration_and_layout_must_agree);
	UT_RUN(foreign_or_corrupt_input);
	UT_RUN(noninitial_inputs);
	UT_RUN(namespace_and_generation);
	UT_RUN(overlapping_output_is_unchanged);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
