/*-------------------------------------------------------------------------
 *
 * test_cluster_space_page_verify.c
 *    Native page-read validation must not admit malformed SPACE metadata.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_page_verify.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_space_reservation.h"
#include "pgstat.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Native GUC storage is defined in bufpage.c, not exported by a header. */
extern bool ignore_checksum_failure;

static bool checksums;

bool
DataChecksumsEnabled(void)
{
	return checksums;
}

void
pgstat_report_checksum_failure(void)
{
	abort(); /* The test never requests statistics side effects. */
}

int
errcode(int sqlerrcode)
{
	(void)sqlerrcode;
	abort();
}

int
errmsg(const char *fmt, ...)
{
	(void)fmt;
	abort();
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

static void
make_page(PGAlignedBlock *page)
{
	ClusterSpaceIdentity id;

	memset(&id, 0, sizeof(id));
	id.key.system_identifier = 1;
	id.key.database_incarnation = 1;
	id.key.storage_uuid[0] = 1;
	id.key.locator.spcOid = DEFAULTTABLESPACE_OID;
	id.key.locator.dbOid = 5;
	id.key.locator.relNumber = 16384;
	id.incarnation[0] = 3;
	id.sequence = 1;
	id.operation = 4;
	id.state = CLUSTER_SPACE_IDENTITY_LIVE;
	if (!cluster_space_identity_page_encode(&id, 5, page->data, BLCKSZ))
		abort();
}

UT_TEST(test_native_read_checks_payload_even_without_checksums)
{
	PGAlignedBlock page;

	checksums = false;
	make_page(&page);
	UT_ASSERT(PageIsVerifiedExtended(page.data, 0, 0));
	page.data[32 + 56] ^= 1;
	UT_ASSERT(!PageIsVerifiedExtended(page.data, 0, 0));
}

UT_TEST(test_ignore_checksum_failure_cannot_bypass_structure)
{
	PGAlignedBlock page;

	checksums = true;
	ignore_checksum_failure = true;
	make_page(&page);
	((PageHeader)page.data)->pd_checksum = pg_checksum_page(page.data, 0);
	UT_ASSERT(PageIsVerifiedExtended(page.data, 0, 0));
	page.data[160] = 1;
	UT_ASSERT(!PageIsVerifiedExtended(page.data, 0, 0));
	ignore_checksum_failure = false;
}

UT_TEST(test_native_page_stays_native)
{
	PGAlignedBlock page;

	checksums = false;
	memset(&page, 0, sizeof(page));
	UT_ASSERT(PageIsVerifiedExtended(page.data, 0, 0));
	PageInit(page.data, BLCKSZ, 0);
	UT_ASSERT(PageIsVerifiedExtended(page.data, 0, 0));
	((PageHeader)page.data)->pd_flags |= PD_LSN_ORIGIN_VALID;
	UT_ASSERT(PageIsVerifiedExtended(page.data, 0, 0));
}

UT_TEST(test_fork_bound_read_rejects_swapped_page_types)
{
	PGAlignedBlock page;

	checksums = false;
	make_page(&page);
	UT_ASSERT(PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 0, 0));
	UT_ASSERT(!PageIsVerifiedForFork(page.data, MAIN_FORKNUM, 0, 0));
	UT_ASSERT(!PageIsVerifiedForFork(page.data, VISIBILITYMAP_FORKNUM, 0, 0));
	UT_ASSERT(!PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 1, 0));
	PageInit(page.data, BLCKSZ, 0);
	UT_ASSERT(!PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 0, 0));
	UT_ASSERT(PageIsVerifiedForFork(page.data, MAIN_FORKNUM, 0, 0));
	memset(&page, 0, sizeof(page));
	/* Native unformatted extension is not an identity; typed decode refuses it. */
	UT_ASSERT(PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 0, 0));
	UT_ASSERT(!cluster_space_identity_page_valid(page.data, BLCKSZ));
	UT_ASSERT(PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 1, 0));
	UT_ASSERT(!cluster_space_reservation_page_valid(page.data, BLCKSZ));
	UT_ASSERT(!PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 2, 0));
	UT_ASSERT(!PageIsVerifiedForFork(page.data, InvalidForkNumber, 0, 0));
	UT_ASSERT(!PageIsVerifiedForFork(page.data, MAX_FORKNUM + 1, 0, 0));
}

UT_TEST(test_reservation_page_has_its_own_block_and_integrity)
{
	PGAlignedBlock page;
	ClusterSpaceReservation reservation = {0};
	ClusterSpaceIdentityKey key = {0};
	uint64 token;

	checksums = false;
	make_page(&page);
	key.system_identifier = key.database_incarnation = 1;
	key.storage_uuid[0] = 1;
	key.locator = (RelFileLocator){DEFAULTTABLESPACE_OID, 5, 16384};
	UT_ASSERT(cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0,
		&key, &reservation.identity, &token));
	reservation.next_block = 11;
	UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 9, page.data, BLCKSZ));
	UT_ASSERT(PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 1, 0));
	UT_ASSERT(!PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 0, 0));
	UT_ASSERT(!PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 2, 0));
	UT_ASSERT(!PageIsVerifiedForFork(page.data, MAIN_FORKNUM, 1, 0));
	checksums = true;
	ignore_checksum_failure = true;
	((PageHeader)page.data)->pd_checksum = pg_checksum_page(page.data, 1);
	UT_ASSERT(PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 1, 0));
	page.data[32 + 16] ^= 1;
	UT_ASSERT(!PageIsVerifiedForFork(page.data, SPACE_FORKNUM, 1, 0));
	ignore_checksum_failure = false;
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_native_read_checks_payload_even_without_checksums);
	UT_RUN(test_ignore_checksum_failure_cannot_bypass_structure);
	UT_RUN(test_native_page_stays_native);
	UT_RUN(test_fork_bound_read_rejects_swapped_page_types);
	UT_RUN(test_reservation_page_has_its_own_block_and_integrity);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
