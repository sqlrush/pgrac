/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_io.c
 *	  Read-only I/O of typed cold-crash replay: DATA observation (absence,
 *	  classification, SPACE identity overlay and its per-relation cache),
 *	  the SPACE owner's pass-1 check adapter, the pass-1 root scan's cut
 *	  proof and the pass-2 source reader.
 *
 *	  Storage, the SPACE codecs, the root visitor and the WAL reader are
 *	  table-driven boundaries (test_cluster_cold_recovery_io_boundary.h);
 *	  the plan is real.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_io.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "miscadmin.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_wal_restart_read.h"
#include "cluster/cluster_wal_tail.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#include "test_cluster_cold_recovery_io_boundary.h"

/* ---- helpers ---- */
static RfPageIdentityV1
page(RelFileNumber rel, BlockNumber block)
{
	RfPageIdentityV1 id;

	memset(&id, 0, sizeof(id));
	id.system_identifier = 99;
	memset(id.storage_uuid, 3, 16);
	id.locator.spcOid = 1663;
	id.locator.dbOid = 5;
	id.locator.relNumber = rel;
	id.forknum = MAIN_FORKNUM;
	id.blockno = block;
	return id;
}

static ClusterColdObserverV1
observer(void)
{
	ClusterColdObserverV1 o;

	memset(&o, 0, sizeof(o));
	o.system_identifier = 99;
	o.database_incarnation = 4;
	memset(o.storage_uuid, 3, 16);
	return o;
}

static bool
incarnation_is(const ClusterColdDataV1 *data, uint8 inc)
{
	uint8 expected[16];

	memset(expected, inc, 16);
	return memcmp(data->version.segment_incarnation, expected, 16) == 0;
}

/* A missing fork or block is absent; a block that is there is classified,
 * and only a verified checksum proves its content. */
UT_TEST(test_observe_absent_and_present)
{
	ClusterColdObserverV1 o = observer();
	RfPageIdentityV1 id = page(100, 0);
	ClusterColdDataV1 data;

	storage_reset();
	identity_decodes = 0;
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT_EQ(data.kind, CLUSTER_COLD_DATA_ABSENT);
	UT_ASSERT_EQ(data.flags, CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED);
	UT_ASSERT_EQ(identity_decodes, 0);
	forks[0].exists = true;
	forks[0].nblocks = 1;
	id = page(100, 1);
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT_EQ(data.kind, CLUSTER_COLD_DATA_ABSENT);

	data_page(&forks[0], 0, 'P', 42);
	identity_page(&forks[1], 'L', 7);
	id = page(100, 0);
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT_EQ(data.kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT_EQ(data.version.mutation_token, 42);
	UT_ASSERT(incarnation_is(&data, 7));
	UT_ASSERT_EQ(data.flags, CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED);
	UT_ASSERT(classify_saw_header_valid && classify_saw_checksums);

	/* Without checksums only the header is known. */
	checksums_on = false;
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT_EQ(data.kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT_EQ(data.flags, 0);
	UT_ASSERT(!classify_saw_checksums);

	/* A page failing verification is invalid and names no incarnation. */
	checksums_on = true;
	header_valid = false;
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT_EQ(data.kind, CLUSTER_COLD_DATA_INVALID);
	UT_ASSERT_EQ(data.flags, 0);
	UT_ASSERT(incarnation_is(&data, 0));
	UT_ASSERT_EQ(o.pages_invalid, 1);
	UT_ASSERT_EQ(o.pages_observed, 5);

	/* A formatted page without a token, or an impossible fork, fails the
	 * observation itself. */
	header_valid = true;
	data_page(&forks[0], 0, 'Z', 0);
	UT_ASSERT(!cluster_cold_observe_data_v1(&o, &id, &data));
	id.forknum = MAX_FORKNUM + 1;
	UT_ASSERT(!cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT(!cluster_cold_observe_data_v1(NULL, &id, &data));
}

/* The incarnation comes from the relation's live SPACE identity, read in
 * the shared namespace; without one the page is reported NO_IDENTITY. */
UT_TEST(test_observe_identity_overlay)
{
	ClusterColdObserverV1 o;
	RfPageIdentityV1 id = page(100, 0);
	ClusterColdDataV1 data;
	char states[3] = { 0, 'T', 'X' };
	int i;

	for (i = 0; i < 3; i++) {
		storage_reset();
		forks[0].exists = true;
		forks[0].nblocks = 1;
		data_page(&forks[0], 0, 'P', 42);
		if (states[i] != 0)
			identity_page(&forks[1], states[i], 7);
		o = observer();
		UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
		UT_ASSERT_EQ(data.kind, CLUSTER_COLD_DATA_PRESENT);
		UT_ASSERT_EQ(data.flags,
					 CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
		UT_ASSERT(incarnation_is(&data, 0));
	}
	UT_ASSERT_EQ(identity_key_seen.system_identifier, 99);
	UT_ASSERT_EQ(identity_key_seen.database_incarnation, 4);
	UT_ASSERT_EQ(identity_key_seen.storage_uuid[0], 3);
	UT_ASSERT_EQ(identity_key_seen.locator.relNumber, 100);
}

/* The identity is read once per relation, and a relation never sees
 * another relation's cached identity. */
UT_TEST(test_observe_identity_cached_per_relation)
{
	ClusterColdObserverV1 o = observer();
	RfPageIdentityV1 id;
	ClusterColdDataV1 data;

	storage_reset();
	forks[0].exists = forks[2].exists = true;
	forks[0].nblocks = forks[2].nblocks = 2;
	data_page(&forks[0], 0, 'P', 42);
	data_page(&forks[0], 1, 'P', 43);
	data_page(&forks[2], 0, 'P', 50);
	data_page(&forks[2], 1, 'I', 0);
	identity_page(&forks[1], 'L', 7);
	identity_page(&forks[3], 'L', 9);
	identity_decodes = 0;
	id = page(100, 0);
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	id = page(100, 1);
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT(incarnation_is(&data, 7));
	UT_ASSERT_EQ(identity_decodes, 1);
	UT_ASSERT_EQ(space_reads, 1);
	id = page(200, 0);
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT(incarnation_is(&data, 9));
	UT_ASSERT_EQ(identity_decodes, 2);
	/* an invalid page consults no identity */
	id = page(200, 1);
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT_EQ(data.kind, CLUSTER_COLD_DATA_INVALID);
	UT_ASSERT_EQ(identity_decodes, 2);
	id = page(100, 0);
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT(incarnation_is(&data, 7));
	UT_ASSERT_EQ(identity_decodes, 3);

	/* After one with an identity, a relation without one names none. */
	forks[3].exists = false;
	id = page(200, 0);
	UT_ASSERT(cluster_cold_observe_data_v1(&o, &id, &data));
	UT_ASSERT((data.flags & CLUSTER_COLD_DATA_FLAG_NO_IDENTITY) != 0);
	UT_ASSERT(incarnation_is(&data, 0));
}

/* The SPACE owner checks the inputs, in feed order, against both SPACE
 * pages read from storage (zero where absent) in the shared namespace. */
UT_TEST(test_space_check_adapter)
{
	static const char payload[3][4] = { "AAA", "BBB", "CCC" };
	ClusterColdObserverV1 o = observer();
	ClusterColdSpaceInputV1 inputs[3];
	RelFileLocator locator = page(100, 0).locator;
	uint32 order[3];
	int i;

	memset(inputs, 0, sizeof(inputs));
	for (i = 0; i < 3; i++) {
		inputs[i].payload = payload[i];
		inputs[i].payload_length = 4;
	}
	storage_reset();
	identity_page(&forks[1], 'L', 7);
	forks[1].nblocks = 2;
	forks[1].blocks[1].data[0] = 'R';
	prepare_answer = true;
	UT_ASSERT(cluster_cold_space_check_v1(&o, &locator, inputs, 3, order));
	UT_ASSERT_EQ(prepare_count, 3);
	UT_ASSERT_EQ(order[0], 2);
	UT_ASSERT_EQ(order[2], 0);
	UT_ASSERT(prepare_inputs[0] == payload[0] && prepare_inputs[2] == payload[2]);
	UT_ASSERT_EQ(prepare_pages[0], 'L');
	UT_ASSERT_EQ(prepare_pages[1], 'R');
	UT_ASSERT_EQ(prepare_key.system_identifier, 99);
	UT_ASSERT_EQ(prepare_key.database_incarnation, 4);
	UT_ASSERT_EQ(prepare_key.storage_uuid[15], 3);
	UT_ASSERT_EQ(prepare_key.locator.relNumber, 100);
	UT_ASSERT_EQ(o.space_relations_checked, 1);

	/* One SPACE block, or none: the missing ones are zero pages. */
	forks[1].nblocks = 1;
	UT_ASSERT(cluster_cold_space_check_v1(&o, &locator, inputs, 3, order));
	UT_ASSERT_EQ(prepare_pages[0], 'L');
	UT_ASSERT_EQ(prepare_pages[1], 0);
	forks[1].exists = false;
	UT_ASSERT(cluster_cold_space_check_v1(&o, &locator, inputs, 3, order));
	UT_ASSERT_EQ(prepare_pages[0], 0);

	/* The owner's refusal is the answer. */
	prepare_answer = false;
	UT_ASSERT(!cluster_cold_space_check_v1(&o, &locator, inputs, 3, order));
	/* Malformed calls never reach an owner that would accept them. */
	prepare_answer = true;
	prepare_count = 99;
	UT_ASSERT(!cluster_cold_space_check_v1(&o, &locator, inputs, 0, order));
	UT_ASSERT(!cluster_cold_space_check_v1(NULL, &locator, inputs, 3, order));
	UT_ASSERT(!cluster_cold_space_check_v1(&o, &locator, inputs, 3, NULL));
	UT_ASSERT_EQ(prepare_count, 99);
}

static ClusterColdPlanV1 *
scan_plan(void)
{
	ClusterColdParticipantV1 cut;
	ClusterColdPlanV1 *plan = NULL;

	memset(&cut, 0, sizeof(cut));
	cut.thread_id = 2;
	cut.timeline = 1;
	cut.owner_incarnation = 12;
	cut.physical_lower = 0x1000;
	cut.native_redo = 0x1000;
	cut.tail_end = 0x1300;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(&cut, 1, 1024 * 1024, &plan), CLUSTER_COLD_OK);
	return plan;
}

static ClusterColdDetailV1
scan(ClusterColdScanResultV1 *result, bool foreign)
{
	ClusterColdPlanV1 *plan = scan_plan();
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	ClusterColdDetailV1 detail;

	memset(&root, 0, sizeof(root));
	memset(&token, 0, sizeof(token));
	root.identity.system_identifier = 77;
	memset(root.identity.storage_uuid, 5, 16);
	root.validated_tail_lsn_exclusive = 0x1300;
	detail = cluster_cold_scan_root_v1(plan, 0, &root, &token, !foreign, foreign, result);
	cluster_cold_plan_destroy_v1(&plan);
	return detail;
}

static void
scan_reset(void)
{
	visit_count = 2;
	visit_read[0] = 0x1000;
	visit_end[0] = 0x1100;
	visit_read[1] = 0x1100;
	visit_end[1] = 0x1300;
	visit_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	observed_records_delta = 0;
	observed_end = 0x1300;
	memset(decode_refusal_at, 0, sizeof(decode_refusal_at));
	decode_releases = 0;
}

/* Every visited record is decoded in the root's namespace and fed; the scan
 * is accepted only when the visit and the observed cut match the ROOT. */
UT_TEST(test_scan_root_proves_the_cut)
{
	ClusterColdScanResultV1 result;

	scan_reset();
	UT_ASSERT_EQ(scan(&result, true), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(result.records, 2);
	UT_ASSERT_EQ(result.root_result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(decode_saw_system, 77);
	UT_ASSERT_EQ(decode_saw_uuid0, 5);
	UT_ASSERT(decode_saw_foreign && !decode_saw_space_active);
	UT_ASSERT_EQ(decode_releases, 1);
	UT_ASSERT_EQ(scan(&result, false), CLUSTER_COLD_OK);
	UT_ASSERT(!decode_saw_foreign && decode_saw_space_active);

	scan_reset();
	visit_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	UT_ASSERT_EQ(scan(&result, true), CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT_EQ(result.root_result, CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	scan_reset();
	observed_records_delta = 1;
	UT_ASSERT_EQ(scan(&result, true), CLUSTER_COLD_SOURCE_GAP);
	scan_reset();
	observed_end = 0x1200;
	UT_ASSERT_EQ(scan(&result, true), CLUSTER_COLD_SOURCE_GAP);

	/* A record refused by the decoder stops the scan at that record. */
	scan_reset();
	decode_refusal_at[1] = CLUSTER_COLD_OPCODE_UNSUPPORTED;
	UT_ASSERT_EQ(scan(&result, true), CLUSTER_COLD_OPCODE_UNSUPPORTED);
	UT_ASSERT_EQ(result.failed_read_rec_ptr, 0x1100);
	UT_ASSERT_EQ(result.route_detail, 9);
	UT_ASSERT_EQ(result.records, 1);
	UT_ASSERT_EQ(decode_releases, 1);

	/* So does one the plan refuses (a record past the cut). */
	scan_reset();
	visit_end[1] = 0x1400;
	observed_end = 0x1400;
	UT_ASSERT_EQ(scan(&result, true), CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT_EQ(result.failed_read_rec_ptr, 0x1100);

	UT_ASSERT_EQ(cluster_cold_scan_root_v1(NULL, 0, NULL, NULL, false, false, &result),
				 CLUSTER_COLD_INVALID_ARGUMENT);
	UT_ASSERT_EQ(result.records, 0);
}

/* The pass-2 reader opens only a fully named source and reads the selected
 * restart segments; it ends where the stream ends. */
UT_TEST(test_reader_open_read_close)
{
	ClusterWalSourceRef source;
	ClusterColdReaderV1 *cold;
	XLogReaderState *state;
	char *errormsg = NULL;
	char path[] = "/tmp/d_s09_io_XXXXXX";
	char buffer[XLOG_BLCKSZ];
	char *expected = calloc(1, 2 * XLOG_BLCKSZ);
	int fd;

	memset(&source, 0, sizeof(source));
	source.timeline = 1;
	source.claim.identity.origin_thread_id = 3;
	UT_ASSERT_NULL(cluster_cold_reader_open_v1(NULL, 99, 0x1000));
	UT_ASSERT_NULL(cluster_cold_reader_open_v1(&source, 0, 0x1000));
	UT_ASSERT_NULL(cluster_cold_reader_open_v1(&source, 99, InvalidXLogRecPtr));
	cluster_wal_threads_dir = "";
	UT_ASSERT_NULL(cluster_cold_reader_open_v1(&source, 99, 0x1000));
	cluster_wal_threads_dir = "/wal";
	source.timeline = 0;
	UT_ASSERT_NULL(cluster_cold_reader_open_v1(&source, 99, 0x1000));
	source.timeline = 1;
	UT_ASSERT_EQ(readers_alive, 0);

	cold = cluster_cold_reader_open_v1(&source, 99, 0x1000);
	UT_ASSERT_NOT_NULL(cold);
	UT_ASSERT_EQ(begin_read_at, 0x1000);
	read_record_answer = false;
	UT_ASSERT_NULL(cluster_cold_reader_next_v1(cold, &errormsg));
	read_record_answer = true;
	state = cluster_cold_reader_next_v1(cold, &errormsg);
	UT_ASSERT_NOT_NULL(state);
	UT_ASSERT_EQ(state->system_identifier, 99);
	UT_ASSERT_EQ(state->seg.ws_tli, 1);
	UT_ASSERT_EQ(state->cluster_expected_thread_id, 3);

	/* The selected stream has no such segment: the stream ends. */
	segment_open_result = CLUSTER_CONTROL_ROOT_ABSENT;
	UT_ASSERT_EQ(state->routine.page_read(state, XLOG_BLCKSZ, XLOG_BLCKSZ, XLOG_BLCKSZ, buffer),
				 -1);
	/* Otherwise the page at its offset in the segment is read. */
	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	memset(expected + XLOG_BLCKSZ, 0x5A, XLOG_BLCKSZ);
	UT_ASSERT_EQ(write(fd, expected, 2 * XLOG_BLCKSZ), 2 * XLOG_BLCKSZ);
	segment_open_fd = fd;
	segment_open_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	UT_ASSERT_EQ(state->routine.page_read(state, XLOG_BLCKSZ, XLOG_BLCKSZ, XLOG_BLCKSZ, buffer),
				 XLOG_BLCKSZ);
	UT_ASSERT_EQ(memcmp(buffer, expected + XLOG_BLCKSZ, XLOG_BLCKSZ), 0);
	/* A page past the written end is short: the stream ends there. */
	UT_ASSERT_EQ(
		state->routine.page_read(state, 2 * XLOG_BLCKSZ, XLOG_BLCKSZ, 2 * XLOG_BLCKSZ, buffer), -1);
	cluster_cold_reader_close_v1(&cold);
	UT_ASSERT_NULL(cold);
	UT_ASSERT_EQ(readers_alive, 0);
	cluster_cold_reader_close_v1(&cold);
	unlink(path);
	free(expected);
}

int
main(void)
{
	UT_RUN(test_observe_absent_and_present);
	UT_RUN(test_observe_identity_overlay);
	UT_RUN(test_observe_identity_cached_per_relation);
	UT_RUN(test_space_check_adapter);
	UT_RUN(test_scan_root_proves_the_cut);
	UT_RUN(test_reader_open_read_close);
	UT_DONE();
	return ut_failed_count != 0;
}
