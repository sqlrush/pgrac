/* Complete-source accounting, without mocks for claim validity or continuity.
 * Synthetic decoded headers do not claim physical WAL or retirement coverage.
 * Author: SqlRush <sqlrush@gmail.com> */
#define USE_PGRAC_CLUSTER 1
#define USE_CLUSTER_UNIT 1
#include "postgres.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static unsigned allocations, releases;
static Size allocated_bytes;

static void *
stream_alloc(size_t count, size_t size)
{
	allocations++;
	allocated_bytes += count * size;
	return calloc(count, size);
}

static void
stream_free(void *p)
{
	if (p != NULL)
		releases++;
	free(p);
}

#define calloc stream_alloc
#define free stream_free
#include "../../backend/cluster/cluster_pi_contribution_stream.c"
#undef calloc
#undef free

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected Assert %s at %s:%d\n", condition, file, line);
	abort();
}

static ClusterWalSourceRef sources[3];
static RfContributorStreamCutV1 cuts[3];

static void
fixture(void)
{
	memset(sources, 0, sizeof(sources));
	memset(cuts, 0, sizeof(cuts));
	allocations = releases = 0;
	allocated_bytes = 0;
	for (uint32 i = 0; i < 3; i++) {
		ClusterWalSourceRef *s = &sources[i];
		s->claim.identity.system_identifier = 99;
		s->claim.identity.storage_uuid[0] = 3;
		s->claim.identity.authority_uuid[0] = 4;
		s->claim.identity.origin_node_id = i < 2 ? 1 : 2;
		s->claim.identity.origin_thread_id = i < 2 ? 2 : 3;
		s->claim.identity.thread_claim_created_at = 10 + i;
		s->claim.identity.origin_owner_incarnation = 9 + i;
		s->claim.identity.root_lineage_seq = 1;
		s->claim.claim_sha256[0] = 5 + i;
		s->claim.max_config_generation = 1;
		s->claim.database_incarnation = 42;
		s->timeline = 7;
		cuts[i].failed_thread = s->claim.identity.origin_thread_id;
		cuts[i].origin_owner_incarnation = s->claim.identity.origin_owner_incarnation;
		cuts[i].timeline_id = 7;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 0x100;
		cuts[i].scan_end_exclusive = 0x180;
	}
}

static ClusterPiContributionStreamV1 *
create(uint32 count)
{
	ClusterPiContributionStreamV1 *stream = NULL;
	UT_ASSERT_EQ(cluster_pi_contribution_stream_create_v1(sources, cuts, count, &stream),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_NOT_NULL(stream);
	return stream;
}

static RfPageProofDetailV1
feed(ClusterPiContributionStreamV1 *stream, uint32 index, XLogRecPtr start, XLogRecPtr prev,
	 RmgrId rmid)
{
	DecodedXLogRecord decoded = { 0 };
	XLogReaderState reader = { 0 };
	decoded.header.xl_tot_len = 64;
	decoded.header.xl_prev = prev;
	decoded.header.xl_rmid = rmid;
	decoded.header.xl_crc = 17;
	reader.ReadRecPtr = start;
	reader.EndRecPtr = start + 64;
	reader.record = &decoded;
	return cluster_pi_contribution_stream_record_v1(stream, index, &sources[index], &reader);
}

static ClusterWalTailObservation
observation(uint64 records)
{
	ClusterWalTailObservation out = { 0 };
	out.database_incarnation = 42;
	out.records = records;
	if (records) {
		out.last_record_start = 0x100 + (records - 1) * 64;
		out.complete_end = out.last_record_start + 64;
		out.last_record_crc = 17;
	}
	return out;
}

UT_TEST(full_generations_and_empty_input)
{
	ClusterPiContributionStreamV1 *stream;
	ClusterWalTailObservation observed = observation(2), empty = observation(0);
	uint64 count = UINT64_MAX;
	fixture();
	cuts[2].flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
	cuts[2].scan_end_exclusive = cuts[2].scan_begin_inclusive;
	stream = create(3);
	if (stream == NULL)
		return;
	/* Same timeline/LSNs in different real generations are independent. */
	for (uint32 i = 0; i < 2; i++) {
		UT_ASSERT_EQ(feed(stream, i, 0x100, 0x80, RM_HEAP_ID), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(feed(stream, i, 0x140, 0x100, RM_XACT_ID), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(cluster_pi_contribution_stream_finish_v1(stream, i, &observed),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT_EQ(cluster_pi_contribution_stream_finish_v1(stream, 2, &empty),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cluster_pi_contribution_stream_seal_v1(stream, &count), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(count, 4);
	cluster_pi_contribution_stream_destroy_v1(&stream);
	UT_ASSERT_NULL(stream);
	UT_ASSERT_EQ(allocations, releases);
}

UT_TEST(missing_first_page_side_space)
{
	const RmgrId classes[] = { RM_HEAP_ID, RM_XACT_ID, RM_SMGR_ID };
	for (uint32 i = 0; i < lengthof(classes); i++) {
		ClusterPiContributionStreamV1 *stream;
		fixture();
		stream = create(1);
		if (stream == NULL)
			return;
		UT_ASSERT_EQ(feed(stream, 0, 0x140, 0x100, classes[i]), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		cluster_pi_contribution_stream_destroy_v1(&stream);
	}
}

UT_TEST(missing_middle_page_side_space)
{
	const RmgrId classes[] = { RM_HEAP_ID, RM_XACT_ID, RM_SMGR_ID };
	for (uint32 i = 0; i < lengthof(classes); i++) {
		ClusterPiContributionStreamV1 *stream;
		uint64 count = 123;
		fixture();
		cuts[0].scan_end_exclusive = 0x1c0;
		stream = create(1);
		if (stream == NULL)
			return;
		UT_ASSERT_EQ(feed(stream, 0, 0x100, 0x80, RM_XLOG_ID), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(feed(stream, 0, 0x180, 0x140, classes[i]), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		UT_ASSERT_EQ(cluster_pi_contribution_stream_seal_v1(stream, &count),
					 RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		UT_ASSERT_EQ(count, 123);
		cluster_pi_contribution_stream_destroy_v1(&stream);
	}
}

UT_TEST(missing_tail_cannot_finish_or_resume)
{
	ClusterPiContributionStreamV1 *stream;
	ClusterWalTailObservation observed = observation(1);
	fixture();
	stream = create(1);
	if (stream == NULL)
		return;
	UT_ASSERT_EQ(feed(stream, 0, 0x100, 0, RM_SMGR_ID), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cluster_pi_contribution_stream_finish_v1(stream, 0, &observed),
				 RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT_EQ(feed(stream, 0, 0x140, 0x100, RM_XACT_ID), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	cluster_pi_contribution_stream_destroy_v1(&stream);
}

UT_TEST(record_order_and_predecessor_are_exact)
{
	const XLogRecPtr starts[] = { 0x100, 0x120, 0x140 };
	const XLogRecPtr prevs[] = { 0x80, 0x100, 0x90 };
	for (uint32 i = 0; i < lengthof(starts); i++) {
		ClusterPiContributionStreamV1 *stream;
		fixture();
		stream = create(1);
		if (stream == NULL)
			return;
		UT_ASSERT_EQ(feed(stream, 0, 0x100, 0x80, RM_HEAP_ID), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(feed(stream, 0, starts[i], prevs[i], RM_HEAP_ID),
					 RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		cluster_pi_contribution_stream_destroy_v1(&stream);
	}
}

UT_TEST(full_source_cannot_be_substituted)
{
	for (uint32 i = 0; i < 6; i++) {
		ClusterPiContributionStreamV1 *stream;
		fixture();
		stream = create(1);
		if (stream == NULL)
			return;
		switch (i) {
		case 0:
			sources[0].claim.identity.origin_owner_incarnation++;
			break;
		case 1:
			sources[0].claim.database_incarnation++;
			break;
		case 2:
			sources[0].claim.claim_sha256[31]++;
			break;
		case 3:
			sources[0].claim.max_config_generation++;
			break;
		case 4:
			sources[0].timeline++;
			break;
		case 5:
			sources[0].claim.identity.authority_uuid[15]++;
			break;
		}
		UT_ASSERT_EQ(feed(stream, 0, 0x100, 0, RM_HEAP_ID), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
		cluster_pi_contribution_stream_destroy_v1(&stream);
	}
}

UT_TEST(invalid_input_vector_never_publishes)
{
	for (uint32 i = 0; i < 11; i++) {
		ClusterPiContributionStreamV1 *stream = NULL;
		uint32 n = 2;
		fixture();
		switch (i) {
		case 0:
			n = 0;
			break;
		case 1:
			n = RF_PAGE_STABLE_MAX_PARTICIPANTS + 1;
			break;
		case 2:
			sources[0].claim.identity.origin_owner_incarnation = 0;
			break;
		case 3:
			cuts[1] = cuts[0];
			sources[1] = sources[0];
			break;
		case 4:
			sources[1].claim.database_incarnation++;
			break;
		case 5:
			cuts[0].origin_owner_incarnation++;
			break;
		case 6:
			cuts[0].flags = 0;
			break;
		case 7:
			cuts[0].flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
			break;
		case 8:
			cuts[0].scan_begin_inclusive = 0;
			break;
		case 9:
			cuts[0].scan_end_exclusive = cuts[0].scan_begin_inclusive;
			break;
		case 10:
			cuts[0].flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
			cuts[0].scan_end_exclusive = cuts[0].scan_begin_inclusive;
			cuts[0].contributor_count = 1;
			break;
		}
		UT_ASSERT(cluster_pi_contribution_stream_create_v1(sources, cuts, n, &stream)
				  != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NULL(stream);
		UT_ASSERT_EQ(allocations, releases);
	}
}

UT_TEST(physical_observation_must_match_every_field)
{
	for (uint32 i = 0; i < 5; i++) {
		ClusterPiContributionStreamV1 *stream;
		ClusterWalTailObservation observed = observation(2);
		fixture();
		stream = create(1);
		if (stream == NULL)
			return;
		UT_ASSERT_EQ(feed(stream, 0, 0x100, 0, RM_HEAP_ID), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(feed(stream, 0, 0x140, 0x100, RM_XACT_ID), RF_PAGE_PROOF_DETAIL_OK);
		switch (i) {
		case 0:
			observed.records++;
			break;
		case 1:
			observed.complete_end++;
			break;
		case 2:
			observed.last_record_start++;
			break;
		case 3:
			observed.last_record_crc++;
			break;
		case 4:
			observed.database_incarnation++;
			break;
		}
		UT_ASSERT_EQ(cluster_pi_contribution_stream_finish_v1(stream, 0, &observed),
					 RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		cluster_pi_contribution_stream_destroy_v1(&stream);
	}
}

UT_TEST(empty_source_still_requires_a_physical_visit)
{
	ClusterPiContributionStreamV1 *stream;
	uint64 count = 123;
	fixture();
	cuts[0].flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
	cuts[0].scan_end_exclusive = cuts[0].scan_begin_inclusive;
	stream = create(1);
	if (stream == NULL)
		return;
	UT_ASSERT_EQ(cluster_pi_contribution_stream_seal_v1(stream, &count),
				 RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT_EQ(count, 123);
	cluster_pi_contribution_stream_destroy_v1(&stream);
}

UT_TEST(finished_source_cannot_accept_more_records)
{
	ClusterPiContributionStreamV1 *stream;
	ClusterWalTailObservation observed = observation(2);
	fixture();
	stream = create(1);
	if (stream == NULL)
		return;
	UT_ASSERT_EQ(feed(stream, 0, 0x100, 0, RM_HEAP_ID), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(feed(stream, 0, 0x140, 0x100, RM_XACT_ID), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cluster_pi_contribution_stream_finish_v1(stream, 0, &observed),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(feed(stream, 0, 0x140, 0x100, RM_XACT_ID), RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION);
	cluster_pi_contribution_stream_destroy_v1(&stream);
}

UT_TEST(million_records_use_constant_storage)
{
	const uint64 n = 1000000;
	ClusterPiContributionStreamV1 *stream;
	ClusterWalTailObservation observed = observation(n);
	uint64 count = 0;
	unsigned initial_allocations;
	Size initial_bytes;
	fixture();
	cuts[0].scan_end_exclusive = observed.complete_end;
	stream = create(1);
	if (stream == NULL)
		return;
	initial_allocations = allocations;
	initial_bytes = allocated_bytes;
	for (uint64 i = 0; i < n; i++) {
		RfPageProofDetailV1 detail
			= feed(stream, 0, 0x100 + i * 64, i == 0 ? 0 : 0x100 + (i - 1) * 64,
				   i % 2 ? RM_XACT_ID : RM_HEAP_ID);
		if (detail != RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
			break;
		}
	}
	UT_ASSERT_EQ(cluster_pi_contribution_stream_finish_v1(stream, 0, &observed),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cluster_pi_contribution_stream_seal_v1(stream, &count), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(count, n);
	UT_ASSERT_EQ(allocations, initial_allocations);
	UT_ASSERT_EQ(allocated_bytes, initial_bytes);
	UT_ASSERT(initial_bytes <= 65536);
	cluster_pi_contribution_stream_destroy_v1(&stream);
	UT_ASSERT_EQ(allocations, releases);
}

UT_TEST(source_padding_is_not_identity)
{
	ClusterPiContributionStreamV1 *stream;
	Size end = offsetof(ClusterWalSourceRef, timeline) + sizeof(TimeLineID);
	fixture();
	stream = create(1);
	if (stream == NULL)
		return;
	/* Struct assignment need not preserve tail padding. */
	memset((char *)&sources[0] + end, 0xa5, sizeof(sources[0]) - end);
	UT_ASSERT_EQ(feed(stream, 0, 0x100, 0, RM_HEAP_ID), RF_PAGE_PROOF_DETAIL_OK);
	cluster_pi_contribution_stream_destroy_v1(&stream);
}

UT_TEST(malformed_decoded_header_is_terminal)
{
	for (uint32 i = 0; i < 5; i++) {
		ClusterPiContributionStreamV1 *stream;
		DecodedXLogRecord decoded = { 0 };
		XLogReaderState reader = { 0 };
		RfPageProofDetailV1 expected = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		fixture();
		stream = create(1);
		if (stream == NULL)
			return;
		decoded.header.xl_tot_len = 64;
		reader.record = &decoded;
		reader.ReadRecPtr = 0x100;
		reader.EndRecPtr = 0x140;
		switch (i) {
		case 0:
			decoded.header.xl_tot_len = SizeOfXLogRecord - 1;
			break;
		case 1:
			decoded.header.xl_tot_len = 65;
			break;
		case 2:
			decoded.header.xl_prev = reader.ReadRecPtr;
			break;
		case 3:
			reader.EndRecPtr = 0x200;
			break;
		case 4:
			reader.record = NULL;
			expected = RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
			break;
		}
		UT_ASSERT_EQ(cluster_pi_contribution_stream_record_v1(stream, 0, &sources[0], &reader),
					 expected);
		UT_ASSERT_EQ(feed(stream, 0, 0x100, 0, RM_HEAP_ID), expected);
		cluster_pi_contribution_stream_destroy_v1(&stream);
	}
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(full_generations_and_empty_input);
	UT_RUN(missing_first_page_side_space);
	UT_RUN(missing_middle_page_side_space);
	UT_RUN(missing_tail_cannot_finish_or_resume);
	UT_RUN(record_order_and_predecessor_are_exact);
	UT_RUN(full_source_cannot_be_substituted);
	UT_RUN(invalid_input_vector_never_publishes);
	UT_RUN(physical_observation_must_match_every_field);
	UT_RUN(empty_source_still_requires_a_physical_visit);
	UT_RUN(finished_source_cannot_accept_more_records);
	UT_RUN(million_records_use_constant_storage);
	UT_RUN(source_padding_is_not_identity);
	UT_RUN(malformed_decoded_header_is_terminal);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
