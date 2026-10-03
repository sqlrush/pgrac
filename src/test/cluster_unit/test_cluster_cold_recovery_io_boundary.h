/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_io_boundary.h
 *	  Table-driven boundaries for test_cluster_cold_recovery_io.c: process
 *	  and memory hooks, relation storage (smgr), page verification, the
 *	  page classifier, the SPACE identity and recovery codecs, the root
 *	  visitor, the record decoder and the WAL reader.
 *
 *	  Included once by test_cluster_cold_recovery_io.c; defines the
 *	  external symbols cluster_cold_recovery_io.c links against.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_io_boundary.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_COLD_RECOVERY_IO_BOUNDARY_H
#define TEST_CLUSTER_COLD_RECOVERY_IO_BOUNDARY_H

/* ---- process and memory boundaries ---- */
volatile sig_atomic_t InterruptPending = false;
static uint32 wait_event_slot;
uint32 *my_wait_event_info = &wait_event_slot;
int wal_segment_size = 16 * 1024 * 1024;
char *cluster_wal_threads_dir = "/wal";

void
ProcessInterrupts(void)
{
	abort();
}

/* Only the segment-open failure reports, and no case reaches it. */
int
errdetail(const char *fmt, ...)
{
	(void)fmt;
	abort();
}

int
errhint(const char *fmt, ...)
{
	(void)fmt;
	abort();
}

void *
palloc0(Size size)
{
	return calloc(1, size);
}

void *
palloc_extended(Size size, int flags)
{
	return (flags & MCXT_ALLOC_ZERO) != 0 ? calloc(1, size) : malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

/* ---- storage: forks of relations 100 and 200 ---- */
#define MAX_FORK_BLOCKS 3

typedef struct FakeFork {
	RelFileNumber rel;
	ForkNumber fork;
	bool exists;
	BlockNumber nblocks;
	PGAlignedBlock blocks[MAX_FORK_BLOCKS];
} FakeFork;

static FakeFork forks[4];
static SMgrRelationData smgr_relation;
static int space_reads;
static bool header_valid = true;
static bool checksums_on = true;

static void
storage_reset(void)
{
	memset(forks, 0, sizeof(forks));
	forks[0].rel = 100;
	forks[0].fork = MAIN_FORKNUM;
	forks[1].rel = 100;
	forks[1].fork = SPACE_FORKNUM;
	forks[2].rel = 200;
	forks[2].fork = MAIN_FORKNUM;
	forks[3].rel = 200;
	forks[3].fork = SPACE_FORKNUM;
	space_reads = 0;
	header_valid = true;
	checksums_on = true;
}

static FakeFork *
fork_of(SMgrRelation relation, ForkNumber fork)
{
	int i;

	for (i = 0; i < (int)lengthof(forks); i++)
		if (forks[i].rel == relation->smgr_rlocator.locator.relNumber && forks[i].fork == fork)
			return &forks[i];
	return NULL;
}

SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	memset(&smgr_relation, 0, sizeof(smgr_relation));
	smgr_relation.smgr_rlocator.locator = locator;
	smgr_relation.smgr_rlocator.backend = backend;
	return &smgr_relation;
}

bool
smgrexists(SMgrRelation relation, ForkNumber fork)
{
	FakeFork *f = fork_of(relation, fork);

	return f != NULL && f->exists;
}

BlockNumber
smgrnblocks(SMgrRelation relation, ForkNumber fork)
{
	FakeFork *f = fork_of(relation, fork);

	UT_ASSERT(f != NULL && f->exists);
	return f->nblocks;
}

void
smgrread(SMgrRelation relation, ForkNumber fork, BlockNumber block, void *buffer)
{
	FakeFork *f = fork_of(relation, fork);

	UT_ASSERT(f != NULL && f->exists && block < f->nblocks && block < MAX_FORK_BLOCKS);
	space_reads += fork == SPACE_FORKNUM;
	memcpy(buffer, f->blocks[block].data, BLCKSZ);
}

bool
PageIsVerifiedExtended(Page page, BlockNumber blkno, int flags)
{
	(void)page;
	(void)blkno;
	(void)flags;
	return header_valid;
}

bool
DataChecksumsEnabled(void)
{
	return checksums_on;
}

/* Data page convention: byte 0 'I' torn, 'Z' formatted without a token,
 * otherwise bytes 8..15 are the token. */
static bool classify_saw_header_valid;
static bool classify_saw_checksums;

bool
cluster_cold_classify_page_v1(const char *page, BlockNumber blkno, bool page_header_valid,
							  bool checksums, ClusterColdDataV1 *out)
{
	(void)blkno;
	classify_saw_header_valid = page_header_valid;
	classify_saw_checksums = checksums;
	memset(out, 0, sizeof(*out));
	if (page[0] == 'Z')
		return false;
	if (page[0] == 'I' || !page_header_valid) {
		out->kind = CLUSTER_COLD_DATA_INVALID;
		return true;
	}
	out->kind = CLUSTER_COLD_DATA_PRESENT;
	memcpy(&out->version.mutation_token, page + 8, sizeof(uint64));
	if (checksums)
		out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
	return true;
}

static void
data_page(FakeFork *f, BlockNumber block, char marker, uint64 token)
{
	memset(f->blocks[block].data, 0, BLCKSZ);
	f->blocks[block].data[0] = marker;
	memcpy(f->blocks[block].data + 8, &token, sizeof(token));
}

/* SPACE identity page convention: byte 0 'L' live (incarnation byte 1),
 * 'T' tombstoned, anything else unreadable. */
static int identity_decodes;
static ClusterSpaceIdentityKey identity_key_seen;

bool
cluster_space_identity_page_decode(const void *page, size_t length, ForkNumber forknum,
								   BlockNumber block, const ClusterSpaceIdentityKey *expected,
								   ClusterSpaceIdentity *out, uint64 *mutation_token)
{
	const uint8 *bytes = (const uint8 *)page;

	identity_decodes++;
	identity_key_seen = *expected;
	UT_ASSERT_EQ(length, BLCKSZ);
	UT_ASSERT_EQ(forknum, SPACE_FORKNUM);
	UT_ASSERT_EQ(block, 0);
	if (bytes[0] != 'L' && bytes[0] != 'T')
		return false;
	memset(out, 0, sizeof(*out));
	out->key = *expected;
	memset(out->incarnation, bytes[1], 16);
	out->state = bytes[0] == 'L' ? CLUSTER_SPACE_IDENTITY_LIVE : CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	*mutation_token = 1;
	return true;
}

static void
identity_page(FakeFork *space, char state, uint8 incarnation)
{
	space->exists = true;
	space->nblocks = Max(space->nblocks, 1);
	memset(space->blocks[0].data, 0, BLCKSZ);
	space->blocks[0].data[0] = state;
	space->blocks[0].data[1] = (char)incarnation;
}

/* ---- the SPACE owner's pure preparation ---- */
static bool prepare_answer = true;
static uint32 prepare_count;
static ClusterSpaceIdentityKey prepare_key;
static char prepare_pages[2];
static const void *prepare_inputs[4];

bool
cluster_space_recovery_prepare(const ClusterSpaceRecoveryInput *inputs, uint32 count,
							   const ClusterSpaceIdentityKey *expected, const void *identity_page,
							   const void *reservation_page, uint32 *order,
							   ClusterSpaceRecoveryImage *out)
{
	uint32 i;

	(void)out;
	prepare_count = count;
	prepare_key = *expected;
	prepare_pages[0] = ((const char *)identity_page)[0];
	prepare_pages[1] = ((const char *)reservation_page)[0];
	for (i = 0; i < count && i < lengthof(prepare_inputs); i++) {
		UT_ASSERT_EQ(inputs[i].length, 4);
		prepare_inputs[i] = inputs[i].data;
		order[i] = count - 1 - i;
	}
	return prepare_answer;
}

/* ---- the root visitor and the record decoder ---- */
#define MAX_VISIT 4
static XLogRecPtr visit_read[MAX_VISIT];
static XLogRecPtr visit_end[MAX_VISIT];
static int visit_count;
static ClusterControlRootResult visit_result;
static uint64 observed_records_delta;
static XLogRecPtr observed_end;
static ClusterColdDetailV1 decode_refusal_at[MAX_VISIT];
static bool decode_saw_foreign;
static bool decode_saw_space_active;
static uint64 decode_saw_system;
static uint8 decode_saw_uuid0;
static int decode_releases;

ClusterControlRootResult
cluster_control_root_recovery_visit(const ClusterControlRootSnapshot *expected,
									const ClusterControlRootReadToken *token,
									ClusterWalRecordVisitor visitor, void *arg,
									ClusterWalTailObservation *out)
{
	XLogReaderState reader;
	int i;

	(void)expected;
	(void)token;
	memset(out, 0, sizeof(*out));
	memset(&reader, 0, sizeof(reader));
	for (i = 0; i < visit_count; i++) {
		reader.ReadRecPtr = visit_read[i];
		reader.EndRecPtr = visit_end[i];
		out->records++;
		if (!visitor(&reader, arg))
			break;
	}
	out->records += observed_records_delta;
	out->complete_end = observed_end;
	return visit_result;
}

ClusterColdDetailV1
cluster_cold_recovery_decode_v1(struct XLogReaderState *reader, uint64 system_identifier,
								const uint8 storage_uuid[16], bool space_active, bool foreign,
								ClusterColdDecodedV1 *out)
{
	int i;

	decode_saw_system = system_identifier;
	decode_saw_uuid0 = storage_uuid[0];
	decode_saw_space_active = space_active;
	decode_saw_foreign = foreign;
	memset(&out->record, 0, sizeof(out->record));
	for (i = 0; i < visit_count; i++)
		if (visit_read[i] == reader->ReadRecPtr)
			break;
	UT_ASSERT(i < visit_count);
	if (decode_refusal_at[i] != CLUSTER_COLD_OK) {
		out->route_detail = 9;
		return decode_refusal_at[i];
	}
	out->record.read_rec_ptr = reader->ReadRecPtr;
	out->record.end_rec_ptr = reader->EndRecPtr;
	out->record.prev_rec_ptr = i == 0 ? reader->ReadRecPtr - 0x40 : visit_read[i - 1];
	out->record.scn = 1;
	out->record.rmid = RM_HEAP_ID;
	return CLUSTER_COLD_OK;
}

void
cluster_cold_decoded_release_v1(ClusterColdDecodedV1 *decoded)
{
	(void)decoded;
	decode_releases++;
}

/* ---- the pass-2 WAL reader ---- */
static XLogRecPtr begin_read_at;
static bool read_record_answer;
static ClusterControlRootResult segment_open_result;
static int segment_open_fd = -1;
static int readers_alive;

XLogReaderState *
XLogReaderAllocate(int segment_size, const char *waldir, XLogReaderRoutine *routine,
				   void *private_data)
{
	XLogReaderState *state = calloc(1, sizeof(XLogReaderState));

	(void)waldir;
	state->segcxt.ws_segsize = segment_size;
	state->routine = *routine;
	state->private_data = private_data;
	readers_alive++;
	return state;
}

void
XLogReaderFree(XLogReaderState *state)
{
	readers_alive--;
	free(state);
}

void
XLogBeginRead(XLogReaderState *state, XLogRecPtr start)
{
	(void)state;
	begin_read_at = start;
}

XLogRecord *
XLogReadRecord(XLogReaderState *state, char **errormsg)
{
	static XLogRecord record;

	(void)state;
	*errormsg = NULL;
	return read_record_answer ? &record : NULL;
}

ClusterControlRootResult
cluster_wal_restart_segment_open(const char *wal_root, const ClusterWalSourceRef *input,
								 TimeLineID timeline, XLogSegNo segno, int segsize, int *fd_out)
{
	(void)wal_root;
	(void)input;
	(void)timeline;
	(void)segno;
	(void)segsize;
	*fd_out = segment_open_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY ? segment_open_fd : -1;
	return segment_open_result;
}

#endif /* TEST_CLUSTER_COLD_RECOVERY_IO_BOUNDARY_H */
