/* Real native output IO and flush bodies. The file handle is a temporary
 * host file: pwrite completion is deliberately separate from fsync.
 * Author: SqlRush <sqlrush@gmail.com> */
#include <unistd.h>
#include "executor/instrument.h"
#include "pg_trace.h"
static bool holder_output_start(BufferDesc *, bool);
static void holder_output_end(BufferDesc *, bool, uint32);
bool cluster_past_image = true;
MemoryContext TopMemoryContext = (MemoryContext)1;
void *MemoryContextAllocAligned(MemoryContext context, Size size, Size alignment, int flags)
{
	void *memory = NULL;
	UT_ASSERT(context == TopMemoryContext); UT_ASSERT_EQ(flags, 0);
	if (posix_memalign(&memory, alignment, size) != 0) abort();
	return memory;
}
static struct SMgrRelationData holder_output_relation;
static BufferUsage holder_output_usage;
static unsigned holder_writes, holder_write_notes, holder_output_wakes;
static unsigned holder_written_wal_flushes;
static bool holder_redirty;
static PGAlignedBlock holder_written;
static SMgrRelation holder_output_open(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(locator, BufTagGetRelFileLocator(&request_buf.tag)));
	UT_ASSERT_EQ(backend, InvalidBackendId);
	UT_ASSERT(LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf)));
	holder_output_relation.smgr_rlocator.locator = locator;
	return &holder_output_relation;
}
static void holder_output_write(SMgrRelation relation, ForkNumber fork, BlockNumber block,
	const void *bytes, bool skip)
{
	UT_ASSERT(relation == &holder_output_relation);
	UT_ASSERT_EQ(fork, request_buf.tag.forkNum); UT_ASSERT_EQ(block, request_buf.tag.blockNum);
	UT_ASSERT(!skip); UT_ASSERT(LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf)));
	UT_ASSERT(ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS) != 0);
	UT_ASSERT((pg_atomic_read_u64(&request_own.write_io_generation) & 1) != 0);
	holder_writes++;
	holder_written_wal_flushes = holder_wal_flushes;
	if (holder_write_error) pg_re_throw();
	memcpy(holder_written.data, bytes, BLCKSZ);
	UT_ASSERT_EQ(pwrite(fileno(disk_file), bytes, BLCKSZ, 0), BLCKSZ);
	if (holder_redirty) pg_atomic_fetch_or_u32(&request_buf.state, BM_JUST_DIRTIED);
}
static void holder_output_note(BufferTag tag, uint64 scn)
{
	UT_ASSERT(BufferTagsEqual(&tag, &request_buf.tag));
	UT_ASSERT_EQ(scn, ((PageHeader)holder_written.data)->pd_block_scn);
	holder_write_notes++;
}
static void holder_output_context(void *arg) { UT_ASSERT(arg == &request_buf); }
static void holder_output_broadcast(ConditionVariable *cv)
{
	UT_ASSERT(cv == BufferDescriptorGetIOCV(&request_buf));
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	UT_ASSERT((pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS) == 0);
	UT_ASSERT((pg_atomic_read_u64(&request_own.write_io_generation) & 1) == 0);
	holder_output_wakes++; ConditionVariableBroadcast(cv);
}
static void holder_output_wait(BufferDesc *buf) { (void)buf; UT_ASSERT(false); pg_re_throw(); }
#define LockBufHdr stage_header_lock
#define BufHdrGetBlock(buf) ((void)(buf), disk_shared.data)
#define BufferGetLSN(buf) ((void)(buf), PageGetLSN((Page)disk_shared.data))
#define FlushBuffer holder_flush
#define FlushBufferWithAttempt holder_flush_attempt
#define FlushBufferWithRecovery holder_flush_recovery
#define StartBufferIO holder_output_start
#define TerminateBufferIO holder_output_end
#define AbortBufferIO holder_output_abort
#define WaitIO holder_output_wait
#define smgropen holder_output_open
#define smgrwrite holder_output_write
#define shared_buffer_write_error_callback holder_output_context
#define cluster_gcs_block_pi_write_note holder_output_note
#define pgBufferUsage holder_output_usage
#define pgstat_prepare_io_time() ((instr_time){0})
#define pgstat_count_io_op_time(object, context, op, start, count) ((void)(start))
#define ConditionVariableBroadcast holder_output_broadcast
#define cluster_smart_fusion false
#define cluster_storage_mode_enabled() true
#define cluster_lever_h_note_pi_implicit_discard() ((void)0)
#define cluster_sf_dep_buffer_flush_blocked(buf) false
#define cluster_space_recovery_flush_permitted_v1(batch, buffer) false
#define cluster_injection_is_armed(name) false
#undef CLUSTER_INJECTION_POINT
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#define cluster_block_io_trace_note(...) ((void)0)
#define cluster_block_io_trace_write(...) ((void)0)
#define relpathperm(locator, fork) strdup("host-holder-output")
#define pfree(pointer) free(pointer)
#include "test_cluster_native_holder_flush_guard.inc"
#include "test_cluster_pcm_flush_owner.inc"
#undef pfree
#undef relpathperm
#undef cluster_block_io_trace_write
#undef cluster_block_io_trace_note
#undef CLUSTER_INJECTION_POINT
#undef cluster_injection_is_armed
#undef cluster_space_recovery_flush_permitted_v1
#undef cluster_sf_dep_buffer_flush_blocked
#undef cluster_smart_fusion
#undef cluster_lever_h_note_pi_implicit_discard
#undef cluster_storage_mode_enabled
#undef ConditionVariableBroadcast
#undef pgstat_count_io_op_time
#undef pgstat_prepare_io_time
#undef pgBufferUsage
#undef cluster_gcs_block_pi_write_note
#undef shared_buffer_write_error_callback
#undef smgrwrite
#undef smgropen
#undef WaitIO
#undef AbortBufferIO
#undef TerminateBufferIO
#undef StartBufferIO
#undef FlushBufferWithRecovery
#undef FlushBufferWithAttempt
#undef FlushBuffer
#undef BufferGetLSN
#undef BufHdrGetBlock
#undef LockBufHdr
