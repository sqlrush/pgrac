/* Real native codec/range and composition receiver. SLRU pages/locks,
 * epoch/origin, memory contexts and ERROR delivery are controlled leaves. */
int composition_component_main(void);
#define main composition_component_main
#include "test_cluster_mx_composition.c"
#undef main
#include "../../backend/access/transam/multixact.c"

bool InRecovery;
int cluster_node_id = 0;
int MaxBackends = 2;
int max_prepared_xacts;
BackendId MyBackendId = 1;
static LWLockPadded native_locks[128];
LWLockPadded *MainLWLockArray = native_locks;
static MultiXactStateData native_state;
static MultiXactId visible[3], oldest_member[3];
static SlruSharedData native_offset_shared, native_member_shared;
static struct NativePage { int number; char bytes[BLCKSZ]; } offset_pages[4], member_pages[64];
static char *native_offset_buffers[4], *native_member_buffers[64];
static bool native_offset_dirty[4], native_member_dirty[64];
static unsigned noffsets, nmembers_pages, offset_reads, member_reads, native_fail_at;
static bool native_reading, native_mine = true;
static int drift;
static unsigned held[4];
static uint64 native_epoch = 7;
static MultiXactMember native_input[66000];

uint64 cluster_epoch_get_current(void) { return native_epoch; }
bool cluster_enabled = true;
static ClusterConf native_conf = {.node_count=2};
ClusterConf *ClusterConfShmem = &native_conf;
bool cluster_mxid_is_mine(MultiXactId multi) { (void)multi; return native_mine; }

static unsigned lock_index(LWLock *lock)
{
	if (lock == MultiXactTruncationLock) return 0;
	if (lock == MultiXactGenLock) return 1;
	if (lock == MultiXactOffsetSLRULock) return 2;
	if (lock == MultiXactMemberSLRULock) return 3;
	abort();
}

bool LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	unsigned i = lock_index(lock);
	UT_ASSERT_EQ(held[i],0);
	if (native_reading) {
		if (i == 0) UT_ASSERT_EQ(mode,LW_SHARED);
		if (i >= 2) { UT_ASSERT_EQ(held[0],1); UT_ASSERT_EQ(held[1],0); UT_ASSERT_EQ(held[5-i],0); }
	}
	held[i] = 1; return true;
}

void LWLockRelease(LWLock *lock)
{
	unsigned i = lock_index(lock); UT_ASSERT_EQ(held[i],1); held[i] = 0;
}

static unsigned native_page(SlruCtl ctl, int pageno)
{
	bool offsets = ctl == MultiXactOffsetCtl;
	struct NativePage *pages = offsets ? offset_pages : member_pages;
	unsigned *count = offsets ? &noffsets : &nmembers_pages;
	for (unsigned i=0;i<*count;i++) if (pages[i].number == pageno) return i;
	UT_ASSERT(*count < (offsets ? 4 : 64));
	pages[*count].number = pageno;
	return (*count)++;
}

int SimpleLruReadPage(SlruCtl ctl, int pageno, bool write_ok, TransactionId xid)
{
	(void)write_ok; (void)xid;
	if (native_reading) {
		UT_ASSERT_EQ(held[0],1); UT_ASSERT_EQ(held[ctl==MultiXactOffsetCtl?2:3],1);
		if (ctl == MultiXactOffsetCtl) offset_reads++; else member_reads++;
		if (native_fail_at && offset_reads+member_reads == native_fail_at) pg_re_throw();
		if (ctl == MultiXactMemberCtl) {
			if (drift == 1) native_state.oldestMultiXactId++;
			if (drift == 2) native_state.oldestOffset++;
			if (drift == 3) native_epoch++;
		}
	}
	return native_page(ctl,pageno);
}

static void native_seed(uint32 count, uint32 start, MultiXactId multi)
{
	memset(&native_state,0,sizeof(native_state));
	memset(offset_pages,0,sizeof(offset_pages)); memset(member_pages,0,sizeof(member_pages));
	memset(visible,0,sizeof(visible)); memset(oldest_member,0,sizeof(oldest_member));
	memset(held,0,sizeof(held)); noffsets=nmembers_pages=0;
	native_reading=false; native_mine=true; cluster_enabled=true; cluster_node_id=0; native_fail_at=0; drift=0; native_epoch=7;
	native_state.finishedStartup=true; native_state.oldestOffsetKnown=true;
	native_state.oldestMultiXactId=multi-1; native_state.nextMXact=multi+2;
	native_state.oldestOffset=start-10; native_state.nextOffset=start+count+4;
	MultiXactState=&native_state; OldestVisibleMXactId=visible; OldestMemberMXactId=oldest_member;
	for(unsigned i=0;i<4;i++) native_offset_buffers[i]=offset_pages[i].bytes;
	for(unsigned i=0;i<64;i++) native_member_buffers[i]=member_pages[i].bytes;
	native_offset_shared.page_buffer=native_offset_buffers; native_offset_shared.page_dirty=native_offset_dirty;
	native_member_shared.page_buffer=native_member_buffers; native_member_shared.page_dirty=native_member_dirty;
	native_offset_shared.ControlLock=MultiXactOffsetSLRULock;
	native_member_shared.ControlLock=MultiXactMemberSLRULock;
	MultiXactOffsetCtl->shared=&native_offset_shared; MultiXactMemberCtl->shared=&native_member_shared;
	for(uint32 i=0;i<count;i++) { native_input[i].xid=100+i; native_input[i].status=MultiXactStatusForShare; }
	RecordNewMultiXactInto(MultiXactOffsetCtl,MultiXactMemberCtl,NULL,multi,start,count,native_input);
	/* A following allocation has initialized the reserved zero page. */
	if ((uint64)start+count >= UINT64_C(0x100000000)) (void)native_page(MultiXactMemberCtl,0);
	native_reading=true; offset_reads=member_reads=0;
}

static ClusterMxCompositionReadStatus native_read(MultiXactId multi,
	const ClusterMxCompositionNativeSource *expected,uint32 first,uint32 cap,
	ClusterMxCompositionNativeChunk *chunk)
{
	ClusterMxCompositionReadStatus result;
	offset_reads=member_reads=0;
	result=cluster_mx_composition_read_native(multi,expected,first,cap,chunk);
	UT_ASSERT(offset_reads<=2); UT_ASSERT(member_reads<=1);
	for(unsigned i=0;i<4;i++) UT_ASSERT_EQ(held[i],0);
	if(result!=MXCOMP_READ_OK) { ClusterMxCompositionNativeChunk zero={0}; UT_ASSERT(memcmp(chunk,&zero,sizeof(zero))==0); }
	return result;
}

static ClusterMxCompositionReply native_reply(const ClusterMxCompositionNativeChunk *chunk)
{
	ClusterMxCompositionReply r={0};
	r.source=&chunk->source; r.source_bytes=sizeof(chunk->source); r.total=chunk->source.span;
	r.first=chunk->first; r.next=chunk->next; r.count=chunk->count; r.cap=256;
	r.last=chunk->last; r.skipped_zero=chunk->skipped_zero; r.members=chunk->members;
	r.member_bytes=chunk->count*sizeof(*r.members); r.frame_bytes=BLCKSZ;
	return r;
}

static void native_complete(uint32 count,uint32 start,MultiXactId multi)
{
	ClusterMxCompositionReceive *op=NULL; ClusterMxCompositionReceiveStats stats={0};
	ClusterMxCompositionNativeChunk chunk; ClusterMxCompositionNativeSource saved;
	uint32 first=0, output=0, seen=0;
	const ClusterMxCompositionPage *page;
	native_seed(count,start,multi);
	do {
		ClusterMxCompositionReply r;
		ClusterMxCompositionReadStatus status=native_read(multi,first?&saved:NULL,first,256,&chunk);
		UT_ASSERT_EQ(status,MXCOMP_READ_OK); if(status!=MXCOMP_READ_OK) break;
		if(first==0) { saved=chunk.source; UT_ASSERT(cluster_mx_composition_begin(&op,&saved,sizeof(saved),saved.span,saved.start,&stats)); }
		r=native_reply(&chunk);
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op,status,&r),chunk.last?MXCOMP_COMPLETE:MXCOMP_PARTIAL);
		if(!chunk.last) { UT_ASSERT_NULL(cluster_mx_composition_pages(op,&output)); UT_ASSERT_EQ(output,0); }
		first=chunk.next;
	} while(!chunk.last);
	page=cluster_mx_composition_pages(op,&output);
	UT_ASSERT_EQ(output,count);
	for(;page;page=page->next) {
		UT_ASSERT_EQ(page->first,seen);
		for(unsigned j=0;j<page->count;j++,seen++) { UT_ASSERT_EQ(page->members[j].xid,100+seen); UT_ASSERT_EQ(page->members[j].member_status,MultiXactStatusForShare); }
	}
	UT_ASSERT_EQ(seen,count); cluster_mx_composition_finish(&op); check_discarded(op);
}

UT_TEST(test_native_sizes_and_two_offset_page_boundary)
{ native_complete(2,100,100); native_complete(256,100,2047); native_complete(257,100,2047); }
UT_TEST(test_native_large_composition_has_no_total_chunk_ceiling)
{ native_complete(65793,100,2047); }
UT_TEST(test_native_member_page_boundary_preserves_logical_order)
{ native_complete(257,1635,100); }
UT_TEST(test_native_wrap_distinguishes_valid_zero_and_placeholder)
{ native_complete(2,UINT32_MAX-1,100); native_complete(3,UINT32_MAX-1,100); }

UT_TEST(test_native_placeholder_only_tail_counts_one_physical_slot)
{
	ClusterMxCompositionNativeChunk a,b;
	native_seed(2,UINT32_MAX-1,100);
	if(native_read(100,NULL,0,256,&a)!=MXCOMP_READ_OK) { UT_ASSERT(false); return; }
	UT_ASSERT_EQ(a.source.span,3); UT_ASSERT_EQ(a.count,2); UT_ASSERT_EQ(a.next,2); UT_ASSERT(!a.last);
	UT_ASSERT_EQ(native_read(100,&a.source,2,256,&b),MXCOMP_READ_OK);
	UT_ASSERT_EQ(b.count,0); UT_ASSERT_EQ(b.next,3); UT_ASSERT(b.last); UT_ASSERT(b.skipped_zero);
}

UT_TEST(test_native_truncation_generation_and_epoch_drift_refuse)
{
	for(int field=0;field<3;field++) {
		ClusterMxCompositionNativeChunk a,b; native_seed(257,100,100);
		if(native_read(100,NULL,0,256,&a)!=MXCOMP_READ_OK) { UT_ASSERT(false); continue; }
		if(field==0) native_state.oldestMultiXactId++;
		if(field==1) native_state.oldestOffset++;
		if(field==2) native_epoch++;
		UT_ASSERT_EQ(native_read(100,&a.source,a.next,256,&b),MXCOMP_READ_UNKNOWN);
	}
	for(int field=1;field<=3;field++) {
		ClusterMxCompositionNativeChunk a; native_seed(257,100,100); drift=field;
		UT_ASSERT_EQ(native_read(100,NULL,0,256,&a),MXCOMP_READ_UNKNOWN);
	}
}

UT_TEST(test_native_unallocated_next_returns_retry_and_discards_receiver)
{
	ClusterMxCompositionNativeChunk a,b; ClusterMxCompositionReceive *op=NULL;
	ClusterMxCompositionReply r;
	unsigned slot;
	ClusterMxCompositionReceiveStats stats={0}; native_seed(257,100,100);
	if(native_read(100,NULL,0,256,&a)!=MXCOMP_READ_OK) { UT_ASSERT(false); return; }
	UT_ASSERT(cluster_mx_composition_begin(&op,&a.source,sizeof(a.source),a.source.span,a.source.start,&stats));
	r=native_reply(&a); UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r),MXCOMP_PARTIAL);
	slot=native_page(MultiXactOffsetCtl,0); ((MultiXactOffset*)offset_pages[slot].bytes)[101]=0;
	UT_ASSERT_EQ(native_read(100,&a.source,256,256,&b),MXCOMP_READ_RETRY);
	UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_RETRY,NULL),MXCOMP_UNKNOWN);
	UT_ASSERT_EQ(stats.restart_requests,1); check_discarded(op);
}

UT_TEST(test_native_errors_release_locks_and_publish_no_chunk)
{
	for(unsigned fail=1;fail<=3;fail++) {
		static ClusterMxCompositionNativeChunk chunk;
		ClusterMxCompositionNativeChunk zero={0}; volatile bool caught=false;
		native_seed(257,100,2047); native_fail_at=fail; memset(&chunk,0xa5,sizeof(chunk));
		PG_TRY(); { (void)native_read(2047,NULL,0,256,&chunk); }
		PG_CATCH(); { caught=true; }
		PG_END_TRY(); native_fail_at=0; UT_ASSERT(caught);
		UT_ASSERT(memcmp(&chunk,&zero,sizeof(zero))==0);
		for(unsigned i=0;i<4;i++) UT_ASSERT_EQ(held[i],0);
	}
}

UT_TEST(test_native_bad_members_and_unretained_ranges_refuse)
{
	for(int bad=0;bad<6;bad++) {
		ClusterMxCompositionNativeChunk chunk; unsigned slot; native_seed(2,100,100);
		slot=native_page(MultiXactMemberCtl,0);
		if(bad<2) { TransactionId xid=bad==0?InvalidTransactionId:FrozenTransactionId; memcpy(member_pages[slot].bytes+MXOffsetToMemberOffset(100),&xid,sizeof(xid)); }
		if(bad==2) member_pages[slot].bytes[MXOffsetToFlagsOffset(100)]=(char)255;
		if(bad==3) native_state.oldestOffset=101;
		if(bad==4) native_state.oldestMultiXactId=101;
		if(bad==5) native_state.oldestOffsetKnown=false;
		UT_ASSERT_EQ(native_read(100,NULL,0,256,&chunk),MXCOMP_READ_UNKNOWN);
	}
}

UT_TEST(test_native_invalid_requests_never_read_pages)
{
	for(int bad=0;bad<5;bad++) {
		ClusterMxCompositionNativeChunk chunk; native_seed(2,100,100);
		if(bad==4) native_mine=false;
		UT_ASSERT_EQ(native_read(bad==3?0:100,NULL,bad==2?1:0,bad==0?0:bad==1?257:256,&chunk),MXCOMP_READ_UNKNOWN);
		UT_ASSERT_EQ(offset_reads+member_reads,0);
	}
}

UT_TEST(test_receiver_refuses_forged_placeholder_and_wrong_physical_advance)
{
	for(int bad=0;bad<3;bad++) {
		ClusterMxCompositionReceiveStats stats={0}; ClusterMxCompositionReceive *op=begin(4,&stats);
		ClusterCurrentMxMemberDesc m[2]; ClusterMxCompositionReply r=reply(4,0,2,m);
		if(bad==0) { r.skipped_zero=true; r.count=1; r.member_bytes=sizeof(*m); }
		if(bad==1) { r.count=0; r.member_bytes=0; }
		if(bad==2) r.next=3;
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r),MXCOMP_UNKNOWN); check_discarded(op);
	}
}

UT_TEST(test_native_reused_chunk_and_latest_zero_endpoint)
{
 ClusterMxCompositionNativeChunk chunk;
 native_seed(257,100,100);
 if(native_read(100,NULL,0,256,&chunk)!=MXCOMP_READ_OK) { UT_ASSERT(false); return; }
 UT_ASSERT_EQ(native_read(100,&chunk.source,256,256,&chunk),MXCOMP_READ_OK);
 UT_ASSERT_EQ(chunk.count,1); UT_ASSERT(chunk.last);
 native_seed(2,UINT32_MAX-1,100);
 native_state.nextMXact=101; native_state.nextOffset=0;
 UT_ASSERT_EQ(native_read(100,NULL,0,256,&chunk),MXCOMP_READ_OK);
 UT_ASSERT_EQ(chunk.source.span,2); UT_ASSERT_EQ(chunk.count,2); UT_ASSERT(chunk.last);
}

UT_TEST(test_native_unqualified_local_context_is_unknown)
{
 for(int bad=0;bad<3;bad++) {
  ClusterMxCompositionNativeChunk chunk; native_seed(2,100,100);
  if(bad==0) cluster_enabled=false;
  if(bad==1) cluster_node_id=-1;
  if(bad==2) native_epoch=0;
  UT_ASSERT_EQ(native_read(100,NULL,0,256,&chunk),MXCOMP_READ_UNKNOWN);
 }
 cluster_enabled=true; cluster_node_id=0; native_epoch=7;
}

int main(void)
{
	UT_PLAN(27);
	UT_RUN(test_complete_2_256_257_without_partial_output);
	UT_RUN(test_totals_and_chunk_ordinals_exceed_old_uint16_and_uint8);
	UT_RUN(test_large_composition_does_not_require_one_large_seen_array);
	UT_RUN(test_adversarial_hash_collisions_remain_within_small_allocations);
	UT_RUN(test_bad_continuations_discard_the_whole_prefix);
	UT_RUN(test_exact_source_copy_rejects_every_changed_byte);
	UT_RUN(test_bad_members_duplicates_and_second_updater_discard_everything);
	UT_RUN(test_later_non_ok_null_and_extra_tail_are_not_success);
	UT_RUN(test_explicit_restart_from_zero_preserves_diagnostics);
	UT_RUN(test_source_snapshot_is_owned_and_finish_is_idempotent);
	UT_RUN(test_missing_tail_and_active_rebegin_discard_without_hidden_retry);
	UT_RUN(test_invalid_total_source_and_large_native_domain);
	UT_RUN(test_begin_allocation_errors_leave_no_handle_or_memory);
	UT_RUN(test_later_page_and_hash_growth_errors_discard_prefix);
	UT_RUN(test_native_sizes_and_two_offset_page_boundary);
	UT_RUN(test_native_large_composition_has_no_total_chunk_ceiling);
	UT_RUN(test_native_member_page_boundary_preserves_logical_order);
	UT_RUN(test_native_wrap_distinguishes_valid_zero_and_placeholder);
	UT_RUN(test_native_placeholder_only_tail_counts_one_physical_slot);
	UT_RUN(test_native_truncation_generation_and_epoch_drift_refuse);
	UT_RUN(test_native_unallocated_next_returns_retry_and_discards_receiver);
	UT_RUN(test_native_errors_release_locks_and_publish_no_chunk);
	UT_RUN(test_native_bad_members_and_unretained_ranges_refuse);
	UT_RUN(test_native_invalid_requests_never_read_pages);
	UT_RUN(test_receiver_refuses_forged_placeholder_and_wrong_physical_advance);
	UT_RUN(test_native_reused_chunk_and_latest_zero_endpoint);
	UT_RUN(test_native_unqualified_local_context_is_unknown);
	UT_DONE(); return ut_failed_count?1:0;
}
