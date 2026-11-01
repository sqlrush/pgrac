/* Composition continuity/lifetime only; no storage or transaction authority. */
#include "postgres.h"
#include "cluster/cluster_multixact_composition.h"
#include "common/hashfn.h"
#include "utils/memutils.h"
#include "unit_test.h"
#include <stdlib.h>

UT_DEFINE_GLOBALS();
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
MemoryContext CurrentMemoryContext = (MemoryContext) 1;
static unsigned allocations, fail_at, live_contexts, live_blocks;
static Size allocation_ceiling = MaxAllocSize;
typedef struct CompositionTestBlock { struct CompositionTestBlock *next; void *ptr; } CompositionTestBlock;
typedef struct Arena { struct Arena *next; CompositionTestBlock *blocks; } Arena;
static Arena *arenas;

void pg_re_throw(void) { if (PG_exception_stack) siglongjmp(*PG_exception_stack, 1); abort(); }
void ExceptionalCondition(const char *c, const char *f, int l) { fprintf(stderr,"%s %s:%d\n",c,f,l); abort(); }
bool errstart(int level, const char *domain) { (void)domain; if (level != ERROR) abort(); return true; }
bool errstart_cold(int level, const char *domain) { return errstart(level,domain); }
int errcode(int code) { return code; }
int errmsg(const char *fmt, ...) { (void)fmt; return 0; }
int errmsg_internal(const char *fmt, ...) { (void)fmt; return 0; }
void errfinish(const char *file, int line, const char *func) { (void)file; (void)line; (void)func; pg_re_throw(); }

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size min, Size init, Size max)
{
	Arena *a = calloc(1, sizeof(*a));
	(void)parent; (void)name; (void)min; (void)init; (void)max;
	if (!a) abort();
	a->next = arenas; arenas = a; live_contexts++;
	return (MemoryContext)a;
}

void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	Arena *a = (Arena *)context;
	CompositionTestBlock *b;
	if (++allocations == fail_at) pg_re_throw();
	if (size > allocation_ceiling) pg_re_throw();
	b = malloc(sizeof(*b)); if (!b) abort();
	b->ptr = calloc(1, size); if (!b->ptr) abort();
	b->next = a->blocks; a->blocks = b; live_blocks++;
	return b->ptr;
}

void *MemoryContextAlloc(MemoryContext context, Size size) { return MemoryContextAllocZero(context,size); }

void
pfree(void *ptr)
{
	for (Arena *a = arenas; a; a = a->next)
		for (CompositionTestBlock **b = &a->blocks; *b; b = &(*b)->next)
			if ((*b)->ptr == ptr) {
				CompositionTestBlock *old = *b; *b = old->next;
				free(old->ptr); free(old); live_blocks--; return;
			}
	abort();
}

void
MemoryContextDelete(MemoryContext context)
{
	Arena *a = (Arena *)context;
	Arena **link = &arenas;
	while (*link && *link != a) link = &(*link)->next;
	if (!*link) abort();
	*link = a->next;
	while (a->blocks) {
		CompositionTestBlock *b = a->blocks; a->blocks = b->next;
		free(b->ptr); free(b); live_blocks--;
	}
	free(a); live_contexts--;
}

static const uint8 source[64] = { 1, 7, 19, 23 };

static ClusterMxCompositionReceive *
begin(uint32 total, ClusterMxCompositionReceiveStats *stats)
{
	ClusterMxCompositionReceive *op = NULL;
	UT_ASSERT(cluster_mx_composition_begin(&op, source, sizeof(source), total, 1, stats));
	return op;
}

static ClusterMxCompositionReply
reply(uint32 total, uint32 first, uint32 count, ClusterCurrentMxMemberDesc *members)
{
	ClusterMxCompositionReply r = { 0 };
	r.source = source; r.source_bytes = sizeof(source);
	r.total = total; r.first = first; r.count = count; r.next = first + count;
	r.cap = 256; r.last = r.next == total;
	r.members = members; r.member_bytes = count * sizeof(*members); r.frame_bytes = 4096;
	for (uint32 i = 0; i < count; i++) {
		memset(&members[i],0,sizeof(members[i])); members[i].xid = 100 + first + i;
	}
	return r;
}

static void
check_discarded(ClusterMxCompositionReceive *op)
{
	uint32 total = 99;
	UT_ASSERT_NULL(op);
	UT_ASSERT_NULL(cluster_mx_composition_pages(op, &total));
	UT_ASSERT_EQ(total,0);
	UT_ASSERT_EQ(live_contexts,0);
	UT_ASSERT_EQ(live_blocks,0);
}

static void
complete(uint32 total)
{
	ClusterMxCompositionReceiveStats stats = { 0 };
	ClusterMxCompositionReceive *op = begin(total,&stats);
	ClusterCurrentMxMemberDesc members[256];
	uint32 output_count = 99, seen = 0;
	const ClusterMxCompositionPage *page;
	if (!op) return;
	for (uint32 first = 0; first < total;) {
		uint32 count = Min(total-first,256);
		ClusterMxCompositionReply r = reply(total,first,count,members);
		if (r.last) members[count-1].member_status = MultiXactStatusUpdate;
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r),
			r.last ? MXCOMP_COMPLETE : MXCOMP_PARTIAL);
		UT_ASSERT_EQ(cluster_mx_composition_next(op),first+count);
		if (!r.last) {
			UT_ASSERT_NULL(cluster_mx_composition_pages(op,&output_count));
			UT_ASSERT_EQ(output_count,0);
		}
		first += count;
	}
	page = cluster_mx_composition_pages(op,&output_count);
	UT_ASSERT_NOT_NULL(page); UT_ASSERT_EQ(output_count,total);
	for (; page; page=page->next) {
		UT_ASSERT_EQ(page->first,seen);
		for (uint32 i=0;i<page->count;i++,seen++) {
			UT_ASSERT_EQ(page->members[i].xid,100+seen);
			UT_ASSERT_EQ(page->members[i].member_status,
				seen+1==total ? MultiXactStatusUpdate : MultiXactStatusForKeyShare);
		}
	}
	UT_ASSERT_EQ(seen,total);
	UT_ASSERT_EQ(stats.accepted_frames,((uint64)total+255)/256);
	UT_ASSERT_EQ(stats.unknown_results+stats.restart_requests,0);
	cluster_mx_composition_finish(&op); cluster_mx_composition_finish(&op);
	check_discarded(op);
}

UT_TEST(test_complete_2_256_257_without_partial_output) { complete(2); complete(256); complete(257); }
UT_TEST(test_totals_and_chunk_ordinals_exceed_old_uint16_and_uint8) { complete(65793); }

UT_TEST(test_large_composition_does_not_require_one_large_seen_array)
{
	volatile bool caught=false;
	allocation_ceiling=3072;
	PG_TRY(); { complete(65793); }
	PG_CATCH(); { caught=true; }
	PG_END_TRY();
	allocation_ceiling=MaxAllocSize;
	UT_ASSERT(!caught); check_discarded(NULL);
}

UT_TEST(test_adversarial_hash_collisions_remain_within_small_allocations)
{
	ClusterMxCompositionReceiveStats stats={0};
	ClusterMxCompositionReceive *volatile op=NULL;
	ClusterCurrentMxMemberDesc m[40]; ClusterMxCompositionReply r=reply(40,0,40,m);
	volatile bool caught=false;
	unsigned n=0; uint32 total=0;
	/* All are in one partition and collide through the low 10 hash bits. */
	for(uint32 i=1;i<(UINT32_C(1)<<22) && n<40;i++)
		if((murmurhash32(i<<10)&1023)==0) m[n++].xid=i<<10;
	UT_ASSERT_EQ(n,40);
	allocation_ceiling=4096;
	PG_TRY(); {
		(void)cluster_mx_composition_begin((ClusterMxCompositionReceive **)&op,source,sizeof(source),40,1,&stats);
		UT_ASSERT_EQ(cluster_mx_composition_accept((ClusterMxCompositionReceive **)&op,MXCOMP_READ_OK,&r),MXCOMP_COMPLETE);
	}
	PG_CATCH(); { caught=true; }
	PG_END_TRY();
	allocation_ceiling=MaxAllocSize;
	UT_ASSERT(!caught);
	if(op) {
		const ClusterMxCompositionPage *page=cluster_mx_composition_pages(op,&total);
		UT_ASSERT_NOT_NULL(page); UT_ASSERT_EQ(total,40);
		if(page) for(unsigned i=0;i<40;i++) UT_ASSERT_EQ(page->members[i].xid,m[i].xid);
	}
	cluster_mx_composition_finish((ClusterMxCompositionReceive **)&op); check_discarded(op);
}

UT_TEST(test_bad_continuations_discard_the_whole_prefix)
{
	for (int scenario=0;scenario<17;scenario++) {
		ClusterMxCompositionReceiveStats stats={0};
		ClusterMxCompositionReceive *op=begin(4,&stats);
		ClusterCurrentMxMemberDesc m[256];
		ClusterMxCompositionReply r;
		if (!op) continue;
		r=reply(4,0,2,m);
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r),MXCOMP_PARTIAL);
		r=reply(4,2,2,m);
		switch(scenario) {
			case 0: r.first=3; break;
			case 1: r.first=0; break;
			case 2: r.first=1; break;
			case 3: r.next=UINT32_MAX; break;
			case 4: r.last=false; break;
			case 5: r.total=5; break;
			case 6: r.cap=0; break;
			case 7: r.cap=257; break;
			case 8: r.count=0; break;
			case 9: r.count=257; break;
			case 10: r.member_bytes--; break;
			case 11: r.frame_bytes=BLCKSZ+1; break;
			case 12: r.source=NULL; break;
			case 13: r.source_bytes--; break;
			case 14: r.members=NULL; break;
			case 15: r.count=1; r.next=3; r.member_bytes=sizeof(*m); break;
			case 16: r.first=UINT32_MAX; r.next=1; break;
		}
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r),MXCOMP_UNKNOWN);
		check_discarded(op); UT_ASSERT_EQ(stats.unknown_results,1);
	}
}

UT_TEST(test_exact_source_copy_rejects_every_changed_byte)
{
	for (unsigned byte=0;byte<sizeof(source);byte++) {
		ClusterMxCompositionReceiveStats stats={0};
		ClusterMxCompositionReceive *op=begin(4,&stats);
		ClusterCurrentMxMemberDesc m[2]; uint8 drift[sizeof(source)];
		ClusterMxCompositionReply r;
		if (!op) continue;
		r=reply(4,0,2,m); (void)cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r);
		r=reply(4,2,2,m); memcpy(drift,source,sizeof(drift)); drift[byte]^=1; r.source=drift;
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r),MXCOMP_UNKNOWN);
		check_discarded(op);
	}
}

UT_TEST(test_bad_members_duplicates_and_second_updater_discard_everything)
{
	for (int scenario=0;scenario<7;scenario++) {
		ClusterMxCompositionReceiveStats stats={0};
		ClusterMxCompositionReceive *op=begin(4,&stats);
		ClusterCurrentMxMemberDesc m[2]; ClusterMxCompositionReply r;
		if (!op) continue;
		r=reply(4,0,2,m);
		if(scenario==6) m[0].member_status=MultiXactStatusUpdate;
		(void)cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r);
		r=reply(4,2,2,m);
		switch(scenario) {
			case 0: m[0].xid=InvalidTransactionId; break;
			case 1: m[0].member_status=MaxMultiXactStatus+1; break;
			case 2: m[0].reserved8[2]=1; break;
			case 3: m[1].xid=m[0].xid; break;
			case 4: m[0].xid=100; break;
			case 5: m[0].xid=101; m[0].member_status=MultiXactStatusForUpdate; break;
			case 6: m[0].member_status=MultiXactStatusNoKeyUpdate; break;
		}
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r),MXCOMP_UNKNOWN);
		check_discarded(op);
	}
}

UT_TEST(test_later_non_ok_null_and_extra_tail_are_not_success)
{
	for (int scenario=0;scenario<5;scenario++) {
		ClusterMxCompositionReceiveStats stats={0};
		ClusterMxCompositionReceive *op=begin(scenario==4?2:4,&stats);
		ClusterCurrentMxMemberDesc m[2];
		ClusterMxCompositionReply r;
		if(!op) continue;
		r=reply(scenario==4?2:4,0,2,m); (void)cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r);
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op,
			scenario==0?MXCOMP_READ_RETRY:scenario==1?MXCOMP_READ_UNKNOWN:scenario==2?(ClusterMxCompositionReadStatus)77:MXCOMP_READ_OK,
			scenario==3?NULL:&r),MXCOMP_UNKNOWN);
		check_discarded(op); UT_ASSERT_EQ(stats.unknown_results,1);
		UT_ASSERT_EQ(stats.restart_requests,scenario==0?1:0);
	}
}

UT_TEST(test_explicit_restart_from_zero_preserves_diagnostics)
{
	ClusterMxCompositionReceiveStats stats={0};
	ClusterMxCompositionReceive *op=begin(4,&stats);
	ClusterCurrentMxMemberDesc m[2]; ClusterMxCompositionReply r;
	if(!op) return;
	r=reply(4,0,2,m); (void)cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r);
	(void)cluster_mx_composition_accept(&op,MXCOMP_READ_RETRY,NULL);
	check_discarded(op);
	UT_ASSERT(cluster_mx_composition_begin(&op,source,sizeof(source),4,1,&stats));
	UT_ASSERT_EQ(cluster_mx_composition_next(op),0);
	r=reply(4,0,2,m); (void)cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r);
	r=reply(4,2,2,m); UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r),MXCOMP_COMPLETE);
	UT_ASSERT_EQ(stats.accepted_frames,3); UT_ASSERT_EQ(stats.unknown_results,1); UT_ASSERT_EQ(stats.restart_requests,1);
	cluster_mx_composition_finish(&op); check_discarded(op);
}

UT_TEST(test_source_snapshot_is_owned_and_finish_is_idempotent)
{
	ClusterMxCompositionReceiveStats stats={0};
	ClusterMxCompositionReceive *op=NULL;
	ClusterCurrentMxMemberDesc m[2]; ClusterMxCompositionReply r=reply(2,0,2,m);
	uint8 mutable_source[sizeof(source)];
	memcpy(mutable_source,source,sizeof(source));
	UT_ASSERT(cluster_mx_composition_begin(&op,mutable_source,sizeof(source),2,1,&stats));
	if(!op) return;
	memset(mutable_source,0xa5,sizeof(mutable_source));
	UT_ASSERT_EQ(cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r),MXCOMP_COMPLETE);
	cluster_mx_composition_finish(&op); cluster_mx_composition_finish(&op); cluster_mx_composition_finish(NULL);
	check_discarded(op);
}

UT_TEST(test_missing_tail_and_active_rebegin_discard_without_hidden_retry)
{
	ClusterMxCompositionReceiveStats stats={0};
	ClusterMxCompositionReceive *op=begin(4,&stats);
	ClusterCurrentMxMemberDesc m[2]; ClusterMxCompositionReply r;
	if(!op) return;
	r=reply(4,0,2,m); (void)cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r);
	cluster_mx_composition_finish(&op); cluster_mx_composition_finish(&op);
	check_discarded(op); UT_ASSERT_EQ(stats.unknown_results,1);
	UT_ASSERT(cluster_mx_composition_begin(&op,source,sizeof(source),4,1,&stats));
	r=reply(4,0,2,m); (void)cluster_mx_composition_accept(&op,MXCOMP_READ_OK,&r);
	UT_ASSERT(!cluster_mx_composition_begin(&op,source,sizeof(source),4,1,&stats));
	check_discarded(op); UT_ASSERT_EQ(stats.unknown_results,2);
	UT_ASSERT_EQ(stats.restart_requests,0); UT_ASSERT_EQ(stats.accepted_frames,2);
}

UT_TEST(test_invalid_total_source_and_large_native_domain)
{
	const uint32 totals[]={0,UINT32_MAX,(uint32)INT32_MAX+1};
	for(unsigned i=0;i<lengthof(totals);i++) {
		ClusterMxCompositionReceiveStats stats={0}; ClusterMxCompositionReceive *op=NULL;
		UT_ASSERT(!cluster_mx_composition_begin(&op,source,sizeof(source),totals[i],1,&stats)); check_discarded(op);
	}
	for(int bad=0;bad<3;bad++) {
		ClusterMxCompositionReceiveStats stats={0}; ClusterMxCompositionReceive *op=NULL;
		UT_ASSERT(!cluster_mx_composition_begin(&op,bad==0?NULL:source,bad==1?0:BLCKSZ+1,2,1,&stats)); check_discarded(op);
	}
	{
		ClusterMxCompositionReceiveStats stats={0}; ClusterMxCompositionReceive *op=begin(INT32_MAX,&stats);
		UT_ASSERT_NOT_NULL(op); UT_ASSERT_EQ(cluster_mx_composition_next(op),0);
		cluster_mx_composition_finish(&op); check_discarded(op);
	}
}

UT_TEST(test_begin_allocation_errors_leave_no_handle_or_memory)
{
	static ClusterMxCompositionReceiveStats stats;
	for(unsigned i=1;i<=5;i++) {
		ClusterMxCompositionReceive *volatile op=NULL; volatile bool caught=false;
		memset(&stats,0,sizeof(stats));
		fail_at=allocations+i;
		PG_TRY(); { (void)cluster_mx_composition_begin((ClusterMxCompositionReceive **)&op,source,sizeof(source),257,1,&stats); }
		PG_CATCH(); { caught=true; }
		PG_END_TRY(); fail_at=0;
		UT_ASSERT(caught); check_discarded(op);
	}
}

UT_TEST(test_later_page_and_hash_growth_errors_discard_prefix)
{
	static ClusterMxCompositionReceiveStats stats;
	for(unsigned i=1;i<=2;i++) {
		ClusterMxCompositionReceive *volatile op;
		ClusterCurrentMxMemberDesc m[256]; ClusterMxCompositionReply r;
		volatile bool caught=false;
		memset(&stats,0,sizeof(stats)); op=begin(29696,&stats);
		if(!op) continue;
		/* 28 entries/low-10-bit partition, then the 29th forces growth. */
		for(uint32 first=0;first<28672;first+=256) {
			r=reply(29696,first,256,m);
			(void)cluster_mx_composition_accept((ClusterMxCompositionReceive **)&op,MXCOMP_READ_OK,&r);
		}
		r=reply(29696,28672,256,m); fail_at=allocations+i;
		PG_TRY(); { (void)cluster_mx_composition_accept((ClusterMxCompositionReceive **)&op,MXCOMP_READ_OK,&r); }
		PG_CATCH(); { caught=true; }
		PG_END_TRY(); fail_at=0;
		UT_ASSERT(caught); check_discarded(op); UT_ASSERT_EQ(stats.unknown_results,1);
	}
}

int main(void)
{
	UT_PLAN(14);
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
	UT_DONE(); return ut_failed_count?1:0;
}
