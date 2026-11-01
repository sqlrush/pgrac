/* Actual snapshot containers, retention publisher, admission seqlock/policy
 * and history capture. Physical buffer/PCM/canonical publication and allocator
 * leaves are controlled; none of the evidence/verifiers return fixture success.
 * Portions Copyright (c) 2026, pgrac contributors */
int snapshot_fixture_unused_main(void);
#define main snapshot_fixture_unused_main
#include "test_cluster_snapshot_admission.c"
#undef main
#include "cluster/cluster_mx_history_graph.h"
#include "storage/backendid.h"
#include "cluster/cluster_tt_local.h"
#include "storage/ipc.h"
#include "test_cluster_active_sample_types.inc"

/* Transport is unavailable in this native snapshot fixture. It never returns
 * a fabricated reserved request, landing, member proof or grant. The separate
 * graph test drives the actual routed factory/queue/landing. */
BackendId MyBackendId = 1;
static unsigned route_attempts;
ClusterMxHistoryStep
cluster_gcs_block_mx_history_prepare_routed(const ClusterMxHistoryRequest *r,
											ClusterMxHistoryRequestWire *out)
{
	route_attempts++;
	memset(out, 0, sizeof(*out));
	return CMXH_PENDING;
}
bool
cluster_gcs_block_mx_history_submit(const ClusterMxHistoryRequestWire *r)
{
	abort();
}
ClusterMxHistoryStep
cluster_gcs_block_mx_history_poll(const ClusterMxHistoryRequestWire *r,
								  ClusterMxHistoryPeerResult *out)
{
	abort();
}
void
cluster_gcs_block_mx_history_cancel(const ClusterMxHistoryRequestWire *r)
{
	abort();
}
static ClusterSemanticActivationShmem history_gate;
static ClusterSemanticActivationShmem *SemanticActivationShmem = &history_gate;
static const void *SemanticActivationDescriptors[64];
static bool serving = true;
static bool
normal_start_serving_allowed(void)
{
	return serving;
}
static bool
normal_start_bootstrap_allowed(void)
{
	return false;
}
#include "test_cluster_mx_history_admission.inc"
#include "test_cluster_heap_freeze_admission_current.inc"

int NBuffers = 1;
int NLocBuffer;
static BufferDescPadded descriptor;
BufferDescPadded *BufferDescriptors = &descriptor;
static union {
	max_align_t alignment;
	char bytes[BLCKSZ];
} page_storage;
char *BufferBlocks = page_storage.bytes;
TimestampTz MyStartTimestamp = 12345;
volatile uint32 CritSectionCount;
static bool pinned = true, content_locked = true, exclusive = true, oom;
static ClusterPcmOwnSnapshot pcm_source;
static FullTransactionId top_source;
static ClusterCanonicalTxnBinding canonical_source;
static ClusterCanonicalTxnPublishState publication_source;
static ResourceReleaseCallback cleanup_callback;
static void *cleanup_argument;
static pg_on_exit_callback exit_callback;
static Datum exit_argument;
static RelationData relation;
static HeapTupleData tuple;
static ClusterCurrentMxKey mxkey = { 0, 0, 99, 7, 0 };
static ClusterCurrentMxMemberDesc members[2]
	= { { 101, MultiXactStatusForShare, { 0 } }, { 102, MultiXactStatusForKeyShare, { 0 } } };
static ClusterSemanticAdmissionToken admission;
static unsigned canonical_reads, owner_reads;

void *
MemoryContextAllocExtended(MemoryContext context, Size size, int flags)
{
	if (oom)
		return NULL;
	return MemoryContextAlloc(context, size);
}
bool
BufferIsPinnedByMe(Buffer b)
{
	return pinned && b == 1;
}
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return content_locked && lock == BufferDescriptorGetContentLock(&descriptor.bufferdesc)
		   && ((mode == LW_EXCLUSIVE) == exclusive);
}
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *b, ClusterPcmOwnSnapshot *out)
{
	Assert(b == &descriptor.bufferdesc);
	*out = pcm_source;
	return CLUSTER_PCM_OWN_OK;
}
FullTransactionId
GetTopFullTransactionIdIfAny(void)
{
	owner_reads++;
	return top_source;
}
ClusterCanonicalTxnPublishState
cluster_tt_local_capture_canonical_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	canonical_reads++;
	Assert(xid == XidFromFullTransactionId(top_source));
	*out = canonical_source;
	return publication_source;
}
void
RegisterResourceReleaseCallback(ResourceReleaseCallback cb, void *arg)
{
	cleanup_callback = cb;
	cleanup_argument = arg;
}
void
before_shmem_exit(pg_on_exit_callback cb, Datum arg)
{
	exit_callback = cb;
	exit_argument = arg;
}

static void
reset_page(void)
{
	PageHeader page = (PageHeader)BufferBlocks;
	ItemId lp;
	memset(BufferBlocks, 0, BLCKSZ);
	page->pd_flags = PD_HAS_ITL;
	page->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	page->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	page->pd_upper = page->pd_special - MAXALIGN(SizeofHeapTupleHeader);
	PageSetPageSizeAndVersion((Page)page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	PageXLogRecPtrSet(page->pd_lsn, UINT64CONST(77));
	lp = PageGetItemId((Page)page, 1);
	ItemIdSetNormal(lp, page->pd_upper, SizeofHeapTupleHeader);
	memset(&tuple, 0, sizeof(tuple));
	tuple.t_data = (HeapTupleHeader)PageGetItem((Page)page, lp);
	tuple.t_len = SizeofHeapTupleHeader;
	tuple.t_tableOid = relation.rd_id;
	ItemPointerSet(&tuple.t_self, 3, 1);
	tuple.t_data->t_hoff = SizeofHeapTupleHeader;
	tuple.t_data->t_infomask = HEAP_XMAX_IS_MULTI;
	HeapTupleHeaderSetXmax(tuple.t_data, 99);
	for (int i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; ++i)
		ClusterPageGetItlSlots((Page)page)[i].flags = ITL_FLAG_FREE;
	ClusterPageGetItlSlots((Page)page)[0].xid = 101;
	ClusterPageGetItlSlots((Page)page)[0].flags = ITL_FLAG_ACTIVE;
}
static Snapshot
setup(void)
{
	Snapshot s;
	Assert(remembered == 0 && pairingheap_is_empty(&RegisteredSnapshots));
	CurrentResourceOwner = (ResourceOwner)1;
	memset(&relation, 0, sizeof(relation));
	relation.rd_id = 20000;
	relation.rd_locator = (RelFileLocator){ 1663, 5, 20000 };
	memset(&descriptor, 0, sizeof(descriptor));
	InitBufferTag(&descriptor.bufferdesc.tag, &relation.rd_locator, MAIN_FORKNUM, 3);
	memset(&pcm_source, 0, sizeof(pcm_source));
	pcm_source.tag = descriptor.bufferdesc.tag;
	pcm_source.pcm_state = PCM_STATE_X;
	pcm_source.generation = 2;
	pcm_source.reservation_token = 3;
	pcm_source.buffer_type = BUF_TYPE_XCUR;
	pcm_source.semantic_buf_state = BM_TAG_VALID | BM_VALID;
	memset(&history_gate, 0, sizeof(history_gate));
	pg_atomic_init_u64(&history_gate.admission_seq, 2);
	pg_atomic_init_u64(&history_gate.active_bits, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	pg_atomic_init_u64(&history_gate.record_generation, 9);
	pg_atomic_init_u64(&history_gate.formation_epoch, 7);
	pg_atomic_init_u32(&history_gate.transition_closed, 0);
	for (int i = 0; i < 64; ++i)
		SemanticActivationDescriptors[i] = &history_gate;
	admission = (ClusterSemanticAdmissionToken){ CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 9, 7,
												 CLUSTER_SEMANTIC_TARGET_SIDE, true };
	top_source = InvalidFullTransactionId;
	publication_source = CLUSTER_CANONICAL_TXN_NONE;
	memset(&canonical_source, 0, sizeof(canonical_source));
	canonical_reads = owner_reads = 0;
	serving = pinned = content_locked = exclusive = true;
	oom = false;
	CritSectionCount = 0;
	reset_page();
	s = registered(100);
	return s;
}
static bool
begin(Snapshot s, ClusterMxHistoryCollector **c)
{
	return cluster_mx_history_capture_begin(&relation, 1, &tuple, s, &mxkey, members, 2, &admission,
											c);
}
static void
clean(ClusterMxHistoryCollector *c, Snapshot s)
{
	cluster_mx_history_cancel(c);
	cluster_mx_history_cancel(c);
	if (s)
		UnregisterSnapshot(s);
	UT_ASSERT_EQ(remembered, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
}
static void
real_capture_and_relock(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	ClusterMxHistoryCapture view;
	bool ok = begin(s, &c);
	UT_ASSERT(ok && c != NULL);
	if (ok) {
		UT_ASSERT_EQ(remembered, 2);
		UT_ASSERT(cluster_mx_history_capture_read(c, &view));
		UT_ASSERT_EQ(view.read_scn, 100);
		UT_ASSERT_EQ(view.retained_floor, 100);
		UT_ASSERT(view.collector_id && view.capture_generation && view.snapshot_identity);
		UT_ASSERT_EQ(view.backend_incarnation, MyStartTimestamp);
		UT_ASSERT_EQ(view.requester_owner.flags, 0);
		UT_ASSERT_EQ(view.nmembers, 2);
		UT_ASSERT_EQ(memcmp(view.members, members, sizeof(members)), 0);
		UT_ASSERT_EQ(memcmp(view.roots, ClusterPageGetItlSlots(BufferBlocks), sizeof(view.roots)),
					 0);
		content_locked = false;
		UT_ASSERT(cluster_mx_history_capture_read(c, &view));
		content_locked = true;
		UT_ASSERT(cluster_mx_history_capture_recheck(c, &relation, 1, &tuple, &mxkey, members, 2));
	}
	clean(c, s);
}
static void
snapshot_own_reference_keeps_original_floor(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	ClusterMxHistoryCapture view;
	bool ok = begin(s, &c);
	UT_ASSERT(ok);
	UnregisterSnapshot(s);
	s = NULL;
	if (ok) {
		UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 100);
		UT_ASSERT(cluster_mx_history_capture_read(c, &view));
	}
	clean(c, s);
}
static void
original_scope_restored_and_older_floor_preserved(void)
{
	Snapshot s = setup(), older = registered(40);
	ClusterSnapshotReadScopeV1 outer;
	ClusterMxHistoryCollector *c = NULL;
	ClusterMxHistoryCapture view;
	Snapshot actual;
	SCN floor;
	const char *reason;
	bool ok;
	cluster_snapshot_read_enter_v1(&outer, older);
	ok = begin(s, &c);
	UT_ASSERT(ok);
	if (ok) {
		UT_ASSERT(cluster_mx_history_capture_read(c, &view));
		UT_ASSERT_EQ(view.retained_floor, 40);
	}
	UT_ASSERT(cluster_snapshot_read_evidence_v1(40, &actual, &floor, &reason));
	UT_ASSERT(actual == older);
	cluster_mx_history_cancel(c);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 40);
	cluster_snapshot_read_exit_v1(&outer);
	UnregisterSnapshot(older);
	clean(c, s);
}
static void
forged_and_retired_snapshots_refused(void)
{
	Snapshot s = setup();
	SnapshotData fake = *s;
	ClusterMxHistoryCollector *c = (void *)1;
	fake.regd_count = fake.active_count = 999;
	UT_ASSERT(!begin(&fake, &c));
	UT_ASSERT(c == NULL);
	UnregisterSnapshot(s);
	UT_ASSERT(!begin(s, &c));
	UT_ASSERT(c == NULL);
	clean(c, NULL);
}
static void
source_changes_cancel_whole_capture(void)
{
	for (unsigned fault = 0; fault < 10; ++fault) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		ClusterMxHistoryCapture view, zero = { 0 };
		bool ok = begin(s, &c);
		UT_ASSERT(ok);
		if (ok) {
			switch (fault) {
			case 0:
				s->read_scn++;
				break;
			case 1:
				s->read_epoch++;
				break;
			case 2:
				s->cluster_source = SNAPSHOT_SOURCE_LOCAL;
				break;
			case 3:
				CurrentResourceOwner = (ResourceOwner)2;
				break;
			case 4:
				pg_atomic_write_u64(&proc.cluster_read_scn_atomic, 101);
				break;
			case 5:
				pg_atomic_write_u64(&history_gate.record_generation, 10);
				break;
			case 6:
				pg_atomic_write_u32(&history_gate.transition_closed, 1);
				break;
			case 7:
				serving = false;
				break;
			case 8:
				MyStartTimestamp++;
				break;
			case 9:
				top_source = FullTransactionIdFromEpochAndXid(1, 100);
				publication_source = CLUSTER_CANONICAL_TXN_PUBLISHING;
				break;
			}
			memset(&view, 0xa5, sizeof(view));
			UT_ASSERT(!cluster_mx_history_capture_read(c, &view));
			UT_ASSERT_EQ(memcmp(&view, &zero, sizeof(view)), 0);
			CurrentResourceOwner = (ResourceOwner)1;
			MyStartTimestamp = 12345;
		}
		clean(c, s);
	}
}
static void
complete_heap_fingerprint_rechecked(void)
{
	for (unsigned fault = 0; fault < 20; ++fault) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		ClusterCurrentMxMemberDesc changed[2];
		ClusterCurrentMxKey key = mxkey;
		Relation r = &relation;
		Buffer b = 1;
		bool ok = begin(s, &c);
		UT_ASSERT(ok);
		memcpy(changed, members, sizeof(changed));
		if (ok) {
			switch (fault) {
			case 0:
				PageXLogRecPtrSet(((PageHeader)BufferBlocks)->pd_lsn, UINT64CONST(78));
				break;
			case 1:
				tuple.t_data->t_infomask2++;
				break;
			case 2:
				HeapTupleHeaderSetXmax(tuple.t_data, 100);
				break;
			case 3:
				ClusterPageGetItlSlots(BufferBlocks)[7].wrap++;
				break;
			case 4:
				pcm_source.generation++;
				break;
			case 5:
				pcm_source.reservation_token++;
				break;
			case 6:
				pcm_source.flags++;
				break;
			case 7:
				pcm_source.resource_x_activation_generation++;
				break;
			case 8:
				relation.rd_locator.relNumber++;
				break;
			case 9:
				tuple.t_self.ip_posid++;
				break;
			case 10:
				changed[0].member_status++;
				break;
			case 11: {
				ClusterCurrentMxMemberDesc tmp = changed[0];
				changed[0] = changed[1];
				changed[1] = tmp;
				break;
			}
			case 12:
				key.multixact_id++;
				break;
			case 13:
				ItemIdSetDead(PageGetItemId(BufferBlocks, 1));
				break;
			case 14:
				tuple.t_len++;
				break;
			case 15:
				pinned = false;
				break;
			case 16:
				content_locked = false;
				break;
			case 17:
				CritSectionCount = 1;
				break;
			case 18:
				b = 2;
				break;
			case 19:
				relation.rd_id++;
				break;
			}
			UT_ASSERT(!cluster_mx_history_capture_recheck(c, r, b, &tuple, &key, changed, 2));
		}
		CritSectionCount = 0;
		clean(c, s);
	}
}
static void
closed_activation_fence_refused(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	bool ok = begin(s, &c);
	UT_ASSERT(ok);
	/* A closed activation fence is never read authority, including on relock. */
	if (ok) {
		pcm_source.writer_activation_token = 1;
		UT_ASSERT(!cluster_mx_history_capture_recheck(c, &relation, 1, &tuple, &mxkey, members, 2));
	}
	clean(c, s);
}
static void
read_image_requires_new_bracket(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	bool ok;
	exclusive = false;
	pcm_source.pcm_state = PCM_STATE_READ_IMAGE;
	pcm_source.buffer_type = BUF_TYPE_CURRENT;
	ok = begin(s, &c);
	UT_ASSERT(ok);
	if (ok)
		UT_ASSERT(!cluster_mx_history_capture_recheck(c, &relation, 1, &tuple, &mxkey, members, 2));
	clean(c, s);
	s = setup();
	c = NULL;
	exclusive = false;
	pcm_source.pcm_state = PCM_STATE_READ_IMAGE;
	pcm_source.buffer_type = BUF_TYPE_CURRENT;
	ok = begin(s, &c);
	UT_ASSERT(ok);
	if (ok) {
		pcm_source.generation++;
		pcm_source.reservation_token++;
		UT_ASSERT(cluster_mx_history_capture_recheck(c, &relation, 1, &tuple, &mxkey, members, 2));
		/* A second relock cannot spend that same one-shot bracket again. */
		UT_ASSERT(!cluster_mx_history_capture_recheck(c, &relation, 1, &tuple, &mxkey, members, 2));
	}
	clean(c, s);
}
static void
cleanup_on_owner_release_and_exit(void)
{
	for (unsigned path = 0; path < 3; ++path) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		ClusterMxHistoryCapture view;
		bool ok = begin(s, &c);
		UT_ASSERT(ok);
		if (ok) {
			UT_ASSERT(cleanup_callback != NULL && exit_callback != NULL);
			if (path == 0)
				cleanup_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, cleanup_argument);
			else if (path == 1)
				cleanup_callback(RESOURCE_RELEASE_BEFORE_LOCKS, true, false, cleanup_argument);
			else
				exit_callback(0, exit_argument);
			UT_ASSERT_EQ(remembered, 1);
			UT_ASSERT(!cluster_mx_history_capture_read(c, &view));
		}
		clean(c, s);
	}
}
static void
allocation_and_overlapping_begin_refused(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL, *second = (void *)1;
	bool ok;
	oom = true;
	UT_ASSERT(!begin(s, &c));
	UT_ASSERT(c == NULL);
	UT_ASSERT_EQ(remembered, 1);
	oom = false;
	ok = begin(s, &c);
	UT_ASSERT(ok);
	if (ok) {
		UT_ASSERT(!begin(s, &second));
		UT_ASSERT(second == NULL);
		UT_ASSERT_EQ(remembered, 2);
	}
	clean(c, s);
}
static void
exact_owner_challenge_and_generation_zero(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	ClusterMxHistoryCapture view;
	bool ok;
	top_source = FullTransactionIdFromEpochAndXid(1, 100);
	canonical_source = (ClusterCanonicalTxnBinding){
		1, 0, 100, 2, 0, 2, CLUSTER_CANONICAL_TXN_PUBLISHED, 0, 123
	};
	publication_source = CLUSTER_CANONICAL_TXN_PUBLISHED;
	ok = begin(s, &c);
	UT_ASSERT(ok);
	if (ok) {
		UT_ASSERT(cluster_mx_history_capture_read(c, &view));
		UT_ASSERT_EQ(view.requester_owner.flags, CLUSTER_R4_CR_OWNER_PRESENT);
		UT_ASSERT_EQ(view.requester_owner.segment_generation, 0);
		canonical_source.slot_wrap++;
		UT_ASSERT(!cluster_mx_history_capture_read(c, &view));
	}
	clean(c, s);
}
static void
native_current_image_shapes_and_invalid_images(void)
{
	for (unsigned shape = 0; shape < 8; ++shape) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		bool expected = shape < 2, ok;
		exclusive = false;
		pcm_source.pcm_state = PCM_STATE_S;
		pcm_source.buffer_type = shape == 0 ? BUF_TYPE_SCUR : BUF_TYPE_XCUR;
		switch (shape) {
		case 2:
			pcm_source.buffer_type = BUF_TYPE_PI;
			break;
		case 3:
			pcm_source.buffer_type = BUF_TYPE_CR;
			break;
		case 4:
			pcm_source.semantic_buf_state &= ~BM_VALID;
			break;
		case 5:
			pcm_source.pcm_state = PCM_STATE_X;
			pcm_source.buffer_type = BUF_TYPE_SCUR;
			break;
		case 6:
			pcm_source.buffer_type = BUF_TYPE_CURRENT;
			break;
		case 7:
			pcm_source.pcm_state = PCM_STATE_READ_IMAGE;
			break;
		}
		ok = begin(s, &c);
		UT_ASSERT_EQ(ok, expected);
		clean(c, s);
	}
}

static void
fresh_capture_never_reuses_lineage(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *first = NULL, *second = NULL;
	ClusterMxHistoryCapture before, after;
	bool ok = begin(s, &first);
	UT_ASSERT(ok);
	if (ok) {
		UT_ASSERT(cluster_mx_history_capture_read(first, &before));
		cluster_mx_history_cancel(first);
		ok = begin(s, &second);
		UT_ASSERT(ok);
		if (ok) {
			cluster_mx_history_cancel(first);
			UT_ASSERT(cluster_mx_history_capture_read(second, &after));
			UT_ASSERT(after.collector_id > before.collector_id);
			UT_ASSERT(after.capture_generation > before.capture_generation);
		}
	}
	clean(second, s);
}

static void
malformed_descriptor_and_page_refused(void)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = (void *)1;
		ClusterCurrentMxMemberDesc changed[2];
		memcpy(changed, members, sizeof(changed));
		switch (fault) {
		case 0:
			changed[1].xid = changed[0].xid;
			break;
		case 1:
			changed[1].reserved8[0] = 1;
			break;
		case 2:
			tuple.t_data->t_hoff = 255;
			break;
		case 3:
			((PageHeader)BufferBlocks)->pd_special = BLCKSZ;
			break;
		case 4:
			tuple.t_data = (HeapTupleHeader)(BufferBlocks + 20);
			break;
		case 5:
			pcm_source.tag.blockNum++;
			break;
		}
		UT_ASSERT(!cluster_mx_history_capture_begin(&relation, 1, &tuple, s, &mxkey, changed, 2,
													&admission, &c));
		UT_ASSERT(c == NULL);
		clean(c, s);
	}
}
static void
collector_empty_graph_requires_full_relock(void)
{
	for (unsigned changed = 0; changed < 2; ++changed) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		const ClusterMxHistoryGraph *g = (void *)1;
		memset(ClusterPageGetItlSlots(BufferBlocks), 0, CLUSTER_ITL_SPECIAL_SIZE);
		UT_ASSERT(begin(s, &c));
		if (c) {
			UT_ASSERT(!cluster_mx_history_collect_recheck_graph(c, &relation, 1, &tuple, &mxkey,
																members, 2, &g));
			UT_ASSERT(g == NULL);
			content_locked = false;
			for (unsigned n = 0; n < 8; ++n)
				UT_ASSERT_EQ(cluster_mx_history_collect_poll(c), CMXH_PENDING);
			UT_ASSERT_EQ(cluster_mx_history_collect_poll(c), CMXH_COMPLETE);
			content_locked = true;
			if (changed)
				members[0].member_status = MultiXactStatusForKeyShare;
			UT_ASSERT_EQ(cluster_mx_history_collect_recheck_graph(c, &relation, 1, &tuple, &mxkey,
																  members, 2, &g),
						 !changed);
			if (changed) {
				UT_ASSERT(g == NULL);
				members[0].member_status = MultiXactStatusForShare;
			} else {
				UT_ASSERT(g != NULL);
				UT_ASSERT_EQ(cluster_mx_history_graph_count(g), 0);
			}
		}
		clean(c, s);
	}
}
static void
collector_pending_holds_snapshot_and_owner_cleanup(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	ClusterItlSlotData *root = ClusterPageGetItlSlots(BufferBlocks);
	root[0].write_scn = 90;
	root[0].undo_segment_head = uba_encode(1, 7, 3, 0);
	route_attempts = 0;
	UT_ASSERT(begin(s, &c));
	if (c) {
		content_locked = false;
		for (unsigned n = 0; n < 25; ++n)
			UT_ASSERT_EQ(cluster_mx_history_collect_poll(c), CMXH_PENDING);
		UT_ASSERT(route_attempts > 0);
		UT_ASSERT_EQ(remembered, 2);
		cleanup_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, cleanup_argument);
		UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT_EQ(cluster_mx_history_collect_poll(c), CMXH_UNKNOWN);
	}
	clean(c, s);
}
static void
collector_never_waits_under_content_lock_or_without_pin(void)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		UT_ASSERT(begin(s, &c));
		route_attempts = 0;
		if (c) {
			unsigned before = owner_reads;
			content_locked = fault == 0;
			if (fault == 1)
				pinned = false;
			if (fault == 2)
				CritSectionCount = 1;
			UT_ASSERT_EQ(cluster_mx_history_collect_poll(c), CMXH_UNKNOWN);
			if (fault == 2)
				UT_ASSERT_EQ(owner_reads, before);
			UT_ASSERT_EQ(route_attempts, 0);
			UT_ASSERT_EQ(remembered, 1);
		}
		CritSectionCount = 0;
		clean(c, s);
	}
}
int
main(void)
{
	MyProc = &proc;
	proc.pid = 1234;
	procArray = calloc(1, sizeof(ProcArrayStruct) + 2 * sizeof(int));
	procArray->maxProcs = 2;
	procArray->numProcs = 1;
	procArray->pgprocnos[0] = 0;
	procArray->pgprocnos[1] = 1;
	ProcGlobal = &snapshot_proc_header;
	ProcGlobal->statusFlags = snapshot_status_flags;
	pg_atomic_init_u64(&proc.cluster_read_scn_atomic, 0);
	UndoHorizonShmem = &horizon;
	pg_atomic_init_u64(&horizon.self_admitted_epoch, 8);
	pg_atomic_init_u64(&horizon.admission_refuse_count, 0);
	oldSnapshotControl = &snapshot_control;
	pg_atomic_init_u64(&snapshot_control.cr_identity_generation, 0);
	UT_PLAN(17);
	UT_RUN(real_capture_and_relock);
	UT_RUN(snapshot_own_reference_keeps_original_floor);
	UT_RUN(original_scope_restored_and_older_floor_preserved);
	UT_RUN(forged_and_retired_snapshots_refused);
	UT_RUN(source_changes_cancel_whole_capture);
	UT_RUN(complete_heap_fingerprint_rechecked);
	UT_RUN(closed_activation_fence_refused);
	UT_RUN(read_image_requires_new_bracket);
	UT_RUN(cleanup_on_owner_release_and_exit);
	UT_RUN(allocation_and_overlapping_begin_refused);
	UT_RUN(exact_owner_challenge_and_generation_zero);
	UT_RUN(malformed_descriptor_and_page_refused);
	UT_RUN(fresh_capture_never_reuses_lineage);
	UT_RUN(native_current_image_shapes_and_invalid_images);
	UT_RUN(collector_empty_graph_requires_full_relock);
	UT_RUN(collector_pending_holds_snapshot_and_owner_cleanup);
	UT_RUN(collector_never_waits_under_content_lock_or_without_pin);
	free(procArray);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
