/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_direct_init.c
 *	Operation-scoped exact proof for the S3 PCM direct-init exception.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_pcm_direct_init.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#ifndef BUFMGR_SOURCE_PATH
#error "BUFMGR_SOURCE_PATH must identify production bufmgr.c"
#endif
#ifndef VM_SOURCE_PATH
#error "VM_SOURCE_PATH must identify production visibilitymap.c"
#endif
#ifndef FSM_SOURCE_PATH
#error "FSM_SOURCE_PATH must identify production freespace.c"
#endif
#ifndef HEAPAM_SOURCE_PATH
#error "HEAPAM_SOURCE_PATH must identify production heapam.c"
#endif
#ifndef HIO_SOURCE_PATH
#error "HIO_SOURCE_PATH must identify production hio.c"
#endif

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static BufferTag
make_tag(ForkNumber forknum, BlockNumber blocknum)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 20000;
	tag.forkNum = forknum;
	tag.blockNum = blocknum;
	return tag;
}

static ClusterPcmDirectInitSnapshot
make_snapshot(ClusterPcmDirectInitKind kind)
{
	ClusterPcmDirectInitSnapshot snapshot;

	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.buf_id = 7;
	snapshot.tag = make_tag(MAIN_FORKNUM, 42);
	snapshot.generation = 11;
	snapshot.reservation_token = 4;
	snapshot.private_refcount = 1;
	snapshot.buffer_type = (uint8)BUF_TYPE_CURRENT;
	snapshot.pcm_state = (uint8)PCM_STATE_N;
	snapshot.page_is_new = true;

	if (kind == CLUSTER_PCM_DIRECT_INIT_VM)
		snapshot.tag.forkNum = VISIBILITYMAP_FORKNUM;
	else if (kind == CLUSTER_PCM_DIRECT_INIT_FSM)
		snapshot.tag.forkNum = FSM_FORKNUM;

	if (kind == CLUSTER_PCM_DIRECT_INIT_VM || kind == CLUSTER_PCM_DIRECT_INIT_FSM)
		snapshot.buf_state = BM_TAG_VALID | BM_VALID | 1;
	else
		snapshot.buf_state = BM_TAG_VALID | BM_IO_IN_PROGRESS | 1;

	return snapshot;
}

static char *
read_source(const char *path)
{
	FILE *fp;
	char *source;
	long length;

	fp = fopen(path, "rb");
	UT_ASSERT(fp != NULL);
	if (fp == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_END), 0);
	length = ftell(fp);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT(source != NULL);
	if (source == NULL) {
		fclose(fp);
		return NULL;
	}
	UT_ASSERT_EQ((long)fread(source, 1, (size_t)length, fp), length);
	source[length] = '\0';
	fclose(fp);
	return source;
}

static void
assert_ordered(const char *source, const char *const *needles, int count)
{
	const char *cursor = source;

	for (int i = 0; i < count; i++) {
		cursor = strstr(cursor, needles[i]);
		UT_ASSERT(cursor != NULL);
		if (cursor == NULL)
			return;
		cursor += strlen(needles[i]);
	}
}

static void
expect_valid_round_trip(ClusterPcmDirectInitKind kind)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot snapshot = make_snapshot(kind);

	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(kind, &snapshot, &proof), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_consume(kind, &snapshot, &proof),
				 CLUSTER_PCM_OWN_OK);
}

UT_TEST(test_valid_read_miss_proof)
{
	expect_valid_round_trip(CLUSTER_PCM_DIRECT_INIT_READ_MISS);
}

UT_TEST(test_valid_extend_proof)
{
	expect_valid_round_trip(CLUSTER_PCM_DIRECT_INIT_EXTEND);
}

UT_TEST(test_valid_vm_and_fsm_proofs)
{
	expect_valid_round_trip(CLUSTER_PCM_DIRECT_INIT_VM);
	expect_valid_round_trip(CLUSTER_PCM_DIRECT_INIT_FSM);
}

UT_TEST(test_proof_is_single_use_and_kind_exact)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot snapshot = make_snapshot(CLUSTER_PCM_DIRECT_INIT_READ_MISS);

	UT_ASSERT_EQ(
		cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_READ_MISS, &snapshot, &proof),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(
		cluster_pcm_direct_init_proof_consume(CLUSTER_PCM_DIRECT_INIT_EXTEND, &snapshot, &proof),
		CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(
		cluster_pcm_direct_init_proof_consume(CLUSTER_PCM_DIRECT_INIT_READ_MISS, &snapshot, &proof),
		CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_identity_mismatch_rejects_buf_tag_generation_and_token)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot base = make_snapshot(CLUSTER_PCM_DIRECT_INIT_EXTEND);
	ClusterPcmDirectInitSnapshot changed;

#define EXPECT_IDENTITY_REJECT(field, value)                                                       \
	do {                                                                                           \
		changed = base;                                                                            \
		UT_ASSERT_EQ(                                                                              \
			cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_EXTEND, &base, &proof),      \
			CLUSTER_PCM_OWN_OK);                                                                   \
		changed.field = (value);                                                                   \
		UT_ASSERT_EQ(cluster_pcm_direct_init_proof_consume(CLUSTER_PCM_DIRECT_INIT_EXTEND,         \
														   &changed, &proof),                      \
					 CLUSTER_PCM_OWN_STALE);                                                       \
	} while (0)

	EXPECT_IDENTITY_REJECT(buf_id, 8);
	EXPECT_IDENTITY_REJECT(generation, 12);
	EXPECT_IDENTITY_REJECT(reservation_token, 5);
	EXPECT_IDENTITY_REJECT(private_refcount, 2);

	changed = base;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_EXTEND, &base, &proof),
				 CLUSTER_PCM_OWN_OK);
	changed.tag.blockNum++;
	UT_ASSERT_EQ(
		cluster_pcm_direct_init_proof_consume(CLUSTER_PCM_DIRECT_INIT_EXTEND, &changed, &proof),
		CLUSTER_PCM_OWN_STALE);
#undef EXPECT_IDENTITY_REJECT
}

UT_TEST(test_reuse_dirty_and_shape_are_rejected)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot base = make_snapshot(CLUSTER_PCM_DIRECT_INIT_READ_MISS);
	ClusterPcmDirectInitSnapshot changed;

#define EXPECT_SHAPE_REJECT(mutator)                                                               \
	do {                                                                                           \
		changed = base;                                                                            \
		UT_ASSERT_EQ(                                                                              \
			cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_READ_MISS, &base, &proof),   \
			CLUSTER_PCM_OWN_OK);                                                                   \
		mutator;                                                                                   \
		UT_ASSERT_EQ(cluster_pcm_direct_init_proof_consume(CLUSTER_PCM_DIRECT_INIT_READ_MISS,      \
														   &changed, &proof),                      \
					 CLUSTER_PCM_OWN_STALE);                                                       \
	} while (0)

	EXPECT_SHAPE_REJECT(changed.buf_state |= BM_DIRTY);
	EXPECT_SHAPE_REJECT(changed.buf_state |= BM_JUST_DIRTIED);
	EXPECT_SHAPE_REJECT(changed.buf_state |= BM_VALID);
	EXPECT_SHAPE_REJECT(changed.buf_state &= ~BM_IO_IN_PROGRESS);
	EXPECT_SHAPE_REJECT(changed.buffer_type = (uint8)BUF_TYPE_PI);
	EXPECT_SHAPE_REJECT(changed.page_is_new = false);
	EXPECT_SHAPE_REJECT(changed.tag.blockNum++);
#undef EXPECT_SHAPE_REJECT
}

UT_TEST(test_state_s_and_live_reservations_are_rejected_by_class)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot snapshot = make_snapshot(CLUSTER_PCM_DIRECT_INIT_VM);

	snapshot.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_VM, &snapshot, &proof),
				 CLUSTER_PCM_OWN_STALE);

	snapshot = make_snapshot(CLUSTER_PCM_DIRECT_INIT_VM);
	snapshot.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_VM, &snapshot, &proof),
				 CLUSTER_PCM_OWN_BUSY);

	snapshot.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_VM, &snapshot, &proof),
				 CLUSTER_PCM_OWN_BUSY);

	snapshot.flags = PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_VM, &snapshot, &proof),
				 CLUSTER_PCM_OWN_CORRUPT);
}

UT_TEST(test_revalidate_rejects_state_pin_and_reservation_changes)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot base = make_snapshot(CLUSTER_PCM_DIRECT_INIT_VM);
	ClusterPcmDirectInitSnapshot changed;

#define EXPECT_REVALIDATE_REJECT(mutator, expected_result)                                         \
	do {                                                                                           \
		changed = base;                                                                            \
		UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_VM, &base, &proof), \
					 CLUSTER_PCM_OWN_OK);                                                          \
		mutator;                                                                                   \
		UT_ASSERT_EQ(                                                                              \
			cluster_pcm_direct_init_proof_consume(CLUSTER_PCM_DIRECT_INIT_VM, &changed, &proof),   \
			expected_result);                                                                      \
	} while (0)

	EXPECT_REVALIDATE_REJECT(changed.pcm_state = (uint8)PCM_STATE_S, CLUSTER_PCM_OWN_STALE);
	EXPECT_REVALIDATE_REJECT(changed.private_refcount = 0, CLUSTER_PCM_OWN_STALE);
	EXPECT_REVALIDATE_REJECT(changed.flags = PCM_OWN_FLAG_GRANT_PENDING, CLUSTER_PCM_OWN_BUSY);
	EXPECT_REVALIDATE_REJECT(changed.flags = PCM_OWN_FLAG_REVOKING, CLUSTER_PCM_OWN_BUSY);
	EXPECT_REVALIDATE_REJECT(changed.flags = PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING,
							 CLUSTER_PCM_OWN_CORRUPT);
#undef EXPECT_REVALIDATE_REJECT
}

UT_TEST(test_missing_backend_pin_is_rejected)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot snapshot = make_snapshot(CLUSTER_PCM_DIRECT_INIT_FSM);

	snapshot.private_refcount = 0;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_FSM, &snapshot, &proof),
				 CLUSTER_PCM_OWN_STALE);

	snapshot = make_snapshot(CLUSTER_PCM_DIRECT_INIT_FSM);
	snapshot.buf_state &= ~BUF_REFCOUNT_MASK;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_FSM, &snapshot, &proof),
				 CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_vm_fsm_fork_and_valid_shape_are_exact)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot snapshot = make_snapshot(CLUSTER_PCM_DIRECT_INIT_VM);

	snapshot.tag.forkNum = FSM_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_VM, &snapshot, &proof),
				 CLUSTER_PCM_OWN_STALE);

	snapshot = make_snapshot(CLUSTER_PCM_DIRECT_INIT_FSM);
	snapshot.buf_state &= ~BM_VALID;
	snapshot.buf_state |= BM_IO_IN_PROGRESS;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(CLUSTER_PCM_DIRECT_INIT_FSM, &snapshot, &proof),
				 CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_bufmgr_consumes_proof_before_reservation_and_wire)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	static const char *const order[]
		= { "cluster_bufmgr_pcm_gate_direct_init(", "cluster_pcm_direct_init_proof_consume",
			"cluster_pcm_own_reservation_begin_exact", "cluster_pcm_lock_acquire_buffer" };

	UT_ASSERT(source != NULL);
	if (source != NULL) {
		assert_ordered(source, order, lengthof(order));
		free(source);
	}
}

UT_TEST(test_read_miss_and_found_hit_have_no_raw_unproven_lock)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	static const char *const miss_order[]
		= { "MemSet((char *) bufBlock, 0, BLCKSZ)", "CLUSTER_PCM_DIRECT_INIT_READ_MISS",
			"cluster_bufmgr_pcm_gate_direct_init",
			"LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE)" };
	static const char *const found_order[]
		= { "if (found)", "if (mode == RBM_ZERO_AND_LOCK)",
			"LockBuffer(BufferDescriptorGetBuffer(bufHdr), BUFFER_LOCK_EXCLUSIVE)" };

	UT_ASSERT(source != NULL);
	if (source != NULL) {
		assert_ordered(source, miss_order, lengthof(miss_order));
		assert_ordered(source, found_order, lengthof(found_order));
		free(source);
	}
}

UT_TEST(test_extend_proof_is_after_zeroextend_and_before_lock)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	static const char *const order[]
		= { "smgrzeroextend(bmr.smgr, fork, first_block, extend_by, false)",
			"CLUSTER_PCM_DIRECT_INIT_EXTEND", "cluster_bufmgr_pcm_gate_direct_init",
			"LWLockAcquire(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE)",
			"TerminateBufferIO(buf_hdr, false, BM_VALID)" };

	UT_ASSERT(source != NULL);
	if (source != NULL) {
		assert_ordered(source, order, lengthof(order));
		free(source);
	}
}

UT_TEST(test_vm_fsm_use_dedicated_init_wrappers)
{
	char *vm_source = read_source(VM_SOURCE_PATH);
	char *fsm_source = read_source(FSM_SOURCE_PATH);

	UT_ASSERT(vm_source != NULL);
	UT_ASSERT(fsm_source != NULL);
	if (vm_source != NULL) {
		UT_ASSERT(strstr(vm_source, "LockBufferForVisibilityMapPageInit(buf)") != NULL);
		free(vm_source);
	}
	if (fsm_source != NULL) {
		UT_ASSERT(strstr(fsm_source, "LockBufferForFreeSpaceMapPageInit(buf)") != NULL);
		free(fsm_source);
	}
}

UT_TEST(test_valid_n_s_x_without_proof_enters_queue_before_legacy_wire)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	static const char *const order[]
		= { "pcm_x_writer = cluster_bufmgr_pcm_x_writer_prepare(buf, pcm_mode,",
			"cluster_bufmgr_pcm_begin_grant_reservation_wait(",
			"cluster_pcm_lock_acquire_buffer(buf, pcm_mode," };

	UT_ASSERT(source != NULL);
	if (source != NULL) {
		assert_ordered(source, order, lengthof(order));
		free(source);
	}
}

UT_TEST(test_direct_init_one_shot_image_cannot_return_without_x)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	static const char *const order[]
		= { "cluster_pcm_own_abort_grant_or_error(buf, &pending_base, pending_token",
			"cluster_bufmgr_pcm_direct_init_no_grant_failclosed" };

	UT_ASSERT(source != NULL);
	if (source != NULL) {
		assert_ordered(source, order, lengthof(order));
		free(source);
	}
}

UT_TEST(test_wire_throw_exact_aborts_reservation_before_rethrow)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	static const char *const order[]
		= { "cluster_bufmgr_pcm_gate_direct_init(", "PG_CATCH();",
			"cluster_pcm_own_abort_grant_after_error(buf, &pending_base, pending_token",
			"\"direct-init acquire\"", "PG_RE_THROW();" };

	UT_ASSERT(source != NULL);
	if (source != NULL) {
		assert_ordered(source, order, lengthof(order));
		free(source);
	}
}

/* t/400 L3 item 3 — a nested-guard BARRIER_CLOSED at the pre-crit VM lock
 * must unwind to the caller that owns the outer heap content lock instead of
 * escaping as a client ERROR.  bufmgr exposes the refusal (never releasing
 * the foreign lock itself); heapam releases its own lock(s), warms the map
 * page's node X while holding no content lock, and re-enters a proven
 * requalify/reacquire point. */
UT_TEST(test_precrit_vm_barrier_refusal_unwinds_to_caller)
{
	char *bufmgr = read_source(BUFMGR_SOURCE_PATH);
	char *heapam = read_source(HEAPAM_SOURCE_PATH);

	UT_ASSERT(bufmgr != NULL);
	UT_ASSERT(heapam != NULL);
	if (bufmgr != NULL) {
		/* The refusal arm sits between the queue acquire and the ERROR
		 * report, and only the barrier-aware entry can consume it. */
		static const char *const refusal_order[]
			= { "cluster_gcs_pcm_x_acquire_writer(buf, &entry->claim",
				"PCM_X_QUEUE_BARRIER_CLOSED && barrier_refused != NULL",
				"cluster_bufmgr_pcm_x_writer_report_failure(result, buf, \"queue acquire\")" };

		UT_ASSERT(strstr(bufmgr, "ClusterLockBufferExclusiveBarrierAware(Buffer buffer)") != NULL);
		assert_ordered(bufmgr, refusal_order, lengthof(refusal_order));
		free(bufmgr);
	}
	if (heapam != NULL) {
		static const char *const pretoast_order[]
			= { "PGRAC: vm barrier unwind (update pre-toast)",
				"LockBuffer(buffer, BUFFER_LOCK_UNLOCK)", "cluster_heap_vm_barrier_warm",
				"ReleaseBuffer(vmbuffer)", "vmbuffer = InvalidBuffer",
				"cluster_heap_lock_with_vm_repin", "goto l2;" };
		static const char *const requalify_order[]
			= { "PGRAC: vm barrier unwind (update requalify)",
				"LockBuffer(buffer, BUFFER_LOCK_UNLOCK)", "cluster_heap_vm_barrier_warm",
				"ReleaseBuffer(vmbuffer)", "vmbuffer = InvalidBuffer",
				"cluster_heap_lock_with_vm_repin", "goto l2;" };
		static const char *const reacquire_order[]
			= { "PGRAC: vm barrier unwind (update reacquire)",
				"LockBuffer(newbuf, BUFFER_LOCK_UNLOCK)",
				"ReleaseBuffer(newbuf)",
				"LockBuffer(buffer, BUFFER_LOCK_UNLOCK)",
				"cluster_heap_vm_barrier_warm",
				"goto l_pgrac_reacquire;" };
		static const char *const delete_order[]
			= { "PGRAC: vm barrier unwind (delete requalify)",
				"LockBuffer(buffer, BUFFER_LOCK_UNLOCK)", "cluster_heap_vm_barrier_warm",
				"ReleaseBuffer(vmbuffer)", "vmbuffer = InvalidBuffer",
				"cluster_heap_lock_with_vm_repin", "goto l1;" };

		/* The warm helper itself may only run with no content lock held:
		 * it must take and drop the map-page lock, nothing else. */
		static const char *const warm_order[] = { "cluster_heap_vm_barrier_warm(Buffer vmbuf)",
												  "LockBuffer(vmbuf, BUFFER_LOCK_EXCLUSIVE)",
												  "LockBuffer(vmbuf, BUFFER_LOCK_UNLOCK)" };

		assert_ordered(heapam, warm_order, lengthof(warm_order));
		assert_ordered(heapam, pretoast_order, lengthof(pretoast_order));
		assert_ordered(heapam, requalify_order, lengthof(requalify_order));
		assert_ordered(heapam, reacquire_order, lengthof(reacquire_order));
		assert_ordered(heapam, delete_order, lengthof(delete_order));
		free(heapam);
	}
}

/* P0-20: an UPDATE must not keep its visibility-map pin while it can block
 * acquiring heap X.  Another writer can already be transferring that same
 * VM page, and VM/FSM deliberately cannot become a retained PI while pinned.
 * Pre-read the VM page, release the pin before the heap PCM wait, then repin
 * only the exact recent descriptor without I/O after heap X is held. */
UT_TEST(test_heap_update_drops_vm_pin_across_heap_pcm_wait)
{
	char *heapam = read_source(HEAPAM_SOURCE_PATH);
	char *visibilitymap = read_source(VM_SOURCE_PATH);

	UT_ASSERT(heapam != NULL);
	UT_ASSERT(visibilitymap != NULL);
	if (heapam != NULL) {
		const char *helper = strstr(heapam, "cluster_heap_lock_with_vm_repin(");
		const char *helper_end = helper != NULL ? strstr(helper, "\n}\n") : NULL;
		const char *update = strstr(heapam, "\nheap_update(");
		const char *update_lock
			= update != NULL ? strstr(update, "cluster_heap_lock_with_vm_repin(") : NULL;
		const char *repin_branch = update != NULL
			? strstr(update, "if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))")
			: NULL;
		const char *repin_branch_end
			= repin_branch != NULL ? strstr(repin_branch, "goto l2;") : NULL;
		const char *repin_branch_helper = repin_branch != NULL
			? strstr(repin_branch, "cluster_heap_lock_with_vm_repin(") : NULL;
		const char *success_tail = update != NULL
			? strstr(update, "recptr = log_heap_update(")
			: NULL;
		const char *vm_unlock = success_tail != NULL
			? strstr(success_tail,
					 "if (vm_locked)\n\t\tLockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);")
			: NULL;
		const char *vm_release = vm_unlock != NULL
			? strstr(vm_unlock, "ReleaseBuffer(vmbuffer);")
			: NULL;
		const char *heap_unlock = vm_unlock != NULL
			? strstr(vm_unlock, "LockBuffer(buffer, BUFFER_LOCK_UNLOCK);")
			: NULL;
		static const char *const helper_order[]
			= { "visibilitymap_pin(relation, heap_block, vmbuffer)",
				"ReleaseBuffer(*vmbuffer)",
				"*vmbuffer = InvalidBuffer",
				"LockBuffer(heap_buffer, BUFFER_LOCK_EXCLUSIVE)",
				"visibilitymap_pin_recent(relation, heap_block, recent_vm, vmbuffer)",
				"LockBuffer(heap_buffer, BUFFER_LOCK_UNLOCK)" };

		UT_ASSERT(helper != NULL);
		UT_ASSERT(helper_end != NULL);
		UT_ASSERT(update != NULL);
		UT_ASSERT(update_lock != NULL);
		UT_ASSERT(repin_branch != NULL);
		UT_ASSERT(repin_branch_end != NULL);
		UT_ASSERT(repin_branch_helper != NULL);
		UT_ASSERT(repin_branch_helper < repin_branch_end);
		UT_ASSERT(vm_unlock != NULL);
		UT_ASSERT(vm_release != NULL);
		UT_ASSERT(heap_unlock != NULL);
		UT_ASSERT(vm_release < heap_unlock);
		if (helper != NULL && helper_end != NULL)
			assert_ordered(helper, helper_order, lengthof(helper_order));
		free(heapam);
	}
	if (visibilitymap != NULL) {
		const char *repin = strstr(visibilitymap, "\nvisibilitymap_pin_recent(");
		const char *repin_end = repin != NULL ? strstr(repin, "\n}\n") : NULL;
		static const char *const repin_order[]
			= { "HEAPBLK_TO_MAPBLOCK(heapBlk)", "ReadRecentBuffer(",
				"VISIBILITYMAP_FORKNUM", "recent_buffer" };

		UT_ASSERT(repin != NULL);
		UT_ASSERT(repin_end != NULL);
		if (repin != NULL && repin_end != NULL)
			assert_ordered(repin, repin_order, lengthof(repin_order));
		free(visibilitymap);
	}
}

/* P0-20: RelationGetBufferForTuple acquires two heap content locks for a
 * cross-page UPDATE.  If the second queue acquire sees the first page under
 * a frozen revoke barrier, it must release the first lock, resolve the second
 * conversion with no content lock held, and retry the ordered pair. */
UT_TEST(test_cross_page_heap_pair_barrier_refusal_retries_unlocked)
{
	char *hio = read_source(HIO_SOURCE_PATH);
	const char *helper;
	const char *helper_end;
	const char *pins;
	const char *pins_end;
	const char *pins_call;
	const char *relation;
	const char *relation_end;
	const char *lower_branch;
	const char *lower_call;
	const char *upper_branch;
	const char *upper_call;
	const char *extension;
	const char *extension_call;
	static const char *const helper_order[]
		= { "cluster_hio_lock_buffer_pair(Buffer first, Buffer second)",
			"LockBuffer(first, BUFFER_LOCK_EXCLUSIVE)",
			"ClusterLockBufferExclusiveBarrierAware(second)",
			"LockBuffer(first, BUFFER_LOCK_UNLOCK)",
			"LockBuffer(second, BUFFER_LOCK_EXCLUSIVE)",
			"LockBuffer(second, BUFFER_LOCK_UNLOCK)" };

	UT_ASSERT(hio != NULL);
	if (hio == NULL)
		return;
	helper = strstr(hio, "cluster_hio_lock_buffer_pair(Buffer first, Buffer second)");
	helper_end = helper != NULL ? strstr(helper, "\n}\n") : NULL;
	pins = strstr(hio, "GetVisibilityMapPins(Relation relation");
	pins_end = pins != NULL ? strstr(pins, "\n}\n") : NULL;
	pins_call = pins != NULL ? strstr(pins, "cluster_hio_lock_buffer_pair(") : NULL;
	relation = strstr(hio, "\nRelationGetBufferForTuple(");
	relation_end = relation != NULL ? strstr(relation, "\n}\n") : NULL;
	lower_branch = relation != NULL ? strstr(relation, "else if (otherBlock < targetBlock)") : NULL;
	lower_call = lower_branch != NULL ? strstr(lower_branch, "cluster_hio_lock_buffer_pair(") : NULL;
	upper_branch = lower_branch != NULL ? strstr(lower_branch, "\n\t\telse\n\t\t{") : NULL;
	upper_call = upper_branch != NULL ? strstr(upper_branch, "cluster_hio_lock_buffer_pair(") : NULL;
	extension = relation != NULL ? strstr(relation, "Reacquire locks if necessary") : NULL;
	extension_call = extension != NULL ? strstr(extension, "cluster_hio_lock_buffer_pair(") : NULL;

	UT_ASSERT(helper != NULL);
	UT_ASSERT(helper_end != NULL);
	UT_ASSERT(pins != NULL);
	UT_ASSERT(pins_end != NULL);
	UT_ASSERT(relation != NULL);
	UT_ASSERT(relation_end != NULL);
	if (helper != NULL && helper_end != NULL)
		assert_ordered(helper, helper_order, lengthof(helper_order));
	UT_ASSERT(pins != NULL && pins_end != NULL && pins_call != NULL
			  && pins_call < pins_end);
	UT_ASSERT(lower_branch != NULL && lower_call != NULL && upper_branch != NULL
			  && lower_call < upper_branch);
	UT_ASSERT(upper_branch != NULL && upper_call != NULL && relation_end != NULL
			  && upper_call < relation_end);
	UT_ASSERT(extension != NULL && extension_call != NULL && relation_end != NULL
			  && extension_call < relation_end);
	free(hio);
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_valid_read_miss_proof);
	UT_RUN(test_valid_extend_proof);
	UT_RUN(test_valid_vm_and_fsm_proofs);
	UT_RUN(test_proof_is_single_use_and_kind_exact);
	UT_RUN(test_identity_mismatch_rejects_buf_tag_generation_and_token);
	UT_RUN(test_reuse_dirty_and_shape_are_rejected);
	UT_RUN(test_state_s_and_live_reservations_are_rejected_by_class);
	UT_RUN(test_revalidate_rejects_state_pin_and_reservation_changes);
	UT_RUN(test_missing_backend_pin_is_rejected);
	UT_RUN(test_vm_fsm_fork_and_valid_shape_are_exact);
	UT_RUN(test_bufmgr_consumes_proof_before_reservation_and_wire);
	UT_RUN(test_read_miss_and_found_hit_have_no_raw_unproven_lock);
	UT_RUN(test_extend_proof_is_after_zeroextend_and_before_lock);
	UT_RUN(test_vm_fsm_use_dedicated_init_wrappers);
	UT_RUN(test_valid_n_s_x_without_proof_enters_queue_before_legacy_wire);
	UT_RUN(test_direct_init_one_shot_image_cannot_return_without_x);
	UT_RUN(test_wire_throw_exact_aborts_reservation_before_rethrow);
	UT_RUN(test_precrit_vm_barrier_refusal_unwinds_to_caller);
	UT_RUN(test_heap_update_drops_vm_pin_across_heap_pcm_wait);
	UT_RUN(test_cross_page_heap_pair_barrier_refusal_retries_unlocked);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
