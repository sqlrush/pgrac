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
#include "cluster/cluster_pcm_x_bufmgr.h"
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

static int
count_occurrences(const char *source, const char *needle)
{
	int			count = 0;
	const char *cursor = source;

	while ((cursor = strstr(cursor, needle)) != NULL)
	{
		count++;
		cursor += strlen(needle);
	}
	return count;
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

UT_TEST(test_aux_pending_observer_accepts_only_exact_existing_reservation)
{
	ClusterPcmDirectInitSnapshot snapshot
		= make_snapshot(CLUSTER_PCM_DIRECT_INIT_VM);

	snapshot.flags = PCM_OWN_FLAG_GRANT_PENDING;
	snapshot.reservation_token = 9;
	UT_ASSERT_EQ(cluster_pcm_direct_init_aux_pending_observer_validate(
		CLUSTER_PCM_DIRECT_INIT_VM, &snapshot), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_direct_init_aux_pending_observer_validate(
		CLUSTER_PCM_DIRECT_INIT_READ_MISS, &snapshot),
		CLUSTER_PCM_OWN_INVALID);

	snapshot.reservation_token = 0;
	UT_ASSERT_EQ(cluster_pcm_direct_init_aux_pending_observer_validate(
		CLUSTER_PCM_DIRECT_INIT_VM, &snapshot), CLUSTER_PCM_OWN_STALE);
	snapshot.reservation_token = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_direct_init_aux_pending_observer_validate(
		CLUSTER_PCM_DIRECT_INIT_VM, &snapshot), CLUSTER_PCM_OWN_STALE);

	snapshot = make_snapshot(CLUSTER_PCM_DIRECT_INIT_FSM);
	snapshot.flags = PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING;
	snapshot.reservation_token = 9;
	UT_ASSERT_EQ(cluster_pcm_direct_init_aux_pending_observer_validate(
		CLUSTER_PCM_DIRECT_INIT_FSM, &snapshot), CLUSTER_PCM_OWN_CORRUPT);

	snapshot = make_snapshot(CLUSTER_PCM_DIRECT_INIT_FSM);
	snapshot.flags = PCM_OWN_FLAG_GRANT_PENDING;
	snapshot.reservation_token = 9;
	snapshot.page_is_new = false;
	UT_ASSERT_EQ(cluster_pcm_direct_init_aux_pending_observer_validate(
		CLUSTER_PCM_DIRECT_INIT_FSM, &snapshot), CLUSTER_PCM_OWN_STALE);
	snapshot.page_is_new = true;
	snapshot.pcm_state = (uint8)PCM_STATE_X;
	UT_ASSERT_EQ(cluster_pcm_direct_init_aux_pending_observer_validate(
		CLUSTER_PCM_DIRECT_INIT_FSM, &snapshot), CLUSTER_PCM_OWN_STALE);
	snapshot.pcm_state = (uint8)PCM_STATE_N;
	snapshot.generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_direct_init_aux_pending_observer_validate(
		CLUSTER_PCM_DIRECT_INIT_FSM, &snapshot),
		CLUSTER_PCM_OWN_EXHAUSTED);
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

UT_TEST(test_target_pending_reservation_remains_bound_to_consumed_known_new_proof)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot base
		= make_snapshot(CLUSTER_PCM_DIRECT_INIT_EXTEND);
	ClusterPcmDirectInitSnapshot pending;

	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(
		CLUSTER_PCM_DIRECT_INIT_EXTEND, &base, &proof), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_consume(
		CLUSTER_PCM_DIRECT_INIT_EXTEND, &base, &proof), CLUSTER_PCM_OWN_OK);
	pending = base;
	pending.flags = PCM_OWN_FLAG_GRANT_PENDING;
	pending.reservation_token++;
	UT_ASSERT_EQ(cluster_pcm_direct_init_target_pending_validate(
		CLUSTER_PCM_DIRECT_INIT_EXTEND, &pending, &proof), CLUSTER_PCM_OWN_OK);

	pending.buf_state &= ~BM_IO_IN_PROGRESS;
	UT_ASSERT_EQ(cluster_pcm_direct_init_target_pending_validate(
		CLUSTER_PCM_DIRECT_INIT_EXTEND, &pending, &proof), CLUSTER_PCM_OWN_STALE);
	pending = base;
	pending.flags = PCM_OWN_FLAG_GRANT_PENDING;
	pending.reservation_token += 2;
	UT_ASSERT_EQ(cluster_pcm_direct_init_target_pending_validate(
		CLUSTER_PCM_DIRECT_INIT_EXTEND, &pending, &proof), CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_target_post_t3_commit_revalidates_exact_known_new_identity)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot base
		= make_snapshot(CLUSTER_PCM_DIRECT_INIT_READ_MISS);
	ClusterPcmDirectInitSnapshot committed;

	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(
		CLUSTER_PCM_DIRECT_INIT_READ_MISS, &base, &proof), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_consume(
		CLUSTER_PCM_DIRECT_INIT_READ_MISS, &base, &proof), CLUSTER_PCM_OWN_OK);
	committed = base;
	committed.generation++;
	committed.reservation_token++;
	committed.buffer_type = (uint8)BUF_TYPE_XCUR;
	committed.pcm_state = (uint8)PCM_STATE_X;
	UT_ASSERT_EQ(cluster_pcm_direct_init_target_commit_validate(
		CLUSTER_PCM_DIRECT_INIT_READ_MISS, &committed, &proof), CLUSTER_PCM_OWN_OK);

	committed.page_is_new = false;
	UT_ASSERT_EQ(cluster_pcm_direct_init_target_commit_validate(
		CLUSTER_PCM_DIRECT_INIT_READ_MISS, &committed, &proof), CLUSTER_PCM_OWN_STALE);
	committed.page_is_new = true;
	committed.generation++;
	UT_ASSERT_EQ(cluster_pcm_direct_init_target_commit_validate(
		CLUSTER_PCM_DIRECT_INIT_READ_MISS, &committed, &proof), CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_target_fresh_generation_zero_is_exact_known_new_identity)
{
	ClusterPcmDirectInitProof proof;
	ClusterPcmDirectInitSnapshot base
		= make_snapshot(CLUSTER_PCM_DIRECT_INIT_EXTEND);
	ClusterPcmDirectInitSnapshot pending;
	ClusterPcmDirectInitSnapshot committed;

	base.generation = 0;
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_arm(
		CLUSTER_PCM_DIRECT_INIT_EXTEND, &base, &proof), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_direct_init_proof_consume(
		CLUSTER_PCM_DIRECT_INIT_EXTEND, &base, &proof), CLUSTER_PCM_OWN_OK);
	pending = base;
	pending.flags = PCM_OWN_FLAG_GRANT_PENDING;
	pending.reservation_token++;
	UT_ASSERT_EQ(cluster_pcm_direct_init_target_pending_validate(
		CLUSTER_PCM_DIRECT_INIT_EXTEND, &pending, &proof), CLUSTER_PCM_OWN_OK);
	committed = pending;
	committed.generation = 1;
	committed.flags = 0;
	committed.buffer_type = (uint8)BUF_TYPE_XCUR;
	committed.pcm_state = (uint8)PCM_STATE_X;
	UT_ASSERT_EQ(cluster_pcm_direct_init_target_commit_validate(
		CLUSTER_PCM_DIRECT_INIT_EXTEND, &committed, &proof), CLUSTER_PCM_OWN_OK);
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

UT_TEST(test_target_direct_init_uses_exact_resource_x_round_without_legacy_fallback)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	static const char *const target_order[] = {
		"cluster_bufmgr_pcm_gate_direct_init(",
		"cluster_pcm_direct_init_proof_consume",
		"cluster_pcm_own_reservation_begin_exact",
		"cluster_pcm_direct_init_target_pending_validate(",
		"cluster_resource_x_writer_path_snapshot(",
		"writer_path != RESOURCE_X_WRITER_TARGET",
		"cluster_pcm_own_abort_grant_after_error(",
		"cluster_gcs_resource_x_target_direct_init_acquire_exact(",
		"pending_base.generation",
		"pending_token",
		"cluster_pcm_direct_init_target_commit_validate(",
		"cluster_bufmgr_pcm_x_writer_track_target_direct_init("
	};

	UT_ASSERT(source != NULL);
	if (source != NULL) {
		assert_ordered(source, target_order, lengthof(target_order));
		UT_ASSERT(strstr(source, "case RESOURCE_X_WRITER_SOURCE:") == NULL);
		UT_ASSERT(strstr(source,
			"cluster_pcm_lock_acquire_buffer(buf, PCM_LOCK_MODE_X") == NULL);
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

UT_TEST(test_aux_pending_direct_init_joins_exact_round_without_second_proof)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	const char *join_helper;
	const char *join_helper_end;
	const char *arm_helper;
	const char *arm_helper_end;
	const char *gate;
	const char *gate_end;
	const char *aux;
	const char *aux_end;
	static const char *const aux_order[] = {
		"LockBufferForAuxiliaryPageInit(",
		"cluster_bufmgr_pcm_arm_direct_init(",
		"CLUSTER_BUFMGR_PCM_DIRECT_INIT_CACHED_X",
		"cluster_bufmgr_pcm_join_aux_direct_init_exact(",
		"continue;",
		"cluster_bufmgr_pcm_gate_direct_init("
	};

	UT_ASSERT(source != NULL);
	if (source == NULL)
		return;
	assert_ordered(source, aux_order, lengthof(aux_order));
	arm_helper = strstr(source, "cluster_bufmgr_pcm_arm_direct_init(");
	arm_helper_end = arm_helper != NULL
		? strstr(arm_helper, "\n}\n") : NULL;
	UT_ASSERT(arm_helper != NULL && arm_helper_end != NULL);
	if (arm_helper != NULL && arm_helper_end != NULL) {
		UT_ASSERT(strstr(arm_helper,
			"cluster_pcm_direct_init_proof_arm(") < arm_helper_end);
		UT_ASSERT(strstr(arm_helper,
			"cluster_pcm_direct_init_aux_pending_observer_validate(")
			< arm_helper_end);
	}
	join_helper = strstr(source,
		"cluster_bufmgr_pcm_join_aux_direct_init_exact(");
	join_helper_end = join_helper != NULL
		? strstr(join_helper, "\n}\n") : NULL;
	UT_ASSERT(join_helper != NULL && join_helper_end != NULL);
	if (join_helper != NULL && join_helper_end != NULL) {
		UT_ASSERT(strstr(join_helper,
			"cluster_gcs_resource_x_target_direct_init_join_exact(")
			< join_helper_end);
		UT_ASSERT(strstr(join_helper,
			"cluster_gcs_resource_x_target_direct_init_acquire_exact(") == NULL
			|| strstr(join_helper,
				"cluster_gcs_resource_x_target_direct_init_acquire_exact(")
				> join_helper_end);
		UT_ASSERT(strstr(join_helper,
			"cluster_pcm_own_reservation_begin_exact(") == NULL
			|| strstr(join_helper,
				"cluster_pcm_own_reservation_begin_exact(") > join_helper_end);
		UT_ASSERT(strstr(join_helper,
			"cluster_resource_x_writer_path_snapshot(") == NULL
			|| strstr(join_helper,
				"cluster_resource_x_writer_path_snapshot(") > join_helper_end);
	}
	aux = strstr(source, "LockBufferForAuxiliaryPageInit(");
	aux_end = aux != NULL ? strstr(aux, "\n}\n") : NULL;
	UT_ASSERT(aux != NULL && aux_end != NULL);
	gate = strstr(source, "cluster_bufmgr_pcm_gate_direct_init(");
	gate_end = gate != NULL ? strstr(gate, "\n}\n") : NULL;
	UT_ASSERT(gate != NULL && gate_end != NULL);
	if (gate != NULL && gate_end != NULL)
		UT_ASSERT(strstr(gate,
			"cluster_pcm_direct_init_target_pending_validate(")
			< gate_end);
	free(source);
}

UT_TEST(test_valid_n_s_x_without_proof_uses_target_or_s_reservation)
{
	char *source = read_source(BUFMGR_SOURCE_PATH);
	static const char *const order[]
		= { "if (pcm_mode == PCM_LOCK_MODE_X)",
			"cluster_bufmgr_pcm_x_writer_prepare_target(",
			"else",
			"cluster_bufmgr_pcm_begin_grant_reservation_wait(",
			"cluster_pcm_lock_acquire_buffer(",
			"buf, PCM_LOCK_MODE_S, &retry_denied" };

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
		= { "cluster_gcs_resource_x_target_direct_init_acquire_exact(",
			"aux_pin_required ? &creation_reobserve : NULL",
			"resource_x_result == RESOURCE_X_APPLY_NOT_FOUND && creation_reobserve",
			"resource_x_result != RESOURCE_X_APPLY_APPLIED",
			"cluster_pcm_own_abort_grant_after_error(",
			"cluster_bufmgr_resource_x_writer_report_failure(",
			"cluster_pcm_direct_init_target_commit_validate(",
			"cluster_bufmgr_pcm_x_writer_track_target_direct_init(",
			"return;" };

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
		= { "cluster_gcs_resource_x_target_direct_init_acquire_exact(", "PG_CATCH();",
			"cluster_pcm_own_abort_grant_after_error(",
			"\"direct-init Resource-X acquire\"", "PG_RE_THROW();" };

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
		/* The refusal arm sits between target acquire and the ERROR report,
		 * and only the barrier-aware entry can consume it. */
		static const char *const refusal_order[]
			= { "cluster_gcs_resource_x_target_acquire_until_exact(",
				"cluster_pcm_lock_resource_x_gate_snapshot(&gate)",
				"gate.phase != RESOURCE_X_GATE_OPEN",
				"*pcm_barrier_refused = true",
				"return NULL",
				"cluster_bufmgr_resource_x_writer_report_failure(" };

		static const char *const wrapper_signature[]
			= { "ClusterLockBufferExclusiveBarrierAware(Buffer buffer,",
				"ClusterBufferBarrierSiteId site_id,",
				"bool *pin_replaced)" };

		assert_ordered(bufmgr, wrapper_signature, lengthof(wrapper_signature));
		assert_ordered(bufmgr, refusal_order, lengthof(refusal_order));
		free(bufmgr);
	}
	if (heapam != NULL) {
		static const char *const pretoast_order[] = { "PGRAC: vm barrier unwind (update pre-toast)",
													  "LockBuffer(buffer, BUFFER_LOCK_UNLOCK)",
													  "cluster_heap_vm_barrier_warm",
													  "ReleaseBuffer(vmbuffer)",
													  "vmbuffer = InvalidBuffer",
													  "cluster_heap_lock_with_vm_repin",
													  "goto l2;" };
		static const char *const requalify_order[]
			= { "PGRAC: BARRIER_CLOSED caller-owned unwind",
				"if (!old_tuple_temp_locked)",
				"LockBuffer(buffer, BUFFER_LOCK_UNLOCK)",
				"cluster_heap_vm_barrier_warm",
				"ReleaseBuffer(vmbuffer)",
				"vmbuffer = InvalidBuffer",
				"cluster_heap_lock_with_vm_repin",
				"goto l2;" };
		static const char *const reacquire_order[]
			= { "PGRAC: vm barrier unwind (update reacquire)",
				"LockBuffer(newbuf, BUFFER_LOCK_UNLOCK)",
				"ReleaseBuffer(newbuf)",
				"LockBuffer(buffer, BUFFER_LOCK_UNLOCK)",
				"cluster_heap_vm_barrier_warm",
				"goto l_pgrac_reacquire;" };
		static const char *const delete_order[] = { "PGRAC: vm barrier unwind (delete requalify)",
													"LockBuffer(buffer, BUFFER_LOCK_UNLOCK)",
													"cluster_heap_vm_barrier_warm",
													"ReleaseBuffer(vmbuffer)",
													"vmbuffer = InvalidBuffer",
													"cluster_heap_lock_with_vm_repin",
													"goto l1;" };

		/* The warm helper itself may only run with no content lock held:
		 * it must take and drop the map-page lock, nothing else. */
		static const char *const warm_order[]
			= { "cluster_heap_vm_barrier_warm(Buffer *vmbuf, Buffer *alias,",
				"ClusterLockBufferExclusiveAuxiliaryAliasAware(vmbuf, alias, NULL, context)",
				"LockBuffer(*vmbuf, BUFFER_LOCK_UNLOCK)" };

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
		const char *repin_branch
			= update != NULL
				  ? strstr(update, "if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))")
				  : NULL;
		const char *repin_branch_end
			= repin_branch != NULL ? strstr(repin_branch, "goto l2;") : NULL;
		const char *repin_branch_helper
			= repin_branch != NULL ? strstr(repin_branch, "cluster_heap_lock_with_vm_repin(")
								   : NULL;
		const char *success_tail
			= update != NULL ? strstr(update, "recptr = log_heap_update(") : NULL;
		const char *vm_unlock
			= success_tail != NULL
				  ? strstr(success_tail,
						   "if (vm_locked)\n\t\tLockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);")
				  : NULL;
		const char *vm_release
			= vm_unlock != NULL ? strstr(vm_unlock, "ReleaseBuffer(vmbuffer);") : NULL;
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
			= { "HEAPBLK_TO_MAPBLOCK(heapBlk)", "ReadRecentBuffer(", "VISIBILITYMAP_FORKNUM",
				"recent_buffer" };

		UT_ASSERT(repin != NULL);
		UT_ASSERT(repin_end != NULL);
		if (repin != NULL && repin_end != NULL)
			assert_ordered(repin, repin_order, lengthof(repin_order));
		free(visibilitymap);
	}
}

/* VM/FSM pin-only readers cannot retain a caller pin across a distributed
 * Resource-X conversion.  Ordinary X, direct-init leader, and pending
 * follower all use the same stack-only handoff: release every private ref,
 * wait, then repin only the same BufferTag in the same descriptor before any
 * content lock or page access. */
UT_TEST(test_vm_fsm_resource_x_wait_releases_and_exactly_repins)
{
	char *bufmgr = read_source(BUFMGR_SOURCE_PATH);
	const char *handoff;
	const char *handoff_end;
	const char *ordinary;
	const char *ordinary_end;
	const char *direct;
	const char *direct_end;
	const char *follower;
	const char *follower_end;
	static const char *const handoff_order[] = {
		"cluster_pcm_x_revoke_finish_mode(expected_resource, 0)",
		"GetPrivateRefCount(buffer)",
		"BufferTagsEqual(expected_resource, &buf->tag)",
		"handoff->tag = buf->tag",
		"ReleaseBuffer(buffer)",
		"ReadRecentBuffer(",
		"BufferTagsEqual(&handoff->tag, &buf->tag)"
	};
	static const char *const ordinary_order[] = {
		"cluster_bufmgr_pcm_aux_pin_handoff_begin(",
		"cluster_bufmgr_pcm_x_writer_prepare_target(",
		"cluster_bufmgr_pcm_aux_pin_handoff_finish_exact(",
		"LWLockAcquire("
	};
	static const char *const direct_order[] = {
		"aux_pin_required = kind == CLUSTER_PCM_DIRECT_INIT_VM",
		"cluster_pcm_own_reservation_begin_exact(",
		"if (aux_pin_required\n\t\t&& !cluster_bufmgr_pcm_aux_pin_handoff_begin(",
		"cluster_gcs_resource_x_target_direct_init_acquire_exact(",
		"if (aux_pin_required\n\t\t\t\t&& !cluster_bufmgr_pcm_aux_pin_handoff_finish_exact(",
		"cluster_pcm_direct_init_target_commit_validate("
	};
	static const char *const follower_order[] = {
		"cluster_bufmgr_pcm_aux_pin_handoff_begin(",
		"cluster_gcs_resource_x_target_direct_init_join_exact(",
		"cluster_bufmgr_resource_x_wait_retry(",
		"cluster_bufmgr_pcm_aux_pin_handoff_finish_exact("
	};

	UT_ASSERT(bufmgr != NULL);
	if (bufmgr == NULL)
		return;
	handoff = strstr(bufmgr, "\ncluster_bufmgr_pcm_aux_pin_handoff_begin(");
	handoff_end = handoff != NULL
		? strstr(handoff,
			"\nstatic bool\ncluster_bufmgr_pcm_aux_pin_handoff_finish_exact(")
		: NULL;
	ordinary = strstr(bufmgr, "\nLockBufferInternal(");
	ordinary_end = ordinary != NULL
		? strstr(ordinary, "\nvoid\nLockBuffer(") : NULL;
	direct = strstr(bufmgr, "\ncluster_bufmgr_pcm_gate_direct_init(");
	direct_end = direct != NULL
		? strstr(direct, "\n}\n\n#endif") : NULL;
	follower = strstr(bufmgr,
		"\ncluster_bufmgr_pcm_join_aux_direct_init_exact(");
	follower_end = follower != NULL
		? strstr(follower, "\ntypedef enum ClusterBufmgrPcmDirectInitArmResult")
		: NULL;

	UT_ASSERT(handoff != NULL);
	UT_ASSERT(handoff_end != NULL);
	UT_ASSERT(ordinary != NULL);
	UT_ASSERT(ordinary_end != NULL);
	UT_ASSERT(direct != NULL);
	UT_ASSERT(direct_end != NULL);
	UT_ASSERT(follower != NULL);
	UT_ASSERT(follower_end != NULL);
	if (handoff != NULL && handoff_end != NULL)
	{
		const char *pregrant_current_image = strstr(
			handoff,
			"cluster_bufmgr_pcm_current_image_locked(buf, buf_state)");

		assert_ordered(handoff, handoff_order, lengthof(handoff_order));
		UT_ASSERT(pregrant_current_image == NULL
			|| pregrant_current_image >= handoff_end);
	}
	if (ordinary != NULL && ordinary_end != NULL)
		assert_ordered(ordinary, ordinary_order, lengthof(ordinary_order));
	if (direct != NULL && direct_end != NULL)
		assert_ordered(direct, direct_order, lengthof(direct_order));
	if (follower != NULL && follower_end != NULL)
		assert_ordered(follower, follower_order, lengthof(follower_order));
	free(bufmgr);
}

/* Proof arm still requires the caller's fresh pin.  Once that proof has
 * atomically published exact VM/FSM GRANT_PENDING, however, the approved
 * pin handoff deliberately drives the Resource-X round with shared refcount
 * zero.  The live token/T2/T3 sidecars and fork-exact descriptor shape must
 * cover that bounded interval; MAIN/INIT must retain their pin requirement. */
UT_TEST(test_aux_direct_init_pending_lifecycle_survives_pin_handoff_only)
{
	char *bufmgr = read_source(BUFMGR_SOURCE_PATH);
	const char *known_new;
	const char *known_new_end;
	const char *candidate;
	const char *candidate_end;

	UT_ASSERT(bufmgr != NULL);
	if (bufmgr == NULL)
		return;
	known_new = strstr(bufmgr,
		"\ncluster_bufmgr_pcm_direct_init_known_new_locked(");
	known_new_end = known_new != NULL
		? strstr(known_new,
			"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_n_direct_init_candidate_exact(")
		: NULL;
	candidate = known_new_end;
	candidate_end = candidate != NULL
		? strstr(candidate,
			"\n/* PGRAC adaptation for the Stage-8 remote non-requester S-holder path.")
		: NULL;

	UT_ASSERT(known_new != NULL);
	UT_ASSERT(known_new_end != NULL);
	UT_ASSERT(candidate != NULL);
	UT_ASSERT(candidate_end != NULL);
	if (known_new != NULL && known_new_end != NULL)
	{
		const char *aux_guard = strstr(known_new,
			"aux_pin_handoff_protected");
		const char *pending = strstr(known_new,
			"PCM_OWN_FLAG_GRANT_PENDING");
		const char *writer = strstr(known_new,
			"cluster_pcm_own_writer_activation_token_get");
		const char *resource_x = strstr(known_new,
			"cluster_pcm_own_resource_x_activation_generation_get");
		const char *fork_vm = strstr(known_new, "VISIBILITYMAP_FORKNUM");
		const char *fork_fsm = strstr(known_new, "FSM_FORKNUM");

		UT_ASSERT(aux_guard != NULL && aux_guard < known_new_end);
		UT_ASSERT(pending != NULL && pending < known_new_end);
		UT_ASSERT(writer != NULL && writer < known_new_end);
		UT_ASSERT(resource_x != NULL && resource_x < known_new_end);
		UT_ASSERT(fork_vm != NULL && fork_vm < known_new_end);
		UT_ASSERT(fork_fsm != NULL && fork_fsm < known_new_end);
		UT_ASSERT(strstr(known_new,
			"BUF_STATE_GET_REFCOUNT(buf_state) != 0\n\t\t|| aux_pin_handoff_protected")
			< known_new_end);
	}
	if (candidate != NULL && candidate_end != NULL)
		UT_ASSERT(strstr(candidate,
			"cluster_bufmgr_pcm_direct_init_known_new_locked(buf, buf_state)")
			< candidate_end);
	free(bufmgr);
}

/* P0-20: RelationGetBufferForTuple acquires two heap content locks for a
 * cross-page UPDATE.  The second acquire must always be conditional: a
 * Resource-X conversion can start before a later type-17 freezes the first
 * page, so barrier-aware blocking is still an outer-lock wait.  On any miss,
 * release the first lock, resolve the second conversion with no content lock
 * held, and retry the ordered pair. */
UT_TEST(test_cross_page_heap_pair_second_acquire_never_waits_under_first)
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
			"ConditionalLockBuffer(second)",
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
	lower_call
		= lower_branch != NULL ? strstr(lower_branch, "cluster_hio_lock_buffer_pair(") : NULL;
	upper_branch = lower_branch != NULL ? strstr(lower_branch, "\n\t\telse\n\t\t{") : NULL;
	upper_call
		= upper_branch != NULL ? strstr(upper_branch, "cluster_hio_lock_buffer_pair(") : NULL;
	extension = relation != NULL ? strstr(relation, "Reacquire locks if necessary") : NULL;
	extension_call = extension != NULL ? strstr(extension, "cluster_hio_lock_buffer_pair(") : NULL;

	UT_ASSERT(helper != NULL);
	UT_ASSERT(helper_end != NULL);
	UT_ASSERT(pins != NULL);
	UT_ASSERT(pins_end != NULL);
	UT_ASSERT(relation != NULL);
	UT_ASSERT(relation_end != NULL);
	if (helper != NULL && helper_end != NULL)
	{
		assert_ordered(helper, helper_order, lengthof(helper_order));
		UT_ASSERT(strstr(helper,
			"ClusterLockBufferExclusiveBarrierAware(second,") == NULL);
	}
	UT_ASSERT(pins != NULL && pins_end != NULL && pins_call != NULL && pins_call < pins_end);
	UT_ASSERT(lower_branch != NULL && lower_call != NULL && upper_branch != NULL
			  && lower_call < upper_branch);
	UT_ASSERT(upper_branch != NULL && upper_call != NULL && relation_end != NULL
			  && upper_call < relation_end);
	UT_ASSERT(extension != NULL && extension_call != NULL && relation_end != NULL
			  && extension_call < relation_end);
	free(hio);
}

/* D11's public half is a single, observation-only static probe.  This source
 * contract does not claim a real receipt: it freezes the exact seven caller
 * identities and four event anchors that a probe-enabled external tracer
 * must later join dynamically. */
UT_TEST(test_d11_passive_identity_probe_has_exact_sites_and_phase_chain)
{
	char *bufmgr = read_source(BUFMGR_SOURCE_PATH);
	char *heapam = read_source(HEAPAM_SOURCE_PATH);
	char *hio = read_source(HIO_SOURCE_PATH);
	char *probes = read_source("../../backend/utils/probes.d");
	const char *common;
	const char *update;
	static const char *const probe_payload[]
		= { "probe r2__passive__identity__receipt(", "R2SiteId", "R2Phase",
			"R2SpcOid", "R2DbOid", "R2RelNumber", "R2ForkNumber",
			"R2BlockNumber", "R2Outcome", "R2ProofMask" };
	static const char *const common_chain[]
		= { "CLUSTER_BUFFER_BARRIER_PHASE_LOWER_REFUSED",
			"CLUSTER_BUFFER_BARRIER_OUTCOME_BARRIER_CLOSED",
			"CLUSTER_BUFFER_BARRIER_PROOF_LOWER_REFUSED",
			"cluster_bufmgr_pcm_unwind_barrier_refusal(",
			"CLUSTER_BUFFER_BARRIER_PHASE_COMMON_EMPTY",
			"CLUSTER_BUFFER_BARRIER_OUTCOME_EMPTY",
			"CLUSTER_BUFFER_BARRIER_PROOF_COMMON_EMPTY" };
	static const char *const heap_sites[]
		= { "CLUSTER_BUFFER_BARRIER_SITE_HEAP_DELETE_VM",
			"CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_PRETOAST_VM",
			"CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_PAIR_NEW_FIRST",
			"CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_PAIR_OLD_SECOND",
			"CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_OLD",
			"CLUSTER_BUFFER_BARRIER_SITE_HEAP_UPDATE_NEW" };
	static const char *const caller_reentry[]
		= { "CLUSTER_BUFFER_BARRIER_PHASE_CALLER_POST",
			"CLUSTER_BUFFER_BARRIER_OUTCOME_POSTCONDITION_OK",
			"CLUSTER_BUFFER_BARRIER_PROOF_CALLER_POST",
			"cluster_heap_vm_barrier_warm(",
			"CLUSTER_BUFFER_BARRIER_PHASE_REENTRY",
			"CLUSTER_BUFFER_BARRIER_OUTCOME_REQUALIFIED",
			"CLUSTER_BUFFER_BARRIER_PROOF_REENTRY" };

	UT_ASSERT(bufmgr != NULL);
	UT_ASSERT(heapam != NULL);
	UT_ASSERT(hio != NULL);
	UT_ASSERT(probes != NULL);
	if (probes != NULL)
	{
		UT_ASSERT_EQ(count_occurrences(probes,
									   "probe r2__passive__identity__receipt("), 1);
		assert_ordered(probes, probe_payload, lengthof(probe_payload));
		free(probes);
	}
	if (bufmgr != NULL)
	{
		common = strstr(bufmgr, "cluster_lockbuffer_barrier_refusal:");
		UT_ASSERT(common != NULL);
		if (common != NULL)
			assert_ordered(common, common_chain, lengthof(common_chain));
		UT_ASSERT_EQ(count_occurrences(bufmgr,
									   "TRACE_POSTGRESQL_R2_PASSIVE_IDENTITY_RECEIPT("), 1);
		free(bufmgr);
	}
	if (heapam != NULL)
	{
		const char *stamp_helper = strstr(heapam,
			"cluster_current_mx_stamp_lock_buffer(");
		static const char *const stamp_helper_chain[]
			= { "cluster_current_mx_stamp_lock_buffer(",
				"if (context != NULL)",
				"ClusterLockBufferExclusiveAuxiliaryAliasAware(",
				"if (barrier_aware)",
				"ClusterLockBufferExclusiveBarrierAware(",
				"buffer, site, pin_replaced)",
				"cluster_current_mx_stamp_cancel(first)",
				"cluster_current_mx_stamp_cancel(second)" };

		for (int i = 0; i < lengthof(heap_sites); i++)
			UT_ASSERT(strstr(heapam, heap_sites[i]) != NULL);
		UT_ASSERT(stamp_helper != NULL);
		if (stamp_helper != NULL)
			assert_ordered(stamp_helper, stamp_helper_chain,
						   lengthof(stamp_helper_chain));
		/* Legacy compatibility stays centralized in the MX helper. Primary
		 * VM consumers use the explicit-context branch, including DELETE. */
		UT_ASSERT_EQ(count_occurrences(heapam, "ClusterLockBufferExclusiveBarrierAware("), 1);

		update = strstr(heapam, "PGRAC: BARRIER_CLOSED caller-owned unwind");
		UT_ASSERT(update != NULL);
		if (update != NULL)
			assert_ordered(update, caller_reentry, lengthof(caller_reentry));
		free(heapam);
	}
	if (hio != NULL)
	{
		UT_ASSERT_EQ(count_occurrences(hio,
									   "ClusterLockBufferExclusiveBarrierAware("), 0);
		UT_ASSERT(strstr(hio, "ConditionalLockBuffer(second)") != NULL);
		free(hio);
	}
}

/* A VM/FSM handoff deliberately drops every backend pin before waiting on
 * Resource-X.  If replacement reuses the old BufferDesc during that window,
 * the stale numeric Buffer handle is no longer safe to return to the caller.
 * This is pre-mutation authority drift: the exact round/proof must be retried
 * through the relation read path, without globally fencing the current R4
 * gate and without silently treating the old descriptor as current. */
UT_TEST(test_aux_repin_replacement_restarts_from_fresh_relation_ref)
{
	char *bufmgr = read_source(BUFMGR_SOURCE_PATH);
	char *vm = read_source(VM_SOURCE_PATH);
	char *fsm = read_source(FSM_SOURCE_PATH);
	const char *join;
	const char *join_decl;
	const char *join_end;
	const char *gate;
	const char *gate_end;
	const char *aux;
	const char *aux_end;
	const char *repin;
	const char *post_t3;

	UT_ASSERT(bufmgr != NULL);
	UT_ASSERT(vm != NULL);
	UT_ASSERT(fsm != NULL);
	if (bufmgr != NULL)
	{
		join_decl = strstr(bufmgr,
			"static bool\ncluster_bufmgr_pcm_join_aux_direct_init_exact(");
		join = join_decl == NULL ? NULL : strstr(join_decl,
			"\ncluster_bufmgr_pcm_join_aux_direct_init_exact(");
		join_end = join == NULL ? NULL
			: strstr(join,
				"\ntypedef enum ClusterBufmgrPcmDirectInitArmResult");
		gate = strstr(bufmgr, "\ncluster_bufmgr_pcm_gate_direct_init(");
		gate_end = gate == NULL ? NULL : strstr(gate, "\n}\n\n#endif");
		aux = strstr(bufmgr, "\nLockBufferForAuxiliaryPageInit(");
		aux_end = aux == NULL ? NULL
			: strstr(aux,
				"\nBuffer\nLockBufferForVisibilityMapPageInit(");

		UT_ASSERT(join != NULL);
		UT_ASSERT(join_end != NULL);
		UT_ASSERT(gate != NULL);
		UT_ASSERT(gate_end != NULL);
		UT_ASSERT(aux != NULL);
		UT_ASSERT(aux_end != NULL);
		if (join != NULL && join_end != NULL)
		{
			UT_ASSERT(strstr(join, "pin_handoff.active = false;") < join_end);
			UT_ASSERT(strstr(join,
				"cluster_bufmgr_resource_x_fail_closed_current()") == NULL
				|| strstr(join,
					"cluster_bufmgr_resource_x_fail_closed_current()") >= join_end);
		}
		if (gate != NULL && gate_end != NULL)
		{
			UT_ASSERT(strstr(gate, "bool *pin_replaced") < gate_end);
			repin = strstr(gate,
				"cluster_bufmgr_pcm_aux_pin_handoff_finish_exact(");
			post_t3 = strstr(gate,
				"The terminal round has already completed T2/T3");
			UT_ASSERT(repin != NULL);
			UT_ASSERT(post_t3 != NULL);
			if (repin != NULL && post_t3 != NULL)
			{
				UT_ASSERT(repin < post_t3);
				UT_ASSERT(strstr(repin, "*pin_replaced = true;") < post_t3);
				UT_ASSERT(strstr(repin, "return false;") < post_t3);
				UT_ASSERT(strstr(repin, "if (pin_replaced == NULL)") < post_t3);
				UT_ASSERT(strstr(repin,
					"cluster_bufmgr_resource_x_fail_closed_current()") < post_t3);
			}
		}
		if (aux != NULL && aux_end != NULL)
		{
			UT_ASSERT(strstr(aux, "static Buffer") < aux);
			UT_ASSERT(strstr(aux, "bool pin_replaced = false;") < aux_end);
			UT_ASSERT(strstr(aux, "return InvalidBuffer;") < aux_end);
		}
		free(bufmgr);
	}
	if (vm != NULL)
	{
		const char *lock = strstr(vm,
			"buf = LockBufferForVisibilityMapPageInit(buf);");
		const char *invalid = lock == NULL ? NULL
			: strstr(lock, "if (!BufferIsValid(buf))");
		const char *retry = invalid == NULL ? NULL
			: strstr(invalid, "continue;");

		UT_ASSERT(lock != NULL);
		UT_ASSERT(invalid != NULL);
		UT_ASSERT(retry != NULL);
		if (lock != NULL && invalid != NULL && retry != NULL)
			UT_ASSERT(lock < invalid && invalid < retry);
		free(vm);
	}
	if (fsm != NULL)
	{
		const char *lock = strstr(fsm,
			"buf = LockBufferForFreeSpaceMapPageInit(buf);");
		const char *invalid = lock == NULL ? NULL
			: strstr(lock, "if (!BufferIsValid(buf))");
		const char *retry = invalid == NULL ? NULL
			: strstr(invalid, "continue;");

		UT_ASSERT(lock != NULL);
		UT_ASSERT(invalid != NULL);
		UT_ASSERT(retry != NULL);
		if (lock != NULL && invalid != NULL && retry != NULL)
			UT_ASSERT(lock < invalid && invalid < retry);
		free(fsm);
	}
}

/* Ordinary VM writers use the same deliberate pinless Resource-X interval as
 * direct-init.  A descriptor replacement is therefore a caller-owned retry,
 * not a cluster-wide gate failure and not a client ERROR.  The barrier-aware
 * wrapper must expose replacement separately, and heapam must invalidate the
 * stale numeric Buffer before returning to its existing relation-level repin
 * and requalification paths. */
UT_TEST(test_ordinary_aux_repin_replacement_reaches_heap_retry)
{
	char *bufmgr = read_source(BUFMGR_SOURCE_PATH);
	char *heapam = read_source(HEAPAM_SOURCE_PATH);
	const char *wrapper;
	const char *wrapper_end;
	const char *refusal;
	const char *helper;
	const char *helper_end;

	UT_ASSERT(bufmgr != NULL);
	UT_ASSERT(heapam != NULL);
	if (bufmgr != NULL)
	{
		wrapper = strstr(bufmgr,
			"ClusterLockBufferExclusiveBarrierAware(Buffer buffer,");
		wrapper_end = wrapper == NULL ? NULL
			: strstr(wrapper, "\n}\n\n/*\n * Resource-X aware EXCLUSIVE lock");
		refusal = strstr(bufmgr, "cluster_lockbuffer_barrier_refusal:");

		UT_ASSERT(wrapper != NULL);
		UT_ASSERT(wrapper_end != NULL);
		UT_ASSERT(refusal != NULL);
		if (wrapper != NULL && wrapper_end != NULL)
		{
			UT_ASSERT(strstr(wrapper, "bool *pin_replaced") < wrapper_end);
			UT_ASSERT(strstr(wrapper, "&transient_refused") < wrapper_end);
			UT_ASSERT(strstr(wrapper, "*pin_replaced = replaced") < wrapper_end);
			UT_ASSERT(strstr(wrapper, "&& !replaced") < wrapper_end);
		}
		if (refusal != NULL)
		{
			const char *common_empty = strstr(refusal,
				"CLUSTER_BUFFER_BARRIER_PHASE_COMMON_EMPTY");

			UT_ASSERT(common_empty != NULL);
			if (common_empty != NULL)
			{
				UT_ASSERT(strstr(refusal, "if (pcm_pin_replaced != NULL)")
					< common_empty);
				UT_ASSERT(strstr(refusal, "*pcm_pin_replaced = true;")
					< common_empty);
			}
		}
		free(bufmgr);
	}
	if (heapam != NULL)
	{
		helper = strstr(heapam, "cluster_current_mx_stamp_lock_buffer(");
		helper_end = helper == NULL ? NULL : strstr(helper, "\n}\n");
		UT_ASSERT(helper != NULL);
		UT_ASSERT(helper_end != NULL);
		if (helper != NULL && helper_end != NULL)
		{
			UT_ASSERT(strstr(helper, "bool *pin_replaced") < helper_end);
			UT_ASSERT(strstr(helper,
				"ClusterLockBufferExclusiveBarrierAware(buffer, site,")
				< helper_end);
		}
		UT_ASSERT(strstr(heapam, "refused_pin_replaced = false;") != NULL);
		UT_ASSERT(strstr(heapam, "if (refused_pin_replaced)") != NULL);
		UT_ASSERT(strstr(heapam, "vmbuffer = InvalidBuffer;") != NULL);
		free(heapam);
	}
}

UT_TEST(test_vm_clear_observation_uses_existing_pcm_relation_scope)
{
	char *heapam = read_source(HEAPAM_SOURCE_PATH);
	const char *cursor;
	int leg;

	UT_ASSERT(!cluster_pcm_x_relation_number_tracked(1255, false));
	UT_ASSERT(cluster_pcm_x_relation_number_tracked(16386, false));
	UT_ASSERT(cluster_pcm_x_relation_number_tracked(1255, true));
	UT_ASSERT_NOT_NULL(heapam);
	if (heapam == NULL)
		return;
	cursor = strstr(heapam, "/* clear PD_ALL_VISIBLE flags, reset all visibilitymap bits */");
	UT_ASSERT_NOT_NULL(cursor);
	for (leg = 0; cursor != NULL && leg < 2; leg++) {
		const char *scope = strstr(cursor, "bool vm_diagnostic_tracked");
		const char *clear = strstr(cursor, "visibilitymap_clear_locked(relation,");
		const char *publish = strstr(cursor, "cluster_pcm_vm_clear_note(");
		const char *predicate
			= scope == NULL ? NULL : strstr(scope, "cluster_pcm_x_relation_number_tracked(");
		const char *gap = scope == NULL ? NULL : strstr(scope, "else if (vm_diagnostic_tracked");
		const char *read = scope == NULL ? NULL : strstr(scope, "if (vm_diagnostic_tracked");
		const char *close = gap == NULL ? NULL : strstr(gap, "#endif");

		UT_ASSERT(scope != NULL && clear != NULL && publish != NULL);
		UT_ASSERT(predicate != NULL && clear != NULL && predicate < clear);
		UT_ASSERT(read != NULL && clear != NULL && read < clear);
		UT_ASSERT(gap != NULL && clear != NULL && gap < clear);
		/* The cluster observation conditional ends before the real clear. */
		UT_ASSERT(close != NULL && clear != NULL && close < clear);
		UT_ASSERT(clear != NULL && publish != NULL && clear < publish);
		cursor = publish == NULL ? NULL : publish + strlen("cluster_pcm_vm_clear_note(");
	}
	UT_ASSERT(leg == 2);
	free(heapam);
}

/* The real HIO tests exercise allocation; this census binds the other
 * TEMP_LOCK caller to its existing exact repin helper before same-page use. */
UT_TEST(test_temp_lock_releases_vm_before_nested_work_and_requalifies)
{
	char *heapam = read_source(HEAPAM_SOURCE_PATH);
	const char *start;
	const char *end;
	const char *release;
	const char *nested;
	const char *same;
	const char *tokens[] = { "!RelationUsesLocalBuffers(relation)", "vmbuffer = InvalidBuffer;",
							 "ReleaseBuffer(vmbuffer_new);", "vmbuffer_new = InvalidBuffer;" };
	size_t i;

	UT_ASSERT_NOT_NULL(heapam);
	if (heapam == NULL)
		return;
	start = strstr(heapam, "TEMP_LOCK is already published.");
	UT_ASSERT_NOT_NULL(start);
	if (start == NULL) {
		free(heapam);
		return;
	}
	end = strstr(start, "#endif");
	release = strstr(start, "ReleaseBuffer(vmbuffer);");
	nested = strstr(start, "heap_toast_insert_or_update(");
	UT_ASSERT(end != NULL && release != NULL && release < end);
	UT_ASSERT(nested != NULL && end != NULL && end < nested);
	for (i = 0; i < lengthof(tokens); i++) {
		const char *match = strstr(start, tokens[i]);

		UT_ASSERT(match != NULL && end != NULL && match < end);
	}
	same = strstr(start, "cluster_heap_lock_with_vm_repin(relation, block, buffer, &vmbuffer);");
	UT_ASSERT(same != NULL && nested != NULL && nested < same);
	UT_ASSERT(same != NULL && strstr(same, "pagefree = PageGetHeapFreeSpace(page);") != NULL);
	free(heapam);
}

int
main(void)
{
	UT_PLAN(33);
	UT_RUN(test_valid_read_miss_proof);
	UT_RUN(test_valid_extend_proof);
	UT_RUN(test_valid_vm_and_fsm_proofs);
	UT_RUN(test_proof_is_single_use_and_kind_exact);
	UT_RUN(test_identity_mismatch_rejects_buf_tag_generation_and_token);
	UT_RUN(test_reuse_dirty_and_shape_are_rejected);
	UT_RUN(test_state_s_and_live_reservations_are_rejected_by_class);
	UT_RUN(test_aux_pending_observer_accepts_only_exact_existing_reservation);
	UT_RUN(test_revalidate_rejects_state_pin_and_reservation_changes);
	UT_RUN(test_missing_backend_pin_is_rejected);
	UT_RUN(test_vm_fsm_fork_and_valid_shape_are_exact);
	UT_RUN(test_target_pending_reservation_remains_bound_to_consumed_known_new_proof);
	UT_RUN(test_target_post_t3_commit_revalidates_exact_known_new_identity);
	UT_RUN(test_target_fresh_generation_zero_is_exact_known_new_identity);
	UT_RUN(test_bufmgr_consumes_proof_before_reservation_and_wire);
	UT_RUN(test_target_direct_init_uses_exact_resource_x_round_without_legacy_fallback);
	UT_RUN(test_read_miss_and_found_hit_have_no_raw_unproven_lock);
	UT_RUN(test_extend_proof_is_after_zeroextend_and_before_lock);
	UT_RUN(test_vm_fsm_use_dedicated_init_wrappers);
	UT_RUN(test_aux_pending_direct_init_joins_exact_round_without_second_proof);
	UT_RUN(test_valid_n_s_x_without_proof_uses_target_or_s_reservation);
	UT_RUN(test_direct_init_one_shot_image_cannot_return_without_x);
	UT_RUN(test_wire_throw_exact_aborts_reservation_before_rethrow);
	UT_RUN(test_precrit_vm_barrier_refusal_unwinds_to_caller);
	UT_RUN(test_heap_update_drops_vm_pin_across_heap_pcm_wait);
	UT_RUN(test_vm_fsm_resource_x_wait_releases_and_exactly_repins);
	UT_RUN(test_aux_direct_init_pending_lifecycle_survives_pin_handoff_only);
	UT_RUN(test_cross_page_heap_pair_second_acquire_never_waits_under_first);
	UT_RUN(test_d11_passive_identity_probe_has_exact_sites_and_phase_chain);
	UT_RUN(test_aux_repin_replacement_restarts_from_fresh_relation_ref);
	UT_RUN(test_ordinary_aux_repin_replacement_reaches_heap_retry);
	UT_RUN(test_vm_clear_observation_uses_existing_pcm_relation_scope);
	UT_RUN(test_temp_lock_releases_vm_before_nested_work_and_requalifies);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
