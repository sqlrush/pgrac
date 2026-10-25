/*-------------------------------------------------------------------------
 * test_cluster_relmap_xid_order.c
 *    Run the real relmap producer across explicit backend boundaries.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_xid_order.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

/* Keep the producer, committed-image validation and merge unchanged. */
#include "../../backend/utils/cache/relmapper.c"
#include "../../backend/cluster/cluster_relmap_owner.c"
#include "../../backend/cluster/cluster_wal_writer.c"
#include "../../backend/cluster/cluster_wal_claim.c"
#include "cluster/cluster_lock_acquire.h"
#include "test_cluster_relmap_native_lock.h"
#include "test_cluster_relmap_physical_pair.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_shared_catalog = true;
bool cluster_enabled = true, cluster_shared_config = true, enableFsync = true;
int cluster_node_id = 3;
int MyProcPid = 79;
BackendType MyBackendType = B_BACKEND;
volatile sig_atomic_t ShutdownRequestPending;
int wal_segment_size = 1024 * 1024;
char *cluster_wal_threads_dir = "/fixture-wal", *DataDir = "/fixture-data";
int cluster_injection_armed_count;
bool allowSystemTableMods;
Oid MyDatabaseId = 12345;
Oid MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
char *DatabasePath = "base/12345";
ProcessingMode Mode = NormalProcessing;
volatile uint32 CritSectionCount;
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static ClusterConf peer_conf;
ClusterConf *ClusterConfShmem;

static sigjmp_buf error_target;
static bool error_expected;
static int error_level;
static bool authority_held, authority_available, lock_available, local_held;
static bool fail_assignment;
static uint64 pending_generation;
static uint64 authority_pending[2];
static unsigned int discard_calls[2];
static unsigned int publish_calls[2], local_invalidations[2], ack_attempts[2];
static unsigned int ack_failures[2], unlock_calls;
enum PublishEvent
{
	PUBLISH_SHARED = 1, INVALIDATE_SHARED, ACK_SHARED,
	PUBLISH_DATABASE, INVALIDATE_DATABASE, ACK_DATABASE, UNLOCK
};
static enum PublishEvent publish_trace[16];
static unsigned int publish_events;
static FullTransactionId assigned_xid, next_xid;
static unsigned int sequence, xid_order, lock_order, wal_order, stage_order;
static unsigned int full_requests, raw_requests, assignments;
static unsigned int xid_under_lock, xid_in_crit, local_acquires;
static unsigned int begins, inserts, flushes, stages, preserves, sync_commits;
static unsigned int registered;
static TransactionId xid_at_lock, xid_at_wal[2];
static xl_relmap_update wal_header;
static RelMapFile committed, updates, wal_image, staged_image[2];
static ClusterRelmapOwner staged_owner[2];
static bool staged_shared[2];
static Oid staged_dbid[2];
static uint64 writer_epoch, boot_incarnation, admitted_incarnation;
static ClusterWalSourceRef writer_ref;
static bool writer_available;
static TimeLineID insert_timeline;
static unsigned int writer_reads, observed_epoch_reads;
static unsigned int drift_at, drift_kind;

/* New stage-only tests share the original providers below. */
static LWLock *v2_retention_lock;
static bool v2_retention_held, v2_xidgen_held;
static void (*v2_preserve_observer)(RelFileLocator locator, bool at_commit);
static bool v2_terminal_context;
static bool v2_abort_callback_context;
static ClusterRelmapV2PairResult (*v2_pair_observer)(const ClusterRelmapV2Namespace *, ClusterRelmapV2View *);
static bool (*v2_write_observer)(const ClusterRelmapV2Namespace *, uint64,
	const ClusterRelmapV2Owner *, const RelMapFile *, ClusterRelmapRetentionDirectory *, LWLock *);
static bool (*v2_prepared_pair_observer)(const ClusterRelmapV2Image *);
static void (*v2_send_observer)(const SharedInvalidationMessage *, int);
static ClusterSinvalAckResult (*v2_ack_observer)(const SharedInvalidationMessage *, int);
static void (*v2_unlock_observer)(void);
static bool v2_native_lock_active;
static ClusterRelmapV2ClearResult (*v2_terminal_observer)(const ClusterRelmapV2Image *, bool);
static bool v2_postcommit_error_stack;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
MemoryContext ErrorContext;
MemoryContext CurrentMemoryContext;

/* Original runtime providers are explicit fixtures; token checks are real. */
uint64 GetSystemIdentifier(void) { return UINT64CONST(0x1122334455667788); }
uint64 cluster_epoch_get_current(void) { return writer_epoch; }
uint64 cluster_qvotec_get_self_incarnation(void) { return boot_incarnation; }
uint64
cluster_membership_get_last_admitted_incarnation(int32 node)
{
	UT_ASSERT_EQ(node, cluster_node_id);
	return admitted_incarnation;
}
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	UT_ASSERT_EQ(node, cluster_node_id);
	return CLUSTER_MEMBER_MEMBER;
}
bool cluster_external_fence_runtime_active(void) { return true; }
bool cluster_reconfig_has_pending_prebump_stage(void) { return false; }
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	writer_reads++;
	*out = writer_ref;
	return writer_available;
}
bool
cluster_wal_thread_initialized_writer_matches(const ClusterWalSourceRef *ref pg_attribute_unused(),
											 uint64 epoch pg_attribute_unused())
{
	return false;
}
bool
cluster_wal_thread_clean_writer_matches(const ClusterWalSourceRef *ref pg_attribute_unused(),
									   uint64 epoch pg_attribute_unused())
{
	return false;
}
void
cluster_write_fence_observe(ClusterWriteFenceObservation *out)
{
	memset(out, 0, sizeof(*out));
	out->enforcing = out->attached = out->engaged = out->allowed = true;
	out->epoch_current = out->authorized_epoch = writer_epoch;
	out->now_us = 100;
	out->expiry_us = 10000;
}
bool IsTransactionState(void) { return !v2_terminal_context; }
bool RecoveryInProgress(void) { return false; }
TimeLineID GetWALInsertionTimeLine(void) { return insert_timeline; }
bool
LWLockHeldByMe(LWLock *lock)
{
	if (lock == v2_retention_lock)
		return v2_retention_held;
	if (lock == XidGenLock)
		return v2_xidgen_held;
	return lock == RelationMappingLock && local_held;
}
FullTransactionId GetTopFullTransactionIdIfAny(void) { return assigned_xid; }

static void
change_original_owner(void)
{
	switch (drift_kind)
	{
		case 0: writer_epoch++; break;
		case 1: boot_incarnation++; break;
		case 2: admitted_incarnation++; break;
		case 3: writer_ref.claim.claim_sha256[0]++; break;
		case 4:
			assigned_xid = FullTransactionIdFromU64(U64FromFullTransactionId(assigned_xid)
												 + UINT64CONST(0x100000000));
			break;
		case 5: authority_held = false; break;
		case 6: insert_timeline++; break;
		case 7: ShutdownRequestPending = true; break;
		default: abort();
	}
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected Assert: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel < ERROR)
		return false;
	if (!error_expected)
		abort();
	error_level = elevel;
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errcode(int code pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	if (v2_postcommit_error_stack && error_level == ERROR && PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	siglongjmp(error_target, 1);
}

/* The allocator is a fixture; observations test the real caller's ordering. */
static FullTransactionId
assign_top(void)
{
	xid_order = ++sequence;
	xid_under_lock += local_held;
	xid_in_crit += CritSectionCount != 0;
	if (fail_assignment)
		ereport(ERROR, (errmsg("top xid assignment failed")));
	if (!FullTransactionIdIsValid(assigned_xid))
	{
		assigned_xid = next_xid;
		assignments++;
	}
	return assigned_xid;
}

FullTransactionId
GetTopFullTransactionId(void)
{
	full_requests++;
	return assign_top();
}

TransactionId
GetTopTransactionId(void)
{
	raw_requests++;
	return XidFromFullTransactionId(assign_top());
}

bool
cluster_relmap_authority_x_held(void)
{
	if (v2_native_lock_active)
		return test_relmap_native_lock_held();
	return authority_held;
}

bool
cluster_relmap_authority_x_lock(void)
{
	if (v2_native_lock_active)
	{
		authority_held = test_relmap_native_lock_acquire();
		return authority_held;
	}
	authority_held = lock_available;
	return authority_held;
}

bool
cluster_relmap_authority_read_header(bool shared pg_attribute_unused(),
									   Oid dbid pg_attribute_unused(),
									   ClusterRelmapAuthorityHeader *out)
{
	UT_ASSERT(authority_held);
	memset(out, 0, sizeof(*out));
	out->committed_generation = 7;
	out->pending_generation = pending_generation ? pending_generation :
		authority_pending[shared ? 0 : 1];
	return authority_available;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock == v2_retention_lock)
	{
		UT_ASSERT(mode == LW_EXCLUSIVE && !v2_retention_held);
		v2_retention_held = true;
		return true;
	}
	UT_ASSERT(lock == RelationMappingLock && mode == LW_EXCLUSIVE);
	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
	local_held = true;
	local_acquires++;
	lock_order = ++sequence;
	xid_at_lock = XidFromFullTransactionId(assigned_xid);
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (lock == v2_retention_lock)
	{
		UT_ASSERT(v2_retention_held);
		v2_retention_held = false;
		return;
	}
	UT_ASSERT(lock == RelationMappingLock && local_held);
	local_held = false;
}

bool
cluster_relmap_authority_read_committed(bool shared, Oid dbid,
										  char *out, uint32 *len)
{
	UT_ASSERT(authority_held && local_held);
	UT_ASSERT_EQ(dbid, shared ? InvalidOid : MyDatabaseId);
	memcpy(out, &committed, sizeof(committed));
	*len = sizeof(committed);
	if (drift_at == 1)
		change_original_owner();
	return true;
}

/* The cluster path must never fall back to a node-local map file. */
int
OpenTransientFile(const char *name pg_attribute_unused(), int flags pg_attribute_unused())
{
	abort();
}

int
CloseTransientFile(int fd pg_attribute_unused())
{
	abort();
}

void
XLogBeginInsert(void)
{
	UT_ASSERT(local_held && CritSectionCount == 1);
	begins++;
	registered = 0;
}

void
XLogRegisterData(char *data, uint32 len)
{
	if (registered++ == 0)
	{
		UT_ASSERT_EQ(len, MinSizeOfRelmapUpdate);
		memcpy(&wal_header, data, len);
	}
	else
	{
		UT_ASSERT_EQ(registered, 2);
		UT_ASSERT_EQ(len, sizeof(wal_image));
		memcpy(&wal_image, data, len);
	}
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT_EQ(rmid, RM_RELMAP_ID);
	UT_ASSERT_EQ(info, XLOG_RELMAP_UPDATE);
	UT_ASSERT_EQ(registered, 2);
	UT_ASSERT(local_held && CritSectionCount == 1 && inserts < 2);
	xid_at_wal[inserts] = XidFromFullTransactionId(assigned_xid);
	wal_order = ++sequence;
	inserts++;
	return (XLogRecPtr) (1000 + inserts);
}

void
XLogFlush(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(lsn, 1000 + inserts);
	UT_ASSERT(local_held && CritSectionCount == 1);
	flushes++;
	if (drift_at == 2)
		change_original_owner();
}

uint64
cluster_reconfig_get_observed_epoch(int32 node)
{
	UT_ASSERT_EQ(node, cluster_node_id);
	observed_epoch_reads++;
	return 909;
}

void
cluster_relmap_authority_write_pending(bool shared, Oid dbid, const char *image,
										 uint32 len, uint64 generation,
										 const ClusterRelmapOwner *owner)
{
	UT_ASSERT(authority_held && local_held && CritSectionCount == 0 && stages < 2);
	UT_ASSERT_EQ(flushes, inserts);
	UT_ASSERT_EQ(generation, 8);
	UT_ASSERT_EQ(len, sizeof(RelMapFile));
	UT_ASSERT(memcmp(image, &wal_image, len) == 0);
	UT_ASSERT_EQ(wal_header.dbid, dbid);
	UT_ASSERT_EQ(wal_header.tsid, shared ? GLOBALTABLESPACE_OID : MyDatabaseTableSpace);
	UT_ASSERT_EQ(wal_header.nbytes, len);
	staged_shared[stages] = shared;
	staged_dbid[stages] = dbid;
	staged_owner[stages] = *owner;
	memcpy(&staged_image[stages], image, len);
	authority_pending[shared ? 0 : 1] = generation;
	stage_order = ++sequence;
	stages++;
}

/* File I/O stays a fixture; the producer and transaction callbacks are real. */
void
cluster_relmap_authority_discard_pending(bool shared, Oid dbid, uint64 generation)
{
	unsigned int slot = shared ? 0 : 1;

	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
	UT_ASSERT_EQ(dbid, shared ? InvalidOid : MyDatabaseId);
	UT_ASSERT_EQ(generation, 8);
	UT_ASSERT_EQ(authority_pending[slot], generation);
	authority_pending[slot] = 0;
	discard_calls[slot]++;
}

static void
record_publish_event(enum PublishEvent event)
{
	UT_ASSERT(publish_events < lengthof(publish_trace));
	if (publish_events < lengthof(publish_trace))
		publish_trace[publish_events++] = event;
}

void
cluster_relmap_authority_publish(bool shared, Oid dbid, uint64 generation)
{
	unsigned int slot = shared ? 0 : 1;

	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
	UT_ASSERT_EQ(dbid, shared ? InvalidOid : MyDatabaseId);
	UT_ASSERT_EQ(generation, 8);
	UT_ASSERT_EQ(authority_pending[slot], generation);
	authority_pending[slot] = 0;
	publish_calls[slot]++;
	record_publish_event(shared ? PUBLISH_SHARED : PUBLISH_DATABASE);
}

void
SendSharedInvalidMessages(const SharedInvalidationMessage *msgs, int n)
{
	unsigned int slot = msgs[0].rm.dbId == InvalidOid ? 0 : 1;

	if (v2_send_observer)
	{
		v2_send_observer(msgs, n);
		return;
	}
	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(msgs[0].rm.id, SHAREDINVALRELMAP_ID);
	UT_ASSERT_EQ(msgs[0].rm.dbId, slot == 0 ? InvalidOid : MyDatabaseId);
	local_invalidations[slot]++;
	record_publish_event(slot == 0 ? INVALIDATE_SHARED : INVALIDATE_DATABASE);
}

/*
 * Network boundary only.  The real shared-catalog API returns only DONE;
 * injected failures exercise the callback's existing defensive branches.
 */
ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *msgs, int n)
{
	unsigned int slot = msgs[0].rm.dbId == InvalidOid ? 0 : 1;

	if (v2_ack_observer)
		return v2_ack_observer(msgs, n);
	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(msgs[0].rm.id, SHAREDINVALRELMAP_ID);
	UT_ASSERT_EQ(msgs[0].rm.dbId, slot == 0 ? InvalidOid : MyDatabaseId);
	UT_ASSERT(cluster_relmap_publish_arms[slot].armed);
	ack_attempts[slot]++;
	if (ack_attempts[slot] <= ack_failures[slot])
		return ack_attempts[slot] % 2 ? CLUSTER_SINVAL_ACK_TIMEOUT :
			CLUSTER_SINVAL_ACK_ENQUEUE_FAILED;
	record_publish_event(slot == 0 ? ACK_SHARED : ACK_DATABASE);
	return CLUSTER_SINVAL_ACK_DONE;
}

void
cluster_relmap_authority_x_unlock(void)
{
	if (v2_native_lock_active)
	{
		test_relmap_native_lock_release();
		authority_held = test_relmap_native_lock_held();
		return;
	}
	if (v2_unlock_observer)
	{
		v2_unlock_observer();
		return;
	}
	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
	unlock_calls++;
	record_publish_event(UNLOCK);
	authority_held = false;
}

void
RelationPreserveStorage(RelFileLocator locator, bool istemp)
{
	if (v2_preserve_observer)
	{
		v2_preserve_observer(locator, istemp);
		return;
	}
	UT_ASSERT(local_held && !istemp);
	UT_ASSERT_EQ(locator.relNumber, updates.mappings[0].mapfilenumber);
	preserves++;
}

void
ForceSyncCommit(void)
{
	UT_ASSERT(!local_held && CritSectionCount == 0);
	sync_commits++;
}

void
cluster_injection_run(const char *name pg_attribute_unused())
{
	abort();
}

bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

static void
reset(bool preassigned)
{
	authority_held = local_held = fail_assignment = error_expected = false;
	authority_available = lock_available = true;
	pending_generation = 0;
	memset(authority_pending, 0, sizeof(authority_pending));
	memset(discard_calls, 0, sizeof(discard_calls));
	memset(publish_calls, 0, sizeof(publish_calls));
	memset(local_invalidations, 0, sizeof(local_invalidations));
	memset(ack_attempts, 0, sizeof(ack_attempts));
	memset(ack_failures, 0, sizeof(ack_failures));
	memset(publish_trace, 0, sizeof(publish_trace));
	publish_events = unlock_calls = 0;
	memset(&peer_conf, 0, sizeof(peer_conf));
	peer_conf.node_count = 2;
	ClusterConfShmem = NULL;
	error_level = 0;
	sequence = xid_order = lock_order = wal_order = stage_order = 0;
	full_requests = raw_requests = assignments = 0;
	xid_under_lock = xid_in_crit = local_acquires = 0;
	begins = inserts = flushes = stages = preserves = sync_commits = 0;
	CritSectionCount = 0;
	ShutdownRequestPending = false;
	writer_available = true;
	writer_epoch = UINT64CONST(0x100000123);
	boot_incarnation = admitted_incarnation = 99;
	insert_timeline = 1;
	writer_reads = observed_epoch_reads = drift_at = drift_kind = 0;
	memset(&writer_ref, 0, sizeof(writer_ref));
	writer_ref.timeline = insert_timeline;
	writer_ref.claim.identity.system_identifier = GetSystemIdentifier();
	memset(writer_ref.claim.identity.storage_uuid, 3, 16);
	memset(writer_ref.claim.identity.authority_uuid, 5, 16);
	writer_ref.claim.identity.origin_thread_id = cluster_node_id + 1;
	writer_ref.claim.identity.origin_node_id = cluster_node_id;
	writer_ref.claim.identity.origin_owner_incarnation = boot_incarnation;
	writer_ref.claim.identity.root_lineage_seq = 9;
	writer_ref.claim.identity.thread_claim_created_at = 23;
	writer_ref.claim.database_incarnation = 7;
	writer_ref.claim.max_config_generation = 1;
	writer_ref.claim.claim_sha256[0] = 1;
	memset(cluster_relmap_publish_arms, 0, sizeof(cluster_relmap_publish_arms));
	memset(&shared_map, 0, sizeof(shared_map));
	memset(&local_map, 0, sizeof(local_map));
	memset(xid_at_wal, 0, sizeof(xid_at_wal));
	next_xid = FullTransactionIdFromEpochAndXid(7, 100);
	assigned_xid = preassigned ? next_xid : InvalidFullTransactionId;
	memset(&committed, 0, sizeof(committed));
	committed.magic = RELMAPPER_FILEMAGIC;
	committed.num_mappings = 1;
	committed.mappings[0].mapoid = 1259;
	committed.mappings[0].mapfilenumber = 500;
	INIT_CRC32C(committed.crc);
	COMP_CRC32C(committed.crc, (char *) &committed, offsetof(RelMapFile, crc));
	FIN_CRC32C(committed.crc);
	updates = committed;
	updates.mappings[0].mapfilenumber = 501;
}

static void
check_success(bool shared, bool preassigned)
{
	reset(preassigned);
	cluster_perform_relmap_update(shared, &updates);
	UT_ASSERT_EQ(full_requests, 1);
	UT_ASSERT_EQ(raw_requests, 0);
	UT_ASSERT_EQ(assignments, preassigned ? 0 : 1);
	UT_ASSERT_EQ(xid_under_lock, 0);
	UT_ASSERT_EQ(xid_in_crit, 0);
	UT_ASSERT(xid_order < lock_order && lock_order < wal_order && wal_order < stage_order);
	UT_ASSERT_EQ(xid_at_lock, XidFromFullTransactionId(next_xid));
	UT_ASSERT_EQ(xid_at_wal[0], XidFromFullTransactionId(next_xid));
	UT_ASSERT_EQ(U64FromFullTransactionId(assigned_xid), U64FromFullTransactionId(next_xid));
	UT_ASSERT_EQ(begins, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(stages, 1);
	UT_ASSERT_EQ(preserves, 1);
	UT_ASSERT_EQ(sync_commits, 1);
	UT_ASSERT_EQ(staged_shared[0], shared);
	UT_ASSERT_EQ(staged_dbid[0], shared ? InvalidOid : MyDatabaseId);
	UT_ASSERT_EQ(staged_owner[0].owner_node, cluster_node_id);
	UT_ASSERT_EQ(staged_owner[0].owner_xid, xid_at_wal[0]);
	UT_ASSERT_EQ(staged_owner[0].owner_epoch, UINT64CONST(0x100000123));
	UT_ASSERT_EQ(observed_epoch_reads, 0);
	UT_ASSERT(writer_reads > 0);
	UT_ASSERT_EQ(staged_owner[0].relmap_lsn, 1001);
	UT_ASSERT_EQ(staged_image[0].mappings[0].mapfilenumber, 501);
	UT_ASSERT(memcmp(shared ? &shared_map : &local_map,
					 &staged_image[0], sizeof(RelMapFile)) == 0);
	UT_ASSERT(cluster_relmap_publish_arms[shared ? 0 : 1].armed);
	UT_ASSERT(!local_held && CritSectionCount == 0);
}

static void
check_error(bool shared, bool assignment_reached)
{
	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
	{
		cluster_perform_relmap_update(shared, &updates);
		UT_ASSERT(false);
	}
	error_expected = false;
	UT_ASSERT_EQ(error_level, ERROR);
	UT_ASSERT_EQ(full_requests, assignment_reached ? 1 : 0);
	UT_ASSERT_EQ(raw_requests, 0);
	UT_ASSERT_EQ(assignments, 0);
	UT_ASSERT_EQ(local_acquires, 0);
	UT_ASSERT_EQ(begins, 0);
	UT_ASSERT_EQ(inserts, 0);
	UT_ASSERT_EQ(flushes, 0);
	UT_ASSERT_EQ(stages, 0);
	UT_ASSERT_EQ(preserves, 0);
	UT_ASSERT_EQ(sync_commits, 0);
	UT_ASSERT(!local_held && CritSectionCount == 0);
	UT_ASSERT(!cluster_relmap_publish_arms[0].armed && !cluster_relmap_publish_arms[1].armed);
}

UT_TEST(shared_assigns_before_lock_and_wal) { check_success(true, false); }
UT_TEST(database_assigns_before_lock_and_wal) { check_success(false, false); }
UT_TEST(shared_reuses_existing_full_xid) { check_success(true, true); }
UT_TEST(database_reuses_existing_full_xid) { check_success(false, true); }

UT_TEST(shared_assignment_error_has_no_local_effects)
{
	reset(false);
	fail_assignment = true;
	check_error(true, true);
}

UT_TEST(database_assignment_error_has_no_local_effects)
{
	reset(false);
	fail_assignment = true;
	check_error(false, true);
}

UT_TEST(unresolved_pending_rejects_before_assignment)
{
	reset(false);
	pending_generation = 8;
	check_error(true, false);
}

UT_TEST(unavailable_header_rejects_before_assignment)
{
	reset(false);
	authority_available = false;
	check_error(false, false);
}

UT_TEST(unavailable_authority_lock_rejects_before_assignment)
{
	reset(false);
	lock_available = false;
	check_error(true, false);
}

UT_TEST(unqualified_original_writer_rejects_before_local_effects)
{
	for (unsigned fault = 0; fault < 3; fault++)
	{
		reset(false);
		if (fault == 0)
			writer_available = false;
		if (fault == 1)
			admitted_incarnation++;
		if (fault == 2)
			writer_epoch = 0;
		error_expected = true;
		if (sigsetjmp(error_target, 1) == 0)
		{
			cluster_perform_relmap_update(fault != 1, &updates);
			UT_ASSERT(false);
		}
		error_expected = false;
		UT_ASSERT_EQ(error_level, ERROR);
		UT_ASSERT_EQ(full_requests, 1);
		UT_ASSERT_EQ(assignments, 1);
		UT_ASSERT_EQ(local_acquires + begins + inserts + flushes + stages, 0);
		UT_ASSERT_EQ(preserves + sync_commits + observed_epoch_reads, 0);
		UT_ASSERT(!local_held && CritSectionCount == 0);
		UT_ASSERT(!cluster_relmap_publish_arms[0].armed && !cluster_relmap_publish_arms[1].armed);
	}
}

static void
check_owner_drift(unsigned point)
{
	for (unsigned fault = 0; fault < 8; fault++)
	{
		reset(false);
		drift_at = point;
		drift_kind = fault;
		error_expected = true;
		if (sigsetjmp(error_target, 1) == 0)
		{
			cluster_perform_relmap_update((fault % 2) != 0, &updates);
			UT_ASSERT(false);
		}
		error_expected = false;
		UT_ASSERT_EQ(error_level, ERROR);
		UT_ASSERT_EQ(full_requests, 1);
		UT_ASSERT_EQ(assignments, 1);
		UT_ASSERT_EQ(raw_requests + xid_under_lock + xid_in_crit, 0);
		UT_ASSERT_EQ(local_acquires, 1);
		UT_ASSERT_EQ(begins, point == 2 ? 1 : 0);
		UT_ASSERT_EQ(inserts, point == 2 ? 1 : 0);
		UT_ASSERT_EQ(flushes, point == 2 ? 1 : 0);
		UT_ASSERT_EQ(stages + preserves + sync_commits + observed_epoch_reads, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT(!cluster_relmap_publish_arms[0].armed && !cluster_relmap_publish_arms[1].armed);
	}
}

UT_TEST(original_owner_drift_during_map_read_refuses_before_wal) { check_owner_drift(1); }
UT_TEST(original_owner_drift_during_flush_refuses_before_pending) { check_owner_drift(2); }

UT_TEST(both_namespaces_share_one_transaction_birth)
{
	reset(false);
	cluster_perform_relmap_update(true, &updates);
	cluster_perform_relmap_update(false, &updates);
	UT_ASSERT_EQ(full_requests, 2);
	UT_ASSERT_EQ(raw_requests, 0);
	UT_ASSERT_EQ(assignments, 1);
	UT_ASSERT_EQ(xid_under_lock, 0);
	UT_ASSERT_EQ(xid_in_crit, 0);
	UT_ASSERT_EQ(xid_at_wal[0], XidFromFullTransactionId(next_xid));
	UT_ASSERT_EQ(xid_at_wal[1], xid_at_wal[0]);
	UT_ASSERT_EQ(staged_owner[0].owner_xid, xid_at_wal[0]);
	UT_ASSERT_EQ(staged_owner[1].owner_xid, xid_at_wal[1]);
	UT_ASSERT_EQ(stages, 2);
	UT_ASSERT(cluster_relmap_publish_arms[0].armed && cluster_relmap_publish_arms[1].armed);
}

static void
check_abort_cleanup(unsigned int expected_shared, unsigned int expected_database)
{
	/* AbortTransaction releases LW locks before the relmap abort callback. */
	if (local_held)
		LWLockRelease(RelationMappingLock);
	AtAbort_ClusterRelmapPending();
	UT_ASSERT_EQ(discard_calls[0], expected_shared);
	UT_ASSERT_EQ(discard_calls[1], expected_database);
	UT_ASSERT(!cluster_relmap_publish_arms[0].armed && !cluster_relmap_publish_arms[1].armed);
	/* The original caller drops global X after pending cleanup returns. */
	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
	AtAbort_ClusterRelmapPending();
	UT_ASSERT_EQ(discard_calls[0], expected_shared);
	UT_ASSERT_EQ(discard_calls[1], expected_database);
}

static void
check_single_namespace_abort(bool shared)
{
	reset(false);
	cluster_perform_relmap_update(shared, &updates);
	UT_ASSERT_EQ(authority_pending[shared ? 0 : 1], 8);
	check_abort_cleanup(shared ? 1 : 0, shared ? 0 : 1);
	UT_ASSERT_EQ(authority_pending[0], 0);
	UT_ASSERT_EQ(authority_pending[1], 0);
	UT_ASSERT_EQ(stages, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(sync_commits, 1);
}

UT_TEST(shared_abort_discards_only_its_staged_namespace) { check_single_namespace_abort(true); }
UT_TEST(database_abort_discards_only_its_staged_namespace) { check_single_namespace_abort(false); }

UT_TEST(abort_discards_both_staged_namespaces_in_either_producer_order)
{
	for (unsigned int first = 0; first < 2; first++)
	{
		reset(false);
		cluster_perform_relmap_update(first == 0, &updates);
		cluster_perform_relmap_update(first != 0, &updates);
		UT_ASSERT_EQ(authority_pending[0], 8);
		UT_ASSERT_EQ(authority_pending[1], 8);
		check_abort_cleanup(1, 1);
		UT_ASSERT_EQ(authority_pending[0], 0);
		UT_ASSERT_EQ(authority_pending[1], 0);
		UT_ASSERT_EQ(assignments, 1);
		UT_ASSERT_EQ(stages, 2);
		UT_ASSERT_EQ(inserts, 2);
		UT_ASSERT_EQ(sync_commits, 2);
	}
}

static void
check_second_namespace_error(bool first_shared)
{
	unsigned int first_slot = first_shared ? 0 : 1;
	unsigned int second_slot = first_shared ? 1 : 0;

	for (unsigned int fault = 0; fault < 3; fault++)
	{
		reset(false);
		cluster_perform_relmap_update(first_shared, &updates);
		if (fault == 0)
			/* A different namespace already has another writer's residue. */
			authority_pending[second_slot] = 19;
		else
		{
			/* Real owner rejection before WAL, or after flush before stage. */
			drift_at = fault;
			drift_kind = 0;
		}
		error_expected = true;
		if (sigsetjmp(error_target, 1) == 0)
		{
			cluster_perform_relmap_update(!first_shared, &updates);
			UT_ASSERT(false);
		}
		error_expected = false;
		UT_ASSERT_EQ(error_level, ERROR);
		UT_ASSERT_EQ(stages, 1);
		UT_ASSERT_EQ(preserves, 1);
		UT_ASSERT_EQ(sync_commits, 1);
		UT_ASSERT_EQ(inserts, fault == 2 ? 2 : 1);
		UT_ASSERT_EQ(flushes, inserts);
		UT_ASSERT(cluster_relmap_publish_arms[first_slot].armed);
		UT_ASSERT(!cluster_relmap_publish_arms[second_slot].armed);
		UT_ASSERT_EQ(authority_pending[first_slot], 8);
		check_abort_cleanup(first_shared ? 1 : 0, first_shared ? 0 : 1);
		UT_ASSERT_EQ(authority_pending[first_slot], 0);
		UT_ASSERT_EQ(authority_pending[second_slot], fault == 0 ? 19 : 0);
		UT_ASSERT_EQ(stages, 1);
		UT_ASSERT_EQ(inserts, fault == 2 ? 2 : 1);
		UT_ASSERT_EQ(sync_commits, 1);
	}
}

UT_TEST(database_error_still_discards_prior_shared_stage) { check_second_namespace_error(true); }
UT_TEST(shared_error_still_discards_prior_database_stage) { check_second_namespace_error(false); }

UT_TEST(abort_before_stage_preserves_another_writers_pending)
{
	for (unsigned int slot = 0; slot < 2; slot++)
	{
		reset(false);
		authority_pending[slot] = 19;
		check_error(slot == 0, false);
		check_abort_cleanup(0, 0);
		UT_ASSERT_EQ(authority_pending[slot], 19);
		UT_ASSERT_EQ(authority_pending[1 - slot], 0);
		UT_ASSERT_EQ(stages, 0);
	}
}

static void
check_publish_trace(const enum PublishEvent *expected, unsigned int count)
{
	UT_ASSERT_EQ(publish_events, count);
	UT_ASSERT(memcmp(publish_trace, expected, count * sizeof(*expected)) == 0);
}

static void
check_commit_cleanup(unsigned int shared, unsigned int database,
					 unsigned int shared_acks, unsigned int database_acks)
{
	unsigned int events = publish_events;

	UT_ASSERT_EQ(publish_calls[0], shared);
	UT_ASSERT_EQ(publish_calls[1], database);
	UT_ASSERT_EQ(local_invalidations[0], shared);
	UT_ASSERT_EQ(local_invalidations[1], database);
	UT_ASSERT_EQ(ack_attempts[0], shared_acks);
	UT_ASSERT_EQ(ack_attempts[1], database_acks);
	UT_ASSERT_EQ(unlock_calls, 1);
	UT_ASSERT(!authority_held && !local_held && CritSectionCount == 0);
	UT_ASSERT(!cluster_relmap_publish_arms[0].armed && !cluster_relmap_publish_arms[1].armed);
	/* A repeated callback and a later abort callback must do no more work. */
	AtEOXact_ClusterRelmapPublish();
	AtAbort_ClusterRelmapPending();
	UT_ASSERT_EQ(publish_calls[0], shared);
	UT_ASSERT_EQ(publish_calls[1], database);
	UT_ASSERT_EQ(local_invalidations[0], shared);
	UT_ASSERT_EQ(local_invalidations[1], database);
	UT_ASSERT_EQ(ack_attempts[0], shared_acks);
	UT_ASSERT_EQ(ack_attempts[1], database_acks);
	UT_ASSERT_EQ(unlock_calls, 1);
	UT_ASSERT_EQ(publish_events, events);
	UT_ASSERT_EQ(discard_calls[0] + discard_calls[1], 0);
}

static void
check_single_namespace_commit(bool shared)
{
	const enum PublishEvent shared_peer[] = {PUBLISH_SHARED, INVALIDATE_SHARED, ACK_SHARED, UNLOCK};
	const enum PublishEvent local_peer[] = {PUBLISH_DATABASE, INVALIDATE_DATABASE, ACK_DATABASE, UNLOCK};
	const enum PublishEvent shared_solo[] = {PUBLISH_SHARED, INVALIDATE_SHARED, UNLOCK};
	const enum PublishEvent local_solo[] = {PUBLISH_DATABASE, INVALIDATE_DATABASE, UNLOCK};

	for (unsigned int peers = 0; peers < 2; peers++)
	{
		reset(false);
		ClusterConfShmem = peers ? &peer_conf : NULL;
		/* An unarmed namespace belongs to another writer. */
		authority_pending[shared ? 1 : 0] = 19;
		cluster_perform_relmap_update(shared, &updates);
		AtEOXact_ClusterRelmapPublish();
		check_publish_trace(peers ? (shared ? shared_peer : local_peer) :
							(shared ? shared_solo : local_solo), peers ? 4 : 3);
		check_commit_cleanup(shared ? 1 : 0, shared ? 0 : 1,
							 shared ? peers : 0, shared ? 0 : peers);
		UT_ASSERT_EQ(authority_pending[shared ? 0 : 1], 0);
		UT_ASSERT_EQ(authority_pending[shared ? 1 : 0], 19);
		UT_ASSERT_EQ(stages, 1);
		UT_ASSERT_EQ(sync_commits, 1);
	}
}

UT_TEST(shared_commit_publishes_invalidates_and_unlocks) { check_single_namespace_commit(true); }
UT_TEST(database_commit_publishes_invalidates_and_unlocks) { check_single_namespace_commit(false); }

UT_TEST(commit_publishes_both_namespaces_in_slot_order_after_either_producer_order)
{
	const enum PublishEvent expected[] = {PUBLISH_SHARED, INVALIDATE_SHARED, ACK_SHARED,
		PUBLISH_DATABASE, INVALIDATE_DATABASE, ACK_DATABASE, UNLOCK};

	for (unsigned int first = 0; first < 2; first++)
	{
		reset(false);
		ClusterConfShmem = &peer_conf;
		cluster_perform_relmap_update(first == 0, &updates);
		cluster_perform_relmap_update(first != 0, &updates);
		AtEOXact_ClusterRelmapPublish();
		check_publish_trace(expected, lengthof(expected));
		check_commit_cleanup(1, 1, 1, 1);
		UT_ASSERT_EQ(authority_pending[0] + authority_pending[1], 0);
		UT_ASSERT_EQ(assignments, 1);
		UT_ASSERT_EQ(stages, 2);
		UT_ASSERT_EQ(sync_commits, 2);
	}
}

UT_TEST(commit_retries_ack_before_next_namespace_and_unlock)
{
	const enum PublishEvent expected[] = {PUBLISH_SHARED, INVALIDATE_SHARED, ACK_SHARED,
		PUBLISH_DATABASE, INVALIDATE_DATABASE, ACK_DATABASE, UNLOCK};

	reset(false);
	ClusterConfShmem = &peer_conf;
	ack_failures[0] = ack_failures[1] = 2;
	cluster_perform_relmap_update(true, &updates);
	cluster_perform_relmap_update(false, &updates);
	AtEOXact_ClusterRelmapPublish();
	check_publish_trace(expected, lengthof(expected));
	check_commit_cleanup(1, 1, 3, 3);
	UT_ASSERT_EQ(authority_pending[0] + authority_pending[1], 0);
}

static void
check_ack_exhaustion(unsigned int failed_slot)
{
	const enum PublishEvent shared_failed[] = {PUBLISH_SHARED, INVALIDATE_SHARED};
	const enum PublishEvent local_failed[] = {PUBLISH_SHARED, INVALIDATE_SHARED, ACK_SHARED,
		PUBLISH_DATABASE, INVALIDATE_DATABASE};

	reset(false);
	ClusterConfShmem = &peer_conf;
	ack_failures[failed_slot] = 10;
	cluster_perform_relmap_update(true, &updates);
	cluster_perform_relmap_update(false, &updates);
	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
	{
		AtEOXact_ClusterRelmapPublish();
		UT_ASSERT(false);
	}
	error_expected = false;
	UT_ASSERT_EQ(error_level, PANIC);
	check_publish_trace(failed_slot == 0 ? shared_failed : local_failed,
						failed_slot == 0 ? lengthof(shared_failed) : lengthof(local_failed));
	UT_ASSERT_EQ(publish_calls[0], 1);
	UT_ASSERT_EQ(publish_calls[1], failed_slot == 1 ? 1 : 0);
	UT_ASSERT_EQ(local_invalidations[0], 1);
	UT_ASSERT_EQ(local_invalidations[1], failed_slot == 1 ? 1 : 0);
	UT_ASSERT_EQ(ack_attempts[0], failed_slot == 0 ? 10 : 1);
	UT_ASSERT_EQ(ack_attempts[1], failed_slot == 1 ? 10 : 0);
	UT_ASSERT(cluster_relmap_publish_arms[1].armed);
	UT_ASSERT_EQ(cluster_relmap_publish_arms[0].armed, failed_slot == 0);
	UT_ASSERT_EQ(authority_pending[0], 0);
	UT_ASSERT_EQ(authority_pending[1], failed_slot == 0 ? 8 : 0);
	UT_ASSERT_EQ(unlock_calls, 0);
	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
	UT_ASSERT_EQ(discard_calls[0] + discard_calls[1], 0);
	/* PANIC is observed at the boundary; do not simulate an abort afterwards. */
}

UT_TEST(shared_ack_exhaustion_panics_with_both_arms_retained) { check_ack_exhaustion(0); }
UT_TEST(database_ack_exhaustion_panics_with_remaining_arm_retained) { check_ack_exhaustion(1); }

UT_TEST(unarmed_commit_preserves_other_writers_pending_and_global_lock)
{
	reset(false);
	ClusterConfShmem = &peer_conf;
	authority_held = true;
	authority_pending[0] = authority_pending[1] = 19;
	AtEOXact_ClusterRelmapPublish();
	UT_ASSERT_EQ(publish_events, 0);
	UT_ASSERT_EQ(publish_calls[0] + publish_calls[1], 0);
	UT_ASSERT_EQ(local_invalidations[0] + local_invalidations[1], 0);
	UT_ASSERT_EQ(ack_attempts[0] + ack_attempts[1], 0);
	UT_ASSERT_EQ(unlock_calls, 0);
	UT_ASSERT_EQ(authority_pending[0], 19);
	UT_ASSERT_EQ(authority_pending[1], 19);
	UT_ASSERT(authority_held && !local_held && CritSectionCount == 0);
}

#include "test_cluster_relmap_producer_stage.inc"
#include "test_cluster_relmap_producer_prepare.inc"
#include "test_cluster_relmap_producer_terminal.inc"
#include "test_cluster_relmap_postcommit.inc"
#include "test_cluster_relmap_abort.inc"
#include "test_cluster_relmap_unarmed.inc"
#include "test_cluster_relmap_precommit.inc"
#include "test_cluster_relmap_native_lock.inc"
#include "test_cluster_relmap_physical_pair.inc"

int
main(void)
{
	UT_PLAN(60);
	UT_RUN(v2_physical_pairs_finish_before_notification_and_unlock);
	UT_RUN(v2_physical_stage_sync_refusal_keeps_original_batch);
	UT_RUN(v2_physical_second_finish_refusal_keeps_all_custody);
	UT_RUN(v2_native_lock_batch_finishes_before_original_release);
	UT_RUN(v2_native_lock_release_refusal_retains_original_custody);
	UT_RUN(v2_native_lock_refusals_do_not_enter_release);
	UT_RUN(v2_precommit_consumes_original_cci_batch_before_terminal);
	UT_RUN(v2_precommit_preflight_refusals_keep_original_queues);
	UT_RUN(v2_precommit_partial_batch_error_retains_all_input_and_debt);
	UT_RUN(v2_precommit_empty_reentry_and_capacity_keep_original_custody);
	UT_RUN(v2_unarmed_prepare_false_blocks_terminal_cleanup);
	UT_RUN(v2_unarmed_prepare_error_keeps_protection);
	UT_RUN(v2_unarmed_prepare_cannot_retry_or_change_polarity);
	UT_RUN(v2_pre_effect_refusals_preserve_normal_abort);
	UT_RUN(v2_abort_discards_all_receipts_before_unlock);
	UT_RUN(v2_abort_refusals_keep_original_custody);
	UT_RUN(v2_abort_partial_cleanup_is_one_shot);
	UT_RUN(v2_abort_unlock_error_and_empty_debt_isolation);
	UT_RUN(v2_postcommit_publishes_then_invalidates_before_unlock);
	UT_RUN(v2_postcommit_ack_retries_keep_storage_one_shot);
	UT_RUN(v2_postcommit_errors_and_source_loss_fail_stop);
	UT_RUN(v2_postcommit_preflight_storage_failures_and_empty_isolation);
	UT_RUN(v2_terminal_custody_uses_original_prepare_for_both_flavours);
	UT_RUN(v2_terminal_arm_refuses_without_replacing_original_custody);
	UT_RUN(v2_terminal_attempt_context_refusals_are_sticky);
	UT_RUN(v2_terminal_partial_unknown_and_errors_keep_all_custody);
	UT_RUN(v2_prepare_preserves_installs_and_forces_sync);
	UT_RUN(v2_prepare_refusals_keep_local_map_and_debt);
	UT_RUN(v2_staged_runtime_check_requires_original_exact_pin);
	UT_RUN(v2_prepare_errors_keep_original_failure_duties);
	UT_RUN(v2_original_stage_captures_merges_and_retains);
	UT_RUN(v2_original_stage_pre_wal_refusals_preserve_outputs);
	UT_RUN(v2_original_stage_late_refusals_keep_original_protection);
	UT_RUN(v2_original_stage_errors_leave_original_failure_duties);
	UT_RUN(shared_assigns_before_lock_and_wal);
	UT_RUN(database_assigns_before_lock_and_wal);
	UT_RUN(shared_reuses_existing_full_xid);
	UT_RUN(database_reuses_existing_full_xid);
	UT_RUN(shared_assignment_error_has_no_local_effects);
	UT_RUN(database_assignment_error_has_no_local_effects);
	UT_RUN(unresolved_pending_rejects_before_assignment);
	UT_RUN(unavailable_header_rejects_before_assignment);
	UT_RUN(unavailable_authority_lock_rejects_before_assignment);
	UT_RUN(unqualified_original_writer_rejects_before_local_effects);
	UT_RUN(original_owner_drift_during_map_read_refuses_before_wal);
	UT_RUN(original_owner_drift_during_flush_refuses_before_pending);
	UT_RUN(both_namespaces_share_one_transaction_birth);
	UT_RUN(shared_abort_discards_only_its_staged_namespace);
	UT_RUN(database_abort_discards_only_its_staged_namespace);
	UT_RUN(abort_discards_both_staged_namespaces_in_either_producer_order);
	UT_RUN(database_error_still_discards_prior_shared_stage);
	UT_RUN(shared_error_still_discards_prior_database_stage);
	UT_RUN(abort_before_stage_preserves_another_writers_pending);
	UT_RUN(shared_commit_publishes_invalidates_and_unlocks);
	UT_RUN(database_commit_publishes_invalidates_and_unlocks);
	UT_RUN(commit_publishes_both_namespaces_in_slot_order_after_either_producer_order);
	UT_RUN(commit_retries_ack_before_next_namespace_and_unlock);
	UT_RUN(shared_ack_exhaustion_panics_with_both_arms_retained);
	UT_RUN(database_ack_exhaustion_panics_with_remaining_arm_retained);
	UT_RUN(unarmed_commit_preserves_other_writers_pending_and_global_lock);
	UT_DONE();
	return ut_failed_count != 0;
}
