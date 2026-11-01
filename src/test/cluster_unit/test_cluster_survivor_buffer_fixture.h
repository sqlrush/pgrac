/* Native PinBuffer/UnpinBuffer and private refcount code are extracted whole.
 * Hash storage, ResourceOwner bookkeeping, descriptor memory and lock primitives
 * are host boundaries. No GCS grant or buffer IO is provided. Unexpected PCM,
 * mapping, wakeup or reclamation fails the fixture rather than succeeding.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_SURVIVOR_BUFFER_FIXTURE_H
#define TEST_CLUSTER_SURVIVOR_BUFFER_FIXTURE_H
#include "cluster/cluster_ko.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "storage/buf_internals.h"
#include "utils/memdebug.h"
#include "utils/resowner_private.h"
#include "test_cluster_survivor_buffer_type.inc"
#define REFCOUNT_ARRAY_ENTRIES 8
static PrivateRefCountEntry PrivateRefCountArray[REFCOUNT_ARRAY_ENTRIES];
static HTAB *PrivateRefCountHash;
static int32 PrivateRefCountOverflowed;
static uint32 PrivateRefCountClock;
static PrivateRefCountEntry *ReservedRefCountEntry;
static PrivateRefCountEntry pin_hash[32];
static bool pin_hash_used[32];
static unsigned pin_remembered;
static BufferDescPadded pin_descriptors[24];
int NBuffers = lengthof(pin_descriptors);
BufferDescPadded *BufferDescriptors = pin_descriptors;
LWLockPadded *MainLWLockArray;
int NLocBuffer;
static int32 pin_local_refs[2];
int32 *LocalRefCount = pin_local_refs;
ClusterPcmOwnEntry *ClusterPcmOwnArray; /* Absent: this test supplies no PCM authority. */
static void ReservePrivateRefCountEntry(void);
static PrivateRefCountEntry *NewPrivateRefCountEntry(Buffer buffer);
static PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer buffer, bool move);
static void ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref);
void *
hash_search(HTAB *table, const void *key, HASHACTION action, bool *found)
{
	int free_slot = -1;
	Buffer buffer = *(const Buffer *)key;
	UT_ASSERT(table == (HTAB *)pin_hash);
	if (found != NULL) *found = false;
	for (unsigned i = 0; i < lengthof(pin_hash); ++i) {
		if (!pin_hash_used[i]) { if (free_slot < 0) free_slot = i; continue; }
		if (pin_hash[i].buffer != buffer) continue;
		if (found != NULL) *found = true;
		if (action == HASH_REMOVE) pin_hash_used[i] = false;
		return &pin_hash[i];
	}
	if (action == HASH_FIND || action == HASH_REMOVE) return NULL;
	UT_ASSERT_EQ(action, HASH_ENTER); UT_ASSERT(free_slot >= 0);
	if (free_slot < 0) abort();
	memset(&pin_hash[free_slot], 0, sizeof(pin_hash[free_slot]));
	pin_hash_used[free_slot] = true; pin_hash[free_slot].buffer = buffer;
	return &pin_hash[free_slot];
}
void ResourceOwnerRememberBuffer(ResourceOwner owner, Buffer b)
{ UT_ASSERT(owner == CurrentResourceOwner); UT_ASSERT(b > 0); ++pin_remembered; }
void ResourceOwnerForgetBuffer(ResourceOwner owner, Buffer b)
{ UT_ASSERT(owner == CurrentResourceOwner); UT_ASSERT(b > 0 && pin_remembered > 0); --pin_remembered; }
static uint32 WaitBufHdrUnlocked(BufferDesc *buf pg_attribute_unused()) { abort(); }
uint32 LockBufHdr(BufferDesc *buf pg_attribute_unused()) { abort(); }
bool LWLockHeldByMe(LWLock *lock pg_attribute_unused()) { abort(); }
bool LWLockConditionalAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused()) { abort(); }
void ProcSendSignal(int pgprocno pg_attribute_unused()) { abort(); }
uint32 BufTableHashCode(BufferTag *tag pg_attribute_unused()) { abort(); }
ClusterPcmOwnResult
cluster_pcm_own_reclaim_read_image_exact(BufferDesc *buf pg_attribute_unused(),
	const ClusterPcmOwnSnapshot *published pg_attribute_unused(), uint64 *cleared pg_attribute_unused())
{ abort(); }
static bool
InvalidateBufferCommitLocked(BufferDesc *buf pg_attribute_unused(), const BufferTag *tag pg_attribute_unused(),
	uint32 hash pg_attribute_unused(), LWLock *lock pg_attribute_unused(), uint32 state pg_attribute_unused())
{ abort(); }
#include "test_cluster_survivor_buffer_native.inc"
static void
pin_return_begin(void)
{
	backend_return_begin(); UT_ASSERT_EQ(pin_remembered, 0);
	memset(PrivateRefCountArray, 0, sizeof(PrivateRefCountArray));
	memset(pin_hash, 0, sizeof(pin_hash)); memset(pin_hash_used, 0, sizeof(pin_hash_used));
	PrivateRefCountHash = (HTAB *)pin_hash; PrivateRefCountOverflowed = 0;
	PrivateRefCountClock = 0; ReservedRefCountEntry = NULL; NLocBuffer = 0;
	LocalRefCount = pin_local_refs; memset(pin_local_refs, 0, sizeof(pin_local_refs));
	memset(pin_descriptors, 0, sizeof(pin_descriptors));
	for (unsigned i = 0; i < lengthof(pin_descriptors); ++i) {
		pin_descriptors[i].bufferdesc.buf_id = i;
		pg_atomic_init_u32(&pin_descriptors[i].bufferdesc.state, BM_VALID);
	}
}
#endif
