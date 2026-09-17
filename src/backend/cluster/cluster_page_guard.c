/*-------------------------------------------------------------------------
 *
 * cluster_page_guard.c
 *    STOP-06 no-wait PAGE target protection.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_guard.h"

#ifndef USE_CLUSTER_UNIT
#include "miscadmin.h"
#include "storage/lwlock.h"
#endif

#define RF_PAGE_GUARD_TRANCHE "RfPageGuard"

#ifndef USE_CLUSTER_UNIT
static LWLockPadded *page_guard_locks;
#else
static bool page_guard_locks[RF_PAGE_GUARD_PARTITIONS];
#endif

static bool
bytes_nonzero(const uint8 *bytes, Size length)
{
	uint8 value = 0;
	Size i;

	for (i = 0; i < length; i++)
		value |= bytes[i];
	return value != 0;
}

static bool
page_identity_valid(const RfPageIdentityV1 *identity)
{
	return identity != NULL && identity->system_identifier != 0
		   && bytes_nonzero(identity->storage_uuid, sizeof(identity->storage_uuid))
		   && identity->locator.spcOid != InvalidOid && identity->locator.dbOid != InvalidOid
		   && identity->locator.relNumber != InvalidRelFileNumber
		   && identity->blockno != InvalidBlockNumber && identity->reserved_zero == 0;
}

static bool
page_identity_equal(const RfPageIdentityV1 *left, const RfPageIdentityV1 *right)
{
	return left != NULL && right != NULL && left->system_identifier == right->system_identifier
		   && memcmp(left->storage_uuid, right->storage_uuid, 16) == 0
		   && RelFileLocatorEquals(left->locator, right->locator) && left->forknum == right->forknum
		   && left->blockno == right->blockno && left->reserved_zero == 0
		   && right->reserved_zero == 0;
}

static uint64
fingerprint_bytes(uint64 hash, const void *bytes, Size length)
{
	const uint8 *input = (const uint8 *)bytes;
	Size i;

	for (i = 0; i < length; i++) {
		hash ^= input[i];
		hash *= UINT64CONST(1099511628211);
	}
	return hash;
}

static uint64
page_identity_fingerprint(const RfPageIdentityV1 *identity)
{
	uint64 hash = UINT64CONST(1469598103934665603);

	hash = fingerprint_bytes(hash, &identity->system_identifier,
							 sizeof(identity->system_identifier));
	hash = fingerprint_bytes(hash, identity->storage_uuid, sizeof(identity->storage_uuid));
	hash = fingerprint_bytes(hash, &identity->locator.spcOid, sizeof(identity->locator.spcOid));
	hash = fingerprint_bytes(hash, &identity->locator.dbOid, sizeof(identity->locator.dbOid));
	hash = fingerprint_bytes(hash, &identity->locator.relNumber,
							 sizeof(identity->locator.relNumber));
	hash = fingerprint_bytes(hash, &identity->forknum, sizeof(identity->forknum));
	hash = fingerprint_bytes(hash, &identity->blockno, sizeof(identity->blockno));
	return hash == 0 ? UINT64CONST(1) : hash;
}

void
rf_page_guard_request_lwlocks_v1(void)
{
#ifndef USE_CLUSTER_UNIT
	RequestNamedLWLockTranche(RF_PAGE_GUARD_TRANCHE, RF_PAGE_GUARD_PARTITIONS);
#endif
}

void
rf_page_guard_shmem_init_v1(void)
{
#ifndef USE_CLUSTER_UNIT
	page_guard_locks = GetNamedLWLockTranche(RF_PAGE_GUARD_TRANCHE);
#else
	memset(page_guard_locks, 0, sizeof(page_guard_locks));
#endif
}

bool
rf_page_guard_preflight_v1(const RfPageIdentityV1 *identity, RfPageGuardPreflightV1 *preflight)
{
	uint64 fingerprint;

	if (preflight == NULL)
		return false;
	memset(preflight, 0, sizeof(*preflight));
	if (!page_identity_valid(identity))
		return false;
	fingerprint = page_identity_fingerprint(identity);
	preflight->page_identity = *identity;
	preflight->identity_fingerprint = fingerprint;
	preflight->partition = (uint32)(fingerprint & (RF_PAGE_GUARD_PARTITIONS - 1));
	preflight->state = RF_PAGE_GUARD_PREFLIGHTED;
	return true;
}

bool
rf_page_guard_promote_nowait_v1(const RfPageGuardPreflightV1 *preflight, RfPageGuardV1 *guard)
{
	uint64 fingerprint;

	if (preflight == NULL || guard == NULL || preflight->state != RF_PAGE_GUARD_PREFLIGHTED
		|| guard->state != RF_PAGE_GUARD_EMPTY || !page_identity_valid(&preflight->page_identity))
		return false;
	fingerprint = page_identity_fingerprint(&preflight->page_identity);
	if (fingerprint != preflight->identity_fingerprint
		|| preflight->partition >= RF_PAGE_GUARD_PARTITIONS
		|| preflight->partition != (uint32)(fingerprint & (RF_PAGE_GUARD_PARTITIONS - 1)))
		return false;
#ifndef USE_CLUSTER_UNIT
	if (page_guard_locks == NULL
		|| !LWLockConditionalAcquire(&page_guard_locks[preflight->partition].lock, LW_EXCLUSIVE))
		return false;
#else
	if (page_guard_locks[preflight->partition])
		return false;
	page_guard_locks[preflight->partition] = true;
#endif
	guard->page_identity = preflight->page_identity;
	guard->identity_fingerprint = fingerprint;
	guard->partition = preflight->partition;
#ifndef USE_CLUSTER_UNIT
	guard->owner_pid = MyProcPid;
#else
	guard->owner_pid = 1;
#endif
	guard->state = RF_PAGE_GUARD_PROMOTED;
	return true;
}

bool
rf_page_guard_revalidate_nowait_v1(const RfPageGuardV1 *guard, const RfPageIdentityV1 *identity)
{
	if (guard == NULL || guard->state != RF_PAGE_GUARD_PROMOTED
		|| guard->partition >= RF_PAGE_GUARD_PARTITIONS
		|| !page_identity_equal(&guard->page_identity, identity)
		|| guard->identity_fingerprint != page_identity_fingerprint(identity))
		return false;
#ifndef USE_CLUSTER_UNIT
	return page_guard_locks != NULL && guard->owner_pid == MyProcPid
		   && LWLockHeldByMeInMode(&page_guard_locks[guard->partition].lock, LW_EXCLUSIVE);
#else
	return guard->owner_pid == 1 && page_guard_locks[guard->partition];
#endif
}

bool
rf_page_guard_covers_nowait_v1(const RfPageGuardV1 *guard, const RfPageIdentityV1 *identity)
{
	uint64 fingerprint;

	if (guard == NULL || guard->state != RF_PAGE_GUARD_PROMOTED
		|| guard->partition >= RF_PAGE_GUARD_PARTITIONS || !page_identity_valid(identity))
		return false;
	fingerprint = page_identity_fingerprint(identity);
	if (guard->partition != (uint32)(fingerprint & (RF_PAGE_GUARD_PARTITIONS - 1)))
		return false;
#ifndef USE_CLUSTER_UNIT
	return page_guard_locks != NULL && guard->owner_pid == MyProcPid
		   && LWLockHeldByMeInMode(&page_guard_locks[guard->partition].lock, LW_EXCLUSIVE);
#else
	return guard->owner_pid == 1 && page_guard_locks[guard->partition];
#endif
}

void
rf_page_guard_release_v1(RfPageGuardV1 *guard)
{
	if (guard == NULL || guard->state != RF_PAGE_GUARD_PROMOTED
		|| guard->partition >= RF_PAGE_GUARD_PARTITIONS)
		return;
#ifndef USE_CLUSTER_UNIT
	if (page_guard_locks != NULL && guard->owner_pid == MyProcPid
		&& LWLockHeldByMeInMode(&page_guard_locks[guard->partition].lock, LW_EXCLUSIVE))
		LWLockRelease(&page_guard_locks[guard->partition].lock);
#else
	page_guard_locks[guard->partition] = false;
#endif
	guard->state = RF_PAGE_GUARD_RELEASED;
}
