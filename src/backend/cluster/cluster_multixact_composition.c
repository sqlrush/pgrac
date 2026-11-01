/* Operation-local assembly of immutable MultiXact composition metadata. */
#include "postgres.h"

#include "cluster/cluster_multixact_composition.h"
#include "common/hashfn.h"
#include "utils/memutils.h"

typedef struct CompositionSeenMember {
	TransactionId xid;
	uint32 status;
} CompositionSeenMember;

#define COMPOSITION_SEEN_DIRECTORY_ENTRIES 256
#define COMPOSITION_SEEN_DIRECTORIES 4
/* Each low-10-bit partition has at most 2^22 distinct uint32 XIDs. Collision
 * growth is allowed at fill >=0.1, so its last doubling can reach 2^26
 * buckets (not merely the normal 0.9 sizing). Both policies are bound below.
 * This is a storage partition, never a member-count admission limit. */
StaticAssertDecl(((uint64)sizeof(CompositionSeenMember) << 26) <= MaxAllocSize,
	"each composition seen partition must fit ordinary allocation limits");

#define SH_PREFIX composition_seen
#define SH_ELEMENT_TYPE CompositionSeenMember
#define SH_KEY_TYPE TransactionId
#define SH_KEY xid
#define SH_HASH_KEY(tb, key) murmurhash32(key)
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#define SH_USE_NONDEFAULT_ALLOCATOR
#define SH_FILLFACTOR 0.9
#define SH_GROW_MIN_FILLFACTOR 0.1
#include "lib/simplehash.h"

/* Keep the hash within ordinary per-allocation limits; allocation ERROR is
 * caught before any provisional output. No HUGE allocation is requested. */
static inline void *
composition_seen_allocate(composition_seen_hash *table, Size bytes)
{
	return MemoryContextAllocZero(table->ctx, bytes);
}

static inline void
composition_seen_free(composition_seen_hash *table, void *ptr)
{
	(void)table;
	pfree(ptr);
}

typedef struct CompositionSeenDirectory {
	composition_seen_hash *partitions[COMPOSITION_SEEN_DIRECTORY_ENTRIES];
} CompositionSeenDirectory;

struct ClusterMxCompositionReceive {
	MemoryContext context;
	ClusterMxCompositionReceiveStats *stats;
	void *source;
	Size source_bytes;
	uint32 total, accepted, updaters;
	bool complete;
	CompositionSeenDirectory *seen[COMPOSITION_SEEN_DIRECTORIES];
	ClusterMxCompositionPage *head, *tail;
};

StaticAssertDecl(sizeof(ClusterMxCompositionPage) < MaxAllocSize,
	"composition pages must fit ordinary allocation limits");

static composition_seen_hash *
composition_receive_partition(ClusterMxCompositionReceive *receive, TransactionId xid)
{
	uint32 directory = (xid >> 8) & (COMPOSITION_SEEN_DIRECTORIES - 1);
	uint32 slot = xid & (COMPOSITION_SEEN_DIRECTORY_ENTRIES - 1);
	CompositionSeenDirectory *page = receive->seen[directory];
	if (page == NULL) {
		page = MemoryContextAllocZero(receive->context, sizeof(*page));
		receive->seen[directory] = page;
	}
	if (page->partitions[slot] == NULL)
		page->partitions[slot] = composition_seen_create(receive->context, Min(receive->total, 16), NULL);
	return page->partitions[slot];
}

static void
composition_count(uint64 *counter)
{
	if (*counter != UINT64_MAX)
		(*counter)++;
}

static void
composition_release(ClusterMxCompositionReceive **op)
{
	if (op != NULL && *op != NULL) {
		MemoryContext context = (*op)->context;
		*op = NULL;
		MemoryContextDelete(context);
	}
}

static ClusterMxCompositionReceiveResult
composition_unknown(ClusterMxCompositionReceive **op, bool restart)
{
	if (op != NULL && *op != NULL) {
		composition_count(&(*op)->stats->unknown_results);
		if (restart)
			composition_count(&(*op)->stats->restart_requests);
		composition_release(op);
	}
	return MXCOMP_UNKNOWN;
}

bool
cluster_mx_composition_begin(ClusterMxCompositionReceive **op, const void *source,
	Size source_bytes, uint32 total, ClusterMxCompositionReceiveStats *stats)
{
	MemoryContext context;

	if (op != NULL && *op != NULL) {
		(void)composition_unknown(op, false);
		return false;
	}
	if (op == NULL || source == NULL || source_bytes == 0 || source_bytes > BLCKSZ
		|| total == 0 || total > INT32_MAX || stats == NULL) {
		if (stats != NULL)
			composition_count(&stats->unknown_results);
		return false;
	}
	context = AllocSetContextCreate(CurrentMemoryContext, "MultiXact composition receive",
		ALLOCSET_DEFAULT_SIZES);
	PG_TRY();
	{
		ClusterMxCompositionReceive *created = MemoryContextAllocZero(context, sizeof(*created));
		created->context = context;
		created->stats = stats;
		created->source = MemoryContextAlloc(context, source_bytes);
		memcpy(created->source, source, source_bytes);
		created->source_bytes = source_bytes;
		created->total = total;
		(void)composition_receive_partition(created, 0);
		*op = created;
	}
	PG_CATCH();
	{
		MemoryContextDelete(context);
		composition_count(&stats->unknown_results);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return true;
}

ClusterMxCompositionReceiveResult
cluster_mx_composition_accept(ClusterMxCompositionReceive **op, ClusterMxCompositionReadStatus status,
	const ClusterMxCompositionReply *reply)
{
	ClusterMxCompositionReceive *receive;
	bool valid = true;

	if (op == NULL || *op == NULL)
		return MXCOMP_UNKNOWN;
	receive = *op;
	if (status != MXCOMP_READ_OK || reply == NULL || receive->complete)
		return composition_unknown(op, status == MXCOMP_READ_RETRY);
	if (reply->source == NULL || reply->members == NULL
		|| reply->source_bytes != receive->source_bytes || reply->total != receive->total
		|| reply->first != receive->accepted || reply->count == 0
		|| reply->cap == 0 || reply->cap > CLUSTER_MX_COMPOSITION_PAGE_MEMBERS
		|| reply->count > reply->cap
		|| (uint64)reply->first + reply->count != reply->next
		|| reply->next > receive->total || reply->last != (reply->next == receive->total)
		|| reply->member_bytes != (uint64)reply->count * sizeof(*reply->members)
		|| reply->frame_bytes > BLCKSZ
		|| (uint64)reply->source_bytes + reply->member_bytes > reply->frame_bytes
		|| memcmp(reply->source, receive->source, receive->source_bytes) != 0)
		return composition_unknown(op, false);

	PG_TRY();
	{
		ClusterMxCompositionPage *page = MemoryContextAllocZero(receive->context, sizeof(*page));
		uint32 updaters = receive->updaters;
		for (uint32 i = 0; i < reply->count; i++) {
			const ClusterCurrentMxMemberDesc *member = &reply->members[i];
			bool found;
			if (!TransactionIdIsNormal(member->xid) || member->member_status > MaxMultiXactStatus
				|| member->reserved8[0] != 0 || member->reserved8[1] != 0 || member->reserved8[2] != 0
				|| (ISUPDATE_from_mxstatus(member->member_status) && ++updaters > 1)) {
				valid = false;
				break;
			}
			(void)composition_seen_insert(composition_receive_partition(receive, member->xid), member->xid, &found);
			if (found) {
				valid = false;
				break;
			}
			page->members[i] = *member;
		}
		if (valid) {
			page->first = reply->first;
			page->count = reply->count;
			if (receive->tail != NULL)
				receive->tail->next = page;
			else
				receive->head = page;
			receive->tail = page;
			receive->accepted = reply->next;
			receive->updaters = updaters;
			receive->complete = reply->last;
			composition_count(&receive->stats->accepted_frames);
		}
	}
	PG_CATCH();
	{
		(void)composition_unknown(op, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!valid)
		return composition_unknown(op, false);
	return receive->complete ? MXCOMP_COMPLETE : MXCOMP_PARTIAL;
}

uint32
cluster_mx_composition_next(const ClusterMxCompositionReceive *op)
{
	return op == NULL ? 0 : op->accepted;
}

const ClusterMxCompositionPage *
cluster_mx_composition_pages(const ClusterMxCompositionReceive *op, uint32 *total)
{
	if (total != NULL)
		*total = 0;
	if (op == NULL || !op->complete || total == NULL)
		return NULL;
	*total = op->total;
	return op->head;
}

void
cluster_mx_composition_finish(ClusterMxCompositionReceive **op)
{
	if (op != NULL && *op != NULL && !(*op)->complete)
		(void)composition_unknown(op, false);
	else
		composition_release(op);
}
