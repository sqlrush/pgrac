/* Exact last-mutation WAL source for a versioned resident page.
 * In-memory values only; never DATA durability or writer authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PAGE_WAL_H
#define CLUSTER_PAGE_WAL_H

#include "cluster/cluster_page_stable_base.h"
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_wal_source.h"
#include "storage/buf.h"
#include "storage/bufpage.h"

typedef struct ClusterPageWalBindingV1 {
	ClusterWalSourceRef source;
	RfPageIdentityV1 identity;
	RfPageVersionV1 version;
	XLogRecPtr record_start;
	XLogRecPtr record_end;
	uint32 record_crc;
	uint8 rmid;
	uint8 info;
	uint16 flags;
} ClusterPageWalBindingV1;

#define CLUSTER_PAGE_WAL_NATIVE_FLUSHED UINT16_C(1)

/* One owned reference to the existing immutable claim pool, independent of
 * a BufferDesc. The keyed owner supplies its unchanged physical address.
 * Zero initialize; never copy a live reference, and release exactly once.
 * Retaining attribution is not a flush, ancestry or DATA certificate. */
typedef struct ClusterPageWalRefV1 {
	uint8 incarnation[16];
	uint64 token;
	XLogRecPtr start, end;
	uint32 crc;
	uint16 source_flags;
	uint8 rmid, info;
} ClusterPageWalRefV1;

extern bool cluster_page_wal_ref_retain_v1(const ClusterPageWalBindingV1 *binding,
										   ClusterPageWalRefV1 *out);
extern bool cluster_page_wal_ref_read_v1(const ClusterPageWalRefV1 *ref, RelFileLocator locator,
										 ForkNumber forknum, BlockNumber blockno,
										 ClusterPageWalBindingV1 *out);
extern bool cluster_page_wal_ref_release_v1(ClusterPageWalRefV1 *ref);

typedef enum ClusterPageWalCaptureResultV1 {
	CLUSTER_PAGE_WAL_CAPTURED,
	CLUSTER_PAGE_WAL_UNATTRIBUTED,
	CLUSTER_PAGE_WAL_INVARIANT_BROKEN,
} ClusterPageWalCaptureResultV1;

/* Process-local T2 preparation owns one source reference until publication
 * or release. Never copy a live preparation or prepare twice into it. */
typedef struct ClusterPageWalInstallV1 {
	ClusterPageWalBindingV1 binding;
	uint16 source_slot;
} ClusterPageWalInstallV1;

/* Pure carrier consistency. Validation cannot certify a new flush or grant
 * authority. The match helper also requires aligned page bytes and an
 * independently selected physical address. */
static inline bool
cluster_page_wal_binding_shape_v1(const ClusterPageWalBindingV1 *b)
{
	static const uint8 zero[32] = { 0 };
	const ClusterControlRootIdentity *id;
	if (b == NULL)
		return false;
	id = &b->source.claim.identity;
	return id->system_identifier != 0 && id->reserved42 == 0 && id->reserved60 == 0
		   && id->origin_thread_id > 0 && id->origin_thread_id <= PGRAC_PAGE_LSN_ORIGIN_MAX + 1
		   && id->origin_node_id == (int32)id->origin_thread_id - 1
		   && id->thread_claim_created_at > 0 && id->origin_owner_incarnation != 0
		   && id->root_lineage_seq != 0 && memcmp(id->storage_uuid, zero, 16) != 0
		   && memcmp(id->authority_uuid, zero, 16) != 0 && b->source.claim.database_incarnation != 0
		   && b->source.claim.max_config_generation != 0
		   && memcmp(b->source.claim.claim_sha256, zero, 32) != 0 && b->source.timeline != 0
		   && b->identity.system_identifier == id->system_identifier
		   && memcmp(b->identity.storage_uuid, id->storage_uuid, 16) == 0
		   && (b->identity.forknum == MAIN_FORKNUM || b->identity.forknum == VISIBILITYMAP_FORKNUM
			   || (b->identity.forknum == SPACE_FORKNUM && b->identity.blockno < 2))
		   && b->identity.reserved_zero == 0 && (b->flags & ~CLUSTER_PAGE_WAL_NATIVE_FLUSHED) == 0
		   && memcmp(b->version.segment_incarnation, zero, 16) != 0
		   && b->version.mutation_token != 0 && b->record_start != InvalidXLogRecPtr
		   && b->record_start < b->record_end;
}

static inline bool
cluster_page_wal_binding_matches_v1(const ClusterPageWalBindingV1 *b, RelFileLocator locator,
									ForkNumber forknum, BlockNumber blockno, Page page)
{
	int origin;
	return page != NULL && cluster_page_wal_binding_shape_v1(b)
		   && RelFileLocatorEquals(b->identity.locator, locator) && b->identity.forknum == forknum
		   && b->identity.blockno == blockno && !PageIsNew(page)
		   && ((PageHeader)page)->pd_block_scn == b->version.mutation_token
		   && PageGetLSN(page) == b->record_end && PageGetLSNOrigin(page, &origin)
		   && origin == b->source.claim.identity.origin_thread_id - 1;
}

/* Only the original selected native writer can certify a new flush. A
 * carrier already certified by that owner may relay it in the same namespace.
 * Caller keeps its original pin/ownership but need not hold a content lock;
 * it must revalidate the exact page binding after the potentially blocking I/O.
 * Failure/ERROR leaves output untouched and cannot certify DATA or ancestry. */
extern bool cluster_page_wal_flush_source_v1(const ClusterPageWalBindingV1 *binding,
											 ClusterPageWalBindingV1 *certified);
extern bool cluster_page_wal_same_mutation_v1(const ClusterPageWalBindingV1 *a,
											  const ClusterPageWalBindingV1 *b);

extern Size cluster_page_wal_shmem_size(void);
extern void cluster_page_wal_shmem_init(void);
extern void cluster_page_wal_shmem_register(void);

/* Cold redo failure latches before releasing the suspect page's content-X.
 * Allocation/error-free, shared across processes, reset only with new shared
 * memory. A local redo abort must never reopen DATA writes. */
extern bool cluster_page_wal_cold_redo_write_allowed_v1(void);
extern void cluster_page_wal_cold_redo_fail_v1(void);

/* Sole native WAL insertion owner after successful insertion, while its
 * registered buffer is still pinned and content-X. No allocation, I/O or
 * lock upgrade. On ordinary attribution failure the insertion owner must
 * clear the previous binding under the same content-X; absence is not a
 * durability receipt and does not discharge the original DATA obligation.
 * Broken buffer ownership or a mismatched result page is an invariant
 * failure, even when a source reference is temporarily unavailable.
 * End/start/CRC belong to the actual inserted record, not its retry candidate. */
extern ClusterPageWalCaptureResultV1
cluster_page_wal_capture_native_v1(Buffer buffer, const RfPageVersionEdgeEntryV1 *edge,
								   uint64 result_token, XLogRecPtr start, XLogRecPtr end,
								   uint32 crc, uint8 rmid, uint8 info);

/* Original private-WAL/resident publisher, immediately after installing its
 * successful record's bytes, version and LSN under the same content-X.
 * A private bulk image alone must never call this. No new WAL, allocation,
 * I/O or lock upgrade; the capture result has the same polarity as above. */
extern ClusterPageWalCaptureResultV1
cluster_page_wal_capture_published_v1(Buffer buffer, const RfPageVersionEdgeEntryV1 *edge,
									  uint64 result_token, XLogRecPtr end);

/* Original native SPACE publisher only, after stamping both result and LSN.
 * Observes the actual just-inserted record, never a guessed foreign LSN.
 * The caller retains content-X; unavailable attribution requires forget,
 * while wrong ownership/bytes/record identity remains an invariant failure. */
extern ClusterPageWalCaptureResultV1
cluster_page_wal_capture_space_v1(Buffer buffer, const ClusterSpaceIdentityKey *key,
								  XLogRecPtr end);

/* Clear attribution under the original pin/content-X, without allocation or
 * I/O. False means the caller no longer has the required buffer invariant. */
extern bool cluster_page_wal_forget_v1(Buffer buffer);

/* Original shared descriptor invalidation/reuse owner, with header locked
 * and no other user of the old residency. Never acquires a buffer lock. */
struct BufferDesc;
extern void cluster_page_wal_reset_reuse_locked(struct BufferDesc *buf);

/* Caller already pins and content-locks this descriptor and supplies the
 * lifecycle-qualified SPACE identity. Does not acquire any page or grant
 * write/flush authority. SPACE0/1 also decode and match the complete typed
 * identity, including its state/sequence/operation. Only SPACE may use a
 * tombstoned identity. Failure leaves output untouched. */
extern bool cluster_page_wal_read_v1(Buffer buffer, const ClusterSpaceIdentity *identity,
									 ClusterPageWalBindingV1 *out);

/*
 * First own record since the page was last clean (D S09 R-A22).  A capture
 * sets it when it is empty; later captures, installs and forget leave it.
 * All functions below require the descriptor header lock (and the content
 * lock the caller already holds for the page).  A present first record is
 * returned as its 48-byte reference; observe copies it without a source
 * reference, retain takes one the caller must move or release.
 */
typedef enum ClusterPageWalFirstResultV1 {
	CLUSTER_PAGE_WAL_FIRST_ABSENT,
	CLUSTER_PAGE_WAL_FIRST_PRESENT,
	CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED, /* version/LSN, no source: keep the lower */
	CLUSTER_PAGE_WAL_FIRST_INVALID,
} ClusterPageWalFirstResultV1;

extern ClusterPageWalFirstResultV1
cluster_page_wal_first_observe_locked_v1(struct BufferDesc *buf, ClusterPageWalRefV1 *out);
extern ClusterPageWalFirstResultV1
cluster_page_wal_first_retain_locked_v1(struct BufferDesc *buf, ClusterPageWalRefV1 *out);
/* The write owner, after TerminateBufferIO(true) and still under content
 * SHARE: clears the first record observed before the write only when the
 * page is clean now, the slot is unchanged and the written version covers
 * it, including a first record whose source could not be retained. Such a
 * record is never a handover binding. False keeps it (re-dirtied, drifted
 * or failed). */
extern bool cluster_page_wal_first_clear_written_locked_v1(struct BufferDesc *buf,
														   const ClusterPageWalRefV1 *observed,
														   uint64 written_token);
/* A producer whose receiver already holds this first record clears it. */
extern bool cluster_page_wal_first_handover_locked_v1(struct BufferDesc *buf,
													  const ClusterPageWalRefV1 *observed);

typedef struct ClusterPageWalDirtyFloorV1 {
	XLogRecPtr floor; /* least first record of the source; 0 = none */
	uint32 dirty;	  /* buffers with a first record */
	uint32 foreign;	  /* ... of another source (not compared numerically) */
	uint32 unattributed;
} ClusterPageWalDirtyFloorV1;

/* Lock-free scan of every buffer's first record for one WAL source. */
extern bool cluster_page_wal_dirty_floor_v1(const ClusterWalSourceRef *source,
											ClusterPageWalDirtyFloorV1 *out);

/* Carrier-only observation under the existing pin/content lock. Does not
 * establish current SPACE incarnation or qualify a DATA write. */
extern bool cluster_page_wal_snapshot_v1(Buffer buffer, ClusterPageWalBindingV1 *out);
/* Original output owner only: a failed dirty output retains its binding. */
extern bool cluster_page_wal_output_snapshot_v1(Buffer buffer, ClusterPageWalBindingV1 *out);

/* Strict frozen PI projection under the descriptor header lock. No pin or
 * content lock is borrowed; !BM_VALID/PI/N/no-I/O excludes byte mutation.
 * The caller must revalidate descriptor generation before any discard. */
extern bool cluster_page_wal_pi_snapshot_locked_v1(struct BufferDesc *buf,
												   ClusterPageWalBindingV1 *out);

/* Original cached-X eviction owner: header locked, exact X+REVOKING fence,
 * clean bytes and only the caller's optional pin. An absent attribution is
 * explicit and cannot certify DATA or retire any responsibility. */
struct ClusterPcmOwnSnapshot;
extern ClusterPageWalCaptureResultV1
cluster_page_wal_eviction_snapshot_locked_v1(struct BufferDesc *buf,
											 const struct ClusterPcmOwnSnapshot *fence,
											 uint32 caller_pins, ClusterPageWalBindingV1 *out);

/* Original T2 owner preflights under content-X before touching page/authority.
 * An all-zero carrier explicitly clears old attribution. A successful prepare
 * reserves its source before page/authority mutation. The process-local value
 * must be published in the same uninterrupted lock hold, only after exact T2
 * succeeds, or released on any other outcome. Publication consumes the held
 * reference. Neither function grants ownership. Failure leaves output intact. */
extern bool cluster_page_wal_prepare_install_v1(Buffer buffer,
												const ClusterPageWalBindingV1 *carrier, Page image,
												ClusterPageWalInstallV1 *prepared);
extern bool cluster_page_wal_publish_install_v1(Buffer buffer, ClusterPageWalInstallV1 *prepared);
extern void cluster_page_wal_release_install_v1(ClusterPageWalInstallV1 *prepared);
/* Original T2/T3 owner, still content-X: compare the installed attribution
 * with that acquisition's original carrier, including explicit absence.
 * A failed snapshot is never evidence that the sidecar is empty. */
extern bool cluster_page_wal_install_matches_v1(Buffer buffer,
											 const ClusterPageWalBindingV1 *expected);

#endif
