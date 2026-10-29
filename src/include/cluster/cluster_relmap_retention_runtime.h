/* PGRAC: private shared relmap retention coordination.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_RETENTION_RUNTIME_H
#define CLUSTER_RELMAP_RETENTION_RUNTIME_H

#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_owner.h"

/* Original shared-memory registry only. New storage is UNKNOWN for every
 * origin; attachment never rebuilds, clears pins or cancels reservations. */
extern void cluster_relmap_retention_runtime_shmem_register(void);

/* Original Startup only, before its first ROOT/census/retained-source scan.
 * Under the sole runtime lock revoke ALL origins to UNKNOWN, preserving every
 * pin, TT binding, staged marker, identity, floor/issued observation and native
 * truncate reservation. Retry/attach never makes an old generation available.
 * Requires attached shmem, no CRIT/shutdown, and no caller-held mapping,
 * XidGenLock or retention lock. Refusal changes nothing and releases no caller
 * lock. No allocation, native owner query or I/O occurs under retention.
 * This grants no rebuild/COMPLETE publication or floor/issued qualification;
 * live v2 and destructive-consumer activation remain separate gates. */
extern bool cluster_relmap_retention_runtime_recovery_invalidate_v2(void);

/* Dormant original-backend preparation, with the same lock/context contract
 * as owner_retention_begin_v2. Supplies the one real runtime retention lock
 * and the original sample's origin directory; callers cannot substitute an
 * empty directory or a different lock. No live caller or COMPLETE publisher
 * is provided here. Without original qualified publication this refuses.
 * OK remains preparation only, not WAL/stage/TT release permission. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_begin_v2(
	const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Namespace *identity,
	uint64 generation);

/* Read-only original in-progress producer conjunct. Recheck original owner,
 * namespace, COMPLETE directory and exact staged pin/end under the sole lock,
 * then original owner again outside it. No assignment, I/O, state mutation,
 * retry, image/durability/terminal proof or release. MappingLock may be held;
 * caller must hold no XidGen/retention lock. Snapshot only, not a reservation
 * or permission to adopt a map without the original successful stage and
 * independent exact physical-copy checks. */
extern bool cluster_relmap_retention_runtime_check_staged_v2(
	const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Image *staged);

/* Dormant original producer: with its already captured top FullXid, original
 * global relmap X and RelationMappingLock(X), insert the native RELMAP record,
 * flush its actual returned end, then stage BOTH v2 copies through the original
 * runtime adapter. Requires an exact unstaged preparation, no caller-held
 * XidGen/retention lock, and current FullXid equal to the original top sample;
 * never assigns an xid inside this window. Native WAL and storage I/O run
 * outside retention. Returns only this insertion's end after successful stage
 * and original-owner revalidation. InvalidXLogRecPtr can follow durable WAL or
 * partial/complete pair writes: follow the original failure path, preserve all
 * duties and do not retry or infer rollback. ERROR/fail-stop propagates. No
 * live caller, COMPLETE publication, terminal proof or release permission. */
extern XLogRecPtr cluster_relmap_retention_runtime_insert_pending_v2(
	const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Namespace *identity,
	uint64 generation, const RelMapFile *pending);

/* Dormant original-producer stage after its original relmap WAL insert/flush.
 * The caller supplies that exact durable record end, which this adapter does
 * not independently authenticate. The original relmap X must remain held;
 * RelationMappingLock may remain held after CRIT as in the original producer.
 * Revalidates the sample/namespace and routes the existing dual-copy writer
 * through the private original-origin directory and sole runtime lock. Its
 * stage marker precedes every possible pending write; I/O stays outside the
 * retention lock. True also requires a final original-owner recheck. False
 * never cancels a pin and may leave pending in either copy. No live caller. */
extern bool cluster_relmap_retention_runtime_write_pending_v2(
	const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Namespace *identity,
	uint64 generation, XLogRecPtr wal_end, const RelMapFile *pending);

/* Original owner cleanup for an exact preparation with no possible pending
 * write. A staged pin returns RETAINED even after an unsuccessful stage call.
 * Never clears ACTIVE/BIND, authenticates a terminal outcome or releases a
 * staged obligation; it supplies no terminal/pin-release permission. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_cancel_prepare_v2(
	const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Namespace *identity,
	uint64 generation);

/* Dormant original producer terminal cleanup, before transaction context
 * teardown and while its global relmap X remains held. Exact original owner
 * provenance is rechecked even in postcommit's TRANS_COMMIT interval; capture,
 * stage and cancel retain their original in-progress requirement. The caller still
 * authenticates the matching original commit/abort and applies its original
 * failure polarity, including postcommit fail-stop; these are not recovery
 * terminal classifiers. Route only an exact already-staged runtime pin to the
 * existing dual-copy publish/discard, then recheck owner and pin.
 * Publish first syncs/readbacks that same current writer's native CLOG and
 * requires COMMITTED with its exact FullXid/full identity, then rechecks the
 * original owner/directory/staged pin before any pair write. Abort retains
 * its earlier pre-abort-record order and does not require terminal CLOG here.
 * This additional current-native observation is no cold base/source/lifetime
 * or terminal-release certificate; all original qualifications remain.
 * Native owner queries and all I/O stay outside the sole retention lock.
 * No internal retry.
 * CLEARED_POSTREAD certifies storage cleanup in this call only, never terminal
 * proof, COMPLETE publication or pin/native/TT/SPACE release. All pins remain,
 * including after a clean retry; later success cannot erase an earlier I/O
 * failure. UNKNOWN can follow partial or complete disk cleanup. No live caller. */
extern ClusterRelmapV2ClearResult cluster_relmap_retention_runtime_publish_v2(
	const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Image *staged);
extern ClusterRelmapV2ClearResult cluster_relmap_retention_runtime_discard_pending_v2(
	const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Image *staged);

/* Dormant original ACTIVE-owner adapter, after actual publication and before
 * terminal/release work. Reads the current real FullXid without assignment and
 * the original TT owner's published receipt, then rechecks both the full era
 * and native writer outside the one runtime lock. Under that lock it enrolls
 * all existing same-FullXid pins, including staged pins and all namespaces.
 * No global relmap X lock is required. An empty COMPLETE origin stays empty;
 * UNKNOWN/BUILDING is never a qualified empty set. Inputs cannot be supplied
 * by another backend. Refusal preserves ACTIVE/BIND and pins; the
 * ACTIVE hook below follows its ERROR path without cancelling publication.
 * OK grants no stage, terminal, TT reuse or pin release permission. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_bind_current_v2(void);

/* Original ACTIVE publisher, after successful publication/admission leave and
 * on reuse, before returning success. Only this backend's actual successful
 * v2 begin creates a routing duty; no begin or a different actual FullXid is
 * a no-op, not an assertion that an UNKNOWN directory is empty. The last
 * proven zero-effect cancellation clears this routing duty; ERROR, staged
 * cleanup and shmem reattachment do not. Same-top calls enroll the real TT
 * receipt through bind_current, preserving its source/COMPLETE/lock gates.
 * False/ERROR must propagate OUTSIDE the ACTIVE publication catch so its
 * already published binding remains available to original abort. No stage,
 * terminal, COMPLETE or release permission, and no live v2 producer activation. */
extern bool cluster_relmap_retention_runtime_after_active_v2(TransactionId top_xid);

/* Dormant read-only relmap conjunct for an original TT release/reuse consumer
 * (ordinary A0/GC or recovery D0). The original caller authenticates the
 * system/DB incarnation and published receipt; this does not mint provenance.
 * Selects the binding's original origin, never the current node's directory,
 * under the sole runtime lock. All namespaces count, including unstaged pins;
 * an unbound same-raw obligation refuses. UNKNOWN/BUILDING is never empty.
 * OK is a snapshot, not a reservation or permission to mutate/reuse TT,
 * release a pin or certify a terminal outcome. The eventual destructive
 * consumer must serialize its mutation with the original owner/retention
 * gates and satisfy every other original gate; unlocking here grants none.
 * No native/TT owner query, allocation or I/O occurs under the lock. No live
 * caller or qualified COMPLETE publisher is installed by this interface. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_tt_release_check_v2(
	uint64 system_identifier, uint64 database_incarnation, const ClusterCanonicalTxnBinding *binding);

/* Volatile original-owner routing receipt, not a persistent ABI or an
 * authenticated capability. Origin is 0-based, as in the retention directory. */
typedef struct ClusterRelmapRetentionRuntimeTruncateV2 {
	uint64 system_identifier;
	uint64 database_incarnation;
	uint16 origin;
	ClusterRelmapRetentionTruncate reservation;
} ClusterRelmapRetentionRuntimeTruncateV2;

/* Dormant original native-truncation owner only. That owner authenticates the
 * origin, system/DB incarnation and qualified FullXid cut, and satisfies every
 * existing native retention gate. Raw xid, nextXid or another origin's empty
 * directory cannot supply that qualification. Under the sole runtime lock,
 * begin clamps the cut to ALL pending pins and reserves it against new begin;
 * on refusal the caller's receipt is unchanged. No owner query, allocation or
 * I/O occurs under this lock. The original owner must use the RETURNED cutoff
 * (never the wider requested cut) for its original I/O after unlocking.
 * Complete is called only after successful corresponding original I/O;
 * uncertain retains the reservation/pins and invalidates the directory. It
 * cannot be undone by ERROR cleanup or a later complete. These interfaces
 * neither release pins nor authorize native mutation by themselves. No live
 * caller, COMPLETE publisher or floor/issued certification is installed. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_truncate_begin_v2(
	uint64 system_identifier, uint64 database_incarnation, uint16 origin,
	uint64 requested_cutoff, ClusterRelmapRetentionRuntimeTruncateV2 *reservation);
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_truncate_complete_v2(
	const ClusterRelmapRetentionRuntimeTruncateV2 *reservation);
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_truncate_uncertain_v2(
	const ClusterRelmapRetentionRuntimeTruncateV2 *reservation);

/* Dormant entry into original LOCAL native TruncateCLOG. The original owner
 * must authenticate sysid/DB/origin and the FullXid cut, qualify COMPLETE and
 * all native/source/base/era/history/lifetime inputs, and retain its original
 * truncation exclusion, checked as WrapLimitsVacuumLock(X). Numbers are only
 * routing, never a certificate.
 * Require normal non-recovery context and no caller-held XidGen/SLRU/native
 * truncation/retention lock. Reserve the stricter cut against late begin,
 * then run unchanged native floor/WAL/SLRU work outside retention. Confirm
 * no eligible files remain before completing; a clean no-op may advance the
 * conservative runtime floor without changing native oldestClogXid.
 * Entered false/ERROR may follow durable WAL, advanced native floor or partial
 * deletion; do not retry or infer rollback. Uncertain work preserves pins and
 * reservation as UNKNOWN. ERROR releases only this call's native locks and
 * rethrows. No pin retirement, live caller, COMPLETE publisher or v2 activation. */
extern bool cluster_relmap_retention_truncate_clog_v2(FullTransactionId oldestXact,
	Oid oldestxid_datoid, uint64 system_identifier, uint64 database_incarnation, uint16 origin);

/* Dormant original allocator only, with its original XidGenLock EXCLUSIVE.
 * The caller authenticates the original 0-based origin, sys/DB incarnation and
 * qualified COMPLETE floor/issued inputs, and preserves every original wrap,
 * StopLimit and lease gate. No current-node or raw-xid era substitution.
 * Check is a read-only candidate snapshot for first/recomputed candidates;
 * it is NOT a reservation. Publish rechecks the FINAL candidate under the
 * sole retention lock and observes a conservative issued upper bound. On OK
 * it invokes the original caller's nofail publication callback ONCE UNDER
 * THAT LOCK, then releases retention while leaving XidGenLock held. Refusal
 * never invokes the callback and releases only its own acquisition.
 *
 * Complete all fallible SLRU extension, allocation, I/O and owner queries
 * BEFORE publish. The callback does ONLY the original nofail nextXid/ProcArray
 * stores, retaining both locks; no ERROR, wait, lock acquisition/release or
 * other work belongs in it. An unexpected ERROR releases this retention
 * acquisition before rethrow to original native cleanup; it never rewinds
 * the observation, which can conservatively refuse a skipped candidate.
 * No begin/end capability or backend-local marker escapes the invocation;
 * later calls cannot release an unrelated caller-held retention lock.
 * Observation is not proof of actual issuance/birth or a source certificate.
 * No live caller, qualified COMPLETE publisher or native issuance authority
 * is installed here; no I/O/native owner query occurs under retention. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_allocation_check_v2(
	uint64 system_identifier, uint64 database_incarnation, uint16 origin, uint64 candidate);
typedef void (*ClusterRelmapRetentionAllocationPublishV2)(void *argument);
extern ClusterRelmapRetentionResult cluster_relmap_retention_runtime_allocation_publish_v2(
	uint64 system_identifier, uint64 database_incarnation, uint16 origin, uint64 candidate,
	ClusterRelmapRetentionAllocationPublishV2 publish, void *argument);

/* Dormant entry into the ORIGINAL native allocator. The original caller must
 * already authenticate this origin/system/DB and qualify its complete native
 * floor/issued/history/lifetime inputs; these numbers supply no certificate.
 * Enter from the original allocation context with neither XidGenLock nor the
 * retention lock held; this function acquires and releases the native lock.
 * Reuses all original native wrap/StopLimit/stripe/herding/lease checks and
 * SLRU extension. CLOG receives the actual FullXid for every skipped position
 * and the candidate; native page initialization is not an issuance or lifetime
 * certificate. First and recomputed candidates are checked before extension;
 * the final candidate and original nofail stores share XidGenLock->retention.
 * Refusal raises existing53RB5 with no fallback or automatic retry. Ordinary
 * GetNewTransactionId remains unchanged in behavior. No live caller, COMPLETE
 * publisher, boot admission or v2 activation is installed by this entry. */
extern FullTransactionId cluster_relmap_retention_allocate_xid_v2(bool isSubXact,
	uint64 system_identifier, uint64 database_incarnation, uint16 origin);

/* Dormant original Startup check AFTER all native/SIDE recovery/import and
 * BEFORE opening allocation. The original owner authenticates DB incarnation,
 * complete native/source/base/era/history/lifetime inputs and COMPLETE; numbers
 * alone supply no certificate. Only the actual local native origin/system are
 * accepted. Caller ALREADY holds XidGenLock EXCLUSIVE and original final-fold/
 * admission exclusion, with no retention lock. Read actual cached nextXid and
 * apply the existing read-only guard in XidGen->retention order. No supplied
 * candidate, nextXid clamp, last-issued observation or native publication.
 * Refusal raises53RA3; original caller retains its native lock/ERROR cleanup
 * duties and every pin/debt. Success is a snapshot, not an admission token:
 * keep original exclusion until opening; any later import requires recheck.
 * No live Startup caller, COMPLETE publisher or v2 activation is installed. */
extern void cluster_relmap_retention_check_recovered_next_v2(uint64 system_identifier,
	uint64 database_incarnation, uint16 origin);

#endif
