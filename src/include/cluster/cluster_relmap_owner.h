/* PGRAC: original relmap producer identity, not pending/retention authority.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_OWNER_H
#define CLUSTER_RELMAP_OWNER_H

#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_relmap_retention.h"
#include "common/cluster_relmap_v2.h"
#include "storage/lwlock.h"

struct XLogReaderState;
struct ClusterWalInputsV1;
struct ClusterRelmapPendingSideSource;

/* Process-local preparation only. Keep the complete native writer reference;
 * the on-disk relmap header cannot substitute for its WAL/claim retention.
 * owner.wal_end is zero: this is not an encodable pending owner. */
typedef struct ClusterRelmapOwnerSampleV2 {
	ClusterWalWriterToken writer;
	ClusterRelmapV2Owner owner;
	int producer_pid;
} ClusterRelmapOwnerSampleV2;

/* Original backend, in a transaction and holding the global relmap X lock.
 * May assign the real top FullXid, so call BEFORE RelationMappingLock and
 * before a relmap WAL/critical window. Top assignment may write transaction
 * metadata. Refusal clears out; no relmap stage or retention permission. */
extern bool cluster_relmap_owner_capture_v2(ClusterRelmapOwnerSampleV2 *out);

/* Dormant retained producer only. Sample the original native writer first,
 * derive its system/database/origin, then use the explicit retained xact
 * allocator before mapping/XidGen locks. Keep that writer across assignment;
 * a changed writer refuses instead of adopting its successor. False/ERROR
 * clears out but may leave an issued XID, native CLOG and original pins/debt:
 * follow the original abort path, never retry or infer rollback. This does
 * not activate retained allocation or grant COMPLETE/release authority. */
extern bool cluster_relmap_owner_capture_retained_v2(ClusterRelmapOwnerSampleV2 *out);

/* While the original transaction is in progress: no XID assignment, adoption
 * of a newer writer, IO or wait. The same backend
 * keeps the global X lock; RelationMappingLock may now be held. This rechecks
 * provenance only, never proves retention, WAL durability, stage or release. */
extern bool cluster_relmap_owner_matches_v2(const ClusterRelmapOwnerSampleV2 *sample);

/* Dormant clean-v1 conversion, BEFORE RelationMappingLock/WAL/critical work.
 * Keep this process-local sample immutable and the original transaction/global
 * relmap X throughout. Derive identity from its qualified native claim and the
 * actual database namespace; no assignment or caller-provided incarnation.
 * Both copies must be clean. Conversion syncs backup before primary and then
 * rechecks the exact original owner. False may follow durable conversion and
 * is NOT rollback proof. Caller keeps original publishers disabled until the
 * complete migration succeeds and owns failure handling. No live caller,
 * retention COMPLETE, stage, release or floor/last-issued permission here. */
extern bool cluster_relmap_owner_convert_clean_v2(
	const ClusterRelmapOwnerSampleV2 *sample, bool shared_map);

/* Dormant original terminal caller only, before top FullXid teardown. Same
 * exact backend, FullXid, native writer and global X checks, including during
 * postcommit when IsTransactionState is false. No assignment, terminal
 * classification or commit/abort proof: the caller supplies its original
 * polarity and failure handling. Never use this for capture or stage. */
extern bool cluster_relmap_owner_terminal_matches_v2(const ClusterRelmapOwnerSampleV2 *sample);

/* Read-only, provisional match of a decoded pending image to the original
 * selected source's current native RELMAP UPDATE. The retained visitor must
 * have checked native CRC/source authority and supplied exact start/end LSNs.
 * Neither a claim nor native WAL encodes the header's FullXid era, membership
 * epoch or boot incarnation: qualify those independently, never reconstruct
 * them here. Whole-source coverage and final retained-source/ROOT revalidation
 * remain caller duties. True grants no terminal, stage or release authority. */
extern bool cluster_relmap_owner_wal_matches_v2(const ClusterRelmapV2Image *image,
	const ClusterWalSourceRef *source, const struct XLogReaderState *reader);

/* Dormant read-only consumer of one decoded pending image and the caller's
 * original held cold read scope. Read ALL selected sources through the native
 * retained census and its final ROOT rechecks; require one exact native match.
 * The caller owns/release-checks the scope, including on ERROR. OPEN returns
 * WAIT, never partial success. No new scope, writer or retention owner here.
 * Dual-copy union and independent era/epoch/boot qualification remain upstream
 * duties. OK grants no terminal, COMPLETE, stage or release authority. */
extern ClusterControlRootResult cluster_relmap_owner_retained_wal_v2(
	const ClusterRelmapV2Image *image, struct ClusterWalInputsV1 *inputs);

/* Check the pending record when present, and match the complete supplied
 * sealed RECOVERY SIDE physical participant set to this original cold scope's
 * actual full claims and cuts in ONE retained scan. Include every participant
 * of each plan and every selected source, including explicit-empty terminals.
 * Match every retained SMGR/XACT record to its sealed operation in per-source
 * order, including history: exact LSNs/CRC/header frame and whole owned main
 * payload. A clean v2 image still requires this complete structural check;
 * only its pending RELMAP assertion is absent. Missing or extra structural
 * operations refuse. Other SIDE/PAGE
 * capture completeness remains an independent caller duty.
 * Keep image, sources and plans immutable for the call. The caller owns the
 * scope, including release on ERROR. No plan capture, redo
 * eligibility, final SIDE revalidation or independent era/epoch/boot/floor/
 * last-issued qualification here. OK grants no COMPLETE or release authority;
 * no live caller is introduced. Graph component counts are not source proof. */
extern ClusterControlRootResult cluster_relmap_owner_retained_side_wal_v2(
	const ClusterRelmapV2Image *image, struct ClusterWalInputsV1 *inputs,
	const struct ClusterRelmapPendingSideSource *sources, size_t source_count);

/* Dormant preparation adapter, BEFORE RelationMappingLock/WAL/critical work.
 * Recheck this original sample, read the original TT owner's published receipt
 * (if any), and enroll its exact key under the supplied retention lock. Inputs
 * are stable process-local copies; the caller keeps global relmap X throughout.
 * No new XID, binding, native floor/issued proof or COMPLETE publication here.
 * The caller must supply the real qualified directory and its one runtime lock,
 * held in neither mode on entry. Only the pure core runs under that lock.
 * Refusal preserves pins; OK is preparation, not WAL/stage/release authority.
 * A later original writer recheck and independent WAL/claim duties still apply.
 * No live caller or shared directory is registered by this interface. */
extern ClusterRelmapRetentionResult cluster_relmap_owner_retention_begin_v2(
	const ClusterRelmapOwnerSampleV2 *sample, const ClusterRelmapV2Namespace *identity,
	uint64 generation, ClusterRelmapRetentionDirectory *directory, LWLock *retention_lock);

#endif
