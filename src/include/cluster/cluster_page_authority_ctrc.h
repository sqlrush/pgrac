/*-------------------------------------------------------------------------
 *
 * cluster_page_authority_ctrc.h
 *     Native CTRC replay-set protection of exact PAGE targets.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/include/cluster/cluster_page_authority_ctrc.h
 *
 * NOTES
 *     Separate from ordinary RR serial/pin and PAGE authority objects.
 *     Original native scopes retain their IR/WALR lifetime and debts.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_AUTHORITY_CTRC_H
#define CLUSTER_PAGE_AUTHORITY_CTRC_H

#include "cluster/cluster_page_authority.h"

typedef struct RfPageCtrcAuthorityV1 RfPageCtrcAuthorityV1;
typedef struct RfPageCtrcAuthorityBuildV1 {
	RfPageCtrcAuthorityV1 *authority;
	RfPageAuthorityVerdictV1 verdict;
} RfPageCtrcAuthorityBuildV1;

/* Copy complete targets and the full original native replay set before PAGE.
 * Each target needs its matching replay-set stable proof, already preflighted
 * against the original whole ROOT. Retain all original arrays, proofs, graph
 * inputs and native scope until destruction. No caller output can alias them.
 * The budget bounds this adapter's one private allocation; insufficiency is
 * OOM. This does not acquire an IR, renew a pin or perform ROOT/WAL I/O. */
extern RfPageCtrcAuthorityBuildV1 rf_page_authority_ctrc_replays_preflight_v1(
	const RfPageAuthorityTargetV1 *targets, uint32 target_count,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *sources, uint32 source_count,
	Size memory_budget);
/* Same original PAGE partitions, conditional acquisition and local waiting.
 * A collision unwinds only this attempt's PAGE locks and returns WOULD_BLOCK;
 * the original native owners remain held for a later caller-driven retry.
 * Before any PAGE lock, count distinct partitions and all native caller-held
 * LWLocks. A batch exceeding the native ceiling is INVALID_ARGUMENT; currently
 * insufficient remaining capacity is WOULD_BLOCK without partial grants. */
extern RfPageAuthorityVerdictV1 rf_page_authority_ctrc_replays_promote_nowait_v1(
	RfPageCtrcAuthorityV1 *authority);
/* One-image WAL consumer: require exactly this one original target and no
 * caller-held native LWLocks before PAGE. General batch promotion above is
 * unchanged; its near-ceiling batches are not native WAL insertion inputs. */
extern RfPageAuthorityVerdictV1 rf_page_authority_ctrc_replays_promote_fpi_nowait_v1(
	RfPageCtrcAuthorityV1 *authority, const RfPageIdentityV1 *identity,
	const RfPageVersionV1 *expected_before);
/* Drop only local PAGE locks and retain the same immutable preflight for an
 * exact later promotion. No I/O, allocation or borrowed-owner dereference or
 * retirement. The caller must revalidate/reobserve before using PAGE again. */
extern bool rf_page_authority_ctrc_replays_unpromote_v1(RfPageCtrcAuthorityV1 *authority);
extern RfPageAuthorityVerdictV1 rf_page_authority_ctrc_replays_revalidate_nowait_v1(
	const RfPageCtrcAuthorityV1 *authority);
/* Exact target version under all original current native owners and PAGE.
 * No I/O/allocation/new lock acquisition, disk mutation or completion result. */
extern bool rf_page_authority_ctrc_replays_covers_nowait_v1(
	const RfPageCtrcAuthorityV1 *authority, const RfPageIdentityV1 *identity,
	const RfPageVersionV1 *expected_before);
/* Bind the promoted PAGE object to the exact original observation scope.
 * All its original native holders and PAGE locks must still be current.
 * Local only, with no I/O/allocation/acquisition or borrowed-owner release. */
extern bool rf_page_authority_ctrc_replays_scope_nowait_v1(
	const RfPageCtrcAuthorityV1 *authority, const struct ClusterWalInputsV1 *scope);
typedef struct RfPageCtrcReferenceV1 {
	bool ready;
	RfPageRecoveryReferenceV1 reference;
} RfPageCtrcReferenceV1;
/* Value-only F4 comparison metadata from an exact original native source and
 * promoted target: actual IR request cookie and actual PAGE fingerprint and
 * partition. Not authority or an ordinary RR serial/pin conversion. No I/O,
 * allocation, new acquisition, mutation or borrowed-owner retirement. Retain
 * all original backing storage; store the by-value result separately. */
extern RfPageCtrcReferenceV1 rf_page_authority_ctrc_replays_reference_nowait_v1(
	const RfPageCtrcAuthorityV1 *authority,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	const RfPageIdentityV1 *identity, const RfPageVersionV1 *expected_before);
/* Reverse-release local PAGE locks, then private memory, even if original
 * native custody has retired. Never dereference/release a borrowed owner. */
extern void rf_page_authority_ctrc_replays_destroy_v1(RfPageCtrcAuthorityV1 **authority);

#endif /* CLUSTER_PAGE_AUTHORITY_CTRC_H */
