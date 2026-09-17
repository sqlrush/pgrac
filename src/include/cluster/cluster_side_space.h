/*-------------------------------------------------------------------------
 *
 * cluster_side_space.h
 *	  RF-SIDE D-SIDE-05 — canonical space metadata gates (HWM / extent /
 *	  bitmap) under STOP-RF-SIDE-SPACE-ABI.
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-05 ("WAL-logged shared HWM/extent/bitmap pages；在
 *	  Rule-26 approval 前只写 RED/STOP，不写 ABI"), §2.5 canonical
 *	  space metadata, §5.1 U-SIDE-11/12/18.
 *
 *	  CONTRACT (spec §2.5 + §5.1):
 *	    - canonical bytes 位于 shared storage 且由 WAL 恢复；volatile
 *	      HWM/master cache 只从 canonical page 派生;
 *	    - reserve/extend/free/reuse 必须在 exact resource authority 下
 *	      更新 canonical page；成功返回/shmem raise/counter/old
 *	      checkpoint image 都不能发布 allocation authority;
 *	    - update 先通过 RF-PAGE class、PageVersion、expected-before 与
 *	      source provenance；mismatch/unknown class BLOCKED;
 *	    - metadata bytes 完成 durability barrier 后必须 post-read
 *	      验证；通过前禁止把 extent/block 分配给 writer;
 *	    - U-SIDE-18: `STOP-RF-SIDE-SPACE-ABI` 不能由 config/test
 *	      override —— 本 API 没有任何 override 途径（无 config/GUC/
 *	      test 参数），mutation 门恒关闭;
 *	    - U-SIDE-12: metadata PageVersion 的 result-skip 与
 *	      expected-before apply 两门分别负测（委托 RF-PAGE §3.2
 *	      decide）。
 *
 *	  DELIVERED HERE: the judgement gates only.  The canonical space
 *	  page layout/ABI and the allocation execution stay STOPPED (RED)
 *	  until a Rule-26 approval.  No ABI change.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_side_space.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_SPACE_H
#define CLUSTER_SIDE_SPACE_H

#include "cluster/cluster_page_version.h"

typedef enum ClusterSideSpaceKind {
	CLUSTER_SIDE_SPACE_HWM = 0,
	CLUSTER_SIDE_SPACE_EXTENT,
	CLUSTER_SIDE_SPACE_BITMAP
} ClusterSideSpaceKind;

/*
 * U-SIDE-18 / U-SIDE-11: the space-metadata mutation gate.  ALWAYS
 * false while STOP-RF-SIDE-SPACE-ABI is active — the unapproved
 * canonical space page/redo ABI cannot be written by any path, and the
 * API exposes NO config/GUC/test override (there is no parameter that
 * could turn it on).  A future Rule-26 approval revises this function.
 */
extern bool cluster_side_space_metadata_mutation_allowed(ClusterSideSpaceKind kind);

/*
 * U-SIDE-12: the §2.5-3 metadata PageVersion gates — one admission
 * decision for the canonical metadata page update, delegating to the
 * RF-PAGE §3.2 decide (result-version skip / expected-before apply,
 * both negatively tested: a mismatch is BLOCKED and a numeric-higher
 * source never skips).  The kind selects the contract; the version
 * maths are the RF-PAGE ones.
 */
extern ClusterPageApplyVerdict cluster_side_space_metadata_page_verdict(
	ClusterSideSpaceKind kind, const ClusterPageVersion *current_working,
	const ClusterPageVersion *expected_before, const ClusterPageVersion *result_version,
	const ClusterPageVersion *trusted_source_version);

#endif /* CLUSTER_SIDE_SPACE_H */
