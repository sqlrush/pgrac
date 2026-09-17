/*-------------------------------------------------------------------------
 * cluster_unit_no_normal_stop.h
 *   Fail-stop boundaries for fixtures that do not execute normal shutdown.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/cluster_unit_no_normal_stop.h
 *
 * NOTES
 *   A new path into shutdown must fail the fixture, never grant authority.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNIT_NO_NORMAL_STOP_H
#define CLUSTER_UNIT_NO_NORMAL_STOP_H

#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_reconfig.h"

bool
cluster_reconfig_normal_stop_snapshot_admitted_membership(
	const ClusterSemanticActivationRecord *open_record pg_attribute_unused(),
	const uint8 *root_descriptor pg_attribute_unused(),
	uint64 *out_members_lo pg_attribute_unused(), uint64 *out_members_hi pg_attribute_unused(),
	uint64 *out_formation_epoch pg_attribute_unused())
{
	abort();
}

bool
cluster_normal_stop_peer_receipt_tail(
	const ClusterSemanticActivationRecord *open_record pg_attribute_unused(),
	const uint8 root_descriptor[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] pg_attribute_unused(),
	int peer pg_attribute_unused(), uint64 admitted_incarnation pg_attribute_unused())
{
	abort();
}
#endif
