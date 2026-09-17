/*-------------------------------------------------------------------------
 *
 * cluster_ir.c
 *	  STOP03 recovery serialization -- PURE layer (RF-ROOT P3).  The full-duty
 *	  resource-id encoder is standalone-
 *	  linkable so the cluster_unit test links it directly.
 *	  The backend (shmem counters and typed GES release) lives in
 *	  cluster_ir_lock.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ir.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s8-stop-03-root-serialization.md §17 (frozen)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ir.h"
#include "cluster/cluster_recovery_duty.h"

bool
cluster_recovery_serial_resid_encode(const ClusterRecoveryDutyKey *duty, ClusterResId *out)
{
	ClusterResId encoded;

	if (out == NULL || !cluster_recovery_duty_key_valid_v1(duty))
		return false;
	memset(&encoded, 0, sizeof(encoded));
	encoded.field1 = (uint32)duty->origin_thread_id;
	encoded.field2 = (uint32)(duty->root_lineage_seq & UINT64_C(0xffffffff));
	encoded.field3 = (uint32)(duty->root_lineage_seq >> 32);
	encoded.type = CLUSTER_IR_RESID_TYPE;
	encoded.lockmethodid = DEFAULT_LOCKMETHOD;
	*out = encoded;
	return true;
}
