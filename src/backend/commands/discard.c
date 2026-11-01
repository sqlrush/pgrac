/*-------------------------------------------------------------------------
 *
 * discard.c
 *	  The implementation of the DISCARD command
 *
 * Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/discard.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_tt_local.h"
#include "miscadmin.h"
#endif
#include "commands/async.h"
#include "commands/discard.h"
#include "commands/prepare.h"
#include "commands/sequence.h"
#include "utils/guc.h"
#include "utils/portal.h"

static void DiscardAll(bool isTopLevel);
static void DiscardTemp(void);

/*
 * DISCARD { ALL | SEQUENCES | TEMP | PLANS }
 */
void
DiscardCommand(DiscardStmt *stmt, bool isTopLevel)
{
	switch (stmt->target)
	{
		case DISCARD_ALL:
			DiscardAll(isTopLevel);
			break;

		case DISCARD_PLANS:
			ResetPlanCache();
			break;

		case DISCARD_SEQUENCES:
			ResetSequenceCaches();
			break;

		case DISCARD_TEMP:
			DiscardTemp();
			break;

		default:
			elog(ERROR, "unrecognized DISCARD target: %d", stmt->target);
	}
}

static void
DiscardAll(bool isTopLevel)
{
	/*
	 * Disallow DISCARD ALL in a transaction block. This is arguably
	 * inconsistent (we don't make a similar check in the command sequence
	 * that DISCARD ALL is equivalent to), but the idea is to catch mistakes:
	 * DISCARD ALL inside a transaction block would leave the transaction
	 * still uncommitted.
	 */
	PreventInTransactionBlock(isTopLevel, "DISCARD ALL");

	/* Closing portals might run user-defined code, so do that first. */
	PortalHashTableDeleteAll();
	SetPGVariable("session_authorization", NIL, false);
	ResetAllOptions();
	DropAllPreparedStatements();
	Async_UnlistenAll();
	LockReleaseAll(USER_LOCKMETHOD, true);
	ResetPlanCache();
	DiscardTemp();
	ResetSequenceCaches();
}

static void
DiscardTemp(void)
{
#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_catalog && cluster_storage_mode_enabled()) {
		Oid temp_namespace;
		Oid temp_toast_namespace;

		GetTempNamespaceState(&temp_namespace, &temp_toast_namespace);
		if (OidIsValid(temp_namespace)) {
			ClusterCanonicalTxnBinding binding;
			TransactionId current_xid;

			PreventCommandIfReadOnly("DISCARD TEMP");
			PreventCommandIfParallelMode("DISCARD TEMP");
			PreventCommandDuringRecovery("DISCARD TEMP");
			if (IsSubTransaction())
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("shared catalog alteration does not support subtransactions")));
			current_xid = GetCurrentTransactionIdIfAny();
			if (TransactionIdIsValid(current_xid)
				&& !TransactionIdEquals(current_xid, GetTopTransactionIdIfAny()))
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("shared catalog top-level transaction identity mismatch")));
			if (!cluster_tt_local_prepare_canonical_active(GetTopTransactionId(), &binding))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("canonical ACTIVE publication failed before shared catalog alteration")));
		}
	}
#endif
	ResetTempTableNamespace();
}
