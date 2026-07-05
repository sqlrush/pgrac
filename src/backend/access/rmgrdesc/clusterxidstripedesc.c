/*-------------------------------------------------------------------------
 *
 * clusterxidstripedesc.c
 *	  rmgr descriptor for RM_CLUSTER_XID_STRIPE.
 *
 *	  Human-readable WAL descriptor/identifier for the spec-6.15 xid
 *	  stripe resource manager (activation-knowledge JOIN / RETIRE
 *	  records).  pg_waldump and backend rmgrdesc callers use this file
 *	  to decode the records without the stripe runtime itself.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/clusterxidstripedesc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.15-xid-space-segmentation.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_xid_stripe_xlog.h"

void
cluster_xid_stripe_desc(StringInfo buf, XLogReaderState *record)
{
	char *payload = XLogRecGetData(record);
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info) {
	case XLOG_CLUSTER_XID_STRIPE_JOIN: {
		xl_cluster_xid_stripe_join *rec = (xl_cluster_xid_stripe_join *)payload;

		appendStringInfo(buf, "slot %d floor " UINT64_FORMAT " epoch " UINT64_FORMAT, rec->slot,
						 rec->activated_floor_full, rec->stride_mode_epoch);
		break;
	}
	case XLOG_CLUSTER_XID_STRIPE_RETIRE: {
		xl_cluster_xid_stripe_retire *rec = (xl_cluster_xid_stripe_retire *)payload;

		appendStringInfo(buf, "slot %d retired", rec->slot);
		break;
	}
	default:
		appendStringInfo(buf, "unknown op %u", info);
		break;
	}
}

const char *
cluster_xid_stripe_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK) {
	case XLOG_CLUSTER_XID_STRIPE_JOIN:
		return "XID_STRIPE_JOIN";
	case XLOG_CLUSTER_XID_STRIPE_RETIRE:
		return "XID_STRIPE_RETIRE";
	default:
		return NULL;
	}
}

#endif /* USE_PGRAC_CLUSTER */
