/*-------------------------------------------------------------------------
 *
 * cluster_space_storage.c
 *    Native buffer and WAL integration for the SPACE identity profile.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_space_storage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_page_producer.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"

static bool
space_namespace(RelFileLocator locator, bool redo, ClusterSpaceIdentityKey *out)
{
	ClusterWalDurablePrefixRef ref;

	if (!cluster_enabled || !cluster_shared_config || cluster_node_id < 0 || cluster_node_id >= 16
		|| cluster_smgr_which_for(locator, InvalidBackendId) != 1)
		return false;
	if (!(redo ? cluster_wal_thread_restart_v2_ref(&ref) : cluster_wal_thread_current_v2_ref(&ref)))
		return false;
	memset(out, 0, sizeof(*out));
	out->system_identifier = ref.claim.identity.system_identifier;
	out->database_incarnation = ref.claim.database_incarnation;
	memcpy(out->storage_uuid, ref.claim.identity.storage_uuid, 16);
	out->locator = locator;
	return true;
}

/* Native merged recovery may remap the stored LSN into the recovering
 * thread's durable coordinate. Preserve that existing LSN handling, but
 * never replace the typed SPACE mutation token with a record's numeric SCN.
 * Full cross-origin contribution retirement is a separate mandatory gate. */
static void
space_set_lsn(Page page, XLogRecPtr lsn, uint64 token)
{
	PageSetLSN(page, lsn);
	((PageHeader)page)->pd_block_scn = token;
}

bool
cluster_space_relation_create(RelFileLocator locator)
{
	ClusterSpaceWalChange change;
	PGAlignedBlock result;
	uint8 bytes[CLUSTER_SPACE_WAL_BYTES];
	SMgrRelation rel;
	Buffer buffer;
	XLogRecPtr lsn;

	if (!cluster_shared_config || cluster_smgr_which_for(locator, InvalidBackendId) != 1)
		return true;
	memset(&change, 0, sizeof(change));
	if (RecoveryInProgress() || !space_namespace(locator, false, &change.result.key))
		return false;
	change.action = CLUSTER_SPACE_WAL_CREATE;
	change.nblocks = InvalidBlockNumber;
	change.result_token = rf_page_mutation_token_next();
	change.result.sequence = 1;
	change.result.operation = change.result_token;
	change.result.state = CLUSTER_SPACE_IDENTITY_LIVE;
	if (!pg_strong_random(change.result.incarnation, sizeof(change.result.incarnation))
		|| !cluster_space_wal_encode(&change, bytes, sizeof(bytes)))
		return false;
	/* All namespace/randomness checks precede storage mutation. An existing
	 * fork is never proof that this create owns its identity. */
	rel = smgropen(locator, InvalidBackendId);
	if (smgrexists(rel, SPACE_FORKNUM))
		return false;
	smgrcreate(rel, SPACE_FORKNUM, false);
	buffer
		= ReadBufferWithoutRelcache(locator, SPACE_FORKNUM, P_NEW, RBM_ZERO_AND_LOCK, NULL, true);
	if (BufferGetBlockNumber(buffer) != 0) {
		UnlockReleaseBuffer(buffer);
		return false;
	}
	memcpy(result.data, BufferGetPage(buffer), BLCKSZ);
	if (cluster_space_wal_apply(&change, &change.result.key, result.data, BLCKSZ)
		!= CLUSTER_SPACE_IDENTITY_APPLY) {
		UnlockReleaseBuffer(buffer);
		return false;
	}
	XLogBeginInsert();
	START_CRIT_SECTION();
	memcpy(BufferGetPage(buffer), result.data, BLCKSZ);
	MarkBufferDirty(buffer);
	XLogRegisterData((char *)bytes, sizeof(bytes));
	lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE);
	space_set_lsn(BufferGetPage(buffer), lsn, change.result_token);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(buffer);
	return true;
}

bool
cluster_space_relation_redo(XLogReaderState *record)
{
	ClusterSpaceWalChange change;
	ClusterSpaceIdentityKey expected;
	ClusterSpaceIdentityTransition transition;
	PGAlignedBlock result;
	uint8 check[CLUSTER_SPACE_IDENTITY_BYTES];
	SMgrRelation rel;
	BlockNumber blocks;
	Buffer buffer;

	if (record == NULL || record->record == NULL || !RecoveryInProgress()
		|| XLogRecGetRmid(record) != RM_SMGR_ID
		|| (XLogRecGetInfo(record) & ~XLR_INFO_MASK) != XLOG_SMGR_SPACE_IDENTITY
		|| XLogRecHasAnyBlockRefs(record) || XLogRecPtrIsInvalid(record->EndRecPtr)
		|| !cluster_space_wal_decode(XLogRecGetData(record), XLogRecGetDataLen(record), &change)
		|| !space_namespace(change.result.key.locator, true, &expected))
		return false;
	/* Independently selected namespace, not the untrusted WAL payload. */
	if (!cluster_space_identity_encode(&change.result, check, sizeof(check))
		|| !cluster_space_identity_decode(check, sizeof(check), &expected, &change.result))
		return false;
	/* These structural owners are wired separately before format activation.
	 * Never treat their decoded record as an already-applied CREATE. */
	if (change.action != CLUSTER_SPACE_WAL_CREATE)
		return false;
	rel = smgropen(expected.locator, InvalidBackendId);
	smgrcreate(rel, SPACE_FORKNUM, true);
	blocks = smgrnblocks(rel, SPACE_FORKNUM);
	if (blocks > 1)
		return false;
	buffer = ReadBufferWithoutRelcache(expected.locator, SPACE_FORKNUM, blocks == 0 ? P_NEW : 0,
									   blocks == 0 ? RBM_ZERO_AND_LOCK : RBM_NORMAL, NULL, true);
	if (blocks != 0)
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	if (BufferGetBlockNumber(buffer) != 0) {
		UnlockReleaseBuffer(buffer);
		return false;
	}
	memcpy(result.data, BufferGetPage(buffer), BLCKSZ);
	transition = cluster_space_wal_apply(&change, &expected, result.data, BLCKSZ);
	if (transition == CLUSTER_SPACE_IDENTITY_APPLY) {
		START_CRIT_SECTION();
		memcpy(BufferGetPage(buffer), result.data, BLCKSZ);
		MarkBufferDirty(buffer);
		space_set_lsn(BufferGetPage(buffer), record->EndRecPtr, change.result_token);
		END_CRIT_SECTION();
	}
	UnlockReleaseBuffer(buffer);
	return transition == CLUSTER_SPACE_IDENTITY_APPLY
		   || transition == CLUSTER_SPACE_IDENTITY_ALREADY;
}
