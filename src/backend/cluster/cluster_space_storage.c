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
#include "catalog/pg_control.h"
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
space_namespace(RelFileLocator locator, bool redo, ClusterSpaceIdentityKey *out, uint16 *thread)
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
	if (thread != NULL)
		*thread = ref.claim.identity.origin_thread_id;
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
cluster_space_relation_read_identity(RelFileLocator locator, ClusterSpaceIdentity *out)
{
	ClusterSpaceIdentityKey expected;
	ClusterSpaceIdentity identity;
	SMgrRelation rel;
	Buffer buffer;
	bool valid;
	uint64 token;

	if (out == NULL || RecoveryInProgress() || !space_namespace(locator, false, &expected, NULL))
		return false;
	rel = smgropen(locator, InvalidBackendId);
	if (!smgrexists(rel, SPACE_FORKNUM) || smgrnblocks(rel, SPACE_FORKNUM) != 1)
		return false;
	buffer = ReadBufferWithoutRelcache(locator, SPACE_FORKNUM, 0, RBM_NORMAL, NULL, true);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	valid = BufferGetBlockNumber(buffer) == 0
			&& cluster_space_identity_page_decode(BufferGetPage(buffer), BLCKSZ, SPACE_FORKNUM, 0,
												  &expected, &identity, &token)
			&& identity.state == CLUSTER_SPACE_IDENTITY_LIVE;
	UnlockReleaseBuffer(buffer);
	if (valid)
		*out = identity;
	return valid;
}

bool
cluster_space_copy_page_wal(const ClusterSpaceIdentity *identity, ForkNumber forknum,
							BlockNumber block, void *page, XLogRecPtr *lsn)
{
	ClusterSpaceIdentityKey expected;
	ClusterSpaceIdentity checked;
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;
	PGAlignedBlock result;
	XLogRecPtr recptr;

	if (identity == NULL || page == NULL || lsn == NULL || block == InvalidBlockNumber
		|| RecoveryInProgress()
		|| (forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM && forknum != FSM_FORKNUM)
		|| !space_namespace(identity->key.locator, false, &expected, NULL)
		|| !cluster_space_identity_encode(identity, encoded, sizeof(encoded))
		|| !cluster_space_identity_decode(encoded, sizeof(encoded), &expected, &checked)
		|| checked.state != CLUSTER_SPACE_IDENTITY_LIVE)
		return false;
	/* An unformatted ordinary source is not PRESENT. Its typed structural
	 * replay must be supplied before that copy shape can be admitted. */
	if ((((PageHeader)page)->pd_flags & (PD_SPACE_METADATA | PD_UNDO_SEG_HEADER)) != 0
		|| (forknum != FSM_FORKNUM && PageIsNew(page)))
		return false;
	memset(&component, 0, sizeof(component));
	memset(&result, 0, sizeof(result));
	if (forknum == FSM_FORKNUM) {
		component.page_class = RF_PAGE_CLASS_REBUILDABLE_FSM;
		component.before_kind = RF_PAGE_STATE_REBUILDABLE;
	} else {
		component.page_class = RF_PAGE_CLASS_ORDINARY;
		component.before_kind = RF_PAGE_STATE_ABSENT;
		component.page = result.data;
		memcpy(component.segment_incarnation, checked.incarnation, 16);
	}
	/* Capture the new destination's absent state before copying any source
	 * bytes. The original relation owner, not this codec, proves an empty
	 * destination fork and retains the object lifecycle lock. */
	if (!rf_page_producer_prepare_v1(&component, 1, &batch))
		return false;
	memcpy(result.data, page, BLCKSZ);
	if (forknum != FSM_FORKNUM)
		((PageHeader)result.data)->pd_block_scn = 0;
	if (!rf_page_producer_stamp_v1(&batch))
		return false;
	XLogBeginInsert();
	XLogRegisterBlock(0, &checked.key.locator, forknum, block, result.data, REGBUF_FORCE_IMAGE);
	if (!rf_page_producer_register_wal_v1(&batch))
		elog(ERROR, "new SPACE page version changed before WAL registration");
	recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);
	if (!PageIsNew(result.data)) {
		if (forknum == FSM_FORKNUM)
			PageSetLSN(result.data, recptr);
		else
			space_set_lsn(result.data, recptr, batch.result_token);
	}
	memcpy(page, result.data, BLCKSZ);
	*lsn = recptr;
	return true;
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
	if (RecoveryInProgress() || !space_namespace(locator, false, &change.result.key, NULL))
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
	uint16 local_thread;

	if (record == NULL || record->record == NULL || !RecoveryInProgress()
		|| XLogRecGetRmid(record) != RM_SMGR_ID
		|| (XLogRecGetInfo(record) & ~XLR_INFO_MASK) != XLOG_SMGR_SPACE_IDENTITY
		|| XLogRecHasAnyBlockRefs(record) || XLogRecPtrIsInvalid(record->EndRecPtr)
		|| !cluster_space_wal_decode(XLogRecGetData(record), XLogRecGetDataLen(record), &change)
		|| !space_namespace(change.result.key.locator, true, &expected, &local_thread))
		return false;
	/* Independently selected namespace, not the untrusted WAL payload. */
	if (!cluster_space_identity_encode(&change.result, check, sizeof(check))
		|| !cluster_space_identity_decode(check, sizeof(check), &expected, &change.result))
		return false;
	/* Foreign structural replay requires its selected per-origin durability
	 * input, not the recovering thread's minRecoveryPoint. Keep it closed
	 * until that owner is wired; never flush a foreign numeric LSN locally. */
	if (change.action != CLUSTER_SPACE_WAL_CREATE
		&& (change.action != CLUSTER_SPACE_WAL_TRUNCATE || cluster_recmerge_apply_foreign
			|| local_thread == 0 || record->cluster_expected_thread_id != local_thread))
		return false;
	rel = smgropen(expected.locator, InvalidBackendId);
	if (change.action == CLUSTER_SPACE_WAL_CREATE)
		smgrcreate(rel, SPACE_FORKNUM, true);
	else if (!smgrexists(rel, SPACE_FORKNUM))
		return false;
	blocks = smgrnblocks(rel, SPACE_FORKNUM);
	if (blocks > 1 || (change.action != CLUSTER_SPACE_WAL_CREATE && blocks != 1))
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
	if (transition != CLUSTER_SPACE_IDENTITY_APPLY
		&& transition != CLUSTER_SPACE_IDENTITY_ALREADY) {
		UnlockReleaseBuffer(buffer);
		return false;
	}
	if (change.action == CLUSTER_SPACE_WAL_TRUNCATE) {
		xl_smgr_truncate truncate;

		truncate.rlocator = expected.locator;
		truncate.blkno = change.nblocks;
		truncate.flags = SMGR_TRUNCATE_ALL;
		/* Native preparation allocates memory; only its physical shrink is
		 * critical. Keep SPACE locked, finish the structural action first,
		 * then publish identity. A restart can repeat a completed shrink even
		 * when SPACE already has result, or still has expected identity. */
		smgr_redo_truncate(record->EndRecPtr, &truncate);
	}
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
