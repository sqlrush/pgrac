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
#include "access/xact.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_page_producer.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_space_recovery.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/checksum.h"
#include "storage/smgr.h"
#include "storage/proc.h"
#include "utils/catcache.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* Backend-private adjunct, like the native relfilenumber mapping cache.
 * No cached pointer is persisted in a relcache initialization file. */
typedef struct SpaceIdentityCacheEntry {
	Oid relid;
	ClusterSpaceIdentity identity;
} SpaceIdentityCacheEntry;

static HTAB *space_identity_cache;
static uint64 space_identity_invalidations;

bool
cluster_space_cold_install_v1(const ClusterSpaceIdentityKey *key,
							  const ClusterSpaceRecoveryInput *inputs,
							  const ClusterSpaceColdSourceV1 *sources, uint32 count, uint32 through,
							  uint8 shrink_forks)
{
	return cluster_space_recovery_cold_relation_install_v1(key, inputs, sources, count, through,
														   shrink_forks);
}

static void
space_identity_invalidate(Datum arg, Oid relid)
{
	Assert(space_identity_cache != NULL);
	if (++space_identity_invalidations == 0)
		elog(ERROR, "SPACE identity invalidation counter exhausted");
	if (OidIsValid(relid))
		hash_search(space_identity_cache, &relid, HASH_REMOVE, NULL);
	else {
		HASH_SEQ_STATUS scan;
		SpaceIdentityCacheEntry *entry;

		hash_seq_init(&scan, space_identity_cache);
		while ((entry = hash_seq_search(&scan)) != NULL)
			hash_search(space_identity_cache, &entry->relid, HASH_REMOVE, NULL);
	}
}

static void
space_identity_cache_init(void)
{
	HASHCTL ctl;

	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(SpaceIdentityCacheEntry);
	ctl.hcxt = CacheMemoryContext;
	space_identity_cache
		= hash_create("SPACE relation identities", 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	CacheRegisterRelcacheCallback(space_identity_invalidate, (Datum)0);
}

static bool
space_identity_key_matches(const ClusterSpaceIdentityKey *a, const ClusterSpaceIdentityKey *b)
{
	return a->system_identifier == b->system_identifier
		   && a->database_incarnation == b->database_incarnation
		   && memcmp(a->storage_uuid, b->storage_uuid, sizeof(a->storage_uuid)) == 0
		   && RelFileLocatorEquals(a->locator, b->locator);
}

static bool
space_namespace(RelFileLocator locator, bool redo, ClusterSpaceIdentityKey *out, uint16 *thread)
{
	ClusterWalSourceRef ref;

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

static void
space_capture_native(Buffer buffer, const ClusterSpaceIdentityKey *key, XLogRecPtr lsn)
{
	ClusterPageWalCaptureResultV1 capture;
	/* Recovery has its original retained-source owner. Never attribute it
	 * to the recovering executor's last local insertion. */
	if (RecoveryInProgress())
		return;
	capture = cluster_page_wal_capture_space_v1(buffer, key, lsn);
	if (capture == CLUSTER_PAGE_WAL_UNATTRIBUTED) {
		if (!cluster_page_wal_forget_v1(buffer))
			elog(PANIC, "SPACE WAL lost its attribution owner");
	} else if (capture != CLUSTER_PAGE_WAL_CAPTURED)
		elog(PANIC, "SPACE WAL lost its native mutation invariant");
}

static Buffer
space_read_identity_buffer(RelFileLocator locator, bool redo, ClusterSpaceIdentityKey *expected)
{
	SMgrRelation rel;

	if (RecoveryInProgress() != redo || !space_namespace(locator, redo, expected, NULL))
		return InvalidBuffer;
	rel = smgropen(locator, InvalidBackendId);
	if (!smgrexists(rel, SPACE_FORKNUM) || smgrnblocks(rel, SPACE_FORKNUM) != 2)
		return InvalidBuffer;
	return ReadBufferWithoutRelcache(locator, SPACE_FORKNUM, 0, RBM_NORMAL, NULL, true);
}

static bool
space_read_locked_identity(Buffer buffer, const ClusterSpaceIdentityKey *expected,
						   ClusterSpaceIdentity *out)
{
	ClusterSpaceIdentity identity;
	bool valid;
	uint64 token;

	valid = BufferGetBlockNumber(buffer) == 0
			&& cluster_space_identity_page_decode(BufferGetPage(buffer), BLCKSZ, SPACE_FORKNUM, 0,
												  expected, &identity, &token)
			&& identity.state == CLUSTER_SPACE_IDENTITY_LIVE;
	UnlockReleaseBuffer(buffer);
	if (valid)
		*out = identity;
	return valid;
}

static bool
space_read_identity(RelFileLocator locator, bool redo, ClusterSpaceIdentity *out)
{
	ClusterSpaceIdentityKey expected;
	Buffer buffer;

	if (out == NULL)
		return false;
	buffer = space_read_identity_buffer(locator, redo, &expected);
	if (!BufferIsValid(buffer))
		return false;
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	return space_read_locked_identity(buffer, &expected, out);
}

bool
cluster_space_relation_read_identity(RelFileLocator locator, ClusterSpaceIdentity *out)
{
	return space_read_identity(locator, false, out);
}

bool
cluster_space_relation_read_maintenance_identity(RelFileLocator locator, ClusterSpaceIdentity *out)
{
	ClusterSpaceIdentityKey expected;
	Buffer buffer;

	if (out == NULL)
		return false;
	buffer = space_read_identity_buffer(locator, false, &expected);
	if (!BufferIsValid(buffer))
		return false;
	/* The legacy shared-read requester is backend-indexed. Maintenance
	 * already uses this current owner for its target page; use the same
	 * auxiliary-capable path for the preceding identity read. */
	if (!ClusterLockBufferExclusiveRetryAware(buffer)) {
		ReleaseBuffer(buffer);
		return false;
	}
	return space_read_locked_identity(buffer, &expected, out);
}

bool
cluster_space_relation_read_redo_identity(RelFileLocator locator, ClusterSpaceIdentity *out)
{
	return space_read_identity(locator, true, out);
}

bool
cluster_space_relation_get_identity(Relation relation, ClusterSpaceIdentity *out)
{
	if (relation == NULL || out == NULL || relation->rd_rel == NULL
		|| !OidIsValid(RelationGetRelid(relation)))
		return false;

	for (;;) {
		ClusterSpaceIdentityKey expected, current;
		ClusterSpaceIdentity identity;
		SpaceIdentityCacheEntry *entry;
		Oid relid = RelationGetRelid(relation);
		uint64 invalidations;
		bool valid;

		CHECK_FOR_INTERRUPTS();
		if (!relation->rd_isvalid) {
			if (space_identity_cache != NULL)
				space_identity_invalidate((Datum)0, relid);
			return false;
		}
		if (RecoveryInProgress() || !RelationIsPermanent(relation) || !RelationNeedsWAL(relation)
			|| !space_namespace(relation->rd_locator, false, &expected, NULL))
			return false;
		if (space_identity_cache == NULL)
			space_identity_cache_init();
		entry = hash_search(space_identity_cache, &relid, HASH_FIND, NULL);
		if (entry != NULL && space_identity_key_matches(&entry->identity.key, &expected)) {
			*out = entry->identity;
			return true;
		}
		/* Never retain an entry or SMgr pointer across native buffer access:
		 * it may process invalidations and remove the very entry just found. */
		hash_search(space_identity_cache, &relid, HASH_REMOVE, NULL);
		invalidations = space_identity_invalidations;
		valid = cluster_space_relation_read_identity(expected.locator, &identity);
		if (invalidations != space_identity_invalidations)
			continue;
		if (!valid || !relation->rd_isvalid || RelationGetRelid(relation) != relid
			|| !space_namespace(relation->rd_locator, false, &current, NULL))
			return false;
		if (!space_identity_key_matches(&expected, &current))
			continue;
		entry = hash_search(space_identity_cache, &relid, HASH_ENTER, NULL);
		entry->identity = identity;
		*out = identity;
		return true;
	}
}

static bool
space_copy_prepare(const ClusterSpaceIdentity *identity, ForkNumber forknum, BlockNumber block,
				   const void *source, Page before, uint8 before_kind, Page result,
				   RfPageProducerBatchV1 *batch)
{
	ClusterSpaceIdentityKey expected;
	ClusterSpaceIdentity checked;
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];
	RfPageProducerComponentV1 component;

	if (identity == NULL || source == NULL || block == InvalidBlockNumber || RecoveryInProgress()
		|| (forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM && forknum != FSM_FORKNUM)
		|| !space_namespace(identity->key.locator, false, &expected, NULL)
		|| !cluster_space_identity_encode(identity, encoded, sizeof(encoded))
		|| !cluster_space_identity_decode(encoded, sizeof(encoded), &expected, &checked)
		|| checked.state != CLUSTER_SPACE_IDENTITY_LIVE)
		return false;
	/* An unformatted ordinary source is not PRESENT. Its typed structural
	 * replay must be supplied before that copy shape can be admitted. */
	if ((((PageHeader)source)->pd_flags & (PD_SPACE_METADATA | PD_UNDO_SEG_HEADER)) != 0
		|| (forknum != FSM_FORKNUM && PageIsNew((Page)source)))
		return false;
	memset(&component, 0, sizeof(component));
	memset(result, 0, BLCKSZ);
	if (before_kind == RF_PAGE_STATE_UNFORMATTED) {
		/* A zero header with surviving bytes is not the new-fork base. The
		 * native copy owner holds its exclusive content lock across this
		 * exact before observation and publication. */
		if (before == NULL || memcmp(before, result, BLCKSZ) != 0)
			return false;
	} else if (before_kind != RF_PAGE_STATE_ABSENT || before != NULL)
		return false;
	if (forknum == FSM_FORKNUM) {
		component.page_class = RF_PAGE_CLASS_REBUILDABLE_FSM;
		component.before_kind = RF_PAGE_STATE_REBUILDABLE;
	} else {
		component.page_class = RF_PAGE_CLASS_ORDINARY;
		component.before_kind = before_kind;
		component.page = result;
		memcpy(component.segment_incarnation, checked.incarnation, 16);
	}
	/* Capture before copying source bytes. The original owner proves the
	 * initially empty fork and retains the object lifecycle lock. */
	if (!rf_page_producer_prepare_v1(&component, 1, batch))
		return false;
	memcpy(result, source, BLCKSZ);
	if (forknum != FSM_FORKNUM)
		((PageHeader)result)->pd_block_scn = 0;
	return rf_page_producer_stamp_v1(batch);
}

ClusterSpaceHintResult
cluster_space_hint_begin(Buffer buffer, RfPageProducerBatchV1 *batch)
{
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber block;
	ClusterSpaceIdentityKey expected;
	ClusterSpaceIdentity identity;
	SpaceIdentityCacheEntry *entry;
	HASH_SEQ_STATUS scan;
	const uint8 block_id = 0;
	bool found = false;

	if (!BufferIsValid(buffer) || batch == NULL)
		return CLUSTER_SPACE_HINT_SKIPPED;
	if (!cluster_shared_config || BufferIsLocal(buffer) || !BufferIsPermanent(buffer))
		return CLUSTER_SPACE_HINT_NATIVE;
	BufferGetTag(buffer, &locator, &forknum, &block);
	if (cluster_smgr_which_for(locator, InvalidBackendId) != 1 || forknum == FSM_FORKNUM)
		return CLUSTER_SPACE_HINT_NATIVE;
	if ((forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM)
		|| RecoveryInProgress() || CritSectionCount != 0
		|| !LWLockHeldByMeInMode(BufferDescriptorGetContentLock(GetBufferDescriptor(buffer - 1)),
							   LW_EXCLUSIVE)
		|| !cluster_bufmgr_pcm_x_content_holder_write_permitted(GetBufferDescriptor(buffer - 1))
		|| space_identity_cache == NULL
		|| !space_namespace(locator, false, &expected, NULL))
		return CLUSTER_SPACE_HINT_SKIPPED;

	/* The relation owner filled this cache before taking content locks and
	 * retains the lifecycle lock. A miss is never repaired while locked. */
	hash_seq_init(&scan, space_identity_cache);
	while ((entry = hash_seq_search(&scan)) != NULL) {
		if (space_identity_key_matches(&entry->identity.key, &expected)) {
			identity = entry->identity;
			found = true;
			hash_seq_term(&scan);
			break;
		}
	}
	if (!found || !cluster_space_prepare_buffer_versions(&identity, &buffer, &block_id, 1, batch))
		return CLUSTER_SPACE_HINT_SKIPPED;
	START_CRIT_SECTION();
	if (!rf_page_producer_stamp_v1(batch))
		elog(PANIC, "shared hint page version changed before mutation");
	return CLUSTER_SPACE_HINT_VERSIONED;
}

void
cluster_space_hint_finish(Buffer buffer, bool standard, const RfPageProducerBatchV1 *batch)
{
	XLogRecPtr lsn;

	if (CritSectionCount == 0 || batch == NULL || !batch->stamped
		|| batch->entry_count != 1 || batch->entries[0].block_id != 0
		|| batch->ordinary_pages[0] != BufferGetPage(buffer))
		elog(PANIC, "shared hint has no exact prepared page version");
	MarkBufferDirty(buffer);
	XLogBeginInsert();
	XLogRegisterBuffer(0, buffer, REGBUF_FORCE_IMAGE | (standard ? REGBUF_STANDARD : 0));
	if (!rf_page_producer_register_wal_v1(batch))
		elog(PANIC, "shared hint cannot register page version");
	lsn = XLogInsert(RM_XLOG_ID, XLOG_FPI_FOR_HINT);
	PageSetLSN(BufferGetPage(buffer), lsn);
	END_CRIT_SECTION();
}

bool
cluster_space_buffer_version_component(const ClusterSpaceIdentity *identity, Buffer buffer,
									   uint8 block_id, uint16 ordinal,
									   RfPageProducerComponentV1 *component)
{
	ClusterSpaceIdentityKey expected;
	ClusterSpaceIdentity checked;
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber block;
	Page page;
	RfPageProducerComponentV1 captured;

	if (identity == NULL || component == NULL || RecoveryInProgress()
		|| !space_namespace(identity->key.locator, false, &expected, NULL)
		|| !cluster_space_identity_encode(identity, encoded, sizeof(encoded))
		|| !cluster_space_identity_decode(encoded, sizeof(encoded), &expected, &checked)
		|| checked.state != CLUSTER_SPACE_IDENTITY_LIVE || !BufferIsValid(buffer)
		|| BufferIsLocal(buffer) || !BufferIsPermanent(buffer))
		return false;
	BufferGetTag(buffer, &locator, &forknum, &block);
	page = BufferGetPage(buffer);
	if (!RelFileLocatorEquals(locator, checked.key.locator) || block == InvalidBlockNumber
		|| (forknum != MAIN_FORKNUM && forknum != VISIBILITYMAP_FORKNUM)
		|| (((PageHeader)page)->pd_flags & (PD_SPACE_METADATA | PD_UNDO_SEG_HEADER)) != 0)
		return false;
	memset(&captured, 0, sizeof(captured));
	captured.block_id = block_id;
	captured.component_ordinal = ordinal;
	captured.page_class = RF_PAGE_CLASS_ORDINARY;
	captured.before_kind = RF_PAGE_STATE_PRESENT;
	captured.page = page;
	memcpy(captured.segment_incarnation, checked.incarnation, 16);
	*component = captured;
	return true;
}

bool
cluster_space_prepare_buffer_versions(const ClusterSpaceIdentity *identity, const Buffer *buffers,
									  const uint8 *block_ids, uint8 count,
									  RfPageProducerBatchV1 *batch)
{
	RfPageProducerComponentV1 components[RF_PAGE_PRODUCER_MAX_COMPONENTS];
	int i;

	if (buffers == NULL || block_ids == NULL || batch == NULL || count == 0
		|| count > RF_PAGE_PRODUCER_MAX_COMPONENTS)
		return false;
	for (i = 0; i < count; i++)
		if (!cluster_space_buffer_version_component(identity, buffers[i], block_ids[i], i,
													&components[i]))
			return false;
	return rf_page_producer_prepare_v1(components, count, batch);
}

static XLogRecPtr
space_copy_insert(const ClusterSpaceIdentity *identity, ForkNumber forknum, BlockNumber block,
				  Page result, const RfPageProducerBatchV1 *batch)
{
	RelFileLocator locator = identity->key.locator;
	XLogRecPtr recptr;

	XLogRegisterBlock(0, &locator, forknum, block, result, REGBUF_FORCE_IMAGE);
	if (!rf_page_producer_register_wal_v1(batch))
		elog(ERROR, "new SPACE page version changed before WAL registration");
	recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);
	if (!PageIsNew(result)) {
		if (forknum == FSM_FORKNUM)
			PageSetLSN(result, recptr);
		else
			space_set_lsn(result, recptr, batch->result_token);
	}
	return recptr;
}

static bool
space_copy_page_wal(const ClusterSpaceIdentity *identity, ForkNumber forknum, BlockNumber block,
					void *page, const void *zero_before, XLogRecPtr *lsn)
{
	RfPageProducerBatchV1 batch;
	PGAlignedBlock result;
	XLogRecPtr recptr;

	if (lsn == NULL
		|| !space_copy_prepare(identity, forknum, block, page, (Page)zero_before,
							   zero_before == NULL ? RF_PAGE_STATE_ABSENT
												   : RF_PAGE_STATE_UNFORMATTED,
							   result.data, &batch))
		return false;
	XLogBeginInsert();
	recptr = space_copy_insert(identity, forknum, block, result.data, &batch);
	memcpy(page, result.data, BLCKSZ);
	*lsn = recptr;
	return true;
}

bool
cluster_space_copy_page_wal(const ClusterSpaceIdentity *identity, ForkNumber forknum,
							BlockNumber block, void *page, XLogRecPtr *lsn)
{
	return space_copy_page_wal(identity, forknum, block, page, NULL, lsn);
}

bool
cluster_space_btree_build_page_wal(const ClusterSpaceIdentity *identity, BlockNumber block,
								   void *page, const void *zero_before, XLogRecPtr *lsn)
{
	return space_copy_page_wal(identity, MAIN_FORKNUM, block, page, zero_before, lsn);
}

bool
cluster_space_copy_buffer_wal(const ClusterSpaceIdentity *identity, const void *source,
							  Buffer destination)
{
	RfPageProducerBatchV1 batch;
	PGAlignedBlock result;
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber block;
	Page target;

	if (identity == NULL || !BufferIsValid(destination) || BufferIsLocal(destination)
		|| !BufferIsPermanent(destination))
		return false;
	BufferGetTag(destination, &locator, &forknum, &block);
	target = BufferGetPage(destination);
	if (!RelFileLocatorEquals(locator, identity->key.locator) || source == target
		|| !space_copy_prepare(identity, forknum, block, source, target, RF_PAGE_STATE_UNFORMATTED,
							   result.data, &batch))
		return false;
	XLogBeginInsert();
	START_CRIT_SECTION();
	/* A checkpoint whose redo follows this FPI must already select the
	 * destination. Its content lock prevents writeout until publication;
	 * marking it only after WAL insertion could leave a zero DATA page
	 * behind that checkpoint's redo boundary. */
	MarkBufferDirty(destination);
	(void)space_copy_insert(identity, forknum, block, result.data, &batch);
	memcpy(target, result.data, BLCKSZ);
	END_CRIT_SECTION();
	return true;
}

bool
cluster_space_init_heap_buffer_wal(const ClusterSpaceIdentity *identity, Buffer destination)
{
	PGAlignedBlock initialized;
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber block;

	if (!BufferIsValid(destination) || BufferIsLocal(destination))
		return false;
	BufferGetTag(destination, &locator, &forknum, &block);
	if (forknum != MAIN_FORKNUM)
		return false;
	/* Only private bytes change here. The existing buffered publisher captures
	 * the actual all-zero predecessor before the first target mutation. */
	PageInitHeapPage(initialized.data, BLCKSZ, 0);
	return cluster_space_copy_buffer_wal(identity, initialized.data, destination);
}

bool
cluster_space_init_vm_buffer_wal(const ClusterSpaceIdentity *identity, Buffer destination)
{
	PGAlignedBlock initialized;
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber block;

	if (!BufferIsValid(destination) || BufferIsLocal(destination))
		return false;
	BufferGetTag(destination, &locator, &forknum, &block);
	if (forknum != VISIBILITYMAP_FORKNUM)
		return false;
	/* VM has no heap tuples or ITL area. The buffered publisher proves the
	 * actual zero predecessor and stamps this private native-layout image. */
	PageInit(initialized.data, BLCKSZ, 0);
	return cluster_space_copy_buffer_wal(identity, initialized.data, destination);
}

bool
cluster_space_relation_create(RelFileLocator locator)
{
	ClusterSpaceStructureChange change = {0};
	ClusterSpaceWalChange *identity = &change.identity;
	PGAlignedBlock result[2];
	uint8 bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint8 apply_mask;
	SMgrRelation rel;
	Buffer buffers[2] = {InvalidBuffer, InvalidBuffer};
	XLogRecPtr lsn;
	bool success = false;

	if (!cluster_shared_config || cluster_smgr_which_for(locator, InvalidBackendId) != 1)
		return true;
	if (RecoveryInProgress() || !space_namespace(locator, false, &identity->result.key, NULL))
		return false;
	identity->action = CLUSTER_SPACE_WAL_CREATE;
	identity->nblocks = InvalidBlockNumber;
	identity->result_token = rf_page_mutation_token_next();
	identity->result.sequence = 1;
	identity->result.operation = identity->result_token;
	identity->result.state = CLUSTER_SPACE_IDENTITY_LIVE;
	if (!pg_strong_random(identity->result.incarnation, sizeof(identity->result.incarnation)))
		return false;
	change.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
	change.reservation.result.identity = identity->result;
	change.reservation.result_token = identity->result_token;
	if (!cluster_space_structure_wal_encode(&change, bytes, sizeof(bytes)))
		return false;
	/* All namespace/randomness checks precede storage mutation. An existing
	 * fork is never proof that this create owns its identity. */
	rel = smgropen(locator, InvalidBackendId);
	if (smgrexists(rel, SPACE_FORKNUM))
		return false;
	smgrcreate(rel, SPACE_FORKNUM, false);
	for (int i = 0; i < 2; i++) {
		buffers[i] = ReadBufferWithoutRelcache(locator, SPACE_FORKNUM, P_NEW,
											 RBM_ZERO_AND_LOCK, NULL, true);
		if (BufferGetBlockNumber(buffers[i]) != i)
			goto done;
		memcpy(result[i].data, BufferGetPage(buffers[i]), BLCKSZ);
	}
	if (cluster_space_structure_apply(&change, &identity->result.key, result[0].data,
			result[1].data, BLCKSZ, &apply_mask) != CLUSTER_SPACE_IDENTITY_APPLY || apply_mask != 3)
		goto done;
	XLogBeginInsert();
	START_CRIT_SECTION();
	for (int i = 0; i < 2; i++) {
		memcpy(BufferGetPage(buffers[i]), result[i].data, BLCKSZ);
		MarkBufferDirty(buffers[i]);
	}
	XLogRegisterData((char *)bytes, sizeof(bytes));
	lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE);
	for (int i = 0; i < 2; i++) {
		space_set_lsn(BufferGetPage(buffers[i]), lsn, identity->result_token);
		space_capture_native(buffers[i], &identity->result.key, lsn);
	}
	END_CRIT_SECTION();
	success = true;
done:
	for (int i = 1; i >= 0; i--)
		if (BufferIsValid(buffers[i]))
			UnlockReleaseBuffer(buffers[i]);
	return success;
}

BlockNumber
cluster_space_reserve(const ClusterSpaceIdentity *identity, const struct HwLock *lock,
					  uint32 want, uint32 *granted)
{
	ClusterSpaceIdentityKey expected;
	ClusterSpaceIdentity checked;
	ClusterSpaceReservationChange change = {0};
	ClusterResId resid;
	PGAlignedBlock result;
	uint8 expected_identity[CLUSTER_SPACE_IDENTITY_BYTES];
	uint8 before_identity[CLUSTER_SPACE_IDENTITY_BYTES];
	uint8 bytes[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	BlockNumber first = InvalidBlockNumber;
	Buffer buffer;
	XLogRecPtr lsn;

	if (granted == NULL)
		return InvalidBlockNumber;
	*granted = 0;
	if (identity == NULL || lock == NULL || !lock->held || !lock->coordinated
		|| lock->req.lockmode != ExclusiveLock || want == 0 || RecoveryInProgress()
		|| !space_namespace(identity->key.locator, false, &expected, NULL)
		|| !cluster_space_identity_encode(identity, expected_identity, sizeof(expected_identity))
		|| !cluster_space_identity_decode(expected_identity, sizeof(expected_identity), &expected, &checked)
		|| checked.state != CLUSTER_SPACE_IDENTITY_LIVE)
		return InvalidBlockNumber;
	cluster_hw_resid_encode(expected.locator, MAIN_FORKNUM, &resid);
	if (memcmp(&resid, &lock->req.resid, sizeof(resid)) != 0)
		return InvalidBlockNumber;

	/* Existing HW(X) serializes reservations; existing GCS current/content-X
	 * protects the canonical bytes. No separate master counter or raw-DATA
	 * channel can grant space. A missing/unformatted target never seeds zero. */
	buffer = ReadBufferWithoutRelcache(expected.locator, SPACE_FORKNUM,
		CLUSTER_SPACE_RESERVATION_BLOCK, RBM_NORMAL, NULL, true);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	if (BufferIsLocal(buffer) || BufferGetBlockNumber(buffer) != CLUSTER_SPACE_RESERVATION_BLOCK
		|| !cluster_bufmgr_pcm_x_content_holder_write_permitted(GetBufferDescriptor(buffer - 1))
		|| !cluster_space_reservation_page_decode(BufferGetPage(buffer), BLCKSZ, SPACE_FORKNUM,
			CLUSTER_SPACE_RESERVATION_BLOCK, &expected, &change.before, &change.before_token)
		|| !cluster_space_identity_encode(&change.before.identity, before_identity, sizeof(before_identity))
		|| memcmp(before_identity, expected_identity, sizeof(before_identity)) != 0)
		goto done;
	change.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	change.result = change.before;
	change.first_block = cluster_hw_alloc_segment(change.before.next_block, want,
		&change.granted, &change.result.next_block);
	if (change.granted == 0)
		goto done;
	change.result_token = rf_page_mutation_token_next();
	if (!cluster_space_reservation_wal_encode(&change, bytes, sizeof(bytes)))
		goto done;
	memcpy(result.data, BufferGetPage(buffer), BLCKSZ);
	if (cluster_space_reservation_apply(&change, &expected, result.data, BLCKSZ)
		!= CLUSTER_SPACE_IDENTITY_APPLY)
		goto done;
	XLogBeginInsert();
	START_CRIT_SECTION();
	memcpy(BufferGetPage(buffer), result.data, BLCKSZ);
	MarkBufferDirty(buffer);
	XLogRegisterData((char *)bytes, sizeof(bytes));
	lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION | XLR_SPECIAL_REL_UPDATE);
	space_set_lsn(BufferGetPage(buffer), lsn, change.result_token);
	space_capture_native(buffer, &expected, lsn);
	END_CRIT_SECTION();
	XLogFlush(lsn);
	/* A membership/activation fence during the flush may deny the grant.
	 * Its durable reservation remains consumed; never roll it back or re-use
	 * its range. The original checkpoint owns eventual DATA durability. */
	if (cluster_bufmgr_pcm_x_content_holder_write_permitted(GetBufferDescriptor(buffer - 1))) {
		first = change.first_block;
		*granted = change.granted;
	}
done:
	UnlockReleaseBuffer(buffer);
	return first;
}

bool
cluster_space_reserve_exact(const ClusterSpaceIdentity *identity, BlockNumber first, uint32 count)
{
	ClusterResId resid;
	HwLock lock;
	BlockNumber reserved;
	uint32 granted = 0;

	if (identity == NULL || count == 0 || first == InvalidBlockNumber
		|| (uint64)first + count > InvalidBlockNumber)
		return false;
	cluster_hw_resid_encode(identity->key.locator, MAIN_FORKNUM, &resid);
	if (!cluster_hw_lock(&resid, &lock))
		return false;
	PG_TRY();
	{
		reserved = cluster_space_reserve(identity, &lock, count, &granted);
	}
	PG_FINALLY();
	{
		cluster_hw_unlock(&lock);
	}
	PG_END_TRY();
	return reserved == first && granted == count;
}

struct ClusterSpaceTruncateState {
	Buffer buffers[2];
	ClusterKoCompletionV2 *ko_completion;
	PGAlignedBlock result[2];
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint64 token;
	XLogRecPtr lsn;
	bool base_synced;
	bool truncate_published;
};

struct ClusterSpaceDropState {
	int count;
	bool published;
	char *wal;
	uint32 wal_len;
	ClusterSpaceTruncateState *entries[FLEXIBLE_ARRAY_MEMBER];
};

StaticAssertDecl(XACT_SPACE_DROP_RECORD_BYTES == CLUSTER_SPACE_STRUCTURE_WAL_BYTES,
	"native COMMIT and SPACE structural formats must agree");

static void
space_truncate_release(ClusterSpaceTruncateState *state)
{
	for (int i = 1; i >= 0; i--)
		if (BufferIsValid(state->buffers[i]))
			UnlockReleaseBuffer(state->buffers[i]);
	pfree(state);
}

static ClusterSpaceTruncateState *
space_structure_prepare(const ClusterSpaceIdentityKey *expected,
						ClusterSpaceWalAction action, BlockNumber nblocks)
{
	ClusterSpaceTruncateState *state;
	ClusterSpaceStructureChange change = {0};
	ClusterSpaceWalChange *identity = &change.identity;
	ClusterSpaceReservationChange *reservation = &change.reservation;
	SMgrRelation smgr;
	uint8 apply_mask;

	Assert(action == CLUSTER_SPACE_WAL_TRUNCATE || action == CLUSTER_SPACE_WAL_TOMBSTONE);
	smgr = smgropen(expected->locator, InvalidBackendId);
	if (!smgrexists(smgr, SPACE_FORKNUM) || smgrnblocks(smgr, SPACE_FORKNUM) != 2)
		return NULL;
	state = palloc0(sizeof(*state));
	for (int i = 0; i < 2; i++) {
		state->buffers[i] = ReadBufferWithoutRelcache(expected->locator, SPACE_FORKNUM, i,
			RBM_NORMAL, NULL, true);
		LockBuffer(state->buffers[i], BUFFER_LOCK_EXCLUSIVE);
		if (BufferIsLocal(state->buffers[i]) || BufferGetBlockNumber(state->buffers[i]) != i
			|| !cluster_bufmgr_pcm_x_content_holder_write_permitted(
				GetBufferDescriptor(state->buffers[i] - 1)))
			goto refused;
		memcpy(state->result[i].data, BufferGetPage(state->buffers[i]), BLCKSZ);
	}
	if (!cluster_space_identity_page_decode(state->result[0].data, BLCKSZ, SPACE_FORKNUM, 0,
			expected, &identity->expected, &identity->before_token)
		|| !cluster_space_reservation_page_decode(state->result[1].data, BLCKSZ, SPACE_FORKNUM, 1,
			expected, &reservation->before, &reservation->before_token)
		|| identity->expected.state != CLUSTER_SPACE_IDENTITY_LIVE
		|| identity->expected.sequence == UINT64_MAX
		|| (action == CLUSTER_SPACE_WAL_TRUNCATE && nblocks > reservation->before.next_block))
		goto refused;
	/* Take the original barrier while the exact old identity is still
	 * locked. Its ResourceOwner retains the completion through transaction
	 * exit; preparing a replacement page does not prove durable completion. */
	if (!cluster_ko_shared_claim_v2(expected, identity->expected.incarnation,
								  &state->ko_completion))
		goto refused;
	identity->action = action;
	identity->nblocks = nblocks;
	identity->result = identity->expected;
	identity->result.sequence++;
	if (action == CLUSTER_SPACE_WAL_TRUNCATE) {
		if (!pg_strong_random(identity->result.incarnation, sizeof(identity->result.incarnation)))
			goto refused;
	} else
		identity->result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	identity->result.operation = identity->result_token = rf_page_mutation_token_next();
	reservation->action = action == CLUSTER_SPACE_WAL_TRUNCATE
		? CLUSTER_SPACE_RESERVATION_RESET : CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	reservation->result.identity = identity->result;
	reservation->first_block = action == CLUSTER_SPACE_WAL_TRUNCATE ? nblocks : 0;
	reservation->result.next_block = action == CLUSTER_SPACE_WAL_TRUNCATE
		? nblocks : reservation->before.next_block;
	reservation->result_token = identity->result_token;
	if (!cluster_space_structure_wal_encode(&change, state->wal, sizeof(state->wal))
		|| cluster_space_structure_apply(&change, expected, state->result[0].data,
			state->result[1].data, BLCKSZ, &apply_mask) != CLUSTER_SPACE_IDENTITY_APPLY
		|| apply_mask != 3)
		goto refused;
	state->token = identity->result_token;
	return state;
refused:
	space_truncate_release(state);
	return NULL;
}

ClusterSpaceTruncateState *
cluster_space_truncate_prepare(Relation rel, BlockNumber nblocks)
{
	ClusterSpaceIdentityKey expected;
	ClusterSpaceTruncateState *state;
	SMgrRelation smgr;

	if (rel == NULL || RecoveryInProgress() || !RelationIsPermanent(rel)
		|| !RelationNeedsWAL(rel) || nblocks == InvalidBlockNumber
		|| !space_namespace(rel->rd_locator, false, &expected, NULL))
		return NULL;
	smgr = smgropen(expected.locator, InvalidBackendId);
	if (!smgrexists(smgr, SPACE_FORKNUM) || smgrnblocks(smgr, SPACE_FORKNUM) != 2)
		return NULL;
	/* KO has drained peer buffers. No SPACE content lock is held while
	 * finishing writeback/fsync of the base inherited by the new UUID. */
	FlushRelationBuffers(rel);
	for (ForkNumber fork = MAIN_FORKNUM; fork <= SPACE_FORKNUM; fork++) {
		if (fork == INIT_FORKNUM)
			continue;
		smgr = smgropen(expected.locator, InvalidBackendId);
		if (smgrexists(smgr, fork))
			smgrimmedsync(smgr, fork);
	}
	/* End the origin's old MAIN/VM carriers, including surviving low
	 * blocks, before changing the incarnation. A clean cached-X still has
	 * its latest WAL binding and could otherwise create old PI on a later
	 * handoff. The original eviction keeps any logical PI responsibility;
	 * failure leaves the structural pages and files unchanged. */
	{
		ForkNumber forks[] = { MAIN_FORKNUM, VISIBILITYMAP_FORKNUM };
		BlockNumber first[] = { 0, 0 };
		smgr = smgropen(expected.locator, InvalidBackendId);
		DropRelationBuffers(smgr, forks, lengthof(forks), first);
	}
	state = space_structure_prepare(&expected, CLUSTER_SPACE_WAL_TRUNCATE, nblocks);
	if (state != NULL)
		state->base_synced = true;
	return state;
}

static XLogRecPtr
space_structure_log(ClusterSpaceTruncateState *state)
{
	Assert(state != NULL && CritSectionCount > 0);
	/* Mark before WAL insertion so a concurrent checkpoint cannot advance
	 * its redo past this record without selecting both still-locked pages. */
	for (int i = 0; i < 2; i++)
		MarkBufferDirty(state->buffers[i]);
	XLogBeginInsert();
	XLogRegisterData((char *)state->wal, sizeof(state->wal));
	state->lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE);
	return state->lsn;
}

static void
space_structure_publish(ClusterSpaceTruncateState *state, XLogRecPtr lsn)
{
	Assert(state != NULL && CritSectionCount > 0 && !XLogRecPtrIsInvalid(lsn));
	state->lsn = lsn;
	for (int i = 0; i < 2; i++) {
		memcpy(BufferGetPage(state->buffers[i]), state->result[i].data, BLCKSZ);
		space_set_lsn(BufferGetPage(state->buffers[i]), lsn, state->token);
		if (!RecoveryInProgress()) {
			ClusterSpaceStructureChange change;
			if (!cluster_space_structure_wal_decode(state->wal, sizeof(state->wal), &change))
				elog(PANIC, "SPACE publisher lost its original structural record");
			space_capture_native(state->buffers[i], &change.identity.result.key, lsn);
		}
	}
}

XLogRecPtr
cluster_space_truncate_log(ClusterSpaceTruncateState *state)
{
	return space_structure_log(state);
}

void
cluster_space_truncate_publish(ClusterSpaceTruncateState *state)
{
	space_structure_publish(state, state->lsn);
	/* The sole native caller has completed smgrtruncate2 and synced every
	 * shrunken fork inside its original critical section before publishing.
	 * Merely logging the record or publishing DROP cannot set this phase. */
	state->truncate_published = true;
}

/* The caller retains both content-X locks and relation lifecycle authority.
 * A successful sync alone is not an exact structural completion observation.
 * Check the raw storage copy, independently of ignore_checksum_failure, before
 * normalizing only the checksum that the native writer sets in its copy. */
static bool
space_structure_readback(SMgrRelation rel, Buffer buffer, BlockNumber block)
{
	PGIOAlignedBlock disk;
	const PageHeaderData *expected = (const PageHeaderData *)BufferGetPage(buffer);

	smgrread(rel, SPACE_FORKNUM, block, disk.data);
	if (PageIsNew(disk.data)
		|| (DataChecksumsEnabled()
			&& ((PageHeader)disk.data)->pd_checksum != pg_checksum_page(disk.data, block)))
		return false;
	((PageHeader)disk.data)->pd_checksum = expected->pd_checksum;
	return memcmp(disk.data, expected, BLCKSZ) == 0;
}

/* Keep the original record with its preallocated KO owner. Missing native
 * attribution cannot manufacture a structural completion or retire a PI. */
static void
space_structure_observe(ClusterSpaceTruncateState *state)
{
	ClusterSpaceStructureChange change;
	ClusterPageWalBindingV1 before, after, certified;
	if (state->ko_completion == NULL || RecoveryInProgress())
		return;
	if (!cluster_space_structure_wal_decode(state->wal, sizeof(state->wal), &change))
		elog(PANIC, "SPACE observation lost its original structural record");
	if (!cluster_page_wal_read_v1(state->buffers[0], &change.identity.result, &before)
		|| before.record_end != state->lsn
		|| !cluster_page_wal_flush_source_v1(&before, &certified)
		|| !cluster_page_wal_read_v1(state->buffers[0], &change.identity.result, &after)
		|| memcmp(&before, &after, sizeof(before)) != 0)
		return;
	if (cluster_ko_shared_observe_space_v2(state->ko_completion, &certified,
			state->wal, sizeof(state->wal))
		&& change.identity.action == CLUSTER_SPACE_WAL_TRUNCATE && state->base_synced
		&& state->truncate_published)
		(void)cluster_ko_shared_observe_truncate_v2(state->ko_completion);
}

void
cluster_space_truncate_finish(ClusterSpaceTruncateState *state, Relation rel)
{
	Assert(state != NULL && CritSectionCount == 0);
	/* The caller still holds the relation lifecycle lock and both SPACE
	 * content locks. Persist the new incarnation before another writer can
	 * make its pages durable: an old on-disk identity cannot distinguish
	 * those pages from the retired incarnation after a crash. */
	PG_TRY();
	{
		SMgrRelation smgr;

		for (int i = 0; i < 2; i++)
			FlushOneBuffer(state->buffers[i]);
		smgr = smgropen(rel->rd_locator, InvalidBackendId);
		smgrimmedsync(smgr, SPACE_FORKNUM);
		for (int i = 0; i < 2; i++)
			if (!space_structure_readback(smgr, state->buffers[i], i))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("truncated SPACE page does not match its durable write"),
								errdetail("Relation %u/%u/%u, SPACE block %d.", rel->rd_locator.spcOid,
										  rel->rd_locator.dbOid, rel->rd_locator.relNumber, i),
								errhint("Check the storage device before restarting the instance.")));
		space_structure_observe(state);
	}
	PG_CATCH();
	{
		/* The published shared pages cannot be rolled back. An ordinary
		 * transaction abort would release the locks and expose that cached
		 * incarnation to other writers without its durable SPACE anchor. */
		ereport(PANIC, (errcode(ERRCODE_IO_ERROR),
						errmsg("could not durably publish the truncated relation identity"),
						errdetail("Relation %u/%u/%u has an unconfirmed SPACE incarnation.",
								  rel->rd_locator.spcOid, rel->rd_locator.dbOid,
								  rel->rd_locator.relNumber),
						errhint("Restore storage availability and restart the instance to recover "
								"the truncation.")));
	}
	PG_END_TRY();
	space_truncate_release(state);
	if (space_identity_cache != NULL)
		space_identity_invalidate((Datum)0, RelationGetRelid(rel));
	CacheInvalidateRelcache(rel);
}

static int
space_locator_compare(const void *a, const void *b)
{
	const RelFileLocator *left = a, *right = b;

	if (left->spcOid != right->spcOid)
		return left->spcOid < right->spcOid ? -1 : 1;
	if (left->dbOid != right->dbOid)
		return left->dbOid < right->dbOid ? -1 : 1;
	if (left->relNumber != right->relNumber)
		return left->relNumber < right->relNumber ? -1 : 1;
	return 0;
}

ClusterSpaceDropState *
cluster_space_drop_prepare(const RelFileLocator *locators, int count)
{
	ClusterSpaceDropState *state;
	RelFileLocator *sorted;

	if (!cluster_shared_config || RecoveryInProgress() || count <= 0 || locators == NULL
		|| (Size)count > (MaxAllocSize - sizeof(*state))
			/ (sizeof(*sorted) + sizeof(*state->entries))
		|| (Size)count > (MaxAllocSize - sizeof(uint32)) / CLUSTER_SPACE_STRUCTURE_WAL_BYTES)
		return NULL;
	state = palloc0(sizeof(*state) + (Size)count * sizeof(*state->entries));
	sorted = palloc((Size)count * sizeof(*sorted));
	memcpy(sorted, locators, (Size)count * sizeof(*sorted));
	qsort(sorted, count, sizeof(*sorted), space_locator_compare);
	for (int i = 0; i < count; i++) {
		ClusterSpaceIdentityKey expected;
		ClusterSpaceTruncateState *entry;

		if ((i != 0 && RelFileLocatorEquals(sorted[i], sorted[i - 1]))
			|| cluster_smgr_which_for(sorted[i], InvalidBackendId) != 1)
			continue;
		if (!space_namespace(sorted[i], false, &expected, NULL))
			goto refused;
		entry = space_structure_prepare(&expected, CLUSTER_SPACE_WAL_TOMBSTONE,
			InvalidBlockNumber);
		if (entry == NULL)
			goto refused;
		state->entries[state->count++] = entry;
	}
	if (state->count != 0) {
		uint32 n = state->count;

		state->wal_len = sizeof(n) + n * CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
		state->wal = palloc(state->wal_len);
		memcpy(state->wal, &n, sizeof(n));
		for (int i = 0; i < state->count; i++)
			memcpy(state->wal + sizeof(n) + (Size)i * CLUSTER_SPACE_STRUCTURE_WAL_BYTES,
				state->entries[i]->wal, CLUSTER_SPACE_STRUCTURE_WAL_BYTES);
	}
	pfree(sorted);
	return state;
refused:
	pfree(sorted);
	cluster_space_drop_finish(state);
	return NULL;
}

const char *
cluster_space_drop_wal(const ClusterSpaceDropState *state, uint32 *len)
{
	*len = state == NULL ? 0 : state->wal_len;
	return state == NULL ? NULL : state->wal;
}

void
cluster_space_drop_mark_dirty(ClusterSpaceDropState *state)
{
	Assert(state != NULL && CritSectionCount > 0);
	Assert(MyProc->delayChkptFlags & DELAY_CHKPT_START);
	for (int i = 0; i < state->count; i++)
		for (int j = 0; j < 2; j++)
			MarkBufferDirty(state->entries[i]->buffers[j]);
}

void
cluster_space_drop_publish(ClusterSpaceDropState *state, XLogRecPtr commit_lsn)
{
	Assert(state != NULL && CritSectionCount > 0 && !state->published);
	Assert(MyProc->delayChkptFlags & DELAY_CHKPT_START);
	Assert(!XLogRecPtrIsInvalid(commit_lsn) && !XLogNeedsFlush(commit_lsn));
	for (int i = 0; i < state->count; i++) {
		space_structure_publish(state->entries[i], commit_lsn);
	}
	state->published = true;
}

void
cluster_space_drop_finish(ClusterSpaceDropState *state)
{
	Assert(state != NULL && CritSectionCount == 0);
	if (state->published && !RecoveryInProgress()) {
		/* COMMIT is already durable. Preserve the original locked SPACE
		 * owner until every tombstone has an exact durable observation;
		 * pending-delete unlink must not run after an unconfirmed result.
		 * An unpublished preparation/abort must never write the tombstone.
		 * Replay retains its separate recovery durability owner; it does
		 * not create this live origin's structural completion receipt. */
		PG_TRY();
		{
			for (int i = 0; i < state->count; i++) {
				ClusterSpaceTruncateState *entry = state->entries[i];
				ClusterSpaceStructureChange change;
				SMgrRelation smgr;

				if (!cluster_space_structure_wal_decode(entry->wal, sizeof(entry->wal), &change)
					|| change.identity.action != CLUSTER_SPACE_WAL_TOMBSTONE)
					elog(PANIC, "SPACE deletion lost its original committed record");
				for (int j = 0; j < 2; j++)
					FlushOneBuffer(entry->buffers[j]);
				smgr = smgropen(change.identity.result.key.locator, InvalidBackendId);
				smgrimmedsync(smgr, SPACE_FORKNUM);
				for (int j = 0; j < 2; j++)
					if (!space_structure_readback(smgr, entry->buffers[j], j))
						ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
										errmsg("deleted SPACE page does not match its durable write"),
										errdetail("Relation %u/%u/%u, SPACE block %d.",
												  change.identity.result.key.locator.spcOid,
												  change.identity.result.key.locator.dbOid,
												  change.identity.result.key.locator.relNumber, j)));
				space_structure_observe(entry);
			}
		}
		PG_CATCH();
		{
			/* ERROR cleanup would pretend this committed transaction aborted
			 * and expose its cached tombstone without the durable anchor. */
			ereport(PANIC, (errcode(ERRCODE_IO_ERROR),
							errmsg("could not durably publish committed relation deletion"),
							errdetail("The original SPACE owners remain held before file removal."),
							errhint("Restore storage availability and restart the instance to recover "
									"the committed deletion.")));
		}
		PG_END_TRY();
	}
	for (int i = state->count - 1; i >= 0; i--)
		space_truncate_release(state->entries[i]);
	if (state->published && state->count != 0 && space_identity_cache != NULL)
		space_identity_invalidate((Datum)0, InvalidOid);
	if (state->wal != NULL)
		pfree(state->wal);
	pfree(state);
}

static ClusterSpaceTruncateState *
space_drop_prepare_replay(const ClusterSpaceStructureChange *change,
						  const ClusterSpaceIdentityKey *expected)
{
	ClusterSpaceTruncateState *entry = palloc0(sizeof(*entry));
	ClusterSpaceRecoveryImage prepared;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterSpaceRecoveryInput input = {wal, sizeof(wal)};
	uint32 order;

	for (int i = 0; i < 2; i++) {
		entry->buffers[i] = ReadBufferWithoutRelcache(expected->locator, SPACE_FORKNUM, i,
			RBM_NORMAL, NULL, true);
		LockBuffer(entry->buffers[i], BUFFER_LOCK_EXCLUSIVE);
		if (BufferIsLocal(entry->buffers[i]) || BufferGetBlockNumber(entry->buffers[i]) != i
			|| !cluster_bufmgr_pcm_x_content_holder_write_permitted(
				GetBufferDescriptor(entry->buffers[i] - 1)))
			goto refused;
		memcpy(entry->result[i].data, BufferGetPage(entry->buffers[i]), BLCKSZ);
	}
	if (!cluster_space_structure_wal_encode(change, wal, sizeof(wal))
		|| !cluster_space_recovery_prepare(&input, 1, expected,
			entry->result[0].data, entry->result[1].data, &order, &prepared))
		goto refused;
	memcpy(entry->result, prepared.pages, sizeof(entry->result));
	entry->token = change->identity.result_token;
	return entry;
refused:
	space_truncate_release(entry);
	return NULL;
}

bool
cluster_space_drop_replay_commit(XLogReaderState *record, TransactionId xid)
{
	ClusterSpaceDropState *state;
	RelFileLocator *sorted;
	xl_xact_parsed_commit parsed;
	int count;
	uint32 consumed = 0;
	bool cold = cluster_shared_config && cluster_cold_replay_window_active_v1();

	if (record == NULL || record->record == NULL || !RecoveryInProgress()
		|| !TransactionIdIsNormal(xid) || XLogRecGetRmid(record) != RM_XACT_ID
		|| (XLogRecGetInfo(record) & XLOG_XACT_OPMASK) != XLOG_XACT_COMMIT
		|| XLogRecGetXid(record) != xid || XLogRecHasAnyBlockRefs(record)
		|| XLogRecPtrIsInvalid(record->ReadRecPtr) || record->EndRecPtr <= record->ReadRecPtr
		|| !ParseCommitRecord(XLogRecGetInfo(record), (xl_xact_commit *)XLogRecGetData(record),
			XLogRecGetDataLen(record), &parsed))
		return false;
	count = parsed.nrels;
	if (count == 0)
		return parsed.nspace_drops == 0;
	if (cluster_recmerge_apply_foreign || (Size)count > (MaxAllocSize - sizeof(*state))
		/ (sizeof(*sorted) + sizeof(*state->entries)))
		return false;
	state = palloc0(sizeof(*state) + (Size)count * sizeof(*state->entries));
	sorted = palloc((Size)count * sizeof(*sorted));
	memcpy(sorted, parsed.xlocators, (Size)count * sizeof(*sorted));
	qsort(sorted, count, sizeof(*sorted), space_locator_compare);
	for (int i = 0; i < count; i++) {
		ClusterSpaceIdentityKey expected;
		ClusterSpaceStructureChange change;
		ClusterSpaceTruncateState *entry;
		SMgrRelation smgr;
		uint16 local_thread;

		if ((i != 0 && RelFileLocatorEquals(sorted[i], sorted[i - 1]))
			|| cluster_smgr_which_for(sorted[i], InvalidBackendId) != 1)
			continue;
		if (!space_namespace(sorted[i], true, &expected, &local_thread) || local_thread == 0
			|| local_thread != record->cluster_expected_thread_id
			|| record->system_identifier != expected.system_identifier)
			goto refused;
		if (consumed >= parsed.nspace_drops
			|| !cluster_space_structure_wal_decode(parsed.space_drops
				+ (Size)consumed * CLUSTER_SPACE_STRUCTURE_WAL_BYTES,
				CLUSTER_SPACE_STRUCTURE_WAL_BYTES, &change)
			|| change.identity.action != CLUSTER_SPACE_WAL_TOMBSTONE
			|| !space_identity_key_matches(&expected, &change.identity.result.key))
			goto refused;
		consumed++;
		if (cold) {
			ClusterSpaceRecoveryInput input
				= { parsed.space_drops + (Size)(consumed - 1) * CLUSTER_SPACE_STRUCTURE_WAL_BYTES,
					CLUSTER_SPACE_STRUCTURE_WAL_BYTES };
			ClusterSpaceColdSourceV1 source = { local_thread, record->EndRecPtr };

			if (!cluster_space_recovery_cold_drop_already_v1(&expected, &input, &source))
				goto refused;
			continue;
		}
		smgr = smgropen(expected.locator, InvalidBackendId);
		if (!smgrexists(smgr, SPACE_FORKNUM)) {
			/* Native unlink removes SPACE last. A missing identity alone
			 * cannot authorize deletion of some other extant generation. */
			for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++)
				if (smgrexists(smgr, fork))
					goto refused;
			continue;
		}
		if (smgrnblocks(smgr, SPACE_FORKNUM) != 2)
			goto refused;
		entry = space_drop_prepare_replay(&change, &expected);
		if (entry == NULL)
			goto refused;
		state->entries[state->count++] = entry;
	}
	if (consumed != parsed.nspace_drops)
		goto refused;
	pfree(sorted);
	if (cold) {
		/* The original COMMIT owner deletes only after every target has
		 * qualified. Its source is already pinned; no local WAL flush or
		 * SPACE rewrite is allowed for a cold ALREADY result. */
		cluster_space_drop_finish(state);
		return true;
	}
	/* All targets were checked privately. Bind minRecoveryPoint to this
	 * same local stream's COMMIT before any tombstone becomes dirty. */
	XLogFlush(record->EndRecPtr);
	START_CRIT_SECTION();
	for (int i = 0; i < state->count; i++) {
		MarkBufferDirty(state->entries[i]->buffers[0]);
		MarkBufferDirty(state->entries[i]->buffers[1]);
		space_structure_publish(state->entries[i], record->EndRecPtr);
	}
	state->published = true;
	END_CRIT_SECTION();
	cluster_space_drop_finish(state);
	return true;
refused:
	pfree(sorted);
	cluster_space_drop_finish(state);
	return false;
}

static bool
space_reservation_redo(XLogReaderState *record)
{
	ClusterSpaceReservationChange change;
	ClusterSpaceIdentityKey expected;
	ClusterSpaceIdentity checked;
	uint8 identity[CLUSTER_SPACE_IDENTITY_BYTES];
	ClusterSpaceRecoveryImage prepared;
	ClusterSpaceRecoveryInput input = {XLogRecGetData(record), XLogRecGetDataLen(record)};
	uint32 order;
	SMgrRelation rel;
	Buffer buffer, identity_buffer;
	uint16 local_thread;
	bool success = false;

	if (!RecoveryInProgress() || cluster_recmerge_apply_foreign
		|| XLogRecHasAnyBlockRefs(record) || XLogRecPtrIsInvalid(record->EndRecPtr)
		|| !cluster_space_reservation_wal_decode(XLogRecGetData(record), XLogRecGetDataLen(record), &change)
		|| change.action != CLUSTER_SPACE_RESERVATION_ADVANCE
		|| !space_namespace(change.result.identity.key.locator, true, &expected, &local_thread)
		|| local_thread == 0 || record->cluster_expected_thread_id != local_thread
		|| !cluster_space_identity_encode(&change.result.identity, identity, sizeof(identity))
		|| !cluster_space_identity_decode(identity, sizeof(identity), &expected, &checked))
		return false;
	rel = smgropen(expected.locator, InvalidBackendId);
	if (!smgrexists(rel, SPACE_FORKNUM) || smgrnblocks(rel, SPACE_FORKNUM) != 2)
		return false;
	/* Keep the independently versioned identity stable while checking and
	 * installing its reservation. Recovery follows the same block lock order
	 * as structural owners, but never mutates block zero for ADVANCE. */
	identity_buffer = ReadBufferWithoutRelcache(expected.locator, SPACE_FORKNUM, 0,
		RBM_NORMAL, NULL, true);
	LockBuffer(identity_buffer, BUFFER_LOCK_SHARE);
	buffer = ReadBufferWithoutRelcache(expected.locator, SPACE_FORKNUM,
		CLUSTER_SPACE_RESERVATION_BLOCK, RBM_NORMAL, NULL, true);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	if (BufferIsLocal(identity_buffer) || BufferGetBlockNumber(identity_buffer) != 0
		|| BufferIsLocal(buffer) || BufferGetBlockNumber(buffer) != CLUSTER_SPACE_RESERVATION_BLOCK
		|| !cluster_bufmgr_pcm_x_content_holder_write_permitted(GetBufferDescriptor(buffer - 1)))
		goto done;
	if (!cluster_space_recovery_prepare(&input, 1, &expected, BufferGetPage(identity_buffer),
		BufferGetPage(buffer), &order, &prepared) || (prepared.apply_mask & 1) != 0)
		goto done;
	if (prepared.apply_mask & 2) {
		START_CRIT_SECTION();
		memcpy(BufferGetPage(buffer), prepared.pages[1].data, BLCKSZ);
		MarkBufferDirty(buffer);
		space_set_lsn(BufferGetPage(buffer), record->EndRecPtr, change.result_token);
		END_CRIT_SECTION();
	}
	success = true;
done:
	UnlockReleaseBuffer(buffer);
	UnlockReleaseBuffer(identity_buffer);
	return success;
}

bool
cluster_space_relation_redo(XLogReaderState *record)
{
	ClusterSpaceStructureChange change;
	ClusterSpaceWalChange *identity = &change.identity;
	ClusterSpaceIdentityKey expected;
	ClusterSpaceRecoveryInput input;
	ClusterSpaceRecoveryImage prepared;
	uint32 order;
	PGAlignedBlock result[2] = {{0}};
	PGAlignedBlock zero = {0};
	uint8 check[CLUSTER_SPACE_IDENTITY_BYTES];
	SMgrRelation rel;
	BlockNumber blocks;
	Buffer buffers[2] = {InvalidBuffer, InvalidBuffer};
	uint16 local_thread;
	uint8 apply_mask;
	bool exists, success = false;

	if (record != NULL && record->record != NULL && XLogRecGetRmid(record) == RM_SMGR_ID
		&& (XLogRecGetInfo(record) & ~XLR_INFO_MASK) == XLOG_SMGR_SPACE_RESERVATION)
		return space_reservation_redo(record);
	if (record == NULL || record->record == NULL || !RecoveryInProgress()
		|| XLogRecGetRmid(record) != RM_SMGR_ID
		|| (XLogRecGetInfo(record) & ~XLR_INFO_MASK) != XLOG_SMGR_SPACE_IDENTITY
		|| XLogRecHasAnyBlockRefs(record) || XLogRecPtrIsInvalid(record->EndRecPtr)
		|| !cluster_space_structure_wal_decode(XLogRecGetData(record), XLogRecGetDataLen(record), &change)
		|| !space_namespace(identity->result.key.locator, true, &expected, &local_thread))
		return false;
	/* Independently selected namespace, not the untrusted WAL payload. */
	if (!cluster_space_identity_encode(&identity->result, check, sizeof(check))
		|| !cluster_space_identity_decode(check, sizeof(check), &expected, &identity->result))
		return false;
	/* A standalone structural record has no transaction commit decision. */
	if (identity->action == CLUSTER_SPACE_WAL_TOMBSTONE)
		return false;
	/* Foreign structural replay requires its selected per-origin durability
	 * input, not the recovering thread's minRecoveryPoint. Keep it closed
	 * until that owner is wired; never flush a foreign numeric LSN locally. */
	if (identity->action != CLUSTER_SPACE_WAL_CREATE
		&& (identity->action != CLUSTER_SPACE_WAL_TRUNCATE || cluster_recmerge_apply_foreign
			|| local_thread == 0 || record->cluster_expected_thread_id != local_thread))
		return false;
	rel = smgropen(expected.locator, InvalidBackendId);
	exists = smgrexists(rel, SPACE_FORKNUM);
	if (!exists && identity->action != CLUSTER_SPACE_WAL_CREATE)
		return false;
	blocks = exists ? smgrnblocks(rel, SPACE_FORKNUM) : 0;
	if (blocks > 2 || (identity->action != CLUSTER_SPACE_WAL_CREATE && blocks != 2))
		return false;
	/* Read every existing component before any create/extend/truncate.
	 * Missing CREATE components are private zero predecessors until the
	 * complete structural record passes. Lock pages in block order. */
	for (BlockNumber i = 0; i < blocks; i++) {
		buffers[i] = ReadBufferWithoutRelcache(expected.locator, SPACE_FORKNUM, i,
											 RBM_NORMAL, NULL, true);
		LockBuffer(buffers[i], BUFFER_LOCK_EXCLUSIVE);
		if (BufferGetBlockNumber(buffers[i]) != i)
			goto done;
		memcpy(result[i].data, BufferGetPage(buffers[i]), BLCKSZ);
	}
	input = (ClusterSpaceRecoveryInput){XLogRecGetData(record), XLogRecGetDataLen(record)};
	if (!cluster_space_recovery_prepare(&input, 1, &expected, result[0].data, result[1].data,
		&order, &prepared))
		goto done;
	apply_mask = prepared.apply_mask;
	memcpy(result, prepared.pages, sizeof(result));
	if (!exists)
		smgrcreate(rel, SPACE_FORKNUM, true);
	for (BlockNumber i = blocks; i < 2; i++) {
		buffers[i] = ReadBufferWithoutRelcache(expected.locator, SPACE_FORKNUM, P_NEW,
											 RBM_ZERO_AND_LOCK, NULL, true);
		if (BufferGetBlockNumber(buffers[i]) != i
			|| memcmp(BufferGetPage(buffers[i]), zero.data, BLCKSZ) != 0)
			goto done;
	}
	if (identity->action == CLUSTER_SPACE_WAL_TRUNCATE && (apply_mask & 1)) {
		xl_smgr_truncate truncate;

		truncate.rlocator = expected.locator;
		truncate.blkno = identity->nblocks;
		truncate.flags = SMGR_TRUNCATE_ALL;
		/* Native preparation allocates memory; only its physical shrink is
		 * critical. Keep SPACE locked and finish the shrink before publishing
		 * identity. Result identity proves this shrink was already durable;
		 * repeating it could remove a peer's later checkpointed extension. */
		smgr_redo_truncate(record->EndRecPtr, &truncate);
	}
	if (apply_mask != 0) {
		START_CRIT_SECTION();
		for (int i = 0; i < 2; i++) {
			if (!(apply_mask & (1 << i)))
				continue;
			memcpy(BufferGetPage(buffers[i]), result[i].data, BLCKSZ);
			MarkBufferDirty(buffers[i]);
			space_set_lsn(BufferGetPage(buffers[i]), record->EndRecPtr, identity->result_token);
		}
		END_CRIT_SECTION();
	}
	success = true;
done:
	for (int i = 1; i >= 0; i--)
		if (BufferIsValid(buffers[i]))
			UnlockReleaseBuffer(buffers[i]);
	return success;
}
