/*-------------------------------------------------------------------------
 *
 * cluster_page_install.c
 *    STOP-06 storage install, durability and canonical post-read.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_install.h"
#include "storage/bufpage.h"

#ifdef USE_CLUSTER_UNIT
#define page_install_alloc0(size_) calloc(1, (size_))
#define page_install_free(pointer_) free((pointer_))
#else
#define page_install_alloc0(size_) palloc0(size_)
#define page_install_free(pointer_) pfree(pointer_)
#endif

#ifndef USE_CLUSTER_UNIT
#include "access/xlog.h"
#include "storage/checksum.h"
#include "storage/smgr.h"
#ifdef USE_PGRAC_CLUSTER
#include "access/xlogrecovery.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_stamp.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "utils/memutils.h"
#endif
#endif

static bool
page_all_zero(const char page[BLCKSZ])
{
	const uint8 *bytes = (const uint8 *)page;
	uint8 value = 0;
	int i;

	for (i = 0; i < BLCKSZ; i++)
		value |= bytes[i];
	return value == 0;
}

static bool
page_layout_valid(const char page[BLCKSZ])
{
	const PageHeader header = (const PageHeader)page;

	return !PageIsNew((Page)page) && PageGetPageSize((Page)page) == BLCKSZ
		   && PageGetPageLayoutVersion((Page)page) == PG_PAGE_LAYOUT_VERSION
		   && header->pd_lower >= SizeOfPageHeaderData && header->pd_lower <= header->pd_upper
		   && header->pd_upper <= header->pd_special && header->pd_special <= BLCKSZ;
}

static bool
version_zero(const RfPageVersionV1 *version)
{
	static const RfPageVersionV1 zero;

	return memcmp(version, &zero, sizeof(*version)) == 0;
}

static bool
component_versions_valid(const RfPageStorageInstallComponentV1 *component)
{
	static const uint8 zero_reserved[7];
	bool ordinary_fork;

	ordinary_fork = component->page_identity.forknum == MAIN_FORKNUM
					|| component->page_identity.forknum == VISIBILITYMAP_FORKNUM
					|| component->page_identity.forknum == INIT_FORKNUM;
	if (!rf_page_identity_valid_v1(&component->page_identity)
		|| !rf_page_version_present_v1(&component->expected_result)
		|| component->canonical_page == NULL || !ordinary_fork
		|| memcmp(component->reserved_zero, zero_reserved, sizeof(zero_reserved)) != 0)
		return false;
	switch (component->before_kind) {
	case RF_PAGE_STATE_PRESENT:
		return rf_page_version_present_v1(&component->expected_before)
			   && memcmp(component->expected_before.segment_incarnation,
						 component->expected_result.segment_incarnation, 16)
					  == 0;
	case RF_PAGE_STATE_UNFORMATTED:
		return component->expected_before.mutation_token == 0
			   && memcmp(component->expected_before.segment_incarnation,
						 component->expected_result.segment_incarnation, 16)
					  == 0;
	case RF_PAGE_STATE_ABSENT:
		return version_zero(&component->expected_before);
	default:
		return false;
	}
}

static bool
page_checksum_valid(const RfPageInstallStorageOpsV1 *storage, const RfPageIdentityV1 *identity,
					const char page[BLCKSZ])
{
	if (!storage->checksums_enabled)
		return ((const PageHeader)page)->pd_checksum == 0;
	return ((const PageHeader)page)->pd_checksum
		   == storage->checksum(storage->arg, page, identity->blockno);
}

static bool
canonicalize_page(const RfPageInstallStorageOpsV1 *storage,
				  const RfPageStorageInstallComponentV1 *component, char page[BLCKSZ])
{
	PageHeader header = (PageHeader)page;

	memcpy(page, component->canonical_page, BLCKSZ);
	if (page_all_zero(page) || !page_layout_valid(page)
		|| header->pd_block_scn != component->expected_result.mutation_token)
		return false;
	header->pd_checksum = 0;
	if (storage->checksums_enabled)
		header->pd_checksum
			= storage->checksum(storage->arg, page, component->page_identity.blockno);
	return page_checksum_valid(storage, &component->page_identity, page);
}

static RfPageProofDetailV1
release_after_failure(const RfPageInstallAuthorityOpsV1 *authority, RfPageProofDetailV1 detail)
{
	if (!authority->release(authority->arg))
		return RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION;
	return detail;
}

RfPageProofDetailV1
rf_page_storage_install_execute_v1(const RfPageStorageInstallRequestV1 *request,
								   RfPageStorageInstallProofV1 *proof)
{
	const RfPageInstallStorageOpsV1 *storage;
	const RfPageInstallAuthorityOpsV1 *authority;
	RfPageStorageInstallProofV1 completed;
	bool *write_required;
	bool *extend_required;
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;
	uint32 i;

	if (request == NULL || proof == NULL || request->components == NULL || request->storage == NULL
		|| request->authority == NULL || request->prepared_pages == NULL
		|| request->io_pages == NULL || request->component_count == 0
		|| request->component_count > RF_PAGE_STABLE_MAX_EDGES
		|| request->prepared_capacity < (Size)request->component_count * BLCKSZ
		|| request->io_capacity < (Size)request->component_count * BLCKSZ)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	storage = request->storage;
	authority = request->authority;
	if (storage->read == NULL || storage->write == NULL || storage->sync == NULL
		|| storage->checksum == NULL || authority->validate_identity == NULL
		|| authority->promote == NULL || authority->publish == NULL || authority->release == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (!request->global_preflight_ok)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	write_required
		= (bool *)page_install_alloc0((Size)request->component_count * sizeof(*write_required));
	extend_required
		= (bool *)page_install_alloc0((Size)request->component_count * sizeof(*extend_required));
	if (write_required == NULL || extend_required == NULL) {
		if (extend_required != NULL)
			page_install_free(extend_required);
		if (write_required != NULL)
			page_install_free(write_required);
		return RF_PAGE_PROOF_DETAIL_OOM;
	}

	for (i = 0; i < request->component_count; i++) {
		const RfPageStorageInstallComponentV1 *component = &request->components[i];
		char *prepared = request->prepared_pages + (Size)i * BLCKSZ;

		if (!component_versions_valid(component)
			|| !authority->validate_identity(authority->arg, &component->page_identity,
											 component->expected_result.segment_incarnation)) {
			detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
			goto done;
		}
		if (!canonicalize_page(storage, component, prepared)) {
			detail = RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;
			goto done;
		}
	}

	/*
	 * Target state is meaningful only while PAGE authority protects the
	 * certified target set.  Keep all argument/image work before promotion,
	 * but do not touch storage until promotion succeeds.
	 */
	if (!authority->promote(authority->arg)) {
		detail = RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
		goto done;
	}

	for (i = 0; i < request->component_count; i++) {
		const RfPageStorageInstallComponentV1 *component = &request->components[i];
		const char *prepared = request->prepared_pages + (Size)i * BLCKSZ;
		char *target = request->io_pages + (Size)i * BLCKSZ;
		bool exists;
		bool torn;
		uint64 token;

		if (!storage->read(storage->arg, i, &component->page_identity, target, &exists)) {
			detail = release_after_failure(authority, RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
			goto done;
		}
		if (!exists) {
			if (component->before_kind != RF_PAGE_STATE_ABSENT) {
				detail = release_after_failure(authority, RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH);
				goto done;
			}
			write_required[i] = true;
			extend_required[i] = true;
			continue;
		}

		torn = page_all_zero(target) || !page_layout_valid(target)
			   || !page_checksum_valid(storage, &component->page_identity, target);
		if (torn) {
			write_required[i] = true;
			continue;
		}
		token = ((PageHeader)target)->pd_block_scn;
		if (token == component->expected_result.mutation_token) {
			if (memcmp(target, prepared, BLCKSZ) != 0) {
				detail
					= release_after_failure(authority, RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED);
				goto done;
			}
			continue;
		}
		if (component->before_kind == RF_PAGE_STATE_PRESENT
			&& token == component->expected_before.mutation_token) {
			write_required[i] = true;
			continue;
		}
		if (component->before_kind == RF_PAGE_STATE_UNFORMATTED && token == 0) {
			write_required[i] = true;
			continue;
		}
		if (authority->covers_version != NULL && token != 0) {
			RfPageVersionV1 observed = component->expected_result;

			observed.mutation_token = token;
			if (authority->covers_version(authority->arg, &component->page_identity, &observed,
										  &component->expected_result)) {
				write_required[i] = true;
				continue;
			}
		}
		detail = release_after_failure(authority, RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH);
		goto done;
	}

	memset(&completed, 0, sizeof(completed));
	completed.component_count = request->component_count;

	for (i = 0; i < request->component_count; i++) {
		if (!write_required[i]) {
			completed.result_skip_count++;
			continue;
		}
		if (!storage->write(storage->arg, i, &request->components[i].page_identity,
							request->prepared_pages + (Size)i * BLCKSZ, extend_required[i])) {
			detail = release_after_failure(authority, RF_PAGE_PROOF_DETAIL_INTERNAL);
			goto done;
		}
		completed.write_count++;
	}
	for (i = 0; i < request->component_count; i++) {
		if (!storage->sync(storage->arg, i, &request->components[i].page_identity)) {
			detail = release_after_failure(authority, RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED);
			goto done;
		}
	}
	completed.durability_complete = true;

	for (i = 0; i < request->component_count; i++) {
		const RfPageStorageInstallComponentV1 *component = &request->components[i];
		char *postread = request->io_pages + (Size)i * BLCKSZ;
		const char *prepared = request->prepared_pages + (Size)i * BLCKSZ;
		bool exists;

		if (!storage->read(storage->arg, i, &component->page_identity, postread, &exists) || !exists
			|| page_all_zero(postread) || !page_layout_valid(postread)
			|| ((PageHeader)postread)->pd_block_scn != component->expected_result.mutation_token
			|| !page_checksum_valid(storage, &component->page_identity, postread)
			|| !authority->validate_identity(authority->arg, &component->page_identity,
											 component->expected_result.segment_incarnation)
			|| memcmp(postread, prepared, BLCKSZ) != 0) {
			detail = release_after_failure(authority, RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED);
			goto done;
		}
	}
	completed.postread_complete = true;
	if (!authority->publish(authority->arg)) {
		detail = release_after_failure(authority, RF_PAGE_PROOF_DETAIL_INTERNAL);
		goto done;
	}
	completed.proof_published = true;
	if (!authority->release(authority->arg)) {
		detail = RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION;
		goto done;
	}
	completed.authority_released = true;
	*proof = completed;
	detail = RF_PAGE_PROOF_DETAIL_OK;

done:
	page_install_free(extend_required);
	page_install_free(write_required);
	return detail;
}

#ifndef USE_CLUSTER_UNIT

struct RfPageSmgrPreopenV1 {
	const RfPageStorageInstallRequestV1 *request;
	SMgrRelation *relations;
	bool *opened;
};

typedef struct RfPageSmgrAuthorityContextV1 {
	const RfPageInstallAuthorityOpsV1 *delegate;
	/* Read after the PG_TRY longjmp when storage or post-read throws. */
	volatile bool promoted;
} RfPageSmgrAuthorityContextV1;

static SMgrRelation
page_smgr_relation(RfPageSmgrPreopenV1 *context, uint32 index)
{
	if (context == NULL || index >= context->request->component_count || !context->opened[index])
		return NULL;
	return context->relations[index];
}

static bool
page_smgr_read(void *arg, uint32 index, const RfPageIdentityV1 *identity, char page[BLCKSZ],
			   bool *exists)
{
	RfPageSmgrPreopenV1 *context = (RfPageSmgrPreopenV1 *)arg;
	SMgrRelation relation = page_smgr_relation(context, index);
	ForkNumber forknum = (ForkNumber)identity->forknum;

	if (relation == NULL)
		return false;
	*exists = smgrexists(relation, forknum) && identity->blockno < smgrnblocks(relation, forknum);
	if (*exists)
		smgrread(relation, forknum, identity->blockno, page);
	else
		memset(page, 0, BLCKSZ);
	return true;
}

static bool
page_smgr_write(void *arg, uint32 index, const RfPageIdentityV1 *identity, const char page[BLCKSZ],
				bool extend)
{
	RfPageSmgrPreopenV1 *context = (RfPageSmgrPreopenV1 *)arg;
	SMgrRelation relation = page_smgr_relation(context, index);
	ForkNumber forknum = (ForkNumber)identity->forknum;
	BlockNumber nblocks;

	if (relation == NULL)
		return false;
	if (!smgrexists(relation, forknum))
		return false;
	nblocks = smgrnblocks(relation, forknum);
	if (extend) {
		if (identity->blockno != nblocks)
			return false;
		smgrextend(relation, forknum, identity->blockno, page, false);
	} else {
		if (identity->blockno >= nblocks)
			return false;
		smgrwrite(relation, forknum, identity->blockno, page, false);
	}
	return true;
}

static bool
page_smgr_sync(void *arg, uint32 index, const RfPageIdentityV1 *identity)
{
	RfPageSmgrPreopenV1 *context = (RfPageSmgrPreopenV1 *)arg;
	SMgrRelation relation;
	uint32 i;

	for (i = 0; i < index; i++) {
		const RfPageIdentityV1 *prior = &context->request->components[i].page_identity;

		if (RelFileLocatorEquals(prior->locator, identity->locator)
			&& prior->forknum == identity->forknum)
			return true;
	}
	relation = page_smgr_relation(context, index);
	if (relation == NULL)
		return false;
	smgrimmedsync(relation, (ForkNumber)identity->forknum);
	return true;
}

static uint16
page_smgr_checksum(void *arg, const char page[BLCKSZ], BlockNumber blockno)
{
	return pg_checksum_page((char *)page, blockno);
}

static bool
page_smgr_authority_validate(void *arg, const RfPageIdentityV1 *identity,
							 const uint8 incarnation[16])
{
	RfPageSmgrAuthorityContextV1 *context = (RfPageSmgrAuthorityContextV1 *)arg;

	return context->delegate->validate_identity(context->delegate->arg, identity, incarnation);
}

static bool
page_smgr_authority_promote(void *arg)
{
	RfPageSmgrAuthorityContextV1 *context = (RfPageSmgrAuthorityContextV1 *)arg;

	if (!context->delegate->promote(context->delegate->arg))
		return false;
	context->promoted = true;
	return true;
}

static bool
page_smgr_authority_publish(void *arg)
{
	RfPageSmgrAuthorityContextV1 *context = (RfPageSmgrAuthorityContextV1 *)arg;

	return context->delegate->publish(context->delegate->arg);
}

static bool
page_smgr_authority_release(void *arg)
{
	RfPageSmgrAuthorityContextV1 *context = (RfPageSmgrAuthorityContextV1 *)arg;

	if (!context->delegate->release(context->delegate->arg))
		return false;
	context->promoted = false;
	return true;
}

static bool
page_smgr_authority_covers_version(void *arg, const RfPageIdentityV1 *identity,
								   const RfPageVersionV1 *version, const RfPageVersionV1 *result)
{
	RfPageSmgrAuthorityContextV1 *context = (RfPageSmgrAuthorityContextV1 *)arg;

	return context->promoted && context->delegate->covers_version != NULL
		   && context->delegate->covers_version(context->delegate->arg, identity, version, result);
}

RfPageProofDetailV1
rf_page_storage_smgr_preopen_v1(const RfPageStorageInstallRequestV1 *request,
								RfPageSmgrPreopenV1 **out_preopen)
{
	RfPageSmgrPreopenV1 *context;
	uint32 i;

	if (out_preopen == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	*out_preopen = NULL;
	if (request == NULL || request->components == NULL || request->component_count == 0
		|| request->component_count > RF_PAGE_STABLE_MAX_EDGES || request->storage != NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	context = palloc0(sizeof(*context));
	context->request = request;
	context->relations = palloc0((Size)request->component_count * sizeof(*context->relations));
	context->opened = palloc0((Size)request->component_count * sizeof(*context->opened));
	for (i = 0; i < request->component_count; i++) {
		context->relations[i]
			= smgropen(request->components[i].page_identity.locator, InvalidBackendId);
		context->opened[i] = true;
	}
	*out_preopen = context;
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
rf_page_storage_smgr_preopen_destroy_v1(RfPageSmgrPreopenV1 **preopen)
{
	if (preopen == NULL || *preopen == NULL)
		return;
	pfree((*preopen)->opened);
	pfree((*preopen)->relations);
	pfree(*preopen);
	*preopen = NULL;
}

RfPageProofDetailV1
rf_page_storage_install_smgr_preopened_v1(const RfPageStorageInstallRequestV1 *request,
										  RfPageSmgrPreopenV1 *preopen,
										  RfPageStorageInstallProofV1 *proof)
{
	RfPageSmgrAuthorityContextV1 authority_context;
	RfPageInstallStorageOpsV1 storage;
	RfPageInstallAuthorityOpsV1 authority;
	RfPageStorageInstallRequestV1 smgr_request;
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_INTERNAL;

	if (request == NULL || request->components == NULL || request->component_count == 0
		|| request->component_count > RF_PAGE_STABLE_MAX_EDGES || preopen == NULL
		|| preopen->request != request || request->storage != NULL || request->authority == NULL
		|| request->authority->validate_identity == NULL || request->authority->promote == NULL
		|| request->authority->publish == NULL || request->authority->release == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	memset(&authority_context, 0, sizeof(authority_context));
	authority_context.delegate = request->authority;
	memset(&storage, 0, sizeof(storage));
	storage.arg = preopen;
	storage.checksums_enabled = DataChecksumsEnabled();
	storage.read = page_smgr_read;
	storage.write = page_smgr_write;
	storage.sync = page_smgr_sync;
	storage.checksum = page_smgr_checksum;
	memset(&authority, 0, sizeof(authority));
	authority.arg = &authority_context;
	authority.validate_identity = page_smgr_authority_validate;
	authority.promote = page_smgr_authority_promote;
	authority.publish = page_smgr_authority_publish;
	authority.release = page_smgr_authority_release;
	if (request->authority->covers_version != NULL)
		authority.covers_version = page_smgr_authority_covers_version;
	smgr_request = *request;
	smgr_request.storage = &storage;
	smgr_request.authority = &authority;
	PG_TRY();
	{
		detail = rf_page_storage_install_execute_v1(&smgr_request, proof);
	}
	PG_CATCH();
	{
		if (authority_context.promoted)
			(void)request->authority->release(request->authority->arg);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return detail;
}

RfPageProofDetailV1
rf_page_storage_install_smgr_v1(const RfPageStorageInstallRequestV1 *request,
								RfPageStorageInstallProofV1 *proof)
{
	RfPageSmgrPreopenV1 *preopen = NULL;
	RfPageProofDetailV1 detail;

	detail = rf_page_storage_smgr_preopen_v1(request, &preopen);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	PG_TRY();
	{
		detail = rf_page_storage_install_smgr_preopened_v1(request, preopen, proof);
	}
	PG_CATCH();
	{
		rf_page_storage_smgr_preopen_destroy_v1(&preopen);
		PG_RE_THROW();
	}
	PG_END_TRY();
	rf_page_storage_smgr_preopen_destroy_v1(&preopen);
	return detail;
}

#endif /* !USE_CLUSTER_UNIT */

#if defined(USE_PGRAC_CLUSTER) && !defined(USE_CLUSTER_UNIT)
static bool
page_recovery_fpi_current(const ClusterRecoveryFpiInsertV1 *inserted,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
	const RfPageIdentityV1 *identity)
{
	ClusterRecoveryStampFpiV1 envelope;
	RfPageRecoveryReferenceV1 reference;
	TimeLineID timeline;

	if (!cluster_enabled || !cluster_shared_config || !IsUnderPostmaster
		|| MyBackendType != B_STARTUP || MyProcPid <= 0 || CritSectionCount != 0
		|| ShutdownRequestPending || RecoveryInProgress() || inserted->guard == NULL
		|| !inserted->wal_flushed || serial == NULL || pin == NULL || !serial->held
		|| serial->release_uncertain || serial->mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED
		|| inserted->window.owner.executor_pid != MyProcPid
		|| memcmp(&serial->duty, &inserted->window.owner.target.identity, sizeof(serial->duty)) != 0
		|| inserted->window.payload_length != sizeof(envelope)
		|| !cluster_recovery_stamp_fpi_inserted_current_v1(&inserted->window,
			inserted->record_start, inserted->record_end)
		|| GetFlushRecPtr(&timeline) < inserted->record_end
		|| timeline != inserted->window.owner.writer.ref.timeline
		|| cluster_wal_writer_check(&inserted->window.owner.writer)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	memcpy(&envelope, inserted->window.payload, sizeof(envelope));
	return rf_page_authority_recovery_reference_v1(inserted->guard, serial, pin, identity,
			&envelope.image.version.before, &reference)
		&& memcmp(&reference, &envelope.protection, sizeof(reference)) == 0;
}

/* The cleanup publisher already holds PAGE and owns an actual flushed record.
 * Reuse native typed storage primitives, retaining that same protection until
 * the caller completes the original first/pin/companion responsibilities. */
static bool
page_recovery_fpi_install(ClusterRecoveryFpiInsertV1 *inserted,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin)
{
	ClusterRecoveryStampFpiV1 envelope;
	RfPageStorageInstallComponentV1 component = {0};
	RfPageStorageInstallRequestV1 request = {0};
	RfPageInstallStorageOpsV1 storage = {0};
	RfPageSmgrPreopenV1 context = {0};
	ClusterWalWriterToken installed;
	uint8 operation_uuid[16];
	uint64 install_generation;
	SMgrRelation relation;
	PGAlignedBlock canonical, current;
	bool opened = true, exists;
	uint64 token;

	component.page_identity.system_identifier = inserted->image.identity.key.system_identifier;
	memcpy(component.page_identity.storage_uuid, inserted->image.identity.key.storage_uuid, 16);
	component.page_identity.locator = inserted->image.identity.key.locator;
	component.page_identity.forknum = inserted->image.fork_number;
	component.page_identity.blockno = inserted->image.block_number;
	if (!page_recovery_fpi_current(inserted, serial, pin, &component.page_identity))
		return false;
	if (!ClusterStartupInstalledRecoveryWriterV1(&installed, operation_uuid, &install_generation)
		|| install_generation == 0 || installed.epoch != inserted->window.owner.writer.epoch
		|| installed.startup_first_lsn != inserted->window.owner.writer.startup_first_lsn
		|| memcmp(&installed.ref, &inserted->window.owner.writer.ref, sizeof(installed.ref)) != 0)
		return false;
	memcpy(&envelope, inserted->window.payload, sizeof(envelope));
	if (!GenericXLogRecoveryImageMatchesDescriptor(&inserted->image, &envelope.image))
		return false;
	component.before_kind = RF_PAGE_STATE_PRESENT;
	component.expected_before = inserted->image.version.before;
	memcpy(component.expected_result.segment_incarnation,
		inserted->image.version.result_incarnation, 16);
	component.expected_result.mutation_token = inserted->image.result_token;
	/* Reuse current as private staging, then as target-read scratch. */
	current = inserted->image.image;
	PageSetLSN(current.data, inserted->record_end);
	component.canonical_page = current.data;
	storage.arg = &context;
	storage.checksums_enabled = DataChecksumsEnabled();
	storage.checksum = page_smgr_checksum;
	if (!component_versions_valid(&component)
		|| !canonicalize_page(&storage, &component, canonical.data)
		|| !page_recovery_fpi_current(inserted, serial, pin, &component.page_identity))
		return false;
	request.components = &component;
	request.component_count = 1;
	context.request = &request;
	context.relations = &relation;
	context.opened = &opened;
	relation = smgropen(component.page_identity.locator, InvalidBackendId);
	if (!page_recovery_fpi_current(inserted, serial, pin, &component.page_identity)
		|| !page_smgr_read(&context, 0, &component.page_identity, current.data, &exists)
		|| !exists || !page_layout_valid(current.data)
		|| !page_checksum_valid(&storage, &component.page_identity, current.data))
		return false;
	token = ((PageHeader)current.data)->pd_block_scn;
	if (token == component.expected_result.mutation_token) {
		if (memcmp(current.data, canonical.data, BLCKSZ) != 0)
			return false;
	} else if (token == component.expected_before.mutation_token) {
		if (!page_recovery_fpi_current(inserted, serial, pin, &component.page_identity)
			|| !page_smgr_write(&context, 0, &component.page_identity, canonical.data, false))
			return false;
	} else
		return false;
	if (!page_recovery_fpi_current(inserted, serial, pin, &component.page_identity)
		|| !page_smgr_sync(&context, 0, &component.page_identity)
		|| !page_recovery_fpi_current(inserted, serial, pin, &component.page_identity)
		|| !page_smgr_read(&context, 0, &component.page_identity, current.data, &exists)
		|| !exists || !page_layout_valid(current.data)
		|| !page_checksum_valid(&storage, &component.page_identity, current.data)
		|| memcmp(current.data, canonical.data, BLCKSZ) != 0
		|| !page_recovery_fpi_current(inserted, serial, pin, &component.page_identity))
		return false;
	return true;
}

bool
rf_page_recovery_fpi_install_smgr_v1(ClusterRecoveryFpiInsertV1 *inserted,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin)
{
	volatile bool completed = false;

	if (inserted == NULL)
		return false;
	inserted->target_synced = false;
	PG_TRY();
	{
		completed = page_recovery_fpi_install(inserted, serial, pin);
		if (!completed)
			(void)cluster_recovery_stamp_fail_v1(&inserted->window);
	}
	PG_CATCH();
	{
		(void)cluster_recovery_stamp_fail_v1(&inserted->window);
		PG_RE_THROW();
	}
	PG_END_TRY();
	inserted->target_synced = completed;
	return completed;
}

bool
rf_page_recovery_fpi_apply_smgr_v1(const GenericXLogRecoveryImageV1 *image,
	const ClusterRecoveryStampOwnerV1 *owner, RfPageAuthorityPreflightV1 *preflight,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
	ClusterRecoveryFpiInsertV1 *out)
{
	if (!GenericXLogRecoveryImageFinish(image, owner, preflight, serial, pin, out))
		return false;
	return rf_page_recovery_fpi_install_smgr_v1(out, serial, pin);
}

/* A local snapshot binds observations to the same original owners. Neither
 * this value nor the returned page is a completion or mutation capability. */
typedef struct PageRecoveryCtrcReadV1 {
	const struct ClusterRecoveryStartV1 *start;
	const ClusterColdCtrcWorkV1 *work;
	const ClusterColdTypedV1 *typed;
	const RfPageAuthorityGuardV1 *guard;
	ClusterRecoverySerialGuard *serial;
	ClusterWalRetentionPin *pin;
	uint64 work_generation;
	uint32 source_index;
	ClusterWalSourceRef source;
	ClusterRecoveryCheckpointOwnerV1 owner;
	RfPageIdentityV1 identity;
	RfPageVersionV1 before;
	RfPageRecoveryReferenceV1 reference;
} PageRecoveryCtrcReadV1;

static bool
page_recovery_ctrc_actor_current(void)
{
	return cluster_enabled && cluster_shared_config && IsUnderPostmaster
		&& MyBackendType == B_STARTUP && MyProcPid > 0 && CritSectionCount == 0
		&& !ShutdownRequestPending && !RecoveryInProgress();
}

static bool
page_recovery_ctrc_current(const PageRecoveryCtrcReadV1 *context)
{
	ClusterRecoveryCheckpointOwnerV1 owner;
	RfPageRecoveryReferenceV1 reference;
	const ClusterColdTypedV1 *typed;

	if (!page_recovery_ctrc_actor_current()
		|| !ClusterColdRecoveryCtrcWorkCurrent(context->start, context->work)
		|| context->work->generation != context->work_generation)
		return false;
	typed = ClusterColdRecoveryTxInputs(context->start);
	if (typed != context->typed || typed->ctrc_work != context->work
		|| typed->ctrc_work_generation != context->work_generation
		|| typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| typed->replay_count == 0 || typed->replay_count > typed->participant_count
		|| context->source_index >= typed->replay_count
		|| memcmp(&typed->sources[context->source_index], &context->source,
			sizeof(context->source)) != 0
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(context->start, &owner)
		|| memcmp(&owner, &context->owner, sizeof(owner)) != 0
		|| !context->serial->held || context->serial->release_uncertain
		|| context->serial->mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED
		|| memcmp(&context->serial->duty, &context->source.claim.identity,
			sizeof(context->serial->duty)) != 0)
		return false;
	return rf_page_authority_recovery_reference_v1(context->guard, context->serial,
		context->pin, &context->identity, &context->before, &reference)
		&& memcmp(&reference, &context->reference, sizeof(reference)) == 0;
}

static bool
page_recovery_ctrc_output_overlaps(const PGAlignedBlock *out, const void *input, Size size)
{
	uintptr_t a = (uintptr_t)out, b = (uintptr_t)input;

	return size != 0 && (a <= b ? b - a < sizeof(*out) : a - b < size);
}

static bool
page_recovery_ctrc_storage_present(SMgrRelation relation, BlockNumber blockno)
{
	return smgrexists(relation, SPACE_FORKNUM) && smgrnblocks(relation, SPACE_FORKNUM) == 2
		&& smgrexists(relation, MAIN_FORKNUM) && blockno < smgrnblocks(relation, MAIN_FORKNUM);
}

static bool
page_recovery_ctrc_read(const PageRecoveryCtrcReadV1 *context, SMgrRelation relation,
	ForkNumber forknum, BlockNumber blockno, PGAlignedBlock *page)
{
	if (!page_recovery_ctrc_current(context)
		|| !page_recovery_ctrc_storage_present(relation, context->identity.blockno)
		|| !page_recovery_ctrc_current(context))
		return false;
	smgrread(relation, forknum, blockno, page->data);
	return page_recovery_ctrc_current(context)
		&& page_recovery_ctrc_storage_present(relation, context->identity.blockno)
		&& page_recovery_ctrc_current(context);
}

static bool
page_recovery_ctrc_read_image(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageAuthorityGuardV1 *guard,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
	const ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, PGAlignedBlock *out,
	PageRecoveryCtrcReadV1 *retained)
{
	PageRecoveryCtrcReadV1 context = {0};
	RfPageInstallStorageOpsV1 storage = {0};
	RfPageIdentityV1 space_page;
	ClusterSpaceIdentity identity;
	ClusterSpaceIdentityKey key;
	ClusterSpaceReservation reservation;
	uint8 expected_identity[CLUSTER_SPACE_IDENTITY_BYTES], encoded[CLUSTER_SPACE_IDENTITY_BYTES];
	PGAlignedBlock space0, space1, page, verify;
	SMgrRelation relation;
	Size work_bytes;
	uint64 token;
	uint32 p;

	if (start == NULL || work == NULL || guard == NULL || serial == NULL || pin == NULL
		|| space == NULL || expected_before == NULL || out == NULL
		|| !page_recovery_ctrc_actor_current()
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, work) || work->generation == 0)
		return false;
	context.typed = ClusterColdRecoveryTxInputs(start);
	if (context.typed == NULL || context.typed->participant_count > CLUSTER_COLD_MAX_PARTICIPANTS
		|| context.typed->replay_count == 0
		|| context.typed->replay_count > context.typed->participant_count
		|| !ClusterColdRecoveryTxCheckpointOwnerV1(start, &context.owner)
		|| context.owner.owner_pid != MyProcPid || context.owner.owner_boot == 0
		|| context.owner.install_generation == 0 || context.owner.start_generation == 0
		|| context.owner.start_serial == 0
		|| work->counts.subject_count
			> (MaxAllocSize - offsetof(ClusterColdCtrcWorkV1, entries)) / sizeof(work->entries[0]))
		return false;
	work_bytes = offsetof(ClusterColdCtrcWorkV1, entries)
		+ (Size)work->counts.subject_count * sizeof(work->entries[0]);
	if (page_recovery_ctrc_output_overlaps(out, work, work_bytes)
		|| page_recovery_ctrc_output_overlaps(out, context.typed, sizeof(*context.typed))
		|| page_recovery_ctrc_output_overlaps(out, space, sizeof(*space))
		|| page_recovery_ctrc_output_overlaps(out, expected_before, sizeof(*expected_before))
		|| page_recovery_ctrc_output_overlaps(out, serial, sizeof(*serial))
		|| page_recovery_ctrc_output_overlaps(out, start, 1)
		|| page_recovery_ctrc_output_overlaps(out, guard, 1)
		|| page_recovery_ctrc_output_overlaps(out, pin, 1)
		|| space->state != CLUSTER_SPACE_IDENTITY_LIVE
		|| !cluster_space_identity_encode(space, expected_identity, sizeof(expected_identity))
		|| !rf_page_version_present_v1(expected_before)
		|| memcmp(space->incarnation, expected_before->segment_incarnation, 16) != 0
		|| space->key.system_identifier != context.owner.writer.ref.claim.identity.system_identifier
		|| space->key.database_incarnation != context.owner.writer.ref.claim.database_incarnation
		|| memcmp(space->key.storage_uuid, context.owner.writer.ref.claim.identity.storage_uuid, 16) != 0)
		return false;
	context.start = start;
	context.work = work;
	context.work_generation = work->generation;
	context.guard = guard;
	context.serial = serial;
	context.pin = pin;
	context.before = *expected_before;
	key = space->key;
	context.identity.system_identifier = space->key.system_identifier;
	memcpy(context.identity.storage_uuid, space->key.storage_uuid, 16);
	context.identity.locator = space->key.locator;
	context.identity.forknum = MAIN_FORKNUM;
	context.identity.blockno = blockno;
	for (p = 0; p < context.typed->replay_count; p++)
		if (memcmp(&serial->duty, &context.typed->sources[p].claim.identity,
			sizeof(serial->duty)) == 0)
			break;
	if (p == context.typed->replay_count || !rf_page_identity_valid_v1(&context.identity))
		return false;
	context.source_index = p;
	memcpy(&context.source, &context.typed->sources[p], sizeof(context.source));
	if (!rf_page_authority_recovery_reference_v1(guard, serial, pin, &context.identity,
		&context.before, &context.reference) || !page_recovery_ctrc_current(&context))
		return false;
	storage.checksums_enabled = DataChecksumsEnabled();
	storage.checksum = page_smgr_checksum;
	space_page = context.identity;
	space_page.forknum = SPACE_FORKNUM;
	space_page.blockno = 0;
	relation = smgropen(context.identity.locator, InvalidBackendId);
	if (!page_recovery_ctrc_read(&context, relation, SPACE_FORKNUM, 0, &space0)
		|| !page_checksum_valid(&storage, &space_page, space0.data)
		|| !cluster_space_identity_page_decode(space0.data, BLCKSZ, SPACE_FORKNUM, 0,
			&key, &identity, &token)
		|| !cluster_space_identity_encode(&identity, encoded, sizeof(encoded))
		|| memcmp(encoded, expected_identity, sizeof(encoded)) != 0)
		return false;
	space_page.blockno = 1;
	if (!page_recovery_ctrc_read(&context, relation, SPACE_FORKNUM, 1, &space1)
		|| !page_checksum_valid(&storage, &space_page, space1.data)
		|| !cluster_space_reservation_page_decode(space1.data, BLCKSZ, SPACE_FORKNUM, 1,
			&key, &reservation, &token)
		|| reservation.next_block <= blockno
		|| !cluster_space_identity_encode(&reservation.identity, encoded, sizeof(encoded))
		|| memcmp(encoded, expected_identity, sizeof(encoded)) != 0
		|| !page_recovery_ctrc_read(&context, relation, MAIN_FORKNUM, blockno, &page)
		|| !page_layout_valid(page.data)
		|| !page_checksum_valid(&storage, &context.identity, page.data)
		|| ((PageHeader)page.data)->pd_block_scn != context.before.mutation_token
		|| !page_recovery_ctrc_read(&context, relation, SPACE_FORKNUM, 0, &verify)
		|| memcmp(verify.data, space0.data, BLCKSZ) != 0
		|| !page_recovery_ctrc_read(&context, relation, SPACE_FORKNUM, 1, &verify)
		|| memcmp(verify.data, space1.data, BLCKSZ) != 0
		|| !page_recovery_ctrc_current(&context))
		return false;
	memcpy(out, &page, sizeof(page));
	if (retained != NULL)
		*retained = context;
	return true;
}

bool
rf_page_recovery_ctrc_read_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageAuthorityGuardV1 *guard,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
	const ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, PGAlignedBlock *out)
{
	return page_recovery_ctrc_read_image(start, work, guard, serial, pin, space,
		blockno, expected_before, out, NULL);
}

typedef struct PageRecoveryCtrcItlInputsV1 {
	const ClusterColdPlanV1 *plan;
	const struct ClusterColdTopTxCensusV1 *census;
	uint64 generation;
	ClusterColdCtrcReleaseCensusV1 counts;
	Size count;
	RfPageRecoveryCtrcItlSelectionV1 selections[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterColdTtSlotInputV1 subjects[CLUSTER_ITL_INITRANS_DEFAULT];
} PageRecoveryCtrcItlInputsV1;

static bool
page_recovery_ctrc_itl_inputs_current(const PageRecoveryCtrcReadV1 *context,
	const RfPageRecoveryCtrcItlSelectionV1 *selections, const PageRecoveryCtrcItlInputsV1 *fixed)
{
	if (!page_recovery_ctrc_current(context)
		|| context->work_generation != fixed->generation
		|| context->work->plan != fixed->plan || context->work->top_tx_census != fixed->census
		|| memcmp(&context->work->counts, &fixed->counts, sizeof(fixed->counts)) != 0
		|| memcmp(selections, fixed->selections, fixed->count * sizeof(*selections)) != 0)
		return false;
	for (Size i = 0; i < fixed->count; i++)
		if (memcmp(&context->work->entries[fixed->selections[i].subject_index],
			&fixed->subjects[i], sizeof(fixed->subjects[i])) != 0)
			return false;
	return true;
}

static bool
page_recovery_ctrc_itl_output_overlaps(const void *out, Size out_size,
	const void *input, Size size)
{
	uintptr_t a = (uintptr_t)out, b = (uintptr_t)input;

	return size != 0 && (a <= b ? b - a < out_size : a - b < size);
}

static bool
page_recovery_ctrc_fpi_identity_current(const ClusterSpaceIdentity *space,
	const uint8 identity[CLUSTER_SPACE_IDENTITY_BYTES], const RfPageVersionV1 *before,
	const RfPageVersionV1 *fixed_before)
{
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];

	return cluster_space_identity_encode(space, encoded, sizeof(encoded))
		&& memcmp(encoded, identity, sizeof(encoded)) == 0
		&& memcmp(before, fixed_before, sizeof(*before)) == 0;
}

static bool
page_recovery_ctrc_itl_image(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageAuthorityGuardV1 *guard,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
	const ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, const RfPageRecoveryCtrcItlSelectionV1 *selections,
	Size count, RfPageRecoveryCtrcItlImageV1 *out, RfPageRecoveryCtrcItlFpiV1 *fpi_out)
{
	PageRecoveryCtrcReadV1 context;
	PageRecoveryCtrcItlInputsV1 fixed;
	ClusterCtrcRecoveryItlPlanV1 plans[CLUSTER_ITL_INITRANS_DEFAULT];
	RfPageRecoveryCtrcItlImageV1 candidate = {0};
	ClusterCtrcItlCleanoutApplyResult result;
	const ClusterColdTypedV1 *typed;
	PGAlignedBlock before;
	ClusterSpaceIdentity frame_space;
	RfPageVersionV1 frame_before;
	uint8 frame_identity[CLUSTER_SPACE_IDENTITY_BYTES];
	void *output = out != NULL ? (void *)out : (void *)fpi_out;
	Size out_size = out != NULL ? sizeof(*out) : sizeof(*fpi_out);
	Size work_bytes;

	if (start == NULL || work == NULL || guard == NULL || serial == NULL || pin == NULL
		|| space == NULL || expected_before == NULL || selections == NULL || output == NULL
		|| count == 0 || count > CLUSTER_ITL_INITRANS_DEFAULT
		|| !page_recovery_ctrc_actor_current()
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, work))
		return false;
	typed = ClusterColdRecoveryTxInputs(start);
	if (typed == NULL || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| work->generation == 0 || work->counts.subject_count
			> (MaxAllocSize - offsetof(ClusterColdCtrcWorkV1, entries)) / sizeof(work->entries[0]))
		return false;
	work_bytes = offsetof(ClusterColdCtrcWorkV1, entries)
		+ (Size)work->counts.subject_count * sizeof(work->entries[0]);
	if (page_recovery_ctrc_itl_output_overlaps(output, out_size, work, work_bytes)
		|| page_recovery_ctrc_itl_output_overlaps(output, out_size, typed, sizeof(*typed))
		|| page_recovery_ctrc_itl_output_overlaps(output, out_size, selections, count * sizeof(*selections))
		|| page_recovery_ctrc_itl_output_overlaps(output, out_size, space, sizeof(*space))
		|| page_recovery_ctrc_itl_output_overlaps(output, out_size, expected_before, sizeof(*expected_before))
		|| page_recovery_ctrc_itl_output_overlaps(output, out_size, serial, sizeof(*serial))
		|| page_recovery_ctrc_itl_output_overlaps(output, out_size, start, 1)
		|| page_recovery_ctrc_itl_output_overlaps(output, out_size, guard, 1)
		|| page_recovery_ctrc_itl_output_overlaps(output, out_size, pin, 1))
		return false;
	if (fpi_out != NULL) {
		frame_space = *space;
		frame_before = *expected_before;
		if (!cluster_space_identity_encode(&frame_space, frame_identity, sizeof(frame_identity)))
			return false;
	}
	fixed.plan = work->plan;
	fixed.census = work->top_tx_census;
	fixed.generation = work->generation;
	memcpy(&fixed.counts, &work->counts, sizeof(fixed.counts));
	fixed.count = count;
	memcpy(fixed.selections, selections, count * sizeof(*selections));
	for (Size i = 0; i < count; i++) {
		const RfPageRecoveryCtrcItlSelectionV1 *selection = &fixed.selections[i];

		if (selection->subject_index >= fixed.counts.subject_count
			|| selection->itl_index >= CLUSTER_ITL_INITRANS_DEFAULT
			|| (selection->itl_class != 1 && selection->itl_class != 2))
			return false;
		for (Size j = 0; j < i; j++)
			if (fixed.selections[j].itl_index == selection->itl_index)
				return false;
		memcpy(&fixed.subjects[i], &work->entries[selection->subject_index],
			sizeof(fixed.subjects[i]));
		plans[i].subject = &fixed.subjects[i];
		plans[i].itl_index = selection->itl_index;
		plans[i].expected_itl_wrap = selection->expected_itl_wrap;
		plans[i].itl_class = selection->itl_class;
	}
	if (!page_recovery_ctrc_read_image(start, work, guard, serial, pin, space,
		blockno, expected_before, &before, &context)
		|| context.typed != typed
		|| !page_recovery_ctrc_itl_inputs_current(&context, selections, &fixed))
		return false;
	result = cluster_ctrc_recovery_itl_batch_image_v1(plans, count, before.data, candidate.image.data);
	if (result == CLUSTER_CTRC_ITL_CLEANOUT_RETAIN
		|| !page_recovery_ctrc_itl_inputs_current(&context, selections, &fixed))
		return false;
	candidate.rewritten = result == CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN;
	if (fpi_out != NULL) {
		RfPageRecoveryCtrcItlFpiV1 fpi;

		memset(&fpi, 0, sizeof(fpi));
		if (!page_recovery_ctrc_fpi_identity_current(space, frame_identity,
			expected_before, &frame_before))
			return false;
		fpi.rewritten = candidate.rewritten;
		if (fpi.rewritten && !GenericXLogRecoveryImagePrepare(GENERIC_XLOG_CTRC_ITL,
			&frame_space, MAIN_FORKNUM, blockno, before.data, candidate.image.data, &fpi.frame))
			return false;
		/* The original token producer can yield or throw. Its private result
		 * is publishable only while every borrowed input still matches. */
		if (!page_recovery_ctrc_itl_inputs_current(&context, selections, &fixed)
			|| !page_recovery_ctrc_fpi_identity_current(space, frame_identity,
				expected_before, &frame_before))
			return false;
		memcpy(fpi_out, &fpi, sizeof(fpi));
		return true;
	}
	memcpy(out, &candidate, sizeof(candidate));
	return true;
}

bool
rf_page_recovery_ctrc_itl_image_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageAuthorityGuardV1 *guard,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
	const ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, const RfPageRecoveryCtrcItlSelectionV1 *selections,
	Size count, RfPageRecoveryCtrcItlImageV1 *out)
{
	return page_recovery_ctrc_itl_image(start, work, guard, serial, pin, space,
		blockno, expected_before, selections, count, out, NULL);
}

bool
rf_page_recovery_ctrc_itl_fpi_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageAuthorityGuardV1 *guard,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
	const ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, const RfPageRecoveryCtrcItlSelectionV1 *selections,
	Size count, RfPageRecoveryCtrcItlFpiV1 *out)
{
	return page_recovery_ctrc_itl_image(start, work, guard, serial, pin, space,
		blockno, expected_before, selections, count, NULL, out);
}

typedef struct PageRecoveryCtrcPageInputsV1 {
	const ClusterColdPlanV1 *plan;
	const struct ClusterColdTopTxCensusV1 *census;
	uint64 generation;
	ClusterColdCtrcReleaseCensusV1 counts;
} PageRecoveryCtrcPageInputsV1;

static bool
page_recovery_ctrc_page_inputs_current(const PageRecoveryCtrcReadV1 *context,
	const PageRecoveryCtrcPageInputsV1 *fixed, const ClusterColdTtSlotInputV1 *subjects)
{
	return page_recovery_ctrc_current(context)
		&& context->work_generation == fixed->generation
		&& context->work->plan == fixed->plan && context->work->top_tx_census == fixed->census
		&& memcmp(&context->work->counts, &fixed->counts, sizeof(fixed->counts)) == 0
		&& (fixed->counts.subject_count == 0
			|| memcmp(context->work->entries, subjects,
				(Size)fixed->counts.subject_count * sizeof(*subjects)) == 0);
}

bool
rf_page_recovery_ctrc_itl_page_fpi_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const RfPageAuthorityGuardV1 *guard,
	ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
	const ClusterSpaceIdentity *space, BlockNumber blockno,
	const RfPageVersionV1 *expected_before, ClusterColdTtSlotInputV1 *scratch,
	Size capacity, RfPageRecoveryCtrcItlFpiV1 *out)
{
	PageRecoveryCtrcReadV1 context;
	PageRecoveryCtrcPageInputsV1 fixed;
	RfPageRecoveryCtrcItlFpiV1 candidate = {0};
	PGAlignedBlock before, image;
	ClusterSpaceIdentity frame_space;
	RfPageVersionV1 frame_before;
	uint8 frame_identity[CLUSTER_SPACE_IDENTITY_BYTES];
	ClusterCtrcItlCleanoutApplyResult result;
	const ClusterColdTypedV1 *typed;
	Size work_bytes, subject_bytes, scratch_bytes;

	if (start == NULL || work == NULL || guard == NULL || serial == NULL || pin == NULL
		|| space == NULL || expected_before == NULL || out == NULL
		|| capacity > MaxAllocSize / sizeof(*scratch) || (capacity != 0 && scratch == NULL)
		|| !page_recovery_ctrc_actor_current()
		|| !ClusterColdRecoveryCtrcWorkCurrent(start, work))
		return false;
	typed = ClusterColdRecoveryTxInputs(start);
	if (typed == NULL || !ClusterColdRecoveryCtrcWorkCurrent(start, work)
		|| work->generation == 0 || work->counts.subject_count > capacity
		|| work->counts.subject_count
			> (MaxAllocSize - offsetof(ClusterColdCtrcWorkV1, entries)) / sizeof(work->entries[0]))
		return false;
	subject_bytes = (Size)work->counts.subject_count * sizeof(*scratch);
	work_bytes = offsetof(ClusterColdCtrcWorkV1, entries) + subject_bytes;
	scratch_bytes = capacity * sizeof(*scratch);
	if (page_recovery_ctrc_itl_output_overlaps(out, sizeof(*out), scratch, scratch_bytes))
		return false;
	{
		const void *borrowed[] = {work, typed, space, expected_before, serial, start, guard, pin};
		const Size bytes[] = {work_bytes, sizeof(*typed), sizeof(*space), sizeof(*expected_before),
			sizeof(*serial), 1, 1, 1};

		for (Size i = 0; i < lengthof(borrowed); i++)
			if (page_recovery_ctrc_itl_output_overlaps(out, sizeof(*out), borrowed[i], bytes[i])
				|| (scratch_bytes != 0 && page_recovery_ctrc_itl_output_overlaps(scratch,
					scratch_bytes, borrowed[i], bytes[i])))
				return false;
	}
	frame_space = *space;
	frame_before = *expected_before;
	if (!cluster_space_identity_encode(&frame_space, frame_identity, sizeof(frame_identity)))
		return false;
	fixed.plan = work->plan;
	fixed.census = work->top_tx_census;
	fixed.generation = work->generation;
	memcpy(&fixed.counts, &work->counts, sizeof(fixed.counts));
	if (subject_bytes != 0)
		memcpy(scratch, work->entries, subject_bytes);
	if (!page_recovery_ctrc_read_image(start, work, guard, serial, pin, space,
		blockno, expected_before, &before, &context)
		|| context.typed != typed
		|| !page_recovery_ctrc_page_inputs_current(&context, &fixed, scratch))
		return false;
	result = cluster_ctrc_recovery_itl_page_image_v1(scratch, fixed.counts.subject_count,
		before.data, image.data);
	if (result == CLUSTER_CTRC_ITL_CLEANOUT_RETAIN
		|| !page_recovery_ctrc_page_inputs_current(&context, &fixed, scratch)
		|| !page_recovery_ctrc_fpi_identity_current(space, frame_identity,
			expected_before, &frame_before))
		return false;
	candidate.rewritten = result == CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN;
	if (candidate.rewritten && !GenericXLogRecoveryImagePrepare(GENERIC_XLOG_CTRC_ITL,
		&frame_space, MAIN_FORKNUM, blockno, before.data, image.data, &candidate.frame))
		return false;
	if (!page_recovery_ctrc_page_inputs_current(&context, &fixed, scratch)
		|| !page_recovery_ctrc_fpi_identity_current(space, frame_identity,
			expected_before, &frame_before))
		return false;
	memcpy(out, &candidate, sizeof(candidate));
	return true;
}
#endif /* USE_PGRAC_CLUSTER && !USE_CLUSTER_UNIT */
