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
	bool promoted;
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
