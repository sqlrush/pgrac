/*-------------------------------------------------------------------------
 *
 * generic_xlog.c
 *	 Implementation of generic xlog records.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/generic_xlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/generic_xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#ifdef USE_PGRAC_CLUSTER
/* PGRAC: exact versions for the three internal cleanup owners. */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_page_cold_redo.h"
#include "cluster/cluster_space_storage.h"
#include "common/cryptohash.h"
#endif

/*-------------------------------------------------------------------------
 * Internally, a delta between pages consists of a set of fragments.  Each
 * fragment represents changes made in a given region of a page.  A fragment
 * is made up as follows:
 *
 * - offset of page region (OffsetNumber)
 * - length of page region (OffsetNumber)
 * - data - the data to place into the region ('length' number of bytes)
 *
 * Unchanged regions of a page are not represented in its delta.  As a result,
 * a delta can be more compact than the full page image.  But having an
 * unchanged region between two fragments that is smaller than the fragment
 * header (offset+length) does not pay off in terms of the overall size of
 * the delta.  For this reason, we merge adjacent fragments if the unchanged
 * region between them is <= MATCH_THRESHOLD bytes.
 *
 * We do not bother to merge fragments across the "lower" and "upper" parts
 * of a page; it's very seldom the case that pd_lower and pd_upper are within
 * MATCH_THRESHOLD bytes of each other, and handling that infrequent case
 * would complicate and slow down the delta-computation code unduly.
 * Therefore, the worst-case delta size includes two fragment headers plus
 * a full page's worth of data.
 *-------------------------------------------------------------------------
 */
#define FRAGMENT_HEADER_SIZE	(2 * sizeof(OffsetNumber))
#define MATCH_THRESHOLD			FRAGMENT_HEADER_SIZE
#define MAX_DELTA_SIZE			(BLCKSZ + 2 * FRAGMENT_HEADER_SIZE)

/* Struct of generic xlog data for single page */
typedef struct
{
	Buffer		buffer;			/* registered buffer */
	int			flags;			/* flags for this buffer */
	int			deltaLen;		/* space consumed in delta field */
	char	   *image;			/* copy of page image for modification, do not
								 * do it in-place to have aligned memory chunk */
	char		delta[MAX_DELTA_SIZE];	/* delta between page images */
#ifdef USE_PGRAC_CLUSTER
	bool		versioned;
	uint64		before_token;
	RfPageProducerComponentV1 component;
#endif
} PageData;

/*
 * State of generic xlog record construction.  Must be allocated at an I/O
 * aligned address.
 */
struct GenericXLogState
{
	/* Page images (properly aligned, must be first) */
	PGIOAlignedBlock images[MAX_GENERIC_XLOG_PAGES];
	/* Info about each page, see above */
	PageData	pages[MAX_GENERIC_XLOG_PAGES];
	bool		isLogged;
#ifdef USE_PGRAC_CLUSTER
	bool		internal;
#endif
};

static void writeFragment(PageData *pageData, OffsetNumber offset,
						  OffsetNumber length, const char *data);
static void computeRegionDelta(PageData *pageData,
							   const char *curpage, const char *targetpage,
							   int targetStart, int targetEnd,
							   int validStart, int validEnd);
static void computeDelta(PageData *pageData, Page curpage, Page targetpage);
static void applyPageRedo(Page page, const char *delta, Size deltaSize);
static GenericXLogState *genericXLogStart(bool is_logged);


/*
 * Write next fragment into pageData's delta.
 *
 * The fragment has the given offset and length, and data points to the
 * actual data (of length length).
 */
static void
writeFragment(PageData *pageData, OffsetNumber offset, OffsetNumber length,
			  const char *data)
{
	char	   *ptr = pageData->delta + pageData->deltaLen;

	/* Verify we have enough space */
	Assert(pageData->deltaLen + sizeof(offset) +
		   sizeof(length) + length <= sizeof(pageData->delta));

	/* Write fragment data */
	memcpy(ptr, &offset, sizeof(offset));
	ptr += sizeof(offset);
	memcpy(ptr, &length, sizeof(length));
	ptr += sizeof(length);
	memcpy(ptr, data, length);
	ptr += length;

	pageData->deltaLen = ptr - pageData->delta;
}

/*
 * Compute the XLOG fragments needed to transform a region of curpage into the
 * corresponding region of targetpage, and append them to pageData's delta
 * field.  The region to transform runs from targetStart to targetEnd-1.
 * Bytes in curpage outside the range validStart to validEnd-1 should be
 * considered invalid, and always overwritten with target data.
 *
 * This function is a hot spot, so it's worth being as tense as possible
 * about the data-matching loops.
 */
static void
computeRegionDelta(PageData *pageData,
				   const char *curpage, const char *targetpage,
				   int targetStart, int targetEnd,
				   int validStart, int validEnd)
{
	int			i,
				loopEnd,
				fragmentBegin = -1,
				fragmentEnd = -1;

	/* Deal with any invalid start region by including it in first fragment */
	if (validStart > targetStart)
	{
		fragmentBegin = targetStart;
		targetStart = validStart;
	}

	/* We'll deal with any invalid end region after the main loop */
	loopEnd = Min(targetEnd, validEnd);

	/* Examine all the potentially matchable bytes */
	i = targetStart;
	while (i < loopEnd)
	{
		if (curpage[i] != targetpage[i])
		{
			/* On unmatched byte, start new fragment if not already in one */
			if (fragmentBegin < 0)
				fragmentBegin = i;
			/* Mark unmatched-data endpoint as uncertain */
			fragmentEnd = -1;
			/* Extend the fragment as far as possible in a tight loop */
			i++;
			while (i < loopEnd && curpage[i] != targetpage[i])
				i++;
			if (i >= loopEnd)
				break;
		}

		/* Found a matched byte, so remember end of unmatched fragment */
		fragmentEnd = i;

		/*
		 * Extend the match as far as possible in a tight loop.  (On typical
		 * workloads, this inner loop is the bulk of this function's runtime.)
		 */
		i++;
		while (i < loopEnd && curpage[i] == targetpage[i])
			i++;

		/*
		 * There are several possible cases at this point:
		 *
		 * 1. We have no unwritten fragment (fragmentBegin < 0).  There's
		 * nothing to write; and it doesn't matter what fragmentEnd is.
		 *
		 * 2. We found more than MATCH_THRESHOLD consecutive matching bytes.
		 * Dump out the unwritten fragment, stopping at fragmentEnd.
		 *
		 * 3. The match extends to loopEnd.  We'll do nothing here, exit the
		 * loop, and then dump the unwritten fragment, after merging it with
		 * the invalid end region if any.  If we don't so merge, fragmentEnd
		 * establishes how much the final writeFragment call needs to write.
		 *
		 * 4. We found an unmatched byte before loopEnd.  The loop will repeat
		 * and will enter the unmatched-byte stanza above.  So in this case
		 * also, it doesn't matter what fragmentEnd is.  The matched bytes
		 * will get merged into the continuing unmatched fragment.
		 *
		 * Only in case 3 do we reach the bottom of the loop with a meaningful
		 * fragmentEnd value, which is why it's OK that we unconditionally
		 * assign "fragmentEnd = i" above.
		 */
		if (fragmentBegin >= 0 && i - fragmentEnd > MATCH_THRESHOLD)
		{
			writeFragment(pageData, fragmentBegin,
						  fragmentEnd - fragmentBegin,
						  targetpage + fragmentBegin);
			fragmentBegin = -1;
			fragmentEnd = -1;	/* not really necessary */
		}
	}

	/* Deal with any invalid end region by including it in final fragment */
	if (loopEnd < targetEnd)
	{
		if (fragmentBegin < 0)
			fragmentBegin = loopEnd;
		fragmentEnd = targetEnd;
	}

	/* Write final fragment if any */
	if (fragmentBegin >= 0)
	{
		if (fragmentEnd < 0)
			fragmentEnd = targetEnd;
		writeFragment(pageData, fragmentBegin,
					  fragmentEnd - fragmentBegin,
					  targetpage + fragmentBegin);
	}
}

/*
 * Compute the XLOG delta record needed to transform curpage into targetpage,
 * and store it in pageData's delta field.
 */
static void
computeDelta(PageData *pageData, Page curpage, Page targetpage)
{
	int			targetLower = ((PageHeader) targetpage)->pd_lower,
				targetUpper = ((PageHeader) targetpage)->pd_upper,
				curLower = ((PageHeader) curpage)->pd_lower,
				curUpper = ((PageHeader) curpage)->pd_upper;

	pageData->deltaLen = 0;

	/* Compute delta records for lower part of page ... */
	computeRegionDelta(pageData, curpage, targetpage,
					   0, targetLower,
					   0, curLower);
	/* ... and for upper part, ignoring what's between */
	computeRegionDelta(pageData, curpage, targetpage,
					   targetUpper, BLCKSZ,
					   curUpper, BLCKSZ);

	/*
	 * If xlog debug is enabled, then check produced delta.  Result of delta
	 * application to curpage should be equivalent to targetpage.
	 */
#ifdef WAL_DEBUG
	if (XLOG_DEBUG)
	{
		PGAlignedBlock tmp;

		memcpy(tmp.data, curpage, BLCKSZ);
		applyPageRedo(tmp.data, pageData->delta, pageData->deltaLen);
		if (memcmp(tmp.data, targetpage, targetLower) != 0 ||
			memcmp(tmp.data + targetUpper, targetpage + targetUpper,
				   BLCKSZ - targetUpper) != 0)
			elog(ERROR, "result of generic xlog apply does not match");
	}
#endif
}

/*
 * Start new generic xlog record for modifications to specified relation.
 */
GenericXLogState *
GenericXLogStart(Relation relation)
{
	return GenericXLogStartLogged(RelationNeedsWAL(relation));
}

/*
 * Start a new generic xlog record when the caller already knows whether
 * the target relfilenode needs WAL.  This is used by code paths that only
 * carry a RelFileLocator at finish time and intentionally avoid relcache
 * lookup in transaction-end hooks.
 */
GenericXLogState *
GenericXLogStartLogged(bool is_logged)
{
#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("shared generic WAL requires an internal cleanup owner")));
#endif
	return genericXLogStart(is_logged);
}

static GenericXLogState *
genericXLogStart(bool is_logged)
{
	GenericXLogState *state;
	int			i;

	state = (GenericXLogState *) palloc_aligned(sizeof(GenericXLogState),
												PG_IO_ALIGN_SIZE,
												0);
	state->isLogged = is_logged;
#ifdef USE_PGRAC_CLUSTER
	state->internal = false;
#endif

	for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
	{
		state->pages[i].image = state->images[i].data;
		state->pages[i].buffer = InvalidBuffer;
#ifdef USE_PGRAC_CLUSTER
		state->pages[i].versioned = false;
#endif
	}

	return state;
}

#ifdef USE_PGRAC_CLUSTER
GenericXLogState *
GenericXLogStartInternal(bool is_logged, GenericXLogInternalOwner owner)
{
	GenericXLogState *state;

	if (owner != GENERIC_XLOG_ITL_FINISH && owner != GENERIC_XLOG_CTRC_MX &&
		owner != GENERIC_XLOG_CTRC_ITL)
		elog(ERROR, "unknown internal generic WAL owner");
	state = genericXLogStart(is_logged);
	state->internal = true;
	return state;
}
#endif

/*
 * Register new buffer for generic xlog record.
 *
 * Returns pointer to the page's image in the GenericXLogState, which
 * is what the caller should modify.
 *
 * If the buffer is already registered, just return its existing entry.
 * (It's not very clear what to do with the flags in such a case, but
 * for now we stay with the original flags.)
 */
Page
GenericXLogRegisterBuffer(GenericXLogState *state, Buffer buffer, int flags)
{
#ifdef USE_PGRAC_CLUSTER
	return GenericXLogRegisterBufferVersioned(state, buffer, flags, NULL);
}

Page
GenericXLogRegisterBufferVersioned(GenericXLogState *state, Buffer buffer,
								   int flags, const ClusterSpaceIdentity *identity)
{
	RfPageProducerComponentV1 component;
	bool versioned = cluster_shared_config && !BufferIsLocal(buffer);
#endif
	int			block_id;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config && !state->internal)
		elog(ERROR, "shared generic WAL has no internal cleanup owner");
#endif
	/* Search array for existing entry or first unused slot */
	for (block_id = 0; block_id < MAX_GENERIC_XLOG_PAGES; block_id++)
	{
		PageData   *page = &state->pages[block_id];

#ifdef USE_PGRAC_CLUSTER
		if ((BufferIsInvalid(page->buffer) || page->buffer == buffer) && versioned)
		{
			if (!state->isLogged ||
				!cluster_space_buffer_version_component(identity, buffer, block_id,
														   block_id, &component) ||
				PageIsNew(component.page) || ((PageHeader) component.page)->pd_block_scn == 0)
				elog(ERROR, "shared generic WAL requires exact SPACE and initialized page");
			if (page->buffer == buffer &&
				(!page->versioned ||
				 memcmp(page->component.segment_incarnation, component.segment_incarnation, 16) != 0 ||
				 page->before_token != ((PageHeader) component.page)->pd_block_scn))
				elog(ERROR, "shared generic WAL buffer identity changed");
		}
#endif
		if (BufferIsInvalid(page->buffer))
		{
			/* Empty slot, so use it (there cannot be a match later) */
			page->buffer = buffer;
			page->flags = flags;
			memcpy(page->image, BufferGetPage(buffer), BLCKSZ);
#ifdef USE_PGRAC_CLUSTER
			page->versioned = versioned;
			if (versioned)
			{
				page->component = component;
				page->component.page = (Page) page->image;
				page->before_token = ((PageHeader) page->image)->pd_block_scn;
			}
#endif
			return (Page) page->image;
		}
		else if (page->buffer == buffer)
		{
			/*
			 * Buffer is already registered.  Just return the image, which is
			 * already prepared.
			 */
			return (Page) page->image;
		}
	}

	elog(ERROR, "maximum number %d of generic xlog buffers is exceeded",
		 MAX_GENERIC_XLOG_PAGES);
	/* keep compiler quiet */
	return NULL;
}

/* The native full-image representation has a zeroed page hole. Keep the
 * ordinary buffer and the recovery frame on the same byte calculation. */
static void
genericXLogCopyImage(Page destination, const char *source)
{
	const PageHeaderData *header = (const PageHeaderData *)source;

	memcpy(destination, source, header->pd_lower);
	memset(destination + header->pd_lower, 0, header->pd_upper - header->pd_lower);
	memcpy(destination + header->pd_upper, source + header->pd_upper,
		   BLCKSZ - header->pd_upper);
}

#ifdef USE_PGRAC_CLUSTER
static bool
genericXLogImageLayoutValid(const char *image)
{
	const PageHeaderData *header = (const PageHeaderData *)image;

	return image != NULL && !PageIsNew((Page)image)
		&& PageGetPageSize((Page)image) == BLCKSZ
		&& PageGetPageLayoutVersion((Page)image) == PG_PAGE_LAYOUT_VERSION
		&& header->pd_lower >= SizeOfPageHeaderData && header->pd_lower <= header->pd_upper
		&& header->pd_upper <= header->pd_special && header->pd_special <= BLCKSZ;
}

static bool
genericXLogImageRangesOverlap(const void *a, Size a_size, const void *b, Size b_size)
{
	uintptr_t left = (uintptr_t)a, right = (uintptr_t)b;

	return left <= right ? right - left < a_size : left - right < b_size;
}

bool
GenericXLogRecoveryImagePrepare(GenericXLogInternalOwner owner,
	const ClusterSpaceIdentity *identity, ForkNumber fork_number, BlockNumber block_number,
	const char *before_image, const char *after_image, GenericXLogRecoveryImageV1 *out)
{
	GenericXLogRecoveryImageV1 value;
	RfPageProducerComponentV1 component;
	RfPageProducerBatchV1 batch;
	uint8 encoded_identity[CLUSTER_SPACE_IDENTITY_BYTES];

	if ((owner != GENERIC_XLOG_CTRC_ITL && owner != GENERIC_XLOG_CTRC_MX)
		|| identity == NULL || out == NULL || before_image == NULL || after_image == NULL
		|| identity->state != CLUSTER_SPACE_IDENTITY_LIVE || fork_number != MAIN_FORKNUM
		|| block_number == InvalidBlockNumber
		|| genericXLogImageRangesOverlap(out, sizeof(*out), before_image, BLCKSZ)
		|| genericXLogImageRangesOverlap(out, sizeof(*out), after_image, BLCKSZ)
		|| !cluster_space_identity_encode(identity, encoded_identity, sizeof(encoded_identity))
		|| !genericXLogImageLayoutValid(before_image) || !genericXLogImageLayoutValid(after_image)
		|| ((const PageHeaderData *)before_image)->pd_block_scn == 0
		|| ((const PageHeaderData *)after_image)->pd_block_scn
			!= ((const PageHeaderData *)before_image)->pd_block_scn)
		return false;

	memset(&value, 0, sizeof(value));
	value.owner = owner;
	value.identity = *identity;
	value.fork_number = fork_number;
	value.block_number = block_number;
	genericXLogCopyImage(value.image.data, after_image);
	memset(&component, 0, sizeof(component));
	component.page_class = RF_PAGE_CLASS_ORDINARY;
	component.before_kind = RF_PAGE_STATE_PRESENT;
	memcpy(component.segment_incarnation, identity->incarnation, 16);
	component.page = value.image.data;
	if (!rf_page_producer_prepare_v1(&component, 1, &batch)
		|| !rf_page_producer_stamp_v1(&batch))
		return false;
	value.result_token = batch.result_token;
	value.version = batch.entries[0];
	*out = value;
	return true;
}

static bool
genericXLogRecoveryImageValid(const GenericXLogRecoveryImageV1 *image)
{
	RfPageVersionEdgeEntryV1 expected;
	const PageHeaderData *header;
	uint8 encoded_identity[CLUSTER_SPACE_IDENTITY_BYTES];

	if (image == NULL
		|| (image->owner != GENERIC_XLOG_CTRC_ITL && image->owner != GENERIC_XLOG_CTRC_MX)
		|| image->identity.state != CLUSTER_SPACE_IDENTITY_LIVE
		|| image->fork_number != MAIN_FORKNUM || image->block_number == InvalidBlockNumber
		|| image->result_token == 0 || image->version.before.mutation_token == 0
		|| !cluster_space_identity_encode(&image->identity, encoded_identity, sizeof(encoded_identity))
		|| !genericXLogImageLayoutValid(image->image.data)
		|| ((PageHeader)image->image.data)->pd_block_scn != image->result_token)
		return false;
	memset(&expected, 0, sizeof(expected));
	expected.page_class = RF_PAGE_CLASS_ORDINARY;
	expected.before_kind = RF_PAGE_STATE_PRESENT;
	expected.result_kind = RF_PAGE_STATE_PRESENT;
	memcpy(expected.before.segment_incarnation, image->identity.incarnation, 16);
	expected.before.mutation_token = image->version.before.mutation_token;
	memcpy(expected.result_incarnation, image->identity.incarnation, 16);
	if (memcmp(&expected, &image->version, sizeof(expected)) != 0)
		return false;
	header = (const PageHeaderData *)image->image.data;
	for (uint16 pos = header->pd_lower; pos < header->pd_upper; pos++)
		if (image->image.data[pos] != 0)
			return false;
	return true;
}

bool
GenericXLogRecoveryImageDescribe(const GenericXLogRecoveryImageV1 *image,
	GenericXLogRecoveryImageDescriptorV1 *out)
{
	GenericXLogRecoveryImageDescriptorV1 value;
	pg_cryptohash_ctx *hash;
	bool ok;

	if (CritSectionCount != 0 || out == NULL || !genericXLogRecoveryImageValid(image)
		|| genericXLogImageRangesOverlap(out, sizeof(*out), image, sizeof(*image)))
		return false;
	memset(&value, 0, sizeof(value));
	if (!cluster_space_identity_encode(&image->identity, value.space_identity,
									   sizeof(value.space_identity)))
		return false;
	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL)
		return false;
	ok = pg_cryptohash_init(hash) == 0
		 && pg_cryptohash_update(hash, (const uint8 *)image->image.data, BLCKSZ) == 0
		 && pg_cryptohash_final(hash, value.after_sha256, sizeof(value.after_sha256)) == 0;
	pg_cryptohash_free(hash);
	if (!ok)
		return false;
	value.owner = image->owner;
	value.fork_number = image->fork_number;
	value.block_number = image->block_number;
	value.result_token = image->result_token;
	value.version = image->version;
	*out = value;
	return true;
}

bool
GenericXLogRecoveryImageMatchesDescriptor(const GenericXLogRecoveryImageV1 *image,
	const GenericXLogRecoveryImageDescriptorV1 *descriptor)
{
	GenericXLogRecoveryImageDescriptorV1 value;

	return descriptor != NULL && GenericXLogRecoveryImageDescribe(image, &value)
		&& value.owner == descriptor->owner
		&& value.fork_number == descriptor->fork_number
		&& value.block_number == descriptor->block_number
		&& value.result_token == descriptor->result_token
		&& memcmp(value.space_identity, descriptor->space_identity, sizeof(value.space_identity)) == 0
		&& memcmp(&value.version, &descriptor->version, sizeof(value.version)) == 0
		&& memcmp(value.after_sha256, descriptor->after_sha256, sizeof(value.after_sha256)) == 0;
}

bool
GenericXLogRecoveryImageRegister(GenericXLogRecoveryImageV1 *image)
{
	RfPageProducerBatchV1 batch;

	/* Payload registration only. The actual owner begins the native record
	 * and records F4 INSERTING before entering this critical section. */
	if (CritSectionCount == 0 || !genericXLogRecoveryImageValid(image))
		return false;
	memset(&batch, 0, sizeof(batch));
	batch.result_token = image->result_token;
	batch.entry_count = 1;
	batch.prepared = batch.stamped = true;
	batch.entries[0] = image->version;
	batch.ordinary_pages[0] = image->image.data;
	XLogRegisterBlock(0, &image->identity.key.locator, image->fork_number,
		image->block_number, image->image.data, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
	return rf_page_producer_register_wal_v1(&batch);
}
#endif

/*
 * Apply changes represented by GenericXLogState to the actual buffers,
 * and emit a generic xlog record.
 */
XLogRecPtr
GenericXLogFinish(GenericXLogState *state)
{
	XLogRecPtr	lsn;
	int			i;
#ifdef USE_PGRAC_CLUSTER
	RfPageProducerBatchV1 batch;
	RfPageProducerComponentV1 components[MAX_GENERIC_XLOG_PAGES];
	uint8 nversions = 0;
	uint8 npages = 0;

	/* Validate the complete batch before changing any live page or allocating
	 * its one token. Stamp private images, not the predecessor: the native
	 * delta must contain the version transition as well as the cleanup bytes. */
	for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
	{
		PageData *data = &state->pages[i];
		PageHeader image;

		if (BufferIsInvalid(data->buffer))
			continue;
		npages++;
		if (!data->versioned)
			continue;
		image = (PageHeader) data->image;
		if (((PageHeader) BufferGetPage(data->buffer))->pd_block_scn != data->before_token ||
			image->pd_block_scn != data->before_token ||
			image->pd_lower < SizeOfPageHeaderData || image->pd_lower > image->pd_upper ||
			image->pd_upper > image->pd_special || image->pd_special > BLCKSZ)
			elog(ERROR, "shared generic WAL predecessor or private image changed");
		components[nversions++] = data->component;
	}
	if (nversions != 0 &&
		(nversions != npages || !state->isLogged ||
		 !rf_page_producer_prepare_v1(components, nversions, &batch) ||
		 !rf_page_producer_stamp_v1(&batch)))
		elog(ERROR, "shared generic WAL cannot prepare exact page batch");
#endif

	if (state->isLogged)
	{
		/* Logged relation: make xlog record in critical section. */
		XLogBeginInsert();

		START_CRIT_SECTION();

		/*
		 * Compute deltas if necessary, write changes to buffers, mark
		 * buffers dirty, and register changes.
		 */
		for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		{
			PageData   *pageData = &state->pages[i];
			Page		page;

			if (BufferIsInvalid(pageData->buffer))
				continue;

			page = BufferGetPage(pageData->buffer);

			/*
			 * Compute delta while we still have both the unmodified page and
			 * the new image. Not needed if we are logging the full image.
			 */
			if (!(pageData->flags & GENERIC_XLOG_FULL_IMAGE))
				computeDelta(pageData, page, (Page) pageData->image);

			/*
			 * Apply the image, being careful to zero the "hole" between
			 * pd_lower and pd_upper in order to avoid divergence between
			 * actual page state and what replay would produce.
			 */
			genericXLogCopyImage(page, pageData->image);

			MarkBufferDirty(pageData->buffer);

			if (pageData->flags & GENERIC_XLOG_FULL_IMAGE)
			{
				XLogRegisterBuffer(i, pageData->buffer,
								   REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
			}
			else
			{
				XLogRegisterBuffer(i, pageData->buffer, REGBUF_STANDARD);
				XLogRegisterBufData(i, pageData->delta, pageData->deltaLen);
			}
		}

		/* Insert xlog record */
#ifdef USE_PGRAC_CLUSTER
		if (nversions != 0 && !rf_page_producer_register_wal_v1(&batch))
			elog(ERROR, "shared generic WAL lost its prepared page versions");
#endif
		lsn = XLogInsert(RM_GENERIC_ID, 0);

		/* Set LSN */
		for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		{
			PageData   *pageData = &state->pages[i];

			if (BufferIsInvalid(pageData->buffer))
				continue;
			PageSetLSN(BufferGetPage(pageData->buffer), lsn);
		}
		END_CRIT_SECTION();
	}
	else
	{
		/* Unlogged relation: skip xlog-related stuff */
		START_CRIT_SECTION();
		for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		{
			PageData   *pageData = &state->pages[i];

			if (BufferIsInvalid(pageData->buffer))
				continue;
			memcpy(BufferGetPage(pageData->buffer),
				   pageData->image,
				   BLCKSZ);
			/* We don't worry about zeroing the "hole" in this case */
			MarkBufferDirty(pageData->buffer);
		}
		END_CRIT_SECTION();
		/* We don't have a LSN to return, in this case */
		lsn = InvalidXLogRecPtr;
	}

	pfree(state);

	return lsn;
}

/*
 * Abort generic xlog record construction.  No changes are applied to buffers.
 *
 * Note: caller is responsible for releasing locks/pins on buffers, if needed.
 */
void
GenericXLogAbort(GenericXLogState *state)
{
	pfree(state);
}

/*
 * Apply delta to given page image.
 */
static void
applyPageRedo(Page page, const char *delta, Size deltaSize)
{
	const char *ptr = delta;
	const char *end = delta + deltaSize;

	while (ptr < end)
	{
		OffsetNumber offset,
					length;

		memcpy(&offset, ptr, sizeof(offset));
		ptr += sizeof(offset);
		memcpy(&length, ptr, sizeof(length));
		ptr += sizeof(length);

		memcpy(page + offset, ptr, length);

		ptr += length;
	}
}

/*
 * Redo function for generic xlog record.
 */
#ifdef USE_PGRAC_CLUSTER
static bool
genericVersionPageValid(Page page)
{
	PageHeader header = (PageHeader) page;

	return PageGetPageSize(page) == BLCKSZ &&
		PageGetPageLayoutVersion(page) == PG_PAGE_LAYOUT_VERSION &&
		header->pd_lower >= SizeOfPageHeaderData && header->pd_lower <= header->pd_upper &&
		header->pd_upper <= header->pd_special && header->pd_special <= BLCKSZ &&
		(header->pd_flags & (PD_SPACE_METADATA | PD_UNDO_SEG_HEADER)) == 0 &&
		header->pd_block_scn != 0;
}

static bool
genericDeltaValid(const char *data, Size length)
{
	Size position = 0;
	unsigned previous_end = 0;

	if (data == NULL || length == 0 || length > MAX_DELTA_SIZE)
		return false;
	while (position < length)
	{
		OffsetNumber offset, size;

		if (length - position < FRAGMENT_HEADER_SIZE)
			return false;
		memcpy(&offset, data + position, sizeof(offset));
		memcpy(&size, data + position + sizeof(offset), sizeof(size));
		position += FRAGMENT_HEADER_SIZE;
		if (size == 0 || size > length - position || offset < previous_end ||
			(unsigned) offset + size > BLCKSZ)
			return false;
		position += size;
		previous_end = offset + size;
	}
	return true;
}

/* Compare result bytes while ignoring native hints and WAL coordinates that
 * may legitimately change on replay or buffer write. Never mask ITL/tuple
 * contents, visibility, geometry, or the opaque result token. */
static bool
genericResultMatches(Page live, Page image)
{
	PageHeader actual = (PageHeader) live;
	PageHeader expected = (PageHeader) image;
	uint16 hints = PD_HAS_FREE_LINES | PD_PAGE_FULL | PD_CLUSTER_FORCE_FPI |
		PD_LSN_ORIGIN_VALID | PD_LSN_ORIGIN_MASK;

	expected->pd_lsn = actual->pd_lsn;
	expected->pd_checksum = actual->pd_checksum;
	expected->pd_prune_xid = actual->pd_prune_xid;
	expected->pd_flags = (expected->pd_flags & ~hints) | (actual->pd_flags & hints);
	return actual->pd_lower == expected->pd_lower && actual->pd_upper == expected->pd_upper &&
		memcmp(live, image, actual->pd_lower) == 0 &&
		memcmp(live + actual->pd_upper, image + actual->pd_upper, BLCKSZ - actual->pd_upper) == 0;
}

static void
genericVersionedRedo(XLogReaderState *record)
{
	const RfPageVersionEdgeV1 *edge;
	PGAlignedBlock images[MAX_GENERIC_XLOG_PAGES];
	Buffer buffers[MAX_GENERIC_XLOG_PAGES] = {0};
	bool apply[MAX_GENERIC_XLOG_PAGES] = {false};
	bool skip[MAX_GENERIC_XLOG_PAGES] = { false };
	bool cold = cluster_page_cold_redo_active_v1;
	const char *failure = "record shape";
	int count = XLogRecMaxBlockId(record) + 1;
	int i, j;

	if (!RecoveryInProgress() || count <= 0 || count > MAX_GENERIC_XLOG_PAGES ||
		!XLogRecHasPageVersionEdge(record) || XLogRecPtrIsInvalid(record->EndRecPtr) ||
		(XLogRecGetInfo(record) & ~XLR_INFO_MASK) != 0)
		goto refused;
	edge = XLogRecGetPageVersionEdge(record);
	if (edge->entry_count != count || edge->result_token == 0)
		goto refused;

	/* Read every SPACE identity before taking any heap content lock. Original
	 * recovery isolation/lifecycle ownership remains an independent gate. */
	for (i = 0; i < count; i++)
	{
		DecodedBkpBlock *block;
		const RfPageVersionEdgeEntryV1 *entry = &edge->entries[i];
		ClusterSpaceIdentity identity;
		ClusterColdRedoBlockV1 decision;
		bool has_image;
		Size length;
		char *delta;

		if (!XLogRecHasBlockRef(record, i))
			goto refused;
		block = XLogRecGetBlock(record, i);
		has_image = XLogRecBlockImageApply(record, i);
		if (block->forknum != MAIN_FORKNUM || block->blkno == InvalidBlockNumber
			|| (block->flags & BKPBLOCK_WILL_INIT) != 0 || entry->block_id != i
			|| entry->component_ordinal != block->component_ordinal
			|| entry->page_class != RF_PAGE_CLASS_ORDINARY
			|| entry->before_kind != RF_PAGE_STATE_PRESENT
			|| entry->result_kind != RF_PAGE_STATE_PRESENT || entry->before.mutation_token == 0
			|| entry->before.mutation_token == edge->result_token
			|| entry->edge_flags
				   != (has_image ? RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE : 0))
			goto refused;
		for (j = 0; j < i; j++)
		{
			DecodedBkpBlock *prior = XLogRecGetBlock(record, j);
			if (RelFileLocatorEquals(prior->rlocator, block->rlocator) && prior->blkno == block->blkno)
				goto refused;
		}
		/* Cold begin already checked SPACE before taking any DATA lock. The
		 * published decision must precede every direct read, including SKIP. */
		if (!cluster_cold_redo_block_decision_v1(record, i, &decision))
			goto refused;
		if (cold) {
			if (decision.action == CLUSTER_COLD_REDO_SKIP) {
				skip[i] = true;
				continue;
			}
			if (decision.action != CLUSTER_COLD_REDO_APPLY
				|| decision.result.mutation_token != edge->result_token
				|| memcmp(entry->before.segment_incarnation, decision.result.segment_incarnation,
						  16)
					   != 0
				|| memcmp(entry->result_incarnation, decision.result.segment_incarnation, 16) != 0)
				goto refused;
		} else if (decision.action != CLUSTER_COLD_REDO_NATIVE
				   || !cluster_space_relation_read_redo_identity(block->rlocator, &identity)
				   || memcmp(entry->before.segment_incarnation, identity.incarnation, 16) != 0
				   || memcmp(entry->result_incarnation, identity.incarnation, 16) != 0)
			goto refused;
		if (has_image)
		{
			if (!XLogRecHasBlockImage(record, i) || !RestoreBlockImage(record, i, images[i].data) ||
				!genericVersionPageValid(images[i].data) ||
				((PageHeader) images[i].data)->pd_block_scn != edge->result_token)
				goto refused;
		}
		else
		{
			delta = XLogRecGetBlockData(record, i, &length);
			if (!genericDeltaValid(delta, length))
				goto refused;
		}
	}

	failure = "predecessor or result bytes";
	for (i = 0; i < count; i++)
	{
		DecodedBkpBlock *block = XLogRecGetBlock(record, i);
		Page page;

		if (cold) {
			XLogRedoAction action = XLogReadBufferForRedo(record, i, &buffers[i]);
			if (skip[i]) {
				if (action != BLK_NOTFOUND || BufferIsValid(buffers[i]))
					goto refused;
				continue;
			}
			if (!BufferIsValid(buffers[i]))
				goto refused;
			if (action == BLK_RESTORED)
				continue;
			if (action != BLK_NEEDS_REDO)
				goto refused;
		} else {
			buffers[i] = XLogReadBufferExtended(block->rlocator, MAIN_FORKNUM, block->blkno,
												RBM_NORMAL, InvalidBuffer);
			if (!BufferIsValid(buffers[i]))
				goto refused;
			LockBuffer(buffers[i], BUFFER_LOCK_EXCLUSIVE);
		}
		page = BufferGetPage(buffers[i]);
		if (!genericVersionPageValid(page) ||
			(((PageHeader) page)->pd_block_scn != edge->entries[i].before.mutation_token &&
			 ((PageHeader) page)->pd_block_scn != edge->result_token))
			goto refused;
		if (!XLogRecBlockImageApply(record, i))
		{
			Size length;
			char *delta = XLogRecGetBlockData(record, i, &length);
			memcpy(images[i].data, page, BLCKSZ);
			applyPageRedo(images[i].data, delta, length);
		}
		if (!genericVersionPageValid(images[i].data) ||
			((PageHeader) images[i].data)->pd_block_scn != edge->result_token)
			goto refused;
		apply[i] = cold || ((PageHeader)page)->pd_block_scn != edge->result_token;
		if (!apply[i] && !genericResultMatches(page, images[i].data))
			goto refused;
	}

	/* Native shared replay validates all predecessors before changing pages.
	 * Cold FPI blocks were restored by the exact reader; all WAL payloads were
	 * checked before I/O, and any remaining deltas are now ready to publish. */
	START_CRIT_SECTION();
	for (i = 0; i < count; i++)
	{
		Page page;
		PageHeader header;
		if (!apply[i])
			continue;
		page = BufferGetPage(buffers[i]);
		header = (PageHeader) images[i].data;
		memcpy(page, images[i].data, BLCKSZ);
		memset(page + header->pd_lower, 0, header->pd_upper - header->pd_lower);
		PageSetLSN(page, record->EndRecPtr);
		((PageHeader) page)->pd_block_scn = edge->result_token;
		MarkBufferDirty(buffers[i]);
	}
	END_CRIT_SECTION();
	for (i = 0; i < count; i++)
		if (BufferIsValid(buffers[i]))
			UnlockReleaseBuffer(buffers[i]);
	return;

refused:
	for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		if (BufferIsValid(buffers[i]))
			UnlockReleaseBuffer(buffers[i]);
	elog(ERROR, "shared generic WAL replay refused: %s", failure);
}
#endif

void
generic_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buffers[MAX_GENERIC_XLOG_PAGES];
	uint8		block_id;

#ifdef USE_PGRAC_CLUSTER
	if (cluster_shared_config)
	{
		genericVersionedRedo(record);
		return;
	}
#endif

	/* Protect limited size of buffers[] array */
	Assert(XLogRecMaxBlockId(record) < MAX_GENERIC_XLOG_PAGES);

	/* Iterate over blocks */
	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
	{
		XLogRedoAction action;

		if (!XLogRecHasBlockRef(record, block_id))
		{
			buffers[block_id] = InvalidBuffer;
			continue;
		}

		action = XLogReadBufferForRedo(record, block_id, &buffers[block_id]);

		/* Apply redo to given block if needed */
		if (action == BLK_NEEDS_REDO)
		{
			Page		page;
			PageHeader	pageHeader;
			char	   *blockDelta;
			Size		blockDeltaSize;

			page = BufferGetPage(buffers[block_id]);
			blockDelta = XLogRecGetBlockData(record, block_id, &blockDeltaSize);
			applyPageRedo(page, blockDelta, blockDeltaSize);

			/*
			 * Since the delta contains no information about what's in the
			 * "hole" between pd_lower and pd_upper, set that to zero to
			 * ensure we produce the same page state that application of the
			 * logged action by GenericXLogFinish did.
			 */
			pageHeader = (PageHeader) page;
			memset(page + pageHeader->pd_lower, 0,
				   pageHeader->pd_upper - pageHeader->pd_lower);

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffers[block_id]);
		}
	}

	/* Changes are done: unlock and release all buffers */
	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
	{
		if (BufferIsValid(buffers[block_id]))
			UnlockReleaseBuffer(buffers[block_id]);
	}
}

/*
 * Mask a generic page before performing consistency checks on it.
 */
void
generic_mask(char *page, BlockNumber blkno)
{
	mask_page_lsn_and_checksum(page);

	mask_unused_space(page);
}
