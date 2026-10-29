/* Join the unchanged native PAGE-WAL owner and complete original fixture.
 * Only TAP bookkeeping is shared with the Startup translation unit.
 * Buffer/content ownership, output-completion state and WAL remain controlled.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_startup_first_wal_namespace.h"
#include "unit_test.h"
#undef UT_DEFINE_GLOBALS
#define UT_DEFINE_GLOBALS() /* Original Startup owns the three TAP globals. */
int main(void);
#include "test_cluster_page_wal.c"
#include "../../backend/cluster/cluster_page_wal.c"
#include "test_cluster_startup_first_wal.h"

static ClusterPageWalRefV1 startup_native_first;

void
test_startup_first_native_begin(const ClusterWalSourceRef *source, unsigned mode)
{
	UT_ASSERT(source != NULL && mode <= 2);
	reset();
	writer = *source;
	if (mode == 1) writer.claim.identity.origin_owner_incarnation++;
	if (mode == 2) selected = false;
	if (mode == 2) {
		UT_ASSERT_EQ(cluster_page_wal_capture_native_v1(1, &edge, 80,
			0x120, 0x200, 0x9192, RM_HEAP_ID, 0), CLUSTER_PAGE_WAL_UNATTRIBUTED);
	} else UT_ASSERT(capture_at(80, 0x120, 0x200));
	UT_ASSERT_EQ(observe_first(&startup_native_first), mode == 2
		? CLUSTER_PAGE_WAL_FIRST_UNATTRIBUTED : CLUSTER_PAGE_WAL_FIRST_PRESENT);
}

bool
test_startup_first_native_scan(const ClusterWalSourceRef *source,
	ClusterPageWalDirtyFloorV1 *out)
{
	return cluster_page_wal_dirty_floor_v1(source, out);
}

bool
test_startup_first_native_clear(unsigned fault)
{
	ClusterPageWalRefV1 observed = startup_native_first;

	UT_ASSERT(fault <= 6);
	if (fault == 4) observed.start++;
	return clear_written(&observed, fault == 3 ? 79 : 80,
		fault == 1 ? BM_DIRTY : fault == 2 ? BM_JUST_DIRTIED
		: fault == 5 ? BM_IO_IN_PROGRESS : fault == 6 ? BM_IO_ERROR : 0);
}

void
test_startup_first_native_recapture(void)
{
	UT_ASSERT(capture_at(81, 0x220, 0x300));
}

unsigned
test_startup_first_native_references(void)
{
	unsigned count = 0;

	for (unsigned i = 0; i < PAGE_WAL_SOURCE_SLOTS; i++)
		count += page_wal_shared->sources[i].references;
	return count;
}

void
test_startup_first_native_end(void)
{
	/* Disposable fixture allocation only; no production/shared debt reset. */
	free(shared_memory);
	shared_memory = NULL;
	page_wal_shared = NULL;
}

BufferDescPadded *
test_startup_first_native_residency(ClusterSpaceIdentity *identity,
	RfPageVersionV1 *version, char *image)
{
	UT_ASSERT(identity != NULL && version != NULL && image != NULL);
	*identity = space;
	memset(version, 0, sizeof(*version));
	memcpy(version->segment_incarnation, space.incarnation, 16);
	version->mutation_token = ((PageHeader)page.data)->pd_block_scn;
	memcpy(image, page.data, BLCKSZ);
	return BufferDescriptors;
}

ClusterPageWalFirstResultV1
test_startup_first_native_observe(BufferDesc *buf, ClusterPageWalRefV1 *out)
{
	UT_ASSERT(buf == &desc.bufferdesc);
	return cluster_page_wal_first_observe_locked_v1(buf, out);
}

void
test_startup_first_native_reuse(BufferDesc *buf)
{
	UT_ASSERT(buf == &desc.bufferdesc);
	cluster_page_wal_reset_reuse_locked(buf);
}
