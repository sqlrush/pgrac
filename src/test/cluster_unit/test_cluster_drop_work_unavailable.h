/* Standalone storage fixtures have no original KO work. Reject the unused
 * owner boundary; test_cluster_drop_work drives the real implementation.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_ko.h"
#include "storage/fd.h"
#include "miscadmin.h"

#ifndef CLUSTER_EXTENSION_TARGET_FIXTURE
bool
cluster_ko_extend_recovery_target_v3(RelFileLocator locator pg_attribute_unused(),
								   ForkNumber fork pg_attribute_unused(),
								   BlockNumber block pg_attribute_unused())
{
	return false;
}
#endif

static uint32 extension_wait_event;
uint32 *my_wait_event_info = &extension_wait_event;
volatile sig_atomic_t InterruptPending;

void
ProcessInterrupts(void)
{
	abort();
}

int
FileGetRawDesc(File file)
{
	return (int)file;
}

/* No original work can be acquired here, so its trace boundary is unexpected. */
void
cluster_ko_drop_work_note_v1(ClusterKoDropWorkV2 *work pg_attribute_unused(),
							  uint32 phase pg_attribute_unused(), uint32 step pg_attribute_unused(),
							  ForkNumber fork pg_attribute_unused(), bool io_attempted pg_attribute_unused(),
							  int io_error pg_attribute_unused())
{
	abort();
}

bool
cluster_ko_shared_drop_work_begin_v2(uint32 *cursor pg_attribute_unused(),
									 Size size pg_attribute_unused(),
									 ClusterKoDropWorkV2 **out pg_attribute_unused())
{
	return false;
}
void *
cluster_ko_shared_drop_work_state_v2(const ClusterKoDropWorkV2 *work pg_attribute_unused(),
									 Size size pg_attribute_unused())
{
	return NULL;
}
bool
cluster_ko_shared_drop_work_read_v2(const ClusterKoDropWorkV2 *work pg_attribute_unused(),
									struct ClusterPageWalBindingV1 *terminal pg_attribute_unused(),
									void *wal pg_attribute_unused(),
									Size length pg_attribute_unused())
{
	return false;
}
bool
cluster_ko_shared_drop_work_revalidate_v2(const ClusterKoDropWorkV2 *work pg_attribute_unused())
{
	return false;
}
bool
cluster_ko_shared_drop_work_finish_v2(ClusterKoDropWorkV2 **work pg_attribute_unused())
{
	return false;
}
void *
cluster_ko_shared_drop_work_abandon_v2(ClusterKoDropWorkV2 *work pg_attribute_unused(),
									   Size size pg_attribute_unused())
{
	return NULL;
}
bool
cluster_ko_shared_drop_work_abandon_next_v2(uint32 *cursor pg_attribute_unused(),
											Size size pg_attribute_unused(),
											ClusterKoDropWorkV2 **out pg_attribute_unused())
{
	return false;
}
bool
AcquireExternalFD(void)
{
	return false;
}
void
ReleaseExternalFD(void)
{
	abort();
}
bool
cluster_ko_shared_native_drop_deferred_v2(RelFileLocator locator pg_attribute_unused())
{
	return false;
}
