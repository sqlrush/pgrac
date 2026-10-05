/* Standalone storage fixtures have no original KO work. Reject the unused
 * owner boundary; test_cluster_drop_work drives the real implementation.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_ko.h"
#include "storage/fd.h"

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
