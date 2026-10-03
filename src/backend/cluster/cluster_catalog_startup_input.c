/* Postmaster-local input source owned by the shared bootstrap path.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "cluster/cluster_catalog_startup.h"
#include "miscadmin.h"

static ClusterCatalogStartupSource startup_source;

bool
cluster_catalog_startup_set_source(const ClusterCatalogStartupSource *source)
{
	if (IsUnderPostmaster)
		return false;
	memset(&startup_source, 0, sizeof(startup_source));
	if (source == NULL || source->read == NULL || source->current == NULL
		|| source->release == NULL)
		return false;
	startup_source = *source;
	return true;
}

bool
cluster_catalog_startup_shared_verify(void)
{
	ClusterCatalogStartupSource source = startup_source;
	ClusterCatalogStartupInput input = { 0 };
	bool valid;

	if (IsUnderPostmaster || source.read == NULL || source.current == NULL
		|| source.release == NULL)
		return false;
	valid = source.read(&input, source.arg) && cluster_catalog_startup_validate(&input)
			&& source.current(source.arg);
	source.release(source.arg);
	return valid;
}
