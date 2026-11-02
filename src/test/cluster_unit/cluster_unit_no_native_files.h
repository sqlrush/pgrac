/* Legacy-only storage fixtures. No selected native table exists here;
 * accidentally reaching a managed acquire/current/release must fail loudly. */
#ifndef CLUSTER_UNIT_NO_NATIVE_FILES_H
#define CLUSTER_UNIT_NO_NATIVE_FILES_H
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_guc.h"
#ifndef CLUSTER_UNIT_NATIVE_GUC_PROVIDED
bool cluster_enabled = false;
#endif
ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void)
{ return CLUSTER_NATIVE_FILES_UNKNOWN_V2; }
bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
	ClusterControlNativeFileBorrowV2 *borrow)
{ (void) key; (void) borrow; abort(); }
bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *borrow)
{ (void) borrow; abort(); }
bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *borrow)
{ (void) borrow; abort(); }
ClusterControlNativeResultV2 cluster_control_native_files_inventory_v2(
	Size budget, ClusterControlNativeInventoryV2 **fresh)
{ (void) budget; (void) fresh; abort(); }
bool cluster_control_native_files_inventory_current_v2(
	const ClusterControlNativeInventoryV2 *inventory)
{ (void) inventory; abort(); }
void cluster_control_native_inventory_destroy_v2(ClusterControlNativeInventoryV2 **inventory)
{ (void) inventory; abort(); }
#endif
