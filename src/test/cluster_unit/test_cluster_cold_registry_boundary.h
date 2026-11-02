/* Registry/CF are explicit leaves of the original Startup driver fixture.
 * Default is exact legacy ROOTv3. The independent real scanner test opts into
 * managed identities; real registry pair projection is a separate target. */
static bool registry_managed, registry_ok = true, registry_drift, registry_unlock_ok = true;
static bool registry_error;
static unsigned registry_publishes, registry_locks, registry_releases, registry_checks;
static uint64 registry_count;
static ClusterColdRelmapCreateExpectedV1 registry_entries[2];
static ClusterControlRelmapReferenceV2 registry_reference;
static unsigned registry_selection;
bool cluster_cf_lock(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	UT_ASSERT(!registry_cf_held);
	registry_locks++;
	registry_cf_held = true;
	return true;
}
bool cluster_cf_lock_deadline_v1(LOCKMODE mode, uint64 deadline)
{
	UT_ASSERT_EQ(deadline, prepare_probe_deadline);
	return cluster_cf_lock(mode);
}
bool cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return mode == ExclusiveLock && registry_cf_held;
}
ClusterCfReleaseResult cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	registry_releases++;
	if (registry_unlock_ok) registry_cf_held = false;
	return registry_unlock_ok ? CLUSTER_CF_RELEASE_CONFIRMED : CLUSTER_CF_RELEASE_UNCONFIRMED;
}
ClusterControlRootResult cluster_control_root_relmap_format_locked_v2(Size budget,
	ClusterControlRelmapFormatV2 *out)
{
	(void)budget;
	UT_ASSERT(registry_cf_held);
	memset(out, 0, sizeof(*out));
	out->root_version = registry_managed ? 4 : 3;
	out->config.identity.system_identifier = 9;
	out->config.identity.database_incarnation = 5;
	out->config.identity.generation = 1;
	memcpy(out->config.identity.storage_uuid, roots[1].identity.storage_uuid, 16);
	memcpy(out->config.identity.authority_uuid, roots[1].identity.authority_uuid, 16);
	registry_reference.config = out->config;
	if (registry_error) pg_re_throw();
	return registry_ok ? 0 : CLUSTER_CONTROL_ROOT_IO_ERROR;
}
ClusterControlRootResult cluster_control_root_relmap_selection_read_locked_v4(
	const ClusterSharedConfigRef *expected, Size budget, ClusterControlRelmapSelectionV2 **out)
{
	(void)expected; (void)budget;
	UT_ASSERT(registry_cf_held && *out == NULL);
	*out = (void *)&registry_selection;
	return registry_ok ? 0 : CLUSTER_CONTROL_ROOT_IO_ERROR;
}
bool cluster_control_root_relmap_selection_reference_v4(ClusterControlRelmapSelectionV2 *s,
	ClusterControlRelmapReferenceV2 *out)
{
	UT_ASSERT(s == (void *)&registry_selection && registry_cf_held);
	*out = registry_reference;
	return registry_ok;
}
bool cluster_control_root_relmap_creation_count_v4(ClusterControlRelmapSelectionV2 *s, uint64 *out)
{
	UT_ASSERT(s == (void *)&registry_selection && registry_cf_held);
	*out = registry_count;
	return registry_ok;
}
bool cluster_control_root_relmap_creation_at_v4(ClusterControlRelmapSelectionV2 *s, uint64 at,
	ClusterRecoveryFileIdentityV2 *id, uint8 batch[16])
{
	UT_ASSERT(s == (void *)&registry_selection && registry_cf_held && at < registry_count);
	*id = registry_entries[at].expected;
	memcpy(batch, registry_entries[at].batch_uuid, 16);
	return registry_ok;
}
ClusterControlRootResult cluster_control_root_relmap_selection_recheck_locked_v4(
	ClusterControlRelmapSelectionV2 *s)
{
	UT_ASSERT(s == (void *)&registry_selection && registry_cf_held);
	return registry_ok ? 0 : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}
void cluster_control_root_relmap_selection_destroy_v4(ClusterControlRelmapSelectionV2 **s)
{
	*s = NULL;
}
ClusterControlNativeResultV2 cluster_control_native_files_publish_locked_v2(
	const ClusterSharedConfigRef *config, Size budget, uint64 startup)
{
	(void)config; (void)budget; UT_ASSERT_EQ(startup, 1);
	UT_ASSERT(registry_cf_held);
	registry_publishes++;
	return registry_ok ? 0 : CLUSTER_NATIVE_FILES_REFUSED_V2;
}
ClusterControlNativeResultV2 cluster_control_native_files_inventory_v2(Size budget,
	ClusterControlNativeInventoryV2 **out)
{
	UT_ASSERT(registry_cf_held && *out == NULL && budget > sizeof(**out));
	/* Same explicit maximum as the real native inventory consumer. */
	if (budget > CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2)
		return CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2;
	*out = palloc0(sizeof(**out));
	(*out)->private_bytes = sizeof(**out);
	(*out)->info.mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	(*out)->info.table_generation = 17;
	(*out)->info.startup_generation = 1;
	(*out)->info.control = registry_reference;
	return registry_ok ? 0 : CLUSTER_NATIVE_FILES_REFUSED_V2;
}
bool cluster_control_native_files_inventory_current_v2(const ClusterControlNativeInventoryV2 *i)
{
	UT_ASSERT(i != NULL);
	registry_checks++;
	return registry_ok && !registry_drift;
}
void cluster_control_native_inventory_destroy_v2(ClusterControlNativeInventoryV2 **out)
{
	if (*out) pfree(*out);
	*out = NULL;
}
