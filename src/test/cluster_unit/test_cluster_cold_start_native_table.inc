/* Original Start wiring. CF/control selection/publication remain controlled
 * leaves; the separate native-table suite executes real ROOT/registry files. */
static bool start_table_cf_held, start_table_cf_share, start_table_installed;
static unsigned start_table_acquires, start_table_releases, start_table_formats;
static unsigned start_table_publishes, start_table_version, start_table_fault;
static uint64 start_table_boot;

static void
start_native_table_reset(void)
{
	start_table_cf_held = start_table_cf_share = start_table_installed = false;
	start_table_acquires = start_table_releases = start_table_formats = 0;
	start_table_publishes = start_table_fault = 0;
	start_table_version = 3; /* Original default keeps the legacy format. */
	start_table_boot = 0;
}

bool
cluster_cf_held(LOCKMODE mode)
{
	return mode == ExclusiveLock ? start_table_cf_held : start_table_cf_share;
}

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return cluster_cf_held(mode);
}

bool
cluster_cf_lock(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	UT_ASSERT(!start_table_cf_held && !start_table_cf_share);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT(!tx_ir_held && cluster_recovery_cold_start != NULL);
	UT_ASSERT(!window_region.checkpoint_owner.valid);
	start_table_acquires++;
	start_table_cf_held = start_table_fault != 1;
	return start_table_cf_held;
}

ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	start_table_releases++;
	if (start_table_fault == 13) pg_re_throw();
	if (start_table_fault == 8) return CLUSTER_CF_RELEASE_UNCONFIRMED;
	if (!start_table_cf_held) return CLUSTER_CF_RELEASE_NOT_HELD;
	start_table_cf_held = false;
	if (start_table_fault == 7) observed_writer.epoch++;
	return CLUSTER_CF_RELEASE_CONFIRMED;
}

ClusterControlRootResult
cluster_control_root_relmap_format_locked_v2(Size budget, ClusterControlRelmapFormatV2 *out)
{
	ClusterControlRelmapFormatV2 value = {0};
	UT_ASSERT(start_table_cf_held);
	UT_ASSERT_EQ(budget, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2);
	UT_ASSERT(!tx_ir_held);
	start_table_formats++;
	if (start_table_fault == 12) pg_re_throw();
	if (start_table_fault == 2) return CLUSTER_CONTROL_ROOT_IO_ERROR;
	value.root_version = start_table_fault == 3 ? 0 : start_table_version;
	value.config.identity.system_identifier = observed_writer.ref.claim.identity.system_identifier;
	value.config.identity.database_incarnation = observed_writer.ref.claim.database_incarnation;
	value.config.identity.generation = observed_writer.ref.claim.max_config_generation;
	memcpy(value.config.identity.storage_uuid, observed_writer.ref.claim.identity.storage_uuid, 16);
	memcpy(value.config.identity.authority_uuid, observed_writer.ref.claim.identity.authority_uuid, 16);
	value.config.identity.configured[0] = 1;
	value.config.sha256[0] = 31;
	value.root_sha256[0] = 41;
	if (start_table_fault == 5 && start_table_formats > 1) value.root_sha256[0]++;
	if (start_table_fault == 9) value.config.identity.authority_uuid[0]++;
	if (start_table_fault == 10) value.config.identity.generation--;
	*out = value;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlNativeResultV2
cluster_control_native_files_publish_locked_v2(const ClusterSharedConfigRef *config,
	Size budget, uint64 startup)
{
	UT_ASSERT(start_table_cf_held && !tx_ir_held);
	UT_ASSERT_EQ(start_table_formats, 1);
	UT_ASSERT_EQ(config->identity.system_identifier, GetSystemIdentifier());
	UT_ASSERT_EQ(budget, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2);
	UT_ASSERT_EQ(startup, owner_boot);
	UT_ASSERT(!window_region.checkpoint_owner.valid);
	start_table_publishes++;
	if (start_table_fault == 11) pg_re_throw();
	if (start_table_fault == 4) return CLUSTER_NATIVE_FILES_BUSY_V2;
	start_table_installed = true;
	start_table_boot = startup;
	if (start_table_fault == 6) observed_writer.epoch++;
	return CLUSTER_NATIVE_FILES_OK_V2;
}

UT_TEST(test_start_native_table_managed_publish_precedes_original_work)
{
	reset(); retain(); start_table_version = 4;
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(info.cluster_tx_start != NULL && ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(start_table_acquires, 1);
	UT_ASSERT_EQ(start_table_formats, 2);
	UT_ASSERT_EQ(start_table_publishes, 1);
	UT_ASSERT_EQ(start_table_releases, 1);
	UT_ASSERT(start_table_installed && start_table_boot == owner_boot);
	UT_ASSERT(!start_table_cf_held && !start_table_cf_share && !tx_ir_held);
	UT_ASSERT_EQ(scope_begins, 0);
	UT_ASSERT(window_region.checkpoint_owner.valid);
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT_EQ(start_table_publishes, 1);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_native_table_legacy_uses_explicit_control_format)
{
	reset(); retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT_EQ(start_table_formats, 2);
	UT_ASSERT_EQ(start_table_publishes, 0);
	UT_ASSERT_EQ(start_table_acquires, 1);
	UT_ASSERT_EQ(start_table_releases, 1);
	UT_ASSERT(!start_table_cf_held && !start_table_installed);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_native_table_refusal_preserves_original_input_custody)
{
	for (unsigned fault = 1; fault <= 10; fault++) {
		unsigned destroyed;
		reset(); retain(); start_table_version = 4; start_table_fault = fault;
		destroyed = input_destroys;
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT(info.cluster_tx_start == NULL && cluster_recovery_cold_start == NULL);
		UT_ASSERT(info.cluster_cold_inputs == &typed && cluster_recovery_cold_inputs == &typed);
		UT_ASSERT(!window_region.checkpoint_owner.valid);
		UT_ASSERT_EQ(input_destroys, destroyed);
		UT_ASSERT_EQ(scope_begins, 0);
		UT_ASSERT_EQ(start_table_acquires, 1);
		UT_ASSERT_EQ(start_table_releases, 1);
		UT_ASSERT_EQ(start_table_cf_held, fault == 8);
		UT_ASSERT_EQ(start_table_publishes, fault >= 4 && fault <= 8 ? 1 : 0);
		if (fault >= 5 && fault <= 8) UT_ASSERT(start_table_installed);
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT_EQ(start_table_acquires, 1);
	}
}

UT_TEST(test_start_native_table_error_never_publishes_start_or_retries_cleanup)
{
	for (unsigned fault = 11; fault <= 13; fault++) {
		volatile bool threw = false;
		unsigned destroyed;
		reset(); retain(); start_table_version = 4; start_table_fault = fault;
		destroyed = input_destroys;
		PG_TRY();
		{
			(void)ClusterBeginColdRecoveryTx(&info);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT(threw);
		UT_ASSERT(info.cluster_tx_start == NULL && cluster_recovery_cold_start == NULL);
		UT_ASSERT(info.cluster_cold_inputs == &typed && cluster_recovery_cold_inputs == &typed);
		UT_ASSERT(!window_region.checkpoint_owner.valid);
		UT_ASSERT_EQ(input_destroys, destroyed);
		UT_ASSERT_EQ(start_table_acquires, 1);
		UT_ASSERT_EQ(start_table_releases, 1);
		UT_ASSERT_EQ(start_table_cf_held, fault == 13);
		UT_ASSERT_EQ(start_table_publishes, fault == 12 ? 0 : 1);
		UT_ASSERT_EQ(start_table_installed, fault == 13);
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT_EQ(start_table_acquires, 1);
	}
}

UT_TEST(test_start_native_table_rejects_held_locks_before_cf_or_io)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		reset(); retain(); start_table_version = 4;
		switch (fault) {
		case 0: start_table_cf_held = true; break;
		case 1: start_table_cf_share = true; break;
		case 2: ctrc_driver_lwlocks = 1; break;
		case 3: CritSectionCount = 1; break;
		case 4: ShutdownRequestPending = true; break;
		}
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT(info.cluster_tx_start == NULL && cluster_recovery_cold_start == NULL);
		UT_ASSERT_EQ(start_table_acquires, 0);
		UT_ASSERT_EQ(start_table_formats, 0);
		UT_ASSERT_EQ(start_table_publishes, 0);
		UT_ASSERT_EQ(start_table_releases, 0);
		UT_ASSERT_EQ(start_table_cf_held, fault == 0);
		UT_ASSERT_EQ(start_table_cf_share, fault == 1);
		ctrc_driver_lwlocks = CritSectionCount = 0;
		ShutdownRequestPending = false;
	}
}

static void
start_native_table_tests(void)
{
	UT_RUN(test_start_native_table_managed_publish_precedes_original_work);
	UT_RUN(test_start_native_table_legacy_uses_explicit_control_format);
	UT_RUN(test_start_native_table_refusal_preserves_original_input_custody);
	UT_RUN(test_start_native_table_error_never_publishes_start_or_retries_cleanup);
	UT_RUN(test_start_native_table_rejects_held_locks_before_cf_or_io);
	reset();
}
