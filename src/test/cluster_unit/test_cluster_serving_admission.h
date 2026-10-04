/* Exercise final SQL admission independently of the original semantic gates.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
test_serving_semantic_open(bool restart)
{
	test_first_open_reset();
	if (restart)
		pg_atomic_write_u32(&NormalStartCompletion->state, CLUSTER_NORMAL_START_TARGET_READY);
	test_gate_publish(4, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		| CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1, 6, test_current_epoch, false);
	test_resource_x_gate_snapshot_valid = true;
	test_resource_x_gate_snapshot.phase = RESOURCE_X_GATE_OPEN;
	test_resource_x_gate_snapshot.formation = 2;
	test_resource_x_gate_snapshot.freeze_generation = 1;
}

UT_TEST(test_first_start_sql_waits_for_root_serving)
{
	ClusterSemanticActivationRefusal refusal;
	test_serving_semantic_open(false);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT(refusal.result != CLUSTER_SEMANTIC_ACTIVATION_OK);
	test_first_open_finish();
}

UT_TEST(test_clean_restart_sql_waits_for_root_serving)
{
	ClusterSemanticActivationRefusal refusal;
	test_serving_semantic_open(true);
	UT_ASSERT(!cluster_semantic_activation_startup_poll(&refusal));
	UT_ASSERT(refusal.result != CLUSTER_SEMANTIC_ACTIVATION_OK);
	test_first_open_finish();
}
