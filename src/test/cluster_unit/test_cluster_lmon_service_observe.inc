/* Complete original LMON point fanout. Original module return values are
 * explicit host boundaries, not native producer/transport/closure proofs.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#define SHARED_POLL(name, index) \
	ClusterNormalStopPollResult name(const char **reason) \
	{ return answer(index, reason); }
SHARED_POLL(cluster_pi_data_normal_stop_poll_v1, SHARED_PI_DATA)
SHARED_POLL(cluster_wal_cut_normal_stop_poll_v1, SHARED_WAL_CUT)
SHARED_POLL(cluster_pi_writeback_normal_stop_poll_v1, SHARED_PI_WRITEBACK)
SHARED_POLL(cluster_ko_shared_normal_stop_poll_v2, SHARED_KO)
#undef SHARED_POLL

static const char *const shared_domains[] = { "current-holder-data", "native-wal-cut",
	"physical-pi-writeback", "shared-object-flush" };
#define ALL_LMON_OWNERS (roles[0].mask | BIT(SHARED_PI_DATA) | BIT(SHARED_WAL_CUT) \
	| BIT(SHARED_PI_WRITEBACK) | BIT(SHARED_KO))

UT_TEST(lmon_shared_pending_is_owned_until_original_owner_returns)
{
	for (unsigned i = SHARED_PI_DATA; i <= SHARED_KO; i++) {
		ClusterServiceObservation out;
		ClusterNormalStopPollResult before[MODULES];
		reset(B_LMON, LmonProcess);
		answers[i] = CLUSTER_NORMAL_STOP_PENDING;
		memcpy(before, answers, sizeof(before));
		for (unsigned repeat = 0; repeat < 3; repeat++) {
			seen = calls = 0;
			UT_ASSERT_EQ(cluster_lmon_service_observe(&out), CLUSTER_NORMAL_STOP_PENDING);
			UT_ASSERT_EQ(calls, 25);
			UT_ASSERT_EQ(out.modules, 25);
			UT_ASSERT_EQ(seen, ALL_LMON_OWNERS);
			UT_ASSERT(strcmp(out.domain, shared_domains[i - SHARED_PI_DATA]) == 0);
			UT_ASSERT(strcmp(out.reason, "fixture original owner") == 0);
			UT_ASSERT_EQ(memcmp(before, answers, sizeof(before)), 0);
			UT_ASSERT_EQ(stop_close_calls, 0);
		}
		/* Only the original owner changes its return; observation did not. */
		answers[i] = CLUSTER_NORMAL_STOP_READY;
		seen = calls = 0;
		UT_ASSERT_EQ(cluster_lmon_service_observe(&out), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(seen, ALL_LMON_OWNERS);
	}
}

UT_TEST(lmon_shared_scan_preserves_first_invalid_and_visits_all)
{
	for (unsigned i = SHARED_PI_DATA; i <= SHARED_KO; i++)
		for (unsigned unknown = 0; unknown < 2; unknown++) {
			ClusterServiceObservation out;
			reset(B_LMON, LmonProcess);
			answers[CR] = CLUSTER_NORMAL_STOP_PENDING;
			answers[i] = unknown ? (ClusterNormalStopPollResult)73 : CLUSTER_NORMAL_STOP_INVALID;
			UT_ASSERT_EQ(cluster_lmon_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
			UT_ASSERT_EQ(seen, ALL_LMON_OWNERS);
			UT_ASSERT_EQ(calls, 25);
			UT_ASSERT_EQ(out.modules, 25);
			UT_ASSERT(strcmp(out.domain, shared_domains[i - SHARED_PI_DATA]) == 0);
		}
	for (unsigned first = 0; first < 2; first++) {
		ClusterServiceObservation out;
		reset(B_LMON, LmonProcess);
		answers[SHARED_PI_DATA] = CLUSTER_NORMAL_STOP_PENDING;
		answers[SHARED_WAL_CUT] = CLUSTER_NORMAL_STOP_INVALID;
		answers[SHARED_KO] = CLUSTER_NORMAL_STOP_INVALID;
		if (first)
			answers[CR] = CLUSTER_NORMAL_STOP_INVALID;
		UT_ASSERT_EQ(cluster_lmon_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(seen, ALL_LMON_OWNERS);
		UT_ASSERT(strcmp(out.domain, first ? "CR" : "native-wal-cut") == 0);
	}
}

UT_TEST(lmon_complete_point_keeps_original_entry_fanouts)
{
	ClusterServiceObservation out;
	reset(B_LMON, LmonProcess);
	UT_ASSERT_EQ(cluster_lmon_service_observe(&out), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(seen, ALL_LMON_OWNERS);
	UT_ASSERT_EQ(calls, 25);
	UT_ASSERT_EQ(out.modules, 25);
	UT_ASSERT(strcmp(out.reason, "NONE") == 0);
	UT_ASSERT_EQ(stop_close_calls, 0);
	UT_ASSERT_EQ(online_close_calls, 1);
	/* The original private-only API retains its exact original semantics. */
	seen = calls = 0;
	answers[SHARED_PI_DATA] = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(cluster_service_observe(&out), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(seen, roles[0].mask);
	UT_ASSERT_EQ(out.modules, 21);
	seen = calls = 0;
	UT_ASSERT_EQ(cluster_service_normal_stop_observe(&out), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(seen, roles[0].mask);
	UT_ASSERT_EQ(stop_close_calls, 1);
}

UT_TEST(lmon_complete_point_rejects_wrong_owner_before_any_poll)
{
	for (unsigned r = 1; r < lengthof(roles); r++) {
		ClusterServiceObservation out;
		reset(roles[r].role, roles[r].aux);
		UT_ASSERT_EQ(cluster_lmon_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(calls, 0);
	}
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterServiceObservation out;
		reset(B_LMON, LmonProcess);
		switch (fault) {
		case 0: IsUnderPostmaster = false; break;
		case 1: MyBackendType = B_BACKEND; break;
		case 2: MyAuxProcType = NotAnAuxProcess; break;
		case 3: cluster_shared_config = false; break;
		}
		UT_ASSERT_EQ(cluster_lmon_service_observe(&out), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(calls, 0);
		UT_ASSERT_EQ(out.modules, 0);
	}
	reset(B_LMON, LmonProcess);
	UT_ASSERT_EQ(cluster_lmon_service_observe(NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(calls, 0);
}
