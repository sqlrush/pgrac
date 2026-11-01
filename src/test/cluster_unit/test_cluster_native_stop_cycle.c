/* Observe original stop gates inside a real two-transfer ownership cycle.
 * Terminal acceptance is deliberately not inferred from these safety cuts.
 * Author: SqlRush <sqlrush@gmail.com> */
static void ns_cut(unsigned kind);
#include "test_cluster_native_stop_cycle.inc"
#include "test_cluster_native_stop_census.h"
#if defined(NATIVE_STOP_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "stop-census assertion-OFF requires an assertion-disabled configuration"
#endif

enum { NS_LIVE, NS_PARK, NS_ACK, NS_ACK_SENT, NS_CUT_COUNT };
static unsigned ns_seen[NS_CUT_COUNT];
static const char *ns_names[] = { "master-live", "holder-parked", "holder-ACK-unoffered", "holder-ACK-callback" };

static void ns_cut(unsigned kind)
{
	UT_ASSERT(kind < NS_CUT_COUNT); if (kind >= NS_CUT_COUNT) return;
	ns_seen[kind]++;
	UT_ASSERT_EQ(ns_pcm(ns_names[kind]), CLUSTER_NORMAL_STOP_PENDING);
	if (kind == NS_PARK || kind == NS_ACK || kind == NS_ACK_SENT)
		ns_buffer_pending(ns_names[kind]);
}
static bool ns_master(int reader, int writer, int mode)
{
	NM_REQUIRE(hc_master(reader, writer, mode));
	UT_ASSERT_EQ(ns_seen[NS_LIVE], 2);
	UT_ASSERT_EQ(ns_pcm("master-settled"), CLUSTER_NORMAL_STOP_READY);
	return true;
}
static bool ns_reader(int master, int writer, int mode, int source, uint64 boot)
{
	NM_REQUIRE(hc_reader(master, writer, mode, source, boot));
	UT_ASSERT_EQ(ns_seen[NS_PARK], 1); UT_ASSERT_EQ(ns_seen[NS_ACK], 1);
	UT_ASSERT_EQ(ns_seen[NS_ACK_SENT], 1);
	/* Diagnostic, not an assertion of a valid terminal stop: H30 retains a
	 * separate expected-READY probe until the full terminal contract closes. */
	(void)ns_pcm("reader-settled-terminal-diagnostic");
	return true;
}
static bool ns_writer(int master, int reader, int mode)
{
	NM_REQUIRE(hc_writer(master, reader, mode));
	UT_ASSERT_EQ(ns_seen[NS_PARK], 1); UT_ASSERT_EQ(ns_seen[NS_ACK], 1);
	UT_ASSERT_EQ(ns_seen[NS_ACK_SENT], 1);
	(void)ns_pcm("writer-settled-terminal-diagnostic");
	return true;
}
static void ns_run(int order, bool error)
{
	hc_order = order; hc_inject_error = error;
	hc_releases = hc_errors = hc_drains = hc_seed_calls = 0;
	memset(hc_locks, 0, sizeof(hc_locks)); memset(hc_owners, 0, sizeof(hc_owners));
	memset(ns_seen, 0, sizeof(ns_seen));
	nt_master_role = ns_master; nt_holder_role = ns_writer; nt_requester = ns_reader;
	nt_run(5);
}
UT_TEST(test_stop_observes_forward_reader_drain_and_unoffered_ACK) { ns_run(0, false); }
UT_TEST(test_stop_observes_reverse_reader_drain_and_unoffered_ACK) { ns_run(1, false); }
UT_TEST(test_stop_observes_forward_reader_ERROR_without_clearing_debt) { ns_run(0, true); }
UT_TEST(test_stop_observes_reverse_reader_ERROR_without_clearing_debt) { ns_run(1, true); }
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(4);
	UT_RUN(test_stop_observes_forward_reader_drain_and_unoffered_ACK);
	UT_RUN(test_stop_observes_reverse_reader_drain_and_unoffered_ACK);
	UT_RUN(test_stop_observes_forward_reader_ERROR_without_clearing_debt);
	UT_RUN(test_stop_observes_reverse_reader_ERROR_without_clearing_debt);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
