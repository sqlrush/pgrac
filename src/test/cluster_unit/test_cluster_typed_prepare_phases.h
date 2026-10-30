/* Whole prepare driver, with deterministic physical-source/clock leaves.
 * The actual CF/owner path is covered separately by prepare_budget tests.
 * Author: SqlRush <sqlrush@gmail.com> */
enum PreparePhase {
	PREPARE_CHECKPOINT = 1,
	PREPARE_CENSUS,
	PREPARE_SCAN,
	PREPARE_BEFORE_SEAL,
	PREPARE_AFTER_SEAL,
	PREPARE_PROFILE
};
static bool prepare_probe_active, prepare_probe_census_read;
static unsigned prepare_probe_expire, prepare_probe_checkpoints, prepare_probe_seen;
static uint64 prepare_probe_now, prepare_probe_deadline;
static int prepare_probe_code;
static ClusterColdTypedV1 *prepare_probe_typed;
static sigjmp_buf prepare_probe_exception;

int
clock_gettime(clockid_t clock, struct timespec *out)
{
	UT_ASSERT_EQ(clock, CLOCK_MONOTONIC);
	out->tv_sec = prepare_probe_now / UINT64CONST(1000000000);
	out->tv_nsec = prepare_probe_now % UINT64CONST(1000000000);
	return 0;
}

static void
prepare_probe_leaf(unsigned phase)
{
	if (!prepare_probe_active)
		return;
	prepare_probe_seen |= 1U << phase;
	if (phase == prepare_probe_expire)
		prepare_probe_now = prepare_probe_deadline;
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	if (!prepare_probe_active || level != FATAL)
		abort();
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errmsg(const char *format pg_attribute_unused(), ...)
{
	if (!prepare_probe_active)
		abort();
	return 0;
}
int
errcode(int code)
{
	prepare_probe_code = code;
	return 0;
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	if (!prepare_probe_active)
		abort();
	siglongjmp(prepare_probe_exception, 1);
}
