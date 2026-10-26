#include "postgres.h"
#include "test_pgrac_m2_pause.h"
#if defined(USE_PGRAC_CLUSTER) && defined(USE_ASSERT_CHECKING)
void
test_m2_pause_state_init(TestM2PauseState *state)
{
	pg_atomic_init_u64(&state->lease, 0);
	pg_atomic_init_u64(&state->generation, 0);
	pg_atomic_init_u32(&state->node, 0);
	pg_atomic_init_u32(&state->caller, 0);
	pg_atomic_init_u32(&state->xid, 0);
	pg_atomic_init_u32(&state->duration, 0);
}
uint64
test_m2_pause_arm(TestM2PauseState *state, int node, uint32 caller, TransactionId xid,
				  uint32 duration)
{
	uint64 expected = 0;
	uint64 generation;
	uint64 lease;

	if (node < 0 || node > 127 || caller < 1 || caller > 2 || xid < 3 || duration == 0
		|| duration > 60000000)
		return 0;
	/* WRITING is never claimable; only this publisher owns the slot. */
	if (!pg_atomic_compare_exchange_u64(&state->lease, &expected, 1))
		return 0;
	generation = pg_atomic_read_u64(&state->generation);
	if (generation >= (UINT64_MAX >> 2)) {
		pg_atomic_write_u64(&state->lease, 0);
		return 0;
	}
	pg_atomic_write_u64(&state->generation, ++generation);
	pg_atomic_write_u32(&state->node, node);
	pg_atomic_write_u32(&state->caller, caller);
	pg_atomic_write_u32(&state->xid, xid);
	pg_atomic_write_u32(&state->duration, duration);
	lease = (generation << 2) | 2;
	pg_write_barrier();
	pg_atomic_write_u64(&state->lease, lease);
	return lease;
}
uint32
test_m2_pause_claim(TestM2PauseState *state, int node, uint32 caller, TransactionId xid,
					uint64 *lease)
{
	uint64 expected = pg_atomic_read_u64(&state->lease);
	uint32 duration;

	*lease = 0;
	pg_read_barrier();
	if ((expected & 3) != 2 || pg_atomic_read_u32(&state->node) != node
		|| pg_atomic_read_u32(&state->caller) != caller || pg_atomic_read_u32(&state->xid) != xid)
		return 0;
	duration = pg_atomic_read_u32(&state->duration);
	/* A concurrent disarm/rearm changes the generation, so cannot be claimed
	 * using fields sampled from a previous or partly published target. */
	if (!pg_atomic_compare_exchange_u64(&state->lease, &expected, expected | 1))
		return 0;
	*lease = expected;
	return duration;
}
bool
test_m2_pause_clear(TestM2PauseState *state, uint64 lease, bool *consumed)
{
	uint64 expected = pg_atomic_read_u64(&state->lease);

	*consumed = false;
	while (lease >= 4 && (expected >> 2) == (lease >> 2)) {
		uint64 sampled = expected;

		if (pg_atomic_compare_exchange_u64(&state->lease, &expected, 0)) {
			*consumed = (sampled & 3) == 3;
			return true;
		}
	}
	return false;
}
#endif
