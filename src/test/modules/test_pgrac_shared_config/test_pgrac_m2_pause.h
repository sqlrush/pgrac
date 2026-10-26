/* Test-module state; never linked into postgres. */
#ifndef TEST_PGRAC_M2_PAUSE_H
#define TEST_PGRAC_M2_PAUSE_H
#include "port/atomics.h"

#if defined(USE_PGRAC_CLUSTER) && defined(USE_ASSERT_CHECKING)
typedef struct TestM2PauseState {
	pg_atomic_uint64 lease;
	pg_atomic_uint64 generation;
	pg_atomic_uint32 node;
	pg_atomic_uint32 caller;
	pg_atomic_uint32 xid;
	pg_atomic_uint32 duration;
} TestM2PauseState;

extern void test_m2_pause_state_init(TestM2PauseState *state);
extern uint64 test_m2_pause_arm(TestM2PauseState *state, int node, uint32 caller, TransactionId xid,
								uint32 duration);
extern uint32 test_m2_pause_claim(TestM2PauseState *state, int node, uint32 caller,
								  TransactionId xid, uint64 *lease);
extern bool test_m2_pause_clear(TestM2PauseState *state, uint64 lease, bool *consumed);
extern void test_pgrac_m2_pause_init(void);
#endif
#endif
