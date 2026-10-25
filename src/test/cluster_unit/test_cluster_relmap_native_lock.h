#ifndef TEST_CLUSTER_RELMAP_NATIVE_LOCK_H
#define TEST_CLUSTER_RELMAP_NATIVE_LOCK_H

typedef struct TestRelmapNativeLockConfig
{
	int acquire_result;
	int promote_result;
	int release_result;
	bool release_error;
	void (*before_release)(void);
} TestRelmapNativeLockConfig;

typedef struct TestRelmapNativeLockStats
{
	unsigned acquires;
	unsigned promotions;
	unsigned releases;
	bool exact_request;
	bool held_at_release;
	uint64 request_id;
} TestRelmapNativeLockStats;

extern void test_relmap_native_lock_reset(const TestRelmapNativeLockConfig *config);
extern void test_relmap_native_lock_stats(TestRelmapNativeLockStats *out);
extern bool test_relmap_native_lock_acquire(void);
extern bool test_relmap_native_lock_held(void);
extern void test_relmap_native_lock_release(void);

#endif
