/* Original Aux caller cleanup storage. No wire or input-use authority.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAL_INPUTS_JOB_PRIVATE_H
#define CLUSTER_WAL_INPUTS_JOB_PRIVATE_H

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_wal_inputs.h"
#include "utils/resowner.h"

#define CLUSTER_WAL_INPUT_JOB_COUNT 4
#define CLUSTER_WAL_INPUT_JOB_CONTROL_RESERVE 2048

typedef enum ClusterWalInputsJobStateV1 {
	WAL_INPUT_JOB_STATE_NEW = 0,
	WAL_INPUT_JOB_STATE_BUILDING = 1,
	WAL_INPUT_JOB_STATE_USABLE = 2,
	WAL_INPUT_JOB_STATE_SUSPENDED = 3,
	WAL_INPUT_JOB_STATE_RETIRING_CF = 4,
	WAL_INPUT_JOB_STATE_RETIRING_WALR = 5,
	WAL_INPUT_JOB_STATE_DONE = 6
} ClusterWalInputsJobStateV1;

#define WAL_INPUT_JOB_FLAG_CANCELLED UINT16_C(0x1)
#define WAL_INPUT_JOB_FLAG_ERROR UINT16_C(0x2)
#define WAL_INPUT_JOB_FLAG_REVOKED UINT16_C(0x4)
#define WAL_INPUT_JOB_FLAGS UINT16_C(0x7)

struct ClusterWalInputsJobV1 {
	MemoryContext context;
	ClusterWalInputsV1 *inputs;
	ResourceOwner owner;
	ClusterWalReadPinV1 *pin;
	uint32 consumer, state;
	uint64 generation, pin_generation;
	int32 owner_pid;
	uint32 owner_procno;
	uint64 owner_proc_generation;
	ClusterCfJobReceiptV1 cf;
	uint16 walr_next, flags;
	uint32 original_result;
	uint8 reserved[8];
};

/* These mirrors are outside the borrowed allocation. Validate the locator
 * before dereferencing a caller's pointer, and never silently reparent it. */
typedef struct ClusterWalInputsJobSlotV1 {
	ClusterWalInputsJobV1 *job;
	MemoryContext parent;
	MemoryContext context;
	uint64 generation;
	uint32 state;
	uint16 flags, reserved;
} ClusterWalInputsJobSlotV1;

typedef struct ClusterWalInputsJobProcessV1 {
	uint64 generation;
	bool callback_registered;
} ClusterWalInputsJobProcessV1;

/* The standalone layout gate also checks the original ResourceOwner list
 * item's actual type against this envelope, rather than hiding its cost. */
typedef struct ClusterWalInputsJobCallbackBudgetV1 {
	void *next;
	ResourceReleaseCallback callback;
	void *argument;
} ClusterWalInputsJobCallbackBudgetV1;

/* Both original Aux processes own four fixed locators. The role guard
 * permits only two consumer classes per process, hence four live headers
 * across the pair. Include future caller/scope/pin back-links here too.
 * Each opaque CF envelope is separately checked against its real type. */
typedef struct ClusterWalInputsJobControlBudgetV1 {
	ClusterWalInputsJobV1 jobs[CLUSTER_WAL_INPUT_JOB_COUNT];
	ClusterWalInputsJobSlotV1 slots[2][CLUSTER_WAL_INPUT_JOB_COUNT];
	ClusterWalInputsJobProcessV1 processes[2];
	ClusterWalInputsJobCallbackBudgetV1 callbacks[2];
	ClusterWalInputsJobV1 *caller_links[CLUSTER_WAL_INPUT_JOB_COUNT];
	ClusterWalInputsJobV1 *scope_links[CLUSTER_WAL_INPUT_JOB_COUNT];
	ClusterWalInputsJobV1 *pin_links[2];
	/* Original pin/member padding carries these persistent native/retiring
	 * flags. Charge both Aux processes even though only one pin is active. */
	bool native_flags[2][CLUSTER_WAL_RETENTION_MAX_THREADS];
	bool pin_retiring[2];
	uint16 pin_cursors[2];
	uint8 cf_bindings[2][CLUSTER_CF_JOB_BINDING_MAX_BYTES];
} ClusterWalInputsJobControlBudgetV1;

/* Local binding checks never grant input authority. The original active
 * pin and its process generation independently witness the job backlink. */
extern bool cluster_wal_read_pin_job_binding_v1(ClusterWalInputsJobV1 *job,
	ClusterWalReadPinV1 *pin, uint64 generation, uint16 cursor);
extern bool cluster_wal_inputs_job_pin_bind_v1(ClusterWalInputsJobV1 *job,
	ClusterWalReadPinV1 *pin, uint64 generation);
extern bool cluster_wal_inputs_job_pin_matches_v1(ClusterWalInputsJobV1 *job,
	ClusterWalReadPinV1 *pin, uint64 generation, bool owner_cleanup);
extern bool cluster_wal_inputs_job_pin_detach_v1(ClusterWalInputsJobV1 *job,
	ClusterWalReadPinV1 *pin, uint64 generation);
extern bool cluster_wal_inputs_job_pin_cleanup_v1(ClusterWalInputsJobV1 *job,
	ClusterWalReadPinV1 *pin, uint64 generation, bool error, bool owner_cleanup);

StaticAssertDecl(CLUSTER_WAL_INPUT_JOB_COUNT == 4, "WAL input job slot count changed");
StaticAssertDecl(CLUSTER_WAL_RETENTION_MAX_THREADS == 128, "WAL input member limit changed");
StaticAssertDecl(sizeof(ClusterWalInputsJobV1) <= 272, "WAL input job header exceeds budget");
StaticAssertDecl(sizeof(ClusterCfJobReceiptV1) <= 184, "WAL input CF receipt exceeds budget");
StaticAssertDecl(sizeof(ClusterWalInputsJobControlBudgetV1)
					 <= CLUSTER_WAL_INPUT_JOB_CONTROL_RESERVE,
				 "WAL input job control types exceed their reserve");

#endif
