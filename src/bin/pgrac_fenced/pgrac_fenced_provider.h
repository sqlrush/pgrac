/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_provider.h
 *	  Frontend-safe provider ABI v1 and terminal verifier.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_PROVIDER_H
#define PGRAC_FENCED_PROVIDER_H

#include "c.h"

#include <stdint.h>

#define PGRAC_FENCED_PROVIDER_ABI_V1 UINT32_C(1)
#define PGRAC_FENCED_PROVIDER_ID_UNAVAILABLE UINT16_C(0)
#define PGRAC_FENCED_PROVIDER_ID_TEST_ONLY UINT16_C(1)
#define PGRAC_FENCED_PROVIDER_ID_IPMI_LANPLUS_V1 UINT16_C(0x0100)
#define PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1 UINT16_C(0x0101)
#define PGRAC_FENCED_PROVIDER_NAME_PACEMAKER_LIBVIRT_V1 "pacemaker-libvirt-v1"
#define PGRAC_FENCED_PROVIDER_NAME_IPMI_LANPLUS_V1 "ipmi-lanplus-v1"

typedef enum PgracFencedProviderResult
{
	PGRAC_FENCED_PROVIDER_OK = 0,
	PGRAC_FENCED_PROVIDER_PENDING = 1,
	PGRAC_FENCED_PROVIDER_REJECTED = 2,
	PGRAC_FENCED_PROVIDER_UNKNOWN = 3,
	PGRAC_FENCED_PROVIDER_UNAVAILABLE = 4,
	PGRAC_FENCED_PROVIDER_CONFIG_ERROR = 5,
	PGRAC_FENCED_PROVIDER_IO_ERROR = 6
} PgracFencedProviderResult;

typedef enum PgracFencedTargetState
{
	PGRAC_FENCED_TARGET_OFF = 1,
	PGRAC_FENCED_TARGET_ON = 2,
	PGRAC_FENCED_TARGET_TRANSITIONING = 3,
	PGRAC_FENCED_TARGET_UNKNOWN = 4
} PgracFencedTargetState;

typedef enum PgracFencedIoDrainState
{
	PGRAC_FENCED_IO_DRAIN_UNKNOWN = 0,
	PGRAC_FENCED_IO_DRAIN_DRAINED = 1,
	PGRAC_FENCED_IO_DRAIN_NOT_DRAINED = 2
} PgracFencedIoDrainState;

typedef enum PgracFencedProviderTerminal
{
	PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN = 0,
	PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED = 1,
	PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN = 2,
	PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE = 3
} PgracFencedProviderTerminal;

typedef enum PgracFencedProviderWorkerResult
{
	PGRAC_FENCED_PROVIDER_WORKER_OK = 0,
	PGRAC_FENCED_PROVIDER_WORKER_TIMEOUT = 1,
	PGRAC_FENCED_PROVIDER_WORKER_CRASHED = 2,
	PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE = 3
} PgracFencedProviderWorkerResult;

typedef struct PgracFencedTargetV1
{
	uint8_t target_uuid[16];
	int32_t victim_node_id;
	uint32_t reserved0;
	uint64_t mapping_generation;
	const void *adapter_config;
	size_t adapter_config_len;
} PgracFencedTargetV1;

typedef struct PgracFencedReadbackV1
{
	uint32_t state;
	uint32_t io_drain_state;
	int32_t native_status;
	uint32_t reserved0;
	uint8_t observed_target_uuid[16];
} PgracFencedReadbackV1;

StaticAssertDecl(sizeof(PgracFencedReadbackV1) == 32,
				 "pgrac-fenced readback v1 must remain 32 bytes");
StaticAssertDecl(offsetof(PgracFencedReadbackV1, state) == 0,
				 "readback state offset changed");
StaticAssertDecl(offsetof(PgracFencedReadbackV1, io_drain_state) == 4,
				 "readback drain offset changed");
StaticAssertDecl(offsetof(PgracFencedReadbackV1, native_status) == 8,
				 "readback native status offset changed");
StaticAssertDecl(offsetof(PgracFencedReadbackV1, reserved0) == 12,
				 "readback reserved offset changed");
StaticAssertDecl(offsetof(PgracFencedReadbackV1, observed_target_uuid) == 16,
				 "readback target UUID offset changed");

typedef struct PgracFencedProviderOpsV1
{
	uint32_t abi_version;
	uint32_t struct_size;
	uint16_t provider_id;
	uint16_t reserved0;
	const char *provider_name;
	PgracFencedProviderResult (*resolve)(
		const PgracFencedTargetV1 *configured,
		PgracFencedTargetV1 *resolved,
		int32_t *native_status);
	PgracFencedProviderResult (*actuate_off)(
		const PgracFencedTargetV1 *target,
		uint64_t deadline_mono_ns,
		int32_t *native_status);
	PgracFencedProviderResult (*readback)(
		const PgracFencedTargetV1 *target,
		uint64_t deadline_mono_ns,
		PgracFencedReadbackV1 *out);
	PgracFencedProviderResult (*actuate_on)(
		const PgracFencedTargetV1 *target,
		uint64_t deadline_mono_ns,
		int32_t *native_status);
	void (*shutdown)(void);
} PgracFencedProviderOpsV1;

extern const PgracFencedProviderOpsV1 *pgrac_fenced_provider_lookup(
	uint16 provider_id);
extern bool pgrac_fenced_provider_ops_valid(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only);
extern PgracFencedProviderTerminal pgrac_fenced_provider_classify_recovery(
	PgracFencedProviderResult result, const uint8 expected_target_uuid[16],
	const PgracFencedReadbackV1 *readback);
extern PgracFencedProviderTerminal pgrac_fenced_provider_classify_rejoin_on(
	PgracFencedProviderResult result, const uint8 expected_target_uuid[16],
	const PgracFencedReadbackV1 *readback);
extern uint64_t pgrac_fenced_provider_callback_deadline_mono_ns(void);
extern PgracFencedProviderWorkerResult pgrac_fenced_provider_worker_resolve(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only,
	const PgracFencedTargetV1 *configured, uint64_t deadline_mono_ns,
	PgracFencedProviderResult *result, PgracFencedTargetV1 *resolved,
	int32 *native_status);
extern PgracFencedProviderWorkerResult pgrac_fenced_provider_worker_actuate(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only, bool turn_on,
	const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
	PgracFencedProviderResult *result, int32 *native_status);
extern PgracFencedProviderWorkerResult pgrac_fenced_provider_worker_readback(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only,
	const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
	PgracFencedProviderResult *result, PgracFencedReadbackV1 *readback);
extern PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_readback_retry(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only,
	const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
	PgracFencedProviderResult *result, PgracFencedReadbackV1 *readback);

#endif /* PGRAC_FENCED_PROVIDER_H */
