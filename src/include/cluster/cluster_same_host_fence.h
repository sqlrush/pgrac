/* PGRAC: local process identity evidence, never remote-host fencing. */
#ifndef CLUSTER_SAME_HOST_FENCE_H
#define CLUSTER_SAME_HOST_FENCE_H
#include "c.h"

#define CLUSTER_SAME_HOST_RECORD_BYTES 160

typedef struct ClusterSameHostProcessV1 {
	uint8 machine_id[16];
	uint8 boot_id[16];
	uint64 postmaster_pid;
	uint64 start_ticks;
	uint64 pid_namespace;
} ClusterSameHostProcessV1;

typedef struct ClusterSameHostKeyV1 {
	uint64 system_identifier;
	uint32 origin_node_id;
	uint64 incarnation;
	uint8 protected_set_digest[32];
} ClusterSameHostKeyV1;

typedef struct ClusterSameHostRecordV1 {
	ClusterSameHostKeyV1 key;
	ClusterSameHostProcessV1 process;
} ClusterSameHostRecordV1;

typedef enum ClusterSameHostProbeV1 {
	CLUSTER_SAME_HOST_PROCESS_UNREADABLE = 0,
	CLUSTER_SAME_HOST_PROCESS_MISSING = 1,
	CLUSTER_SAME_HOST_PROCESS_PRESENT = 2
} ClusterSameHostProbeV1;

typedef enum ClusterSameHostResultV1 {
	CLUSTER_SAME_HOST_UNAVAILABLE = 0,
	CLUSTER_SAME_HOST_WRITE_EXCLUDED = 1,
	CLUSTER_SAME_HOST_STILL_ALIVE = 2
} ClusterSameHostResultV1;

extern ClusterSameHostResultV1 cluster_same_host_process_excluded_v1(
	const ClusterSameHostProcessV1 *old, const ClusterSameHostProcessV1 *current,
	ClusterSameHostProbeV1 probe, uint64 observed_start_ticks);
extern bool cluster_same_host_process_start_v1(const char *stat_line, uint64 pid, uint64 *out);
extern bool cluster_same_host_record_encode_v1(const ClusterSameHostRecordV1 *record,
	uint8 bytes[CLUSTER_SAME_HOST_RECORD_BYTES]);
extern bool cluster_same_host_record_decode_v1(const uint8 *bytes, size_t length,
	const ClusterSameHostKeyV1 *expected, ClusterSameHostRecordV1 *out);
extern bool cluster_same_host_record_matches_v1(const ClusterSameHostRecordV1 *record,
	const ClusterSameHostKeyV1 *key);
/* Capture is startup-only; neither function changes a ROOT/claim/WAL object. */
extern bool cluster_same_host_fence_record_writer_v1(const char *wal_root,
	const ClusterSameHostKeyV1 *key, uint64 postmaster_pid);
extern bool cluster_same_host_fence_available(void);
extern ClusterSameHostResultV1 cluster_same_host_fence_prove_v1(const char *wal_root,
	const ClusterSameHostKeyV1 *key, ClusterSameHostRecordV1 *out);
#endif
