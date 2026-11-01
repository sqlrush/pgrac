/* Single-target local observation transport. Never a producer receipt.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_SEMANTIC_SURVIVOR_LOCAL_H
#define CLUSTER_SEMANTIC_SURVIVOR_LOCAL_H
#include "port/atomics.h"
#include "cluster/cluster_semantic_survivor.h"
#include "cluster/cluster_semantic_survivor_producer.h"

#define CLUSTER_SURVIVOR_LOCAL_BYTES 2048
#define CLUSTER_SURVIVOR_LOCAL_ABI 1
#define CLUSTER_SURVIVOR_LOCAL_EMPTY 0
#define CLUSTER_SURVIVOR_LOCAL_REQUESTED 1
#define CLUSTER_SURVIVOR_LOCAL_READING 2
#define CLUSTER_SURVIVOR_LOCAL_REPLIED 3
#define CLUSTER_SURVIVOR_LOCAL_MAX_GENERATION (UINT64_MAX >> 2)

typedef struct ClusterSurvivorLocalTarget {
 int32 pid, pgprocno, backend_id;
 uint32 family;
 uint64 proc_generation;
 uint32 backend_type, aux_type;
} ClusterSurvivorLocalTarget;
typedef struct ClusterSurvivorLocalQuery {
 int32 pid, pgprocno, backend_id;
 uint32 family;
 uint64 proc_generation, native_epoch, nonce, round_generation;
 uint64 gate_epoch, record_generation, candidate_seq;
 uint32 backend_type, aux_type;
 uint8 zero[8], basis[32], reserved[8];
} ClusterSurvivorLocalQuery;
typedef struct ClusterSurvivorLocalReply {
 uint64 query_generation;
 uint32 result, family;
 ClusterServiceProducerIdentity identity;
 uint64 nonce, round_generation, gate_epoch, record_generation, candidate_seq;
 uint8 basis[32];
 uint32 payload_bytes, flags;
 uint8 body[1408];
} ClusterSurvivorLocalReply;
typedef struct ClusterSurvivorLocalMailbox {
 pg_atomic_uint64 word, invalidated_generation, reaped_generation;
 int32 sender_pid, sender_pgprocno;
 uint64 sender_proc_generation;
 uint32 local_abi, flags;
 uint8 header_reserved[80];
 ClusterSurvivorLocalQuery query;
 ClusterSurvivorLocalReply reply;
 uint8 reserved[256];
} ClusterSurvivorLocalMailbox;
StaticAssertDecl(sizeof(ClusterSurvivorLocalTarget) == 32, "ClusterSurvivorLocalTarget size");
StaticAssertDecl(sizeof(ClusterSurvivorLocalQuery) == 128, "ClusterSurvivorLocalQuery size");
StaticAssertDecl(sizeof(ClusterSurvivorLocalReply) == 1536, "ClusterSurvivorLocalReply size");
StaticAssertDecl(sizeof(ClusterSurvivorLocalMailbox) == CLUSTER_SURVIVOR_LOCAL_BYTES, "ClusterSurvivorLocalMailbox size");
StaticAssertDecl(offsetof(ClusterSurvivorLocalMailbox, query) == 128, "ClusterSurvivorLocalMailbox.query offset");
StaticAssertDecl(offsetof(ClusterSurvivorLocalMailbox, reply) == 256, "ClusterSurvivorLocalMailbox.reply offset");
StaticAssertDecl(offsetof(ClusterSurvivorLocalMailbox, reserved) == 1792, "ClusterSurvivorLocalMailbox.reserved offset");
StaticAssertDecl(offsetof(ClusterSurvivorLocalQuery, basis) == 88, "ClusterSurvivorLocalQuery.basis offset");
StaticAssertDecl(offsetof(ClusterSurvivorLocalReply, body) == 128, "ClusterSurvivorLocalReply.body offset");
StaticAssertDecl(CLUSTER_SURVIVOR_LOCAL_BYTES + 2784 == sizeof(((ClusterSurvivorControl *)0)->reserved),
 "local mailbox leaves 2784 bytes for target cursor and zero tail");

/* Shared storage only, NULL before original gate allocation. */
extern ClusterSurvivorControl *cluster_semantic_survivor_shared_control(void);
extern ClusterSemanticAdmissionObservationResult cluster_semantic_survivor_local_begin(
 const ClusterSurvivorLocalTarget *target);
/* payload_bytes==0 means no usable sample. A delivered observation is still
 * PENDING, never CURRENT/receipt/closure/ordinary or GCS permission. */
extern ClusterSemanticAdmissionObservationResult cluster_semantic_survivor_local_poll(
 ClusterSurvivorLocalReply *out);
extern void cluster_semantic_survivor_local_process(void);
extern void cluster_semantic_survivor_local_invalidate(void);
/* Called only adjacent to the original successful postmaster waitpid. */
extern void cluster_semantic_survivor_local_postmaster_reap(pid_t pid);
extern bool cluster_semantic_survivor_local_owner_ready(void);
#endif
