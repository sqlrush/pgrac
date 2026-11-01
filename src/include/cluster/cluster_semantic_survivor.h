/*-------------------------------------------------------------------------
 * cluster_semantic_survivor.h
 *    Independent survivor carrier grammar and bounded control layouts.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SEMANTIC_SURVIVOR_H
#define CLUSTER_SEMANTIC_SURVIVOR_H

#define CLUSTER_SURVIVOR_MEMBERS 128
#define CLUSTER_SURVIVOR_CONTROL_BYTES 65536
#define CLUSTER_SURVIVOR_WIRE_VERSION 4
#define CLUSTER_SURVIVOR_HEADER_BYTES 192
#define CLUSTER_SURVIVOR_SCRATCH_BYTES 10240
#define CLUSTER_SURVIVOR_CHUNK_ROWS 64

typedef enum ClusterSurvivorPurpose {
	CLUSTER_SURVIVOR_SERVICE = 1,
	CLUSTER_SURVIVOR_EXECUTOR_BIND = 2,
	CLUSTER_SURVIVOR_EXECUTOR_RETIRE = 3
} ClusterSurvivorPurpose;
typedef enum ClusterSurvivorKind {
	CLUSTER_SURVIVOR_REQUEST = 1,
	CLUSTER_SURVIVOR_ACK = 2
} ClusterSurvivorKind;
typedef enum ClusterSurvivorStage {
	CLUSTER_SURVIVOR_QUIESCED = 1,
	CLUSTER_SURVIVOR_CLAIM_READY = 2,
	CLUSTER_SURVIVOR_SERVICE_READY = 3,
	CLUSTER_SURVIVOR_OPEN_INSTALLED = 4
} ClusterSurvivorStage;
typedef enum ClusterSurvivorBodyKind {
	CLUSTER_SURVIVOR_MANIFEST = 1,
	CLUSTER_SURVIVOR_ACK_RECEIPT = 2,
	CLUSTER_SURVIVOR_OPEN_PROOF = 3,
	CLUSTER_SURVIVOR_EXECUTOR_ROW = 4
} ClusterSurvivorBodyKind;

typedef struct ClusterSurvivorIdentity {
	uint32 node, capability_word;
	uint64 boot, admitted, control_generation, capability_generation, observation_generation;
} ClusterSurvivorIdentity;
typedef struct ClusterSurvivorObserved {
	ClusterSurvivorIdentity identity;
	uint8 receipt[32];
} ClusterSurvivorObserved;
typedef struct ClusterSurvivorTableHeader {
	uint64 seq;
	uint32 stage, flags, coordinator, zero;
	uint64 nonce, survivors[2], observed[2];
	uint64 epoch, round_generation, source_features, target_features, rollback_features;
	uint64 capability_sample;
	uint8 previous_proof[32];
} ClusterSurvivorTableHeader;
typedef struct ClusterSurvivorTable {
	ClusterSurvivorTableHeader header;
	ClusterSurvivorIdentity expected[CLUSTER_SURVIVOR_MEMBERS];
	ClusterSurvivorObserved observed[CLUSTER_SURVIVOR_MEMBERS];
} ClusterSurvivorTable;
typedef struct ClusterSurvivorBasis {
	uint8 pgsa[512], pgrd[512], root[80], format[32];
	uint64 event, formation, native_freeze, open_generation, nonce;
	uint64 survivors[2], failures[2];
	uint8 input_scope[32], source_cut[32], native_census[32], reserved[232];
} ClusterSurvivorBasis;
typedef struct ClusterSurvivorHeader {
	uint64 seq, nonce, epoch, formation, native_freeze, round_generation;
	uint64 service_view_generation, latch_generation;
	uint32 phase, active_table, pending_reason, flags, coordinator, self;
	uint32 ingress_head, ingress_tail;
	uint64 typed_status, callback_debt, executor[2], staged_chunk[2], retiring[2];
	uint8 root[80], previous_proof[32], candidate_proof[32];
	uint64 ordinary_admission, native_closure, producer_pending[2], producer_refused[2];
	uint64 root_poll_cookie, job_reference, reference_generation;
	uint8 reserved[136];
} ClusterSurvivorHeader;
typedef struct ClusterSurvivorExecutor {
	uint32 node, pgprocno;
	uint64 proc_generation;
	int32 pid;
	uint16 consumer, state;
	uint64 boot, attempt;
	uint8 binding[32];
	uint64 slot_generation;
	uint32 active_claims, retiring_claims;
	uint64 job_generation;
} ClusterSurvivorExecutor;
typedef struct ClusterSurvivorIngress {
	uint16 node, kind;
	uint32 length;
	uint64 transport_generation, round_generation;
} ClusterSurvivorIngress;
/* Layout only: no storage is allocated or published by this header/codec. */
typedef struct ClusterSurvivorControl {
	ClusterSurvivorHeader header;
	ClusterSurvivorBasis basis;
	ClusterSurvivorTable tables[2];
	ClusterSurvivorExecutor executors[CLUSTER_SURVIVOR_MEMBERS];
	ClusterSurvivorIngress ingress[CLUSTER_SURVIVOR_MEMBERS];
	uint8 scratch[CLUSTER_SURVIVOR_SCRATCH_BYTES], reserved[4832];
} ClusterSurvivorControl;

#define SURVIVOR_SIZE(type, bytes) StaticAssertDecl(sizeof(type) == (bytes), #type " size")
#define SURVIVOR_OFFSET(type, field, bytes) \
	StaticAssertDecl(offsetof(type, field) == (bytes), #type "." #field " offset")
SURVIVOR_SIZE(ClusterSurvivorIdentity, 48);
SURVIVOR_SIZE(ClusterSurvivorObserved, 80);
SURVIVOR_SIZE(ClusterSurvivorTableHeader, 144);
SURVIVOR_SIZE(ClusterSurvivorTable, 16528);
SURVIVOR_SIZE(ClusterSurvivorBasis, 1536);
SURVIVOR_SIZE(ClusterSurvivorHeader, 512);
SURVIVOR_SIZE(ClusterSurvivorExecutor, 96);
SURVIVOR_SIZE(ClusterSurvivorIngress, 24);
SURVIVOR_SIZE(ClusterSurvivorControl, CLUSTER_SURVIVOR_CONTROL_BYTES);
SURVIVOR_OFFSET(ClusterSurvivorTable, expected, 144);
SURVIVOR_OFFSET(ClusterSurvivorTable, observed, 6288);
SURVIVOR_OFFSET(ClusterSurvivorBasis, root, 1024);
SURVIVOR_OFFSET(ClusterSurvivorBasis, event, 1136);
SURVIVOR_OFFSET(ClusterSurvivorBasis, input_scope, 1208);
SURVIVOR_OFFSET(ClusterSurvivorBasis, reserved, 1304);
SURVIVOR_OFFSET(ClusterSurvivorHeader, root, 160);
SURVIVOR_OFFSET(ClusterSurvivorHeader, ordinary_admission, 304);
SURVIVOR_OFFSET(ClusterSurvivorHeader, producer_pending, 320);
SURVIVOR_OFFSET(ClusterSurvivorHeader, root_poll_cookie, 352);
SURVIVOR_OFFSET(ClusterSurvivorHeader, reserved, 376);
SURVIVOR_OFFSET(ClusterSurvivorExecutor, pid, 16);
SURVIVOR_OFFSET(ClusterSurvivorExecutor, binding, 40);
SURVIVOR_OFFSET(ClusterSurvivorExecutor, job_generation, 88);
SURVIVOR_OFFSET(ClusterSurvivorControl, basis, 512);
SURVIVOR_OFFSET(ClusterSurvivorControl, tables, 2048);
SURVIVOR_OFFSET(ClusterSurvivorControl, executors, 35104);
SURVIVOR_OFFSET(ClusterSurvivorControl, ingress, 47392);
SURVIVOR_OFFSET(ClusterSurvivorControl, scratch, 50464);
SURVIVOR_OFFSET(ClusterSurvivorControl, reserved, 60704);
StaticAssertDecl(192 + 1536 + 80 + 128 * 48 <= CLUSTER_SURVIVOR_SCRATCH_BYTES,
				 "survivor manifest fits the single scratch");
StaticAssertDecl(192 + 1536 + 80 + 64 * 80 <= CLUSTER_SURVIVOR_SCRATCH_BYTES,
				 "survivor proof chunk fits the single scratch");
SURVIVOR_OFFSET(ClusterSurvivorIdentity, boot, 8);
SURVIVOR_OFFSET(ClusterSurvivorIdentity, admitted, 16);
SURVIVOR_OFFSET(ClusterSurvivorIdentity, control_generation, 24);
SURVIVOR_OFFSET(ClusterSurvivorIdentity, capability_generation, 32);
SURVIVOR_OFFSET(ClusterSurvivorIdentity, observation_generation, 40);
SURVIVOR_OFFSET(ClusterSurvivorObserved, receipt, 48);
SURVIVOR_OFFSET(ClusterSurvivorTableHeader, survivors, 32);
SURVIVOR_OFFSET(ClusterSurvivorTableHeader, observed, 48);
SURVIVOR_OFFSET(ClusterSurvivorTableHeader, epoch, 64);
SURVIVOR_OFFSET(ClusterSurvivorTableHeader, previous_proof, 112);
SURVIVOR_OFFSET(ClusterSurvivorBasis, pgsa, 0);
SURVIVOR_OFFSET(ClusterSurvivorBasis, pgrd, 512);
SURVIVOR_OFFSET(ClusterSurvivorBasis, format, 1104);
SURVIVOR_OFFSET(ClusterSurvivorBasis, formation, 1144);
SURVIVOR_OFFSET(ClusterSurvivorBasis, native_freeze, 1152);
SURVIVOR_OFFSET(ClusterSurvivorBasis, open_generation, 1160);
SURVIVOR_OFFSET(ClusterSurvivorBasis, nonce, 1168);
SURVIVOR_OFFSET(ClusterSurvivorBasis, survivors, 1176);
SURVIVOR_OFFSET(ClusterSurvivorBasis, failures, 1192);
SURVIVOR_OFFSET(ClusterSurvivorBasis, source_cut, 1240);
SURVIVOR_OFFSET(ClusterSurvivorBasis, native_census, 1272);
SURVIVOR_OFFSET(ClusterSurvivorHeader, executor, 112);
SURVIVOR_OFFSET(ClusterSurvivorHeader, staged_chunk, 128);
SURVIVOR_OFFSET(ClusterSurvivorHeader, retiring, 144);
SURVIVOR_OFFSET(ClusterSurvivorHeader, previous_proof, 240);
SURVIVOR_OFFSET(ClusterSurvivorHeader, candidate_proof, 272);
SURVIVOR_OFFSET(ClusterSurvivorHeader, producer_refused, 336);
SURVIVOR_OFFSET(ClusterSurvivorHeader, job_reference, 360);
SURVIVOR_OFFSET(ClusterSurvivorHeader, reference_generation, 368);
SURVIVOR_OFFSET(ClusterSurvivorControl, tables[1], 18576);
SURVIVOR_OFFSET(ClusterSurvivorIngress, transport_generation, 8);
SURVIVOR_OFFSET(ClusterSurvivorIngress, round_generation, 16);
#undef SURVIVOR_SIZE
#undef SURVIVOR_OFFSET

/* Host projection of the fixed LE header; body is borrowed encoded bytes.
 * No format id is assigned here. The caller supplies the batch's exact SHA.
 * Decode success is syntax only: original owner/identity/proof/closure and
 * current format admission MUST be checked separately before any effect.
 * Unknown enum values, omitted identity fields and reserved bytes reject.
 * Neither API allocates state, accesses an old table, or sends/accepts ACKs.
 * Decoded body expires with input bytes; no retained payload pointer allowed.
 */
typedef struct ClusterSurvivorMessageV4 {
	uint16 purpose, kind, stage, body_kind;
	uint64 nonce, epoch, formation, native_freeze, round_generation, features;
	uint32 coordinator, sender;
	uint64 survivors[2];
	uint8 basis[32], proof[32], format[32];
	uint16 chunk_start, chunk_count;
	const uint8 *body;
	Size body_bytes;
} ClusterSurvivorMessageV4;
extern bool cluster_semantic_survivor_v4_encode(const ClusterSurvivorMessageV4 *message,
	const uint8 expected_format[32], uint8 *bytes, Size capacity, Size *written);
extern bool cluster_semantic_survivor_v4_decode(const uint8 *bytes, Size length,
	const uint8 expected_format[32], ClusterSurvivorMessageV4 *message);
#endif
