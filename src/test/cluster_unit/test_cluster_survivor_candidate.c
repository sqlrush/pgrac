/* Actual LMON collection boundaries, plus explicitly untrusted wire inputs.
 * Syntax rows below are NEVER producer receipts or acceptance RED evidence.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
int survivor_candidate_original_lmon_main(void);
#define main survivor_candidate_original_lmon_main
#include "test_cluster_lmon.c"
#undef main
#include "test_cluster_backend_return_gate.inc"
#include "cluster/cluster_semantic_survivor_candidate.h"
#include "../../common/sha2_int.h"
#include "port/pg_crc32c.h"
#include "test_cluster_survivor_pgsa.inc"

#define PENDING CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING
#define REFUSED CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED
#define STALE CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE
#define SURVIVOR_POLL_REAL_NATIVE
#include "test_cluster_survivor_producer_fixture.h"
static void
survivor_native_family_test(void)
{
 ClusterSurvivorProducerObservation out;ClusterSurvivorProducerScope scope={3,3,11};
 BackendType backend=MyBackendType;AuxProcType aux=MyAuxProcType;
 bool saved_lmd=cluster_lmd_enabled;
 const BackendType backends[]={B_LMON,B_LMS,B_LMD,B_SINVAL_BCAST,B_LMS_WORKER};
 const AuxProcType auxiliaries[]={LmonProcess,LmsProcess,LmdProcess,SinvalBcastProcess,LmsWorker1Process};
 cluster_lmd_enabled=true;
 for(unsigned i=0;i<lengthof(backends);++i) {
  MyBackendType=backends[i];MyAuxProcType=auxiliaries[i];
  test_producer_seen=0;
  if(i!=0)UT_ASSERT(cluster_normal_stop_service_enter());
  UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_NATIVE,scope,&out),PENDING);
  UT_ASSERT_EQ(out.owner,CLUSTER_SURVIVOR_PRODUCER_NATIVE);
  UT_ASSERT_EQ(out.sample.native.producer.identity.pid,MyProcPid);
  UT_ASSERT_EQ(out.sample.native.producer.identity.backend_id,InvalidBackendId);
  UT_ASSERT(out.sample.native.producer.identity.actor_bit!=0);
  UT_ASSERT(out.sample.native.admission.known);
  UT_ASSERT_EQ(out.sample.native.producer.actor.active_depth,i==0?0:1);
  if(i!=0)UT_ASSERT_EQ(out.sample.native.producer.responsibility.modules,0);
  for(unsigned owner=0;owner<5;++owner) {
   if(owner==CLUSTER_SURVIVOR_PRODUCER_NATIVE)continue;
   UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll((ClusterSurvivorProducerOwner)owner,scope,&out),REFUSED);
   survivor_poll_unknown(&out);
  }
  if(i!=0)UT_ASSERT(cluster_normal_stop_service_leave(true));
 }
 cluster_lmd_enabled=saved_lmd;MyBackendType=backend;MyAuxProcType=aux;
}

static ClusterSurvivorControl candidate, before;
static uint8 manifest[7952], ack[272], body[1536 + 80 + 128 * 48], format[32];
static ClusterSurvivorMessageV4 message;
static Size manifest_bytes;
static unsigned candidate_case;
static const char *candidate_reason;

static void
put64(uint8 *p, uint64 v)
{
	for (unsigned i = 0; i < 8; i++) { p[i] = v & 255; v >>= 8; }
}
static void
make_manifest(void)
{
	ClusterSemanticActivationRecord open = { 0 };
	pg_sha256_ctx hash;
	memset(&candidate, 0, sizeof(candidate)); memset(&message, 0, sizeof(message));
	memset(body, 0, sizeof(body)); memset(format, 0x33, sizeof(format));
	message.purpose = CLUSTER_SURVIVOR_SERVICE; message.kind = CLUSTER_SURVIVOR_REQUEST;
	message.stage = CLUSTER_SURVIVOR_QUIESCED; message.body_kind = CLUSTER_SURVIVOR_MANIFEST;
	message.nonce = 5; message.epoch = 3; message.formation = 7; message.native_freeze = 9;
	message.round_generation = 13; message.features = 16;
	message.coordinator = message.sender = 0;
	message.survivors[0] = message.survivors[1] = UINT64_MAX;
	memcpy(message.format, format, 32); memset(message.proof, 0x44, 32);
	/* Actual original codec; still untrusted syntax, not selected OPEN proof. */
	open.source_feature_bitmap = open.target_feature_bitmap = 16;
	open.transition_epoch = candidate_case == 10 ? 2 : 3;
	open.record_generation = 11; open.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
	open.admitted_members_lo = open.admitted_members_hi = UINT64_MAX;
	open.coordinator_incarnation = 17; open.capability_sample_digest = 19;
	UT_ASSERT(cluster_semantic_activation_record_encode(&open, body));
	memset(body + 512, 0xb6, 512);
	memcpy(body + 1104, format, 32); put64(body + 1136, 1);
	put64(body + 1144, 7); put64(body + 1152, 9); put64(body + 1160, 11);
	put64(body + 1168, 5); put64(body + 1176, UINT64_MAX); put64(body + 1184, UINT64_MAX);
	for (unsigned node = 0; node < 128; node++) {
		uint8 *row = body + 1616 + node * 48;
		row[0] = node; row[4] = 1;
		for (unsigned field = 0; field < 5; field++) put64(row + 8 + field * 8, 100 + node + field);
	}
	pg_sha256_init(&hash); pg_sha256_update(&hash, body, 1536);
	pg_sha256_final(&hash, message.basis);
	message.body = body; message.body_bytes = sizeof(body);
	UT_ASSERT(cluster_semantic_survivor_v4_encode(&message, format, manifest, sizeof(manifest), &manifest_bytes));
}
static void
begin_candidate(void)
{
	make_manifest();
	UT_ASSERT_EQ(cluster_semantic_survivor_candidate_begin(&candidate, manifest, manifest_bytes,
		format, 0, &candidate_reason), PENDING);
	UT_ASSERT_EQ(candidate.header.seq, 2);
	UT_ASSERT_EQ(candidate.basis.open_generation, 11);
}
static void
make_ack(unsigned node)
{
	ClusterSurvivorMessageV4 m = message;
	static uint8 row[80];
	Size n = 0;
	memcpy(row, body + 1616 + 48 * node, 48);
	memset(row + 48, 0x55, 32); /* Untrusted syntax only. */
	m.kind = CLUSTER_SURVIVOR_ACK; m.body_kind = CLUSTER_SURVIVOR_ACK_RECEIPT;
	m.sender = node; m.body = row; m.body_bytes = sizeof(row);
	UT_ASSERT(cluster_semantic_survivor_v4_encode(&m, format, ack, sizeof(ack), &n));
	UT_ASSERT_EQ(n, sizeof(ack));
}
static ClusterSemanticAdmissionObservationResult
stage(unsigned node)
{
	return cluster_semantic_survivor_candidate_stage(&candidate, ack, sizeof(ack), format,
		102 + node, &candidate_reason);
}
static void
unconfirmed(void)
{
	UT_ASSERT_EQ(candidate.tables[0].header.flags, 0);
	UT_ASSERT_EQ(candidate.tables[0].header.observed[0], 0);
	UT_ASSERT_EQ(candidate.tables[0].header.observed[1], 0);
	UT_ASSERT_EQ(candidate.tables[0].header.capability_sample, 0);
	UT_ASSERT_EQ(candidate.header.ordinary_admission, 0);
	UT_ASSERT_EQ(candidate.header.native_closure, 0);
	UT_ASSERT_EQ(candidate.header.typed_status, 0);
}
static void
refresh_ack(ClusterSurvivorMessageV4 *m)
{
	Size n = 0;
	UT_ASSERT(cluster_semantic_survivor_v4_encode(m, format, ack, sizeof(ack), &n));
	UT_ASSERT_EQ(n, sizeof(ack));
}
static ClusterSemanticAdmissionObservationResult
probe(ClusterServiceProducerCollection *out)
{
	test_producer_seen = 0;
	return cluster_semantic_survivor_candidate_probe(&candidate, out, &candidate_reason);
}

/* Shared counters/roles below are explicit host boundary inputs. The original
 * LmonMain, actor bracket, module fanout and collection functions run as-is. */
static ClusterSemanticAdmissionObservationResult
return_collect(uint64 epoch, uint64 gate_epoch, uint64 generation, ClusterServiceProducerCollection *out)
{
	test_producer_seen = 0;
	return cluster_service_producer_collect_return(epoch, gate_epoch, generation, out);
}

static int
candidate_wait(WaitEvent *events pg_attribute_unused())
{
	ClusterServiceProducerCollection out = { 0 };
	ClusterSemanticActivationShmem gate_before;
	ClusterSurvivorMessageV4 m;
	unsigned polls = test_stop_polls;
	uint8 row[80];

	test_collection_samples++;
	if (candidate_case == 10)
		pg_atomic_write_u64(&test_collection_gate.formation_epoch, 2);
	begin_candidate();
	/* Each scenario retains the original idle owner observation/fanout. */
	if (candidate_case != 0) UT_ASSERT_EQ(probe(&out), PENDING);
	switch (candidate_case) {
	case 0:
		/* A closed original feature gate still cannot prove native closure. */
		pg_atomic_write_u32(&test_collection_gate.transition_closed, 1);
		memcpy(&gate_before, &test_collection_gate, sizeof(gate_before)); before = candidate;
		UT_ASSERT_EQ(probe(&out), PENDING);
		UT_ASSERT_EQ(out.producer.identity.proc_generation, 17);
		UT_ASSERT_EQ(out.producer.identity.backend_id, InvalidBackendId);
		UT_ASSERT(out.admission.known);
		UT_ASSERT_EQ(strcmp(candidate_reason, "candidate_producer_closure_unproved"), 0);
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		UT_ASSERT_EQ(memcmp(&gate_before, &test_collection_gate, sizeof(gate_before)), 0);
		UT_ASSERT_EQ(test_stop_polls - polls, 25);
		break;
	case 1:
		for (unsigned node = 0; node < 128; node++) {
			make_ack(node); UT_ASSERT_EQ(stage(node), PENDING); unconfirmed();
			UT_ASSERT_EQ(candidate.ingress[node].node, node);
			UT_ASSERT_EQ(candidate.ingress[node].length, 272);
			UT_ASSERT_EQ(candidate.tables[0].observed[node].identity.node, node);
			UT_ASSERT_EQ(candidate.header.seq, 4 + 2 * node);
		}
		UT_ASSERT_EQ(candidate.header.staged_chunk[0], UINT64_MAX);
		UT_ASSERT_EQ(candidate.header.staged_chunk[1], UINT64_MAX);
		UT_ASSERT_EQ(probe(&out), PENDING); /* All syntax rows still prove nothing. */
		break;
	case 2:
		make_ack(127); UT_ASSERT_EQ(stage(127), PENDING); before = candidate;
		UT_ASSERT_EQ(stage(127), PENDING); UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		UT_ASSERT(cluster_semantic_survivor_v4_decode(ack, sizeof(ack), format, &m));
		memcpy(row, m.body, sizeof(row)); row[48] ^= 1; m.body = row; refresh_ack(&m);
		UT_ASSERT_EQ(stage(127), REFUSED); UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		break;
	case 3:
		for (unsigned field = 0; field < 10; field++) {
			make_ack(1); UT_ASSERT(cluster_semantic_survivor_v4_decode(ack, sizeof(ack), format, &m));
			switch (field) {
			case 0: m.nonce++; break; case 1: m.epoch++; break;
			case 2: m.formation++; break; case 3: m.native_freeze++; break;
			case 4: m.round_generation++; break; case 5: m.features++; break;
			case 6: m.coordinator = 1; break; case 7: m.survivors[1] >>= 1; break;
			case 8: m.basis[0] ^= 1; break; case 9: m.proof[0] ^= 1; break;
			}
			refresh_ack(&m); before = candidate;
			UT_ASSERT_EQ(stage(1), STALE); UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		}
		break;
	case 4:
		for (unsigned field = 0; field < 6; field++) {
			make_ack(64); UT_ASSERT(cluster_semantic_survivor_v4_decode(ack, sizeof(ack), format, &m));
			memcpy(row, m.body, sizeof(row)); row[field == 0 ? 4 : field * 8] ^= 2;
			m.body = row; refresh_ack(&m); before = candidate;
			UT_ASSERT_EQ(stage(64), REFUSED); UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		}
		make_ack(64); before = candidate;
		UT_ASSERT_EQ(cluster_semantic_survivor_candidate_stage(&candidate, ack, sizeof(ack), format, 1, NULL), REFUSED);
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		break;
	case 5:
		make_ack(127); UT_ASSERT_EQ(stage(127), PENDING);
		candidate.header.callback_debt = 3; candidate.header.job_reference = 123;
		candidate.header.executor[1] = candidate.header.retiring[1] = UINT64_C(1) << 63;
		candidate.executors[127].pid = 4321; candidate.executors[127].job_generation = 77;
		before = candidate;
		UT_ASSERT_EQ(cluster_semantic_survivor_candidate_invalidate(&candidate, NULL), STALE);
		UT_ASSERT_EQ(candidate.header.staged_chunk[1], 0);
		UT_ASSERT_EQ(candidate.header.callback_debt, 3); UT_ASSERT_EQ(candidate.header.job_reference, 123);
		UT_ASSERT_EQ(candidate.header.executor[1], before.header.executor[1]);
		UT_ASSERT_EQ(candidate.header.retiring[1], before.header.retiring[1]);
		UT_ASSERT_EQ(memcmp(&candidate.basis, &before.basis, sizeof(candidate.basis)), 0);
		UT_ASSERT_EQ(memcmp(candidate.executors, before.executors, sizeof(candidate.executors)), 0);
		UT_ASSERT_EQ(memcmp(candidate.tables[0].observed, before.tables[0].observed, sizeof(candidate.tables[0].observed)), 0);
		before = candidate;
		UT_ASSERT_EQ(cluster_semantic_survivor_candidate_invalidate(&candidate, NULL), STALE);
		UT_ASSERT_EQ(stage(127), STALE); UT_ASSERT_EQ(probe(&out), STALE);
		UT_ASSERT_EQ(cluster_semantic_survivor_candidate_begin(&candidate, manifest, manifest_bytes, format, 0, NULL), PENDING);
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		break;
	case 6:
		before = candidate;
		UT_ASSERT_EQ(cluster_semantic_survivor_candidate_begin(&candidate, manifest, manifest_bytes, format, 0, NULL), PENDING);
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		memset(&candidate, 0, sizeof(candidate)); before = candidate;
		for (unsigned fault = 0; fault < 4; fault++) {
			uint8 invalid[7952];
			memcpy(invalid, manifest, manifest_bytes);
			if (fault == 0) invalid[120] ^= 1; /* CRC mismatch. */
			if (fault == 1) {
				ClusterSurvivorMessageV4 bad = message;
				bad.basis[0] ^= 1;
				UT_ASSERT(cluster_semantic_survivor_v4_encode(&bad, format, invalid, sizeof(invalid), &manifest_bytes));
			}
			UT_ASSERT_EQ(cluster_semantic_survivor_candidate_begin(&candidate, invalid,
				fault == 3 ? 191 : manifest_bytes, format, fault == 2 ? 128 : 0, NULL), REFUSED);
			UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		}
		break;
	case 7:
		before = candidate;
		pg_atomic_write_u32(&test_collection_gate.inflight[1][63], 1);
		UT_ASSERT_EQ(probe(&out), PENDING); UT_ASSERT_EQ(out.admission.inflight[1][63], 1);
		pg_atomic_write_u32(&test_collection_gate.inflight[1][63], 0);
		test_stop_observation[24] = CLUSTER_NORMAL_STOP_INVALID;
		UT_ASSERT_EQ(probe(&out), REFUSED);
		test_stop_observation[24] = CLUSTER_NORMAL_STOP_READY;
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		pg_atomic_write_u64(&test_collection_gate.record_generation, 12);
		UT_ASSERT_EQ(probe(&out), STALE); UT_ASSERT(candidate.header.flags != 0);
		pg_atomic_write_u64(&test_collection_gate.record_generation, 11);
		break;
	case 8:
		make_ack(1);
		for (unsigned fault = 0; fault < 2; fault++) {
			candidate.header.seq = candidate.tables[0].header.seq = fault == 0 ? UINT64_MAX - 1 : 2;
			candidate.header.latch_generation = fault == 1 ? UINT64_MAX - 1 : 1;
			before = candidate; UT_ASSERT_EQ(stage(1), REFUSED);
			UT_ASSERT_EQ(cluster_semantic_survivor_candidate_invalidate(&candidate, NULL), REFUSED);
			UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		}
		break;
	case 12:
		pg_atomic_write_u64(&test_collection_gate.formation_epoch, 2);
		before = candidate;
		for (unsigned side = 0; side < 2; side++)
			for (unsigned bit = 0; bit < 64; bit++) {
				pg_atomic_write_u32(&test_collection_gate.inflight[side][bit], 1);
				memcpy(&gate_before, &test_collection_gate, sizeof(gate_before));
				UT_ASSERT_EQ(return_collect(3, 2, 11, &out), PENDING);
				UT_ASSERT(out.admission.known);
				UT_ASSERT_EQ(out.admission.inflight[side][bit], 1);
				UT_ASSERT_EQ(out.producer.identity.epoch, 3);
				UT_ASSERT_EQ(out.admission.formation_epoch, 2);
				UT_ASSERT_EQ(out.producer.responsibility.modules, 25);
				UT_ASSERT_EQ(memcmp(&gate_before, &test_collection_gate, sizeof(gate_before)), 0);
				pg_atomic_write_u32(&test_collection_gate.inflight[side][bit], 0);
				UT_ASSERT_EQ(return_collect(3, 2, 11, &out), PENDING);
				UT_ASSERT_EQ(out.admission.inflight[side][bit], 0);
				UT_ASSERT_EQ(strcmp(out.reason, "NATIVE_COLLECTION_PRODUCER_CLOSURE_UNPROVED"), 0);
			}
		pg_atomic_write_u32(&test_collection_gate.inflight[1][63], UINT32_MAX);
		UT_ASSERT_EQ(return_collect(3, 2, 11, &out), PENDING);
		UT_ASSERT_EQ(out.admission.inflight[1][63], UINT32_MAX);
		UT_ASSERT_EQ(pg_atomic_read_u32(&test_collection_gate.inflight[1][63]), UINT32_MAX);
		pg_atomic_write_u32(&test_collection_gate.inflight[1][63], 0);
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		break;
	case 13:
		for (unsigned fault = 0; fault < 5; fault++) {
			uint64 saved_generation = MyProc->cluster_grd_generation;
			test_collection_fault = fault; test_collection_barriers = 0;
			test_collection_barrier_hook = test_collection_change_at_shared_cut;
			UT_ASSERT_EQ(return_collect(3, 3, 11, &out), fault == 1 || fault == 2 ? STALE : PENDING);
			test_collection_barrier_hook = NULL;
			UT_ASSERT_EQ(test_collection_barriers, 3);
			if (fault != 4) UT_ASSERT(!out.admission.known);
			if (fault == 4) {
				UT_ASSERT_EQ(out.producer.actor.active_depth, 1);
				UT_ASSERT(cluster_normal_stop_service_leave(true));
			}
			MyProc->cluster_grd_generation = saved_generation;
			pg_atomic_write_u64(&test_collection_gate.admission_seq, 2);
			pg_atomic_write_u64(&test_collection_gate.formation_epoch, 3);
			pg_atomic_write_u64(&test_collection_gate.record_generation, 11);
		}
		break;
	case 14:
		for (unsigned fault = 0; fault < 7; fault++) {
			unsigned seen = test_stop_polls;
			UT_ASSERT_EQ(return_collect(fault == 5 ? 0 : fault == 6 ? UINT64_MAX : 3,
				fault == 0 ? 0 : fault == 1 ? UINT64_MAX : fault == 2 ? 4 : 3,
				fault == 3 ? 0 : fault == 4 ? UINT64_MAX : 11, &out), REFUSED);
			UT_ASSERT(!out.admission.known);
			UT_ASSERT_EQ(test_stop_polls, seen);
		}
		UT_ASSERT_EQ(return_collect(3, 3, 11, NULL), REFUSED);
		test_stop_observation[0] = CLUSTER_NORMAL_STOP_PENDING;
		test_stop_observation[24] = CLUSTER_NORMAL_STOP_INVALID;
		UT_ASSERT_EQ(return_collect(3, 3, 11, &out), REFUSED);
		UT_ASSERT_EQ(out.producer.responsibility.modules, 25);
		UT_ASSERT(!out.admission.known);
		test_stop_observation[24] = CLUSTER_NORMAL_STOP_READY;
		UT_ASSERT_EQ(return_collect(3, 3, 11, &out), PENDING);
		UT_ASSERT_EQ(out.producer.responsibility.modules, 25);
		test_stop_observation[0] = CLUSTER_NORMAL_STOP_READY;
		SemanticActivationShmem = NULL;
		UT_ASSERT_EQ(return_collect(3, 3, 11, &out), REFUSED);
		SemanticActivationShmem = &test_collection_gate;
		pg_atomic_write_u64(&test_collection_gate.admission_seq, 3);
		UT_ASSERT_EQ(return_collect(3, 3, 11, &out), PENDING);
		UT_ASSERT(!out.admission.known);
		pg_atomic_write_u64(&test_collection_gate.admission_seq, 2);
		/* The same open current-epoch zero gate still has no return barrier. */
		UT_ASSERT_EQ(return_collect(3, 3, 11, &out), PENDING);
		UT_ASSERT_EQ(strcmp(out.reason, "NATIVE_COLLECTION_ADMISSION_PENDING"), 0);
		break;
	case 15:
		{
			ClusterSemanticActivationRecord open;
			UT_ASSERT(cluster_semantic_activation_record_decode(candidate.basis.pgsa, &open, NULL));
			/* OPEN's historical source may differ from its current target. */
			open.source_feature_bitmap = 0;
			UT_ASSERT(cluster_semantic_activation_record_encode(&open, candidate.basis.pgsa));
			before = candidate;
			UT_ASSERT_EQ(probe(&out), PENDING);
			UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		}
		break;
	case 11:
		for (unsigned fault = 0; fault < 12; fault++) {
			ClusterSemanticActivationRecord open;
			ClusterSurvivorBasis saved = candidate.basis;
			unsigned seen;
			UT_ASSERT(cluster_semantic_activation_record_decode(candidate.basis.pgsa, &open, NULL));
			switch (fault) {
			case 0: open.phase = CLUSTER_SEMANTIC_PHASE_PREPARE; break;
			case 1: open.record_generation = 12; break;
			case 2: open.record_generation = UINT64_MAX; break;
			case 3: open.transition_epoch = 0; break;
			case 4: open.transition_epoch = 4; break;
			case 5: open.transition_epoch = UINT64_MAX; break;
			case 6: open.target_feature_bitmap ^= 1; break;
			}
			UT_ASSERT(cluster_semantic_activation_record_encode(&open, candidate.basis.pgsa));
			if (fault == 7) candidate.basis.pgsa[100] = 1; /* Reserved byte. */
			if (fault == 8) candidate.basis.pgsa[96] ^= 1; /* Original CRC. */
			if (fault == 9) memset(candidate.basis.pgsa, 0, 512);
			if (fault == 10) candidate.tables[0].header.source_features ^= 1;
			if (fault == 11) candidate.basis.open_generation = 0;
			seen = test_stop_polls;
			memset(&out, 0xa5, sizeof(out));
			before = candidate;
			UT_ASSERT_EQ(probe(&out), REFUSED);
			UT_ASSERT(!out.admission.known);
			UT_ASSERT_EQ(test_stop_polls, seen);
			UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
			if (fault == 10) candidate.tables[0].header.source_features ^= 1;
			candidate.basis = saved;
		}
		break;
	case 10:
		before = candidate;
		pg_atomic_write_u32(&test_collection_gate.inflight[1][63], 1);
		UT_ASSERT_EQ(probe(&out), PENDING);
		UT_ASSERT(out.admission.known);
		UT_ASSERT_EQ(out.admission.formation_epoch, 2);
		UT_ASSERT_EQ(out.producer.identity.epoch, 3);
		UT_ASSERT_EQ(out.admission.inflight[1][63], 1);
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		pg_atomic_write_u32(&test_collection_gate.inflight[1][63], 0);
		UT_ASSERT_EQ(probe(&out), PENDING);
		UT_ASSERT_EQ(out.admission.inflight[1][63], 0);
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		break;
	case 9:
		make_ack(1); before = candidate;
		for (unsigned fault = 0; fault < 5; fault++) {
			int saved_pid = MyProcPid; BackendType saved_backend = MyBackendType;
			uint64 saved_generation = MyProc->cluster_grd_generation;
			if (fault == 0) MyProcPid++; if (fault == 1) MyBackendType = B_BACKEND;
			if (fault == 2) MyProc->backendId = 0;
			if (fault == 3) MyProc->cluster_grd_generation = 0;
			if (fault == 4) MyProc->cluster_grd_generation = UINT64_MAX;
			UT_ASSERT_EQ(stage(1), REFUSED); UT_ASSERT_EQ(probe(&out), REFUSED);
			UT_ASSERT_EQ(cluster_semantic_survivor_candidate_invalidate(&candidate, NULL), REFUSED);
			UT_ASSERT_EQ(cluster_semantic_survivor_candidate_begin(&candidate, manifest, manifest_bytes, format, 0, NULL), REFUSED);
			UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
			MyProcPid = saved_pid; MyBackendType = saved_backend;
			MyProc->backendId = InvalidBackendId; MyProc->cluster_grd_generation = saved_generation;
		}
		break;
	}
	if (candidate_case == 16) survivor_native_family_test();
	unconfirmed();
	ShutdownRequestPending = true;
	return 0;
}
static void run_candidate(unsigned which)
{
	candidate_case = which; test_collection_kind = 4;
	test_collection_extra_wait = candidate_wait;
	for (unsigned mode = 0; mode < 2; mode++) test_collection_run(mode != 0);
	test_collection_extra_wait = NULL;
}
UT_TEST(candidate_does_not_turn_an_idle_owner_point_into_quiescence) { run_candidate(0); }
UT_TEST(all_128_untrusted_rows_remain_unconfirmed) { run_candidate(1); }
UT_TEST(exact_duplicates_are_idempotent_conflicts_preserve_first) { run_candidate(2); }
UT_TEST(stale_scopes_cannot_change_current_candidate) { run_candidate(3); }
UT_TEST(every_identity_field_and_transport_must_match) { run_candidate(4); }
UT_TEST(invalidation_retains_original_jobs_and_callbacks) { run_candidate(5); }
UT_TEST(nonempty_or_invalid_begin_cannot_erase_previous_object) { run_candidate(6); }
UT_TEST(actual_native_debt_refusal_and_generation_drift_remain_distinct) { run_candidate(7); }
UT_TEST(generations_never_wrap_or_partially_stage) { run_candidate(8); }
UT_TEST(only_original_auxiliary_owner_can_touch_candidate) { run_candidate(9); }
UT_TEST(original_epoch_returns_remain_bound_to_current_lmon) { run_candidate(10); }
UT_TEST(original_open_baseline_is_decoded_and_bound_before_sampling) { run_candidate(11); }
UT_TEST(all_original_128_return_cells_keep_their_own_gate) { run_candidate(12); }
UT_TEST(return_collection_rechecks_original_identity_and_late_work) { run_candidate(13); }
UT_TEST(return_collection_preserves_refusal_and_pending_priorities) { run_candidate(14); }
UT_TEST(original_open_historical_source_does_not_replace_current_target) { run_candidate(15); }
UT_TEST(survivor_native_dispatch_uses_actual_roles_at_original_lmon_return) { run_candidate(16); }
#include "test_cluster_survivor_candidate_invalidation.inc"
int main(void)
{
	UT_PLAN(40);
	UT_RUN(candidate_does_not_turn_an_idle_owner_point_into_quiescence);
	UT_RUN(all_128_untrusted_rows_remain_unconfirmed);
	UT_RUN(exact_duplicates_are_idempotent_conflicts_preserve_first);
	UT_RUN(stale_scopes_cannot_change_current_candidate);
	UT_RUN(every_identity_field_and_transport_must_match);
	UT_RUN(invalidation_retains_original_jobs_and_callbacks);
	UT_RUN(nonempty_or_invalid_begin_cannot_erase_previous_object);
	UT_RUN(actual_native_debt_refusal_and_generation_drift_remain_distinct);
	UT_RUN(generations_never_wrap_or_partially_stage);
	UT_RUN(only_original_auxiliary_owner_can_touch_candidate);
	UT_RUN(original_epoch_returns_remain_bound_to_current_lmon);
	UT_RUN(original_open_baseline_is_decoded_and_bound_before_sampling);
	UT_RUN(all_original_128_return_cells_keep_their_own_gate);
	UT_RUN(return_collection_rechecks_original_identity_and_late_work);
	UT_RUN(return_collection_preserves_refusal_and_pending_priorities);
	UT_RUN(original_open_historical_source_does_not_replace_current_target);
	UT_RUN(survivor_native_dispatch_uses_actual_roles_at_original_lmon_return);
	UT_RUN(candidate_late_row_rechecks_actual_native_epoch_at_original_return);
	UT_RUN(candidate_late_row_rechecks_original_gate_generation);
	UT_RUN(candidate_late_row_rechecks_original_gate_epoch);
	UT_RUN(candidate_missing_gate_defers_without_mutation);
	UT_RUN(candidate_odd_gate_defers_without_mutation);
	UT_RUN(candidate_duplicate_rechecks_local_epoch);
	UT_RUN(candidate_foreign_scope_cannot_invalidate_local_candidate);
	UT_RUN(candidate_stale_native_request_cannot_begin);
	UT_RUN(candidate_stale_gate_request_cannot_begin);
	UT_RUN(candidate_unknown_gate_request_cannot_begin);
	UT_RUN(candidate_epoch_drift_during_write_retains_unconfirmed_rows);
	UT_RUN(candidate_gate_drift_during_write_retains_unconfirmed_rows);
	UT_RUN(candidate_owner_drift_during_write_revokes_candidate);
	UT_RUN(candidate_gate_drift_during_begin_retains_original_basis);
	UT_RUN(candidate_unknown_cut_during_write_never_confirms);
	UT_RUN(candidate_probe_clears_all_early_rejections);
	UT_RUN(candidate_invalidation_uses_one_bounded_generation_change);
	UT_RUN(native_survivor_entry_refuses_all_exiting_auxiliary_roles);
	UT_RUN(native_survivor_return_clears_all_exiting_auxiliary_roles);
	UT_RUN(candidate_exiting_entry_preserves_all_original_obligations);
	UT_RUN(candidate_exit_during_begin_retains_invalid_original_basis);
	UT_RUN(candidate_exit_during_stage_retains_rows_jobs_and_callbacks);
	UT_RUN(candidate_probe_exit_clears_native_sample_without_retiring_debt);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
