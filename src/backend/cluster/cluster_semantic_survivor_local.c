/*-------------------------------------------------------------------------
 * Single-target original-owner observations. This is local transport, never
 * a receipt, a producer seal, an admission transition or a cleanup owner.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *------------------------------------------------------------------------- */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_semantic_survivor_candidate.h"
#include "cluster/cluster_semantic_survivor_local.h"
#include "cluster/cluster_semantic_survivor_close.h"
#include "access/xact.h"
#include "../../common/sha2_int.h"

#define LOCAL_PENDING CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING
#define LOCAL_STALE CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE
#define LOCAL_REFUSED CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED
#define LOCAL_STATE(word) ((word) & UINT64_C(3))
#define LOCAL_GENERATION(word) ((word) >> 2)
#define LOCAL_WORD(generation, state) (((generation) << 2) | (state))
#define LOCAL_PAYLOAD_BYTES 1128

static ClusterSurvivorLocalMailbox *
local_mailbox(void)
{
	ClusterSurvivorControl *c = cluster_semantic_survivor_shared_control();
	return c == NULL ? NULL : (ClusterSurvivorLocalMailbox *)c->reserved;
}

static bool
local_reaped(ClusterSurvivorLocalMailbox *m, uint64 generation)
{
	/* Zero is the initial sentinel, never an observed child exit. */
	return generation != 0 && pg_atomic_read_u64(&m->reaped_generation) == generation;
}

/* No PID search and no dereference of another process's private state. */
static PGPROC *
local_proc(int32 pid, int32 pgprocno, uint64 generation)
{
	PGPROC *proc;
	if (pid <= 0 || pgprocno < 0 || generation == 0 || generation == UINT64_MAX
		|| ProcGlobal == NULL || ProcGlobal->allProcs == NULL || pgprocno >= ProcGlobal->allProcCount)
		return NULL;
	proc = &ProcGlobal->allProcs[pgprocno];
	if (proc->pgprocno != pgprocno || proc->pid != pid
		|| proc->cluster_grd_generation != generation)
		return NULL;
	return proc;
}

static void
local_held(LWLock *lock, LWLockMode mode, void *arg)
{
 (void)lock; (void)mode; *(bool *)arg = true;
}
static bool
local_unlocked(void)
{
 bool held = false;
 ForEachLWLockHeldByMe(local_held, &held);
 return !held;
}

bool
cluster_semantic_survivor_local_owner_ready(void)
{
	return IsUnderPostmaster && cluster_enabled && cluster_shared_config && !proc_exit_inprogress
		&& MyBackendType == B_LMON && AmLmonProcess() && MyBackendId == InvalidBackendId
		&& MyProc != NULL && MyProc->backendId == InvalidBackendId
		&& local_proc(MyProcPid, MyProc->pgprocno, MyProc->cluster_grd_generation) == MyProc
		&& CritSectionCount == 0 && local_unlocked();
}

static bool
local_sender(const ClusterSurvivorLocalMailbox *m)
{
	return cluster_semantic_survivor_local_owner_ready() && m->sender_pid == MyProcPid && m->sender_pgprocno == MyProc->pgprocno
		&& m->sender_proc_generation == MyProc->cluster_grd_generation;
}

static bool
local_target_shape(const ClusterSurvivorLocalTarget *t)
{
	if (t == NULL) return false;
	if (t->family == CLUSTER_SURVIVOR_PRODUCER_BACKEND)
		return t->backend_type == B_BACKEND && t->aux_type == NotAnAuxProcess
			&& t->backend_id > 0 && t->backend_id <= MaxBackends;
	if (t->backend_id != InvalidBackendId) return false;
	if (t->family == CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS)
		return (t->backend_type == B_CHECKPOINTER && t->aux_type == CheckpointerProcess)
			|| (t->backend_type == B_BG_WRITER && t->aux_type == BgWriterProcess);
	if (t->family != CLUSTER_SURVIVOR_PRODUCER_NATIVE) return false;
	return (t->backend_type == B_LMON && t->aux_type == LmonProcess)
		|| (t->backend_type == B_LMS && t->aux_type == LmsProcess)
		|| (t->backend_type == B_LMD && t->aux_type == LmdProcess)
		|| (t->backend_type == B_SINVAL_BCAST && t->aux_type == SinvalBcastProcess)
		|| (t->backend_type == B_LMS_WORKER && t->aux_type >= LmsWorker1Process
			&& t->aux_type <= LmsWorker7Process);
}

/* Local ABI2 uses the original scalar close identity, never a candidate. */
#define LOCAL_CLOSE_BINDING_BYTES 128
#define LOCAL_CLOSE_FACT_OFFSET 1128
#define LOCAL_CLOSE_FACT_BYTES 128
StaticAssertDecl(LOCAL_CLOSE_BINDING_BYTES + 128 == sizeof(((ClusterSurvivorLocalMailbox *)0)->reserved),
 "close binding occupies only half the original reserved content");
StaticAssertDecl(LOCAL_CLOSE_FACT_OFFSET + LOCAL_CLOSE_FACT_BYTES == CLUSTER_SURVIVOR_LOCAL_CLOSE_PAYLOAD,
 "close fact payload size");
StaticAssertDecl(CLUSTER_SURVIVOR_LOCAL_CLOSE_PAYLOAD + 152 == sizeof(((ClusterSurvivorLocalReply *)0)->body),
 "close fact leaves the original trailing zero bytes");

StaticAssertDecl(sizeof(ClusterSurvivorCloseRecord) == LOCAL_CLOSE_BINDING_BYTES,
 "close scalar projection covers the original record");
StaticAssertDecl(offsetof(ClusterSurvivorCloseRecord, generation) == 8
 && offsetof(ClusterSurvivorCloseRecord, event_id) == 16
 && offsetof(ClusterSurvivorCloseRecord, event_seq) == 24
 && offsetof(ClusterSurvivorCloseRecord, epoch) == 32
 && offsetof(ClusterSurvivorCloseRecord, old_epoch) == 40, "close event scalar offsets");
StaticAssertDecl(offsetof(ClusterSurvivorCloseRecord, gate_seq) == 48
 && offsetof(ClusterSurvivorCloseRecord, gate_epoch) == 56
 && offsetof(ClusterSurvivorCloseRecord, record_generation) == 64
 && offsetof(ClusterSurvivorCloseRecord, owner_generation) == 72, "close baseline scalar offsets");
StaticAssertDecl(offsetof(ClusterSurvivorCloseRecord, owner_pgprocno) == 84
 && offsetof(ClusterSurvivorCloseRecord, state) == 88
 && offsetof(ClusterSurvivorCloseRecord, kind) == 92, "close owner scalar offsets");
StaticAssertDecl(offsetof(ClusterSurvivorLocalMailbox, reserved) + LOCAL_CLOSE_BINDING_BYTES == 1920,
 "close binding preserves the mailbox trailing zero region");

static void local_put(uint8 *p, uint64 value, unsigned bytes);
static uint64
local_get(const uint8 *p, unsigned bytes)
{
 uint64 value = 0;
 for (unsigned i = 0; i < bytes; i++) value |= (uint64)p[i] << (8 * i);
 return value;
}
static bool
local_zero(const uint8 *p, Size bytes)
{
 for (Size i = 0; i < bytes; i++) if (p[i] != 0) return false;
 return true;
}
static bool
local_close_scope(uint8 binding[128], uint8 digest[32])
{
 ClusterSurvivorCloseSnapshot snapshot;
 ClusterSurvivorCloseRecord *r = &snapshot.record;
 pg_sha256_ctx hash;
 if (cluster_semantic_survivor_close_observe(&snapshot) != LOCAL_PENDING
  || !snapshot.blocked || r->state != CLUSTER_SURVIVOR_CLOSE_CLOSING
  || r->generation == 0) return false;
 memset(binding, 0, 128);
 local_put(binding, pg_atomic_read_u64(&r->seq), 8);
 local_put(binding + 8, r->generation, 8);
 local_put(binding + 16, r->event_id, 8); local_put(binding + 24, r->event_seq, 8);
 local_put(binding + 32, r->epoch, 8); local_put(binding + 40, r->old_epoch, 8);
 local_put(binding + 48, r->gate_seq, 8); local_put(binding + 56, r->gate_epoch, 8);
 local_put(binding + 64, r->record_generation, 8); local_put(binding + 72, r->owner_generation, 8);
 local_put(binding + 80, (uint32)r->owner_pid, 4); local_put(binding + 84, (uint32)r->owner_pgprocno, 4);
 local_put(binding + 88, r->state, 4); local_put(binding + 92, r->kind, 4);
 memcpy(binding + 96, r->dead_bitmap, 16);
 pg_sha256_init(&hash); pg_sha256_update(&hash, (const uint8 *)"P58-CLOSE-LOCAL-V2", 18);
 pg_sha256_update(&hash, binding, 128); pg_sha256_final(&hash, digest);
 return true;
}
static bool
local_close_matches(const ClusterSurvivorLocalMailbox *m, const ClusterSurvivorLocalQuery *q)
{
 uint8 binding[128], digest[32];
 return m->local_abi == CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI
  && m->flags == CLUSTER_SURVIVOR_LOCAL_CLOSE_BOUND
  && local_zero(m->header_reserved, sizeof(m->header_reserved))
  && local_zero(m->reserved + 128, 128)
  && local_zero(q->zero, sizeof(q->zero)) && local_zero(q->reserved, sizeof(q->reserved))
  && local_close_scope(binding, digest) && memcmp(binding, m->reserved, 128) == 0
  && memcmp(q->basis, digest, 32) == 0
  && q->native_epoch == local_get(binding + 32, 8)
  && q->nonce == local_get(binding + 16, 8) && q->round_generation == local_get(binding + 8, 8)
  && q->gate_epoch == local_get(binding + 56, 8) && q->record_generation == local_get(binding + 64, 8)
  && q->candidate_seq == local_get(binding, 8)
  && m->sender_pid == (int32)local_get(binding + 80, 4)
  && m->sender_pgprocno == (int32)local_get(binding + 84, 4)
  && m->sender_proc_generation == local_get(binding + 72, 8);
}

static bool
local_close_target_current(const ClusterSurvivorLocalQuery *q)
{
 PGPROC *p = local_proc(q->pid, q->pgprocno, q->proc_generation);
 return p != NULL && p->backendId == q->backend_id;
}

static bool
local_query_matches(const ClusterSurvivorLocalQuery *q)
{
	ClusterSurvivorControl *c = cluster_semantic_survivor_shared_control();
	ClusterSurvivorProducerScope scope;
	uint8 basis[32];
	return cluster_semantic_survivor_candidate_scope(c, &scope, basis)
		&& q->native_epoch == scope.native_epoch && q->gate_epoch == scope.gate_epoch
		&& q->record_generation == scope.record_generation && q->nonce == c->header.nonce
		&& q->round_generation == c->header.round_generation && q->candidate_seq == c->header.seq
		&& memcmp(q->basis, basis, sizeof(basis)) == 0;
}

static void
local_wake(int32 pid, int32 pgprocno, uint64 generation)
{
	PGPROC *proc = local_proc(pid, pgprocno, generation);
	if (proc != NULL) SetLatch(&proc->procLatch);
}

void
cluster_semantic_survivor_local_invalidate(void)
{
	ClusterSurvivorLocalMailbox *m = local_mailbox();
	uint64 word;
	if (m == NULL || !cluster_semantic_survivor_local_owner_ready()) return;
	word = pg_atomic_read_u64(&m->word);
	if (LOCAL_STATE(word) == CLUSTER_SURVIVOR_LOCAL_EMPTY) return;
	pg_atomic_write_u64(&m->invalidated_generation, LOCAL_GENERATION(word));
	/* A callback owns READING until it publishes or is actually reaped. */
	if (LOCAL_STATE(word) == CLUSTER_SURVIVOR_LOCAL_REQUESTED)
		(void)pg_atomic_compare_exchange_u64(&m->word, &word,
			LOCAL_WORD(LOCAL_GENERATION(word), CLUSTER_SURVIVOR_LOCAL_EMPTY));
}

static ClusterSemanticAdmissionObservationResult
local_begin_version(const ClusterSurvivorLocalTarget *target, bool close)
{
	ClusterSurvivorControl *c = cluster_semantic_survivor_shared_control();
	ClusterSurvivorLocalMailbox *m = local_mailbox();
	ClusterSurvivorProducerScope scope;
	ClusterSurvivorLocalQuery q = {0};
	uint8 binding[128];
	PGPROC *proc;
	uint64 word, generation;

	if (m == NULL || !cluster_semantic_survivor_local_owner_ready() || !local_target_shape(target)) return LOCAL_REFUSED;
 if (m->flags != 0 && !(m->local_abi == CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI
  && m->flags == CLUSTER_SURVIVOR_LOCAL_CLOSE_BOUND)) return LOCAL_REFUSED;
	word = pg_atomic_read_u64(&m->word);
	if (LOCAL_STATE(word) != CLUSTER_SURVIVOR_LOCAL_EMPTY) return LOCAL_PENDING;
	if (LOCAL_GENERATION(word) == CLUSTER_SURVIVOR_LOCAL_MAX_GENERATION) return LOCAL_REFUSED;
	proc = local_proc(target->pid, target->pgprocno, target->proc_generation);
	if (proc == NULL || proc->backendId != target->backend_id) return LOCAL_REFUSED;
 if (close) {
  if (!local_close_scope(binding, q.basis)) return LOCAL_REFUSED;
  scope.native_epoch = local_get(binding + 32, 8);
  scope.gate_epoch = local_get(binding + 56, 8); scope.record_generation = local_get(binding + 64, 8);
 } else if (!cluster_semantic_survivor_candidate_scope(c, &scope, q.basis)) return LOCAL_REFUSED;
	q.pid = target->pid; q.pgprocno = target->pgprocno; q.backend_id = target->backend_id;
	q.family = target->family; q.proc_generation = target->proc_generation;
	q.backend_type = target->backend_type; q.aux_type = target->aux_type;
	q.native_epoch = scope.native_epoch; q.gate_epoch = scope.gate_epoch;
	q.record_generation = scope.record_generation; q.nonce = c->header.nonce;
	q.round_generation = c->header.round_generation; q.candidate_seq = c->header.seq;
 if (close) {
  q.nonce = local_get(binding + 16, 8); q.round_generation = local_get(binding + 8, 8);
  q.candidate_seq = local_get(binding, 8);
 }
	generation = LOCAL_GENERATION(word) + 1;
	m->local_abi = close ? CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI : CLUSTER_SURVIVOR_LOCAL_ABI;
 m->flags = close ? CLUSTER_SURVIVOR_LOCAL_CLOSE_BOUND : 0;
 memset(m->reserved, 0, sizeof(m->reserved));
 if (close) memcpy(m->reserved, binding, 128);
	m->sender_pid = MyProcPid; m->sender_pgprocno = MyProc->pgprocno;
	m->sender_proc_generation = MyProc->cluster_grd_generation;
	m->query = q;
 if (close && !local_close_matches(m, &q)) return LOCAL_STALE;
	memset(&m->reply, 0, sizeof(m->reply));
	pg_write_barrier();
	pg_atomic_write_u64(&m->word, LOCAL_WORD(generation, CLUSTER_SURVIVOR_LOCAL_REQUESTED));
	/* Positive BackendId avoids SendProcSignal's PID scan. Auxiliaries which
	 * intentionally lack ProcSignal registration use their original latch. */
	if (target->family == CLUSTER_SURVIVOR_PRODUCER_BACKEND) {
		if (SendProcSignal(q.pid, PROCSIG_CLUSTER_SURVIVOR_OBSERVE, q.backend_id) != 0)
			cluster_semantic_survivor_local_invalidate();
	} else
		local_wake(q.pid, q.pgprocno, q.proc_generation);
	return LOCAL_PENDING;
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_local_begin(const ClusterSurvivorLocalTarget *target)
{ return local_begin_version(target, false); }
ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_local_close_begin(const ClusterSurvivorLocalTarget *target)
{ return local_begin_version(target, true); }

/* Explicit scalar projection: never memcpy a structure containing pointers. */
static void
local_put(uint8 *p, uint64 value, unsigned bytes)
{
	for (unsigned i = 0; i < bytes; i++) { p[i] = value & 255; value >>= 8; }
}
static void
local_identity(uint8 *p, const ClusterServiceProducerIdentity *id)
{
	local_put(p, (uint32)id->pid, 4); local_put(p + 4, (uint32)id->pgprocno, 4);
	local_put(p + 8, (uint32)id->backend_id, 4); local_put(p + 12, id->actor_bit, 4);
	local_put(p + 16, id->proc_generation, 8); local_put(p + 24, id->epoch, 8);
}
static void
local_text(ClusterSurvivorLocalReply *r, unsigned offset, unsigned capacity, const char *text)
{
	unsigned n = 0;
	if (text == NULL) return;
	while (n + 1 < capacity && text[n] != '\0') { r->body[offset + n] = text[n]; n++; }
	if (text[n] != '\0') r->flags |= 1; /* Diagnostic truncation only. */
}
static void
local_encode(ClusterSurvivorLocalReply *r, const ClusterSurvivorProducerObservation *sample)
{
	const ClusterSemanticAdmissionObservation *admission = NULL;
	const ClusterServiceProducerObservation *native = NULL;
	uint8 *p = r->body;
	uint32 flags = 0;
	if (sample->owner == CLUSTER_SURVIVOR_PRODUCER_BACKEND) {
		const ClusterServiceBackendCollection *b = &sample->sample.backend;
		r->identity = b->snapshots.identity; admission = &b->admission;
		local_put(p + 32, b->snapshots.retained_read_scn, 8);
		local_put(p + 40, b->snapshots.references, 4);
		flags = (b->snapshots.known ? 1U : 0) | (b->buffer_pins_pending ? 2U : 0)
			| (b->buffer_io_pending ? 4U : 0) | (b->transaction_pending ? 8U : 0);
	} else if (sample->owner == CLUSTER_SURVIVOR_PRODUCER_NATIVE) {
		native = &sample->sample.native.producer;
		r->identity = native->identity; admission = &sample->sample.native.admission;
		local_put(p + 592, native->actor.actor_bit, 4);
		local_put(p + 596, native->actor.bound_actor_bit, 4);
		local_put(p + 600, native->actor.active_depth, 4);
		local_put(p + 604, (uint32)native->responsibility.slot, 4);
		local_put(p + 608, native->responsibility.position, 4);
		local_put(p + 612, native->responsibility.modules, 4);
		local_put(p + 616, native->responsibility.key, 8);
		local_text(r, 760, 48, native->responsibility.domain);
		local_text(r, 968, 160, native->responsibility.reason);
	} else if (sample->owner == CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS) {
		const ClusterWalInputsJobsObservationV1 *jobs = &sample->sample.inputs;
		r->identity.pid = jobs->pid; r->identity.pgprocno = jobs->pgprocno;
		r->identity.backend_id = jobs->backend_id;
		r->identity.proc_generation = jobs->proc_generation; r->identity.epoch = jobs->epoch;
		flags = jobs->known ? 1U : 0;
		local_put(p + 624, jobs->present, 4);
		for (unsigned i = 0; i < CLUSTER_WAL_INPUT_JOB_OBSERVATIONS; i++) {
			const ClusterWalInputJobObservationV1 *job = &jobs->jobs[i];
			uint8 *row = p + 632 + i * 32;
			local_put(row, job->generation, 8); local_put(row + 8, job->pin_generation, 8);
			local_put(row + 16, job->state, 4); local_put(row + 20, job->cf_state, 4);
			local_put(row + 24, job->flags, 2); local_put(row + 26, job->walr_next, 2);
			row[28] = job->has_scope; row[29] = job->has_pin;
		}
	}
	if (admission != NULL) {
		flags |= (admission->known ? 16U : 0) | (admission->transition_closed ? 32U : 0);
		local_put(p + 48, admission->seq, 8); local_put(p + 56, admission->active_bits, 8);
		local_put(p + 64, admission->record_generation, 8);
		local_put(p + 72, admission->formation_epoch, 8);
		for (unsigned side = 0; side < 2; side++)
			for (unsigned bit = 0; bit < 64; bit++)
				local_put(p + 80 + (side * 64 + bit) * 4, admission->inflight[side][bit], 4);
	}
	local_identity(p, &r->identity); local_put(p + 44, flags, 4);
	local_text(r, 808, 160, sample->reason);
	r->payload_bytes = LOCAL_PAYLOAD_BYTES;
}

static void
local_close_fact(ClusterSurvivorLocalReply *r, const ClusterSurvivorLocalMailbox *m, uint32 origin)
{
 uint8 *p = r->body + LOCAL_CLOSE_FACT_OFFSET;
 const uint8 *b = m->reserved;
 local_put(p, 1, 4); local_put(p + 4, 128, 4);
 local_put(p + 8, origin != 0 ? 1 : 0, 8);
 local_put(p + 16, local_get(b + 8, 8), 8); local_put(p + 24, local_get(b, 8), 8);
 local_put(p + 32, local_get(b + 16, 8), 8); local_put(p + 40, local_get(b + 24, 8), 8);
 local_put(p + 48, local_get(b + 32, 8), 8); local_put(p + 56, local_get(b + 48, 8), 8);
 local_put(p + 64, local_get(b + 56, 8), 8); local_put(p + 72, local_get(b + 64, 8), 8);
 local_put(p + 80, origin, 4);
 r->payload_bytes = CLUSTER_SURVIVOR_LOCAL_CLOSE_PAYLOAD;
}
static bool
local_close_fact_valid(const ClusterSurvivorLocalReply *r, const ClusterSurvivorLocalMailbox *m)
{
 const uint8 *p = r->body + LOCAL_CLOSE_FACT_OFFSET, *b = m->reserved;
 uint64 coverage = local_get(p + 8, 8), origin = local_get(p + 80, 4);
 return local_get(p, 4) == 1 && local_get(p + 4, 4) == 128
  && coverage <= 1 && (coverage == 0 ? origin == 0
   : r->family == CLUSTER_SURVIVOR_PRODUCER_BACKEND
	&& (origin == CLUSTER_SURVIVOR_RETURN_COMMAND || origin == CLUSTER_SURVIVOR_RETURN_START_WAIT))
  && local_get(p + 16, 8) == local_get(b + 8, 8) && local_get(p + 24, 8) == local_get(b, 8)
  && local_get(p + 32, 8) == local_get(b + 16, 8) && local_get(p + 40, 8) == local_get(b + 24, 8)
  && local_get(p + 48, 8) == local_get(b + 32, 8) && local_get(p + 56, 8) == local_get(b + 48, 8)
  && local_get(p + 64, 8) == local_get(b + 56, 8) && local_get(p + 72, 8) == local_get(b + 64, 8)
  && local_zero(p + 84, 44) && local_zero(r->body + CLUSTER_SURVIVOR_LOCAL_CLOSE_PAYLOAD, 152);
}
static bool
local_backend_return_ready(uint32 origin)
{
 return (origin == CLUSTER_SURVIVOR_RETURN_COMMAND || origin == CLUSTER_SURVIVOR_RETURN_START_WAIT)
  && MyBackendType == B_BACKEND && IsNormalProcessingMode() && CurrentResourceOwner == NULL
  && cluster_transaction_survivor_returned()
  && (origin != CLUSTER_SURVIVOR_RETURN_COMMAND || !cluster_transaction_service_pending());
}

static void
local_publish(ClusterSurvivorLocalMailbox *m, uint64 generation,
	const ClusterSurvivorLocalQuery *q, ClusterSemanticAdmissionObservationResult result,
	const ClusterSurvivorProducerObservation *sample, uint32 origin)
{
	/* Sender identity is saved while this callback still exclusively owns
	 * READING. After REPLIED publication no callback writes the slot again. */
	int32 sender_pid = m->sender_pid, sender_pgprocno = m->sender_pgprocno;
	uint64 sender_generation = m->sender_proc_generation;
	ClusterSurvivorLocalReply *r = &m->reply;
	memset(r, 0, sizeof(*r));
	r->query_generation = generation; r->family = q->family;
	r->nonce = q->nonce; r->round_generation = q->round_generation;
	r->gate_epoch = q->gate_epoch; r->record_generation = q->record_generation;
	r->candidate_seq = q->candidate_seq; memcpy(r->basis, q->basis, 32);
	r->result = result == LOCAL_PENDING || result == LOCAL_STALE ? result : LOCAL_REFUSED;
	if (r->result == LOCAL_PENDING && sample != NULL) {
  local_encode(r, sample);
  if (m->local_abi == CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI) local_close_fact(r, m, origin);
 }
	pg_write_barrier();
	pg_atomic_write_u64(&m->word, LOCAL_WORD(generation, CLUSTER_SURVIVOR_LOCAL_REPLIED));
	local_wake(sender_pid, sender_pgprocno, sender_generation);
}

static void
local_process_version(uint32 origin)
{
	ClusterSurvivorLocalMailbox *m = local_mailbox();
	ClusterSurvivorLocalQuery q;
	uint64 word, generation;
	if (m == NULL || MyProc == NULL || !IsUnderPostmaster || !cluster_enabled
		|| !cluster_shared_config || proc_exit_inprogress || CritSectionCount != 0) return;
	word = pg_atomic_read_u64(&m->word);
	if (LOCAL_STATE(word) != CLUSTER_SURVIVOR_LOCAL_REQUESTED) return;
	pg_read_barrier();
	q = m->query;
	if ((m->local_abi != CLUSTER_SURVIVOR_LOCAL_ABI
  && m->local_abi != CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI) || q.pid != MyProcPid
		|| q.pgprocno != MyProc->pgprocno || q.backend_id != MyProc->backendId
		|| q.proc_generation != MyProc->cluster_grd_generation
		|| q.backend_type != MyBackendType || q.aux_type != MyAuxProcType) return;
 if (m->local_abi == CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI
  && q.family == CLUSTER_SURVIVOR_PRODUCER_BACKEND && !local_backend_return_ready(origin)) return;
 if (m->local_abi == CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI && !local_unlocked()) return;
	generation = LOCAL_GENERATION(word);
	if (!pg_atomic_compare_exchange_u64(&m->word, &word,
		LOCAL_WORD(generation, CLUSTER_SURVIVOR_LOCAL_READING))) return;
	pg_read_barrier();
	q = m->query;
	PG_TRY();
	{
		ClusterSurvivorProducerObservation sample;
		ClusterSurvivorProducerScope scope = {q.native_epoch, q.gate_epoch, q.record_generation};
		ClusterSemanticAdmissionObservationResult result;
		bool zero = true;
		bool close = m->local_abi == CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI;
		bool valid;
		uint32 fact_origin;
		for (unsigned i = 0; i < sizeof(q.zero); i++)
			if (q.zero[i] != 0 || q.reserved[i] != 0) zero = false;
		valid = close ? local_close_matches(m, &q) : m->flags == 0;
		fact_origin = close && q.family == CLUSTER_SURVIVOR_PRODUCER_BACKEND ? origin : 0;
		result = zero && valid ? cluster_semantic_survivor_producer_poll(q.family, scope, &sample)
			: LOCAL_REFUSED;
  if (close && (!local_close_matches(m, &q)
   || (fact_origin != 0 && !local_backend_return_ready(fact_origin)))) result = LOCAL_STALE;
		local_publish(m, generation, &q, result, &sample, fact_origin);
	}
	PG_CATCH();
	{
		local_publish(m, generation, &q, LOCAL_REFUSED, NULL, 0);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void cluster_semantic_survivor_local_process(void) { local_process_version(0); }
static void
local_backend_process(uint32 origin)
{
 ClusterSurvivorLocalMailbox *m = local_mailbox();
 /* A new boundary must not run an old candidate callback earlier than its
  * original interrupt priority, or run ahead of cancellation/exit. */
 if (m == NULL || m->local_abi != CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI
  || MyBackendType != B_BACKEND || InterruptPending || QueryCancelPending
  || ProcDiePending || InterruptHoldoffCount != 0 || QueryCancelHoldoffCount != 0) return;
 local_process_version(origin);
}
void cluster_semantic_survivor_backend_command_return(void)
{ local_backend_process(CLUSTER_SURVIVOR_RETURN_COMMAND); }
void cluster_semantic_survivor_backend_start_wait(void)
{ local_backend_process(CLUSTER_SURVIVOR_RETURN_START_WAIT); }

static ClusterSemanticAdmissionObservationResult
local_poll_version(ClusterSurvivorLocalReply *out, bool close)
{
	ClusterSurvivorLocalMailbox *m = local_mailbox();
	ClusterSurvivorLocalQuery q;
	ClusterSemanticAdmissionObservationResult result = LOCAL_REFUSED;
	uint64 word, generation;
	if (out != NULL) memset(out, 0, sizeof(*out));
	if (m == NULL || out == NULL || !cluster_semantic_survivor_local_owner_ready()) return LOCAL_REFUSED;
	word = pg_atomic_read_u64(&m->word);
	if (LOCAL_STATE(word) == CLUSTER_SURVIVOR_LOCAL_EMPTY) return LOCAL_PENDING;
 if ((close && m->local_abi == CLUSTER_SURVIVOR_LOCAL_ABI && m->flags == 0)
  || (!close && m->local_abi == CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI
   && m->flags == CLUSTER_SURVIVOR_LOCAL_CLOSE_BOUND)) return LOCAL_REFUSED;
	if (close ? m->local_abi != CLUSTER_SURVIVOR_LOCAL_CLOSE_ABI || m->flags != CLUSTER_SURVIVOR_LOCAL_CLOSE_BOUND
  : m->local_abi != CLUSTER_SURVIVOR_LOCAL_ABI || m->flags != 0) {
		cluster_semantic_survivor_local_invalidate();
		if (LOCAL_STATE(word) == CLUSTER_SURVIVOR_LOCAL_REPLIED
			|| (LOCAL_STATE(word) == CLUSTER_SURVIVOR_LOCAL_READING
				&& local_reaped(m, LOCAL_GENERATION(word))))
			(void)pg_atomic_compare_exchange_u64(&m->word, &word,
				LOCAL_WORD(LOCAL_GENERATION(word), CLUSTER_SURVIVOR_LOCAL_EMPTY));
		return LOCAL_REFUSED;
	}
	generation = LOCAL_GENERATION(word);
	pg_read_barrier(); q = m->query;
	if (!local_sender(m) || !(close ? local_close_matches(m, &q) : local_query_matches(&q))
		|| (close ? !local_close_target_current(&q) : local_proc(q.pid, q.pgprocno, q.proc_generation) == NULL)) {
		cluster_semantic_survivor_local_invalidate();
		result = LOCAL_STALE;
	}
	if (LOCAL_STATE(word) == CLUSTER_SURVIVOR_LOCAL_READING
		|| LOCAL_STATE(word) == CLUSTER_SURVIVOR_LOCAL_REQUESTED) {
		if (!local_reaped(m, generation)) return result == LOCAL_STALE ? result : LOCAL_PENDING;
		/* Only a real waitpid proves the old callback can no longer write. */
		result = LOCAL_REFUSED;
	} else if (LOCAL_STATE(word) == CLUSTER_SURVIVOR_LOCAL_REPLIED) {
		const ClusterSurvivorLocalReply *r = &m->reply;
		if (pg_atomic_read_u64(&m->invalidated_generation) < generation && local_sender(m)
			&& (close ? local_close_matches(m, &q) && local_close_target_current(&q)
	: local_query_matches(&q) && local_proc(q.pid, q.pgprocno, q.proc_generation) != NULL)
			&& r->query_generation == generation && r->family == q.family
			&& r->nonce == q.nonce && r->round_generation == q.round_generation
			&& r->gate_epoch == q.gate_epoch && r->record_generation == q.record_generation
			&& r->candidate_seq == q.candidate_seq && memcmp(r->basis, q.basis, 32) == 0
			&& (r->flags & ~1U) == 0) {
			if (r->result == LOCAL_PENDING && r->payload_bytes == (close ? CLUSTER_SURVIVOR_LOCAL_CLOSE_PAYLOAD : LOCAL_PAYLOAD_BYTES)
	&& (!close || local_close_fact_valid(r, m))
				&& r->identity.pid == q.pid && r->identity.pgprocno == q.pgprocno
				&& r->identity.backend_id == q.backend_id
				&& r->identity.proc_generation == q.proc_generation && r->identity.epoch == q.native_epoch) {
				*out = *r; result = LOCAL_PENDING;
			} else if (r->result == LOCAL_STALE) result = LOCAL_STALE;
		} else result = LOCAL_STALE;
	} else return result == LOCAL_STALE ? result : LOCAL_PENDING;
	/* The actual waitpid notification precedes later PGPROC cleanup. A
	 * returned sample is no longer usable once that exact exit was seen. */
	if (local_reaped(m, generation)) {
		memset(out, 0, sizeof(*out));
		result = LOCAL_REFUSED;
	}
	(void)pg_atomic_compare_exchange_u64(&m->word, &word,
		LOCAL_WORD(generation, CLUSTER_SURVIVOR_LOCAL_EMPTY));
	return result;
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_local_poll(ClusterSurvivorLocalReply *out)
{ return local_poll_version(out, false); }
ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_local_close_poll(ClusterSurvivorLocalReply *out)
{ return local_poll_version(out, true); }

void
cluster_semantic_survivor_local_postmaster_reap(pid_t pid)
{
	ClusterSurvivorLocalMailbox *m = local_mailbox();
	uint64 word;
	if (m == NULL || IsUnderPostmaster || !IsPostmasterEnvironment || pid <= 0) return;
	word = pg_atomic_read_u64(&m->word);
	if (LOCAL_STATE(word) == CLUSTER_SURVIVOR_LOCAL_EMPTY || LOCAL_GENERATION(word) == 0) return;
	pg_read_barrier();
	if (m->query.pid == pid) {
		/* Sole postmaster writer; never reset on a new query or LMON restart. */
		pg_atomic_write_u64(&m->reaped_generation, LOCAL_GENERATION(word));
		local_wake(m->sender_pid, m->sender_pgprocno, m->sender_proc_generation);
	}
}
