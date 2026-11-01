/* Original PGPROC + backend-status target observations. No receipt,
 * permission, cleanup, per-backend storage, timer or new notification. */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "utils/backend_status.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_semantic_survivor_candidate.h"
#include "cluster/cluster_semantic_survivor_targets.h"
#include "cluster/cluster_semantic_survivor_close.h"

/* A status auxiliary index denotes its actual AuxProcType, unlike the
 * dynamically selected auxiliary PGPROC slot. Unknown combinations refuse. */
static bool
targets_family(ClusterSurvivorLocalTarget *t)
{
	if (t->backend_id > 0) {
		if (t->backend_type != B_BACKEND) return false;
		t->family = CLUSTER_SURVIVOR_PRODUCER_BACKEND;
		return true;
	}
	if ((t->backend_type == B_CHECKPOINTER && t->aux_type == CheckpointerProcess)
		|| (t->backend_type == B_BG_WRITER && t->aux_type == BgWriterProcess)) {
		t->family = CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS;
		return true;
	}
	if ((t->backend_type == B_LMON && t->aux_type == LmonProcess)
		|| (t->backend_type == B_LMS && t->aux_type == LmsProcess)
		|| (t->backend_type == B_LMD && t->aux_type == LmdProcess)
		|| (t->backend_type == B_SINVAL_BCAST && t->aux_type == SinvalBcastProcess)
		|| (t->backend_type == B_LMS_WORKER && t->aux_type >= LmsWorker1Process
			&& t->aux_type <= LmsWorker7Process)) {
		t->family = CLUSTER_SURVIVOR_PRODUCER_NATIVE;
		return true;
	}
	return false;
}

/* false means UNKNOWN, including startup, an unstable status, duplicate
 * auxiliary PID or a replaced PGPROC. It never means responsibility zero. */
static bool
targets_read(uint32 slot, ClusterSurvivorLocalTarget *out)
{
	volatile PGPROC *p;
	ClusterSurvivorLocalTarget t = {0};
	BackendType type;
	int pid;
	bool found = false;
	memset(out, 0, sizeof(*out));
	if (ProcGlobal == NULL || ProcGlobal->allProcs == NULL || slot >= ProcGlobal->allProcCount)
		return false;
	p = &ProcGlobal->allProcs[slot];
	t.pid = p->pid; t.pgprocno = p->pgprocno; t.backend_id = p->backendId;
	t.proc_generation = p->cluster_grd_generation;
	pg_read_barrier();
	if (t.pid <= 0 || t.pgprocno != (int32)slot || t.proc_generation == 0
		|| t.proc_generation == UINT64_MAX) return false;
	if (t.backend_id > 0 && t.backend_id <= MaxBackends) {
		found = pgstat_read_backend_identity(t.backend_id - 1, &pid, &type) && pid == t.pid;
		t.backend_type = type; t.aux_type = NotAnAuxProcess;
	} else if (t.backend_id == InvalidBackendId) {
		for (int aux = 0; aux < NUM_AUXPROCTYPES; aux++) {
			if (!pgstat_read_backend_identity(MaxBackends + aux, &pid, &type))
				return false;
			if (pid != t.pid) continue;
			if (found) return false;
			found = true; t.backend_type = type; t.aux_type = aux;
		}
	}
	pg_read_barrier();
	if (!found || p->pid != t.pid || p->pgprocno != t.pgprocno
		|| p->backendId != t.backend_id || p->cluster_grd_generation != t.proc_generation
		|| !targets_family(&t)) return false;
	*out = t;
	return true;
}

/* Invalidating a scope withdraws only its own sample. Even after the scope
 * is gone, finish transporting a returned/reaped callback before reuse. */
static void
targets_withdraw(ClusterSurvivorTargetCensus *s, ClusterSurvivorLocalMailbox *m)
{
	ClusterSurvivorLocalReply discarded;
	s->phase = CLUSTER_SURVIVOR_TARGETS_STALE;
	if (s->query_generation != 0 && (pg_atomic_read_u64(&m->word) >> 2) == s->query_generation) {
		cluster_semantic_survivor_local_invalidate();
		if (s->purpose == 1) (void)cluster_semantic_survivor_local_close_poll(&discarded);
  else (void)cluster_semantic_survivor_local_poll(&discarded);
	}
}

static void
targets_tick_version(bool close)
{
	ClusterSurvivorControl *c = cluster_semantic_survivor_shared_control();
	ClusterSurvivorTargetCensus *s;
	ClusterSurvivorLocalMailbox *m;
	ClusterSurvivorProducerScope scope;
	ClusterSemanticWaitGraphSnapshot gate;
	uint8 basis[32];
 ClusterSurvivorCloseSnapshot closure;
 uint64 nonce, round;
 bool valid;
	bool retry = false;
	if (c == NULL || !cluster_semantic_survivor_local_owner_ready()) return;
	s = (ClusterSurvivorTargetCensus *)(c->reserved + CLUSTER_SURVIVOR_LOCAL_BYTES);
	m = (ClusterSurvivorLocalMailbox *)c->reserved;
 /* The old candidate collector never withdraws a close-owned callback. */
 if (!close && s->purpose != 0) return;
 if (close) {
  valid = cluster_semantic_survivor_close_observe(&closure) == CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING
   && closure.blocked && closure.record.state == CLUSTER_SURVIVOR_CLOSE_CLOSING
   && closure.record.generation != 0;
  scope.native_epoch = closure.record.epoch; scope.gate_epoch = closure.record.gate_epoch;
  scope.record_generation = closure.record.record_generation;
  nonce = closure.record.event_id; round = closure.record.generation;
 } else {
  valid = cluster_semantic_survivor_candidate_scope(c, &scope, basis)
   && cluster_semantic_activation_wait_graph_snapshot(&gate) && gate.known
   && gate.seq != 0 && !(gate.seq & 1) && gate.transition_closed
   && gate.formation_epoch == scope.gate_epoch && gate.record_generation == scope.record_generation;
  nonce = c->header.nonce; round = c->header.round_generation;
 }
 if (!valid) {
  if (s->phase != 0 && s->purpose == (close ? 1U : 0U)) targets_withdraw(s, m);
  return;
 }
 if (s->zero != 0 || s->purpose > 1) return;
 if (close && s->purpose == 1 && s->phase != 0 && s->nonce == nonce && s->round == round) {
  uint64 seq = 0;
  for (unsigned i = 0; i < 8; i++) seq |= (uint64)m->reserved[i] << (8*i);
  if ((seq != 0 && seq != pg_atomic_read_u64(&closure.record.seq))
   || s->birth_cut != cluster_grd_generation_snapshot()) {
   targets_withdraw(s, m);
   /* Only a real new birth event can restart this census, after the old
	* callback has retired. It never creates or inherits an owner seal. */
   if ((pg_atomic_read_u64(&m->word) & 3) != CLUSTER_SURVIVOR_LOCAL_EMPTY
	|| (seq != 0 && seq != pg_atomic_read_u64(&closure.record.seq))) return;
   s->phase = 0;
  }
 }
	if (s->phase == CLUSTER_SURVIVOR_TARGETS_STALE) targets_withdraw(s, m);
	if (s->phase == 0 || s->purpose != (close ? 1U : 0U) || s->nonce != nonce || s->round != round) {
		uint64 birth = cluster_grd_generation_snapshot();
		if ((pg_atomic_read_u64(&m->word) & 3) != CLUSTER_SURVIVOR_LOCAL_EMPTY) {
			if (s->phase != 0) targets_withdraw(s, m);
			if ((pg_atomic_read_u64(&m->word) & 3) != CLUSTER_SURVIVOR_LOCAL_EMPTY) return;
		}
		if (ProcGlobal == NULL || ProcGlobal->allProcs == NULL || birth == 0 || birth == UINT64_MAX
			|| MaxBackends <= 0 || MaxBackends > INT_MAX - NUM_AUXILIARY_PROCS
			|| ProcGlobal->allProcCount > (uint32)(MaxBackends + NUM_AUXILIARY_PROCS)) return;
		memset(s, 0, sizeof(*s));
		s->nonce = nonce; s->round = round; s->purpose = close ? 1 : 0;
		s->native_epoch = scope.native_epoch; s->gate_epoch = scope.gate_epoch;
		s->record_generation = scope.record_generation; s->birth_cut = birth;
		s->limit = ProcGlobal->allProcCount; s->phase = CLUSTER_SURVIVOR_TARGETS_RUN;
	}
	if (s->native_epoch != scope.native_epoch || s->gate_epoch != scope.gate_epoch
		|| s->record_generation != scope.record_generation || ProcGlobal == NULL
		|| s->limit != ProcGlobal->allProcCount) {
		targets_withdraw(s, m); return;
	}
	if (s->phase == CLUSTER_SURVIVOR_TARGETS_WAIT) {
		ClusterSurvivorLocalReply reply;
		ClusterSurvivorLocalTarget current;
		ClusterSemanticAdmissionObservationResult result;
		if ((pg_atomic_read_u64(&m->word) >> 2) != s->query_generation) {
			s->phase = CLUSTER_SURVIVOR_TARGETS_STALE;
			s->unknown++; return;
		}
		result = close ? cluster_semantic_survivor_local_close_poll(&reply)
   : cluster_semantic_survivor_local_poll(&reply);
		if ((pg_atomic_read_u64(&m->word) & 3) != CLUSTER_SURVIVOR_LOCAL_EMPTY) return;
		if (result == CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING && reply.payload_bytes != 0)
			s->observed++;
		else if (result != CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE) s->refused++;
		/* A real reap retires transport as REFUSED even when the selected
		 * PGPROC has been replaced. Recheck identity separately before
		 * advancing; only the retired callback permits asking the new owner. */
		if (result == CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE
			|| !targets_read(s->cursor, &current) || memcmp(&current, &s->target, sizeof(current)) != 0) {
			s->stale++; retry = true;
		}
		/* These are counts of unconfirmed observations. No old debt retires. */
		if (!retry) s->cursor++;
		s->phase = CLUSTER_SURVIVOR_TARGETS_RUN;
	}
	while (s->phase == CLUSTER_SURVIVOR_TARGETS_RUN && s->cursor < s->limit) {
		ClusterSurvivorLocalTarget target;
		volatile PGPROC *p = &ProcGlobal->allProcs[s->cursor];
		if (p->pid == 0) {
			if (retry) s->unknown++;
			s->cursor++; retry = false; continue;
		}
		if (!targets_read(s->cursor, &target) || (!retry && target.proc_generation >= s->birth_cut)) {
			s->unknown++; s->cursor++; retry = false; continue;
		}
		if ((pg_atomic_read_u64(&m->word) & 3) != CLUSTER_SURVIVOR_LOCAL_EMPTY) return;
		if ((close ? cluster_semantic_survivor_local_close_begin(&target)
   : cluster_semantic_survivor_local_begin(&target)) != CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING
			|| (pg_atomic_read_u64(&m->word) & 3) == CLUSTER_SURVIVOR_LOCAL_EMPTY) {
			s->refused++; s->cursor++; retry = false; continue;
		}
		s->target = target; s->query_generation = pg_atomic_read_u64(&m->word) >> 2;
		s->attempts++; s->phase = CLUSTER_SURVIVOR_TARGETS_WAIT;
		return;
	}
	if (s->phase == CLUSTER_SURVIVOR_TARGETS_RUN) s->phase = CLUSTER_SURVIVOR_TARGETS_DONE;
}

void cluster_semantic_survivor_targets_tick(void) { targets_tick_version(false); }
void cluster_semantic_survivor_targets_close_tick(void) { targets_tick_version(true); }
