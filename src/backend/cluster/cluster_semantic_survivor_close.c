/* Event-driven, negative admission only. The old gate, callbacks and debts
 * have their original owners. No positive closure or reopening is implemented.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/wait_event.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_survivor_close.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_lock_owner.h"

#define CLOSE_PENDING CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING
#define CLOSE_STALE CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE
#define CLOSE_REFUSED CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED

static ClusterSurvivorCloseRecord *
close_record(void)
{
 ClusterSurvivorControl *c = cluster_semantic_survivor_shared_control();
 return c == NULL ? NULL
  : (ClusterSurvivorCloseRecord *)(c->reserved + CLUSTER_SURVIVOR_CLOSE_OFFSET);
}

static bool
close_nonzero(const void *data, Size size)
{
 const uint8 *p = data;
 for (Size i = 0; i < size; i++)
  if (p[i] != 0) return true;
 return false;
}

static bool
close_owner(const ClusterSurvivorCloseRecord *r)
{
 PGPROC *p;
 if (r->owner_pid <= 0 || r->owner_pgprocno < 0 || r->owner_generation == 0
  || r->owner_generation == UINT64_MAX || ProcGlobal == NULL
  || ProcGlobal->allProcs == NULL || r->owner_pgprocno >= ProcGlobal->allProcCount)
  return false;
 p = &ProcGlobal->allProcs[r->owner_pgprocno];
 return p->pid == r->owner_pid && p->pgprocno == r->owner_pgprocno
  && p->cluster_grd_generation == r->owner_generation && p->backendId == InvalidBackendId;
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_close_observe(ClusterSurvivorCloseSnapshot *out)
{
 ClusterSurvivorCloseRecord *r = close_record();
 ClusterSemanticWaitGraphSnapshot gate;
 uint64 seq;
 if (out == NULL) return CLOSE_REFUSED;
 memset(out, 0, sizeof(*out));
 out->blocked = true;
 if (r == NULL) return CLOSE_REFUSED;
 seq = pg_atomic_read_u64(&r->seq);
 if (seq & 1) return CLOSE_STALE;
 pg_read_barrier();
 memcpy((uint8 *)&out->record + sizeof(r->seq), (uint8 *)r + sizeof(r->seq),
  sizeof(*r) - sizeof(r->seq));
 pg_read_barrier();
 if (seq != pg_atomic_read_u64(&r->seq)) {
  memset(&out->record, 0, sizeof(out->record));
  return CLOSE_STALE;
 }
 pg_atomic_init_u64(&out->record.seq, seq);
 if (seq == 0 && !close_nonzero((uint8 *)&out->record + sizeof(r->seq),
							 sizeof(*r) - sizeof(r->seq))) {
  out->blocked = false;
  return CLOSE_PENDING;
 }
 if (out->record.state != CLUSTER_SURVIVOR_CLOSE_CLOSING
  || out->record.generation == 0 || out->record.event_id == 0 || out->record.event_seq == 0
  || out->record.kind != RECONFIG_KIND_FAIL_STOP || out->record.epoch == 0
  || out->record.epoch == UINT64_MAX || out->record.old_epoch == 0
  || out->record.old_epoch >= out->record.epoch
  || !close_nonzero(out->record.dead_bitmap, sizeof(out->record.dead_bitmap))
  || close_nonzero(out->record.reserved, sizeof(out->record.reserved))
  || !close_owner(&out->record) || cluster_epoch_get_current() != out->record.epoch
  || !cluster_semantic_activation_wait_graph_snapshot(&gate) || !gate.known
  || gate.seq == 0 || (gate.seq & 1) || gate.seq != out->record.gate_seq
  || gate.formation_epoch != out->record.gate_epoch
  || gate.record_generation != out->record.record_generation)
  return CLOSE_STALE;
 return CLOSE_PENDING;
}

void
cluster_semantic_survivor_close_invalidate(void)
{
 ClusterSurvivorCloseRecord *r = close_record();
 uint64 seq;
 if (r == NULL || !cluster_semantic_survivor_local_owner_ready()) return;
 seq = pg_atomic_read_u64(&r->seq);
 if ((seq & 1) || seq > UINT64_MAX - 2 || r->state == CLUSTER_SURVIVOR_CLOSE_IDLE
  || r->state == CLUSTER_SURVIVOR_CLOSE_STALE || r->owner_pid != MyProcPid
  || r->owner_pgprocno != MyProc->pgprocno
  || r->owner_generation != MyProc->cluster_grd_generation) return;
 if (!pg_atomic_compare_exchange_u64(&r->seq, &seq, seq + 1)) return;
 r->state = CLUSTER_SURVIVOR_CLOSE_STALE;
 pg_write_barrier();
 pg_atomic_write_u64(&r->seq, seq + 2);
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_close_begin(void)
{
 ClusterSurvivorCloseRecord *r = close_record();
 ClusterSemanticWaitGraphSnapshot gate;
 ClusterSurvivorCloseSnapshot current;
 ReconfigEvent event;
 uint64 seq, epoch, generation;
 bool known;
 if (r == NULL || !cluster_semantic_survivor_local_owner_ready()) return CLOSE_REFUSED;
 epoch = cluster_epoch_get_current();
 cluster_reconfig_get_last_event(&event);
 if (event.reconfig_kind != RECONFIG_KIND_FAIL_STOP || event.event_id == 0
  || event.event_seq == 0 || event.old_epoch == 0 || event.old_epoch >= event.new_epoch
  || epoch == 0 || epoch == UINT64_MAX || event.new_epoch != epoch
  || (event.observer_role != CLUSTER_RECONFIG_OBSERVER_COORDINATOR
   && event.observer_role != CLUSTER_RECONFIG_OBSERVER_SURVIVOR)
  || !close_nonzero(event.dead_bitmap, sizeof(event.dead_bitmap))) {
  if (r->state != CLUSTER_SURVIVOR_CLOSE_IDLE)
   cluster_semantic_survivor_close_invalidate();
  return CLOSE_REFUSED;
 }
 (void)cluster_semantic_survivor_close_observe(&current);
 seq = pg_atomic_read_u64(&r->seq);
 if ((seq & 1) || seq > UINT64_MAX - 2 || current.record.generation == UINT64_MAX)
  return CLOSE_STALE;
 if (current.blocked) {
  if (current.record.event_seq == event.event_seq) {
   if (current.record.event_id != event.event_id
	|| current.record.owner_pid != MyProcPid
	|| current.record.owner_pgprocno != MyProc->pgprocno
	|| current.record.owner_generation != MyProc->cluster_grd_generation)
	return CLOSE_STALE;
   if (current.record.state != CLUSTER_SURVIVOR_CLOSE_CLOSING
	|| current.record.gate_seq == 0
	|| current.record.epoch != cluster_epoch_get_current())
	cluster_semantic_survivor_close_invalidate();
   return CLOSE_PENDING;
  }
  if (event.event_seq <= current.record.event_seq
   || (current.record.state != CLUSTER_SURVIVOR_CLOSE_CLOSING
	&& current.record.state != CLUSTER_SURVIVOR_CLOSE_STALE))
   return CLOSE_STALE;
 }
 generation = current.record.generation + 1;
 known = cluster_semantic_activation_wait_graph_snapshot(&gate) && gate.known
  && gate.seq != 0 && !(gate.seq & 1);
 if (!cluster_semantic_survivor_local_owner_ready() || cluster_epoch_get_current() != epoch)
  return CLOSE_STALE;
 if (!pg_atomic_compare_exchange_u64(&r->seq, &seq, seq + 1)) return CLOSE_STALE;
 /* No throwing operations while odd; this record contains only observations,
  * never callback or resource ownership. Other control bytes are untouched. */
 memset((uint8 *)r + sizeof(r->seq), 0, sizeof(*r) - sizeof(r->seq));
 r->generation = generation; r->event_id = event.event_id; r->event_seq = event.event_seq;
 r->epoch = epoch; r->old_epoch = event.old_epoch;
 if (known) {
  r->gate_seq = gate.seq; r->gate_epoch = gate.formation_epoch;
  r->record_generation = gate.record_generation;
 }
 r->owner_generation = MyProc->cluster_grd_generation;
 r->owner_pid = MyProcPid; r->owner_pgprocno = MyProc->pgprocno;
 r->state = known ? CLUSTER_SURVIVOR_CLOSE_CLOSING : CLUSTER_SURVIVOR_CLOSE_STALE;
 r->kind = event.reconfig_kind;
 memcpy(r->dead_bitmap, event.dead_bitmap, sizeof(r->dead_bitmap));
 pg_write_barrier();
 pg_atomic_write_u64(&r->seq, seq + 2);
 return CLOSE_PENDING;
}

void
cluster_semantic_survivor_ordinary_wait(void)
{
 ClusterSurvivorCloseSnapshot sample;
 if (!IsUnderPostmaster || !cluster_enabled || !cluster_shared_config
  || MyBackendType != B_BACKEND || !IsNormalProcessingMode()) return;
 for (;;) {
  CHECK_FOR_INTERRUPTS();
  ResetLatch(MyLatch);
  (void)cluster_semantic_survivor_close_observe(&sample);
  if (!sample.blocked) return;
  cluster_semantic_survivor_backend_start_wait();
  /* Only the original caller advances its cleanup debt, without acquiring
   * fresh inputs. ACK latch wakes drive this, not a timer or another owner. */
  cluster_cf_retirement_poll();
  cluster_lock_owners_service_poll();
  CHECK_FOR_INTERRUPTS();
  (void)WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1,
				 WAIT_EVENT_RECONFIG_BARRIER_WAIT);
 }
}
