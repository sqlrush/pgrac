/* Configured input boundary inherited by the original PI business tests.
 * It has no CF, native S, pin or ACK producer. This adapter preserves those
 * original begin/revalidate faults and release counters; it proves neither
 * real job identity nor deferred retirement. test_cluster_wal_inputs_job_pi
 * separately executes real job/CF/WALR/registry/ACK with explicit native leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_PI_INPUTS_BOUNDARY_H
#define TEST_CLUSTER_PI_INPUTS_BOUNDARY_H

typedef struct PiInputsBoundary {
 ClusterWalInputsJobV1 job;
 void *header;
 bool active;
} PiInputsBoundary;
static PiInputsBoundary pi_inputs_boundary[3][CLUSTER_WAL_INPUT_JOB_COUNT];

static PiInputsBoundary *pi_boundary_find(ClusterWalInputsJobV1 *job)
{
 for(unsigned n=0;n<3;++n) for(unsigned c=0;c<CLUSTER_WAL_INPUT_JOB_COUNT;++c) {
  PiInputsBoundary *b=&pi_inputs_boundary[n][c];
  if(&b->job==job && b->active && n==(unsigned)cluster_node_id
   && job->consumer==c && job->owner==CurrentResourceOwner && job->owner_pid==MyProcPid)
   return b;
 }
 return NULL;
}
ClusterControlRootResult
cluster_wal_inputs_job_open_aux_v1(ClusterWalInputsConsumerV1 consumer,ClusterWalInputsJobV1 **out)
{
 PiInputsBoundary *b;
 if(out==NULL||*out!=NULL||cluster_node_id<0||cluster_node_id>=3
  ||(consumer!=WAL_INPUT_JOB_PI_NOTICE && consumer!=WAL_INPUT_JOB_PI_BATCH && consumer!=WAL_INPUT_JOB_PI_REBUILD)
  ||(consumer==WAL_INPUT_JOB_PI_BATCH ? MyBackendType!=B_CHECKPOINTER : MyBackendType!=B_BG_WRITER))
  return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
 b=&pi_inputs_boundary[cluster_node_id][consumer];
 if(!b->active) {
  memset(b,0,sizeof(*b));b->active=true;b->job.consumer=consumer;
  b->job.owner=CurrentResourceOwner;b->job.owner_pid=MyProcPid;
 }
 if(pi_boundary_find(&b->job)==NULL) return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
 *out=&b->job;
 return b->job.state==WAL_INPUT_JOB_STATE_NEW ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
}
MemoryContext cluster_wal_inputs_job_context_v1(ClusterWalInputsJobV1 *job)
{
 return pi_boundary_find(job)!=NULL && job->flags==0 && job->state!=WAL_INPUT_JOB_STATE_DONE
  ? (MemoryContext)job : NULL;
}
static void *pi_boundary_alloc(MemoryContext context,Size bytes)
{
 PiInputsBoundary *b=pi_boundary_find((ClusterWalInputsJobV1 *)context);
 void *memory;
 if(b==NULL||b->header!=NULL) abort();
 memory=wb_palloc0(bytes);b->header=memory;return memory;
}
bool cluster_wal_inputs_job_consumer_matches_v1(ClusterWalInputsJobV1 *job,ClusterWalInputsConsumerV1 consumer)
{ return pi_boundary_find(job)!=NULL && job->consumer==(uint32)consumer; }
bool cluster_wal_inputs_job_owner_revoke_v1(ClusterWalInputsJobV1 *job,ClusterWalInputsConsumerV1 consumer,bool error)
{
 if(!cluster_wal_inputs_job_consumer_matches_v1(job,consumer)) return false;
 job->flags|=WAL_INPUT_JOB_FLAG_REVOKED|(error?WAL_INPUT_JOB_FLAG_ERROR:0);
 if(job->state!=WAL_INPUT_JOB_STATE_DONE) job->state=WAL_INPUT_JOB_STATE_RETIRING_CF;
 return true;
}
bool cluster_wal_inputs_job_revoke_v1(ClusterWalInputsJobV1 *job,bool error)
{ return pi_boundary_find(job)!=NULL && cluster_wal_inputs_job_owner_revoke_v1(job,(ClusterWalInputsConsumerV1)job->consumer,error); }
ClusterControlRootResult cluster_wal_inputs_job_begin_v1(ClusterWalInputsJobV1 *job,const uint8 uuid[16],uint64 sysid,ClusterWalInputsV1 **out)
{
 ClusterControlRootResult r;
 if(pi_boundary_find(job)==NULL||job->state!=WAL_INPUT_JOB_STATE_NEW||out==NULL) return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
 r=cluster_wal_inputs_begin_v1(uuid,sysid,out);job->inputs=*out;
 job->state=r==CLUSTER_CONTROL_ROOT_OK_PRIMARY ? WAL_INPUT_JOB_STATE_USABLE : WAL_INPUT_JOB_STATE_RETIRING_CF;
 return r;
}
ClusterControlRootResult cluster_wal_inputs_job_revalidate_v1(ClusterWalInputsJobV1 *job)
{ return pi_boundary_find(job)!=NULL && job->state==WAL_INPUT_JOB_STATE_USABLE && job->flags==0
 ? cluster_wal_inputs_revalidate_v1(job->inputs) : CLUSTER_CONTROL_ROOT_STALE_TOKEN; }
ClusterControlRootResult cluster_wal_inputs_job_resume_v1(ClusterWalInputsJobV1 *job,ClusterWalInputsV1 **out)
{
 ClusterControlRootResult r;
 if(out==NULL) return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
 *out=NULL;
 if(pi_boundary_find(job)==NULL||job->flags!=0||(job->state!=WAL_INPUT_JOB_STATE_USABLE&&job->state!=WAL_INPUT_JOB_STATE_SUSPENDED)) return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
 r=cluster_wal_inputs_resume_v1(job->inputs);
 /* Preserve the original fixture's unpinned lock-wait interval. The
  * output borrow is NULL; the original suspended scope still exists. */
 if(r==CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE) {
  (void)cluster_wal_inputs_suspend_v1(job->inputs);
  job->state=WAL_INPUT_JOB_STATE_SUSPENDED;
 }
 if(r==CLUSTER_CONTROL_ROOT_OK_PRIMARY) {job->state=WAL_INPUT_JOB_STATE_USABLE;*out=job->inputs;}
 return r;
}
ClusterControlRootResult cluster_wal_inputs_job_suspend_v1(ClusterWalInputsJobV1 *job)
{
 ClusterControlRootResult r;
 if(pi_boundary_find(job)==NULL||job->flags!=0) return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
 r=cluster_wal_inputs_suspend_v1(job->inputs);
 if(r==CLUSTER_CONTROL_ROOT_OK_PRIMARY) job->state=WAL_INPUT_JOB_STATE_SUSPENDED;
 return r;
}
ClusterWalInputsJobResultV1 cluster_wal_inputs_job_retire_v1(ClusterWalInputsJobV1 *job)
{
 if(pi_boundary_find(job)==NULL) return WAL_INPUT_JOB_INVALID;
 if(job->inputs!=NULL && job->state!=WAL_INPUT_JOB_STATE_DONE)
  (void)cluster_wal_inputs_suspend_v1(job->inputs);
 job->flags|=WAL_INPUT_JOB_FLAG_REVOKED;job->state=WAL_INPUT_JOB_STATE_DONE;
 return WAL_INPUT_JOB_DONE;
}
ClusterWalInputsJobResultV1 cluster_wal_inputs_job_wait_failed_v1(ClusterWalInputsJobV1 *job)
{ return cluster_wal_inputs_job_retire_v1(job); }
ClusterControlRootResult cluster_wal_inputs_job_failed_root_v1(ClusterWalInputsJobV1 *job)
{ return pi_boundary_find(job)!=NULL && job->state==WAL_INPUT_JOB_STATE_DONE && (job->flags&WAL_INPUT_JOB_FLAG_ERROR)==0
 ? cluster_wal_inputs_wait_failed_v1(job->inputs) : CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT; }
ClusterWalInputsJobResultV1 cluster_wal_inputs_job_finish_v1(ClusterWalInputsJobV1 **job)
{
 PiInputsBoundary *b=job==NULL?NULL:pi_boundary_find(*job);
 if(b==NULL) return WAL_INPUT_JOB_INVALID;
 if(b->job.state!=WAL_INPUT_JOB_STATE_DONE) return WAL_INPUT_JOB_PENDING;
 cluster_wal_inputs_release_v1(&b->job.inputs);pfree(b->header);memset(b,0,sizeof(*b));*job=NULL;
 return WAL_INPUT_JOB_DONE;
}
#endif
