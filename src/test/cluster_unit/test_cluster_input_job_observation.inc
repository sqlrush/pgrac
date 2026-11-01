/* The original complete job/CF/WALR/ACK and loop prefixes are executed.
 * Process context, lock ownership and one barrier interleave remain explicit
 * host inputs; observation never supplies a receipt or producer closure.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
input_observation_begin(bool bgwriter)
{
	pin_begin();
	IsUnderPostmaster = true;
	MyBackendType = bgwriter ? B_BG_WRITER : B_CHECKPOINTER;
	MyAuxProcType = bgwriter ? BgWriterProcess : CheckpointerProcess;
	MyProc->backendId = InvalidBackendId;
	job_observation_lock_held = false;
	job_observation_barrier_hook = NULL;
}
static void
input_observation_unknown(const ClusterWalInputsJobsObservationV1 *out)
{
	ClusterWalInputsJobsObservationV1 zero = {0}, value = *out;
	value.reason = NULL;
	UT_ASSERT(memcmp(&value,&zero,sizeof(value)) == 0);
	UT_ASSERT(out->reason != NULL);
}
static void
input_observation_read(ClusterWalInputsJobsObservationV1 *out)
{
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_service_observe_v1(owner_epoch,out),WAL_INPUT_JOBS_OBSERVATION_PENDING);
	UT_ASSERT(out->known && out->pid == MyProcPid && out->pgprocno == MyProc->pgprocno);
	UT_ASSERT_EQ(out->backend_id,InvalidBackendId);
	UT_ASSERT_EQ(out->proc_generation,MyProc->cluster_grd_generation);
	UT_ASSERT_EQ(out->epoch,owner_epoch);
}
static unsigned input_observation_empty_drift_kind;
static void
input_observation_empty_drift(void)
{
	job_observation_barrier_hook=NULL;
	if(input_observation_empty_drift_kind==0) {
		MyBackendType=B_BG_WRITER;MyAuxProcType=BgWriterProcess;
	} else
		CurrentResourceOwner=AuxProcessResourceOwner=(ResourceOwner)&job_fixture_foreign_owner;
}
UT_TEST(input_observation_empty_original_owner_stays_pending)
{
	ClusterWalInputsJobsObservationV1 out;
	input_observation_begin(false);
	memset(&out,0xa5,sizeof(out));
	input_observation_read(&out);
	UT_ASSERT_EQ(out.present,0);
	UT_ASSERT(strcmp(out.reason,"INPUT_JOBS_PRODUCER_CLOSURE_UNPROVED")==0);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(),WAL_INPUT_JOB_DONE);
	for(unsigned i=0;i<2;++i) {
		ResourceOwner resource=CurrentResourceOwner;
		ClusterWalInputsJobsObservationResultV1 observed;
		input_observation_empty_drift_kind=i;job_observation_barrier_hook=input_observation_empty_drift;
		observed=cluster_wal_inputs_jobs_service_observe_v1(owner_epoch,&out);
		MyBackendType=B_CHECKPOINTER;MyAuxProcType=CheckpointerProcess;
		CurrentResourceOwner=AuxProcessResourceOwner=resource;
		UT_ASSERT_EQ(observed,WAL_INPUT_JOBS_OBSERVATION_INVALID);
		input_observation_unknown(&out);
	}
	foundation_end();
}
UT_TEST(input_observation_all_four_consumers_preserve_original_cf_and_done)
{
	for (unsigned consumer=0;consumer<4;++consumer) {
		ClusterWalInputsJobV1 *job;
		ClusterWalInputsJobsObservationV1 out;
		unsigned waits,created,deleted,wakes;
		input_observation_begin(consumer==1 || consumer==3);
		job=foundation_open((ClusterWalInputsConsumerV1)consumer);
		input_observation_read(&out);
		UT_ASSERT_EQ(out.present,1U<<consumer);
		UT_ASSERT_EQ(out.jobs[consumer].state,WAL_INPUT_JOB_STATE_NEW);
		UT_ASSERT(!out.jobs[consumer].has_scope && !out.jobs[consumer].has_pin);
		UT_ASSERT_EQ(out.jobs[consumer].generation,job->generation);
		UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
		input_observation_read(&out);
		UT_ASSERT(out.jobs[consumer].has_scope);
		UT_ASSERT_EQ(out.jobs[consumer].state,WAL_INPUT_JOB_STATE_BUILDING);
		g_seven_result=CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT(cluster_cf_lock(ShareLock));
		UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock,NULL,&job->cf),CF_JOB_PENDING);
		UT_ASSERT(cluster_wal_inputs_job_revoke_v1(job,false));
		waits=owner_wait_calls;created=job_contexts_created;deleted=job_contexts_deleted;wakes=job_wakes;
		for (unsigned read=0;read<3;++read) {
			input_observation_read(&out);
			UT_ASSERT_EQ(out.jobs[consumer].state,WAL_INPUT_JOB_STATE_RETIRING_CF);
			UT_ASSERT_EQ(out.jobs[consumer].cf_state,CF_JOB_RECEIPT_RETIRING);
			UT_ASSERT_EQ(out.jobs[consumer].flags,job->flags);
			UT_ASSERT_EQ(owner_wait_calls,waits);UT_ASSERT_EQ(job_contexts_created,created);
			UT_ASSERT_EQ(job_contexts_deleted,deleted);UT_ASSERT_EQ(job_wakes,wakes);
		}
		job_ack(&job->cf);
		input_observation_read(&out); /* Receipt state changes only at the old poll. */
		UT_ASSERT_EQ(out.jobs[consumer].cf_state,CF_JOB_RECEIPT_RETIRING);
		UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job),WAL_INPUT_JOB_DONE);
		input_observation_read(&out);
		UT_ASSERT_EQ(out.present,1U<<consumer);
		UT_ASSERT_EQ(out.jobs[consumer].state,WAL_INPUT_JOB_STATE_DONE);
		UT_ASSERT(out.jobs[consumer].has_scope && !out.jobs[consumer].has_pin);
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_stop_census_v1(),WAL_INPUT_JOB_DONE);
		UT_ASSERT(job->inputs!=NULL && cluster_wal_inputs_count_v1(job->inputs)==0);
		UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&job),WAL_INPUT_JOB_DONE);
		input_observation_read(&out);UT_ASSERT_EQ(out.present,0);
		foundation_end();
	}
}
UT_TEST(input_observation_128_walr_acks_are_passive_at_original_ticks)
{
	for (unsigned role=0;role<2;++role) {
		uint16 threads[128];ClusterWalReadPinV1 *pin=NULL;ClusterWalInputsJobV1 *job;
		ClusterWalInputsJobsObservationV1 out;unsigned waits;
		void (*tick)(void)=role?bgwriter_tick:checkpointer_tick;
		unsigned consumer=role?WAL_INPUT_JOB_PI_REBUILD:WAL_INPUT_JOB_RETAINED_CUT;
		input_observation_begin(role!=0);
		for (unsigned i=0;i<128;++i)threads[i]=(uint16)(i+1);
		job=foundation_open((ClusterWalInputsConsumerV1)consumer);
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads,128,job,&pin),CLUSTER_WAL_PIN_OK);
		job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,job_resource_argument);
		waits=owner_wait_calls;
		for (unsigned remaining=128;remaining>0;--remaining) {
			ClusterWalInputsJobV1 before; unsigned calls,wakes,refs;
			tick();before=*job;calls=pin_native_calls;wakes=job_wakes;refs=pin_native_refs;
			input_observation_read(&out);input_observation_read(&out);
			UT_ASSERT_EQ(out.jobs[consumer].state,WAL_INPUT_JOB_STATE_RETIRING_WALR);
			UT_ASSERT_EQ(out.jobs[consumer].walr_next,job->walr_next);
			UT_ASSERT_EQ(out.jobs[consumer].pin_generation,pin->generation);
			UT_ASSERT(out.jobs[consumer].has_pin);
			UT_ASSERT(memcmp(&before,job,sizeof(before))==0);
			UT_ASSERT_EQ(pin_native_calls,calls);UT_ASSERT_EQ(pin_native_refs,refs);UT_ASSERT_EQ(job_wakes,wakes);
			pin_ack_member(&pin->locks[remaining-1].request,false);
			tick();UT_ASSERT_EQ(pin_native_refs,remaining-1);
		}
		input_observation_read(&out);
		UT_ASSERT_EQ(out.jobs[consumer].state,WAL_INPUT_JOB_STATE_DONE);
		UT_ASSERT(!out.jobs[consumer].has_pin);UT_ASSERT_EQ(owner_wait_calls,waits);
		pin=NULL;pin_cleanup(&job,&pin);
	}
}
UT_TEST(input_observation_never_confirms_cf_or_releases_walr)
{
	uint16 threads[]={1};ClusterWalReadPinV1 *pin=NULL;ClusterWalInputsJobV1 *job;
	ClusterWalInputsJobsObservationV1 out;unsigned waits,refs,wakes;
	input_observation_begin(false);job=foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads,1,job,&pin),CLUSTER_WAL_PIN_OK);
	g_seven_result=CLUSTER_LOCK_ACQUIRE_OK_GRANTED;UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock,NULL,&job->cf),CF_JOB_PENDING);
	job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,job_resource_argument);
	waits=owner_wait_calls;refs=pin_native_refs;wakes=job_wakes;
	for(unsigned i=0;i<4;++i) {
		input_observation_read(&out);
		UT_ASSERT_EQ(out.jobs[0].state,WAL_INPUT_JOB_STATE_RETIRING_CF);
		UT_ASSERT(cluster_lock_owner_request_usable(&pin->locks[0].request));
	}
	UT_ASSERT_EQ(owner_wait_calls,waits);UT_ASSERT_EQ(pin_native_refs,refs);UT_ASSERT_EQ(job_wakes,wakes);
	job_ack(&job->cf);input_observation_read(&out);
	UT_ASSERT_EQ(out.jobs[0].cf_state,CF_JOB_RECEIPT_RETIRING);
	checkpointer_tick();input_observation_read(&out);
	UT_ASSERT_EQ(out.jobs[0].state,WAL_INPUT_JOB_STATE_RETIRING_WALR);
	pin_cleanup(&job,&pin);
}
UT_TEST(input_observation_invalid_slot_dominates_prior_pending)
{
	ClusterWalInputsJobV1 *job;ClusterWalInputsJobsObservationV1 out;
	ClusterWalInputsJobSlotV1 saved;
	input_observation_begin(false);job=foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
	input_observation_read(&out);saved=inputs_jobs[3];inputs_jobs[3].reserved=1;
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_service_observe_v1(owner_epoch,&out),WAL_INPUT_JOBS_OBSERVATION_INVALID);
	input_observation_unknown(&out);inputs_jobs[3]=saved;
	foundation_finish(&job);foundation_end();
}
UT_TEST(input_observation_rejects_wrong_owner_and_context)
{
	ClusterWalInputsJobV1 *job;ClusterWalInputsJobsObservationV1 out;PGPROC *proc;
	input_observation_begin(false);job=foundation_open(WAL_INPUT_JOB_RETAINED_CUT);proc=MyProc;
	for (unsigned i=0;i<17;++i) {
		BackendType backend=MyBackendType;AuxProcType aux=MyAuxProcType;ResourceOwner owner=CurrentResourceOwner;
		PGPROC saved=*proc;int pid=MyProcPid;bool under=IsUnderPostmaster,enabled=cluster_enabled,shared=cluster_shared_config;
		uint64 epoch=owner_epoch;bool exiting=proc_exit_inprogress;int critical=CritSectionCount;
		switch(i) {
		case 0:MyBackendType=B_BACKEND;break;case 1:MyAuxProcType=BgWriterProcess;break;
		case 2:proc->backendId=0;break;case 3:proc->cluster_grd_generation=UINT64_MAX;break;
		case 4:proc->pid++;break;case 5:proc->pgprocno=-1;break;case 6:MyProc=NULL;break;
		case 7:IsUnderPostmaster=false;break;case 8:cluster_enabled=false;break;case 9:cluster_shared_config=false;break;
		case 10:CurrentResourceOwner=NULL;break;case 11:CritSectionCount=1;break;case 12:proc_exit_inprogress=true;break;
		case 13:job_observation_lock_held=true;break;case 14:epoch=0;break;case 15:epoch=UINT64_MAX;break;
		case 16:job->owner_proc_generation++;break;
		}
		memset(&out,0xa5,sizeof(out));
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_service_observe_v1(epoch,&out),WAL_INPUT_JOBS_OBSERVATION_INVALID);
		input_observation_unknown(&out);
		if(i==16)job->owner_proc_generation--;
		MyProc=proc;*proc=saved;MyProcPid=pid;MyBackendType=backend;MyAuxProcType=aux;
		CurrentResourceOwner=owner;IsUnderPostmaster=under;cluster_enabled=enabled;cluster_shared_config=shared;
		CritSectionCount=critical;proc_exit_inprogress=exiting;job_observation_lock_held=false;
	}
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_service_observe_v1(owner_epoch+1,&out),WAL_INPUT_JOBS_OBSERVATION_STALE);
	input_observation_unknown(&out);
	UT_ASSERT_EQ(cluster_wal_inputs_jobs_service_observe_v1(owner_epoch,NULL),WAL_INPUT_JOBS_OBSERVATION_INVALID);
	foundation_finish(&job);foundation_end();
}
static unsigned input_observation_drift_kind;
static ClusterWalInputsJobV1 *input_observation_drift_job;
static void
input_observation_drift(void)
{
	job_observation_barrier_hook=NULL;
	switch(input_observation_drift_kind) {
	case 0:owner_epoch++;break;case 1:MyProcPid++;break;case 2:MyProc->cluster_grd_generation++;break;
	case 3:MyBackendType=B_BG_WRITER;break;case 4:MyAuxProcType=BgWriterProcess;break;
	case 5:job_observation_lock_held=true;break;case 6:inputs_jobs[0].generation++;break;
	case 7:input_observation_drift_job->generation++;break;
	}
}
UT_TEST(input_observation_discards_identity_epoch_and_job_drift)
{
	ClusterWalInputsJobV1 *job;ClusterWalInputsJobsObservationV1 out;
	input_observation_begin(false);job=foundation_open(WAL_INPUT_JOB_RETAINED_CUT);input_observation_drift_job=job;
	for (unsigned i=0;i<8;++i) {
		PGPROC saved=*MyProc;int pid=MyProcPid;uint64 epoch=owner_epoch,generation=job->generation;
		input_observation_read(&out);input_observation_drift_kind=i;job_observation_barrier_hook=input_observation_drift;
		UT_ASSERT_EQ(cluster_wal_inputs_jobs_service_observe_v1(epoch,&out),i==0?WAL_INPUT_JOBS_OBSERVATION_STALE:WAL_INPUT_JOBS_OBSERVATION_INVALID);
		input_observation_unknown(&out);UT_ASSERT(job_observation_barrier_hook==NULL);
		*MyProc=saved;MyProcPid=pid;owner_epoch=epoch;MyBackendType=B_CHECKPOINTER;MyAuxProcType=CheckpointerProcess;
		job_observation_lock_held=false;job->generation=inputs_jobs[0].generation=generation;
	}
	foundation_finish(&job);foundation_end();
}
UT_TEST(input_observation_retained_done_detect_keeps_business_owner)
{
	ClusterWalInputsJobV1 *job;ClusterWalInputsJobsObservationV1 out;ClusterWalInputsV1 *scope;
	ClusterControlRootResult result;uint64 before;
	input_observation_begin(true);job=foundation_open(WAL_INPUT_JOB_PI_REBUILD);
	UT_ASSERT(cluster_wal_inputs_job_scope_create_v1(job));
	UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(job),WAL_INPUT_JOB_DONE);
	UT_ASSERT(cluster_wal_inputs_job_detect_next_v1(job,&scope,&result));
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_prepare_v1(job,&before),CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	g_seven_result=CLUSTER_LOCK_ACQUIRE_OK_GRANTED;UT_ASSERT(cluster_cf_lock(ShareLock));
	UT_ASSERT_EQ(cluster_wal_inputs_job_cf_release_v1(job,before),CF_JOB_PENDING);
	/* Actual DETECT state transition is driven by the original caller helper. */
	UT_ASSERT(!cluster_wal_inputs_job_detect_next_v1(job,&scope,&result));
	input_observation_read(&out);
	UT_ASSERT_EQ(out.jobs[3].state,WAL_INPUT_JOB_STATE_RETIRING_CF);
	UT_ASSERT(out.jobs[3].has_scope && !out.jobs[3].has_pin);
	UT_ASSERT((out.jobs[3].flags&WAL_INPUT_JOB_FLAG_DETECT)!=0);
	job_ack(&job->cf);UT_ASSERT_EQ(cluster_wal_inputs_job_poll_v1(job),WAL_INPUT_JOB_DONE);
	input_observation_read(&out);UT_ASSERT_EQ(out.jobs[3].state,WAL_INPUT_JOB_STATE_DONE);
	UT_ASSERT((out.jobs[3].flags&WAL_INPUT_JOB_FLAG_DETECT)!=0);
	foundation_finish(&job);foundation_end();
}
