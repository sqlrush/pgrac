/* LMON query -> original auxiliary prefix -> actual input job -> LMON poll.
 * Process roster/scheduling/latch delivery remain host inputs. Metadata is
 * untrusted and all delivered observations remain unconfirmed.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool input_route_bgwriter;
static void
input_route_aux(void)
{
	MyProc=&job_procs[42];MyProcPid=MyProc->pid;MyBackendId=InvalidBackendId;
	MyBackendType=input_route_bgwriter?B_BG_WRITER:B_CHECKPOINTER;
	MyAuxProcType=input_route_bgwriter?BgWriterProcess:CheckpointerProcess;
	MyLatch=&MyProc->procLatch;
}
static void
input_route_lmon(void)
{
	MyProc=&job_procs[41];MyProcPid=MyProc->pid;MyBackendId=InvalidBackendId;
	MyBackendType=B_LMON;MyAuxProcType=LmonProcess;MyLatch=&MyProc->procLatch;
}
static ClusterSurvivorLocalMailbox *input_route_mailbox(void)
{
	return (ClusterSurvivorLocalMailbox *)survivor_local_owner_storage.reserved;
}
static ClusterSurvivorTargetCensus *input_route_census(void)
{
	return (ClusterSurvivorTargetCensus *)(survivor_local_owner_storage.reserved+CLUSTER_SURVIVOR_LOCAL_BYTES);
}
static void input_route_put(uint8 *p,uint64 v)
{
	for(unsigned i=0;i<8;i++){p[i]=v&255;v>>=8;}
}
static void
input_route_prepare(bool bgwriter)
{
	uint8 body[1536+80+2*48]={0},wire[192+sizeof(body)],format[32];
	ClusterSemanticActivationRecord open={0};ClusterSurvivorMessageV4 m={0};
	pg_sha256_ctx hash;Size written=0;
	input_route_bgwriter=bgwriter;input_route_aux();input_observation_begin(bgwriter);
	memset(&survivor_local_owner_storage,0,sizeof(survivor_local_owner_storage));
	memset(&input_route_gate,0,sizeof(input_route_gate));
	SemanticActivationShmem=&input_route_gate;
	pg_atomic_init_u64(&input_route_gate.admission_seq,2);
	pg_atomic_init_u64(&input_route_gate.active_bits,16);
	pg_atomic_init_u64(&input_route_gate.record_generation,19);
	pg_atomic_init_u64(&input_route_gate.formation_epoch,owner_epoch);
	pg_atomic_init_u32(&input_route_gate.transition_closed,1);
	pg_atomic_init_u64(&input_route_grd.next_generation,32);
	memset(input_route_status,0,sizeof(input_route_status));
	input_route_status[MaxBackends+MyAuxProcType].st_changecount=2;
	input_route_status[MaxBackends+MyAuxProcType].st_procpid=MyProcPid;
	input_route_status[MaxBackends+MyAuxProcType].st_backendType=MyBackendType;
	memset(&job_procs[41],0,sizeof(job_procs[41]));
	job_procs[41].pid=4101;job_procs[41].pgprocno=41;
	job_procs[41].backendId=InvalidBackendId;job_procs[41].cluster_grd_generation=17;
	input_route_queries=input_route_replies=0;
	input_route_lmon();
	/* Valid grammar is not an authenticated episode, ROOT or format id. */
	open.source_feature_bitmap=open.target_feature_bitmap=16;
	open.transition_epoch=owner_epoch;open.record_generation=19;
	open.phase=CLUSTER_SEMANTIC_PHASE_OPEN;open.admitted_members_lo=3;
	open.coordinator_incarnation=17;open.capability_sample_digest=19;
	UT_ASSERT(cluster_semantic_activation_record_encode(&open,body));
	memset(body+512,0xb6,512);memset(format,0x33,32);memcpy(body+1104,format,32);
	input_route_put(body+1136,1);input_route_put(body+1144,9);input_route_put(body+1152,11);
	input_route_put(body+1160,19);input_route_put(body+1168,5);input_route_put(body+1176,3);
	for(unsigned node=0;node<2;node++) {
		uint8 *row=body+1616+48*node;row[0]=node;row[4]=1;
		for(unsigned f=0;f<5;f++)input_route_put(row+8+8*f,100+node+f);
	}
	m.purpose=CLUSTER_SURVIVOR_SERVICE;m.kind=CLUSTER_SURVIVOR_REQUEST;
	m.stage=CLUSTER_SURVIVOR_QUIESCED;m.body_kind=CLUSTER_SURVIVOR_MANIFEST;
	m.nonce=5;m.epoch=owner_epoch;m.formation=9;m.native_freeze=11;
	m.round_generation=13;m.features=16;m.survivors[0]=3;
	memcpy(m.format,format,32);memset(m.proof,0x44,32);
	pg_sha256_init(&hash);pg_sha256_update(&hash,body,1536);pg_sha256_final(&hash,m.basis);
	m.body=body;m.body_bytes=sizeof(body);
	UT_ASSERT(cluster_semantic_survivor_v4_encode(&m,format,wire,sizeof(wire),&written));
	UT_ASSERT_EQ(cluster_semantic_survivor_candidate_begin(&survivor_local_owner_storage,
		wire,written,format,0,NULL),LOCAL_PENDING);
	input_route_aux();
}
static void
input_route_begin(void)
{
	ClusterSurvivorLocalTarget target;
	input_route_lmon();UT_ASSERT(targets_read(42,&target));
	UT_ASSERT_EQ(target.backend_id,InvalidBackendId);
	UT_ASSERT_EQ(target.family,CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS);
	UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&target),LOCAL_PENDING);
	UT_ASSERT_EQ(pg_atomic_read_u64(&input_route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
	input_route_aux();UT_ASSERT(MyLatch->is_set);
}
static ClusterSurvivorLocalReply
input_route_poll(void)
{
	ClusterSurvivorLocalReply r;
	input_route_lmon();UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_PENDING);
	UT_ASSERT_EQ(r.payload_bytes,1128);UT_ASSERT_EQ(r.identity.pid,4002);
	UT_ASSERT_EQ(r.identity.pgprocno,42);UT_ASSERT_EQ(r.identity.proc_generation,1);
	UT_ASSERT_EQ(r.identity.backend_id,InvalidBackendId);
	UT_ASSERT_EQ(r.family,CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS);
	UT_ASSERT_EQ(r.record_generation,19);UT_ASSERT_EQ(r.gate_epoch,owner_epoch);
	UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
	UT_ASSERT_EQ(survivor_local_owner_storage.header.ordinary_admission,0);
	UT_ASSERT_EQ(survivor_local_owner_storage.header.typed_status,0);
	input_route_aux();return r;
}
static void
input_route_end(void)
{
	ClusterSurvivorLocalReply r;
	input_route_lmon();cluster_semantic_survivor_local_invalidate();
	(void)cluster_semantic_survivor_local_poll(&r);
	UT_ASSERT_EQ(pg_atomic_read_u64(&input_route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
	input_route_aux();foundation_end();job_procs[41].pid=0;SemanticActivationShmem=NULL;
}
UT_TEST(input_route_cf_and_128_walr_follow_original_ack_ticks)
{
	for(unsigned role=0;role<2;role++) {
		uint16 threads[128];ClusterWalReadPinV1 *pin=NULL;ClusterWalInputsJobV1 *job;
		ClusterSurvivorLocalReply r;unsigned waits,calls,wakes,refs,consumer;
		void (*tick)(void)=role?bgwriter_tick:checkpointer_tick;
		input_route_prepare(role!=0);consumer=role?WAL_INPUT_JOB_PI_REBUILD:WAL_INPUT_JOB_RETAINED_CUT;
		job=foundation_open((ClusterWalInputsConsumerV1)consumer);
		for(unsigned i=0;i<128;i++)threads[i]=i+1;
		UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads,128,job,&pin),CLUSTER_WAL_PIN_OK);
		g_seven_result=CLUSTER_LOCK_ACQUIRE_OK_GRANTED;UT_ASSERT(cluster_cf_lock(ShareLock));
		UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock,NULL,&job->cf),CF_JOB_PENDING);
		job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,job_resource_argument);
		waits=owner_wait_calls;calls=pin_native_calls;wakes=job_wakes;
		input_route_lmon();cluster_semantic_survivor_targets_tick();
		UT_ASSERT_EQ(input_route_census()->phase,CLUSTER_SURVIVOR_TARGETS_WAIT);
		UT_ASSERT_EQ(input_route_census()->cursor,42);UT_ASSERT_EQ(input_route_census()->unknown,1);
		input_route_aux();tick();
		UT_ASSERT_EQ(pin_native_calls,calls);UT_ASSERT_EQ(job_wakes,wakes);
		r=input_route_mailbox()->reply;
		UT_ASSERT_EQ(survivor_local_scalar(r.body+632+consumer*32+16,4),WAL_INPUT_JOB_STATE_RETIRING_CF);
		input_route_lmon();cluster_semantic_survivor_targets_tick();
		UT_ASSERT_EQ(input_route_census()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
		UT_ASSERT_EQ(input_route_census()->observed,1);UT_ASSERT_EQ(input_route_census()->attempts,1);
		input_route_aux();job_ack(&job->cf);tick();
		for(unsigned remaining=128;remaining>0;remaining--) {
			tick();calls=pin_native_calls;wakes=job_wakes;refs=pin_native_refs;
			input_route_begin();tick();r=input_route_poll();
			UT_ASSERT_EQ(survivor_local_scalar(r.body+632+consumer*32,8),job->generation);
			UT_ASSERT_EQ(survivor_local_scalar(r.body+632+consumer*32+8,8),pin->generation);
			UT_ASSERT_EQ(survivor_local_scalar(r.body+632+consumer*32+16,4),WAL_INPUT_JOB_STATE_RETIRING_WALR);
			UT_ASSERT_EQ(pin_native_calls,calls);UT_ASSERT_EQ(job_wakes,wakes);UT_ASSERT_EQ(pin_native_refs,refs);
			pin_ack_member(&pin->locks[remaining-1].request,false);tick();
			UT_ASSERT_EQ(pin_native_refs,remaining-1);
		}
		UT_ASSERT(job->pin==NULL && active_read_pin==NULL);pin=NULL;
		input_route_begin();tick();r=input_route_poll();
		UT_ASSERT_EQ(survivor_local_scalar(r.body+632+consumer*32+16,4),WAL_INPUT_JOB_STATE_DONE);
		UT_ASSERT_EQ(survivor_local_scalar(r.body+624,4),1U<<consumer);
		foundation_finish(&job);input_route_begin();tick();r=input_route_poll();
		UT_ASSERT_EQ(survivor_local_scalar(r.body+624,4),0);
		UT_ASSERT_EQ(owner_wait_calls,waits);UT_ASSERT_EQ(input_route_queries,131);
		UT_ASSERT_EQ(input_route_replies,input_route_queries);input_route_end();
	}
}
static void
input_route_query_during_reset(void)
{
	ack_during_reset=NULL;input_route_begin();
}
UT_TEST(input_route_query_at_original_reset_is_not_lost)
{
	for(unsigned role=0;role<2;role++) {
		ClusterWalInputsJobV1 *job;ClusterSurvivorLocalReply r;unsigned wakes,consumer;
		input_route_prepare(role!=0);consumer=role?WAL_INPUT_JOB_PI_NOTICE:WAL_INPUT_JOB_PI_BATCH;
		job=foundation_open((ClusterWalInputsConsumerV1)consumer);
		wakes=job_wakes;ack_during_reset=input_route_query_during_reset;
		if(role)bgwriter_tick();else checkpointer_tick();
		UT_ASSERT(ack_during_reset==NULL);r=input_route_poll();
		UT_ASSERT_EQ(survivor_local_scalar(r.body+632+consumer*32,8),job->generation);
		UT_ASSERT_EQ(survivor_local_scalar(r.body+632+consumer*32+16,4),WAL_INPUT_JOB_STATE_NEW);
		UT_ASSERT_EQ(job_wakes,wakes);UT_ASSERT_EQ(input_route_queries,1);UT_ASSERT_EQ(input_route_replies,1);
		foundation_finish(&job);input_route_end();
	}
}
UT_TEST(input_route_pending_scope_drift_keeps_original_debt)
{
	for(unsigned fault=0;fault<3;fault++) {
		ClusterWalInputsJobV1 *job;ClusterSurvivorLocalReply r,zero={0};
		uint64 generation;unsigned calls,wakes;
		input_route_prepare(false);job=foundation_open(WAL_INPUT_JOB_RETAINED_CUT);
		g_seven_result=CLUSTER_LOCK_ACQUIRE_OK_GRANTED;UT_ASSERT(cluster_cf_lock(ShareLock));
		UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock,NULL,&job->cf),CF_JOB_PENDING);
		job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,job_resource_argument);
		input_route_begin();checkpointer_tick();calls=pin_native_calls;wakes=job_wakes;
		generation=job_procs[42].cluster_grd_generation;
		if(fault==0)pg_atomic_write_u64(&input_route_gate.record_generation,20);
		if(fault==1)survivor_local_owner_storage.header.round_generation++;
		if(fault==2)job_procs[42].cluster_grd_generation++;
		input_route_lmon();memset(&r,0xa5,sizeof(r));
		UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_STALE);
		UT_ASSERT_EQ(memcmp(&r,&zero,sizeof(r)),0);
		UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
		input_route_aux();UT_ASSERT_EQ(job->state,WAL_INPUT_JOB_STATE_RETIRING_CF);
		UT_ASSERT_EQ(job->cf.state,CF_JOB_RECEIPT_RETIRING);
		UT_ASSERT_EQ(pin_native_calls,calls);UT_ASSERT_EQ(job_wakes,wakes);
		job_procs[42].cluster_grd_generation=generation;
		job_ack(&job->cf);checkpointer_tick();foundation_finish(&job);input_route_end();
	}
}
UT_TEST(input_route_unknown_status_never_counts_as_returned)
{
	for(unsigned role=0;role<2;role++) {
		ClusterSurvivorTargetCensus saved;ClusterWalInputsJobV1 *job;
		unsigned status;
		input_route_prepare(role!=0);job=foundation_open(role?WAL_INPUT_JOB_PI_NOTICE:WAL_INPUT_JOB_PI_BATCH);
		status=MaxBackends+MyAuxProcType;input_route_status[status].st_changecount=3;
		input_route_lmon();cluster_semantic_survivor_targets_tick();
		UT_ASSERT_EQ(input_route_census()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
		UT_ASSERT_EQ(input_route_census()->unknown,2);UT_ASSERT_EQ(input_route_census()->observed,0);
		UT_ASSERT_EQ(input_route_queries,0);saved=*input_route_census();
		input_route_status[status].st_changecount=4;
		for(unsigned wake=0;wake<4;wake++)cluster_semantic_survivor_targets_tick();
		UT_ASSERT_EQ(memcmp(&saved,input_route_census(),sizeof(saved)),0);
		UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
		input_route_aux();UT_ASSERT_EQ(job->state,WAL_INPUT_JOB_STATE_NEW);
		foundation_finish(&job);input_route_end();
	}
}
