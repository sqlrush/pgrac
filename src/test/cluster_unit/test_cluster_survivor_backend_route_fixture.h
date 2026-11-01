/* Connect actual query construction, status identity, signal dispatch,
 * ProcessInterrupts, backend producer and LMON poll in one host schedule.
 * It is not PostgresMain, OS signal delivery or global producer closure.
 * Author: SqlRush <sqlrush@gmail.com> */
static uint8 route_signal_bytes[offsetof(ProcSignalHeader,psh_slot)+2*sizeof(ProcSignalSlot)];
static void route_backend(void)
{
 MyProc=&proc;MyProcPid=1234;MyBackendId=1;MyBackendType=B_BACKEND;MyAuxProcType=NotAnAuxProcess;
 MyLatch=&MyProc->procLatch;MyProcSignalSlot=&ProcSignal->psh_slot[0];
}
static void route_lmon(void)
{
 MyProc=&snapshot_procs[1];MyProcPid=2345;MyBackendId=InvalidBackendId;
 MyBackendType=B_LMON;MyAuxProcType=LmonProcess;MyLatch=&MyProc->procLatch;
 MyProcSignalSlot=NULL;
}
static ClusterSurvivorLocalMailbox *route_mailbox(void)
{ return (ClusterSurvivorLocalMailbox *)survivor_local_owner_storage.reserved; }
static void route_put64(uint8 *p,uint64 value)
{ for(unsigned i=0;i<8;i++){p[i]=value&255;value>>=8;} }
static Snapshot
route_prepare(void)
{
 uint8 body[1536+80+2*48]={0},wire[192+sizeof(body)],format[32];
 ClusterSemanticActivationRecord open={0};ClusterSurvivorMessageV4 m={0};
 pg_sha256_ctx hash;Size written=0;Snapshot held;
 backend_return_begin();snapshot_return_epoch=8;held=registered(88);
 memset(&survivor_local_owner_storage,0,sizeof(survivor_local_owner_storage));
 memset(route_status,0,sizeof(route_status));
 route_status[0].st_changecount=2;route_status[0].st_procpid=1234;route_status[0].st_backendType=B_BACKEND;
 snapshot_procs[1].pid=2345;snapshot_procs[1].pgprocno=1;
 snapshot_procs[1].backendId=InvalidBackendId;snapshot_procs[1].cluster_grd_generation=17;
 ProcGlobal->allProcs=snapshot_procs;ProcGlobal->allProcCount=2;
 memset(route_signal_bytes,0,sizeof(route_signal_bytes));ProcSignal=(ProcSignalHeader *)route_signal_bytes;
 ProcSignal->psh_slot[0].pss_pid=1234;local_notifies=local_wakes=0;local_kill_result=0;local_latch_hook=NULL;
 InterruptPending=ProcDiePending=QueryCancelPending=ClientConnectionLost=false;
 InterruptHoldoffCount=QueryCancelHoldoffCount=CritSectionCount=0;DoingCommandRead=false;
 interrupt_lock_timeout=interrupt_statement_timeout=false;
 pg_atomic_write_u32(&backend_return_gate.inflight[1][63],9);
 route_lmon();
 /* Codec input is deliberately untrusted syntax, never a selected ROOT,
  * authenticated membership, producer receipt or assigned cluster format. */
 open.source_feature_bitmap=open.target_feature_bitmap=16;
 open.transition_epoch=7;open.record_generation=19;open.phase=CLUSTER_SEMANTIC_PHASE_OPEN;
 open.admitted_members_lo=3;open.coordinator_incarnation=17;open.capability_sample_digest=19;
 UT_ASSERT(cluster_semantic_activation_record_encode(&open,body));
 memset(body+512,0xb6,512);memset(format,0x33,32);memcpy(body+1104,format,32);
 route_put64(body+1136,1);route_put64(body+1144,9);route_put64(body+1152,11);
 route_put64(body+1160,19);route_put64(body+1168,5);route_put64(body+1176,3);
 for(unsigned node=0;node<2;node++) {
  uint8 *row=body+1616+48*node;row[0]=node;row[4]=1;
  for(unsigned field=0;field<5;field++)route_put64(row+8+8*field,100+node+field);
 }
 m.purpose=CLUSTER_SURVIVOR_SERVICE;m.kind=CLUSTER_SURVIVOR_REQUEST;
 m.stage=CLUSTER_SURVIVOR_QUIESCED;m.body_kind=CLUSTER_SURVIVOR_MANIFEST;
 m.nonce=5;m.epoch=8;m.formation=9;m.native_freeze=11;m.round_generation=13;m.features=16;
 m.survivors[0]=3;memcpy(m.format,format,32);memset(m.proof,0x44,32);
 pg_sha256_init(&hash);pg_sha256_update(&hash,body,1536);pg_sha256_final(&hash,m.basis);
 m.body=body;m.body_bytes=sizeof(body);
 UT_ASSERT(cluster_semantic_survivor_v4_encode(&m,format,wire,sizeof(wire),&written));
 UT_ASSERT_EQ(cluster_semantic_survivor_candidate_begin(&survivor_local_owner_storage,wire,written,format,0,NULL),LOCAL_PENDING);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.seq,2);
 return held;
}
static void route_begin(void)
{
 ClusterSurvivorLocalTarget target;
 UT_ASSERT(targets_read(0,&target));
 UT_ASSERT_EQ(target.pid,1234);UT_ASSERT_EQ(target.pgprocno,0);
 UT_ASSERT_EQ(target.backend_id,1);UT_ASSERT_EQ(target.proc_generation,11);
 UT_ASSERT_EQ(target.backend_type,B_BACKEND);UT_ASSERT_EQ(target.aux_type,(uint32)NotAnAuxProcess);
 UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&target),LOCAL_PENDING);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
}
static void route_deliver(void)
{
 route_backend();InterruptPending=false;
 procsignal_sigusr1_handler(SIGUSR1);
 ProcessInterrupts();
}
static void route_clear(Snapshot held)
{
 route_lmon();cluster_semantic_survivor_local_invalidate();
 route_backend();if(held!=NULL)UnregisterSnapshot(held);
 ProcDiePending=QueryCancelPending=ClientConnectionLost=InterruptPending=false;
 QueryCancelHoldoffCount=0;DoingCommandRead=false;
 pg_atomic_write_u32(&backend_return_gate.inflight[1][63],0);
 ProcSignal=NULL;MyProcSignalSlot=NULL;MyLatch=NULL;
}
UT_TEST(backend_route_returns_original_snapshot_and_observes_its_actual_release)
{
 ClusterSurvivorLocalReply r;Snapshot held=route_prepare();
 route_begin();UT_ASSERT_EQ(local_notifies,1);UT_ASSERT_EQ(local_notify_pid,1234);
 UT_ASSERT_EQ(route_mailbox()->query.backend_id,1);UT_ASSERT_EQ(route_mailbox()->query.native_epoch,8);
 route_deliver();UT_ASSERT(!InterruptPending);UT_ASSERT_EQ(local_wakes,2);
 route_lmon();UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_PENDING);
 UT_ASSERT_EQ(r.identity.pid,1234);UT_ASSERT_EQ(r.identity.pgprocno,0);UT_ASSERT_EQ(r.identity.proc_generation,11);
 UT_ASSERT_EQ(r.payload_bytes,1128);UT_ASSERT_EQ(r.gate_epoch,7);UT_ASSERT_EQ(r.record_generation,19);
 UT_ASSERT_EQ(survivor_local_scalar(r.body+40,4),CLUSTER_SERVICE_SNAPSHOT_REGISTERED);
 UT_ASSERT_EQ(survivor_local_scalar(r.body+80+127*4,4),9);
 UT_ASSERT_EQ(held->regd_count,1);UT_ASSERT_EQ(remembered,1);
 route_backend();UnregisterSnapshot(held);held=NULL;UT_ASSERT_EQ(remembered,0);
 route_lmon();route_begin();route_deliver();route_lmon();
 UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_PENDING);
 UT_ASSERT_EQ(r.payload_bytes,1128);UT_ASSERT_EQ(survivor_local_scalar(r.body+40,4),0);
 UT_ASSERT_EQ(survivor_local_scalar(r.body+32,8),0);UT_ASSERT_EQ(local_notifies,2);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.ordinary_admission,0);
 route_clear(held);
}
UT_TEST(backend_route_refuses_changed_generation_and_backend_id_before_notification)
{
 ClusterSurvivorLocalTarget target;Snapshot held=route_prepare();
 for(unsigned i=0;i<2;i++) {
  UT_ASSERT(targets_read(0,&target));if(i==0)target.proc_generation++;else target.backend_id++;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&target),LOCAL_REFUSED);
  UT_ASSERT_EQ(local_notifies,0);UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word),0);
 }
 route_clear(held);
}
UT_TEST(backend_route_requires_matching_original_status_identity)
{
 ClusterSurvivorLocalTarget target;Snapshot held=route_prepare();
 route_status[0].st_procpid=1235;UT_ASSERT(!targets_read(0,&target));UT_ASSERT_EQ(target.pid,0);
 route_status[0].st_procpid=1234;route_status[0].st_changecount=3;
 UT_ASSERT(!targets_read(0,&target));UT_ASSERT_EQ(target.pid,0);UT_ASSERT_EQ(local_notifies,0);
 route_clear(held);
}
UT_TEST(backend_route_notification_failure_cannot_publish_a_sample)
{
 ClusterSurvivorLocalTarget target;ClusterSurvivorLocalReply r;Snapshot held=route_prepare();
 UT_ASSERT(targets_read(0,&target));local_kill_result=-1;
 UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&target),LOCAL_PENDING);
 UT_ASSERT_EQ(local_notifies,1);UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
 route_deliver();route_lmon();UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_PENDING);
 UT_ASSERT_EQ(r.payload_bytes,0);UT_ASSERT_EQ(held->regd_count,1);route_clear(held);
}
UT_TEST(backend_route_cancel_preserves_query_until_original_return)
{
 ClusterSurvivorLocalReply r;Snapshot held=route_prepare();volatile bool caught=false;
 route_begin();route_backend();QueryCancelPending=true;procsignal_sigusr1_handler(SIGUSR1);
 PG_TRY();{ProcessInterrupts();}PG_CATCH();{caught=true;}PG_END_TRY();
 UT_ASSERT(caught);UT_ASSERT_EQ(error_code,ERRCODE_QUERY_CANCELED);
 route_lmon();UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_PENDING);
 UT_ASSERT_EQ(r.payload_bytes,0);UT_ASSERT_EQ(local_notifies,1);
 route_backend();InterruptPending=true;ProcessInterrupts();route_lmon();
 UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_PENDING);UT_ASSERT_EQ(r.payload_bytes,1128);
 UT_ASSERT_EQ(held->regd_count,1);route_clear(held);
}
UT_TEST(backend_route_holdoff_uses_original_interrupt_reentry)
{
 ClusterSurvivorLocalReply r;Snapshot held=route_prepare();
 route_begin();route_backend();QueryCancelPending=true;QueryCancelHoldoffCount=1;DoingCommandRead=true;
 procsignal_sigusr1_handler(SIGUSR1);ProcessInterrupts();
 UT_ASSERT(InterruptPending && QueryCancelPending);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
 QueryCancelHoldoffCount=0;ProcessInterrupts();UT_ASSERT(!InterruptPending && !QueryCancelPending);
 route_lmon();UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_PENDING);
 UT_ASSERT_EQ(r.payload_bytes,1128);UT_ASSERT_EQ(local_notifies,1);route_clear(held);
}
static void route_drift(unsigned kind)
{
 ClusterSurvivorLocalReply r,zero={0};Snapshot held=route_prepare();
 route_begin();route_deliver();route_lmon();
 if(kind==0)pg_atomic_write_u64(&backend_return_gate.record_generation,20);
 if(kind==1)survivor_local_owner_storage.header.round_generation++;
 if(kind==2)proc.cluster_grd_generation++;
 memset(&r,0xa5,sizeof(r));UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_STALE);
 UT_ASSERT_EQ(memcmp(&r,&zero,sizeof(r)),0);UT_ASSERT_EQ(held->regd_count,1);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);route_clear(held);
}
UT_TEST(backend_route_gate_change_withdraws_published_sample) {route_drift(0);}
UT_TEST(backend_route_episode_change_withdraws_published_sample) {route_drift(1);}
UT_TEST(backend_route_target_replacement_withdraws_published_sample) {route_drift(2);}
