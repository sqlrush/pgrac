/* Original complete merge consumer with the inherited explicit IR, WALR,
 * formation and source-service leaves. Confirmation getters link to the real
 * control owner. This is component wiring, not native acquisition evidence. */
#ifdef TEST_CONFIRM_MERGE_NAMESPACE
#include "test_cluster_startup_confirm_merge_namespace.h"
#endif
#define TEST_CLUSTER_CONFIRM_OWNER_BRIDGE 1
int confirm_merge_original_main(void);
#define main confirm_merge_original_main
#include "test_cluster_recovery_merge_seal.c"
#undef main
#include "test_cluster_startup_confirm_merge.h"

ClusterRecoveryFencePlan *
confirm_merge_begin(const ClusterControlRootSnapshot *root,
 const ClusterControlRootReadToken *token, uint64 *episode)
{
 ClusterRecoveryFencePlan *plan = suspend_fixture(250);
 if (!cluster_recovery_merge_fence_plan_suspend_serial_v1(plan, episode)) abort();
 /* Construct the component's independently selected single-origin baseline
  * before any confirmation exists; original native acquisition is a leaf. */
 plan->origin_count = 1;
 plan->origins[0].origin_thread = root->identity.origin_thread_id;
 plan->origins[0].duty = root->identity;
 plan->origins[0].root_snapshot = *root;
 plan->origins[0].root_token = *token;
 plan->serial_guards.guards[0].duty = root->identity;
 plan->serial_guards.guards[0].root_read_token = *token;
 roots[root->identity.origin_thread_id] = *root;
 tokens[root->identity.origin_thread_id] = *token;
 return plan;
}
void
confirm_merge_observe(const ClusterControlRootSnapshot *root,
 const ClusterControlRootReadToken *token, bool cf)
{
 roots[root->identity.origin_thread_id] = *root;
 tokens[root->identity.origin_thread_id] = *token;
 suspended_cf_exclusive = cf;
}
bool confirm_merge_current(ClusterRecoveryFencePlan *p, uint64 id)
{ return cluster_recovery_merge_fence_plan_suspended_current_v1(p,id); }
bool confirm_merge_held(ClusterRecoveryFencePlan *p, uint64 id)
{ return cluster_recovery_merge_fence_plan_suspended_held_current_v1(p,id); }
bool confirm_merge_phase(ClusterRecoveryFencePlan *p,uint64 id,
 const struct ClusterColdTypedV1 *t,const struct ClusterControlRelmapRecoveryCreateV2 *c)
{ return cluster_recovery_merge_fence_plan_suspended_confirm_current_v1(p,id,t,c); }
bool confirm_merge_install(ClusterRecoveryFencePlan *p,uint64 id,
 const struct ClusterColdTypedV1 *t,const struct ClusterControlRelmapRecoveryCreateV2 *c)
{ return cluster_recovery_merge_fence_plan_confirm_delta_v1(p,id,t,c); }
bool confirm_merge_baseline(ClusterRecoveryFencePlan *p,const ClusterControlRootReadToken *before)
{ return !memcmp(&p->origins[0].root_token,before,sizeof(*before)); }
bool confirm_merge_resume(ClusterRecoveryFencePlan *p,uint64 id,const ClusterControlRootReadToken *after)
{
 bool resumed;
 suspended_cf_exclusive = false;
 resumed = cluster_recovery_merge_fence_plan_resume_serial_v1(p,id);
 return resumed
  && p->serial_held && !p->serial_suspended
  && !memcmp(&p->serial_guards.guards[0].root_read_token,after,sizeof(*after));
}
bool confirm_merge_end(ClusterRecoveryFencePlan **p)
{
 bool ok;
 suspended_cf_exclusive = false;
 ok = cluster_recovery_merge_fence_plan_release_serial(*p);
 if (ok) cluster_recovery_merge_fence_plan_destroy(p);
 suspended_read_watch = NULL;
 return ok && *p == NULL && ut_failed_count == 0;
}
