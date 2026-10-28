/* PGRAC: actual E1 merge-claim ownership with private real filesystem I/O.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <fcntl.h>
#include <setjmp.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "postmaster/startup.h"
#include "utils/errcodes.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();
#define TEST_REQUIRE(x) do { if (!(x)) { fprintf(stderr,"fixture prerequisite %s at %d\n",#x,__LINE__); abort(); } } while (0)

int cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
char *cluster_shared_data_dir;
char *cluster_wal_threads_dir = "private-wal";
int cluster_node_id = 0;
int cluster_cssd_heartbeat_interval_ms = 100;
int cluster_cssd_dead_deadband_factor = 1;
ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
static ResourceReleaseCallback release_cb;
static void *release_arg;
static uint64 self_boot = 17;
static pid_t owner_pid = 12345;
static jmp_buf failure;
static int report_level, last_state;
static char report_message[512];
static int tracked_fds[128], tracked_count;
static bool sync_error, close_error;
static bool test_cf_held, cf_available=true, cf_clusterwide=true, cf_release_ok=true;
static uint64 cf_cookie=1;
static void (*read_hook)(void);
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
/* CF is intentionally a hard-refusal/grant leaf in this filesystem suite.
 * The actual wait producer and consumer run in test_cluster_startup_cf_entry. */
bool InRecovery, cluster_enabled = true;
bool cluster_shared_config = true, cluster_controlfile_shared_authority = true;
volatile sig_atomic_t ShutdownRequestPending, InterruptPending;
volatile uint32 InterruptHoldoffCount, CritSectionCount;
BackendType MyBackendType = B_STARTUP;
AuxProcType MyAuxProcType = StartupProcess;
PGPROC *MyProc = (PGPROC *)1;
Latch *MyLatch;
LWLockPadded *MainLWLockArray;
uint64
cluster_epoch_get_current(void)
{
	return 1;
}
uint64
cluster_grd_redeclare_generation(void)
{
	return 1;
}
uint64
cluster_cf_attempt_sequence(void)
{
	return 0;
}
bool
cluster_cf_last_attempt_since(uint64 before, ClusterCfAttemptObservation *out)
{
	TEST_REQUIRE(before == 0 && out != NULL);
	return false;
}
bool
cluster_cf_attempt_wait_slice(const ClusterCfAttemptObservation *out)
{
	TEST_REQUIRE(false);
	return false;
}
bool
cluster_cf_retirement_snapshot(LOCKMODE mode, const void *caller, ClusterCfRetirementSnapshot *out)
{
	TEST_REQUIRE(false);
	return false;
}
bool
cluster_cf_release_completed(LOCKMODE mode, uint64 cookie)
{
	TEST_REQUIRE(false);
	return false;
}
bool
cluster_recovery_transport_components_current(void)
{
	TEST_REQUIRE(false);
	return false;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	TEST_REQUIRE(false);
	return false;
}
void
ResetLatch(Latch *latch)
{
	TEST_REQUIRE(false);
}
int
WaitLatch(Latch *latch, int events, long timeout, uint32 event)
{
	TEST_REQUIRE(false);
	return 0;
}
void
cluster_lock_owners_service_poll(void)
{
	TEST_REQUIRE(false);
}
void
cluster_cf_retirement_poll(void)
{
	TEST_REQUIRE(false);
}
void
ProcessInterrupts(void)
{
	TEST_REQUIRE(false);
}
void pg_re_throw(void)
{ if (PG_exception_stack!=NULL) siglongjmp(*PG_exception_stack,1); longjmp(failure,1); }
bool cluster_cf_lock(LOCKMODE mode)
{
 TEST_REQUIRE(mode==ExclusiveLock);
 if (test_cf_held || !cf_available) return false;
 test_cf_held=true;cf_cookie++;return true;
}
bool cluster_cf_held(LOCKMODE mode) { return mode==ExclusiveLock && test_cf_held; }
bool cluster_cf_held_is_clusterwide(LOCKMODE mode) { return cluster_cf_held(mode) && cf_clusterwide; }
uint64 cluster_cf_owner_cookie(LOCKMODE mode) { return cluster_cf_held(mode) ? cf_cookie : 0; }
ClusterCfReleaseResult cluster_cf_unlock_confirmed(LOCKMODE mode)
{
 TEST_REQUIRE(mode==ExclusiveLock && test_cf_held);
 if (!cf_release_ok) return CLUSTER_CF_RELEASE_UNCONFIRMED;
 test_cf_held=false;return CLUSTER_CF_RELEASE_CONFIRMED;
}
static ssize_t claim_pread(int fd, void *out, size_t size, off_t offset)
{ if (read_hook!=NULL) { void (*hook)(void)=read_hook;read_hook=NULL;hook(); } return pread(fd,out,size,offset); }
static void (*poll_hook)(void);
static int polls;
static char scratch[MAXPGPATH], claim_path[MAXPGPATH];

void ExceptionalCondition(const char *c, const char *f, int l)
{ fprintf(stderr, "Assert %s at %s:%d\n", c, f, l); abort(); }
uint16 cluster_wal_thread_id(void) { return 1; }
uint64 GetSystemIdentifier(void) { return UINT64CONST(0x123456789abc); }
uint64 cluster_qvotec_get_self_incarnation(void) { return self_boot; }
void RegisterResourceReleaseCallback(ResourceReleaseCallback cb, void *arg)
{ release_cb = cb; release_arg = arg; }
void UnregisterResourceReleaseCallback(ResourceReleaseCallback cb, void *arg)
{ UT_ASSERT(cb == release_cb && arg == release_arg); release_cb = NULL; }
int OpenTransientFile(const char *path, int flags)
{
 int fd = open(path, flags, 0600);
 if (fd >= 0) { TEST_REQUIRE(tracked_count < lengthof(tracked_fds)); tracked_fds[tracked_count++] = fd; }
 return fd;
}
int CloseTransientFile(int fd)
{
 int i;
 if (close_error) { errno = EIO; return -1; }
 for (i = 0; i < tracked_count; i++) if (tracked_fds[i] == fd) tracked_fds[i] = -1;
 return close(fd);
}
int pg_fsync(int fd) { if (sync_error) { errno=EIO; return -1; } return fsync(fd); }
int MakePGDirectory(const char *path) { return mkdir(path, 0700); }
void fsync_fname(const char *path, bool isdir)
{
 int fd;
 UT_ASSERT(isdir);
 if (sync_error) pg_re_throw();
 fd = open(path, O_RDONLY); TEST_REQUIRE(fd >= 0); TEST_REQUIRE(fsync(fd) == 0); close(fd);
}
const ClusterICPeerStateShmem *cluster_ic_tier1_peer_get(int node)
{
 static ClusterICPeerStateShmem peer;
 UT_ASSERT_EQ(node, 1); peer.state = CLUSTER_IC_PEER_CONNECTED; return &peer;
}
void HandleStartupProcInterrupts(void) { if (poll_hook != NULL) poll_hook(); }
void pg_usleep(long us) { UT_ASSERT_EQ(us, 100000); if (++polls > 3) longjmp(failure, 1); }
static int claim_errcode(int code) { last_state = code; return 0; }
int errcode_for_file_access(void) { return claim_errcode(ERRCODE_IO_ERROR); }
static int claim_message(const char *fmt, ...)
{
 va_list ap; va_start(ap,fmt); vsnprintf(report_message,sizeof(report_message),fmt,ap); va_end(ap); return 0;
}
static int claim_detail(const char *fmt pg_attribute_unused(), ...) { return 0; }
static void claim_report_end(void) { if (report_level >= ERROR) pg_re_throw(); }
#undef ereport
#undef errcode
#undef errmsg
#undef errdetail
#undef errhint
#define errcode(code) claim_errcode(code)
#define errmsg(...) claim_message(__VA_ARGS__)
#define errdetail(...) claim_detail(__VA_ARGS__)
#define errhint(...) claim_detail(__VA_ARGS__)
#define ereport(level, rest) do { report_level = (level); (void)(rest); claim_report_end(); } while (0)
#define getpid() owner_pid
#undef pg_pread
#define pg_pread claim_pread
#include "test_cluster_merge_claim_product.inc"
#undef getpid
#undef pg_pread

/* Test-only cleanup of the private directory, never an owner recovery path. */
static void reset(void)
{
 char global[MAXPGPATH], pattern[] = "/tmp/pgrac-merge-claim-XXXXXX";
 for (int i=0;i<tracked_count;i++) if (tracked_fds[i]>=0) close(tracked_fds[i]);
 tracked_count=0; merge_claim_held=false; sync_error=close_error=false;
 CurrentResourceOwner=(ResourceOwner)(uintptr_t)1; self_boot=17; owner_pid=12345;
 cluster_shared_data_dir=scratch; cluster_wal_threads_dir="private-wal";
 test_cf_held=false;cf_available=cf_clusterwide=cf_release_ok=true;read_hook=NULL;
 polls=0;poll_hook=NULL;last_state=0;report_message[0]=0;
 TEST_REQUIRE(mkdtemp(pattern)!=NULL); strlcpy(scratch,pattern,sizeof(scratch));
 snprintf(global,sizeof(global),"%s/global",scratch);TEST_REQUIRE(mkdir(global,0700)==0);
 snprintf(claim_path,sizeof(claim_path),"%s/pgrac_merge.claim",global);
}
static void put_claim(const char *path, int node)
{
 ClusterMergeClaimFile f; int fd; memset(&f,0,sizeof(f));
 cluster_merge_claim_build(&f,node,UINT64CONST(0x123456789abc));
 fd=open(path,O_WRONLY|O_CREAT|O_TRUNC,0600);TEST_REQUIRE(fd>=0);
 TEST_REQUIRE(write(fd,&f,sizeof(f))==sizeof(f));TEST_REQUIRE(fsync(fd)==0);close(fd);
}
static bool exists(const char *path) { struct stat st; return lstat(path,&st)==0; }
static bool acquire_refused(void)
{ if (setjmp(failure)==0) { cluster_recovery_merge_claim_acquire_blocking();return false; } return true; }
static bool release_refused(void)
{ if (setjmp(failure)==0) { cluster_recovery_merge_claim_release_if_held();return false; } return true; }
UT_TEST(original_owner_links_and_releases)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();UT_ASSERT(exists(claim_path));
 UT_ASSERT(cluster_recovery_merge_claim_is_held());cluster_recovery_merge_claim_acquire_blocking();
 cluster_recovery_merge_claim_release_if_held();UT_ASSERT(!exists(claim_path));UT_ASSERT(!cluster_recovery_merge_claim_is_held());
 cluster_recovery_merge_claim_release_if_held();
}
UT_TEST(same_node_persistent_claim_is_not_adopted)
{
 reset();put_claim(claim_path,0);UT_ASSERT(acquire_refused());UT_ASSERT(!cluster_recovery_merge_claim_is_held());
 UT_ASSERT(exists(claim_path));UT_ASSERT_EQ(last_state,ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
}
UT_TEST(replacement_file_is_not_unlinked)
{
 char old[MAXPGPATH]; reset();cluster_recovery_merge_claim_acquire_blocking();
 snprintf(old,sizeof(old),"%s/old",scratch);TEST_REQUIRE(rename(claim_path,old)==0);put_claim(claim_path,0);
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(release_refused());
 UT_ASSERT(exists(claim_path));UT_ASSERT(exists(old));
}
UT_TEST(changed_original_bytes_are_not_released)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();put_claim(claim_path,1);
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(release_refused());UT_ASSERT(exists(claim_path));
}
UT_TEST(missing_original_file_is_not_confirmed_release)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();TEST_REQUIRE(unlink(claim_path)==0);
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(release_refused());
}
UT_TEST(replaced_symlink_is_not_unlinked)
{
 char old[MAXPGPATH]; reset();cluster_recovery_merge_claim_acquire_blocking();
 snprintf(old,sizeof(old),"%s/old",scratch);TEST_REQUIRE(rename(claim_path,old)==0);TEST_REQUIRE(symlink(old,claim_path)==0);
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(release_refused());UT_ASSERT(exists(claim_path));
}
UT_TEST(other_process_cannot_reenter_or_release)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();owner_pid++;
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(acquire_refused());UT_ASSERT(release_refused());UT_ASSERT(exists(claim_path));
}
UT_TEST(other_resource_owner_cannot_reenter_or_release)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();CurrentResourceOwner=(ResourceOwner)(uintptr_t)2;
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(acquire_refused());UT_ASSERT(release_refused());UT_ASSERT(exists(claim_path));
}
UT_TEST(changed_boot_cannot_reenter_or_release)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();self_boot++;
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(acquire_refused());UT_ASSERT(release_refused());UT_ASSERT(exists(claim_path));
}
UT_TEST(original_owner_release_revokes_without_unlink)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();UT_ASSERT(release_cb!=NULL);
 if (release_cb!=NULL) release_cb(RESOURCE_RELEASE_BEFORE_LOCKS,false,false,release_arg);
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(release_refused());UT_ASSERT(exists(claim_path));
}
UT_TEST(directory_sync_failure_preserves_release_debt)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();sync_error=true;
 UT_ASSERT(release_refused());UT_ASSERT(merge_claim_held);UT_ASSERT(!test_cf_held);
}
UT_TEST(unconfigured_start_does_not_create_claim)
{
 reset();cluster_wal_threads_dir=NULL;cluster_recovery_merge_claim_acquire_blocking();
 UT_ASSERT(!exists(claim_path));UT_ASSERT(!cluster_recovery_merge_claim_is_held());
}
static void peer_release(void) { UT_ASSERT(!test_cf_held);TEST_REQUIRE(unlink(claim_path)==0);poll_hook=NULL; }
UT_TEST(peer_wait_uses_original_atomic_claim)
{
 reset();put_claim(claim_path,1);poll_hook=peer_release;cluster_recovery_merge_claim_acquire_blocking();
 UT_ASSERT(cluster_recovery_merge_claim_is_held());UT_ASSERT_EQ(polls,1);
 cluster_recovery_merge_claim_release_if_held();UT_ASSERT(!exists(claim_path));
}
UT_TEST(replaced_directory_is_not_released)
{
 char global[MAXPGPATH], old[MAXPGPATH], replacement[MAXPGPATH];
 reset();cluster_recovery_merge_claim_acquire_blocking();
 snprintf(global,sizeof(global),"%s/global",scratch);snprintf(old,sizeof(old),"%s/old-dir",scratch);
 TEST_REQUIRE(rename(global,old)==0);TEST_REQUIRE(mkdir(global,0700)==0);put_claim(claim_path,0);
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(release_refused());UT_ASSERT(exists(claim_path));
 snprintf(replacement,sizeof(replacement),"%s/pgrac_merge.claim",old);UT_ASSERT(exists(replacement));
}
UT_TEST(released_other_owner_does_not_revoke_original)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();CurrentResourceOwner=(ResourceOwner)(uintptr_t)2;
 UT_ASSERT(release_cb!=NULL);if (release_cb==NULL) return;
 release_cb(RESOURCE_RELEASE_BEFORE_LOCKS,false,false,release_arg);
 CurrentResourceOwner=(ResourceOwner)(uintptr_t)1;UT_ASSERT(cluster_recovery_merge_claim_is_held());
 cluster_recovery_merge_claim_release_if_held();UT_ASSERT(!exists(claim_path));
}
UT_TEST(owner_loss_is_sticky_after_identity_returns)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();self_boot++;
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());self_boot--;
 UT_ASSERT(!cluster_recovery_merge_claim_is_held());UT_ASSERT(release_refused());UT_ASSERT(exists(claim_path));
}
UT_TEST(close_failure_preserves_release_debt)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();close_error=true;
 UT_ASSERT(release_refused());UT_ASSERT(merge_claim_held);UT_ASSERT(!test_cf_held);
}
UT_TEST(stale_staging_hardlink_cannot_truncate_claim)
{
 char staging[MAXPGPATH];ClusterMergeClaimFile before,after;int fd;
 reset();put_claim(claim_path,1);snprintf(staging,sizeof(staging),"%s.n0.tmp",claim_path);
 TEST_REQUIRE(link(claim_path,staging)==0);fd=open(claim_path,O_RDONLY);TEST_REQUIRE(fd>=0);
 TEST_REQUIRE(read(fd,&before,sizeof(before))==sizeof(before));close(fd);
 UT_ASSERT(acquire_refused());fd=open(claim_path,O_RDONLY);TEST_REQUIRE(fd>=0);
 TEST_REQUIRE(read(fd,&after,sizeof(after))==sizeof(after));close(fd);UT_ASSERT(memcmp(&before,&after,sizeof(before))==0);
}
static bool replace_blocked;
static void replace_during_read(void)
{
 char old[MAXPGPATH];
 if (!cluster_cf_lock(ExclusiveLock)) { replace_blocked=true;return; }
 snprintf(old,sizeof(old),"%s/old-race",scratch);TEST_REQUIRE(rename(claim_path,old)==0);put_claim(claim_path,0);
 TEST_REQUIRE(cluster_cf_unlock_confirmed(ExclusiveLock)==CLUSTER_CF_RELEASE_CONFIRMED);
}
UT_TEST(cf_serializes_name_release_against_replacement)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();replace_blocked=false;read_hook=replace_during_read;
 cluster_recovery_merge_claim_release_if_held();UT_ASSERT(replace_blocked);UT_ASSERT(!test_cf_held);
 UT_ASSERT(!exists(claim_path));
}
static void inject_external_name_change(void)
{
 char old[MAXPGPATH];snprintf(old,sizeof(old),"%s/old-external",scratch);
 TEST_REQUIRE(rename(claim_path,old)==0);put_claim(claim_path,0);
}
UT_TEST(name_change_during_descriptor_read_is_refused)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();read_hook=inject_external_name_change;
 UT_ASSERT(release_refused());UT_ASSERT(exists(claim_path));UT_ASSERT(!test_cf_held);
}
UT_TEST(unconfirmed_cf_release_keeps_original_debt)
{
 reset();cluster_recovery_merge_claim_acquire_blocking();cf_release_ok=false;
 UT_ASSERT(release_refused());UT_ASSERT(merge_claim_held);UT_ASSERT(test_cf_held);
}
UT_TEST(native_cf_is_not_clusterwide_claim_permission)
{
 reset();cf_clusterwide=false;UT_ASSERT(acquire_refused());UT_ASSERT(!exists(claim_path));UT_ASSERT(!test_cf_held);
}
UT_TEST(cf_refusal_does_not_publish_claim)
{
 reset();cf_available=false;UT_ASSERT(acquire_refused());UT_ASSERT(!exists(claim_path));
}
UT_TEST(native_claim_owner_requires_creator_and_full_original_file)
{
 uint64 owner; char old[MAXPGPATH];
 reset();UT_ASSERT_EQ(cluster_recovery_merge_claim_owner_v1(),0);
 cluster_recovery_merge_claim_acquire_blocking();
 owner=cluster_recovery_merge_claim_owner_v1();UT_ASSERT(owner!=0);
 UT_ASSERT(cluster_recovery_merge_claim_owner_current_nowait_v1(owner));
 /* Local custody is deliberately not an FD/name/bytes witness. The original
  * full check must still detect an identical replacement and revoke custody. */
 snprintf(old,sizeof(old),"%s/old-owner",scratch);
 TEST_REQUIRE(rename(claim_path,old)==0);put_claim(claim_path,0);
 UT_ASSERT(cluster_recovery_merge_claim_owner_current_nowait_v1(owner));
 UT_ASSERT_EQ(cluster_recovery_merge_claim_owner_v1(),0);
 UT_ASSERT(!cluster_recovery_merge_claim_owner_current_nowait_v1(owner));
 UT_ASSERT(exists(claim_path));UT_ASSERT(exists(old));
}
UT_TEST(native_claim_owner_incarnation_cannot_rebind_after_release)
{
 uint64 old,fresh;
 reset();cluster_recovery_merge_claim_acquire_blocking();
 old=cluster_recovery_merge_claim_owner_v1();UT_ASSERT(old!=0);
 cluster_recovery_merge_claim_release_if_held();
 UT_ASSERT(!cluster_recovery_merge_claim_owner_current_nowait_v1(old));
 cluster_recovery_merge_claim_acquire_blocking();
 fresh=cluster_recovery_merge_claim_owner_v1();
 UT_ASSERT(fresh!=0 && fresh!=old);
 UT_ASSERT(!cluster_recovery_merge_claim_owner_current_nowait_v1(old));
 UT_ASSERT(!cluster_recovery_merge_claim_owner_current_nowait_v1(0));
 UT_ASSERT(cluster_recovery_merge_claim_owner_current_nowait_v1(fresh));
 cluster_recovery_merge_claim_release_if_held();
}
UT_TEST(native_claim_owner_checks_original_actor_and_custody_revocation)
{
 for (unsigned fault=0;fault<5;fault++) {
  uint64 owner;
  reset();cluster_recovery_merge_claim_acquire_blocking();
  owner=cluster_recovery_merge_claim_owner_v1();UT_ASSERT(owner!=0);
  switch(fault) {
  case 0: owner_pid++;break;
  case 1: CurrentResourceOwner=(ResourceOwner)(uintptr_t)2;break;
  case 2: self_boot++;break;
  case 3: release_cb(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,release_arg);break;
  case 4: cluster_shared_data_dir="other-private-namespace";break;
  }
  UT_ASSERT(!cluster_recovery_merge_claim_owner_current_nowait_v1(owner));
  UT_ASSERT(exists(claim_path));
  owner_pid=12345;CurrentResourceOwner=(ResourceOwner)(uintptr_t)1;
  self_boot=17;cluster_shared_data_dir=scratch;
  UT_ASSERT(!cluster_recovery_merge_claim_owner_current_nowait_v1(owner));
 }
}
/* Peer observation must never turn a path replacement into the original
 * file or a remote executor proof. These inject real namespace/byte changes. */
static void peer_replace_identical(void)
{
 char saved[MAXPGPATH];
 UT_ASSERT(!test_cf_held);
 snprintf(saved,sizeof(saved),"%s/peer-original",scratch);
 TEST_REQUIRE(rename(claim_path,saved)==0);put_claim(claim_path,1);poll_hook=NULL;
}
static void peer_change_immutable_bytes(void)
{
 ClusterMergeClaimFile f;int fd;
 UT_ASSERT(!test_cf_held);memset(&f,0,sizeof(f));cluster_merge_claim_build(&f,1,GetSystemIdentifier());
 f.pad=7;INIT_CRC32C(f.crc);COMP_CRC32C(f.crc,&f,offsetof(ClusterMergeClaimFile,crc));FIN_CRC32C(f.crc);
 TEST_REQUIRE(cluster_merge_claim_classify(&f,sizeof(f),GetSystemIdentifier())==CLUSTER_MERGE_CLAIM_VALID);
 fd=open(claim_path,O_WRONLY);TEST_REQUIRE(fd>=0);
 TEST_REQUIRE(pwrite(fd,&f,sizeof(f),0)==sizeof(f));TEST_REQUIRE(fsync(fd)==0);close(fd);poll_hook=NULL;
}
static void peer_rebind_directory(void)
{
 char global[MAXPGPATH],saved[MAXPGPATH];
 UT_ASSERT(!test_cf_held);snprintf(global,sizeof(global),"%s/global",scratch);
 snprintf(saved,sizeof(saved),"%s/peer-original-directory",scratch);
 TEST_REQUIRE(rename(global,saved)==0);TEST_REQUIRE(mkdir(global,0700)==0);
 put_claim(claim_path,1);poll_hook=NULL;
}
static void peer_rebind_configured_root(void)
{ UT_ASSERT(!test_cf_held);cluster_shared_data_dir="changed-original-namespace";poll_hook=NULL; }
static void peer_release_then_close_error(void)
{ peer_release();close_error=true; }
UT_TEST(peer_wait_refuses_an_identical_replacement_object)
{
 char saved[MAXPGPATH];
 reset();put_claim(claim_path,1);poll_hook=peer_replace_identical;
 UT_ASSERT(acquire_refused());UT_ASSERT_EQ(last_state,ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
 UT_ASSERT(!merge_claim_held);UT_ASSERT_EQ(polls,1);UT_ASSERT(exists(claim_path));
 snprintf(saved,sizeof(saved),"%s/peer-original",scratch);UT_ASSERT(exists(saved));UT_ASSERT(!test_cf_held);
}
UT_TEST(peer_wait_refuses_changed_original_bytes_even_with_valid_crc)
{
 reset();put_claim(claim_path,1);poll_hook=peer_change_immutable_bytes;
 UT_ASSERT(acquire_refused());UT_ASSERT_EQ(last_state,ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
 UT_ASSERT(!merge_claim_held);UT_ASSERT_EQ(polls,1);UT_ASSERT(exists(claim_path));UT_ASSERT(!test_cf_held);
}
UT_TEST(peer_wait_does_not_follow_an_initial_symlink)
{
 char target[MAXPGPATH];
 reset();snprintf(target,sizeof(target),"%s/real-peer",scratch);put_claim(target,1);
 TEST_REQUIRE(symlink(target,claim_path)==0);
 UT_ASSERT(acquire_refused());UT_ASSERT_EQ(last_state,ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
 UT_ASSERT(!merge_claim_held);UT_ASSERT_EQ(polls,0);UT_ASSERT(exists(claim_path));UT_ASSERT(exists(target));
}
UT_TEST(peer_wait_keeps_its_original_directory)
{
 char saved[MAXPGPATH];
 reset();put_claim(claim_path,1);poll_hook=peer_rebind_directory;
 UT_ASSERT(acquire_refused());UT_ASSERT_EQ(last_state,ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
 UT_ASSERT(!merge_claim_held);UT_ASSERT(exists(claim_path));
 snprintf(saved,sizeof(saved),"%s/peer-original-directory/pgrac_merge.claim",scratch);
 UT_ASSERT(exists(saved));UT_ASSERT(!test_cf_held);
}
UT_TEST(peer_wait_rechecks_the_name_after_descriptor_io)
{
 reset();put_claim(claim_path,1);read_hook=peer_replace_identical;
 UT_ASSERT(acquire_refused());UT_ASSERT_EQ(last_state,ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
 UT_ASSERT(read_hook==NULL);UT_ASSERT(!merge_claim_held);UT_ASSERT(exists(claim_path));UT_ASSERT(!test_cf_held);
}
UT_TEST(peer_wait_retains_one_readonly_cloexec_handle_without_owner_rights)
{
 struct stat named,held;unsigned readers=0;
 reset();put_claim(claim_path,1);UT_ASSERT(acquire_refused());
 UT_ASSERT_EQ(last_state,0);UT_ASSERT(!merge_claim_held);UT_ASSERT_EQ(cluster_recovery_merge_claim_owner_v1(),0);
 TEST_REQUIRE(lstat(claim_path,&named)==0);
 for (int i=0;i<tracked_count;i++) {
  int fd=tracked_fds[i];
  if (fd<0 || fstat(fd,&held)!=0 || held.st_dev!=named.st_dev || held.st_ino!=named.st_ino) continue;
  readers++;UT_ASSERT_EQ(fcntl(fd,F_GETFL)&O_ACCMODE,O_RDONLY);
  UT_ASSERT((fcntl(fd,F_GETFD)&FD_CLOEXEC)!=0);
 }
 UT_ASSERT_EQ(readers,1);UT_ASSERT(exists(claim_path));UT_ASSERT(!test_cf_held);
}
UT_TEST(peer_observer_close_failure_retains_new_original_claim_debt)
{
 reset();put_claim(claim_path,1);poll_hook=peer_release_then_close_error;
 UT_ASSERT(acquire_refused());UT_ASSERT_EQ(last_state,ERRCODE_IO_ERROR);
 UT_ASSERT(merge_claim_held);UT_ASSERT_EQ(cluster_recovery_merge_claim_owner_v1(),0);
 UT_ASSERT(exists(claim_path));UT_ASSERT(!test_cf_held);
}
UT_TEST(peer_wait_cannot_rebind_the_configured_namespace)
{
 reset();put_claim(claim_path,1);poll_hook=peer_rebind_configured_root;
 UT_ASSERT(acquire_refused());UT_ASSERT_EQ(last_state,ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED);
 UT_ASSERT(!merge_claim_held);UT_ASSERT(exists(claim_path));UT_ASSERT(!test_cf_held);
 cluster_shared_data_dir=scratch;
}
UT_TEST(peer_retirement_during_read_preserves_original_atomic_acquire)
{
 unsigned open_count=0;
 reset();put_claim(claim_path,1);read_hook=peer_release;
 UT_ASSERT(!acquire_refused());
 UT_ASSERT(read_hook==NULL);UT_ASSERT_EQ(polls,0);UT_ASSERT(merge_claim_held);
 UT_ASSERT(cluster_recovery_merge_claim_owner_v1()!=0);UT_ASSERT(!test_cf_held);
 for (int i=0;i<tracked_count;i++) if (tracked_fds[i]>=0) open_count++;
 UT_ASSERT_EQ(open_count,2);
 cluster_recovery_merge_claim_release_if_held();UT_ASSERT(!exists(claim_path));
}
int main(void)
{
 setbuf(stdout,NULL); UT_PLAN(35);
 UT_RUN(peer_wait_refuses_an_identical_replacement_object);
 UT_RUN(peer_wait_refuses_changed_original_bytes_even_with_valid_crc);
 UT_RUN(peer_wait_does_not_follow_an_initial_symlink);
 UT_RUN(peer_wait_keeps_its_original_directory);
 UT_RUN(peer_wait_rechecks_the_name_after_descriptor_io);
 UT_RUN(peer_wait_retains_one_readonly_cloexec_handle_without_owner_rights);
 UT_RUN(peer_observer_close_failure_retains_new_original_claim_debt);
 UT_RUN(peer_wait_cannot_rebind_the_configured_namespace);
 UT_RUN(peer_retirement_during_read_preserves_original_atomic_acquire);

 UT_RUN(native_claim_owner_requires_creator_and_full_original_file);
 UT_RUN(native_claim_owner_incarnation_cannot_rebind_after_release);
 UT_RUN(native_claim_owner_checks_original_actor_and_custody_revocation);
 UT_RUN(original_owner_links_and_releases);UT_RUN(same_node_persistent_claim_is_not_adopted);
 UT_RUN(replacement_file_is_not_unlinked);UT_RUN(changed_original_bytes_are_not_released);
 UT_RUN(missing_original_file_is_not_confirmed_release);UT_RUN(replaced_symlink_is_not_unlinked);
 UT_RUN(other_process_cannot_reenter_or_release);UT_RUN(other_resource_owner_cannot_reenter_or_release);
 UT_RUN(changed_boot_cannot_reenter_or_release);UT_RUN(original_owner_release_revokes_without_unlink);
 UT_RUN(directory_sync_failure_preserves_release_debt);UT_RUN(unconfigured_start_does_not_create_claim);
 UT_RUN(peer_wait_uses_original_atomic_claim);UT_RUN(replaced_directory_is_not_released);
 UT_RUN(released_other_owner_does_not_revoke_original);UT_RUN(owner_loss_is_sticky_after_identity_returns);
 UT_RUN(close_failure_preserves_release_debt);UT_RUN(stale_staging_hardlink_cannot_truncate_claim);
 UT_RUN(cf_serializes_name_release_against_replacement);UT_RUN(name_change_during_descriptor_read_is_refused);
 UT_RUN(unconfirmed_cf_release_keeps_original_debt);UT_RUN(native_cf_is_not_clusterwide_claim_permission);
 UT_RUN(cf_refusal_does_not_publish_claim);UT_DONE();return ut_failed_count!=0;
}
