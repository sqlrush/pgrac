/* Original frontend preflight/bind, real closed request and real directories. */
#include "postgres_fe.h"
#include "unit_test.h"
#include "common/cryptohash.h"
#include "common/file_perm.h"
#include "common/logging.h"
#include "common/pgrac_initdb_recovery.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_recovery_format.h"
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

UT_DEFINE_GLOBALS();

static int pgrac_native_thread = 2;
static uint64 pgrac_native_sysid = 91;
static uint64 pgrac_native_database_incarnation = 27;
static uint8 pgrac_native_storage_bytes[16] = {1};
static char *pg_data, *xlog_dir, *pgrac_native_data_path, *pgrac_native_wal_path;
static char *pgrac_native_base_path, *pgrac_native_config_path;
static int pgrac_native_data_fd = -1, pgrac_native_wal_fd = -1, pgrac_native_base_fd = -1;
static PgracInitdbConfigContext pgrac_native_config;
static PgracInitdbRecoveryContext pgrac_native_recovery;
static ClusterRecoveryFileIdentityV2 pgrac_native_relmap_identities[4];
static bool pgrac_native_recovery_requested;
static bool made_new_pgdata, found_existing_pgdata, made_new_xlogdir, found_existing_xlogdir;
static const char *boot_options, *backend_options;
static unsigned phase, refusal_phase;

static void pg_attribute_noreturn() fixture_fatal(const char *fmt,...) pg_attribute_printf(1,2);
static void pg_attribute_noreturn()
fixture_fatal(const char *fmt,...)
{
	if (!refusal_phase || refusal_phase != phase)
	{
		va_list args;
		va_start(args,fmt);
		vfprintf(stderr,fmt,args);
		va_end(args);
		fputc('\n',stderr);
		_exit(73);
	}
	_exit(0);
}
#undef pg_fatal
#define pg_fatal fixture_fatal

static void pgrac_native_config_check(bool);
static void pgrac_native_check_directory(const char *,int,struct stat *);
static void pgrac_native_check_directories(struct stat *,struct stat *);
#include "test_cluster_initdb_paths_entries.inc"

static void
write_file(const char *path)
{
	int fd=open(path,O_WRONLY|O_CREAT|O_EXCL,0600);
	if(fd<0 || write(fd,"original",8)!=8 || close(fd)) abort();
}

/* Each invocation has its own tree and actual immediate-parent pipe. Parent
 * common read/config validators are linked, not replaced by true providers. */
static int
run_case(unsigned fault)
{
	char raw[]="/tmp/pgrac-initdb-paths-XXXXXX";
	char data[MAXPGPATH],wal[MAXPGPATH],shared[MAXPGPATH],conf[MAXPGPATH],name[MAXPGPATH];
	ClusterRecoveryFileIdentityV2 first={0},clog;
	PgracInitdbRecoveryContext request;
	uint64 cohort[2]={3,0};
	struct stat held;
	int datafd,sharedfd,pipefd=-1,status;
	pid_t child;
	char *root;
	if(!mkdtemp(raw)) abort();
	root=realpath(raw,NULL);
	snprintf(data,sizeof(data),"%s/data",root);
	snprintf(wal,sizeof(wal),"%s/wal",root);
	snprintf(shared,sizeof(shared),"%s/shared",root);
	snprintf(conf,sizeof(conf),"%s/request",root);
	if(mkdir(data,0700)||mkdir(wal,0700)||mkdir(shared,0700)) abort();
	write_file(conf);
	if(fault!=16 && fault!=17)
	{
		snprintf(name,sizeof(name),"%s/founder-data",shared);
		write_file(name);
	}
	pgrac_native_config_path=conf;
	pgrac_native_config_check(true);
	first.kind=CLUSTER_RECOVERY_FILE_RELMAP_V2;
	first.format_id=CLUSTER_RECOVERY_FORMAT_ID_V2;
	first.format_version=CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	first.system_identifier=pgrac_native_sysid;
	first.database_incarnation=pgrac_native_database_incarnation;
	first.storage_uuid[0]=1;first.redo_branch_uuid[0]=2;
	first.tablespace_oid=GLOBALTABLESPACE_OID;
	first.redo_timeline=1;first.creation_generation=1;
	first.registration_generation=1;first.redo_branch_generation=1;
	clog=first;clog.kind=CLUSTER_RECOVERY_FILE_CLOG_V2;
	clog.tablespace_oid=0;clog.origin=1;clog.creation_generation=6;
	datafd=open(data,O_RDONLY|O_DIRECTORY);
	sharedfd=open(shared,O_RDONLY|O_DIRECTORY);
	if(datafd<0||sharedfd<0 || !pgrac_initdb_recovery_prepare_cohort_v2(
		&first,&clog,cohort,1,&pgrac_native_config,datafd,sharedfd,&request)) abort();
	if(fault==3) ++request.config.inode;
	if(fault==4) ++request.creator_pid;
	if(fault==5) { request.cohort_origin=0;request.boot_files[0].origin=0; }
	if(fault==6) request.shared_inode++;
	if(fault==7) { snprintf(name,sizeof(name),"%s/occupied",data);write_file(name); }
	if(fault==8) { snprintf(name,sizeof(name),"%s/occupied",wal);write_file(name); }
	if(fault==9 || fault==16) { request.cohort_origin=0;request.boot_files[0].origin=0; }
	if(fault==13) request.data_inode++;
	if(fault==14) { request.boot_file_count=0; memset(request.boot_files,0,sizeof(request.boot_files)); }
	if(fault!=1 && fault!=14 && fault!=17 && !pgrac_initdb_recovery_pipe_v2(&request,&pipefd)) abort();
	if(fault==14)
	{
		int fds[2];
		if(pipe(fds) || write(fds[1],&request,sizeof(request))!=sizeof(request) || close(fds[1])) abort();
		pipefd=fds[0];
	}
	fflush(NULL);
	child=fork();
	if(child<0) abort();
	if(!child)
	{
		char value[32];
		int original_data,original_shared,original_wal;
		bool positive=fault==0 || fault==16 || fault==17;
		close(datafd);close(sharedfd);
		close(pgrac_native_config.fd);
		memset(&pgrac_native_config,0,sizeof(pgrac_native_config));
		pgrac_native_config.fd=-1;
		pgrac_native_base_path=pg_strdup(shared);
		pg_data=data;xlog_dir=wal;
		pgrac_native_thread=fault==9 || fault==16 || fault==17 ? 1 : 2;
		if(fault==2) ++pgrac_native_sysid;
		phase=1;
		refusal_phase=positive ? 0 : (fault==10||fault==11||fault==12||fault==15 ? 2 : 1);
		if(pipefd>=0)
		{
			snprintf(value,sizeof(value),"%d",pipefd);
			if(setenv(PGRAC_INITDB_RECOVERY_ENV,value,1)) _exit(75);
		}
		else unsetenv(PGRAC_INITDB_RECOVERY_ENV);
		pgrac_native_prepare_paths();
		original_data=pgrac_native_data_fd;
		original_shared=pgrac_native_base_fd;
		original_wal=pgrac_native_wal_fd;
		if(!fault && (!pgrac_native_recovery_requested || original_data<3 ||
			 original_shared<3 || original_wal<3 || getenv(PGRAC_INITDB_RECOVERY_ENV))) _exit(76);
		if(fault==10)
		{
			char moved[MAXPGPATH];
			snprintf(moved,sizeof(moved),"%s/old-shared",root);
			if(rename(shared,moved)||mkdir(shared,0700)) _exit(77);
		}
		if(fault==11)
		{
			int fd=open(conf,O_WRONLY|O_APPEND);
			if(fd<0||write(fd,"!",1)!=1||close(fd)) _exit(78);
		}
		if(fault==12) ++pgrac_native_recovery.cohort_origin;
		if(fault==15) pgrac_native_recovery.config.sha256[31]^=1;
		snprintf(name,sizeof(name),"%s/pg_wal",data);
		if(symlink(wal,name)) _exit(79);
		found_existing_pgdata=found_existing_xlogdir=true;
		phase=2;
		pgrac_native_bind_creation();
		if(!positive) _exit(74); /* A negative may not silently fall through. */
		if(fstat(pgrac_native_base_fd,&held) || held.st_ino!=request.shared_inode) _exit(80);
		if(!fault && (original_data!=pgrac_native_data_fd || original_shared!=pgrac_native_base_fd ||
		   original_wal!=pgrac_native_wal_fd)) _exit(81);
		if(fault==17 ? pgrac_native_recovery_requested :
		   !pgrac_native_recovery_requested || getenv(PGRAC_INITDB_RECOVERY_ENV) ||
		   memcmp(&pgrac_native_recovery,&request,sizeof(request))) _exit(82);
		_exit(0);
	}
	if(pipefd>=0) close(pipefd);
	close(datafd);close(sharedfd);close(pgrac_native_config.fd);
	if(waitpid(child,&status,0)!=child||!WIFEXITED(status)) abort();
	if(!rmtree(root,true)) abort();
	free(root);
	return WEXITSTATUS(status);
}

UT_TEST(authenticated_peer_retains_original_fds_through_bind) { UT_ASSERT_EQ(run_case(0),0); }
UT_TEST(nonempty_shared_without_registration_refuses) { UT_ASSERT_EQ(run_case(1),0); }
UT_TEST(wrong_system_config_or_parent_refuses_before_bind) {
	UT_ASSERT_EQ(run_case(2),0);UT_ASSERT_EQ(run_case(3),0);UT_ASSERT_EQ(run_case(4),0);
}
UT_TEST(wrong_origin_or_original_directory_refuses_before_bind) {
	UT_ASSERT_EQ(run_case(5),0);UT_ASSERT_EQ(run_case(6),0);UT_ASSERT_EQ(run_case(13),0);
}
UT_TEST(data_and_wal_still_require_empty_targets) { UT_ASSERT_EQ(run_case(7),0);UT_ASSERT_EQ(run_case(8),0); }
UT_TEST(founder_nonempty_shared_still_refuses) { UT_ASSERT_EQ(run_case(9),0); }
UT_TEST(replaced_shared_path_refuses_original_bind) { UT_ASSERT_EQ(run_case(10),0); }
UT_TEST(changed_configuration_refuses_original_bind) { UT_ASSERT_EQ(run_case(11),0); }
UT_TEST(changed_registration_refuses_original_bind) { UT_ASSERT_EQ(run_case(12),0); }
UT_TEST(missing_boot_inventory_refuses_before_bind) { UT_ASSERT_EQ(run_case(14),0); }
UT_TEST(saved_config_digest_must_still_match_at_bind) { UT_ASSERT_EQ(run_case(15),0); }
UT_TEST(founder_authenticates_original_request_at_bind) { UT_ASSERT_EQ(run_case(16),0); }
UT_TEST(legacy_empty_creation_has_no_registration) { UT_ASSERT_EQ(run_case(17),0); }

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(authenticated_peer_retains_original_fds_through_bind);
	UT_RUN(nonempty_shared_without_registration_refuses);
	UT_RUN(wrong_system_config_or_parent_refuses_before_bind);
	UT_RUN(wrong_origin_or_original_directory_refuses_before_bind);
	UT_RUN(data_and_wal_still_require_empty_targets);
	UT_RUN(founder_nonempty_shared_still_refuses);
	UT_RUN(replaced_shared_path_refuses_original_bind);
	UT_RUN(changed_configuration_refuses_original_bind);
	UT_RUN(changed_registration_refuses_original_bind);
	UT_RUN(missing_boot_inventory_refuses_before_bind);
	UT_RUN(saved_config_digest_must_still_match_at_bind);
	UT_RUN(founder_authenticates_original_request_at_bind);
	UT_RUN(legacy_empty_creation_has_no_registration);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
