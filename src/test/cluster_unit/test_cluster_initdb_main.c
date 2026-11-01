/* Exact original Main; original child/storage work is an observed boundary.
 * This test proves dispatch only, never native initdb or ROOT authority. */
#include "postgres.h"
#include <fcntl.h>
#include <setjmp.h>
#include <sys/stat.h>
#include <unistd.h>
#include "unit_test.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_initdb_cohort.h"
#include "common/pgrac_initdb_cohort.h"
#include "common/cluster_recovery_bootstrap.h"
#include "common/pgrac_initdb_recovery.h"
#include "common/cluster_recovery_registry.h"
#include "cluster/cluster_initdb_config.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster_initdb_common_private.h"
#include "cluster_initdb_origin_private.h"
#include "cluster_initdb_tree_private.h"
#include "cluster_cf_contract_private.h"
#include "pgrac_wal.h"
#include "test_cluster_initdb_main_types.inc"

UT_DEFINE_GLOBALS();
static bool IsUnderPostmaster;
static void *CurrentResourceOwner;
static mode_t pg_mode_mask = 0077;
static ClusterSharedConfigRef config;
static PgracInitdbCohortContext input;
static char paths[4][MAXPGPATH];
static InitdbDirectory held[16];
static unsigned held_count;
static void *main_allocations[2];
static unsigned main_allocation_count;
static void *main_alloc(Size n)
{
	void *p = calloc(1, n);
	if (main_allocation_count >= 2 || !p)
		abort();
	main_allocations[main_allocation_count++] = p;
	return p;
}
static unsigned children, null_requests, root_requests;
static jmp_buf finished;
static void pg_attribute_noreturn() die(const char *reason)
{
	fprintf(stderr, "dispatch refusal: %s\n", reason);
	longjmp(finished, 2);
}
static void noargs(void) {}
bool pg_strong_random(void *p, size_t n)
{
	memset(p, 2, n);
	return true;
}
static void initproc(const char *s) { (void)s; }
static void readrequest(PgracInitdbCohortContext *r) { *r = input; }
void *palloc(Size n) { return calloc(1, n); }
void pfree(void *p) { free(p); }
static void *owner(void *p, const char *name)
{
	(void)p;
	(void)name;
	return &input;
}
static int findexec(const char *a, const char *b, const char *c, char *out)
{
	(void)a;
	(void)b;
	(void)c;
	strlcpy(out, "/test/original-initdb", MAXPGPATH);
	return 0;
}
static ClusterInitdbConfig *preflight(const PgracInitdbConfigContext *r)
{
	(void)r;
	return (ClusterInitdbConfig *)&config;
}
static const ClusterSharedConfigRef *reference(const ClusterInitdbConfig *c)
{
	(void)c;
	return &config;
}
static const char *configpath(const ClusterInitdbConfig *c, unsigned i)
{
	(void)c;
	return paths[i + 1];
}
static void current(const PgracInitdbCohortContext *r, const ClusterSharedConfigRef *c)
{
	if (r->creator_pid != input.creator_pid || c != &config)
		abort();
}
static void predir(const char *path, InitdbDirectory *d)
{
	memset(d, 0, sizeof(*d));
	d->fd = -1;
	strlcpy(d->path, path, sizeof(d->path));
}
static void makedir(InitdbDirectory *d)
{
	if (mkdir(d->path, 0700) || (d->fd = open(d->path, O_RDONLY | O_DIRECTORY)) < 0)
		abort();
	if (held_count >= 16)
		abort();
	held[held_count++] = *d;
}
static void childdir(const InitdbDirectory *p, const char *name, InitdbDirectory *d)
{
	char path[MAXPGPATH];
	snprintf(path, sizeof(path), "%s/%s", p->path, name);
	predir(path, d);
	makedir(d);
}
static void dircurrent(const InitdbDirectory *d)
{
	if (d->fd < 0)
		abort();
}
static unsigned fault;
static bool requests_valid = true;
static void observed_child(const char *exe, const PgracInitdbCohortContext *r,
						   const ClusterSharedConfigRef *c, const char *shared, InitdbOrigin *o,
						   int node, const PgracInitdbRecoveryContext *registration)
{
	ClusterRecoveryFileIdentityV2 boot[2], relmaps[4];
	(void)exe;
	(void)r;
	(void)c;
	(void)shared;
	++children;
	if (!registration)
	{
		++null_requests;
		return;
	}
	if (registration->cohort_origin != (uint64)node || registration->boot_file_count != 1 ||
		memcmp(registration->cohort, c->identity.configured, 16) ||
		!pgrac_initdb_recovery_boot_v2(registration, boot) || boot[0].origin != node ||
		boot[0].creation_generation != (uint64)(5 + node) ||
		boot[0].kind != CLUSTER_RECOVERY_FILE_CLOG_V2 || boot[1].kind != 0)
		requests_valid = false;
	o->relmap_registration = *registration;
	o->managed_boot = true;
	o->managed_relmap = node == 0;
	o->control.system_identifier = c->identity.system_identifier;
	o->control.state = DB_SHUTDOWNED;
	o->control.checkPoint = 100;
	o->control.checkPointCopy.ThisTimeLineID = 1;
	o->control.checkPointCopy.redo = 80;
	o->checkpoint.checkpoint_start = 100;
	o->checkpoint.checkpoint_end = 120;
	o->input.snapshot.identity.system_identifier = c->identity.system_identifier;
	memcpy(o->input.snapshot.identity.storage_uuid, c->identity.storage_uuid, 16);
	memcpy(o->input.snapshot.identity.authority_uuid, c->identity.authority_uuid, 16);
	o->input.snapshot.identity.origin_node_id = node;
	o->input.snapshot.identity.origin_thread_id = node + 1;
	o->input.snapshot.identity.origin_owner_incarnation = 1;
	o->input.snapshot.identity.root_lineage_seq = 1;
	o->input.snapshot.identity.thread_claim_created_at = 1;
	o->input.snapshot.identity.thread_claim_crc32c = 1;
	o->input.refs.claim_sha256[0] = node + 1;
	o->boot_observations[0].identity = boot[0];
	o->boot_observations[0].synced_bytes = 16448;
	o->boot_observations[0].page.birth_observed = true;
	o->boot_observations[0].page.birth_full_xid = FirstNormalFullTransactionId;
	o->boot_observations[0].page.page_generation = 3;
	o->boot_observations[0].image_digest[0] = node + 1;
	if (node == 0 && pgrac_initdb_recovery_relmaps_v2(registration, relmaps))
		for (unsigned i = 0; i < 8; i++)
		{
			ClusterRecoveryCheckpointFileV2 *f = &o->relmap_observations[i];
			f->identity = relmaps[i / 2];
			f->copy_index = i % 2;
			f->copy_count = 2;
			f->synced_bytes = 2304;
			f->page_count = 1;
			f->image_digest[0] = i + 1;
		}
	if (fault == 1 && node == 3)
		o->managed_boot = false;
	if (fault == 2 && node == 3)
		o->boot_observations[0].identity.origin = 0;
	if (fault == 3 && node == 3)
		o->boot_observations[0].image_digest[0] = 0;
}
static void pair(const InitdbDirectory *d, InitdbOrigin *o, const ClusterSharedConfigRef *c)
{
	(void)d;
	(void)o;
	(void)c;
}
static void origin_objects(const InitdbDirectory *d, InitdbOrigin *o,
						   const ClusterSharedConfigRef *c, uint64 t)
{
	(void)d;
	(void)o;
	(void)c;
	(void)t;
}
static void storage(InitdbOrigin *o, const ClusterSharedConfigRef *c)
{
	(void)o;
	(void)c;
}
static void common_objects(const InitdbDirectory *d, InitdbOrigin *o,
						   const ClusterSharedConfigRef *c, ClusterInitdbCommon *v)
{
	(void)d;
	(void)o;
	(void)c;
	memset(v, 0, sizeof(*v));
}
static void undo_dirs(const InitdbDirectory *d, const ClusterSharedConfigRef *c)
{
	(void)d;
	(void)c;
}
static void control(InitdbOrigin *o, uint16 t, uint64 s, bool f)
{
	(void)o;
	(void)t;
	(void)s;
	(void)f;
}
static void route(const InitdbDirectory *d, InitdbOrigin *o, unsigned n)
{
	(void)d;
	(void)o;
	(void)n;
}
static void root_objects(const PgracInitdbCohortContext *r, InitdbDirectory d[4], InitdbOrigin *o,
						 const ClusterSharedConfigRef *c, const ClusterInitdbCommon *m, uint64 t,
						 const ClusterRecoveryRegistryV2 *registration, Size budget)
{
	(void)r;
	(void)d;
	(void)o;
	(void)c;
	(void)m;
	(void)t;
	(void)budget;
	if (registration && registration->coverage.file_count == 12 &&
		registration->coverage.source_count == 4)
	{
		root_requests++;
		for (uint64 i = 0; i < registration->coverage.file_count; i++)
		{
			const ClusterRecoveryCheckpointFileV2 *f = &registration->coverage.files[i];
			if (f->identity.kind == CLUSTER_RECOVERY_FILE_UNDO_V2 || f->source_count != 4)
				requests_valid = false;
		}
	}
}
static void pg_attribute_noreturn() done(int rc)
{
	if (rc)
		abort();
	longjmp(finished, 1);
}
#define refuse die
#define palloc0(n) calloc(1, (n))
#include "test_cluster_initdb_main_helpers.inc"
#define read_context readrequest
#define InitStandaloneProcess initproc
#define install_creation_signals noargs
#define InitializeGUCOptions noargs
#define process_cluster_gucs noargs
#define ResourceOwnerCreate owner
#define find_other_exec findexec
#define cluster_initdb_config_preflight preflight
#define cluster_initdb_config_reference reference
#define cluster_initdb_config_path configpath
#define request_current current
#define preflight_directory predir
#define GetCurrentTimestamp() 1
#define create_directory makedir
#undef palloc0
#define palloc0(n) main_alloc(n)
#define create_child childdir
#define run_origin observed_child
#define directory_current dircurrent
#define create_peer_side pair
#define create_origin_objects origin_objects
#define create_storage_contracts storage
#define create_common_objects common_objects
#define create_undo_directories undo_dirs
#define control_read control
#define route_original_side route
#define create_catalog_objects pair
#define create_root_objects root_objects
#define exit done
#include "test_cluster_initdb_main_body.inc"
#undef exit

static void run_case(unsigned selected_fault)
{
	char temp[] = "/tmp/pgrac-main-red-XXXXXX";
	char *args[] = {"postgres", "--pgrac-initdb-cohort", NULL};
	if (!mkdtemp(temp))
		abort();
	children = null_requests = root_requests = 0;
	held_count = main_allocation_count = 0;
	fault = selected_fault;
	requests_valid = true;
	memset(&config, 0, sizeof(config));
	config.identity.configured[0] = 15;
	config.identity.system_identifier = 91;
	config.identity.database_incarnation = 27;
	config.identity.storage_uuid[0] = 1;
	config.identity.authority_uuid[0] = 3;
	config.identity.generation = 1;
	config.sha256[0] = 10;
	input.config.sha256[0] = 10;
	input.config.fd = 3;
	input.config.bytes = 1;
	input.creator_pid = getppid();
	input.segment_size = 16 * 1024 * 1024;
	for (unsigned i = 0; i < 4; i++)
		snprintf(paths[i], sizeof(paths[i]), "%s/root%u", temp, i);
	strlcpy(input.cache_root, paths[0], sizeof(input.cache_root));
	if (setjmp(finished) == 0)
		ClusterInitdbCohortMain(2, args);
	UT_ASSERT_EQ(children, 4);
	UT_ASSERT_EQ(null_requests, 0);
	UT_ASSERT(requests_valid);
	UT_ASSERT_EQ(root_requests, selected_fault ? 0 : 1);
	while (held_count)
	{
		InitdbDirectory *d = &held[--held_count];
		UT_ASSERT(!close(d->fd));
		UT_ASSERT(!rmdir(d->path));
	}
	UT_ASSERT(!rmdir(temp));
	while (main_allocation_count)
		free(main_allocations[--main_allocation_count]);
}
UT_TEST(original_main_dispatches_complete_cohort_and_registration) { run_case(0); }
UT_TEST(main_missing_peer_never_reaches_root) { run_case(1); }
UT_TEST(main_founder_identity_cannot_cover_peer) { run_case(2); }
UT_TEST(main_incomplete_observation_never_reaches_root) { run_case(3); }
int main(void)
{
	UT_PLAN(4);
	UT_RUN(original_main_dispatches_complete_cohort_and_registration);
	UT_RUN(main_missing_peer_never_reaches_root);
	UT_RUN(main_founder_identity_cannot_cover_peer);
	UT_RUN(main_incomplete_observation_never_reaches_root);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
