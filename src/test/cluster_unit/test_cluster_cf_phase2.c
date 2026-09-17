/*-------------------------------------------------------------------------
 *
 * test_cluster_cf_phase2.c
 *	  Runtime unit tests for the CF Phase-2 cross-node rename-contract
 *	  rendezvous (spec-5.6 T6).  Exercises the probe/ack file format and the
 *	  rendezvous decision logic against a temp shared dir, both roles in one
 *	  process.  The REAL cross-node proof (two postmasters, concurrent
 *	  rendezvous over genuinely shared storage) is the 2-node TAP t/289.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cf_phase2.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (§3.9 T6)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cluster/cluster_cf_phase2.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cf_storage.h"
#include "datatype/timestamp.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "utils/elog.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* ---- globals phase2.o references (verify_or_fail path; not exercised here) ---- */
int cluster_node_id = 0;
bool cluster_enabled = false;
char *cluster_shared_data_dir = NULL;
bool cluster_controlfile_shared_authority = false;
int cluster_cf_enqueue_timeout_ms = 30000;
volatile sig_atomic_t InterruptPending = 0;
int pg_dir_create_mode = 0700;
int MyAuxProcType = 0; /* RF-ROOT P6: phase2.o samples it */

/* ---- Assert + ereport + fd.c stubs (same pattern as the storage test) ---- */
void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR) {
		printf("# unexpected ereport(elevel=%d) -- aborting\n", elevel);
		abort();
	}
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}
int
errcode(int c pg_attribute_unused())
{
	return 0;
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
void
ProcessInterrupts(void)
{}

int
OpenTransientFile(const char *fileName, int fileFlags)
{
	return open(fileName, fileFlags, 0600);
}
int
CloseTransientFile(int fd)
{
	return close(fd);
}
static bool pause_next_allocate_dir;
DIR *
AllocateDir(const char *dirname)
{
	if (pause_next_allocate_dir) {
		pause_next_allocate_dir = false;
		raise(SIGSTOP);
	}
	return opendir(dirname);
}
struct dirent *
ReadDir(DIR *dir, const char *dirname pg_attribute_unused())
{
	return readdir(dir);
}
int
FreeDir(DIR *dir)
{
	return closedir(dir);
}
int
pg_fsync(int fd)
{
	return fsync(fd);
}
int
durable_rename(const char *o, const char *n, int e pg_attribute_unused())
{
	return rename(o, n) == 0 ? 0 : -1;
}

/* GetCurrentTimestamp stub: a fixed value so the rendezvous deadline math is
 * deterministic (timeout_ms=0 -> deadline == now -> immediate timeout). */
TimestampTz
GetCurrentTimestamp(void)
{
	return 1;
}

/* contract update_state/load: not reached by these tests (only verify_or_fail
 * uses them); stub to satisfy the link without pulling in cluster_cf_storage.o. */
bool
cluster_cf_contract_update_state(const char *p pg_attribute_unused(),
								 ClusterCfContractState s pg_attribute_unused())
{
	return true;
}
ClusterCfContractState
cluster_cf_contract_load(const char *p pg_attribute_unused())
{
	return CLUSTER_CF_CONTRACT_UNVERIFIED;
}

/* find_peer_node() / respond_tick deps.  Settable so the respond_tick leg
 * can present a 2-node config (0 and 1) without dragging cluster_conf.o in. */
static int stub_node_count = 1;
static bool stub_configured_nodes[CLUSTER_MAX_NODES];

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 id)
{
	static const ClusterNodeInfo marker;

	return (id >= 0 && id < CLUSTER_MAX_NODES && stub_configured_nodes[id]) ? &marker : NULL;
}
int
cluster_conf_node_count(void)
{
	return stub_node_count;
}

/* pg_strong_random is only used by verify_or_fail (not exercised); a local stub
 * resolves the link without dragging OpenSSL into this standalone unit. */
bool
pg_strong_random(void *buf, size_t len)
{
	memset(buf, 0x5a, len);
	return true;
}

/* ---- fixture ---- */
static char shared_root[MAXPGPATH];

static void
configure_nodes(int count)
{
	int i;

	memset(stub_configured_nodes, 0, sizeof(stub_configured_nodes));
	stub_node_count = count;
	for (i = 0; i < count; i++)
		stub_configured_nodes[i] = true;
}

static void
ack_path(char path[MAXPGPATH], int probe_owner, int responder, uint64 nonce)
{
	snprintf(path, MAXPGPATH, "%s/%s/ack.%d.%d.%016" INT64_MODIFIER "x", shared_root,
			 CLUSTER_CF_PHASE2_DIR, probe_owner, responder, nonce);
}

static void
write_test_bytes(const char *path, const void *bytes, size_t len)
{
	int fd;

	fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, bytes, len) != (ssize_t)len)
		abort();
	close(fd);
}

static void
restamp_v2(ClusterCfPhase2RecordV2 *record)
{
	INIT_CRC32C(record->crc);
	COMP_CRC32C(record->crc, record, offsetof(ClusterCfPhase2RecordV2, crc));
	FIN_CRC32C(record->crc);
}

static void
make_shared_root(void)
{
	char tmpl[MAXPGPATH];
	char sub[MAXPGPATH];

	snprintf(tmpl, sizeof(tmpl), "/tmp/pgrac_cf_p2_XXXXXX");
	if (mkdtemp(tmpl) == NULL) {
		printf("# mkdtemp failed: %s\n", strerror(errno));
		abort();
	}
	strlcpy(shared_root, tmpl, sizeof(shared_root));
	snprintf(sub, sizeof(sub), "%s/global", shared_root);
	if (mkdir(sub, 0700) != 0 && errno != EEXIST) {
		printf("# mkdir global failed: %s\n", strerror(errno));
		abort();
	}
	snprintf(sub, sizeof(sub), "%s/%s", shared_root, CLUSTER_CF_PHASE2_DIR);
	if (mkdir(sub, 0700) != 0 && errno != EEXIST) {
		printf("# mkdir p2 failed: %s\n", strerror(errno));
		abort();
	}
}

/* ======================================================================
 * probe/ack file round-trip + corruption + absence
 * ====================================================================== */
UT_TEST(test_probe_ack_roundtrip)
{
	ClusterCfPhase2RecordV2 got;
	uint64 nonce = UINT64CONST(0xABCDEF0123456789);

	make_shared_root();
	configure_nodes(4);

	/* missing probe/ack read as false */
	UT_ASSERT(!cluster_cf_phase2_read_probe(shared_root, 7, &got));

	/* probe round-trip */
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, nonce));
	UT_ASSERT(cluster_cf_phase2_read_probe(shared_root, 0, &got));
	UT_ASSERT_EQ(got.kind, CLUSTER_CF_P2_PROBE);
	UT_ASSERT_EQ(got.probe_owner_node, 0);
	UT_ASSERT_EQ(got.responder_node, -1);
	UT_ASSERT_EQ(got.probe_nonce, nonce);

	/* ack round-trip binds owner, responder, and exact nonce. */
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, nonce));
	UT_ASSERT(cluster_cf_phase2_read_exact_ack(shared_root, 0, 1, nonce));

	/* a corrupt (truncated) probe reads as false */
	{
		char path[MAXPGPATH];
		int fd;
		char junk[4] = { 1, 2, 3, 4 };

		snprintf(path, sizeof(path), "%s/%s/probe.0", shared_root, CLUSTER_CF_PHASE2_DIR);
		fd = open(path, O_RDWR | O_TRUNC, 0600);
		if (fd < 0 || write(fd, junk, sizeof(junk)) != (ssize_t)sizeof(junk))
			abort();
		close(fd);
		UT_ASSERT(!cluster_cf_phase2_read_probe(shared_root, 0, &got));

		/* An empty V2 path is short, not an exact EOF-terminated record. */
		fd = open(path, O_RDWR | O_TRUNC, 0600);
		UT_ASSERT(fd >= 0);
		close(fd);
		UT_ASSERT(!cluster_cf_phase2_read_probe(shared_root, 0, &got));
	}
}

UT_TEST(test_four_responders_publish_distinct_exact_acks)
{
	uint64 nonce = UINT64CONST(0x1000000000000001);
	int responder;

	make_shared_root();
	configure_nodes(4);
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, nonce));
	for (responder = 1; responder <= 3; responder++)
		UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, responder, nonce));
	for (responder = 1; responder <= 3; responder++)
		UT_ASSERT(cluster_cf_phase2_read_exact_ack(shared_root, 0, responder, nonce));
}

UT_TEST(test_nonselected_responder_cannot_complete_rendezvous)
{
	uint64 nonce = UINT64CONST(0x2000000000000002);

	make_shared_root();
	configure_nodes(4);
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 2, nonce));
	UT_ASSERT(!cluster_cf_phase2_rendezvous(shared_root, 0, 1, nonce, 0));
}

UT_TEST(test_delayed_old_nonce_writer_cannot_replace_current_ack)
{
	uint64 old_nonce = UINT64CONST(0x3000000000000003);
	uint64 new_nonce = UINT64CONST(0x3000000000000004);

	make_shared_root();
	configure_nodes(4);
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, old_nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, old_nonce));
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, new_nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, new_nonce));
	UT_ASSERT(!cluster_cf_phase2_write_ack(shared_root, 0, 1, old_nonce));
	UT_ASSERT(cluster_cf_phase2_read_exact_ack(shared_root, 0, 1, new_nonce));
}

UT_TEST(test_duplicate_exact_ack_is_idempotent)
{
	uint64 nonce = UINT64CONST(0x4000000000000004);

	make_shared_root();
	configure_nodes(4);
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, nonce));
	UT_ASSERT(cluster_cf_phase2_read_exact_ack(shared_root, 0, 1, nonce));
}

UT_TEST(test_v2_crc_and_path_tuple_validation)
{
	typedef struct ClusterCfPhase2RecordV1 {
		uint32 magic;
		uint32 version;
		uint64 nonce;
		/* The fixture writes the complete old wire record, including CRC. */
		/* cppcheck-suppress unusedStructMember */
		pg_crc32c crc;
	} ClusterCfPhase2RecordV1;
	ClusterCfPhase2RecordV1 v1;
	ClusterCfPhase2RecordV2 record;
	char from[MAXPGPATH];
	char path[MAXPGPATH];
	uint64 nonce = UINT64CONST(0x5000000000000005);
	int fd;

	make_shared_root();
	configure_nodes(4);
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, nonce));
	UT_ASSERT(!cluster_cf_phase2_read_exact_ack(shared_root, 0, 2, nonce));
	UT_ASSERT(!cluster_cf_phase2_read_exact_ack(shared_root, 0, 1, nonce + 1));

	ack_path(path, 0, 1, nonce);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(read(fd, &record, sizeof(record)), (ssize_t)sizeof(record));
	close(fd);
	record.crc ^= 1;
	write_test_bytes(path, &record, sizeof(record));
	UT_ASSERT(!cluster_cf_phase2_read_exact_ack(shared_root, 0, 1, nonce));

	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, nonce + 1));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, nonce + 1));
	ack_path(path, 0, 1, nonce + 1);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(read(fd, &record, sizeof(record)), (ssize_t)sizeof(record));
	close(fd);
	record.responder_node = 2;
	restamp_v2(&record);
	write_test_bytes(path, &record, sizeof(record));
	UT_ASSERT(!cluster_cf_phase2_read_exact_ack(shared_root, 0, 1, nonce + 1));

	/* A valid V2 body moved under another probe-owner path is foreign. */
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, nonce + 2));
	snprintf(from, sizeof(from), "%s/%s/probe.0", shared_root, CLUSTER_CF_PHASE2_DIR);
	snprintf(path, sizeof(path), "%s/%s/probe.1", shared_root, CLUSTER_CF_PHASE2_DIR);
	UT_ASSERT_EQ(rename(from, path), 0);
	UT_ASSERT(!cluster_cf_phase2_read_probe(shared_root, 1, &record));

	/* The retired V1 shape cannot produce a positive exact proof. */
	memset(&v1, 0, sizeof(v1));
	v1.magic = UINT32_C(0x43465032);
	v1.version = 1;
	v1.nonce = nonce + 3;
	ack_path(path, 0, 1, nonce + 3);
	write_test_bytes(path, &v1, sizeof(v1));
	UT_ASSERT(!cluster_cf_phase2_read_exact_ack(shared_root, 0, 1, nonce + 3));
}

UT_TEST(test_ack_cleanup_is_scoped_to_exact_responder_pair)
{
	char malformed[MAXPGPATH];
	char old_other_owner[MAXPGPATH];
	char old_other_responder[MAXPGPATH];
	char old_same_pair[MAXPGPATH];
	char current[MAXPGPATH];
	uint64 old_nonce = UINT64CONST(0x6000000000000006);
	uint64 current_nonce = UINT64CONST(0x6000000000000007);
	uint64 foreign_nonce = UINT64CONST(0x6000000000000008);
	char marker = 'x';

	make_shared_root();
	configure_nodes(4);
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, old_nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, old_nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 2, old_nonce));
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 1, foreign_nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 1, 2, foreign_nonce));
	ack_path(old_same_pair, 0, 1, old_nonce);
	ack_path(old_other_responder, 0, 2, old_nonce);
	ack_path(old_other_owner, 1, 2, foreign_nonce);
	snprintf(malformed, sizeof(malformed), "%s/%s/ack.0.1.foreign", shared_root,
			 CLUSTER_CF_PHASE2_DIR);
	write_test_bytes(malformed, &marker, sizeof(marker));

	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, current_nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, current_nonce));
	ack_path(current, 0, 1, current_nonce);
	UT_ASSERT(access(old_same_pair, F_OK) != 0);
	UT_ASSERT(access(current, F_OK) == 0);
	UT_ASSERT(access(old_other_responder, F_OK) == 0);
	UT_ASSERT(access(old_other_owner, F_OK) == 0);
	UT_ASSERT(access(malformed, F_OK) == 0);
}

UT_TEST(test_delayed_old_cleanup_cannot_delete_later_current_ack)
{
	uint64 old_nonce = UINT64CONST(0x6100000000000006);
	uint64 new_nonce = UINT64CONST(0x6100000000000007);
	pid_t child;
	int status;

	make_shared_root();
	configure_nodes(4);
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, old_nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, old_nonce));

	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		pause_next_allocate_dir = true;
		(void)cluster_cf_phase2_write_ack(shared_root, 0, 1, old_nonce);
		_exit(0);
	}
	UT_ASSERT_EQ(waitpid(child, &status, WUNTRACED), child);
	UT_ASSERT(WIFSTOPPED(status));

	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, new_nonce));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, new_nonce));
	UT_ASSERT_EQ(kill(child, SIGCONT), 0);
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT(cluster_cf_phase2_read_exact_ack(shared_root, 0, 1, new_nonce));
}

/* ======================================================================
 * rendezvous succeeds when the peer's probe + my ack are both visible
 * ====================================================================== */
UT_TEST(test_rendezvous_success)
{
	make_shared_root();
	configure_nodes(4);

	/*
	 * Pre-seed the peer side: peer (node 1) has published probe.1, and has
	 * already acked my probe (ack.0 echoes MY nonce).  rendezvous(self=0,
	 * peer=1, nonce=N) then: writes probe.0, sees probe.1 -> writes ack.1,
	 * sees ack.0 == N -> both directions verified on the first iteration.
	 */
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 1, UINT64CONST(0xAAAA0001BBBB0002)));
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, UINT64CONST(0xCCCC0003DDDD0004)));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 1, UINT64CONST(0xCCCC0003DDDD0004)));

	UT_ASSERT(
		cluster_cf_phase2_rendezvous(shared_root, 0, 1, UINT64CONST(0xCCCC0003DDDD0004), 60000));

	/* and it really wrote my probe + the peer's ack */
	{
		ClusterCfPhase2RecordV2 probe;

		UT_ASSERT(cluster_cf_phase2_read_probe(shared_root, 0, &probe));
		UT_ASSERT_EQ(probe.probe_nonce, UINT64CONST(0xCCCC0003DDDD0004));
		UT_ASSERT(
			cluster_cf_phase2_read_exact_ack(shared_root, 1, 0, UINT64CONST(0xAAAA0001BBBB0002)));
	}
}

/* ======================================================================
 * rendezvous fails closed when the peer never shows (timeout_ms = 0)
 * ====================================================================== */
UT_TEST(test_rendezvous_timeout)
{
	make_shared_root();
	configure_nodes(4);

	/* no peer probe, no ack -> deadline (now+0 == now) hits on iter 1 -> false */
	UT_ASSERT(!cluster_cf_phase2_rendezvous(shared_root, 0, 1, 0x5555ULL, 0));
}

/* ======================================================================
 * spec-5.6a: steady-state responder acks a fresh peer probe exactly once
 * (a rejoining peer's rendezvous completes against a silent live node).
 * ====================================================================== */
UT_TEST(test_respond_tick)
{
	char ackpath[MAXPGPATH];
	uint64 nonce1 = UINT64CONST(0x1111000122220002);
	uint64 nonce2 = UINT64CONST(0x3333000344440004);
	uint64 nonce3 = UINT64CONST(0x5555000566660006);

	make_shared_root();

	/* self = node 0; peer = node 1 in a 2-node config */
	cluster_controlfile_shared_authority = true;
	cluster_enabled = true;
	cluster_shared_data_dir = shared_root;
	configure_nodes(4);

	/* no probe yet -> nothing acked */
	cluster_cf_phase2_respond_tick();
	UT_ASSERT(!cluster_cf_phase2_read_exact_ack(shared_root, 1, 0, nonce1));

	/* the rejoining peer publishes a fresh probe -> the tick acks it */
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 1, nonce1));
	cluster_cf_phase2_respond_tick();
	UT_ASSERT(cluster_cf_phase2_read_exact_ack(shared_root, 1, 0, nonce1));

	/* Same exact tuple is idempotently restored if its ephemeral final vanishes. */
	ack_path(ackpath, 1, 0, nonce1);
	unlink(ackpath);
	cluster_cf_phase2_respond_tick();
	UT_ASSERT(cluster_cf_phase2_read_exact_ack(shared_root, 1, 0, nonce1));

	/* a NEW nonce (peer rebooted again) is acked afresh */
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 1, nonce2));
	cluster_cf_phase2_respond_tick();
	UT_ASSERT(cluster_cf_phase2_read_exact_ack(shared_root, 1, 0, nonce2));

	/* gates: authority off -> no-op */
	cluster_controlfile_shared_authority = false;
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 1, nonce3));
	cluster_cf_phase2_respond_tick();
	UT_ASSERT(!cluster_cf_phase2_read_exact_ack(shared_root, 1, 0, nonce3));
	cluster_controlfile_shared_authority = true;
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_probe_ack_roundtrip);
	UT_RUN(test_four_responders_publish_distinct_exact_acks);
	UT_RUN(test_nonselected_responder_cannot_complete_rendezvous);
	UT_RUN(test_delayed_old_nonce_writer_cannot_replace_current_ack);
	UT_RUN(test_duplicate_exact_ack_is_idempotent);
	UT_RUN(test_v2_crc_and_path_tuple_validation);
	UT_RUN(test_ack_cleanup_is_scoped_to_exact_responder_pair);
	UT_RUN(test_delayed_old_cleanup_cannot_delete_later_current_ack);
	UT_RUN(test_rendezvous_success);
	UT_RUN(test_rendezvous_timeout);
	UT_RUN(test_respond_tick);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
