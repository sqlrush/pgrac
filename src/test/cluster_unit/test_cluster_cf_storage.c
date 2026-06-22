/*-------------------------------------------------------------------------
 *
 * test_cluster_cf_storage.c
 *	  Runtime unit tests for the shared pg_control migration / symlink /
 *	  storage rename-contract module (spec-5.6 Da2 / T6).
 *
 *	  Exercises the real module against temp directories: the pure B5
 *	  write-gate, lstat/readlink symlink classification, the Phase-1 local
 *	  rename probe, and end-to-end migrate+symlink of a per-node control
 *	  file into a shared authority.  Cross-node Phase-2 verification is
 *	  TAP territory (t/289).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cf_storage.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Da2/T6)
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
#include <unistd.h>

#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/storage/cluster_shared_fs.h"
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

char *cluster_shared_data_dir = NULL;
bool cluster_controlfile_shared_authority = false;
char *DataDir = NULL;					   /* storage.o's bootstrap orchestrator refs it */
int cluster_cf_enqueue_timeout_ms = 30000; /* orchestrator liveness wait */

/* ---- Assert + ereport + fd.c stubs (same pattern as the authority test) ---- */
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
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errcode(int sqlerrcode pg_attribute_unused())
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

int
pg_fsync(int fd)
{
	return fsync(fd);
}

int
durable_rename(const char *oldfile, const char *newfile, int elevel pg_attribute_unused())
{
	return rename(oldfile, newfile) == 0 ? 0 : -1;
}

/* spec-5.6 increment (iii): the bootstrap orchestrator consults the Phase-2
 * peer-verified signal (cluster_cf_phase2.o, not linked here); the pure tests
 * never call the orchestrator, so a trivial stub satisfies the link. */
bool
cluster_cf_phase2_peer_verified(void)
{
	return false;
}

/* ---- CSSD / qvotec sole-liveness stubs (settable) ---- */
static ClusterCssdStatus g_cssd_status = CLUSTER_CSSD_READY;
static int g_alive_peers = 0;
static bool g_in_quorum = true;

ClusterCssdStatus
cluster_cssd_get_status(void)
{
	return g_cssd_status;
}
int
cluster_cssd_get_alive_peer_count(void)
{
	return g_alive_peers;
}
bool
cluster_qvotec_in_quorum(void)
{
	return g_in_quorum;
}

/* ---- stubs for the bootstrap-window orchestrator's deps (not exercised) ---- */
bool cluster_enabled = false;
int
cluster_conf_node_count(void)
{
	return 1;
}
void
cluster_cf_set_bootstrap_authority(bool on)
{
	(void)on;
}
void
cluster_cf_set_join_readonly(bool on)
{
	(void)on;
}
void
cluster_cf_set_write_skip(bool on)
{
	(void)on;
}

/* spec-5.6 Dc2: cluster_cf_authority.o (linked here for the migrate path) now
 * references the .bak checkpoint-recoverable probe; this test never reaches
 * the .bak fallback, so a trivial stub satisfies the link. */
bool
cluster_cf_bak_checkpoint_recoverable(const ControlFileData *bak pg_attribute_unused())
{
	return true;
}

/* spec-5.6 Dc4: storage + authority objects bump CF counters; cluster_cf_stats.o
 * is not linked here, so a no-op stub satisfies the link. */
void
cluster_cf_counter_inc(ClusterCfCounter which pg_attribute_unused())
{}

/* Increment (i): the contract persist/load reads the shared-storage uuid from
 * the sentinel (cluster_shared_fs_sharedfs.o, not linked).  Settable stub so a
 * test can simulate a storage identity change. */
static char g_storage_uuid[CLUSTER_SHARED_UUID_LEN] = "";
void
cluster_shared_fs_get_storage_uuid(char *out, size_t outlen)
{
	if (out == NULL || outlen == 0)
		return;
	strlcpy(out, g_storage_uuid, outlen);
}

/* ---- fixture ---- */
static char shared_root[MAXPGPATH];

static void
make_tempdir(char *out, size_t outlen, const char *suffix)
{
	char tmpl[MAXPGPATH];
	char sub[MAXPGPATH];

	snprintf(tmpl, sizeof(tmpl), "/tmp/pgrac_cf_%s_XXXXXX", suffix);
	if (mkdtemp(tmpl) == NULL) {
		printf("# mkdtemp failed: %s\n", strerror(errno));
		abort();
	}
	strlcpy(out, tmpl, outlen);
	snprintf(sub, sizeof(sub), "%s/global", out);
	if (mkdir(sub, 0700) != 0 && errno != EEXIST) {
		printf("# mkdir global failed: %s\n", strerror(errno));
		abort();
	}
}

/* Write a valid ControlFileData (8KB, padded) at path. */
static void
write_local_cf(const char *path, uint64 sysid)
{
	ControlFileData cf;
	char buf[PG_CONTROL_FILE_SIZE];
	int fd;

	memset(&cf, 0, sizeof(cf));
	cf.pg_control_version = PG_CONTROL_VERSION;
	cf.system_identifier = sysid;
	cf.state = DB_IN_PRODUCTION;
	INIT_CRC32C(cf.crc);
	COMP_CRC32C(cf.crc, (char *)&cf, offsetof(ControlFileData, crc));
	FIN_CRC32C(cf.crc);

	memset(buf, 0, sizeof(buf));
	memcpy(buf, &cf, sizeof(cf));

	fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, buf, sizeof(buf)) != sizeof(buf)) {
		printf("# write_local_cf failed: %s\n", strerror(errno));
		abort();
	}
	close(fd);
}

/* ======================================================================
 * B5 write-gate (pure)
 * ====================================================================== */
UT_TEST(test_write_allowed)
{
	/* multi-node: only crossnode_verified unblocks single-node-authority */
	UT_ASSERT(!cluster_cf_storage_write_allowed(CLUSTER_CF_CONTRACT_UNVERIFIED, true));
	UT_ASSERT(!cluster_cf_storage_write_allowed(CLUSTER_CF_CONTRACT_LOCAL_PROBED, true));
	UT_ASSERT(cluster_cf_storage_write_allowed(CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED, true));
	/* single-node cluster: always allowed regardless of contract state */
	UT_ASSERT(cluster_cf_storage_write_allowed(CLUSTER_CF_CONTRACT_UNVERIFIED, false));
	UT_ASSERT(cluster_cf_storage_write_allowed(CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED, false));
}

/* ======================================================================
 * symlink classification (real FS)
 * ====================================================================== */
UT_TEST(test_symlink_status)
{
	char root[MAXPGPATH];
	char target[MAXPGPATH];
	char local[MAXPGPATH];

	make_tempdir(root, sizeof(root), "symlink");
	snprintf(target, sizeof(target), "%s/global/pg_control", root);
	snprintf(local, sizeof(local), "%s/local_pg_control", root);

	/* missing */
	UT_ASSERT_EQ(cluster_cf_symlink_status(local, target), CLUSTER_CF_SYMLINK_MISSING);

	/* regular file -> per-node local hazard */
	write_local_cf(local, 0x55);
	UT_ASSERT_EQ(cluster_cf_symlink_status(local, target), CLUSTER_CF_SYMLINK_NOT_SYMLINK);

	/* symlink to the wrong target */
	unlink(local);
	if (symlink("/some/other/pg_control", local) != 0)
		abort();
	UT_ASSERT_EQ(cluster_cf_symlink_status(local, target), CLUSTER_CF_SYMLINK_WRONG_TARGET);

	/* symlink to the expected target */
	unlink(local);
	if (symlink(target, local) != 0)
		abort();
	UT_ASSERT_EQ(cluster_cf_symlink_status(local, target), CLUSTER_CF_SYMLINK_OK);
}

/* ======================================================================
 * Phase-1 local rename probe (real FS)
 * ====================================================================== */
UT_TEST(test_probe_local)
{
	make_tempdir(shared_root, sizeof(shared_root), "probe");
	cluster_shared_data_dir = shared_root;

	UT_ASSERT(cluster_cf_storage_probe_local());
}

/* ======================================================================
 * migrate per-node control file into a shared authority + symlink
 * ====================================================================== */
UT_TEST(test_migrate_and_link)
{
	char pgdata[MAXPGPATH];
	char local[MAXPGPATH];
	char linkbuf[MAXPGPATH];
	ControlFileData out;
	ssize_t n;

	/* fresh shared root (no authority yet) */
	make_tempdir(shared_root, sizeof(shared_root), "migrate_shared");
	cluster_shared_data_dir = shared_root;

	/* node $PGDATA with a real per-node control file */
	make_tempdir(pgdata, sizeof(pgdata), "migrate_pgdata");
	snprintf(local, sizeof(local), "%s/global/pg_control", pgdata);
	write_local_cf(local, 0x0A0B0C0D0E0F1011ULL);

	UT_ASSERT(cluster_cf_migrate_and_link(pgdata));

	/* local path is now a symlink to the shared authority */
	n = readlink(local, linkbuf, sizeof(linkbuf) - 1);
	UT_ASSERT(n > 0);
	if (n > 0) {
		linkbuf[n] = '\0';
		UT_ASSERT_STR_EQ(linkbuf, cluster_cf_shared_path());
	}

	/* shared authority is readable and carries the migrated identity */
	memset(&out, 0xEE, sizeof(out));
	UT_ASSERT(cluster_cf_authority_read(&out));
	UT_ASSERT_EQ(out.system_identifier, 0x0A0B0C0D0E0F1011ULL);

	/* idempotent: a second call leaves the correct symlink in place */
	UT_ASSERT(cluster_cf_migrate_and_link(pgdata));
	UT_ASSERT_EQ(cluster_cf_symlink_status(local, cluster_cf_shared_path()), CLUSTER_CF_SYMLINK_OK);
}

/* ======================================================================
 * Da3 startup gate verdict (pure)
 * ====================================================================== */
UT_TEST(test_startup_verdict)
{
	/* a correct symlink to a readable, identity-matched authority -> OK */
	UT_ASSERT_EQ(cluster_cf_startup_verdict(CLUSTER_CF_SYMLINK_OK, true, true),
				 CLUSTER_CF_STARTUP_OK);
	/* per-node regular file in authority mode -> FATAL (DoD-5) */
	UT_ASSERT_EQ(cluster_cf_startup_verdict(CLUSTER_CF_SYMLINK_NOT_SYMLINK, true, true),
				 CLUSTER_CF_STARTUP_FATAL_NOT_SYMLINK);
	/* symlink to a foreign authority -> FATAL */
	UT_ASSERT_EQ(cluster_cf_startup_verdict(CLUSTER_CF_SYMLINK_WRONG_TARGET, true, true),
				 CLUSTER_CF_STARTUP_FATAL_WRONG_TARGET);
	/* no control path -> FATAL */
	UT_ASSERT_EQ(cluster_cf_startup_verdict(CLUSTER_CF_SYMLINK_MISSING, true, true),
				 CLUSTER_CF_STARTUP_FATAL_MISSING);
	/* correct symlink but authority unreadable/torn -> FATAL */
	UT_ASSERT_EQ(cluster_cf_startup_verdict(CLUSTER_CF_SYMLINK_OK, false, false),
				 CLUSTER_CF_STARTUP_FATAL_UNREADABLE);
	/* correct symlink, readable, but foreign system_identifier -> FATAL */
	UT_ASSERT_EQ(cluster_cf_startup_verdict(CLUSTER_CF_SYMLINK_OK, true, false),
				 CLUSTER_CF_STARTUP_FATAL_IDENTITY);
}

/* ======================================================================
 * B4 sole-liveness positive proof (fail-closed on any uncertainty)
 * ====================================================================== */
UT_TEST(test_sole_liveness)
{
	g_cssd_status = CLUSTER_CSSD_READY;
	g_in_quorum = true;
	g_alive_peers = 0;
	UT_ASSERT(cluster_cf_verify_sole_liveness_or_fail()); /* all positive */

	g_cssd_status = CLUSTER_CSSD_STARTING; /* CSSD not ready */
	UT_ASSERT(!cluster_cf_verify_sole_liveness_or_fail());

	g_cssd_status = CLUSTER_CSSD_READY;
	g_in_quorum = false; /* not in quorum */
	UT_ASSERT(!cluster_cf_verify_sole_liveness_or_fail());

	g_in_quorum = true;
	g_alive_peers = 1; /* a peer is alive */
	UT_ASSERT(!cluster_cf_verify_sole_liveness_or_fail());

	g_alive_peers = 0; /* reset */
}

/* ======================================================================
 * B3/B5 bootstrap authority gate = sole-liveness AND storage contract
 * ====================================================================== */
UT_TEST(test_bootstrap_gate)
{
	g_cssd_status = CLUSTER_CSSD_READY;
	g_in_quorum = true;
	g_alive_peers = 0;

	/* sole-live + single-node cluster -> allowed */
	UT_ASSERT(cluster_cf_bootstrap_authority_gate(false, CLUSTER_CF_CONTRACT_UNVERIFIED));
	/* sole-live + multi-node but never cross-node verified -> blocked (B5) */
	UT_ASSERT(!cluster_cf_bootstrap_authority_gate(true, CLUSTER_CF_CONTRACT_UNVERIFIED));
	/* sole-live + multi-node + cross-node verified -> allowed */
	UT_ASSERT(cluster_cf_bootstrap_authority_gate(true, CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED));

	/* not sole-live (a peer is alive) -> blocked even for single-node config */
	g_alive_peers = 1;
	UT_ASSERT(!cluster_cf_bootstrap_authority_gate(false, CLUSTER_CF_CONTRACT_UNVERIFIED));
	g_alive_peers = 0;
}

/* ======================================================================
 * Storage-contract: pure identity-bound resolve (Increment i)
 * ====================================================================== */
UT_TEST(test_contract_resolve)
{
	/* matching uuid + persisted VERIFIED -> VERIFIED */
	UT_ASSERT_EQ(cluster_cf_contract_resolve("aaa", "aaa", CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED),
				 CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED);
	/* matching uuid but only LOCAL_PROBED persisted -> UNVERIFIED */
	UT_ASSERT_EQ(cluster_cf_contract_resolve("aaa", "aaa", CLUSTER_CF_CONTRACT_LOCAL_PROBED),
				 CLUSTER_CF_CONTRACT_UNVERIFIED);
	/* different uuid (storage swap) even with VERIFIED -> UNVERIFIED (DoD#3) */
	UT_ASSERT_EQ(cluster_cf_contract_resolve("aaa", "bbb", CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED),
				 CLUSTER_CF_CONTRACT_UNVERIFIED);
	/* no current identity -> cannot bind -> UNVERIFIED */
	UT_ASSERT_EQ(cluster_cf_contract_resolve("aaa", "", CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED),
				 CLUSTER_CF_CONTRACT_UNVERIFIED);
	UT_ASSERT_EQ(cluster_cf_contract_resolve("aaa", NULL, CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED),
				 CLUSTER_CF_CONTRACT_UNVERIFIED);
	/* no persisted record -> UNVERIFIED */
	UT_ASSERT_EQ(cluster_cf_contract_resolve("", "aaa", CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED),
				 CLUSTER_CF_CONTRACT_UNVERIFIED);
}

/* ======================================================================
 * Storage-contract: persist + identity-bound load round trip (real FS)
 * ====================================================================== */
UT_TEST(test_contract_persist_load)
{
	char pgdata[MAXPGPATH];
	char file[MAXPGPATH];
	int fd;
	char junk[8] = { 0 };

	make_tempdir(pgdata, sizeof(pgdata), "contract");

	/* nothing persisted yet -> UNVERIFIED */
	strlcpy(g_storage_uuid, "00112233445566778899aabbccddeeff", sizeof(g_storage_uuid));
	UT_ASSERT_EQ(cluster_cf_contract_load(pgdata), CLUSTER_CF_CONTRACT_UNVERIFIED);

	/* persist VERIFIED bound to the current uuid -> load resolves VERIFIED */
	UT_ASSERT(cluster_cf_contract_persist(pgdata, CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED));
	UT_ASSERT_EQ(cluster_cf_contract_load(pgdata), CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED);

	/* storage identity changes under us -> binding breaks -> UNVERIFIED (DoD#3) */
	strlcpy(g_storage_uuid, "ffffffffffffffffffffffffffffffff", sizeof(g_storage_uuid));
	UT_ASSERT_EQ(cluster_cf_contract_load(pgdata), CLUSTER_CF_CONTRACT_UNVERIFIED);

	/* restore identity -> VERIFIED again (proves it was the binding, not loss) */
	strlcpy(g_storage_uuid, "00112233445566778899aabbccddeeff", sizeof(g_storage_uuid));
	UT_ASSERT_EQ(cluster_cf_contract_load(pgdata), CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED);

	/* a torn/short record reads as UNVERIFIED (never throws) */
	snprintf(file, sizeof(file), "%s/%s", pgdata, CLUSTER_CF_CONTRACT_REL_PATH);
	fd = open(file, O_RDWR | O_TRUNC, 0600);
	if (fd < 0 || write(fd, junk, sizeof(junk)) != (ssize_t)sizeof(junk))
		abort();
	close(fd);
	UT_ASSERT_EQ(cluster_cf_contract_load(pgdata), CLUSTER_CF_CONTRACT_UNVERIFIED);

	/* persisting only LOCAL_PROBED never resolves to VERIFIED */
	UT_ASSERT(cluster_cf_contract_persist(pgdata, CLUSTER_CF_CONTRACT_LOCAL_PROBED));
	UT_ASSERT_EQ(cluster_cf_contract_load(pgdata), CLUSTER_CF_CONTRACT_UNVERIFIED);
}

/* ======================================================================
 * Tri-state liveness assessment (Increment ii)
 * ====================================================================== */
UT_TEST(test_assess_liveness)
{
	g_cssd_status = CLUSTER_CSSD_READY;
	g_in_quorum = true;
	g_alive_peers = 0;
	UT_ASSERT_EQ(cluster_cf_assess_liveness(), CLUSTER_CF_LIVENESS_SOLE);

	g_alive_peers = 2; /* a peer is alive */
	UT_ASSERT_EQ(cluster_cf_assess_liveness(), CLUSTER_CF_LIVENESS_PEER_ALIVE);

	g_alive_peers = 0;
	g_cssd_status = CLUSTER_CSSD_STARTING; /* CSSD not ready */
	UT_ASSERT_EQ(cluster_cf_assess_liveness(), CLUSTER_CF_LIVENESS_UNKNOWN);

	g_cssd_status = CLUSTER_CSSD_READY;
	g_in_quorum = false; /* not in quorum, 0 peers */
	UT_ASSERT_EQ(cluster_cf_assess_liveness(), CLUSTER_CF_LIVENESS_UNKNOWN);

	/* a demonstrably-alive peer is PEER_ALIVE even without quorum (the join
	 * read-only role needs no quorum; only the SOLE owner-write path does) */
	g_alive_peers = 1;
	g_in_quorum = false;
	UT_ASSERT_EQ(cluster_cf_assess_liveness(), CLUSTER_CF_LIVENESS_PEER_ALIVE);

	g_in_quorum = true; /* reset */
	g_alive_peers = 0;
}

/* ======================================================================
 * Bootstrap role classification (pure; Increment ii)
 * ====================================================================== */
UT_TEST(test_bootstrap_role)
{
	/* single-node cluster: always owner, regardless of liveness/contract */
	UT_ASSERT_EQ(cluster_cf_bootstrap_role(false, CLUSTER_CF_LIVENESS_UNKNOWN,
										   CLUSTER_CF_CONTRACT_UNVERIFIED),
				 CLUSTER_CF_ROLE_OWNER);

	/* multi-node, verified sole-live node -> owner (may write authority) */
	UT_ASSERT_EQ(cluster_cf_bootstrap_role(true, CLUSTER_CF_LIVENESS_SOLE,
										   CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED),
				 CLUSTER_CF_ROLE_OWNER);
	/* multi-node, sole-live but never verified -> fail closed (B5) */
	UT_ASSERT_EQ(
		cluster_cf_bootstrap_role(true, CLUSTER_CF_LIVENESS_SOLE, CLUSTER_CF_CONTRACT_UNVERIFIED),
		CLUSTER_CF_ROLE_FAILCLOSED);

	/* multi-node, peer alive + verified -> join read-only (must not write) */
	UT_ASSERT_EQ(cluster_cf_bootstrap_role(true, CLUSTER_CF_LIVENESS_PEER_ALIVE,
										   CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED),
				 CLUSTER_CF_ROLE_JOIN_READONLY);
	/* multi-node, peer alive but unverified -> fail closed (can't trust reads) */
	UT_ASSERT_EQ(cluster_cf_bootstrap_role(true, CLUSTER_CF_LIVENESS_PEER_ALIVE,
										   CLUSTER_CF_CONTRACT_UNVERIFIED),
				 CLUSTER_CF_ROLE_FAILCLOSED);

	/* multi-node, liveness unknown -> fail closed even if verified */
	UT_ASSERT_EQ(cluster_cf_bootstrap_role(true, CLUSTER_CF_LIVENESS_UNKNOWN,
										   CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED),
				 CLUSTER_CF_ROLE_FAILCLOSED);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_write_allowed);
	UT_RUN(test_symlink_status);
	UT_RUN(test_probe_local);
	UT_RUN(test_migrate_and_link);
	UT_RUN(test_startup_verdict);
	UT_RUN(test_sole_liveness);
	UT_RUN(test_bootstrap_gate);
	UT_RUN(test_contract_resolve);
	UT_RUN(test_contract_persist_load);
	UT_RUN(test_assess_liveness);
	UT_RUN(test_bootstrap_role);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
