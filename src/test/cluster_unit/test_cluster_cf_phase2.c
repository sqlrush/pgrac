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
#include <unistd.h>

#include "cluster/cluster_cf_phase2.h"
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
int			cluster_node_id = 0;
bool		cluster_enabled = false;
char	   *cluster_shared_data_dir = NULL;
bool		cluster_controlfile_shared_authority = false;
int			cluster_cf_enqueue_timeout_ms = 30000;
volatile sig_atomic_t InterruptPending = 0;
int			pg_dir_create_mode = 0700;

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
	if (elevel >= ERROR)
	{
		printf("# unexpected ereport(elevel=%d) -- aborting\n", elevel);
		abort();
	}
	return false;
}

bool errstart_cold(int elevel, const char *domain) { return errstart(elevel, domain); }
void errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(), const char *fn pg_attribute_unused()) {}
int errcode(int c pg_attribute_unused()) { return 0; }
int errcode_for_file_access(void) { return 0; }
int errmsg(const char *fmt pg_attribute_unused(),...) { return 0; }
int errmsg_internal(const char *fmt pg_attribute_unused(),...) { return 0; }
int errdetail(const char *fmt pg_attribute_unused(),...) { return 0; }
int errhint(const char *fmt pg_attribute_unused(),...) { return 0; }
void ProcessInterrupts(void) {}

int OpenTransientFile(const char *fileName, int fileFlags) { return open(fileName, fileFlags, 0600); }
int CloseTransientFile(int fd) { return close(fd); }
int pg_fsync(int fd) { return fsync(fd); }
int
durable_rename(const char *o, const char *n, int e pg_attribute_unused())
{
	return rename(o, n) == 0 ? 0 : -1;
}

/* GetCurrentTimestamp stub: a fixed value so the rendezvous deadline math is
 * deterministic (timeout_ms=0 -> deadline == now -> immediate timeout). */
TimestampTz GetCurrentTimestamp(void) { return 1; }

/* contract persist/load: not reached by these tests (only verify_or_fail uses
 * them); stub to satisfy the link without pulling in cluster_cf_storage.o. */
bool
cluster_cf_contract_persist(const char *p pg_attribute_unused(),
							ClusterCfContractState s pg_attribute_unused())
{
	return true;
}
ClusterCfContractState
cluster_cf_contract_load(const char *p pg_attribute_unused())
{
	return CLUSTER_CF_CONTRACT_UNVERIFIED;
}

/* find_peer_node() deps (verify_or_fail path; not exercised). */
const void *cluster_conf_lookup_node(int32 id pg_attribute_unused()) { return NULL; }
int cluster_conf_node_count(void) { return 1; }

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
make_shared_root(void)
{
	char		tmpl[MAXPGPATH];
	char		sub[MAXPGPATH];

	snprintf(tmpl, sizeof(tmpl), "/tmp/pgrac_cf_p2_XXXXXX");
	if (mkdtemp(tmpl) == NULL)
	{
		printf("# mkdtemp failed: %s\n", strerror(errno));
		abort();
	}
	strlcpy(shared_root, tmpl, sizeof(shared_root));
	snprintf(sub, sizeof(sub), "%s/global", shared_root);
	if (mkdir(sub, 0700) != 0 && errno != EEXIST)
	{
		printf("# mkdir global failed: %s\n", strerror(errno));
		abort();
	}
	snprintf(sub, sizeof(sub), "%s/%s", shared_root, CLUSTER_CF_PHASE2_DIR);
	if (mkdir(sub, 0700) != 0 && errno != EEXIST)
	{
		printf("# mkdir p2 failed: %s\n", strerror(errno));
		abort();
	}
}

/* ======================================================================
 * probe/ack file round-trip + corruption + absence
 * ====================================================================== */
UT_TEST(test_probe_ack_roundtrip)
{
	uint64		got;

	make_shared_root();

	/* missing probe/ack read as false */
	UT_ASSERT(!cluster_cf_phase2_read_probe(shared_root, 7, &got));
	UT_ASSERT(!cluster_cf_phase2_read_ack(shared_root, 7, &got));

	/* probe round-trip */
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 0, 0xABCDEF0123456789ULL));
	UT_ASSERT(cluster_cf_phase2_read_probe(shared_root, 0, &got));
	UT_ASSERT_EQ(got, 0xABCDEF0123456789ULL);

	/* ack round-trip (ack.<peer> echoes the peer's nonce) */
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, 0x1122334455667788ULL));
	UT_ASSERT(cluster_cf_phase2_read_ack(shared_root, 0, &got));
	UT_ASSERT_EQ(got, 0x1122334455667788ULL);

	/* a corrupt (truncated) probe reads as false */
	{
		char		path[MAXPGPATH];
		int			fd;
		char		junk[4] = {1, 2, 3, 4};

		snprintf(path, sizeof(path), "%s/%s/probe.0", shared_root, CLUSTER_CF_PHASE2_DIR);
		fd = open(path, O_RDWR | O_TRUNC, 0600);
		if (fd < 0 || write(fd, junk, sizeof(junk)) != (ssize_t) sizeof(junk))
			abort();
		close(fd);
		UT_ASSERT(!cluster_cf_phase2_read_probe(shared_root, 0, &got));
	}
}

/* ======================================================================
 * rendezvous succeeds when the peer's probe + my ack are both visible
 * ====================================================================== */
UT_TEST(test_rendezvous_success)
{
	make_shared_root();

	/*
	 * Pre-seed the peer side: peer (node 1) has published probe.1, and has
	 * already acked my probe (ack.0 echoes MY nonce).  rendezvous(self=0,
	 * peer=1, nonce=N) then: writes probe.0, sees probe.1 -> writes ack.1,
	 * sees ack.0 == N -> both directions verified on the first iteration.
	 */
	UT_ASSERT(cluster_cf_phase2_write_probe(shared_root, 1, UINT64CONST(0xAAAA0001BBBB0002)));
	UT_ASSERT(cluster_cf_phase2_write_ack(shared_root, 0, UINT64CONST(0xCCCC0003DDDD0004)));

	UT_ASSERT(cluster_cf_phase2_rendezvous(shared_root, 0, 1,
										   UINT64CONST(0xCCCC0003DDDD0004), 60000));

	/* and it really wrote my probe + the peer's ack */
	{
		uint64		n;

		UT_ASSERT(cluster_cf_phase2_read_probe(shared_root, 0, &n));
		UT_ASSERT(cluster_cf_phase2_read_ack(shared_root, 1, &n));	/* ack.1 echoes peer nonce */
		UT_ASSERT_EQ(n, UINT64CONST(0xAAAA0001BBBB0002));
	}
}

/* ======================================================================
 * rendezvous fails closed when the peer never shows (timeout_ms = 0)
 * ====================================================================== */
UT_TEST(test_rendezvous_timeout)
{
	make_shared_root();

	/* no peer probe, no ack -> deadline (now+0 == now) hits on iter 1 -> false */
	UT_ASSERT(!cluster_cf_phase2_rendezvous(shared_root, 0, 1, 0x5555ULL, 0));
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_probe_ack_roundtrip);
	UT_RUN(test_rendezvous_success);
	UT_RUN(test_rendezvous_timeout);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
