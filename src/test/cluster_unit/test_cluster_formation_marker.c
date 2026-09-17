/*-------------------------------------------------------------------------
 *
 * test_cluster_formation_marker.c
 *    Focused unit tests for the B′ cold-formation commit marker codec:
 *    encode/decode round-trip, wire-CRC integrity (header + compact
 *    incarnation table), the 48/128 capacity clamp, and the corrupt-
 *    n_admitted out-of-bounds guard.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_formation_marker.h"
#include "cluster/cluster_voting_disk_io.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* libpgport's ARM CRC runtime chooser reports self-test diagnostics through
 * elog().  Keep this standalone codec unit independent of the backend error
 * subsystem with an inert logging surface. */
#undef errstart
#undef errstart_cold
#undef errfinish
bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

#include <stdio.h>

static int failures = 0;

#define EXPECT_TRUE(cond, msg)                                                                     \
	do {                                                                                           \
		if (!(cond)) {                                                                             \
			fprintf(stderr, "FAIL: %s (%s:%d)\n", msg, __FILE__, __LINE__);                        \
			failures++;                                                                            \
		} else {                                                                                   \
			printf("ok: %s\n", msg);                                                               \
		}                                                                                          \
	} while (0)

static void
test_roundtrip(void)
{
	ClusterFormationCommitMarker m;
	ClusterFormationCommitMarker out;
	uint64 inc_by_node[CLUSTER_MAX_NODES];
	uint64 out_inc[CLUSTER_MAX_NODES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	m.version = CLUSTER_FORMATION_MARKER_VERSION;
	m.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	m.formation_generation = 1;
	m.formation_epoch = 7;
	m.arbiter_node = 0;
	m.arbiter_incarnation = 1001;
	m.commit_nonce = 0xdeadbeef;
	m.admitted_nodes[0] = 0x03; /* members 0 and 1 */
	m.n_admitted = 2;
	cluster_formation_marker_compute_crc(&m);

	memset(inc_by_node, 0, sizeof(inc_by_node));
	inc_by_node[0] = 1001;
	inc_by_node[1] = 2002;

	EXPECT_TRUE(cluster_formation_marker_encode(&m, inc_by_node, slot), "encode two-member marker");
	memset(&out, 0, sizeof(out));
	memset(out_inc, 0, sizeof(out_inc));
	EXPECT_TRUE(cluster_formation_marker_decode(slot, &out, out_inc), "decode round-trip");
	EXPECT_TRUE(out.formation_generation == 1 && out.formation_epoch == 7 && out.arbiter_node == 0
					&& out.arbiter_incarnation == 1001 && out.commit_nonce == 0xdeadbeef
					&& out.n_admitted == 2,
				"decoded header fields match");
	EXPECT_TRUE(out_inc[0] == 1001 && out_inc[1] == 2002, "decoded incarnation table matches");
}

static void
test_wire_crc_covers_table(void)
{
	ClusterFormationCommitMarker m;
	uint64 inc_by_node[CLUSTER_MAX_NODES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	m.version = CLUSTER_FORMATION_MARKER_VERSION;
	m.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	m.formation_generation = 2;
	m.formation_epoch = 1;
	m.arbiter_node = 1;
	m.arbiter_incarnation = 55;
	m.commit_nonce = 7;
	m.admitted_nodes[0] = 0x03;
	m.n_admitted = 2;
	memset(inc_by_node, 0, sizeof(inc_by_node));
	inc_by_node[0] = 55;
	inc_by_node[1] = 66;
	EXPECT_TRUE(cluster_formation_marker_encode(&m, inc_by_node, slot), "encode for tamper test");

	/* Flip one byte INSIDE the compact table (entry 1's incarnation at
	 * 72 + 9 = 81).  A CRC that covers the table must reject it. */
	slot[81] ^= 0xFF;
	{
		ClusterFormationCommitMarker out;
		uint64 out_inc[CLUSTER_MAX_NODES];

		EXPECT_TRUE(!cluster_formation_marker_decode(slot, &out, out_inc),
					"table tamper rejected by wire CRC");
	}
}

static void
test_capacity_clamp(void)
{
	ClusterFormationCommitMarker m;
	uint64 inc_by_node[CLUSTER_MAX_NODES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	m.version = CLUSTER_FORMATION_MARKER_VERSION;
	m.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	m.formation_generation = 3;
	m.formation_epoch = 1;
	m.arbiter_node = 0;
	m.arbiter_incarnation = 1;
	m.commit_nonce = 1;
	/* member index beyond MAX_MEMBERS (48) — the 128-bit bitmap allows it
	 * but the compact table cannot hold it: must be rejected. */
	m.admitted_nodes[CLUSTER_FORMATION_MARKER_MAX_MEMBERS / 8]
		= (uint8)(1u << (CLUSTER_FORMATION_MARKER_MAX_MEMBERS % 8));
	m.n_admitted = 1;
	memset(inc_by_node, 0, sizeof(inc_by_node));
	inc_by_node[CLUSTER_FORMATION_MARKER_MAX_MEMBERS] = 9;
	EXPECT_TRUE(!cluster_formation_marker_encode(&m, inc_by_node, slot),
				"encode rejects member at MAX_MEMBERS");
}

static void
test_corrupt_n_admitted_oob(void)
{
	ClusterFormationCommitMarker m;
	uint64 inc_by_node[CLUSTER_MAX_NODES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	m.version = CLUSTER_FORMATION_MARKER_VERSION;
	m.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	m.formation_generation = 4;
	m.formation_epoch = 1;
	m.arbiter_node = 0;
	m.arbiter_incarnation = 1;
	m.commit_nonce = 2;
	m.admitted_nodes[0] = 0x01;
	m.n_admitted = 1;
	memset(inc_by_node, 0, sizeof(inc_by_node));
	inc_by_node[0] = 1;
	EXPECT_TRUE(cluster_formation_marker_encode(&m, inc_by_node, slot),
				"encode for corrupt-n test");

	/* Corrupt n_admitted to a huge value: decode must fail BEFORE hashing
	 * n*9 bytes (no out-of-bounds read). */
	slot[64] = 0xFF;
	slot[65] = 0xFF;
	{
		ClusterFormationCommitMarker out;
		uint64 out_inc[CLUSTER_MAX_NODES];

		EXPECT_TRUE(!cluster_formation_marker_decode(slot, &out, out_inc),
					"corrupt n_admitted rejected");
	}
}

static void
test_zero_members_rejected(void)
{
	ClusterFormationCommitMarker m;
	uint64 inc_by_node[CLUSTER_MAX_NODES];
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_FORMATION_MARKER_MAGIC;
	m.version = CLUSTER_FORMATION_MARKER_VERSION;
	m.phase = CLUSTER_FORMATION_MARKER_PHASE_COMMITTED;
	m.formation_generation = 5;
	m.formation_epoch = 1;
	m.arbiter_node = 0;
	m.arbiter_incarnation = 1;
	m.commit_nonce = 3;
	m.n_admitted = 0; /* no members — structural violation */
	memset(inc_by_node, 0, sizeof(inc_by_node));
	EXPECT_TRUE(!cluster_formation_marker_encode(&m, inc_by_node, slot),
				"zero-member marker rejected");
}

int
main(int argc, char **argv)
{
	printf("=== test_cluster_formation_marker ===\n");
	test_roundtrip();
	test_wire_crc_covers_table();
	test_capacity_clamp();
	test_corrupt_n_admitted_oob();
	test_zero_members_rejected();

	if (failures != 0) {
		fprintf(stderr, "%d FAILURE(S)\n", failures);
		return 1;
	}
	printf("all tests passed\n");
	return 0;
}
