/*-------------------------------------------------------------------------
 *
 * test_cluster_ic.c
 *	  Compile-time / link-level invariants for the cluster internal
 *	  IPC abstraction layer introduced in stage 0.18.
 *
 *	  Stage 0.18 ships only the stub interconnect tier: target == self
 *	  is a no-op success, target != self ereports
 *	  ERRCODE_FEATURE_NOT_SUPPORTED.  Real send/recv round-trips are
 *	  verified at the SQL level by cluster_tap t/012_ic.pl on a
 *	  running PG instance; this unit test only locks the wire-format
 *	  size, the magic / protocol_version constants, and the symbol
 *	  surface that Stage 2+ subsystems will link against.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ic.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_ic.o standalone
 *	  pulls in references to ereport / pg_crc32c / cluster_node_id;
 *	  those are stubbed locally below so the binary can run without
 *	  the full PG backend.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_xnode_profile.h" /* spec-5.59 D6 stub — profiling gate */

/*
 * postgres.h transitively pulls in port.h which redirects printf etc.
 * Standalone unit-test binaries do not link libpgport, so undo the
 * redirection before pulling in unit_test.h.
 */
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


/* ----------
 * Stubs needed to link cluster_ic.o standalone.
 *
 *	cluster_ic.c calls into pg_crc32c (provided by libpgport, not
 *	linked here) and references cluster_node_id (defined in
 *	cluster_guc.o, not linked here).  We provide minimal local
 *	definitions; these tests only check linkability and constants,
 *	so the stub return values are inert.
 *
 *	cluster_ic.c also calls ereport on the !self path; ereport is
 *	macro-expanded into errstart/errmsg/errfinish.  We provide minimal
 *	stubs for those so the linker is satisfied.  The tests below never
 *	invoke a path that triggers ereport.
 *
 *	Stage 0.26 added the mock vtable + four cluster_ic_mock_* SRFs to
 *	cluster_ic.o; their bodies reference palloc / pfree / TopMemoryContext
 *	/ pg_detoast_datum_packed / InitMaterializedSRF / tuplestore_putvalues.
 *	None of those paths are exercised here, so the additional stubs are
 *	inert.
 * ----------
 */
#include "fmgr.h"
#include "funcapi.h"
#include "utils/elog.h"
#include "utils/memutils.h"

int cluster_node_id = -1;
int cluster_interconnect_tier = 0; /* CLUSTER_IC_TIER_STUB */
bool cluster_smart_fusion = false;
int cluster_smart_fusion_tier_min = CLUSTER_IC_TIER_3;
/* spec-2.2 additive amendment (spec-5.22e D5 prereq): test-only old-binary
 * simulation gate consumed by cluster_ic.c::cluster_ic_build_hello. */
bool cluster_ic_suppress_caps_reply = false;
/* GCS-race round-2 RC-F mixed-version leg: same discipline for the
 * GCS_DONE_V1 completion-proof capability bit. */
bool cluster_ic_suppress_gcs_done_cap = false;
bool cluster_ic_suppress_xid_flock_cap = false;

/* spec-5.59 D1 stubs: cluster_ic.o now carries GUC-gated profiling probes
 * (cluster_xnode_profile.h); the unit harness links neither cluster_guc.o
 * nor cluster_xnode_profile.o, so define the two gate symbols inertly
 * (probes early-return on enabled=false / Ctl=NULL). */
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;

/*
 * spec-2.1 Hardening v1.0.2 D-I1 -- F2 extension fix in cluster_ic.c::
 * cluster_ic_init adds an extern reference to cluster_enabled
 * (defensive guard mirroring the cluster_conf_load v1.0.1 pattern).
 * Stub matches GUC default; tests do not exercise the
 * !cluster_enabled early-return path (verified at TAP layer L12).
 */
bool cluster_enabled = true;

/* spec-2.2 §3.9 D2 -- cluster_ic.c references MyBackendType for
 * the tier1 caller scope guard.  Stub here; unit test never invokes
 * the runtime path that reads it. */
#include "miscadmin.h"
BackendType MyBackendType = B_INVALID;

/*
 * spec-2.2 D3 (post-codex review) -- test-local ClusterICOps_Tier1 stub.
 *
 * The real Tier1 vtable lives in cluster_ic_tier1.c (production build);
 * cluster_ic.c references ClusterICOps_Tier1 from cluster_ic_init, so
 * any binary that links cluster_ic.o must resolve the symbol.  In the
 * standalone test_cluster_ic binary we DON'T link cluster_ic_tier1.o
 * (which would drag in cluster_conf / cluster_shmem / GetCurrentTimestamp
 * / atomics state that the unit-test stub doesn't model).  Provide our
 * own const struct with non-NULL placeholder function pointers; these
 * functions are never invoked because cluster_unit only takes addresses
 * (test_tier1_vtable_extern_linkable).  Real behaviour is verified at
 * TAP layer (075 single-instance + 076 2-node A-lite, in Steps 10-11).
 */
static ClusterICSendResult
tier1_test_stub_send(int32 t pg_attribute_unused(), const void *b pg_attribute_unused(),
					 size_t l pg_attribute_unused())
{
	return CLUSTER_IC_SEND_HARD_ERROR;
}
static bool
tier1_test_stub_recv(int32 *s pg_attribute_unused(), void *b pg_attribute_unused(),
					 size_t bs pg_attribute_unused(), size_t *r pg_attribute_unused())
{
	return false;
}
static bool
tier1_test_stub_peek(int32 *s pg_attribute_unused())
{
	return false;
}
static void
tier1_test_stub_init(void)
{}
static void
tier1_test_stub_shutdown(void)
{}

const ClusterICOps ClusterICOps_Tier1 = {
	.send_bytes = tier1_test_stub_send,
	.recv_bytes = tier1_test_stub_recv,
	.peek_sender = tier1_test_stub_peek,
	.tier_init = tier1_test_stub_init,
	.tier_shutdown = tier1_test_stub_shutdown,
	.tier_name = "tier1-unit-test-stub",
};

const ClusterICOps ClusterICOps_Tier2 = {
	.send_bytes = tier1_test_stub_send,
	.recv_bytes = tier1_test_stub_recv,
	.peek_sender = tier1_test_stub_peek,
	.tier_init = tier1_test_stub_init,
	.tier_shutdown = tier1_test_stub_shutdown,
	.tier_name = "tier2-unit-test-stub",
};

const ClusterICOps ClusterICOps_Tier3 = {
	.send_bytes = tier1_test_stub_send,
	.recv_bytes = tier1_test_stub_recv,
	.peek_sender = tier1_test_stub_peek,
	.tier_init = tier1_test_stub_init,
	.tier_shutdown = tier1_test_stub_shutdown,
	.tier_name = "tier3-unit-test-stub",
};

const ClusterICOps ClusterICOps_Mux = {
	.send_bytes = tier1_test_stub_send,
	.recv_bytes = tier1_test_stub_recv,
	.peek_sender = tier1_test_stub_peek,
	.tier_init = tier1_test_stub_init,
	.tier_shutdown = tier1_test_stub_shutdown,
	.tier_name = "mux-unit-test-stub",
};

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/error/assert.c */
	abort();
}

void
pg_usleep(long microsec pg_attribute_unused())
{
	/* Stub: real impl in src/port/pgsleep.c.  Tests never reach RPC timeout. */
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	/* Stub: matches PG's cold path wrapper around errstart */
	return false;
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
}

/*
 * pg_crc32c hardware-accelerated functions.  cluster_ic.c uses
 * COMP_CRC32C which expands to one of these depending on platform
 * (USE_ARMV8_CRC32C on macOS arm64, USE_SSE42_CRC32C on Linux x86_64).
 * The unit test never invokes cluster_msg_send (which is what would
 * trigger the CRC compute), but the linker still pulls in the symbol
 * via cluster_ic.o.  Provide both stubs so the binary links on either
 * platform; the runtime-check variants use a function pointer
 * (pg_comp_crc32c) which is not stubbed because PG configure on the
 * platforms that pgrac targets selects the direct-call form.
 *
 * pg_crc32c.h only declares the prototype for whichever variant the
 * current platform uses (#ifdef USE_*_CRC32C); we forward-declare both
 * unconditionally so that defining both is portable across platforms.
 */
extern pg_crc32c pg_comp_crc32c_sse42(pg_crc32c crc, const void *data, size_t len);
extern pg_crc32c pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len);

pg_crc32c
pg_comp_crc32c_sse42(pg_crc32c crc, const void *data pg_attribute_unused(),
					 size_t len pg_attribute_unused())
{
	/* Stub: real impl in src/port/pg_crc32c_sse42.c */
	return crc;
}

pg_crc32c
pg_comp_crc32c_armv8(pg_crc32c crc, const void *data pg_attribute_unused(),
					 size_t len pg_attribute_unused())
{
	/* Stub: real impl in src/port/pg_crc32c_armv8.c */
	return crc;
}

/*
 * Linux x86_64 PG configure picks USE_SSE42_CRC32C_WITH_RUNTIME_CHECK,
 * making COMP_CRC32C expand to a call through the pg_comp_crc32c
 * function pointer.  The real backend initialises this pointer in
 * src/port/pg_crc32c_sse42_choose.c at startup; the unit test never
 * triggers the path, but the linker still needs the variable to
 * resolve.  Initialise to the local sse42 stub above for safety.
 */
pg_crc32c (*pg_comp_crc32c)(pg_crc32c crc, const void *data, size_t len) = pg_comp_crc32c_sse42;

/*
 * Stage 0.26 mock-vtable / mock-SRF dependencies.  Tests below take
 * only addresses; bodies never run, stubs satisfy the linker.
 * MemoryContextSwitchTo is a static inline in palloc.h so no stub is
 * needed for it.
 */
MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;

void *
palloc(Size size pg_attribute_unused())
{
	return NULL;
}

void *
palloc0(Size size pg_attribute_unused())
{
	return NULL;
}

void
pfree(void *pointer pg_attribute_unused())
{}

struct varlena *
pg_detoast_datum_packed(struct varlena *datum)
{
	return datum;
}

void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}

void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}

/*
 * Stage 0.27 injection-framework symbols used by cluster_ic.o
 * (cluster_ic_init + mock_send_bytes expand CLUSTER_INJECTION_POINT).
 * cluster_inject.o is not linked here; stub the symbols.
 */
int cluster_injection_armed_count = 0;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Wire format invariants (compile-time anchors).
 * ============================================================ */

/*
 * spec-2.3 D3: ClusterMsgHeader / cluster_msg_send / cluster_msg_recv /
 * cluster_rpc_call deleted from cluster_ic.{h,c}.  Wire format moved to
 * 36-byte ClusterICEnvelope (see test_cluster_ic_envelope.c U1-U5+U9
 * for ABI lock + verify path coverage; test_cluster_ic_router.c
 * U6-U8 for register / dispatch_table / producer_mask coverage).
 * The msg_header_* tests below were removed accordingly.
 */


/* ============================================================
 * Symbol linkability -- guarantees Stage 2+ subsystems will find
 * the API they expect.
 * ============================================================ */

UT_TEST(test_ic_send_bytes_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_send_bytes);
}

UT_TEST(test_ic_recv_bytes_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_recv_bytes);
}

UT_TEST(test_ic_init_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_init);
}

UT_TEST(test_ic_shutdown_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_shutdown);
}


/* ============================================================
 * Stub vtable contract.
 * ============================================================ */

UT_TEST(test_stub_vtable_tier_name)
{
	UT_ASSERT_NOT_NULL(ClusterICOps_Stub.tier_name);
	UT_ASSERT_STR_EQ(ClusterICOps_Stub.tier_name, "stub");
}

UT_TEST(test_stub_vtable_send_nonnull)
{
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Stub.send_bytes);
}

UT_TEST(test_stub_vtable_recv_nonnull)
{
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Stub.recv_bytes);
}


/* ============================================================
 * spec-2.2 D11 -- Tier1 / HELLO / peer state / mesh role tests.
 *
 * These tests lock interface ABI and pure-helper semantics that
 * spec-2.2 §2.4 / §2.2 / §3.5 frozen.  Behaviour-level tests for
 * tier1 socket I/O live in cluster_tap (075 single-instance + 076
 * 2-node A-lite); cluster_unit only locks the link-time ABI surface.
 *
 * Cross-ref: lessons L45 (on-disk struct byte layout per-field C
 * alignment) -- HELLO is wire-format, treat same as on-disk.
 * ============================================================ */

UT_TEST(test_hello_struct_size_64)
{
	/*
	 * spec-2.2 §2.4 frozen ABI: HELLO must be exactly 64 bytes.  Any
	 * change here breaks cross-version peer handshake; future bumps
	 * MUST go via PGRAC_IC_HELLO_VERSION_V2 (new struct, dispatch on
	 * hello_version field), never resize V1.
	 */
	UT_ASSERT_EQ(sizeof(ClusterICHelloMsg), 64);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_BYTES, 64);
}

UT_TEST(test_hello_field_offsets)
{
	/*
	 * Per-field offset locks (per L45 byte-layout discipline).  Any
	 * compiler that adds padding here breaks the wire ABI.
	 */
	UT_ASSERT_EQ(offsetof(ClusterICHelloMsg, magic), 0);
	UT_ASSERT_EQ(offsetof(ClusterICHelloMsg, hello_version), 4);
	UT_ASSERT_EQ(offsetof(ClusterICHelloMsg, envelope_version), 6);
	UT_ASSERT_EQ(offsetof(ClusterICHelloMsg, source_node_id), 8);
	UT_ASSERT_EQ(offsetof(ClusterICHelloMsg, cluster_name), 12);
	UT_ASSERT_EQ(offsetof(ClusterICHelloMsg, _pad), 36);
}

UT_TEST(test_hello_magic_constant)
{
	/* "HLLO" little-endian = 0x4F4C4C48. */
	UT_ASSERT_EQ(PGRAC_IC_HELLO_MAGIC, (uint32)0x4F4C4C48);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_VERSION_V1, (uint16)1);
	UT_ASSERT_EQ(PGRAC_IC_ENVELOPE_VERSION_V1, (uint16)1);
}

UT_TEST(test_peer_state_enum_size)
{
	/*
	 * Stored as int32 in shmem (per spec-2.2 §2.6
	 * ClusterICPeerStateShmem.state).  Standard C makes enum width
	 * implementation-defined; lock to int via sizeof check.
	 */
	UT_ASSERT_EQ(sizeof(ClusterICPeerState), sizeof(int));
	UT_ASSERT_EQ((int)CLUSTER_IC_PEER_DOWN, 0);
	UT_ASSERT_EQ((int)CLUSTER_IC_PEER_CONNECTING, 1);
	UT_ASSERT_EQ((int)CLUSTER_IC_PEER_CONNECTED, 2);
	UT_ASSERT_EQ((int)CLUSTER_IC_PEER_REJECTED, 3);
}

UT_TEST(test_mesh_role_low_id_active)
{
	/* spec-2.2 §2.2 + §3.5: lower node_id = ACTIVE (initiates connect). */
	UT_ASSERT_EQ(cluster_ic_mesh_role_for_pair(0, 1), CLUSTER_IC_MESH_ACTIVE);
	UT_ASSERT_EQ(cluster_ic_mesh_role_for_pair(0, 127), CLUSTER_IC_MESH_ACTIVE);
	UT_ASSERT_EQ(cluster_ic_mesh_role_for_pair(5, 6), CLUSTER_IC_MESH_ACTIVE);
}

UT_TEST(test_mesh_role_high_id_passive)
{
	/* Higher node_id = PASSIVE (accepts on listener). */
	UT_ASSERT_EQ(cluster_ic_mesh_role_for_pair(1, 0), CLUSTER_IC_MESH_PASSIVE);
	UT_ASSERT_EQ(cluster_ic_mesh_role_for_pair(127, 0), CLUSTER_IC_MESH_PASSIVE);
	UT_ASSERT_EQ(cluster_ic_mesh_role_for_pair(6, 5), CLUSTER_IC_MESH_PASSIVE);
}

UT_TEST(test_tier1_vtable_extern_linkable)
{
	/*
	 * Tier1 vtable is implemented in cluster_ic_tier1.c (spec-2.2 D3
	 * NEW).  This test only verifies the extern symbol resolves at
	 * link time so test_cluster_ic builds cleanly once D3 lands.
	 * Behaviour-level coverage is at TAP layer (075/076).
	 */
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Tier1.send_bytes);
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Tier1.recv_bytes);
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Tier1.peek_sender);
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Tier1.tier_init);
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Tier1.tier_shutdown);
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Tier1.tier_name);
}

UT_TEST(test_rdma_mux_vtable_extern_linkable)
{
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Tier2.send_bytes);
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Tier3.send_bytes);
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Mux.recv_bytes);
	UT_ASSERT_EQ(CLUSTER_IC_RDMA_FALLBACK_AUTO, 0);
	UT_ASSERT_EQ(CLUSTER_IC_RDMA_PROVIDER_VERBS, 1);
	UT_ASSERT_EQ(CLUSTER_IC_RDMA_COMPLETION_BUSYPOLL, 1);
}


/*
 * spec-2.2 D2 (post-codex review) -- HELLO wire roundtrip + reference
 * byte-vector lock.  Verifies that build_hello produces a deterministic
 * byte sequence (no struct-padding leakage, explicit little-endian)
 * and parse_hello round-trips the values cleanly.  Locks the WIRE
 * layout independently of the in-memory ClusterICHelloMsg struct ABI.
 */

UT_TEST(test_hello_wire_roundtrip)
{
	uint8 wire[PGRAC_IC_HELLO_BYTES];
	ClusterICHelloMsg parsed;
	bool ok;

	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 42,
						   "pgrac-test", CLUSTER_IC_PLANE_CONTROL, 0);

	ok = cluster_ic_parse_hello(wire, &parsed);
	UT_ASSERT(ok);
	UT_ASSERT_EQ(parsed.magic, PGRAC_IC_HELLO_MAGIC);
	UT_ASSERT_EQ(parsed.hello_version, PGRAC_IC_HELLO_VERSION_V1);
	UT_ASSERT_EQ(parsed.envelope_version, PGRAC_IC_ENVELOPE_VERSION_V1);
	UT_ASSERT_EQ(parsed.source_node_id, 42);
	UT_ASSERT_EQ(strcmp(parsed.cluster_name, "pgrac-test"), 0);
}

UT_TEST(test_hello_wire_reference_bytes)
{
	uint8 wire[PGRAC_IC_HELLO_BYTES];
	int i;

	/*
	 * Reference byte vector for HELLO V1 with:
	 *   hello_version    = 1
	 *   envelope_version = 1
	 *   source_node_id   = 0x01020304
	 *   cluster_name     = "AB"
	 *
	 * Bytes 0-3:    48 4C 4C 4F            magic "HLLO" little-endian
	 * Bytes 4-5:    01 00                  hello_version = 1 (LE)
	 * Bytes 6-7:    01 00                  envelope_version = 1 (LE)
	 * Bytes 8-11:   04 03 02 01            source_node_id = 0x01020304 (LE)
	 * Bytes 12-13:  41 42                  "AB"
	 * Bytes 14-35:  00..00                 cluster_name NUL pad
	 * Bytes 36-39:  0E 00 00 00            capability bitmap (LE): the
	 *                                      PROTOCOL capabilities advertised
	 *                                      unconditionally by this binary --
	 *                                      D4-6 authority-serve (0x2), D5-2
	 *                                      undo-horizon (0x4) and the
	 *                                      spec-2.2 additive-amendment
	 *                                      CAPS_REPLY_V1 meta bit (0x8)
	 * Bytes 40-63:  00..00                 _pad (must be zero)
	 *
	 * Locking these exact bytes guards against compiler-pad drift,
	 * unintended endian flips, and uninitialized memory leakage.
	 */
	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1,
						   0x01020304, "AB", CLUSTER_IC_PLANE_CONTROL, 0);

	/* magic */
	UT_ASSERT_EQ(wire[0], 0x48);
	UT_ASSERT_EQ(wire[1], 0x4C);
	UT_ASSERT_EQ(wire[2], 0x4C);
	UT_ASSERT_EQ(wire[3], 0x4F);
	/* hello_version = 1 LE */
	UT_ASSERT_EQ(wire[4], 0x01);
	UT_ASSERT_EQ(wire[5], 0x00);
	/* envelope_version = 1 LE */
	UT_ASSERT_EQ(wire[6], 0x01);
	UT_ASSERT_EQ(wire[7], 0x00);
	/* source_node_id = 0x01020304 LE */
	UT_ASSERT_EQ(wire[8], 0x04);
	UT_ASSERT_EQ(wire[9], 0x03);
	UT_ASSERT_EQ(wire[10], 0x02);
	UT_ASSERT_EQ(wire[11], 0x01);
	/* cluster_name "AB" + NUL pad */
	UT_ASSERT_EQ(wire[12], 'A');
	UT_ASSERT_EQ(wire[13], 'B');
	for (i = 14; i < 36; i++)
		UT_ASSERT_EQ(wire[i], 0);
	/* capability bitmap: the unconditional protocol bits -- D4-6
	 * authority-serve (0x2) + spec-5.22e D5-2 undo-horizon (0x4) +
	 * CAPS_REPLY_V1 meta bit (0x8) + GCS-race round-2 F6 completion-proof
	 * (0x10) + round-3 P0-1 xid wrap barrier (0x20) + round-4 P0-1
	 * authority flock (0x40) + ownership-gen ruling② invalidate BUSY
	 * (0x80) (smart-fusion is off in this fixture) */
	UT_ASSERT_EQ(wire[36], 0xFE);
	UT_ASSERT_EQ(wire[37], 0x00);
	UT_ASSERT_EQ(wire[38], 0x00);
	UT_ASSERT_EQ(wire[39], 0x00);
	/* remaining _pad must be all zero: a CONTROL-plane HELLO with
	 * conn_epoch 0 adds nothing past the capability word (spec-7.2 D2
	 * compat pin -- plane byte 40 and epoch bytes 44-51 stay zero). */
	for (i = 40; i < PGRAC_IC_HELLO_BYTES; i++)
		UT_ASSERT_EQ(wire[i], 0);
}

/*
 * spec-7.2 D2 — DATA-plane HELLO reference bytes:  plane byte at offset
 * 40, conn_epoch LE at 44-51, spec-7.3 worker bytes (41/42) still zero,
 * tail 52-63 still zero.  Plus accessor roundtrip.
 */
UT_TEST(test_hello_wire_data_plane_bytes)
{
	uint8 wire[PGRAC_IC_HELLO_BYTES];
	ClusterICHelloMsg parsed;
	int i;

	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 3, "AB",
						   CLUSTER_IC_PLANE_DATA, UINT64CONST(0x1122334455667788));

	UT_ASSERT_EQ(wire[PGRAC_IC_HELLO_PLANE_OFFSET], 1);
	UT_ASSERT_EQ(wire[PGRAC_IC_HELLO_WORKER_ID_OFFSET], 0);
	UT_ASSERT_EQ(wire[PGRAC_IC_HELLO_N_WORKERS_OFFSET], 0);
	UT_ASSERT_EQ(wire[43], 0);
	/* conn_epoch = 0x1122334455667788 little-endian */
	UT_ASSERT_EQ(wire[44], 0x88);
	UT_ASSERT_EQ(wire[45], 0x77);
	UT_ASSERT_EQ(wire[46], 0x66);
	UT_ASSERT_EQ(wire[47], 0x55);
	UT_ASSERT_EQ(wire[48], 0x44);
	UT_ASSERT_EQ(wire[49], 0x33);
	UT_ASSERT_EQ(wire[50], 0x22);
	UT_ASSERT_EQ(wire[51], 0x11);
	for (i = 52; i < PGRAC_IC_HELLO_BYTES; i++)
		UT_ASSERT_EQ(wire[i], 0);

	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT_EQ(cluster_ic_hello_plane(&parsed), CLUSTER_IC_PLANE_DATA);
	UT_ASSERT(cluster_ic_hello_conn_epoch(&parsed) == UINT64CONST(0x1122334455667788));
}

/*
 * spec-7.3 D3 — the worker_id / n_workers HELLO fields (offsets 41/42) are
 * written by cluster_ic_hello_set_worker_fields on the DATA-plane send path
 * and read by the accessors.  build_hello alone leaves them zero, so a plain
 * DATA HELLO stays byte-identical to spec-7.2 (compat pin).
 */
UT_TEST(test_hello_worker_fields_roundtrip)
{
	uint8 wire[PGRAC_IC_HELLO_BYTES];
	ClusterICHelloMsg parsed;

	/* build_hello alone => worker fields zero (spec-7.2 compat). */
	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 3, "AB",
						   CLUSTER_IC_PLANE_DATA, UINT64CONST(0x99));
	UT_ASSERT_EQ(wire[PGRAC_IC_HELLO_WORKER_ID_OFFSET], 0);
	UT_ASSERT_EQ(wire[PGRAC_IC_HELLO_N_WORKERS_OFFSET], 0);
	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT_EQ(cluster_ic_hello_worker_id(&parsed), 0);
	UT_ASSERT_EQ(cluster_ic_hello_n_workers(&parsed), 0);

	/* set worker fields => offsets 41/42 carry them; plane/pad/epoch intact. */
	cluster_ic_hello_set_worker_fields(wire, 3, 5);
	UT_ASSERT_EQ(wire[PGRAC_IC_HELLO_WORKER_ID_OFFSET], 3);
	UT_ASSERT_EQ(wire[PGRAC_IC_HELLO_N_WORKERS_OFFSET], 5);
	UT_ASSERT_EQ(wire[PGRAC_IC_HELLO_PLANE_OFFSET], (uint8)CLUSTER_IC_PLANE_DATA);
	UT_ASSERT_EQ(wire[43], 0);
	UT_ASSERT_EQ(wire[PGRAC_IC_HELLO_CONN_EPOCH_OFFSET], 0x99);

	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT_EQ(cluster_ic_hello_worker_id(&parsed), 3);
	UT_ASSERT_EQ(cluster_ic_hello_n_workers(&parsed), 5);
}

UT_TEST(test_hello_smart_fusion_capability_gate)
{
	uint8 wire[PGRAC_IC_HELLO_BYTES];
	ClusterICHelloMsg parsed;

	/* the spec-5.22d D4-6 authority-serve + spec-5.22e undo-horizon +
	 * CAPS_REPLY_V1 meta bits are unconditional, so they are the
	 * capability-word BASELINE in every row below; only the smart-fusion
	 * bit is GUC/tier-gated */
	cluster_smart_fusion = false;
	cluster_interconnect_tier = CLUSTER_IC_TIER_3;
	cluster_smart_fusion_tier_min = CLUSTER_IC_TIER_3;
	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 1,
						   "sf-off", CLUSTER_IC_PLANE_CONTROL, 0);
	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT_EQ(cluster_ic_hello_capabilities(&parsed),
				 PGRAC_IC_HELLO_CAP_UNDO_AUTHORITY_SERVE_V1 | PGRAC_IC_HELLO_CAP_UNDO_HORIZON_V1
					 | PGRAC_IC_HELLO_CAP_CAPS_REPLY_V1 | PGRAC_IC_HELLO_CAP_GCS_DONE_V1
					 | PGRAC_IC_HELLO_CAP_XID_NATIVE_DISABLE_V1
					 | PGRAC_IC_HELLO_CAP_XID_AUTHORITY_FLOCK_V2
					 | PGRAC_IC_HELLO_CAP_GCS_INVAL_BUSY_V1);

	cluster_smart_fusion = true;
	cluster_interconnect_tier = CLUSTER_IC_TIER_2;
	cluster_smart_fusion_tier_min = CLUSTER_IC_TIER_3;
	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 1,
						   "sf-tier-mismatch", CLUSTER_IC_PLANE_CONTROL, 0);
	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT_EQ(cluster_ic_hello_capabilities(&parsed),
				 PGRAC_IC_HELLO_CAP_UNDO_AUTHORITY_SERVE_V1 | PGRAC_IC_HELLO_CAP_UNDO_HORIZON_V1
					 | PGRAC_IC_HELLO_CAP_CAPS_REPLY_V1 | PGRAC_IC_HELLO_CAP_GCS_DONE_V1
					 | PGRAC_IC_HELLO_CAP_XID_NATIVE_DISABLE_V1
					 | PGRAC_IC_HELLO_CAP_XID_AUTHORITY_FLOCK_V2
					 | PGRAC_IC_HELLO_CAP_GCS_INVAL_BUSY_V1);

	cluster_smart_fusion = true;
	cluster_interconnect_tier = CLUSTER_IC_TIER_3;
	cluster_smart_fusion_tier_min = CLUSTER_IC_TIER_3;
	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 1,
						   "sf-tier-match", CLUSTER_IC_PLANE_CONTROL, 0);
	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT_EQ(cluster_ic_hello_capabilities(&parsed),
				 PGRAC_IC_HELLO_CAP_SMART_FUSION_REPLY_V2
					 | PGRAC_IC_HELLO_CAP_UNDO_AUTHORITY_SERVE_V1
					 | PGRAC_IC_HELLO_CAP_UNDO_HORIZON_V1 | PGRAC_IC_HELLO_CAP_CAPS_REPLY_V1
					 | PGRAC_IC_HELLO_CAP_GCS_DONE_V1 | PGRAC_IC_HELLO_CAP_XID_NATIVE_DISABLE_V1
					 | PGRAC_IC_HELLO_CAP_XID_AUTHORITY_FLOCK_V2
					 | PGRAC_IC_HELLO_CAP_GCS_INVAL_BUSY_V1);

	cluster_smart_fusion = false;
	cluster_interconnect_tier = CLUSTER_IC_TIER_STUB;
	cluster_smart_fusion_tier_min = CLUSTER_IC_TIER_3;
}

/*
 * spec-2.2 additive amendment (spec-5.22e D5 prereq, B1): the CAPS_REPLY_V1
 * meta bit ("I can receive PEER_CAPS_REPLY") is advertised unconditionally,
 * UNLESS the test-only old-binary simulation GUC suppresses it.  Old-binary
 * compat rests on this bit: an acceptor only sends PEER_CAPS_REPLY to a
 * dialer whose HELLO carried the bit, so a peer without it (an actual old
 * binary, or a node simulating one) is never sent a frame it cannot parse.
 */
UT_TEST(test_hello_caps_reply_meta_gate)
{
	uint8 wire[PGRAC_IC_HELLO_BYTES];
	ClusterICHelloMsg parsed;

	/* wire id + bit value are frozen protocol constants */
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_CAPS_REPLY_V1, (uint32)0x00000008U);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_PEER_CAPS_REPLY, 37);

	/* default: meta bit advertised */
	cluster_ic_suppress_caps_reply = false;
	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 1,
						   "caps-on", CLUSTER_IC_PLANE_CONTROL, 0);
	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT((cluster_ic_hello_capabilities(&parsed) & PGRAC_IC_HELLO_CAP_CAPS_REPLY_V1) != 0);

	/* suppressed (old-binary simulation): meta bit absent, the other
	 * protocol bits untouched */
	cluster_ic_suppress_caps_reply = true;
	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 1,
						   "caps-off", CLUSTER_IC_PLANE_CONTROL, 0);
	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT((cluster_ic_hello_capabilities(&parsed) & PGRAC_IC_HELLO_CAP_CAPS_REPLY_V1) == 0);
	UT_ASSERT((cluster_ic_hello_capabilities(&parsed) & PGRAC_IC_HELLO_CAP_UNDO_HORIZON_V1) != 0);

	cluster_ic_suppress_caps_reply = false;
}

/*
 * GCS-race round-2 RC-F mixed-version leg + round-3 P0-1: the GCS_DONE_V1
 * bit yields to its test-only old-binary simulation GUC (same discipline as
 * the CAPS_REPLY_V1 gate above), while the wrap-barrier bit stays
 * unconditional; both wire ids are frozen protocol constants.
 */
UT_TEST(test_hello_gcs_done_and_wrap_barrier_gates)
{
	uint8 wire[PGRAC_IC_HELLO_BYTES];
	ClusterICHelloMsg parsed;

	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_GCS_DONE_V1, (uint32)0x00000010U);
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_XID_NATIVE_DISABLE_V1, (uint32)0x00000020U);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_DONE, 38);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_XID_NATIVE_DISABLE, 39);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_XID_NATIVE_DISABLE_ACK, 40);

	/* default: both bits advertised */
	cluster_ic_suppress_gcs_done_cap = false;
	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 1,
						   "done-on", CLUSTER_IC_PLANE_CONTROL, 0);
	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT((cluster_ic_hello_capabilities(&parsed) & PGRAC_IC_HELLO_CAP_GCS_DONE_V1) != 0);
	UT_ASSERT((cluster_ic_hello_capabilities(&parsed) & PGRAC_IC_HELLO_CAP_XID_NATIVE_DISABLE_V1)
			  != 0);

	/* suppressed (old-binary simulation): DONE bit absent, the other
	 * protocol bits untouched */
	cluster_ic_suppress_gcs_done_cap = true;
	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 1,
						   "done-off", CLUSTER_IC_PLANE_CONTROL, 0);
	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	UT_ASSERT((cluster_ic_hello_capabilities(&parsed) & PGRAC_IC_HELLO_CAP_GCS_DONE_V1) == 0);
	UT_ASSERT((cluster_ic_hello_capabilities(&parsed) & PGRAC_IC_HELLO_CAP_XID_NATIVE_DISABLE_V1)
			  != 0);
	UT_ASSERT((cluster_ic_hello_capabilities(&parsed) & PGRAC_IC_HELLO_CAP_CAPS_REPLY_V1) != 0);

	cluster_ic_suppress_gcs_done_cap = false;
}

UT_TEST(test_hello_parse_rejects_bad_magic)
{
	uint8 wire[PGRAC_IC_HELLO_BYTES];
	ClusterICHelloMsg parsed;

	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 1, "x",
						   CLUSTER_IC_PLANE_CONTROL, 0);
	/* Corrupt magic */
	wire[0] = 0xDE;
	wire[1] = 0xAD;
	wire[2] = 0xBE;
	wire[3] = 0xEF;

	UT_ASSERT(!cluster_ic_parse_hello(wire, &parsed));
}

UT_TEST(test_hello_build_truncates_long_name)
{
	uint8 wire[PGRAC_IC_HELLO_BYTES];
	ClusterICHelloMsg parsed;
	const char *long_name = "this-cluster-name-is-way-longer-than-the-fixed-cap-on-purpose";

	cluster_ic_build_hello(wire, PGRAC_IC_HELLO_VERSION_V1, PGRAC_IC_ENVELOPE_VERSION_V1, 7,
						   long_name, CLUSTER_IC_PLANE_CONTROL, 0);

	UT_ASSERT(cluster_ic_parse_hello(wire, &parsed));
	/*
	 * cluster_name is 24 bytes, last byte forced NUL by parser; so
	 * parsed.cluster_name should be the first 23 chars of long_name
	 * + NUL.
	 */
	UT_ASSERT_EQ(strncmp(parsed.cluster_name, long_name, 23), 0);
	UT_ASSERT_EQ(parsed.cluster_name[23], '\0');
}


int
main(void)
{
	UT_PLAN(23); /* spec-2.3 D3: 6 ClusterMsgHeader/msg_send/recv tests deleted */
	UT_RUN(test_ic_send_bytes_linkable);
	UT_RUN(test_ic_recv_bytes_linkable);
	UT_RUN(test_ic_init_linkable);
	UT_RUN(test_ic_shutdown_linkable);
	UT_RUN(test_stub_vtable_tier_name);
	UT_RUN(test_stub_vtable_send_nonnull);
	UT_RUN(test_stub_vtable_recv_nonnull);
	/* spec-2.2 D11 -- new tests */
	UT_RUN(test_hello_struct_size_64);
	UT_RUN(test_hello_field_offsets);
	UT_RUN(test_hello_magic_constant);
	UT_RUN(test_peer_state_enum_size);
	UT_RUN(test_mesh_role_low_id_active);
	UT_RUN(test_mesh_role_high_id_passive);
	UT_RUN(test_tier1_vtable_extern_linkable);
	UT_RUN(test_rdma_mux_vtable_extern_linkable);
	/* HELLO wire encode/decode + reference bytes (post-codex review) */
	UT_RUN(test_hello_wire_roundtrip);
	UT_RUN(test_hello_wire_reference_bytes);
	UT_RUN(test_hello_wire_data_plane_bytes);	/* spec-7.2 D2 */
	UT_RUN(test_hello_worker_fields_roundtrip); /* spec-7.3 D3 */
	UT_RUN(test_hello_smart_fusion_capability_gate);
	UT_RUN(test_hello_caps_reply_meta_gate);
	UT_RUN(test_hello_gcs_done_and_wrap_barrier_gates);
	UT_RUN(test_hello_parse_rejects_bad_magic);
	UT_RUN(test_hello_build_truncates_long_name);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
