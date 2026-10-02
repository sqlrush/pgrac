/* Actual redeclare codec/retry owner; membership and PCM application are
 * explicit boundaries. Actual PCM and buffer census are tested separately.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/xlog.h"
#include "cluster/cluster_block_redeclare.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "utils/timestamp.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
BackendType MyBackendType = B_LMON;
int cluster_node_id;
bool cluster_enabled = true, cluster_shared_config = true;
static uint64 epoch = 10, boots[2] = { 31, 41 }, serial, census_hash = 133;
static bool quorum = true, prebump, apply_ok = true, writer_ready = true, restart_ready = true;
static int frozen = 1, master = 1;
static uint32 sends, applies;
static TimestampTz now = 1000000;
static uint8 sent[CLUSTER_BLOCK_REDECLARE_BYTES];
static ClusterICSendResult send_result = CLUSTER_IC_SEND_DONE;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition;
	(void)file;
	(void)line;
	abort();
}

uint64
GetSystemIdentifier(void)
{
	return 27;
}
uint64
cluster_epoch_get_current(void)
{
	return epoch;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return boots[cluster_node_id];
}
bool
cluster_qvotec_in_quorum(void)
{
	return quorum;
}
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	return prebump;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return now;
}
int
cluster_gcs_lookup_master(BufferTag tag)
{
	return master;
}
int
cluster_grd_block_redeclare_state_v1(BufferTag tag, uint64 e, uint64 *hash)
{
	*hash = census_hash;
	return e == epoch ? frozen : -1;
}
ClusterMembershipState
cluster_membership_get_state(int32 n)
{
	return n >= 0 && n < 2 ? CLUSTER_MEMBER_MEMBER : CLUSTER_MEMBER_ABSENT;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 n)
{
	return n >= 0 && n < 2 ? boots[n] : 0;
}
bool
pg_strong_random(void *p, size_t size)
{
	UT_ASSERT_EQ(size, sizeof(serial));
	++serial;
	memcpy(p, &serial, size);
	return true;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	memset(out, 0, sizeof(*out));
	out->claim.identity.system_identifier = 27;
	out->claim.identity.origin_node_id = cluster_node_id;
	out->claim.identity.origin_owner_incarnation = boots[cluster_node_id];
	memset(out->claim.identity.storage_uuid, 0x37, 16);
	return writer_ready;
}
bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	(void)cluster_wal_thread_current_v2_ref(out);
	out->claim.identity.origin_owner_incarnation--;
	return restart_ready;
}
bool
cluster_gcs_block_master_rebuild_from_redeclare(BufferTag tag, uint8 mode, XLogRecPtr lsn, SCN scn,
												int32 source, uint64 e)
{
	UT_ASSERT_EQ(cluster_node_id, master);
	UT_ASSERT_EQ(e, epoch);
	applies++;
	return apply_ok;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *p, uint32 len)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_REDECLARE);
	UT_ASSERT_EQ(len, sizeof(sent));
	UT_ASSERT_NE(dest, cluster_node_id);
	memcpy(sent, p, len);
	sends++;
	return send_result;
}

static BufferTag tag
	= { .spcOid = 1663, .dbOid = 5, .relNumber = 901, .forkNum = MAIN_FORKNUM, .blockNum = 8 };

static void
reset(void)
{
	epoch++;
	cluster_node_id = 0;
	quorum = apply_ok = true;
	writer_ready = restart_ready = true;
	prebump = false;
	frozen = master = 1;
	sends = applies = 0;
	send_result = CLUSTER_IC_SEND_DONE;
	now += 1000000;
}

static void
deliver(uint32 source, uint32 destination, const uint8 *bytes)
{
	ClusterICEnvelope env = { .msg_type = PGRAC_IC_MSG_GCS_BLOCK_REDECLARE,
							  .source_node_id = source,
							  .dest_node_id = destination,
							  .epoch = epoch,
							  .payload_length = sizeof(sent) };
	cluster_node_id = destination;
	cluster_block_redeclare_ingress_v1(&env, bytes);
}

UT_TEST(codec_checks_full_length_and_reserved_bytes)
{
	ClusterBlockRedeclareV1 value = { .epoch = 1,
									  .nonce = 2,
									  .source_boot = 3,
									  .master_boot = 4,
									  .system_identifier = 27,
									  .source_node = 0,
									  .master_node = 31,
									  .page_lsn = 80,
									  .page_scn = 90,
									  .kind = CLUSTER_BLOCK_REDECLARE_REQUEST,
									  .mode = PCM_STATE_N,
									  .census_hash = 133 },
							out;
	uint8 bytes[CLUSTER_BLOCK_REDECLARE_BYTES];
	value.tag = tag;
	memset(value.storage_uuid, 7, 16);
	UT_ASSERT(cluster_block_redeclare_encode_v1(&value, bytes));
	UT_ASSERT(cluster_block_redeclare_decode_v1(bytes, sizeof(bytes), &out));
	UT_ASSERT_EQ(out.master_node, 31);
	UT_ASSERT_EQ(out.mode, PCM_STATE_N);
	UT_ASSERT(!cluster_block_redeclare_decode_v1(bytes, sizeof(bytes) - 1, &out));
	bytes[103] = 1;
	UT_ASSERT(!cluster_block_redeclare_decode_v1(bytes, sizeof(bytes), &out));
	value.master_node = 32;
	UT_ASSERT(!cluster_block_redeclare_encode_v1(&value, bytes));
}

UT_TEST(scan_waits_for_actual_master_application_ack)
{
	uint8 request[sizeof(sent)], ack[sizeof(sent)];
	reset();
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_N, 80, 90, epoch, master));
	UT_ASSERT_EQ(sends, 1);
	memcpy(request, sent, sizeof(sent));
	apply_ok = false;
	deliver(0, 1, request);
	UT_ASSERT_EQ(applies, 1);
	UT_ASSERT_EQ(sends, 1); /* Refusal is not an acknowledgement. */
	cluster_node_id = 0;
	now += 1000000;
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_N, 80, 90, epoch, master));
	UT_ASSERT_EQ(memcmp(request, sent, sizeof(sent)), 0);
	apply_ok = true;
	deliver(0, 1, request);
	memcpy(ack, sent, sizeof(sent));
	deliver(1, 0, ack);
	UT_ASSERT(cluster_block_redeclare_poll_v1(tag, PCM_STATE_N, 80, 90, epoch, master));
	UT_ASSERT_EQ(applies, 2);
}

UT_TEST(fresh_master_accepts_namespace_before_writer_install)
{
	uint8 request[sizeof(sent)], ack[sizeof(sent)];
	reset();
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_N, 80, 90, epoch, master));
	memcpy(request, sent, sizeof(sent));
	writer_ready = false;
	restart_ready = false;
	deliver(0, 1, request);
	UT_ASSERT_EQ(applies, 0);
	restart_ready = true;
	deliver(0, 1, request);
	UT_ASSERT_EQ(applies, 1);
	UT_ASSERT_EQ(sends, 2);
	memcpy(ack, sent, sizeof(sent));
	deliver(1, 0, ack);
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_N, 80, 90, epoch, master));
	writer_ready = true;
	deliver(1, 0, ack);
	UT_ASSERT(cluster_block_redeclare_poll_v1(tag, PCM_STATE_N, 80, 90, epoch, master));
}

UT_TEST(late_ack_cannot_cover_changed_buffer_or_incarnation)
{
	uint8 request[sizeof(sent)], ack[sizeof(sent)];
	reset();
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_X, 80, 90, epoch, master));
	memcpy(request, sent, sizeof(sent));
	deliver(0, 1, request);
	memcpy(ack, sent, sizeof(sent));
	cluster_node_id = 0;
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_X, 81, 91, epoch, master));
	deliver(1, 0, ack);
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_X, 81, 91, epoch, master));
	boots[1]++;
	deliver(1, 0, ack);
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_X, 81, 91, epoch, master));
	census_hash++;
	deliver(1, 0, ack);
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_X, 81, 91, epoch, master));
	UT_ASSERT_EQ(applies, 1);
}

UT_TEST(foreign_or_unfrozen_request_does_not_mutate_master)
{
	uint8 request[sizeof(sent)];
	reset();
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_S, 80, 90, epoch, master));
	memcpy(request, sent, sizeof(sent));
	deliver(1, 1, request);
	UT_ASSERT_EQ(applies, 0);
	frozen = 0;
	deliver(0, 1, request);
	UT_ASSERT_EQ(applies, 0);
	frozen = 1;
	boots[0]++;
	deliver(0, 1, request);
	UT_ASSERT_EQ(applies, 0);
}

UT_TEST(self_master_is_applied_and_unknown_cut_never_finishes)
{
	reset();
	master = 0;
	apply_ok = false;
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_X, 0, 0, epoch, master));
	UT_ASSERT_EQ(applies, 1);
	apply_ok = true;
	UT_ASSERT(cluster_block_redeclare_poll_v1(tag, PCM_STATE_X, 0, 0, epoch, master));
	UT_ASSERT_EQ(applies, 2);
	UT_ASSERT_EQ(sends, 0);
	frozen = -1;
	UT_ASSERT(!cluster_block_redeclare_poll_v1(tag, PCM_STATE_X, 0, 0, epoch, master));
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(codec_checks_full_length_and_reserved_bytes);
	UT_RUN(scan_waits_for_actual_master_application_ack);
	UT_RUN(fresh_master_accepts_namespace_before_writer_install);
	UT_RUN(late_ack_cannot_cover_changed_buffer_or_incarnation);
	UT_RUN(foreign_or_unfrozen_request_does_not_mutate_master);
	UT_RUN(self_master_is_applied_and_unknown_cut_never_finishes);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
