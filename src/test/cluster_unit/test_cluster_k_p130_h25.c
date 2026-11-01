/* Full bounded first-fill receipts through the original LMS tick and offers.
 * Native publication and IC admission are explicit host fixture providers.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "cluster/cluster_gcs_block.h"
#include "utils/timestamp.h"

static void h25_record_ack(const GcsBlockInvalidateAckPayload *ack);
static void h25_record_done(const GcsBlockDonePayload *done);
static void h25_record_scan(int worker, int workers, uint64 begin, uint64 end,
	uint64 cap, int64 sampled_at);
#ifndef K_P130_H25_OWNER_INCLUDE
#define K_P130_H25_OWNER_INCLUDE "test_cluster_first_fill_retire.inc"
#endif
#include "test_cluster_k_p130_h25_fixture.inc"

#define H25_CAP (lengthof(retire_blocks) * MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND)
static ClusterPcmReadSource h25_sources[H25_CAP];
static GcsBlockInvalidateAckPayload h25_acks[H25_CAP];
static GcsBlockDonePayload h25_done[H25_CAP];
static unsigned h25_offers[H25_CAP];
static uint64 h25_begin, h25_end, h25_scan_cap;
static unsigned h25_scan_calls, h25_read_work_calls;

/* This test has no master pending-S work. Keep that independent branch
 * explicit while executing the original worker's receipt call unchanged. */
static bool
h25_read_work(uint32 *cursor pg_attribute_unused(), int worker, int workers,
	TimestampTz now, uint64 epoch, ClusterPcmReadGrant *out pg_attribute_unused())
{
	UT_ASSERT_EQ(worker, 0); UT_ASSERT_EQ(workers, 1);
	UT_ASSERT_EQ(now, retire_now); UT_ASSERT_EQ(epoch, retire_epoch);
	h25_read_work_calls++;
	return false;
}
#define cluster_ic_tier1_my_data_channel() 0
#define cluster_pcm_lock_read_grant_work h25_read_work
static bool h25_unexpected(void) { UT_ASSERT(false); return false; }
#define gcs_first_fill_retired_notice(key) h25_unexpected()
#define cluster_pcm_lock_read_grant_retire_notice_admitted(key) h25_unexpected()
#define cluster_block_io_trace_pending_s(...) UT_ASSERT(false)
#define gcs_block_compute_invalidate_checksum(inv) 0
#ifndef K_P130_H25_TICK_INCLUDE
#define K_P130_H25_TICK_INCLUDE "test_cluster_k_p130_h25_tick.inc"
#endif
#include K_P130_H25_TICK_INCLUDE

static int
h25_index(const ClusterPcmReadGrantKey *key)
{
	uint64 seq = key->request_id & GCS_REQID_REQUESTER_SEQ_MASK;
	int index;
	UT_ASSERT(key->backend_id > 0 && key->backend_id <= retire_max_backends);
	UT_ASSERT(seq > 0 && seq <= MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND);
	if (key->backend_id <= 0 || key->backend_id > retire_max_backends
		|| seq == 0 || seq > MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND) return -1;
	index = (key->backend_id - 1) * MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND + seq - 1;
	UT_ASSERT_EQ(key->requester_node, cluster_node_id);
	UT_ASSERT_EQ(key->request_id, h25_sources[index].request_id);
	UT_ASSERT_EQ(key->epoch, h25_sources[index].request_epoch);
	UT_ASSERT(BufferTagsEqual(&key->tag, &h25_sources[index].tag));
	return index;
}

static void
h25_record_ack(const GcsBlockInvalidateAckPayload *ack)
{
	ClusterPcmReadGrantKey key = {0};
	int index;
	UT_ASSERT(GcsBlockInvalidateAckPayloadPendingSKey(ack, &key));
	index = h25_index(&key); if (index < 0) return;
	UT_ASSERT_EQ(ack->checksum, gcs_block_compute_invalidate_ack_checksum(ack));
	if (h25_offers[index]) UT_ASSERT(memcmp(ack, &h25_acks[index], sizeof(*ack)) == 0);
	else h25_acks[index] = *ack;
	h25_offers[index]++;
}

static void
h25_record_done(const GcsBlockDonePayload *done)
{
	ClusterPcmReadGrantKey key = {0};
	int index;
	UT_ASSERT(GcsBlockDonePayloadPendingSKey(done, &key));
	index = h25_index(&key); if (index < 0) return;
	UT_ASSERT_EQ(done->reserved_0[0], GCS_BLOCK_DONE_PENDING_S_CONFIRM);
	if (h25_offers[index]) UT_ASSERT(memcmp(done, &h25_done[index], sizeof(*done)) == 0);
	else h25_done[index] = *done;
	h25_offers[index]++;
}

static void
h25_record_scan(int worker, int workers, uint64 begin, uint64 end, uint64 cap, int64 sampled_at)
{
	UT_ASSERT_EQ(worker, 0); UT_ASSERT_EQ(workers, 1);
	UT_ASSERT_EQ(sampled_at, retire_now); UT_ASSERT_EQ(cap, H25_CAP);
	UT_ASSERT_EQ(end - begin, cap);
	h25_begin = begin; h25_end = end; h25_scan_cap = cap; h25_scan_calls++;
}

static unsigned
h25_census(bool installed, unsigned expected, bool handed)
{
	unsigned count = 0;
	for (unsigned flat = 0; flat < H25_CAP; flat++) {
		ClusterGcsBlockOutstandingSlot *slot = &retire_blocks[flat / MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND].slots[flat % MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND];
		if (!slot->in_use) continue;
		count++;
		UT_ASSERT(slot->first_fill.armed && slot->first_fill.delivery_closed);
		UT_ASSERT_EQ(slot->first_fill.confirm_pending, installed);
		UT_ASSERT_EQ(slot->first_fill.quiet, !installed);
		UT_ASSERT_EQ(slot->first_fill.ack_handed, handed);
		UT_ASSERT_EQ(slot->request_id, h25_sources[flat].request_id);
		UT_ASSERT_EQ(slot->first_fill.source.reservation_token, h25_sources[flat].reservation_token);
		UT_ASSERT_EQ(slot->first_fill.source.base_generation, h25_sources[flat].base_generation);
	}
	UT_ASSERT_EQ(count, expected); UT_ASSERT_EQ(retire_lock_depth, 0);
	return count;
}

static void
h25_fill(BufferDesc *buf, ClusterPcmOwnEntry *entry, bool installed)
{
	retire_reset(buf, entry, true);
	retire_max_backends = lengthof(retire_blocks);
	UT_ASSERT_EQ(H25_CAP, 976);
	memset(h25_offers, 0, sizeof(h25_offers));
	memset(h25_acks, 0, sizeof(h25_acks)); memset(h25_done, 0, sizeof(h25_done));
	h25_scan_calls = h25_read_work_calls = 0;
	for (int backend = 1; backend <= retire_max_backends; backend++) {
		retire_backend = backend; retire_blocks[backend - 1].next_request_id = 1;
		for (int index = 0; index < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; index++) {
			unsigned flat = (backend - 1) * MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND + index;
			ClusterPcmReadSource source;
			buf->pcm_state = PCM_STATE_N;
			pg_atomic_write_u32(&buf->state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
			source = retire_arm(buf, true);
			if (installed) {
				retire_prepare_install(buf, entry, &source);
				cluster_gcs_block_read_source_complete(buf, &source, true);
			} else retire_abort(buf, &source);
			h25_sources[flat] = source;
			if (!installed) {
				GcsBlockInvalidatePayload inv = retire_invalidate(&source);
				UT_ASSERT(gcs_block_invalidate_execute(&inv));
			}
		}
	}
	UT_ASSERT_EQ(retire_sleeps, 0);
	for (int n = 0; n < GCS_READ_SOURCE_LEDGER_MAX; n++) UT_ASSERT(!gcs_read_source_attempts[n].used);
	(void)h25_census(installed, H25_CAP, false);
	/* A successor native descriptor cannot discharge old shared receipts. */
	buf->pcm_state = PCM_STATE_N;
	pg_atomic_write_u64(&entry->generation, 10001);
	pg_atomic_write_u32(&entry->flags, PCM_OWN_FLAG_GRANT_PENDING);
	cluster_trace_gcs_capacity = cluster_trace_tx_identity = true;
}

static void
h25_tick(void)
{
	unsigned scans = h25_scan_calls, work = h25_read_work_calls;
	retire_now += 10000;
	cluster_gcs_block_pending_s_tick();
	UT_ASSERT_EQ(h25_scan_calls, scans + 1);
	UT_ASSERT_EQ(h25_read_work_calls, work + 1);
	UT_ASSERT_EQ(h25_scan_cap, H25_CAP);
	UT_ASSERT_EQ(h25_end - h25_begin, H25_CAP);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 10001);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(retire_tag_reads, 0);
}

static void
h25_consume(unsigned flat)
{
	GcsBlockDonePayload done = retire_notice(&h25_sources[flat]);
	done.requester_backend_id = flat / MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND + 1;
	UT_ASSERT(retire_consume(&done));
}

static void
h25_run(bool installed, ClusterICSendResult result, bool sparse)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	unsigned expected = sparse ? 1 : H25_CAP;
	unsigned initial = installed ? 0 : 1;
	h25_fill(&buf, &entry, installed);
	if (sparse) {
		retire_confirm_admit = true; retire_result = CLUSTER_IC_SEND_DONE;
		for (unsigned n = 0; n + 1 < H25_CAP; n++) {
			if (installed) gcs_first_fill_confirm_offer(n / MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND, n % MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND);
			else { gcs_first_fill_receipt_offer(n / MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND, n % MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND); h25_consume(n); }
		}
		retire_confirm_admit = false; retire_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	}
	for (unsigned attempt = 1; attempt <= 3; attempt++) {
		h25_tick(); (void)h25_census(installed, expected, false);
		for (unsigned n = sparse ? H25_CAP - 1 : 0; n < H25_CAP; n++)
			UT_ASSERT_EQ(h25_offers[n], initial + attempt);
	}
	retire_confirm_admit = true; retire_result = result;
	h25_tick();
	(void)h25_census(installed, installed ? 0 : expected, !installed);
	UT_ASSERT_EQ(h25_offers[H25_CAP - 1], initial + 4);
	/* Admitted quiet transport is still not authenticated M consumption. */
	if (!installed) {
		GcsBlockDonePayload bad = retire_notice(&h25_sources[H25_CAP - 1]);
		bad.requester_backend_id = retire_max_backends; bad.epoch++;
		UT_ASSERT(!retire_consume(&bad));
		retire_peer_session++;
		bad = retire_notice(&h25_sources[H25_CAP - 1]); bad.requester_backend_id = retire_max_backends;
		UT_ASSERT(!retire_consume(&bad)); retire_peer_session--;
		h25_tick();
		UT_ASSERT_EQ(h25_offers[H25_CAP - 1], initial + 4);
		(void)h25_census(false, expected, true);
		for (unsigned n = H25_CAP; n > (sparse ? H25_CAP - 1 : 0); n--) h25_consume(n - 1);
		(void)h25_census(false, 0, false);
	}
	UT_ASSERT_EQ(retire_sleeps, 0); UT_ASSERT(retire_wakes > 0);
	printf("# H25 installed=%d sparse=%d result=%d cap=%u begin=%llu end=%llu visits=%llu tail=975 offers=%u retained=0 gen=%llu flags=%u tag_reads=%u IC_provider=1\n",
		installed, sparse, result, (unsigned)H25_CAP, (unsigned long long)h25_begin, (unsigned long long)h25_end,
		(unsigned long long)(h25_end - h25_begin),
		h25_offers[H25_CAP - 1], (unsigned long long)cluster_pcm_own_gen_get(0),
		cluster_pcm_own_flags_get(0), retire_tag_reads);
}
UT_TEST(h25_full_installed_tail_refuses_then_hands_off) { h25_run(true, CLUSTER_IC_SEND_DONE, false); }
UT_TEST(h25_sparse_installed_last_slot_reached_in_one_tick) { h25_run(true, CLUSTER_IC_SEND_DONE, true); }
UT_TEST(h25_full_quiet_tail_DONE_waits_for_exact_consumption) { h25_run(false, CLUSTER_IC_SEND_DONE, false); }
UT_TEST(h25_full_quiet_tail_WOULD_BLOCK_waits_for_exact_consumption) { h25_run(false, CLUSTER_IC_SEND_WOULD_BLOCK, false); }
UT_TEST(h25_sparse_quiet_last_slot_DONE_reached_in_one_tick) { h25_run(false, CLUSTER_IC_SEND_DONE, true); }
UT_TEST(h25_sparse_quiet_last_slot_WOULD_BLOCK_reached_in_one_tick) { h25_run(false, CLUSTER_IC_SEND_WOULD_BLOCK, true); }

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H25 assertion-OFF requires an assertion-disabled configuration"
#endif
int main(void);
int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H25 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H25 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(6);
	UT_RUN(h25_full_installed_tail_refuses_then_hands_off);
	UT_RUN(h25_sparse_installed_last_slot_reached_in_one_tick);
	UT_RUN(h25_full_quiet_tail_DONE_waits_for_exact_consumption);
	UT_RUN(h25_full_quiet_tail_WOULD_BLOCK_waits_for_exact_consumption);
	UT_RUN(h25_sparse_quiet_last_slot_DONE_reached_in_one_tick);
	UT_RUN(h25_sparse_quiet_last_slot_WOULD_BLOCK_reached_in_one_tick);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
