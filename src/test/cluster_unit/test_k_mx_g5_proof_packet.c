/* Complete operation -> existing kind=2 -> independent request/reply packets.
 * Native/GCS/shard bodies are real; backend/SLRU/CONTROL leaves stay controlled.
 * Packet validity never establishes TT/grant/SELF or service admission. */
int k_packet_component_main(void);
#define K_MX_NATIVE_CALLER_MAIN k_packet_component_main
#include "test_k_mx_g5_native_caller.c"
#include "port/pg_crc32c.h"
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

static void
packet_reseal(ClusterMxProofRequest *packet)
{
	pg_crc32c crc;
	packet->route.checksum = 0;
	INIT_CRC32C(crc); COMP_CRC32C(crc, packet, packet->route.wire_bytes); FIN_CRC32C(crc);
	packet->route.checksum = (uint32)crc;
}

static ClusterMxCompositionReceive *
packet_input(uint32 total, uint32 first, uint32 start, ClusterMxCompositionReceiveStats *stats,
	ClusterMxRangeWireContext *c, ClusterMxProofRequest *packet, uint32 *bytes)
{
	ClusterCurrentMxKey key = {0}; ClusterMxCompositionReceive *op = NULL;
	PGAlignedBlock body; uint32 body_bytes = 0;
	chain_seed(total, start); key.multixact_id = 1000; key.cluster_epoch = 7;
	UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, stats));
	for (uint32 next = 0; next < first;) {
		ClusterMxFullMemberProof entries[7]; ClusterMxProofReply r = proof_window(total, next, entries);
		UT_ASSERT(r.next <= first);
		UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &r), MXCOMP_PARTIAL);
		next = r.next;
	}
	*c = proof_request_context();
	UT_ASSERT(cluster_multixact_current_proofs_request(&op, c, body.data, BLCKSZ, &body_bytes));
	UT_ASSERT(cluster_mx_proof_request_pack(c, 1, 1, body.data, body_bytes, packet, sizeof(*packet), bytes));
	return op;
}

static bool
packet_decode(const ClusterMxProofRequest *packet, uint32 bytes, ClusterMxProofRequestView *view)
{
	return cluster_mx_proof_request_decode(packet, bytes, 202611010,
		CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES, view);
}

UT_TEST(packet_roundtrip_keeps_full_source_and_global_tail)
{
	const uint32 sizes[] = {2, 256, 257, 65537};
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
		ClusterMxProofRequest packet; ClusterMxProofRequestView view;
		uint32 bytes = 0, first = s == 3 ? 65534 : 0, count = s == 0 ? 2 : s == 3 ? 3 : 7;
		ClusterMxCompositionReceive *op = packet_input(sizes[s], first, 1, &stats, &c, &packet, &bytes);
		UT_ASSERT(packet_decode(&packet, bytes, &view));
		UT_ASSERT_EQ(bytes, s == 0 ? 280 : s == 3 ? 292 : 340);
		UT_ASSERT_EQ(view.total, sizes[s]); UT_ASSERT_EQ(view.first, first); UT_ASSERT_EQ(view.count, count);
		UT_ASSERT_EQ(view.next, first + count); UT_ASSERT_EQ(view.peer_generation, 11);
		UT_ASSERT_EQ(view.source.capability_generation, 111); /* Directional records differ. */
		UT_ASSERT_EQ(memcmp(&view.source, &proof_source, sizeof(proof_source)), 0);
		UT_ASSERT_EQ(memcmp(&view.key, &c.key, sizeof(c.key)), 0);
		for (uint32 i = 0; i < count; i++) {
			UT_ASSERT_EQ(view.asks[i].member_ordinal, first + i);
			UT_ASSERT_EQ(view.asks[i].xid, 100 + first + i);
		}
		cluster_multixact_current_describe_finish(&op); chain_clean();
	}
}

UT_TEST(packet_decoder_uses_loaded_catalog_and_negotiated_bits)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxProofRequest packet; ClusterMxProofRequestView view, zero = {0}; uint32 bytes = 0;
	ClusterMxCompositionReceive *op = packet_input(2, 0, 1, &stats, &c, &packet, &bytes);
	const uint32 missing[] = {PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1,
		PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1, PGRAC_IC_HELLO_CAP_MULTIXACT_FULL_V1};
	for (unsigned i = 0; i < lengthof(missing); i++) {
		memset(&view, 0xa5, sizeof(view));
		UT_ASSERT(!cluster_mx_proof_request_decode(&packet, bytes, 202611010,
			CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES & ~missing[i], &view));
		UT_ASSERT_EQ(memcmp(&view, &zero, sizeof(view)), 0);
	}
	for (unsigned i = 0; i < 2; i++) {
		UT_ASSERT(!cluster_mx_proof_request_decode(&packet, bytes, i ? 202611011 : 202611009,
			CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES, &view));
		UT_ASSERT_EQ(memcmp(&view, &zero, sizeof(view)), 0);
	}
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

UT_TEST(packet_decoder_rejects_crc_lengths_and_inner_outer_drift)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxProofRequest good, bad; ClusterMxProofRequestView view, zero = {0}; uint32 bytes = 0;
	ClusterMxCompositionReceive *op = packet_input(257, 0, 1, &stats, &c, &good, &bytes);
	for (unsigned mode = 0; mode < 17; mode++) {
		ClusterMxRangeWireHeader *h; uint32 length = bytes;
		bad = good; h = (ClusterMxRangeWireHeader *)bad.data.data;
		switch (mode) {
		case 0: bad.route.format_id++; break;
		case 1: bad.route.version++; break;
		case 2: bad.route.kind = CLUSTER_MX_RANGE_RESPONSE_KIND; break;
		case 3: bad.route.request_id++; break;
		case 4: bad.route.epoch++; break;
		case 5: bad.route.requester_node = CLUSTER_MAX_NODES; break;
		case 6: bad.route.requester_backend = 0; break;
		case 7: bad.route.reserved = 1; break;
		case 8: bad.route.wire_bytes--; break;
		case 9: length--; break;
		case 10: h->request_id++; break;
		case 11: h->peer_generation = 0; break;
		case 12: h->kind = CLUSTER_MX_RANGE_REPLY; break;
		case 13: h->source_bytes--; break;
		case 14: h->next++; break;
		case 15: h->reserved32[1] = 1; break;
		case 16: bad.route.checksum++; break;
		}
		if (mode != 16) packet_reseal(&bad); /* Isolate semantic checks from CRC. */
		memset(&view, 0xa5, sizeof(view));
		UT_ASSERT(!packet_decode(&bad, length, &view));
		UT_ASSERT_EQ(memcmp(&view, &zero, sizeof(view)), 0);
	}
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

UT_TEST(packet_decoder_rejects_incoherent_composition_source)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxProofRequest good, bad; ClusterMxProofRequestView view, zero = {0}; uint32 bytes = 0;
	ClusterMxCompositionReceive *op = packet_input(257, 0, 1, &stats, &c, &good, &bytes);
	for (unsigned mode = 0; mode < 16; mode++) {
		ClusterMxRangeSource *s;
		bad = good; s = (ClusterMxRangeSource *)(bad.data.data + 80);
		switch (mode) {
		case 0: s->system_identifier = 0; break;
		case 1: s->formation_epoch++; break;
		case 2: s->boot_incarnation = 0; break;
		case 3: s->admission_revision = 0; break;
		case 4: s->admission_revision = UINT64_MAX; break;
		case 5: s->capability_generation = 0; break;
		case 6: s->capability_generation = UINT32_MAX; break;
		case 7: s->reserved = 1; break;
		case 8: s->native.multi++; break;
		case 9: s->native.origin_node++; break;
		case 10: s->native.epoch++; break;
		case 11: s->native.start = 0; break;
		case 12: s->native.span = 0; break;
		case 13: s->native.span++; break; /* No zero slot in this span. */
		case 14: s->snapshot.read_epoch++; break;
		case 15: s->target.forkNum = VISIBILITYMAP_FORKNUM; break;
		}
		packet_reseal(&bad); memset(&view, 0xa5, sizeof(view));
		UT_ASSERT(!packet_decode(&bad, bytes, &view));
		UT_ASSERT_EQ(memcmp(&view, &zero, sizeof(view)), 0);
	}
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

UT_TEST(packet_pack_is_atomic_for_bad_body_and_small_destination)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxProofRequest good, out, unchanged; uint32 bytes = 0, written = 99;
	ClusterMxCompositionReceive *op = packet_input(2, 0, 1, &stats, &c, &good, &bytes);
	memset(&out, 0xa5, sizeof(out)); unchanged = out;
	UT_ASSERT(!cluster_mx_proof_request_pack(&c, 1, 1, good.data.data, bytes - 48,
		&out, bytes - 1, &written));
	UT_ASSERT_EQ(written, 0); UT_ASSERT_EQ(memcmp(&out, &unchanged, sizeof(out)), 0);
	((ClusterMxRangeWireHeader *)good.data.data)->request_id++;
	UT_ASSERT(!cluster_mx_proof_request_pack(&c, 1, 1, good.data.data, bytes - 48,
		&out, sizeof(out), &written));
	UT_ASSERT_EQ(written, 0); UT_ASSERT_EQ(memcmp(&out, &unchanged, sizeof(out)), 0);
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

static void
packet_reply_members(const ClusterMxProofRequestView *view, ClusterCurrentMxMemberDesc *members)
{
	for (uint32 i = 0; i < view->count; i++) {
		memset(&members[i], 0, sizeof(members[i]));
		members[i].xid = view->asks[i].xid; members[i].member_status = view->asks[i].member_status;
	}
}

UT_TEST(packet_reply_preserves_exact_ask_and_canonical_local_ordinals)
{
	const uint32 sizes[] = {2, 257, 65537};
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
		ClusterMxProofRequest packet; ClusterMxProofRequestView view;
		ClusterMxRangeResponse response; ClusterMxFullMemberProof entries[7];
		ClusterCurrentMxMemberDesc members[7]; ClusterMxProofReply r, out;
		uint32 bytes = 0, written = 0, first = s == 2 ? 65534 : 0;
		ClusterMxCompositionReceive *op = packet_input(sizes[s], first, 1, &stats, &c, &packet, &bytes);
		UT_ASSERT(packet_decode(&packet, bytes, &view)); r = proof_window(sizes[s], first, entries);
		UT_ASSERT(cluster_mx_proof_response_pack(&view, &c, MXCOMP_READ_OK, &r,
			&response, sizeof(response), &written));
		UT_ASSERT_EQ(written, s == 0 ? 656 : s == 1 ? 1656 : 856);
		packet_reply_members(&view, members);
		UT_ASSERT_EQ(cluster_mx_range_wire_decode_proofs(response.data.data, written - 48,
			&c, 0, sizes[s], first, members, view.count, &out), MXCOMP_READ_OK);
		UT_ASSERT_EQ(out.count, view.count); UT_ASSERT_EQ(out.next, view.next);
		for (uint32 i = 0; i < out.count; i++) {
			UT_ASSERT_EQ(out.proofs[i].member_ordinal, first + i);
			UT_ASSERT_EQ(out.proofs[i].proof.member_ordinal, i);
		}
		cluster_multixact_current_describe_finish(&op); chain_clean();
	}
}

UT_TEST(packet_reply_rejects_changed_window_or_member_without_partial_bytes)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxProofRequest packet; ClusterMxProofRequestView view;
	ClusterMxRangeResponse response, unchanged; ClusterMxFullMemberProof entries[7];
	uint32 bytes = 0; ClusterMxCompositionReceive *op = packet_input(257, 0, 1, &stats, &c, &packet, &bytes);
	UT_ASSERT(packet_decode(&packet, bytes, &view)); memset(&response, 0xa5, sizeof(response)); unchanged = response;
	for (unsigned mode = 0; mode < 10; mode++) {
		ClusterMxProofReply r = proof_window(257, 0, entries); uint32 written = 99;
		if (mode == 0) r.total++;
		if (mode == 1) { r.first++; r.next++; }
		if (mode == 2) { r.count--; r.next--; r.proof_bytes -= sizeof(*entries); }
		if (mode == 3) entries[1].proof.member_xid++;
		if (mode == 4) entries[1].proof.member_status = MultiXactStatusForKeyShare;
		if (mode == 5) entries[1].member_ordinal++;
		if (mode == 6) entries[1].proof.member_ordinal++;
		if (mode == 7) r.last = true;
		if (mode == 8) r.source_bytes--;
		UT_ASSERT(!cluster_mx_proof_response_pack(&view, &c, MXCOMP_READ_OK, &r,
			&response, mode == 9 ? 1 : sizeof(response), &written));
		UT_ASSERT_EQ(written, 0); UT_ASSERT_EQ(memcmp(&response, &unchanged, sizeof(response)), 0);
	}
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

UT_TEST(packet_non_ok_reply_never_yields_provisional_proofs)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxProofRequest packet; ClusterMxProofRequestView view;
	ClusterMxRangeResponse response; ClusterCurrentMxMemberDesc members[7];
	ClusterMxProofReply out, zero = {0}; uint32 bytes = 0, written = 0;
	ClusterMxCompositionReceive *op = packet_input(257, 0, 1, &stats, &c, &packet, &bytes);
	UT_ASSERT(packet_decode(&packet, bytes, &view)); packet_reply_members(&view, members);
	for (int status = MXCOMP_READ_RETRY; status <= MXCOMP_READ_UNKNOWN; status++) {
		UT_ASSERT(cluster_mx_proof_response_pack(&view, &c, status, NULL,
			&response, sizeof(response), &written));
		UT_ASSERT_EQ(written, 256);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_mx_range_wire_decode_proofs(response.data.data, written - 48,
			&c, 0, 257, 0, members, view.count, &out), status);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	}
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

UT_TEST(packet_short_proof_buffer_is_rejected_before_any_member_read)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxProofRequest packet; ClusterMxProofRequestView view; uint32 bytes = 0;
	ClusterMxFullMemberProof entries[7]; ClusterMxProofReply r;
	ClusterMxCompositionReceive *op = packet_input(257, 0, 1, &stats, &c, &packet, &bytes);
	long page_bytes = sysconf(_SC_PAGESIZE);
	void *pages;
	UT_ASSERT(packet_decode(&packet, bytes, &view));
	UT_ASSERT(page_bytes > 0);
	if (page_bytes <= 0) { cluster_multixact_current_describe_finish(&op); return; }
	pages = mmap(NULL, 2 * (Size)page_bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
	UT_ASSERT(pages != MAP_FAILED);
	if (pages == MAP_FAILED) { cluster_multixact_current_describe_finish(&op); return; }
	UT_ASSERT_EQ(mprotect((char *)pages + page_bytes, page_bytes, PROT_NONE), 0);
	r = proof_window(257, 0, entries);
	for (unsigned short_count = 0; short_count < 2; short_count++) {
		char *short_buffer = (char *)pages + page_bytes - short_count * sizeof(*entries);
		pid_t child;
		int status = 0;
		if (short_count != 0) memcpy(short_buffer, entries, sizeof(*entries));
		r.proofs = (const ClusterMxFullMemberProof *)short_buffer;
		r.proof_bytes = short_count * sizeof(*entries); /* Claimed window still has seven members. */
		child = fork();
		UT_ASSERT(child >= 0);
		if (child == 0) {
			struct rlimit no_core = {0, 0};
			ClusterMxRangeResponse response, unchanged;
			uint32 written = 99;
			bool ok;
			(void)setrlimit(RLIMIT_CORE, &no_core);
			memset(&response, 0xa5, sizeof(response)); unchanged = response;
			ok = cluster_mx_proof_response_pack(&view, &c, MXCOMP_READ_OK, &r,
				&response, sizeof(response), &written);
			_exit(!ok && written == 0 && memcmp(&response, &unchanged, sizeof(response)) == 0 ? 0 : 2);
		}
		if (child > 0) {
			UT_ASSERT_EQ(waitpid(child, &status, 0), child);
			UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
		}
	}
	UT_ASSERT_EQ(munmap(pages, 2 * (Size)page_bytes), 0);
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

UT_TEST(packet_zero_slot_maps_physical_span_to_logical_total)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxProofRequest packet; ClusterMxProofRequestView view; uint32 bytes = 0;
	ClusterMxCompositionReceive *op = packet_input(2, 0, UINT32_MAX - 1, &stats, &c, &packet, &bytes);
	UT_ASSERT(packet_decode(&packet, bytes, &view));
	UT_ASSERT_EQ(view.source.native.span, 3); UT_ASSERT_EQ(view.total, 2);
	((ClusterMxRangeWireHeader *)packet.data.data)->total = 4; packet_reseal(&packet);
	UT_ASSERT(!packet_decode(&packet, bytes, &view));
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

UT_TEST(packet_request_and_reply_use_original_shard_affinity)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxProofRequest packet; ClusterMxProofRequestView view;
	ClusterMxRangeResponse response; ClusterMxFullMemberProof entries[7]; ClusterMxProofReply r;
	ClusterMxRangeRoute route; BufferTag tag, expected; uint32 bytes = 0, written = 0;
	ClusterMxCompositionReceive *op = packet_input(257, 0, 1, &stats, &c, &packet, &bytes);
	UT_ASSERT(packet_decode(&packet, bytes, &view)); r = proof_window(257, 0, entries);
	UT_ASSERT(cluster_mx_proof_response_pack(&view, &c, MXCOMP_READ_OK, &r, &response, sizeof(response), &written));
	expected = GcsBlockCurrentMxRouteTagMake(901, 7, 1, 1);
	UT_ASSERT(cluster_mx_range_transport_route(&packet, bytes, PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &route, &tag));
	UT_ASSERT_EQ(memcmp(&tag, &expected, sizeof(tag)), 0);
	UT_ASSERT(cluster_mx_range_transport_route(&response, written, PGRAC_IC_MSG_GCS_BLOCK_REPLY, &route, &tag));
	for (int workers = 1; workers <= 8; workers *= 2) {
		int shard = cluster_lms_shard_for_tag(&expected, workers);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &packet, bytes, workers), shard);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REPLY, &response, written, workers), shard);
	}
	packet.data.data[0] ^= 1;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &packet, bytes, 4), -1);
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

int main(void)
{
	TopMemoryContext = AllocSetContextCreate(CurrentMemoryContext, "proof packet", ALLOCSET_DEFAULT_SIZES);
	CurrentMemoryContext = TopMemoryContext;
	UT_PLAN(11);
	UT_RUN(packet_roundtrip_keeps_full_source_and_global_tail);
	UT_RUN(packet_decoder_uses_loaded_catalog_and_negotiated_bits);
	UT_RUN(packet_decoder_rejects_crc_lengths_and_inner_outer_drift);
	UT_RUN(packet_decoder_rejects_incoherent_composition_source);
	UT_RUN(packet_pack_is_atomic_for_bad_body_and_small_destination);
	UT_RUN(packet_reply_preserves_exact_ask_and_canonical_local_ordinals);
	UT_RUN(packet_reply_rejects_changed_window_or_member_without_partial_bytes);
	UT_RUN(packet_non_ok_reply_never_yields_provisional_proofs);
	UT_RUN(packet_short_proof_buffer_is_rejected_before_any_member_read);
	UT_RUN(packet_zero_slot_maps_physical_span_to_logical_total);
	UT_RUN(packet_request_and_reply_use_original_shard_affinity);
	MemoryContextDelete(TopMemoryContext); UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
