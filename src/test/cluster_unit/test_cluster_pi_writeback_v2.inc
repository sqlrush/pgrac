/*-------------------------------------------------------------------------
 *
 * test_cluster_pi_writeback_v2.inc
 *	  Canonical structural transport values and refusal boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_pi_writeback_v2.inc
 *
 * NOTES
 *	  PGRAC-original tests included by the writeback unit. Canonical wire
 *	  values alone never constitute a structural completion certificate.
 *
 *-------------------------------------------------------------------------
 */
#include "catalog/storage_xlog.h"
#include "catalog/pg_tablespace_d.h"

static void
wb_v2_fixture(ClusterPiWritebackMessageV2 *m, const ClusterPiDataFactV1 *data)
{
	ClusterPiStructuralFactV2 *s;
	ClusterSpaceWalChange *id;
	ClusterSpaceReservationChange *r;
	memset(m, 0, sizeof(*m));
	m->verb = CLUSTER_PI_WRITEBACK_NOTIFY;
	m->count = 2;
	m->nonce = 81;
	m->epoch = 1;
	m->peer = wb_sources[1];
	m->facts[0].kind = CLUSTER_PI_WRITEBACK_DATA_V2;
	m->facts[0].proof.data = *data;
	m->facts[1].kind = CLUSTER_PI_WRITEBACK_STRUCTURAL_V2;
	s = &m->facts[1].proof.structural;
	s->durability_flags = 31;
	s->terminal = *data;
	s->terminal.storage_cut.resource.blockNum++;
	id = &s->change.identity;
	r = &s->change.reservation;
	id->action = CLUSTER_SPACE_WAL_TRUNCATE;
	id->nblocks = 4;
	id->before_token = 9;
	id->result_token = 11;
	id->expected.key.system_identifier = data->binding.identity.system_identifier;
	id->expected.key.database_incarnation = data->binding.source.claim.database_incarnation;
	memcpy(id->expected.key.storage_uuid, data->binding.identity.storage_uuid, 16);
	id->expected.key.locator = data->binding.identity.locator;
	id->expected.sequence = id->expected.operation = 1;
	id->expected.state = CLUSTER_SPACE_IDENTITY_LIVE;
	memset(id->expected.incarnation, 5, 16);
	id->result = id->expected;
	id->result.sequence = id->result.operation = 2;
	memset(id->result.incarnation, 6, 16);
	r->action = CLUSTER_SPACE_RESERVATION_RESET;
	r->before.identity = id->expected;
	r->before.next_block = 12;
	r->before_token = 10;
	r->result_token = 11;
	r->result.identity = id->result;
	r->first_block = r->result.next_block = 4;
	s->terminal.binding.identity.forknum = SPACE_FORKNUM;
	s->terminal.binding.identity.blockno = 0;
	memcpy(s->terminal.binding.version.segment_incarnation, id->result.incarnation, 16);
	s->terminal.binding.version.mutation_token = id->result_token;
	s->terminal.binding.rmid = RM_SMGR_ID;
	s->terminal.binding.info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
	s->terminal.binding.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	s->ko.verb = CLUSTER_KO_SHARED_REQUEST;
	s->ko.batch_id = 19;
	s->ko.epoch = m->epoch;
	s->ko.origin_node = data->binding.source.claim.identity.origin_node_id;
	s->ko.origin_boot = data->binding.source.claim.identity.origin_owner_incarnation;
	s->ko.peer_node = m->peer.claim.identity.origin_node_id;
	s->ko.peer_boot = m->peer.claim.identity.origin_owner_incarnation;
	s->ko.key = id->expected.key;
	memcpy(s->ko.incarnation, id->expected.incarnation, 16);
	s->ko.members[0] = 7;
	memset(s->ko.member_digest, 0x55, 32);
}

static void
wb_v2_drop(ClusterPiWritebackMessageV2 *m)
{
	ClusterPiStructuralFactV2 *s = &m->facts[1].proof.structural;
	ClusterSpaceWalChange *id = &s->change.identity;
	id->action = CLUSTER_SPACE_WAL_TOMBSTONE;
	id->nblocks = InvalidBlockNumber;
	id->result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	memcpy(id->result.incarnation, id->expected.incarnation, 16);
	s->change.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	s->change.reservation.result.identity = id->result;
	s->change.reservation.result.next_block = s->change.reservation.before.next_block;
	s->change.reservation.first_block = 0;
	memcpy(s->terminal.binding.version.segment_incarnation, id->result.incarnation, 16);
	s->terminal.binding.rmid = RM_XACT_ID;
	s->terminal.binding.info = XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO;
	s->durability_flags = 15;
}

static void
wb_v2_origin_fixture(ClusterPiWritebackMessageV2 *m, const ClusterPiDataFactV1 *data)
{
	ClusterPiStructuralFactV2 *s;
	wb_v2_fixture(m, data);
	m->facts[0] = m->facts[1];
	m->count = 1;
	m->peer = wb_sources[2];
	s = &m->facts[0].proof.structural;
	UT_ASSERT_EQ(s->ko.origin_node, 2);
	UT_ASSERT_EQ(s->terminal.storage_cut.master_node, 0);
	/* The original DDL barrier was 2 -> 0. Master 0 later notifies 2;
	 * no KO self-request is invented for that notification. */
	s->ko.peer_node = 0;
	s->ko.peer_boot = wb_sources[0].claim.identity.origin_owner_incarnation;
}

UT_TEST(writeback_v2_origin_receives_original_master_ko)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 m, decoded, ack;
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
	Size length = 0;
	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterPiStructuralFactV2 *s;
		wb_v2_origin_fixture(&m, &data);
		s = &m.facts[0].proof.structural;
		if (mode == 1) {
			s->terminal.write_cut = data_pi_cut();
			s->terminal.write_cut.master_node = 0;
			s->terminal.write_cut.pi_holders_bitmap = 7;
			s->terminal.write_cut.holder.master_session_incarnation = 9;
			s->terminal.write_cut.holder.requester_target_generation = 73;
			s->terminal.write_cut.holder.assertion.resource = s->terminal.storage_cut.resource;
			memset(&s->terminal.storage_cut, 0, sizeof(s->terminal.storage_cut));
		}
		UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
		UT_ASSERT_EQ(length, 176 + 1424);
		UT_ASSERT(cluster_pi_writeback_decode_v2(bytes, length, &decoded));
		UT_ASSERT_EQ(decoded.peer.claim.identity.origin_node_id, 2);
		UT_ASSERT_EQ(decoded.facts[0].proof.structural.ko.origin_node, 2);
		UT_ASSERT_EQ(decoded.facts[0].proof.structural.ko.peer_node, 0);
		ack = decoded;
		ack.verb = CLUSTER_PI_WRITEBACK_ACK;
		UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&m, &ack));
		ack.facts[0].proof.structural.ko.batch_id++;
		UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&m, &ack));
		ack = decoded;
		ack.verb = CLUSTER_PI_WRITEBACK_ACK;
		if (mode == 0) ack.facts[0].proof.structural.terminal.storage_cut.authority.transition_count++;
		else ack.facts[0].proof.structural.terminal.write_cut.transition_count++;
		UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&m, &ack));
	}
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_origin_rejects_retargeted_or_stale_ko)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 m, out, sentinel;
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2], unchanged[sizeof(bytes)];
	Size length = 0;
	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	wb_v2_origin_fixture(&m, &data);
	UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
	memset(unchanged, 0xa5, sizeof(unchanged));
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterPiStructuralFactV2 *s;
		wb_v2_origin_fixture(&m, &data);
		s = &m.facts[0].proof.structural;
		if (fault == 0) s->ko.peer_node = 1;
		if (fault == 1) s->ko.peer_boot++;
		if (fault == 2) m.peer.claim.identity.origin_owner_incarnation++;
		if (fault == 3) s->terminal.storage_cut.master_session_incarnation++;
		if (fault == 4) s->ko.peer_node = s->ko.origin_node;
		if (fault == 5) s->ko.members[0] &= ~1u;
		if (fault == 6) s->ko.origin_boot++;
		length = 93;
		memcpy(bytes, unchanged, sizeof(bytes));
		UT_ASSERT(!cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
		UT_ASSERT_EQ(length, 93);
		UT_ASSERT(memcmp(bytes, unchanged, sizeof(bytes)) == 0);
	}
	/* Decoder enforces the same master-peer relationship. */
	wb_v2_origin_fixture(&m, &data);
	UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
	bytes[176 + 904 + 110] = 1;
	memset(&sentinel, 0xa5, sizeof(sentinel));
	out = sentinel;
	UT_ASSERT(!cluster_pi_writeback_decode_v2(bytes, length, &out));
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_has_explicit_data_and_structural_layout)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 m, decoded;
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2 + 1], structure[656], ko[160];
	Size length = 99;
	const Size at = 176 + 600;
	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	wb_v2_fixture(&m, &data);
	UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes + 1, sizeof(bytes) - 1, &length));
	UT_ASSERT_EQ(length, 176 + 600 + 1424);
	UT_ASSERT(memcmp(bytes + 1, "PPWB\002\000\260\000\001\000\000\000\002\000\000\000", 16) == 0);
	UT_ASSERT(memcmp(bytes + 1 + 176, "\001\000\000\000\130\002\000\000", 8) == 0);
	UT_ASSERT(memcmp(bytes + 1 + at, "\002\000\000\000\220\005\000\000\037\000\000\000\000\000\000\000", 16) == 0);
	UT_ASSERT(cluster_space_structure_wal_encode(&m.facts[1].proof.structural.change, structure, sizeof(structure)));
	UT_ASSERT(cluster_ko_shared_encode_v2(&m.facts[1].proof.structural.ko, ko, sizeof(ko)));
	UT_ASSERT(memcmp(bytes + 1 + at + 248, structure, sizeof(structure)) == 0);
	UT_ASSERT(memcmp(bytes + 1 + at + 904, ko, sizeof(ko)) == 0);
	UT_ASSERT(cluster_pi_writeback_decode_v2(bytes + 1, length, &decoded));
	UT_ASSERT_EQ(decoded.facts[1].proof.structural.terminal.storage_cut.authority.transition_count,
		m.facts[1].proof.structural.terminal.storage_cut.authority.transition_count);
	UT_ASSERT_EQ(decoded.facts[1].proof.structural.terminal.binding.identity.forknum, SPACE_FORKNUM);
	{
		ClusterPiWritebackMessageV2 x = m;
		ClusterPiStructuralFactV2 *s = &x.facts[0].proof.structural;
		BufferTag retired = m.facts[1].proof.structural.terminal.storage_cut.resource;
		x.count = 1;
		x.facts[0] = m.facts[1];
		s->terminal.write_cut = data_pi_cut();
		s->terminal.write_cut.master_node = 0;
		s->terminal.write_cut.pi_holders_bitmap = 7;
		s->terminal.write_cut.holder.master_session_incarnation = 9;
		s->terminal.write_cut.holder.requester_target_generation = 73;
		s->terminal.write_cut.holder.assertion.resource = retired;
		memset(&s->terminal.storage_cut, 0, sizeof(s->terminal.storage_cut));
		/* Shared catalog locators legitimately have database OID zero. */
		s->terminal.write_cut.holder.assertion.resource.dbOid = 0;
		s->terminal.write_cut.holder.assertion.resource.spcOid = GLOBALTABLESPACE_OID;
		s->terminal.binding.identity.locator.dbOid = 0;
		s->terminal.binding.identity.locator.spcOid = GLOBALTABLESPACE_OID;
		s->change.identity.expected.key.locator.dbOid = 0;
		s->change.identity.expected.key.locator.spcOid = GLOBALTABLESPACE_OID;
		s->change.identity.result.key.locator.dbOid = 0;
		s->change.identity.result.key.locator.spcOid = GLOBALTABLESPACE_OID;
		s->change.reservation.before.identity.key.locator.dbOid = 0;
		s->change.reservation.before.identity.key.locator.spcOid = GLOBALTABLESPACE_OID;
		s->change.reservation.result.identity.key.locator.dbOid = 0;
		s->change.reservation.result.identity.key.locator.spcOid = GLOBALTABLESPACE_OID;
		s->ko.key.locator.dbOid = 0;
		s->ko.key.locator.spcOid = GLOBALTABLESPACE_OID;
		UT_ASSERT(cluster_pcm_pi_write_cut_valid_v1(&s->terminal.write_cut));
		UT_ASSERT(cluster_pi_writeback_encode_v2(&x, bytes, sizeof(bytes), &length));
		UT_ASSERT(cluster_pi_writeback_decode_v2(bytes, length, &decoded));
		UT_ASSERT_EQ(decoded.facts[0].proof.structural.terminal.write_cut.transition_count,
			s->terminal.write_cut.transition_count);
		UT_ASSERT_EQ(decoded.facts[0].proof.structural.terminal.binding.identity.locator.dbOid, 0);
	}
	wb_v2_drop(&m);
	UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
	UT_ASSERT(cluster_pi_writeback_decode_v2(bytes, length, &decoded));
	UT_ASSERT_EQ(decoded.facts[1].proof.structural.change.identity.action, CLUSTER_SPACE_WAL_TOMBSTONE);
	/* All maximum-sized structural facts, with distinct retired blocks. */
	m.facts[0] = m.facts[1];
	m.count = CLUSTER_PI_WRITEBACK_MAX;
	for (unsigned i = 1; i < m.count; i++) {
		m.facts[i] = m.facts[0];
		m.facts[i].proof.structural.terminal.storage_cut.resource.blockNum += i;
	}
	UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
	UT_ASSERT_EQ(length, CLUSTER_PI_WRITEBACK_MAX_BYTES_V2);
	UT_ASSERT(cluster_pi_writeback_decode_v2(bytes, length, &decoded));
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_rejects_unqualified_structural_values)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 m;
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2], unchanged[sizeof(bytes)];
	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	memset(unchanged, 0xa5, sizeof(unchanged));
	for (unsigned fault = 0; fault < 24; fault++) {
		ClusterPiStructuralFactV2 *s;
		Size length = 93;
		wb_v2_fixture(&m, &data);
		s = &m.facts[1].proof.structural;
		if (fault == 0) s->durability_flags &= ~CLUSTER_PI_STRUCTURAL_BASE_DURABLE;
		if (fault == 1) s->durability_flags |= 32;
		if (fault == 2) s->terminal.binding.flags = 0;
		if (fault == 3) s->terminal.binding.identity.forknum = MAIN_FORKNUM;
		if (fault == 4) s->terminal.binding.identity.blockno = 1;
		if (fault == 5) s->terminal.binding.version.mutation_token++;
		if (fault == 6) s->terminal.binding.version.segment_incarnation[0]++;
		if (fault == 7) s->ko.origin_boot++;
		if (fault == 8) s->ko.peer_boot++;
		if (fault == 9) s->ko.epoch++;
		if (fault == 10) s->ko.incarnation[0]++;
		if (fault == 11) s->terminal.storage_cut.resource.relNumber++;
		if (fault == 12) s->terminal.binding.info = XLOG_SMGR_TRUNCATE;
		if (fault == 13) { wb_v2_drop(&m); s->terminal.binding.info = XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO; }
		if (fault == 14) { wb_v2_drop(&m); s->terminal.binding.info = XLOG_XACT_COMMIT_PREPARED | XLOG_XACT_HAS_INFO; }
		if (fault == 15) { wb_v2_drop(&m); s->terminal.binding.info = XLOG_XACT_COMMIT; }
		if (fault == 16) { wb_v2_drop(&m); s->durability_flags |= CLUSTER_PI_STRUCTURAL_BASE_DURABLE; }
		if (fault == 17) s->ko.key.database_incarnation++;
		if (fault == 18) s->ko.verb = CLUSTER_KO_SHARED_ACK;
		if (fault == 19) memset(&s->terminal.storage_cut, 0, sizeof(s->terminal.storage_cut));
		if (fault == 20) s->terminal.write_cut = data_pi_cut();
		if (fault == 21) memset(s->terminal.binding.source.claim.identity.authority_uuid, 0, 16);
		if (fault == 22) m.peer.claim.identity.storage_uuid[0]++;
		if (fault == 23) s->ko.members[0] &= ~1u; /* master is neither KO endpoint */
		memcpy(bytes, unchanged, sizeof(bytes));
		UT_ASSERT(!cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
		UT_ASSERT_EQ(length, 93);
		UT_ASSERT(memcmp(bytes, unchanged, sizeof(bytes)) == 0);
	}
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_refusals_preserve_outputs_and_reject_old_wire)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 m, out, sentinel;
	ClusterPiWritebackMessageV1 old = {0};
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2], good[sizeof(bytes)];
	const unsigned offsets[] = {4, 6, 168, 176, 178, 180, 776, 778, 780, 788, 1024, 1680};
	Size length = 0, wanted;
	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	wb_v2_fixture(&m, &data);
	UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
	wanted = length;
	memcpy(good, bytes, length);
	memset(&sentinel, 0xa5, sizeof(sentinel));
	for (unsigned i = 0; i < lengthof(offsets) + 2; i++) {
		memcpy(bytes, good, wanted);
		out = sentinel;
		length = wanted;
		if (i < lengthof(offsets)) bytes[offsets[i]] ^= 0x80;
		else if (i == lengthof(offsets)) length--;
		else bytes[length++] = 0;
		UT_ASSERT(!cluster_pi_writeback_decode_v2(bytes, length, &out));
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	}
	length = 93;
	UT_ASSERT(!cluster_pi_writeback_encode_v2(&m, bytes, wanted - 1, &length));
	UT_ASSERT_EQ(length, 93);
	old.verb = CLUSTER_PI_WRITEBACK_NOTIFY; old.count = 1;
	old.nonce = m.nonce; old.epoch = m.epoch; old.peer = m.peer; old.facts[0] = data;
	UT_ASSERT(cluster_pi_writeback_encode_v1(&old, bytes, sizeof(bytes), &length));
	out = sentinel;
	UT_ASSERT(!cluster_pi_writeback_decode_v2(bytes, length, &out));
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	UT_ASSERT(!cluster_pi_writeback_encode_v2(&m, (uint8 *)&m, sizeof(m), &length));
	UT_ASSERT(!cluster_pi_writeback_decode_v2(&m, sizeof(m), &m));
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_ack_is_only_an_exact_ordered_subset)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 m, ack;
	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	wb_v2_fixture(&m, &data);
	m.count = 3;
	m.facts[2] = m.facts[1];
	m.facts[2].proof.structural.terminal.storage_cut.resource.blockNum++;
	ack = m; ack.verb = CLUSTER_PI_WRITEBACK_ACK; ack.count = 2;
	ack.facts[0] = m.facts[0]; ack.facts[1] = m.facts[2];
	UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&m, &ack));
	ack.facts[0] = m.facts[2]; ack.facts[1] = m.facts[0];
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&m, &ack));
	ack.count = 1; ack.facts[0] = m.facts[1];
	ack.facts[0].proof.structural.terminal.storage_cut.authority.transition_count++;
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&m, &ack));
	ack.facts[0] = m.facts[1]; ack.facts[0].proof.structural.ko.batch_id++;
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&m, &ack));
	ack.facts[0] = m.facts[1]; ack.peer.claim.identity.origin_owner_incarnation++;
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&m, &ack));
	ack = m; ack.verb = CLUSTER_PI_WRITEBACK_ACK; ack.count = 0;
	UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&m, &ack));
	ack.nonce++;
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&m, &ack));
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}
