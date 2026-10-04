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

/* Run the actual sender, peer physical owner and reply transport. A failed
 * decode is an assertion failure, followed by caller cleanup, never a skip. */
static bool
wb_v2_data_reply(ClusterPiWritebackMessageV2 *request, ClusterPiWritebackMessageV2 *reply)
{
	bool decoded = cluster_pi_writeback_decode_v2(wb_wire, wb_length, request);
	UT_ASSERT(decoded);
	if (!decoded)
		return false;
	wb_deliver(0, 1, wb_wire, wb_length);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT(!wb_inputs_pinned[1]);
	wb_select(1, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_destination, 0);
	decoded = cluster_pi_writeback_decode_v2(wb_wire, wb_length, reply);
	UT_ASSERT(decoded);
	if (decoded)
		UT_ASSERT(cluster_pi_writeback_ack_matches_v2(request, reply));
	return decoded;
}

UT_TEST(writeback_v2_data_job_reaches_actual_remote_ack)
{
	for (unsigned ceiling = 0; ceiling < 2; ceiling++) {
		ClusterPageDataReceiptV1 *data = wb_setup();
		ClusterPiWritebackJobV1 *job = NULL;
		ClusterPiPhysicalAckV1 *ack = NULL;
		ClusterWalSourceRef peer = wb_sources[1];
		ClusterPiWritebackMessageV2 request, reply;
		int32 node = -1;
		peer.claim.max_config_generation += ceiling;
		wb_local_caps = wb_peer_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
		UT_ASSERT_EQ(
			cluster_pi_writeback_begin_v1((const ClusterPageDataReceiptV1 **)&data, 1, &peer, &job),
			CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		wb_select(0, B_LMON);
		cluster_pi_writeback_lmon_tick_v1();
		physical_pi_from_source(&wb_sources[1], 19);
		if (!wb_v2_data_reply(&request, &reply))
			goto done;
		UT_ASSERT_EQ(request.facts[0].kind, CLUSTER_PI_WRITEBACK_DATA_V2);
		UT_ASSERT_EQ(reply.count, 1);
		UT_ASSERT_EQ(memcmp(&reply.peer, &peer, sizeof(peer)), 0);
		UT_ASSERT_EQ(pi_discards, 1);
		UT_ASSERT_EQ(wb_master_completions, 0);
		wb_deliver(1, 0, wb_wire, wb_length);
		wb_select(0, B_CHECKPOINTER);
		UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(cluster_page_data_pi_ack_import_v1(job, 0, data, &ack));
		UT_ASSERT(cluster_page_data_pi_ack_read_v1(ack, data, &node));
		UT_ASSERT_EQ(node, 1);
		UT_ASSERT_EQ(wb_master_completions, 0); /* A wire ACK alone never clears master bits. */
	done:
		wb_select(0, B_CHECKPOINTER);
		cluster_page_data_pi_ack_free_v1(&ack);
		cluster_pi_writeback_release_v1(&job);
		cluster_page_data_receipt_free_v1(&data);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(writeback_v2_data_job_preserves_partial_ack_and_master_cut)
{
	ClusterPageDataReceiptV1 *data[2];
	ClusterPiWritebackJobV1 *job = NULL;
	ClusterPiPhysicalAckV1 *ack = NULL;
	ClusterPiWritebackMessageV2 request, reply;
	data[0] = wb_setup();
	data[1] = wb_second_page();
	wb_local_caps = wb_peer_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
	UT_ASSERT_EQ(cluster_pi_writeback_begin_v1((const ClusterPageDataReceiptV1 **)data, 2,
											   &wb_sources[1], &job),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	wb_select(0, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	physical_pi_from_source(&wb_sources[1], 19);
	pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
	if (!wb_v2_data_reply(&request, &reply))
		goto done;
	UT_ASSERT_EQ(reply.count, 1);
	UT_ASSERT_EQ(reply.facts[0].proof.data.binding.identity.forknum, VISIBILITYMAP_FORKNUM);
	UT_ASSERT_EQ(pi_discards, 0);
	wb_deliver(1, 0, wb_wire, wb_length);
	wb_select(0, B_CHECKPOINTER);
	wb_storage_cut.authority.transition_count++;
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(!cluster_page_data_pi_ack_import_v1(job, 0, data[0], &ack));
	UT_ASSERT(cluster_page_data_pi_ack_import_v1(job, 1, data[1], &ack));
	cluster_page_data_pi_ack_free_v1(&ack);
	wb_vm_cut.authority.transition_count++;
	UT_ASSERT(!cluster_page_data_pi_ack_import_v1(job, 1, data[1], &ack));
done:
	wb_select(0, B_CHECKPOINTER);
	pg_atomic_fetch_sub_u32(&descriptors[1].bufferdesc.state, 1);
	cluster_page_data_pi_ack_free_v1(&ack);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data[0]);
	cluster_page_data_receipt_free_v1(&data[1]);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_data_job_rejects_unrelated_and_old_ack)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job;
	ClusterPiWritebackMessageV2 request, reply, wrong;
	ClusterPiWritebackMessageV1 old = { 0 };
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
	Size length;
	wb_local_caps = wb_peer_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
	job = wb_start(data);
	physical_pi_from_source(&wb_sources[1], 19);
	if (!wb_v2_data_reply(&request, &reply))
		goto done;
	for (unsigned fault = 0; fault < 10; fault++) {
		wrong = reply;
		if (fault == 0)
			wrong.nonce++;
		if (fault == 1)
			wrong.epoch++;
		if (fault == 2)
			wrong.peer.claim.claim_sha256[0]++;
		if (fault == 3)
			wrong.peer.claim.identity.origin_owner_incarnation++;
		if (fault == 4)
			wrong.peer.claim.max_config_generation++;
		if (fault == 5)
			wrong.facts[0].proof.data.binding.record_crc++;
		if (fault == 6)
			wrong.facts[0].proof.data.storage_cut.authority.transition_count++;
		if (fault == 7) {
			old.verb = reply.verb;
			old.count = reply.count;
			old.nonce = reply.nonce;
			old.epoch = reply.epoch;
			old.peer = reply.peer;
			old.facts[0] = reply.facts[0].proof.data;
			UT_ASSERT(cluster_pi_writeback_encode_v1(&old, bytes, sizeof(bytes), &length));
		} else
			UT_ASSERT(cluster_pi_writeback_encode_v2(&wrong, bytes, sizeof(bytes), &length));
		if (fault == 9)
			wb_peer_caps = 0;
		wb_deliver(fault == 8 ? 2 : 1, 0, bytes, length);
		UT_ASSERT(!wb_shared->outbound_complete);
		wb_peer_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
		wb_select(0, B_CHECKPOINTER);
		UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	}
	UT_ASSERT(cluster_pi_writeback_encode_v2(&reply, bytes, sizeof(bytes), &length));
	wb_deliver(1, 0, bytes, length);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
done:
	wb_select(0, B_CHECKPOINTER);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_data_job_original_owner_retires_late_ack)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job, *next = NULL;
	ClusterPiWritebackMessageV2 request, reply;
	uint8 late[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
	Size length;
	wb_local_caps = wb_peer_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
	job = wb_start(data);
	physical_pi_from_source(&wb_sources[1], 19);
	if (!wb_v2_data_reply(&request, &reply))
		goto done;
	UT_ASSERT(cluster_pi_writeback_encode_v2(&reply, late, sizeof(late), &length));
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_begin_v1((const ClusterPageDataReceiptV1 **)&data, 1,
											   &wb_sources[1], &next),
				 CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(next == NULL);
	wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT_EQ(wb_shared->outbound_pid, 0);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	cluster_pi_writeback_release_v1(&job);
	UT_ASSERT_EQ(cluster_pi_writeback_begin_v1((const ClusterPageDataReceiptV1 **)&data, 1,
											   &wb_sources[1], &next),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	wb_deliver(1, 0, late, length);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(next), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT(!wb_shared->outbound_complete);
	wb_exit(0, (Datum)0);
	UT_ASSERT_EQ(wb_shared->outbound_pid, 0);
done:
	wb_select(0, B_CHECKPOINTER);
	cluster_pi_writeback_release_v1(&job);
	cluster_pi_writeback_release_v1(&next);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_data_job_refuses_identity_and_capability_drift)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterPageDataReceiptV1 *data = wb_setup();
		ClusterPiWritebackJobV1 *job;
		wb_local_caps = wb_peer_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
		job = wb_start(data);
		wb_select(0, B_CHECKPOINTER);
		if (fault == 0)
			wb_peer_caps = 0;
		if (fault == 1)
			wb_local_caps = 0;
		if (fault == 2)
			wb_boots[1]++;
		if (fault == 3)
			wb_epoch++;
		if (fault == 4)
			wb_stop = true;
		UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(wb_shared->outbound_pid, 0);
		UT_ASSERT_EQ(wb_master_completions | pi_discards, 0);
		cluster_pi_writeback_release_v1(&job);
		cluster_page_data_receipt_free_v1(&data);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(writeback_v2_data_begin_never_falls_back_to_old_peer)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = NULL;
	wb_local_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
	wb_peer_caps = 0;
	UT_ASSERT_EQ(cluster_pi_writeback_begin_v1((const ClusterPageDataReceiptV1 **)&data, 1,
											   &wb_sources[1], &job),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT(job == NULL);
	UT_ASSERT_EQ(wb_shared->outbound_pid | wb_sends | pi_discards, 0);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

static void
wb_v2_fixture(ClusterPiWritebackMessageV2 *m, const ClusterPiDataFactV1 *data)
{
	ClusterPiStructuralFactV2 *s;
	ClusterSpaceWalChange *id;
	ClusterSpaceReservationChange *r;
	wb_local_caps = wb_peer_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
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

static ClusterPiWritebackJobV1 *
wb_structural_start(ClusterPageDataReceiptV1 *data)
{
	ClusterPiDataFactV1 fact;
	ClusterPiWritebackMessageV2 m;
	ClusterPiWritebackJobV1 *job = NULL;
	const ClusterPageStructuralReceiptV2 *receipt = (const void *)73;
	UT_ASSERT(cluster_page_data_pi_fact_v1(data, &fact));
	wb_v2_fixture(&m, &fact);
	wb_structural_fact = m.facts[1];
	wb_structural_fact_ready = true;
	UT_ASSERT_EQ(cluster_pi_writeback_structural_begin_v2(&receipt, 1, &wb_sources[1], &job),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	wb_select(0, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	return job;
}

UT_TEST(writeback_v2_structural_job_binds_original_fact_and_ack)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = wb_structural_start(data);
	ClusterPiWritebackMessageV2 request, reply;
	ClusterWalWriterToken peer = { 0 };
	bool decoded = cluster_pi_writeback_decode_v2(wb_wire, wb_length, &request);
	Size length;
	UT_ASSERT(decoded);
	if (!decoded)
		goto done;
	UT_ASSERT_EQ(request.count, 1);
	UT_ASSERT_EQ(request.facts[0].kind, CLUSTER_PI_WRITEBACK_STRUCTURAL_V2);
	UT_ASSERT_EQ(memcmp(&request.facts[0], &wb_structural_fact, sizeof(wb_structural_fact)), 0);
	reply = request;
	reply.verb = CLUSTER_PI_WRITEBACK_ACK;
	UT_ASSERT(cluster_pi_writeback_encode_v2(&reply, wb_wire, sizeof(wb_wire), &length));
	wb_deliver(1, 0, wb_wire, length);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_pi_writeback_structural_ack_read_v2(job, 0, (const void *)73, &peer));
	UT_ASSERT_EQ(memcmp(&peer.ref, &wb_sources[1], sizeof(peer.ref)), 0);
	UT_ASSERT(cluster_pi_writeback_structural_ack_current_v2(&wb_structural_fact, &peer));
	UT_ASSERT(!cluster_pi_writeback_ack_read_v1(job, 0, data, &peer));
	UT_ASSERT_EQ(pi_discards | wb_master_completions | logical_pi_retire_calls, 0);
done:
	wb_select(0, B_CHECKPOINTER);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_structural_job_refuses_missing_original_owner)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = NULL;
	const ClusterPageStructuralReceiptV2 *receipt = (const void *)73;
	wb_structural_fact_ready = false;
	wb_local_caps = wb_peer_caps = PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;
	UT_ASSERT_NE(cluster_pi_writeback_structural_begin_v2(&receipt, 1, &wb_sources[1], &job),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(job == NULL);
	UT_ASSERT_EQ(wb_shared->outbound_pid | wb_sends | pi_discards, 0);
	job = wb_structural_start(data);
	wb_select(0, B_CHECKPOINTER);
	wb_structural_fact_ready = false;
	{
		ClusterWalWriterToken peer = { 0 }, before = peer;
		UT_ASSERT(!cluster_pi_writeback_structural_ack_read_v2(job, 0, receipt, &peer));
		UT_ASSERT_EQ(memcmp(&peer, &before, sizeof(peer)), 0);
	}
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_structural_empty_ack_keeps_page_obligation)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = wb_structural_start(data);
	ClusterPiWritebackMessageV2 reply;
	ClusterWalWriterToken peer = { 0 };
	bool decoded = cluster_pi_writeback_decode_v2(wb_wire, wb_length, &reply);
	Size length;
	UT_ASSERT(decoded);
	if (!decoded)
		goto done;
	reply.verb = CLUSTER_PI_WRITEBACK_ACK;
	reply.count = 0;
	UT_ASSERT(cluster_pi_writeback_encode_v2(&reply, wb_wire, sizeof(wb_wire), &length));
	wb_deliver(1, 0, wb_wire, length);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(!cluster_pi_writeback_structural_ack_read_v2(job, 0, (const void *)73, &peer));
	UT_ASSERT_EQ(pi_discards | wb_master_completions | logical_pi_retire_calls, 0);
done:
	wb_select(0, B_CHECKPOINTER);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_structural_job_cannot_accept_data_or_stale_cut)
{
	ClusterPageDataReceiptV1 *data = wb_setup();
	ClusterPiWritebackJobV1 *job = wb_structural_start(data);
	ClusterPiWritebackMessageV2 request, reply;
	ClusterWalWriterToken peer = { 0 };
	bool decoded = cluster_pi_writeback_decode_v2(wb_wire, wb_length, &request);
	Size length;
	UT_ASSERT(decoded);
	if (!decoded)
		goto done;
	for (unsigned fault = 0; fault < 3; fault++) {
		reply = request;
		reply.verb = CLUSTER_PI_WRITEBACK_ACK;
		if (fault == 0) {
			memset(&reply.facts[0], 0, sizeof(reply.facts[0]));
			reply.facts[0].kind = CLUSTER_PI_WRITEBACK_DATA_V2;
			UT_ASSERT(cluster_page_data_pi_fact_v1(data, &reply.facts[0].proof.data));
		} else if (fault == 1)
			reply.facts[0].proof.structural.terminal.storage_cut.authority.transition_count++;
		else
			reply.facts[0].proof.structural.ko.batch_id++;
		UT_ASSERT(cluster_pi_writeback_encode_v2(&reply, wb_wire, sizeof(wb_wire), &length));
		wb_deliver(1, 0, wb_wire, length);
		wb_select(0, B_CHECKPOINTER);
		UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	}
	reply = request;
	reply.verb = CLUSTER_PI_WRITEBACK_ACK;
	UT_ASSERT(cluster_pi_writeback_encode_v2(&reply, wb_wire, sizeof(wb_wire), &length));
	wb_deliver(1, 0, wb_wire, length);
	wb_select(0, B_CHECKPOINTER);
	UT_ASSERT_EQ(cluster_pi_writeback_poll_v1(job), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_pi_writeback_structural_ack_read_v2(job, 0, (const void *)73, &peer));
	wb_structural_fact.proof.structural.terminal.storage_cut.authority.transition_count++;
	UT_ASSERT(!cluster_pi_writeback_structural_ack_read_v2(job, 0, (const void *)73, &peer));
	wb_structural_fact = request.facts[0];
	wb_peer_caps = 0;
	UT_ASSERT(!cluster_pi_writeback_structural_ack_read_v2(job, 0, (const void *)73, &peer));
	UT_ASSERT(!cluster_pi_writeback_structural_ack_current_v2(&wb_structural_fact, &peer));
done:
	wb_select(0, B_CHECKPOINTER);
	cluster_pi_writeback_release_v1(&job);
	cluster_page_data_receipt_free_v1(&data);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
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
	ClusterPiWritebackMessageV2 m, ack = { 0 };
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

/* Relation results precede the master's own per-page cut. This representation
 * never converts the origin into the resource master or a physical PI holder. */
static void
wb_v2_offer_fixture(ClusterPiWritebackMessageV2 *m, const ClusterPiDataFactV1 *data, bool drop)
{
	wb_v2_fixture(m, data);
	if (drop)
		wb_v2_drop(m);
	m->facts[0] = m->facts[1];
	m->count = 1;
	m->facts[0].kind = CLUSTER_PI_WRITEBACK_STRUCTURE_OFFER_V2;
	memset(&m->facts[0].proof.structural.terminal.write_cut, 0,
		sizeof(m->facts[0].proof.structural.terminal.write_cut));
	memset(&m->facts[0].proof.structural.terminal.storage_cut, 0,
		sizeof(m->facts[0].proof.structural.terminal.storage_cut));
}

static uint32 wb_offer_accept_mask;
static unsigned wb_offer_accept_calls;
/* The real KO fixed-slot owner is tested in ko_stop. This endpoint fixture
 * checks the authenticated notice boundary and only acknowledges accepted
 * ownership, without manufacturing PAGE/physical disposal authority. */
bool
cluster_ko_shared_structure_accept_v2(const ClusterPiWritebackNoticeV1 *notice, uint32 index)
{
	ClusterPiWritebackFactV2 fact;
	uint64 revision;
	bool live = cluster_pi_writeback_structure_offer_read_v2(notice, index, &revision, &fact);
	UT_ASSERT(live);
	if (!live)
		return false;
	UT_ASSERT_EQ(fact.kind, CLUSTER_PI_WRITEBACK_STRUCTURE_OFFER_V2);
	UT_ASSERT_EQ(revision, wb_shared->inbound_revision);
	wb_offer_accept_calls++;
	return (wb_offer_accept_mask & (1u << index)) != 0;
}

UT_TEST(writeback_v2_relation_offer_ack_requires_original_owner_acceptance)
{
	for (unsigned mask = 0; mask < 8; mask++) {
		ClusterPageDataReceiptV1 *receipt = wb_setup();
		ClusterPiDataFactV1 data;
		ClusterPiWritebackMessageV2 request, reply;
		ClusterPiStructuralFactV2 *second;
		uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
		Size length;
		UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
		data.binding.source = wb_sources[(mask & 4) ? 2 : 0];
		wb_v2_offer_fixture(&request, &data, false);
		request.count = 2;
		request.facts[1] = request.facts[0];
		second = &request.facts[1].proof.structural;
		second->terminal.binding.identity.locator.relNumber++;
		second->ko.key.locator.relNumber++;
		second->change.identity.expected.key.locator.relNumber++;
		second->change.identity.result.key.locator.relNumber++;
		second->change.reservation.before.identity.key.locator.relNumber++;
		second->change.reservation.result.identity.key.locator.relNumber++;
		UT_ASSERT(cluster_pi_writeback_encode_v2(&request, bytes, sizeof(bytes), &length));
		wb_offer_accept_mask = mask;
		wb_offer_accept_calls = 0;
		wb_deliver(request.facts[0].proof.structural.ko.origin_node, 1, bytes, length);
		UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
		wb_select(1, B_BG_WRITER);
		UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
		UT_ASSERT_EQ(wb_offer_accept_calls, 2);
		UT_ASSERT_EQ(wb_shared->rejections.attempts[CLUSTER_PI_WRITEBACK_STRUCTURE_OWNER],
					 2 - (mask & 1) - ((mask >> 1) & 1));
		UT_ASSERT_EQ(wb_shared->rejections.log_events, (mask & 3) == 3 ? 0 : 1);
		UT_ASSERT_EQ(wb_shared->inbound_state, WB_REPLIED);
		UT_ASSERT(wb_notice == NULL);
		UT_ASSERT_EQ(wb_plan_builds + pi_discards + wb_master_completions, 0);
		UT_ASSERT(!wb_inputs_pinned[1]);
		wb_select(1, B_LMON);
		cluster_pi_writeback_lmon_tick_v1();
		UT_ASSERT_EQ(wb_destination, request.facts[0].proof.structural.ko.origin_node);
		UT_ASSERT(cluster_pi_writeback_decode_v2(wb_wire, wb_length, &reply));
		UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&request, &reply));
		UT_ASSERT_EQ(reply.count, (mask & 1) + ((mask >> 1) & 1));
		if (reply.count != 0)
			UT_ASSERT_EQ(
				memcmp(&reply.facts[0], &request.facts[(mask & 1) ? 0 : 1], sizeof(reply.facts[0])),
				0);
		wb_select(0, B_CHECKPOINTER);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(writeback_v2_relation_offer_reader_rejects_lost_original_notice)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterPageDataReceiptV1 *receipt = wb_setup();
		ClusterPiWritebackNoticeV1 notice = { 0 }, copy;
		ClusterPiDataFactV1 data;
		ClusterPiWritebackFactV2 out, before;
		uint64 revision = 83;
		UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
		wb_v2_offer_fixture(&notice.request_v2, &data, false);
		wb_select(1, B_BG_WRITER);
		notice.owner = CurrentResourceOwner;
		notice.pid = getpid();
		notice.version = 2;
		notice.revision = 71;
		wb_notice = &notice;
		wb_shared->inbound_revision = notice.revision;
		wb_shared->inbound_version = 2;
		wb_shared->inbound_pid = MyProcPid;
		wb_shared->inbound_state = WB_RUNNING;
		memset(&before, 0x5a, sizeof(before));
		out = before;
		copy = notice;
		switch (fault) {
		case 1:
			wb_notice = &copy;
			break;
		case 2:
			notice.pid++;
			break;
		case 3:
			notice.owner = (void *)3;
			break;
		case 4:
			notice.version = 1;
			break;
		case 5:
			MyBackendType = B_LMON;
			break;
		case 6:
			CritSectionCount++;
			break;
		case 7:
			wb_peer_caps = 0;
			break;
		case 8:
			wb_shared->inbound_revision++;
			break;
		case 9:
			wb_shared->inbound_pid++;
			break;
		case 10:
			wb_shared->inbound_state = WB_REPLIED;
			break;
		case 11:
			notice.revision = wb_shared->inbound_revision = 0;
			break;
		case 12:
			wb_ko_current = false;
			break;
		case 13:
			break; /* index out of range */
		}
		UT_ASSERT_EQ(cluster_pi_writeback_structure_offer_read_v2(&notice, fault == 13 ? 1 : 0,
																  &revision, &out),
					 fault == 0);
		UT_ASSERT_EQ(revision, fault == 0 ? 71 : 83);
		UT_ASSERT_EQ(memcmp(&out, fault == 0 ? &notice.request_v2.facts[0] : &before, sizeof(out)),
					 0);
		UT_ASSERT_EQ(wb_sends + wb_plan_builds + pi_discards, 0);
		wb_notice = NULL;
		CritSectionCount = 0;
		wb_select(0, B_CHECKPOINTER);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(writeback_v2_request_cut_checks_real_sender_and_recipient)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 m;
	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	for (unsigned shape = 0; shape < 4; shape++) {
		int sender = shape == 3 ? 2 : 0;
		if (shape == 3)
			wb_v2_offer_fixture(&m, &data, false);
		else if (shape == 2)
			wb_v2_origin_fixture(&m, &data);
		else {
			wb_v2_fixture(&m, &data);
			if (shape == 0) m.count = 1;
		}
		wb_select(sender, B_BG_WRITER);
		UT_ASSERT(cluster_pi_writeback_request_current_v2(&m, true));
		UT_ASSERT(!cluster_pi_writeback_request_current_v2(&m, false));
		wb_select(m.peer.claim.identity.origin_node_id, B_BG_WRITER);
		UT_ASSERT(cluster_pi_writeback_request_current_v2(&m, false));
		UT_ASSERT(!cluster_pi_writeback_request_current_v2(&m, true));
		UT_ASSERT_EQ(wb_sends + wb_plan_builds + pi_discards, 0);
	}
	wb_select(0, B_CHECKPOINTER);
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

/* Exercise the opaque reader itself; queue/transport activation is separate.
 * The fixture establishes the original singleton owner, never PAGE proof. */
UT_TEST(writeback_v2_structural_notice_requires_its_live_original_slot)
{
	for (unsigned fault = 0; fault < 19; fault++) {
		ClusterPageDataReceiptV1 *receipt = wb_setup();
		ClusterPiWritebackNoticeV1 notice = { 0 }, other = { 0 };
		ClusterPiDataFactV1 data;
		ClusterPiWritebackFactV2 out, before;
		uint64 revision = 83;
		UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
		wb_v2_fixture(&notice.request_v2, &data);
		wb_select(1, B_BG_WRITER);
		notice.owner = CurrentResourceOwner;
		notice.pid = getpid();
		notice.version = 2;
		notice.revision = 71;
		wb_notice = &notice;
		wb_shared->inbound_revision = notice.revision;
		wb_shared->inbound_version = 2;
		wb_shared->inbound_pid = MyProcPid;
		wb_shared->inbound_state = WB_RUNNING;
		memset(&out, 0x5a, sizeof(out));
		before = out;
		switch (fault) {
		case 1:
			wb_notice = NULL;
			break;
		case 2:
			notice.pid++;
			break;
		case 3:
			notice.owner = (void *)3;
			break;
		case 4:
			notice.version = 1;
			break;
		case 5:
			MyBackendType = B_LMON;
			break;
		case 6:
			CritSectionCount = 1;
			break;
		case 7:
			wb_peer_caps = 0;
			break;
		case 8:
			wb_shared->inbound_revision++;
			break;
		case 9:
			wb_shared->inbound_pid++;
			break;
		case 10:
			wb_shared->inbound_state = WB_REPLIED;
			break;
		case 11:
			notice.revision = wb_shared->inbound_revision = 0;
			break;
		case 12:
			notice.revision = wb_shared->inbound_revision = UINT64_MAX;
			break;
		case 13:
			break; /* unknown opaque handle */
		case 14:
			break; /* DATA kind cannot construct structural authority */
		case 15:
			break; /* absent index */
		case 16:
			wb_ko_current = false;
			break;
		case 17:
			wb_shared = NULL;
			break;
		case 18:
			break; /* missing revision output */
		}
		UT_ASSERT_EQ(cluster_pi_writeback_structural_notice_read_v2(fault == 13 ? &other : &notice,
																	fault == 14	  ? 0
																	: fault == 15 ? 2
																				  : 1,
																	fault == 18 ? NULL : &revision,
																	&out),
					 fault == 0);
		UT_ASSERT_EQ(revision, fault == 0 ? notice.revision : 83);
		UT_ASSERT(memcmp(&out, fault == 0 ? &notice.request_v2.facts[1] : &before, sizeof(out))
				  == 0);
		UT_ASSERT_EQ(wb_sends + wb_plan_builds + pi_discards, 0);
		wb_notice = NULL;
		CritSectionCount = 0;
		wb_select(0, B_CHECKPOINTER);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

static Size
wb_v2_request(ClusterPageDataReceiptV1 *receipt, ClusterPiWritebackMessageV2 *m,
			  uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2], bool structural)
{
	ClusterPiDataFactV1 fact;
	Size length = 0;
	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &fact));
	wb_v2_fixture(m, &fact);
	if (!structural)
		m->count = 1;
	UT_ASSERT(cluster_pi_writeback_encode_v2(m, bytes, CLUSTER_PI_WRITEBACK_MAX_BYTES_V2, &length));
	return length;
}

UT_TEST(writeback_v2_ingress_reaches_real_data_disposal_and_exact_reply)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiWritebackMessageV2 m, ack;
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
	Size length = wb_v2_request(receipt, &m, bytes, false);
	physical_pi_from_source(&wb_sources[1], 19);
	wb_deliver(0, 1, bytes, length);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(pi_discards, 1);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_REPLIED);
	UT_ASSERT_EQ(wb_input_releases, 1);
	wb_select(1, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_sends, 1);
	UT_ASSERT_EQ(wb_destination, 0);
	UT_ASSERT(cluster_pi_writeback_decode_v2(wb_wire, wb_length, &ack));
	UT_ASSERT_EQ(ack.count, 1);
	UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&m, &ack));
	/* The exact duplicate resends the original response without redoing PI. */
	wb_now += WB_RETRY_US;
	wb_deliver(0, 1, bytes, length);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_sends, 2);
	UT_ASSERT_EQ(pi_discards, 1);
	UT_ASSERT_EQ(wb_plan_builds, 1);
	UT_ASSERT_EQ(wb_master_completions, 0);
	wb_select(0, B_CHECKPOINTER);
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_notice_is_authenticated_before_structural_consumer)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiWritebackMessageV2 m, ack = { 0 };
	ClusterPiWritebackFactV2 fact = { 0 };
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
	uint64 revision = 0;
	Size length = wb_v2_request(receipt, &m, bytes, true);
	physical_pi_from_source(&wb_sources[1], 19);
	wb_deliver(0, 1, bytes, length);
	wb_input_wait = true;
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT(cluster_pi_writeback_structural_notice_read_v2(wb_notice, 1, &revision, &fact));
	UT_ASSERT(revision != 0);
	UT_ASSERT_EQ(memcmp(&fact, &m.facts[1], sizeof(fact)), 0);
	UT_ASSERT(!wb_inputs_pinned[1]);
	UT_ASSERT_EQ(pi_discards | wb_sends, 0);
	wb_input_wait = false;
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	/* The real PAGE ancestry accepts DATA; absent typed structural WAL can
	 * never manufacture a structural ACK from a valid wire representation. */
	UT_ASSERT_EQ(pi_discards, 1);
	UT_ASSERT_EQ(wb_shared->rejections.attempts[CLUSTER_PI_WRITEBACK_PEER_PHYSICAL], 1);
	wb_select(1, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT(cluster_pi_writeback_decode_v2(wb_wire, wb_length, &ack));
	UT_ASSERT_EQ(ack.count, 1);
	UT_ASSERT_EQ(ack.facts[0].kind, CLUSTER_PI_WRITEBACK_DATA_V2);
	UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&m, &ack));
	UT_ASSERT_EQ(wb_master_completions, 0);
	wb_select(0, B_CHECKPOINTER);
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

static bool
wb_v2_allocation_error_caught(void)
{
	volatile bool caught = false;
	wb_allocation_error = true;
	PG_TRY();
	{
		(void)cluster_pi_writeback_bgwriter_tick_v1();
	}
	PG_CATCH();
	{
		caught = true;
		wb_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	}
	PG_END_TRY();
	wb_allocation_error = false;
	return caught;
}

UT_TEST(writeback_v2_original_queue_survives_busy_drift_and_allocation_error)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterPageDataReceiptV1 *receipt = wb_setup();
		ClusterPiWritebackMessageV2 m;
		uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
		Size length = wb_v2_request(receipt, &m, bytes, false);
		uint64 revision;
		physical_pi_from_source(&wb_sources[1], 19);
		wb_deliver(0, 1, bytes, length);
		revision = wb_shared->inbound_revision;
		wb_select(1, B_BG_WRITER);
		if (fault == 0) {
			UT_ASSERT(wb_v2_allocation_error_caught());
			UT_ASSERT(wb_notice == NULL);
			UT_ASSERT_EQ(wb_shared->inbound_state, WB_QUEUED);
		}
		wb_input_wait = true;
		UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
		m.nonce++;
		UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
		wb_deliver(0, 1, bytes, length);
		UT_ASSERT_EQ(wb_shared->inbound_revision, revision);
		UT_ASSERT_EQ(wb_shared->inbound_state, WB_RUNNING);
		wb_select(1, B_BG_WRITER);
		wb_input_wait = false;
		if (fault == 1)
			wb_peer_caps = 0;
		if (fault == 2)
			wb_stop = true;
		if (fault == 3)
			wb_input_current = false;
		UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
		UT_ASSERT_EQ(pi_discards, fault == 0 ? 1 : 0);
		UT_ASSERT_EQ(wb_shared->inbound_state, fault == 0 ? WB_REPLIED : WB_EMPTY);
		UT_ASSERT(wb_notice == NULL);
		UT_ASSERT(!wb_inputs_pinned[1]);
		UT_ASSERT_EQ(wb_input_releases, 1);
		wb_allocation_error = false;
		wb_select(0, B_CHECKPOINTER);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(writeback_v2_ingress_refuses_old_wire_unready_peers_and_relation_offers)
{
	for (unsigned fault = 0; fault < 10; ++fault) {
		ClusterPageDataReceiptV1 *receipt = wb_setup();
		ClusterPiWritebackMessageV2 m;
		ClusterICEnvelope env = { 0 };
		uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
		Size length = wb_v2_request(receipt, &m, bytes, false);
		env.msg_type = PGRAC_IC_MSG_PI_WRITEBACK;
		env.source_node_id = 0;
		env.dest_node_id = 1;
		env.epoch = wb_epoch;
		env.payload_length = length;
		wb_select(1, B_LMON);
		switch (fault) {
		case 0:
			wb_local_caps = 0;
			break;
		case 1:
			wb_peer_caps = 0;
			break;
		case 2:
			wb_cap_generation = 0;
			break;
		case 3:
			env.source_node_id = 2;
			break;
		case 4:
			env.epoch++;
			break;
		case 5:
			env.dest_node_id = 0;
			break;
		case 6:
			m.verb = CLUSTER_PI_WRITEBACK_ACK;
			UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
			break;
		case 7: {
			ClusterPiDataFactV1 fact = m.facts[0].proof.data;
			wb_v2_offer_fixture(&m, &fact, false);
			env.source_node_id = m.facts[0].proof.structural.ko.origin_node + 1;
			UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
			env.payload_length = length;
			break;
		}
		case 8: {
			ClusterPiWritebackMessageV1 old = { 0 };
			old.verb = m.verb;
			old.count = 1;
			old.nonce = m.nonce;
			old.epoch = m.epoch;
			old.peer = m.peer;
			old.facts[0] = m.facts[0].proof.data;
			UT_ASSERT(cluster_pi_writeback_encode_v1(&old, bytes, sizeof(bytes), &length));
			env.payload_length = length;
			break;
		}
		case 9:
			env.payload_length--;
			break;
		}
		cluster_pi_writeback_ingress_v1(&env, bytes);
		UT_ASSERT_EQ(wb_shared->inbound_state, WB_EMPTY);
		UT_ASSERT_EQ(wb_shared->inbound_revision, 0);
		UT_ASSERT_EQ(wb_plan_builds | wb_sends | pi_discards, 0);
		wb_select(0, B_CHECKPOINTER);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(writeback_v2_reply_rechecks_identity_after_physical_completion)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterPageDataReceiptV1 *receipt = wb_setup();
		ClusterPiWritebackMessageV2 m;
		uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
		Size length = wb_v2_request(receipt, &m, bytes, false);
		physical_pi_from_source(&wb_sources[1], 19);
		wb_deliver(0, 1, bytes, length);
		wb_select(1, B_BG_WRITER);
		UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
		UT_ASSERT_EQ(pi_discards, 1);
		UT_ASSERT_EQ(wb_shared->inbound_state, WB_REPLIED);
		wb_select(1, B_LMON);
		if (fault == 0)
			wb_peer_caps = 0;
		if (fault == 1)
			wb_boots[0]++;
		if (fault == 2)
			wb_epoch++;
		if (fault == 3)
			wb_shared->reply_v2.facts[0].proof.data.storage_cut.authority.transition_count++;
		cluster_pi_writeback_lmon_tick_v1();
		UT_ASSERT_EQ(wb_sends | wb_master_completions, 0);
		UT_ASSERT(wb_shared->reply_pending);
		UT_ASSERT(!wb_inputs_pinned[1]);
		wb_select(0, B_CHECKPOINTER);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
}

UT_TEST(writeback_v2_ack_preserves_selected_claim_ceiling)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiWritebackMessageV2 m, ack = { 0 };
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2];
	Size length = wb_v2_request(receipt, &m, bytes, false);
	m.peer.claim.max_config_generation++;
	UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
	physical_pi_from_source(&wb_sources[1], 19);
	wb_deliver(0, 1, bytes, length);
	wb_select(1, B_BG_WRITER);
	UT_ASSERT(cluster_pi_writeback_bgwriter_tick_v1());
	UT_ASSERT_EQ(pi_discards, 1);
	UT_ASSERT_EQ(wb_shared->inbound_state, WB_REPLIED);
	wb_select(1, B_LMON);
	cluster_pi_writeback_lmon_tick_v1();
	UT_ASSERT_EQ(wb_sends, 1);
	UT_ASSERT(cluster_pi_writeback_decode_v2(wb_wire, wb_length, &ack));
	UT_ASSERT_EQ(ack.count, 1);
	UT_ASSERT_EQ(memcmp(&ack.peer, &m.peer, sizeof(m.peer)), 0);
	UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&m, &ack));
	wb_select(0, B_CHECKPOINTER);
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_request_cut_refuses_missing_or_changed_proofs)
{
	for (unsigned fault = 0; fault < 20; fault++) {
		ClusterPageDataReceiptV1 *receipt = wb_setup();
		ClusterPiDataFactV1 data;
		ClusterPiWritebackMessageV2 m, before;
		UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
		wb_v2_fixture(&m, &data);
		switch (fault) {
		case 0: wb_local_caps = 0; break;
		case 1: wb_peer_caps = 0; break;
		case 2: wb_cap_available = false; break;
		case 3: wb_cap_generation = 0; break;
		case 4: wb_ko_current = false; break;
		case 5: wb_cap_drift = 1; break;
		case 6: wb_cap_drift = 2; break;
		case 7: wb_cap_drift = 3; break;
		case 8: wb_route = 2; break;
		case 9: wb_stop = true; break;
		case 10: wb_quorum = false; break;
		case 11: m.epoch++; break;
		case 12: wb_boots[1]++; break;
		case 13: wb_recovered_sources = 2; break;
		case 14: writer.claim.database_incarnation++; break;
		case 15: m.verb = CLUSTER_PI_WRITEBACK_ACK; m.count = 0; break;
		case 16: wb_membership_generation++; break;
		case 17: wb_select(2, B_BG_WRITER); break;
		case 18: m.peer.claim.identity.origin_owner_incarnation++; break;
		case 19: selected = false; break;
		}
		before = m;
		UT_ASSERT(!cluster_pi_writeback_request_current_v2(&m, true));
		UT_ASSERT(memcmp(&before, &m, sizeof(m)) == 0);
		UT_ASSERT_EQ(wb_sends + wb_plan_builds + pi_discards, 0);
		selected = true;
		wb_select(0, B_CHECKPOINTER);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&wb_page_plan);
		clean();
	}
	UT_ASSERT(!cluster_pi_writeback_request_current_v2(NULL, true));
}

UT_TEST(writeback_v2_relation_offer_has_no_page_master_authority)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 m, decoded = {0};
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2 + 1] = {0}, structure[656], ko[160];
	Size length = 0;
	static const ClusterPcmPiWriteCutV1 no_x;
	static const ClusterPcmPiStorageCutV1 no_storage;

	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	for (unsigned drop = 0; drop < 2; drop++) {
		ClusterPiStructuralFactV2 *s;
		wb_v2_offer_fixture(&m, &data, drop);
		s = &m.facts[0].proof.structural;
		UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes + 1, sizeof(bytes) - 1, &length));
		UT_ASSERT_EQ(length, 176 + 1064);
		UT_ASSERT(memcmp(bytes + 1 + 176, "\003\000\000\000\050\004\000\000", 8) == 0);
		UT_ASSERT(cluster_space_structure_wal_encode(&s->change, structure, sizeof(structure)));
		UT_ASSERT(cluster_ko_shared_encode_v2(&s->ko, ko, sizeof(ko)));
		UT_ASSERT(memcmp(bytes + 1 + 176 + 248, structure, sizeof(structure)) == 0);
		UT_ASSERT(memcmp(bytes + 1 + 176 + 904, ko, sizeof(ko)) == 0);
		UT_ASSERT(cluster_pi_writeback_decode_v2(bytes + 1, length, &decoded));
		UT_ASSERT_EQ(decoded.facts[0].kind, CLUSTER_PI_WRITEBACK_STRUCTURE_OFFER_V2);
		UT_ASSERT_EQ(decoded.facts[0].proof.structural.durability_flags, drop ? 15 : 31);
		UT_ASSERT(memcmp(&decoded.facts[0].proof.structural.terminal.write_cut, &no_x, sizeof(no_x)) == 0);
		UT_ASSERT(memcmp(&decoded.facts[0].proof.structural.terminal.storage_cut, &no_storage, sizeof(no_storage)) == 0);
		/* An offer cannot be relabelled as either existing page proof. */
		bytes[1 + 176] = CLUSTER_PI_WRITEBACK_STRUCTURAL_V2;
		UT_ASSERT(!cluster_pi_writeback_decode_v2(bytes + 1, length, &decoded));
	}
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_relation_offer_rejects_misrouting_mixed_or_master_cuts)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 m, decoded, sentinel;
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2], unchanged[sizeof(bytes)];
	Size length;

	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	memset(unchanged, 0xa5, sizeof(unchanged));
	for (unsigned fault = 0; fault < 13; fault++) {
		ClusterPiStructuralFactV2 *s;
		wb_v2_offer_fixture(&m, &data, false);
		s = &m.facts[0].proof.structural;
		switch (fault) {
		case 0: s->terminal.storage_cut = data.storage_cut; break;
		case 1: s->terminal.write_cut.binding_generation = 1; break;
		case 2: s->ko.peer_node = 0; break;
		case 3: s->ko.peer_boot++; break;
		case 4: s->ko.origin_node = 0; break;
		case 5: s->ko.origin_boot++; break;
		case 6: m.peer = wb_sources[2]; break;
		case 7: m.facts[1] = m.facts[0]; m.count = 2; break;
		case 8: m.facts[1].kind = CLUSTER_PI_WRITEBACK_DATA_V2; m.facts[1].proof.data = data; m.count = 2; break;
		case 9: m.epoch++; break;
		case 10: s->durability_flags &= ~CLUSTER_PI_STRUCTURAL_EFFECT_DURABLE; break;
		case 11: s->terminal.binding.flags = 0; break;
		case 12: m.facts[0].kind = CLUSTER_PI_WRITEBACK_STRUCTURAL_V2; break;
		}
		memcpy(bytes, unchanged, sizeof(bytes));
		length = 91;
		UT_ASSERT(!cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
		UT_ASSERT_EQ(length, 91);
		UT_ASSERT(memcmp(bytes, unchanged, sizeof(bytes)) == 0);
	}
	wb_v2_offer_fixture(&m, &data, false);
	UT_ASSERT(cluster_pi_writeback_encode_v2(&m, bytes, sizeof(bytes), &length));
	memset(&sentinel, 0xa5, sizeof(sentinel));
	for (Size truncated = 0; truncated < length; truncated++) {
		decoded = sentinel;
		UT_ASSERT(!cluster_pi_writeback_decode_v2(bytes, truncated, &decoded));
		UT_ASSERT(memcmp(&decoded, &sentinel, sizeof(decoded)) == 0);
	}
	/* Explicit kind, exact length, reserved fields and old protocol reject. */
	for (unsigned fault = 0; fault < 6; fault++) {
		uint8 changed[sizeof(bytes)];
		memcpy(changed, bytes, length);
		if (fault == 0) changed[4] = 1;
		if (fault == 1) changed[176] = 4;
		if (fault == 2) changed[176 + 2] = 1;
		if (fault == 3) changed[176 + 4]++;
		if (fault == 4) changed[176 + 12] = 1;
		if (fault == 5) changed[176 + 904 + 110] = 0;
		decoded = sentinel;
		UT_ASSERT(!cluster_pi_writeback_decode_v2(changed, length, &decoded));
		UT_ASSERT(memcmp(&decoded, &sentinel, sizeof(decoded)) == 0);
	}
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_relation_offer_ack_cannot_ack_a_page_fact)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 offer, ack, pages;

	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	wb_v2_offer_fixture(&offer, &data, false);
	ack = offer;
	ack.verb = CLUSTER_PI_WRITEBACK_ACK;
	UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&offer, &ack));
	ack.facts[0].proof.structural.ko.batch_id++;
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&offer, &ack));
	wb_v2_fixture(&pages, &data);
	pages.facts[0] = pages.facts[1];
	pages.count = 1;
	ack = pages;
	ack.verb = CLUSTER_PI_WRITEBACK_ACK;
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&offer, &ack));
	ack = offer;
	ack.verb = CLUSTER_PI_WRITEBACK_ACK;
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&pages, &ack));
	ack.count = 0;
	UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&offer, &ack));
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}

UT_TEST(writeback_v2_relation_offer_batch_is_bounded_and_ack_ordered)
{
	ClusterPageDataReceiptV1 *receipt = wb_setup();
	ClusterPiDataFactV1 data;
	ClusterPiWritebackMessageV2 offer, ack, decoded;
	uint8 bytes[CLUSTER_PI_WRITEBACK_MAX_BYTES_V2] = {0};
	Size length = 0, unchanged;

	UT_ASSERT(cluster_page_data_pi_fact_v1(receipt, &data));
	wb_v2_offer_fixture(&offer, &data, false);
	offer.count = CLUSTER_PI_WRITEBACK_MAX;
	for (uint32 i = 0; i < offer.count; i++) {
		ClusterPiStructuralFactV2 *s = &offer.facts[i].proof.structural;
		RelFileLocator locator;
		offer.facts[i] = offer.facts[0];
		locator = s->terminal.binding.identity.locator;
		locator.relNumber += i;
		s->terminal.binding.identity.locator = locator;
		s->change.identity.expected.key.locator = locator;
		s->change.identity.result.key.locator = locator;
		s->change.reservation.before.identity.key.locator = locator;
		s->change.reservation.result.identity.key.locator = locator;
		s->ko.key.locator = locator;
	}
	UT_ASSERT(cluster_pi_writeback_encode_v2(&offer, bytes, sizeof(bytes), &length));
	UT_ASSERT_EQ(length, 176 + 16 * 1064);
	UT_ASSERT(cluster_pi_writeback_decode_v2(bytes, length, &decoded));
	UT_ASSERT_EQ(decoded.count, 16);
	unchanged = length;
	UT_ASSERT(!cluster_pi_writeback_encode_v2(&offer, bytes, length - 1, &unchanged));
	UT_ASSERT_EQ(unchanged, length);
	UT_ASSERT(!cluster_pi_writeback_decode_v2(bytes, length + 1, &decoded));
	ack = offer;
	ack.verb = CLUSTER_PI_WRITEBACK_ACK;
	ack.count = 3;
	ack.facts[0] = offer.facts[1];
	ack.facts[1] = offer.facts[7];
	ack.facts[2] = offer.facts[15];
	UT_ASSERT(cluster_pi_writeback_ack_matches_v2(&offer, &ack));
	ack.facts[0] = offer.facts[8];
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&offer, &ack));
	ack.facts[0] = offer.facts[7];
	UT_ASSERT(!cluster_pi_writeback_ack_matches_v2(&offer, &ack));
	offer.facts[15].proof.structural.terminal.binding.source = wb_sources[0];
	offer.facts[15].proof.structural.ko.origin_node = 0;
	offer.facts[15].proof.structural.ko.origin_boot = wb_sources[0].claim.identity.origin_owner_incarnation;
	UT_ASSERT(!cluster_pi_writeback_encode_v2(&offer, bytes, sizeof(bytes), &length));
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&wb_page_plan);
	clean();
}
