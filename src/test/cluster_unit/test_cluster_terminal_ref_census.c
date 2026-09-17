/*-------------------------------------------------------------------------
 *
 * test_cluster_terminal_ref_census.c
 *	  Focused tests for canonical terminal-reference census and release.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_terminal_ref_census.c
 *
 * NOTES
 *	  This is a pgrac-original file.  It exercises the dependency-light
 *	  CTRC state engine with literal identities and deterministic event
 *	  order.  See spec-8.4d-current-mx-transaction-authority-state-matrix.md.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <ctype.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "port/pg_crc32c.h"

#undef printf
#undef fprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	return la < lb ? -1 : la > lb ? 1 : 0;
}

static bool source_file_contains(const char *relative_path, const char *needle);
static int source_file_occurrences(const char *relative_path, const char *needle);

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

/* On ARM64, libpgport's CRC runtime selector contains DEBUG-level ereport
 * probes.  The focused state-machine test needs the CRC implementation but
 * never exercises backend error reporting, so provide the usual unit stubs. */
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

#ifndef CTRC_SOURCE_MANIFEST_PATH
#error "CTRC_SOURCE_MANIFEST_PATH must identify the CTRC source census"
#endif
#ifndef HEAP_REFERENCE_PRODUCER_MANIFEST_PATH
#error "HEAP_REFERENCE_PRODUCER_MANIFEST_PATH must identify the heap producer census"
#endif
#ifndef CURRENT_MX_CARDINALITY_MANIFEST_PATH
#error "CURRENT_MX_CARDINALITY_MANIFEST_PATH must identify the current-MX cardinality census"
#endif
#ifndef PGRAC_SOURCE_ROOT_PATH
#error "PGRAC_SOURCE_ROOT_PATH must identify the source tree"
#endif

#define TEST_GRANT UINT32_C(7)
#define TEST_SEAL UINT64_C(41)
#define TEST_BOOT UINT64_C(1101)
#define TEST_CAPABILITY UINT32_C(19)
#define TEST_FORMATION UINT64_C(23)
#define TEST_ADMISSION UINT64_C(29)

static ClusterCtrcTxnKeyV1
test_key(void)
{
	ClusterCtrcTxnKeyV1 key;

	MemSet(&key, 0, sizeof(key));
	key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key.owner_instance = 1;
	key.origin_node_id = 0;
	key.segment_id = 17;
	key.segment_generation = 5;
	key.slot_offset = 3;
	key.slot_wrap = 9;
	key.xid = 7001;
	key.cluster_epoch = 13;
	key.system_identifier = UINT64_C(0x1122334455667788);
	key.origin_boot_incarnation = TEST_BOOT;
	key.formation_epoch = TEST_FORMATION;
	key.admission_record_generation = TEST_ADMISSION;
	key.root_descriptor_incarnation = 31;
	key.root_id = 37;
	key.root_generation = 43;
	return key;
}

static ClusterCtrcParticipantIdentity
test_participant_identity(uint16 node_id)
{
	ClusterCtrcParticipantIdentity identity;

	MemSet(&identity, 0, sizeof(identity));
	identity.node_id = node_id;
	identity.boot_incarnation = TEST_BOOT + node_id;
	identity.capability_record_generation = TEST_CAPABILITY;
	identity.formation_epoch = TEST_FORMATION;
	identity.admission_record_generation = TEST_ADMISSION;
	return identity;
}

static ClusterCtrcPublicationIdV1
test_publication(uint64 operation_id, ClusterCtrcReferenceKind reference_kind,
				 ClusterCtrcTargetKind target_kind)
{
	ClusterCtrcPublicationIdV1 publication;

	MemSet(&publication, 0, sizeof(publication));
	publication.requester_node_id = 2;
	publication.requester_boot_incarnation = TEST_BOOT + 2;
	publication.capability_record_generation = TEST_CAPABILITY;
	publication.requester_backend_id = 11;
	publication.wire_request_id = 101;
	publication.operation_id = operation_id;
	publication.attempt_generation = 1;
	if (reference_kind == CTRC_REF_HEAP_ITL_UBA) {
		publication.descriptor_hash = 0;
		publication.member_ordinal = UINT16_MAX;
		publication.member_role = 0;
	} else {
		publication.descriptor_hash = UINT64_C(0x9988776655443322);
		publication.member_ordinal = 0;
		publication.member_role = 1;
	}
	publication.reference_kind = reference_kind;
	publication.target_kind = target_kind;
	publication.grant_generation = TEST_GRANT;
	return publication;
}

static ClusterCtrcTargetV1
test_pending_itl_target(void)
{
	ClusterCtrcTargetV1 target;

	MemSet(&target, 0, sizeof(target));
	target.kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	target.spc_oid = 1663;
	target.db_oid = 5;
	target.rel_number = 9001;
	target.fork_number = 0;
	target.block_number = 44;
	/* A newly initialized heap page legitimately has no predecessor LSN/SCN;
	 * exact zero is data, not an absent target. */
	target.predecessor_page_lsn = 0;
	target.predecessor_page_lsn_origin_node_id = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
	target.predecessor_page_scn = 0;
	target.publication_own_generation = 17;
	target.publication_acquisition_epoch = 19;
	target.relation_persistence = 'p';
	target.needs_wal = true;
	target.page_operation_kind = 1;
	return target;
}

static ClusterCtrcTargetV1
test_exact_itl_target(void)
{
	ClusterCtrcTargetV1 target = test_pending_itl_target();

	target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	target.itl_slot_index = 2;
	/* Heap ITL wrap is independent of the canonical TT slot wrap. */
	target.itl_slot_wrap = 3;
	target.itl_xid = 7001;
	target.itl_class = 1;
	MemSet(target.uba, 0x5a, sizeof(target.uba));
	MemSet(target.planned_predecessor_sha256, 0x11, sizeof(target.planned_predecessor_sha256));
	MemSet(target.planned_successor_sha256, 0x22, sizeof(target.planned_successor_sha256));
	return target;
}

static ClusterCtrcTargetV1
test_pending_offnum_target(uint64 descriptor_hash)
{
	ClusterCtrcTargetV1 target = test_pending_itl_target();

	target.kind = CTRC_TARGET_PAGE_PENDING_OFFNUM;
	target.page_operation_kind = 0;
	target.intended_descriptor_hash = descriptor_hash;
	return target;
}

static ClusterCtrcTargetV1
test_exact_tid_target(uint64 descriptor_hash)
{
	ClusterCtrcTargetV1 target = test_pending_offnum_target(descriptor_hash);

	target.kind = CTRC_TARGET_EXACT_TID;
	target.offset_number = 4;
	target.itemid_flags = 1;
	target.itemid_offset = 128;
	target.itemid_length = 40;
	MemSet(target.tuple_header_sha256, 0x33, sizeof(target.tuple_header_sha256));
	target.mx_origin_node_id = 1;
	target.multixact_id = 9001;
	target.mx_cluster_epoch = 13;
	target.descriptor_hash = descriptor_hash;
	target.intended_descriptor_hash = 0;
	return target;
}

static void
test_put_u32_le(uint8 *bytes, uint32 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
	bytes[2] = (uint8)(value >> 16);
	bytes[3] = (uint8)(value >> 24);
}

static void
test_reseal_crc32c(uint8 *bytes, Size length, Size crc_offset)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, crc_offset);
	FIN_CRC32C(crc);
	test_put_u32_le(bytes + crc_offset, (uint32)crc);
	UT_ASSERT_EQ(crc_offset + sizeof(uint32), length);
}

static ClusterCtrcLocalReleaseAckV1
test_zero_range_ack_for_node(uint16 node_id)
{
	ClusterCtrcLocalReleaseAckV1 ack;
	ClusterCtrcLocalReleaseAckV1 decoded;
	uint8 bytes[CLUSTER_CTRC_LOCAL_ACK_BYTES];

	MemSet(&ack, 0, sizeof(ack));
	ack.transaction_key = test_key();
	ack.grant_generation = TEST_GRANT;
	ack.result = CTRC_ACK_RELEASED;
	ack.flags = CTRC_ACK_FLAG_ZERO_RANGE | CTRC_ACK_FLAG_ALL_DURABLE;
	ack.seal_generation = TEST_SEAL;
	ack.participant_node_id = node_id;
	ack.dependency_entry_count = CLUSTER_SF_DEP_MAX_ORIGINS;
	ack.capability_record_generation = TEST_CAPABILITY;
	ack.participant_boot_incarnation = TEST_BOOT + node_id;
	ack.formation_epoch = TEST_FORMATION;
	ack.admission_record_generation = TEST_ADMISSION;
	memcpy(ack.row_digest_sha256, cluster_ctrc_empty_sha256, sizeof(ack.row_digest_sha256));
	if (!cluster_ctrc_local_release_ack_encode(&ack, bytes)
		|| !cluster_ctrc_local_release_ack_decode(bytes, &decoded))
		abort();
	return decoded;
}

static ClusterCtrcLocalReleaseAckV1
test_zero_range_ack(void)
{
	return test_zero_range_ack_for_node(2);
}

static void
test_open_origin(ClusterCtrcOriginEntry *origin)
{
	ClusterCtrcTxnKeyV1 key = test_key();

	MemSet(origin, 0, sizeof(*origin));
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(origin, &key, TEST_GRANT),
				 CLUSTER_CTRC_ORIGIN_OPENED);
}

/* MXA L04/L05: the pending origin reservation is owned by one exact
 * generation.  Before BIND it may return to the byte-zero FREE image; after
 * BIND it may only retain as BLOCKED.  A stale or foreign handle must never
 * clear a newer occupant. */
UT_TEST(test_ctrc_origin_reservation_prebind_cancel_and_postbind_block_are_exact)
{
	ClusterCtrcOriginEntry origin;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcOriginReservation stale;
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcTxnKeyV1 next_key = key;
	ClusterCtrcOriginEntry zero = { 0 };

	MemSet(&origin, 0, sizeof(origin));
	MemSet(&reservation, 0, sizeof(reservation));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(&origin, &key, 3, 11, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT_EQ(reservation.kind, CTRC_ORIGIN_RESERVATION_PENDING_OWNED);
	UT_ASSERT_EQ(reservation.reservation_generation, 11);
	stale = reservation;
	stale.reservation_generation++;
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 3, &stale));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_EMPTY);
	UT_ASSERT_EQ(origin.reservation_generation, 11);
	UT_ASSERT_EQ(memcmp(&origin.key, &key, sizeof(key)), 0);
	UT_ASSERT(cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 3, &reservation));
	UT_ASSERT_EQ(memcmp(&origin, &zero, sizeof(origin)), 0);

	/* A conflicting physical-slot incarnation can reserve after exact cancel. */
	next_key.xid++;
	next_key.slot_wrap++;
	MemSet(&reservation, 0, sizeof(reservation));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(&origin, &next_key, 3, 12, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(cluster_ctrc_origin_block_post_bind_entry(&origin, 3, &reservation));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_BLOCKED);
	UT_ASSERT_EQ(origin.reservation_generation, 12);
	UT_ASSERT_EQ(memcmp(&origin.key, &next_key, sizeof(next_key)), 0);
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 3, &reservation));
	UT_ASSERT(cluster_ctrc_origin_block_post_bind_entry(&origin, 3, &reservation));
}

UT_TEST(test_ctrc_origin_reservation_open_duplicate_and_aba_cleanup_do_not_mutate)
{
	ClusterCtrcOriginEntry origin;
	ClusterCtrcOriginEntry before;
	ClusterCtrcOriginReservation pending;
	ClusterCtrcOriginReservation opened;
	ClusterCtrcOriginReservation foreign;
	ClusterCtrcTxnKeyV1 key = test_key();

	MemSet(&origin, 0, sizeof(origin));
	MemSet(&pending, 0, sizeof(pending));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(&origin, &key, 7, 41, &pending),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&origin, &key, TEST_GRANT),
				 CLUSTER_CTRC_ORIGIN_OPENED);
	MemSet(&opened, 0, sizeof(opened));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(&origin, &key, 7, 99, &opened),
				 CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN);
	UT_ASSERT_EQ(opened.kind, CTRC_ORIGIN_RESERVATION_EXISTING_OPEN);
	UT_ASSERT_EQ(opened.reservation_generation, 41);
	before = origin;
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 7, &opened));
	UT_ASSERT(!cluster_ctrc_origin_block_post_bind_entry(&origin, 7, &opened));
	UT_ASSERT_EQ(memcmp(&origin, &before, sizeof(origin)), 0);

	foreign = pending;
	foreign.origin_index++;
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 7, &foreign));
	UT_ASSERT(!cluster_ctrc_origin_block_post_bind_entry(&origin, 7, &foreign));
	UT_ASSERT_EQ(memcmp(&origin, &before, sizeof(origin)), 0);
	foreign = pending;
	foreign.key.xid++;
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 7, &foreign));
	UT_ASSERT(!cluster_ctrc_origin_block_post_bind_entry(&origin, 7, &foreign));
	UT_ASSERT_EQ(memcmp(&origin, &before, sizeof(origin)), 0);
}

/* A durable release already authorizes physical TT-slot reuse, while the old
 * origin row may still be waiting only for participant-summary reclamation.
 * A newer incarnation at the same physical index must wait without poisoning
 * that RELEASE_PROVEN continuation; otherwise its notification can never
 * finish and every later incarnation is permanently refused. */
UT_TEST(test_ctrc_released_origin_conflict_is_retryable_without_state_loss)
{
	ClusterCtrcOriginEntry origin;
	ClusterCtrcOriginEntry before;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcTxnKeyV1 successor = key;

	MemSet(&origin, 0, sizeof(origin));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(&origin, &key, 7, 41, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&origin, &key, TEST_GRANT),
				 CLUSTER_CTRC_ORIGIN_OPENED);
	origin.state = CTRC_ORIGIN_RELEASE_PROVEN;
	origin.seal_generation = TEST_SEAL;
	before = origin;
	successor.xid++;
	successor.slot_wrap++;
	MemSet(&reservation, 0, sizeof(reservation));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(&origin, &successor, 7, 42, &reservation),
				 CLUSTER_CTRC_ORIGIN_RESERVE_RETRY_RELEASED);
	UT_ASSERT(!reservation.valid);
	UT_ASSERT_EQ(memcmp(&origin, &before, sizeof(origin)), 0);
}

static void
test_open_participant(ClusterCtrcParticipantEntry *participant)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);

	MemSet(participant, 0, sizeof(*participant));
	UT_ASSERT_EQ(cluster_ctrc_participant_open(participant, &key, TEST_GRANT, &identity),
				 CLUSTER_CTRC_PARTICIPANT_OPENED);
}

/* MXA-T20: removing the touched-record call must make a positive grant
 * impossible.  Terminal and unknown decisions never inherit the old grant. */
UT_TEST(test_ctrc_active_grant_records_touched_node_before_positive_proof)
{
	ClusterCtrcOriginEntry origin;
	ClusterCtrcOriginEntry before_terminal;
	ClusterCtrcOriginEntry conflicting;
	ClusterCtrcParticipantIdentity participant = test_participant_identity(2);
	ClusterCtrcParticipantIdentity drifted = participant;
	ClusterCtrcTxnKeyV1 key = test_key();
	uint32 grant = UINT32_MAX;

	test_open_origin(&origin);
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&origin, &key, TEST_GRANT),
				 CLUSTER_CTRC_ORIGIN_DUPLICATE);
	conflicting = origin;
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&conflicting, &key, TEST_GRANT + 1),
				 CLUSTER_CTRC_ORIGIN_REFUSED);
	UT_ASSERT_EQ(conflicting.state, CTRC_ORIGIN_BLOCKED);
	UT_ASSERT_EQ(
		cluster_ctrc_origin_record_touched(&origin, &participant, CTRC_PROOF_ACTIVE, &grant),
		CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(grant, TEST_GRANT);
	UT_ASSERT(cluster_ctrc_origin_has_exact_touch(&origin, &participant));
	grant = UINT32_MAX;
	UT_ASSERT_EQ(
		cluster_ctrc_origin_record_touched(&origin, &participant, CTRC_PROOF_ACTIVE, &grant),
		CLUSTER_CTRC_TOUCH_DUPLICATE);
	UT_ASSERT_EQ(grant, TEST_GRANT);

	drifted.boot_incarnation++;
	grant = UINT32_MAX;
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &drifted, CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_REFUSED);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_BLOCKED);
	UT_ASSERT(!cluster_ctrc_origin_has_exact_touch(&origin, &drifted));

	before_terminal = origin;
	grant = UINT32_MAX;
	UT_ASSERT_EQ(
		cluster_ctrc_origin_record_touched(&origin, &participant, CTRC_PROOF_COMMITTED, &grant),
		CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT(memcmp(&origin, &before_terminal, sizeof(origin)) == 0);
	grant = UINT32_MAX;
	UT_ASSERT_EQ(
		cluster_ctrc_origin_record_touched(&origin, &participant, CTRC_PROOF_UNKNOWN, &grant),
		CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT(memcmp(&origin, &before_terminal, sizeof(origin)) == 0);
}

/* A clean four-node formation has exact epoch zero.  Presence comes from the
 * complete typed identity, not from inventing epoch one or treating zero as
 * an absent key. */
UT_TEST(test_ctrc_epoch_zero_identity_is_present_and_exact)
{
	ClusterCtrcOriginEntry origin;
	ClusterCtrcParticipantEntry participant_entry;
	ClusterCtrcParticipantIdentity participant = test_participant_identity(2);
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcTxnKeyV1 drifted;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(23, CTRC_REF_RECOMPOSED_SURVIVOR, CTRC_TARGET_PAGE_PENDING_OFFNUM);
	ClusterCtrcTargetV1 pending = test_pending_offnum_target(publication.descriptor_hash);
	ClusterCtrcTargetV1 exact = test_exact_tid_target(publication.descriptor_hash);
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;
	uint32 grant = 0;

	key.cluster_epoch = 0;
	key.formation_epoch = 0;
	key.segment_generation = 0;
	participant.formation_epoch = 0;
	MemSet(&origin, 0, sizeof(origin));
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&origin, &key, TEST_GRANT),
				 CLUSTER_CTRC_ORIGIN_OPENED);
	UT_ASSERT_EQ(
		cluster_ctrc_origin_record_touched(&origin, &participant, CTRC_PROOF_ACTIVE, &grant),
		CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(grant, TEST_GRANT);
	UT_ASSERT(cluster_ctrc_origin_grant_publishable_entry(&origin, &key, &participant, grant));

	drifted = key;
	drifted.cluster_epoch = 1;
	UT_ASSERT(!cluster_ctrc_origin_grant_publishable_entry(&origin, &drifted, &participant, grant));
	drifted = key;
	drifted.formation_epoch = 1;
	UT_ASSERT(!cluster_ctrc_origin_grant_publishable_entry(&origin, &drifted, &participant, grant));

	MemSet(&participant_entry, 0, sizeof(participant_entry));
	UT_ASSERT_EQ(cluster_ctrc_participant_open(&participant_entry, &key, TEST_GRANT, &participant),
				 CLUSTER_CTRC_PARTICIPANT_OPENED);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant_entry, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	pending.publication_acquisition_epoch = 0;
	exact.publication_acquisition_epoch = 0;
	MemSet(&receipt, 0, sizeof(receipt));
	publication.operation_id++;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant_entry, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	exact.mx_cluster_epoch = 0;
	MemSet(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant_entry, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);

	/* L11 and L12 consume the same exact epoch identity.  Zero is legal only
	 * for the established clean four-node formation, never an absent-value
	 * shortcut at any of the three recycle entry points. */
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "(expected_epoch == 0 && cluster_conf_node_count() != 4)"));
	UT_ASSERT(source_file_contains("src/backend/cluster/storage/cluster_undo_alloc.c",
								   "(expected_epoch == 0 && cluster_conf_node_count() != 4)"));
	UT_ASSERT(source_file_contains("src/backend/cluster/storage/cluster_undo_block0_current.c",
								   "(expected_epoch == 0 && cluster_conf_node_count() != 4)"));
}

/* MXA-T15: a proof delayed beyond OPEN must never publish the old grant. */
UT_TEST(test_ctrc_delayed_positive_proof_revalidates_open_grant)
{
	ClusterCtrcOriginEntry origin;
	ClusterCtrcParticipantIdentity participant = test_participant_identity(2);
	ClusterCtrcParticipantIdentity drifted = participant;
	ClusterCtrcTxnKeyV1 key = test_key();
	uint32 grant = 0;

	test_open_origin(&origin);
	UT_ASSERT_EQ(
		cluster_ctrc_origin_record_touched(&origin, &participant, CTRC_PROOF_ACTIVE, &grant),
		CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT(cluster_ctrc_origin_grant_publishable_entry(&origin, &key, &participant, grant));

	origin.state = CTRC_ORIGIN_SEALING;
	UT_ASSERT(!cluster_ctrc_origin_grant_publishable_entry(&origin, &key, &participant, grant));
	origin.state = CTRC_ORIGIN_OPEN;
	UT_ASSERT(!cluster_ctrc_origin_grant_publishable_entry(&origin, &key, &participant, grant + 1));
	drifted.boot_incarnation++;
	UT_ASSERT(!cluster_ctrc_origin_grant_publishable_entry(&origin, &key, &drifted, grant));
	key.xid++;
	UT_ASSERT(!cluster_ctrc_origin_grant_publishable_entry(&origin, &key, &participant, grant));
}

/* MXA-T21: changing any full identity byte must prevent APPLY.  Exact
 * duplicates reuse the same participant-key sequence and receipt slot. */
UT_TEST(test_ctrc_receipt_prepare_apply_full_identity_cross_product)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(1, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcReceipt duplicate;
	ClusterCtrcApplyToken token;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_PREPARED);
	UT_ASSERT_EQ(receipt.publication.key_sequence, 1);
	UT_ASSERT(receipt.publication.journal_sequence != 0);
	UT_ASSERT(receipt.publication.journal_slot_generation != 0);

	duplicate = receipt;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &duplicate),
				 CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(duplicate.publication.key_sequence, 1);
	UT_ASSERT_EQ(duplicate.publication.journal_sequence, receipt.publication.journal_sequence);

	MemSet(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT_EQ(receipt.target.kind, CTRC_TARGET_EXACT_ITL_SLOT);
}

UT_TEST(test_ctrc_unpublished_itl_apply_accepts_forward_page_version)
{
	unsigned variant;

	for (variant = 0; variant < 3; variant++) {
		ClusterCtrcParticipantEntry participant;
		ClusterCtrcPublicationIdV1 publication
			= test_publication(201, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
		ClusterCtrcTargetV1 pending = test_pending_itl_target();
		ClusterCtrcTargetV1 exact = test_exact_itl_target();
		ClusterCtrcReceipt receipt = { 0 };
		ClusterCtrcReceipt saved;
		ClusterCtrcApplyToken token;

		pending.predecessor_page_lsn = 1000;
		pending.predecessor_page_lsn_origin_node_id = 0;
		pending.predecessor_page_scn = scn_encode(0, 100);
		exact.predecessor_page_lsn = variant == 1 ? 500 : 1100;
		exact.predecessor_page_lsn_origin_node_id = variant == 1 ? 1 : 0;
		exact.predecessor_page_scn = scn_encode(variant == 1 ? 1 : 0, 101);
		if (variant == 2) {
			pending.predecessor_page_lsn_origin_node_id = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
			pending.predecessor_page_scn = InvalidScn;
		}
		test_open_participant(&participant);
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
					 CLUSTER_CTRC_PREPARE_READY);
		saved = receipt;
		UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
					 CLUSTER_CTRC_APPLY_APPLIED);
		UT_ASSERT(token.valid);
		UT_ASSERT_EQ(memcmp(&receipt.target, &exact, sizeof(exact)), 0);
		UT_ASSERT_EQ(memcmp(&receipt.publication, &saved.publication, sizeof(saved.publication)),
					 0);
		UT_ASSERT_EQ(participant.prepared_count, 0);
		UT_ASSERT_EQ(participant.applied_count, 1);
		UT_ASSERT_EQ(participant.cancelled_count, 0);
		UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
					 CLUSTER_CTRC_APPLY_APPLIED);
		exact.predecessor_page_lsn++;
		UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
					 CLUSTER_CTRC_APPLY_FAIL_CLOSED);
		UT_ASSERT(!token.valid);
	}
}

UT_TEST(test_ctrc_unpublished_itl_reacquired_current_binds_only_at_apply)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(204, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt = { 0 };
	ClusterCtrcApplyToken token;

	exact.publication_own_generation++;
	test_open_participant(&participant);
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);
	UT_ASSERT_EQ(memcmp(&receipt.target, &exact, sizeof(exact)), 0);
	UT_ASSERT_EQ(participant.prepared_count, 0);
	UT_ASSERT_EQ(participant.applied_count, 1);
	UT_ASSERT_EQ(participant.cancelled_count, 0);
	/* Once applied, another generation is not the same publication. */
	exact.publication_own_generation++;
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_FAIL_CLOSED);
	UT_ASSERT(!token.valid);
}

UT_TEST(test_ctrc_unpublished_itl_version_floor_keeps_identity_and_negative_fences)
{
	unsigned variant;

	for (variant = 0; variant < 13; variant++) {
		ClusterCtrcParticipantEntry participant;
		ClusterCtrcPublicationIdV1 publication
			= test_publication(202, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
		ClusterCtrcTargetV1 pending = test_pending_itl_target();
		ClusterCtrcTargetV1 exact = test_exact_itl_target();
		ClusterCtrcReceipt receipt = { 0 };
		ClusterCtrcReceipt saved;
		ClusterCtrcApplyToken token;

		pending.predecessor_page_lsn = 1000;
		pending.predecessor_page_lsn_origin_node_id = 0;
		pending.predecessor_page_scn = scn_encode(0, 100);
		exact.predecessor_page_lsn = 1100;
		exact.predecessor_page_lsn_origin_node_id = 0;
		exact.predecessor_page_scn = scn_encode(0, 101);
		test_open_participant(&participant);
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
					 CLUSTER_CTRC_PREPARE_READY);
		switch (variant) {
		case 0:
			exact.predecessor_page_lsn = 999;
			break;
		case 1:
			exact.predecessor_page_scn = scn_encode(0, 99);
			break;
		case 2:
			exact.predecessor_page_lsn_origin_node_id = UINT16_MAX;
			break;
		case 3:
			exact.block_number++;
			break;
		case 4:
			exact.publication_own_generation--;
			break;
		case 5:
			exact.publication_acquisition_epoch++;
			break;
		case 6:
			exact.page_operation_kind++;
			break;
		case 7:
			exact.needs_wal = false;
			break;
		case 8:
			exact.reserved8[0] = 1;
			break;
		case 9:
			exact.itl_xid++;
			break;
		case 10:
			participant.grant_generation++;
			break;
		case 11:
			participant.state = CTRC_PARTICIPANT_CLOSED_DRAINING;
			break;
		case 12:
			receipt.state = CTRC_RECEIPT_CANCELLED;
			break;
		}
		saved = receipt;
		UT_ASSERT(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token)
				  != CLUSTER_CTRC_APPLY_APPLIED);
		UT_ASSERT(!token.valid);
		UT_ASSERT_EQ(memcmp(&receipt, &saved, sizeof(saved)), 0);
	}
}

UT_TEST(test_ctrc_offnum_still_requires_exact_predecessor_version)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(203, CTRC_REF_CURRENT_MX_LOCKER, CTRC_TARGET_PAGE_PENDING_OFFNUM);
	ClusterCtrcTargetV1 pending = test_pending_offnum_target(publication.descriptor_hash);
	ClusterCtrcTargetV1 exact = test_exact_tid_target(publication.descriptor_hash);
	ClusterCtrcReceipt receipt = { 0 };
	ClusterCtrcReceipt saved;
	ClusterCtrcApplyToken token;

	pending.predecessor_page_lsn = 1000;
	pending.predecessor_page_lsn_origin_node_id = 0;
	pending.predecessor_page_scn = scn_encode(0, 100);
	exact.predecessor_page_lsn = 1100;
	exact.predecessor_page_lsn_origin_node_id = 0;
	exact.predecessor_page_scn = scn_encode(0, 101);
	test_open_participant(&participant);
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	saved = receipt;
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT(!token.valid);
	UT_ASSERT_EQ(memcmp(&receipt, &saved, sizeof(saved)), 0);
}

UT_TEST(test_ctrc_shared_table_exact_duplicate_is_idempotent)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(81, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 target = test_pending_itl_target();
	ClusterCtrcReceipt receipts[3];
	ClusterCtrcReceipt original;
	uint8 probe_states[3];
	Size probe_count = 0;
	uint64 original_index;
	uint64 receipt_index = UINT64_MAX;

	MemSet(&participant, 0, sizeof(participant));
	MemSet(receipts, 0, sizeof(receipts));
	MemSet(probe_states, 0, sizeof(probe_states));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
					 &participant, &key, &identity, TEST_GRANT, &publication, &target, receipts,
					 probe_states, lengthof(receipts), 101, &receipt_index, &probe_count),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(probe_count, 1);
	UT_ASSERT(receipt_index < lengthof(receipts));
	UT_ASSERT_EQ(probe_states[receipt_index], CTRC_RECEIPT_PROBE_OCCUPIED);
	original_index = receipt_index;
	original = receipts[original_index];
	receipt_index = UINT64_MAX;
	probe_count = 0;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
					 &participant, &key, &identity, TEST_GRANT, &publication, &target, receipts,
					 probe_states, lengthof(receipts), 999, &receipt_index, &probe_count),
				 CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(receipt_index, original_index);
	UT_ASSERT_EQ(probe_count, 1);
	UT_ASSERT(memcmp(&receipts[original_index], &original, sizeof(original)) == 0);
	UT_ASSERT_EQ(participant.receipt_count, 1);
	UT_ASSERT_EQ(participant.last_key_sequence, 1);
}

UT_TEST(test_ctrc_shared_table_same_publication_different_target_blocks)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(82, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 target = test_pending_itl_target();
	ClusterCtrcTargetV1 conflicting = target;
	ClusterCtrcReceipt receipts[3];
	uint8 probe_states[3];
	Size probe_count = 0;
	uint64 receipt_index = UINT64_MAX;

	MemSet(&participant, 0, sizeof(participant));
	MemSet(receipts, 0, sizeof(receipts));
	MemSet(probe_states, 0, sizeof(probe_states));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
					 &participant, &key, &identity, TEST_GRANT, &publication, &target, receipts,
					 probe_states, lengthof(receipts), 102, &receipt_index, &probe_count),
				 CLUSTER_CTRC_PREPARE_READY);
	conflicting.block_number++;
	receipt_index = UINT64_MAX;
	probe_count = 0;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
					 &participant, &key, &identity, TEST_GRANT, &publication, &conflicting,
					 receipts, probe_states, lengthof(receipts), 103, &receipt_index, &probe_count),
				 CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);
	UT_ASSERT_EQ(participant.receipt_count, 1);
}

UT_TEST(test_ctrc_shared_table_different_publication_allocates_new_receipt)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 first
		= test_publication(83, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcPublicationIdV1 second = first;
	ClusterCtrcTargetV1 target = test_pending_itl_target();
	ClusterCtrcReceipt receipts[3];
	uint8 probe_states[3];
	Size probe_count = 0;
	uint64 first_index;
	uint64 receipt_index = UINT64_MAX;

	MemSet(&participant, 0, sizeof(participant));
	MemSet(receipts, 0, sizeof(receipts));
	MemSet(probe_states, 0, sizeof(probe_states));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
					 &participant, &key, &identity, TEST_GRANT, &first, &target, receipts,
					 probe_states, lengthof(receipts), 104, &receipt_index, &probe_count),
				 CLUSTER_CTRC_PREPARE_READY);
	first_index = receipt_index;
	second.operation_id++;
	second.attempt_generation++;
	receipt_index = UINT64_MAX;
	probe_count = 0;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
					 &participant, &key, &identity, TEST_GRANT, &second, &target, receipts,
					 probe_states, lengthof(receipts), 105, &receipt_index, &probe_count),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT(receipt_index != first_index);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_OPEN);
	UT_ASSERT_EQ(participant.receipt_count, 2);
	UT_ASSERT_EQ(receipts[first_index].publication.key_sequence, 1);
	UT_ASSERT_EQ(receipts[receipt_index].publication.key_sequence, 2);
}

UT_TEST(test_ctrc_shared_table_participant_key_and_grant_drift_fail_closed)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcPublicationIdV1 publication
		= test_publication(84, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 target = test_pending_itl_target();
	int drift;

	for (drift = 0; drift < 4; drift++) {
		ClusterCtrcTxnKeyV1 request_key = key;
		ClusterCtrcParticipantIdentity request_identity = identity;
		ClusterCtrcParticipantEntry participant;
		ClusterCtrcReceipt receipts[2];
		uint8 probe_states[2];
		Size probe_count = 0;
		uint32 request_grant = TEST_GRANT;
		uint64 receipt_index = UINT64_MAX;

		MemSet(&participant, 0, sizeof(participant));
		MemSet(receipts, 0, sizeof(receipts));
		MemSet(probe_states, 0, sizeof(probe_states));
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
						 &participant, &key, &identity, TEST_GRANT, &publication, &target, receipts,
						 probe_states, lengthof(receipts), 106 + drift, &receipt_index,
						 &probe_count),
					 CLUSTER_CTRC_PREPARE_READY);
		if (drift == 0)
			request_identity.capability_record_generation++;
		else if (drift == 1)
			request_identity.boot_incarnation++;
		else if (drift == 2)
			request_key.xid++;
		else
			request_grant++;
		receipt_index = UINT64_MAX;
		probe_count = 0;
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
						 &participant, &request_key, &request_identity, request_grant, &publication,
						 &target, receipts, probe_states, lengthof(receipts), 120 + drift,
						 &receipt_index, &probe_count),
					 CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT_EQ(participant.receipt_count, 1);
	}
}

UT_TEST(test_ctrc_indexed_receipt_probe_and_tombstone_reclaim_are_exact)
{
	ClusterCtrcTxnKeyV1 first_key = test_key();
	ClusterCtrcTxnKeyV1 second_key;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcParticipantEntry first_participant;
	ClusterCtrcParticipantEntry second_participant;
	ClusterCtrcPublicationIdV1 first_publication
		= test_publication(180, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcPublicationIdV1 second_publication;
	ClusterCtrcTargetV1 target = test_pending_itl_target();
	ClusterCtrcReceipt base_receipts[8];
	ClusterCtrcReceipt receipts[8];
	ClusterCtrcReceipt zero_receipts[8];
	uint8 base_probe_states[8];
	uint8 probe_states[8];
	uint8 zero_probe_states[8];
	Size probe_count = 0;
	Size reclaimed_count = 0;
	uint64 first_index = UINT64_MAX;
	uint64 second_index = UINT64_MAX;
	uint64 duplicate_index = UINT64_MAX;
	int candidate;
	bool collision_found = false;

	MemSet(&first_participant, 0, sizeof(first_participant));
	MemSet(base_receipts, 0, sizeof(base_receipts));
	MemSet(zero_receipts, 0, sizeof(zero_receipts));
	MemSet(base_probe_states, 0, sizeof(base_probe_states));
	MemSet(zero_probe_states, 0, sizeof(zero_probe_states));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
					 &first_participant, &first_key, &identity, TEST_GRANT, &first_publication,
					 &target, base_receipts, base_probe_states, lengthof(base_receipts), 201,
					 &first_index, &probe_count),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(probe_count, 1);

	for (candidate = 1; candidate < 256 && !collision_found; candidate++) {
		second_key = first_key;
		second_key.xid += candidate;
		second_publication = first_publication;
		second_publication.operation_id += candidate;
		second_publication.attempt_generation++;
		MemSet(&second_participant, 0, sizeof(second_participant));
		memcpy(receipts, base_receipts, sizeof(receipts));
		memcpy(probe_states, base_probe_states, sizeof(probe_states));
		probe_count = 0;
		second_index = UINT64_MAX;
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
						 &second_participant, &second_key, &identity, TEST_GRANT,
						 &second_publication, &target, receipts, probe_states, lengthof(receipts),
						 201 + candidate, &second_index, &probe_count),
					 CLUSTER_CTRC_PREPARE_READY);
		collision_found = probe_count > 1;
	}
	UT_ASSERT(collision_found);
	UT_ASSERT(second_index != first_index);
	receipts[first_index].state = CTRC_RECEIPT_ACK_FROZEN;
	UT_ASSERT(cluster_ctrc_receipt_reclaim_frozen_table_locked(
		&first_key, 1, receipts, probe_states, lengthof(receipts), &reclaimed_count));
	UT_ASSERT_EQ(reclaimed_count, 1);
	UT_ASSERT_EQ(probe_states[first_index], CTRC_RECEIPT_PROBE_TOMBSTONE);
	UT_ASSERT(
		memcmp(&receipts[first_index], &(ClusterCtrcReceipt){ 0 }, sizeof(receipts[first_index]))
		== 0);

	probe_count = 0;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(
					 &second_participant, &second_key, &identity, TEST_GRANT, &second_publication,
					 &target, receipts, probe_states, lengthof(receipts), 999, &duplicate_index,
					 &probe_count),
				 CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(duplicate_index, second_index);
	UT_ASSERT(probe_count > 1);

	receipts[second_index].state = CTRC_RECEIPT_ACK_FROZEN;
	reclaimed_count = 0;
	UT_ASSERT(cluster_ctrc_receipt_reclaim_frozen_table_locked(
		&second_key, 1, receipts, probe_states, lengthof(receipts), &reclaimed_count));
	UT_ASSERT_EQ(reclaimed_count, 1);
	UT_ASSERT(memcmp(receipts, zero_receipts, sizeof(receipts)) == 0);
	UT_ASSERT(memcmp(probe_states, zero_probe_states, sizeof(probe_states)) == 0);
}

/* MXA-T22: a mismatched finalize is a pre-mutation retry and leaves the
 * receipt PREPARED.  Only the exact release-CAS returns a mutation token. */
UT_TEST(test_ctrc_heap_apply_is_nonblocking_and_precedes_all_reference_mutation)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(2, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	exact.block_number++;
	MemSet(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT(!token.valid);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_PREPARED);

	exact.block_number--;
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);
}

UT_TEST(test_ctrc_same_itl_retarget_is_exact_nonblocking_and_count_neutral)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(202, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcTargetV1 next_pending = pending;
	ClusterCtrcTargetV1 next_exact = exact;
	ClusterCtrcReceipt before_mismatch;
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcPublicationIdV1 stored_publication;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	stored_publication = receipt.publication;

	next_pending.page_operation_kind = 2;
	next_exact.page_operation_kind = 2;
	MemSet(next_exact.uba, 0x6b, sizeof(next_exact.uba));
	memcpy(next_exact.planned_predecessor_sha256, exact.planned_successor_sha256,
		   sizeof(next_exact.planned_predecessor_sha256));
	MemSet(next_exact.planned_successor_sha256, 0x33, sizeof(next_exact.planned_successor_sha256));
	MemSet(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_ctrc_receipt_retarget_itl(&participant, &receipt, &next_pending,
												   &next_exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT(memcmp(&receipt.target, &next_exact, sizeof(next_exact)) == 0);
	UT_ASSERT(memcmp(&receipt.publication, &stored_publication, sizeof(stored_publication)) == 0);
	UT_ASSERT_EQ(participant.receipt_count, 1);
	UT_ASSERT_EQ(participant.prepared_count, 0);
	UT_ASSERT_EQ(participant.applied_count, 1);

	before_mismatch = receipt;
	receipt.disposition = CTRC_RELEASE_CLEANED_TERMINAL_REWRITE;
	MemSet(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_ctrc_receipt_retarget_itl(&participant, &receipt, &next_pending,
												   &next_exact, &token),
				 CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT(!token.valid);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT(memcmp(&receipt.target, &before_mismatch.target, sizeof(receipt.target)) == 0);
	receipt = before_mismatch;

	next_pending.page_operation_kind = 3;
	next_exact.page_operation_kind = 3;
	MemSet(next_exact.uba, 0x7c, sizeof(next_exact.uba));
	MemSet(next_exact.planned_predecessor_sha256, 0x44,
		   sizeof(next_exact.planned_predecessor_sha256));
	MemSet(next_exact.planned_successor_sha256, 0x55, sizeof(next_exact.planned_successor_sha256));
	MemSet(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_ctrc_receipt_retarget_itl(&participant, &receipt, &next_pending,
												   &next_exact, &token),
				 CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT(!token.valid);
	UT_ASSERT(memcmp(&receipt, &before_mismatch, sizeof(receipt)) == 0);
	UT_ASSERT_EQ(participant.receipt_count, 1);
	UT_ASSERT_EQ(participant.applied_count, 1);
}

UT_TEST(test_ctrc_retargeting_owner_loss_blocks_without_release)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(203, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	receipt.state = CTRC_RECEIPT_RETARGETING;
	UT_ASSERT_EQ(cluster_ctrc_participant_note_owner_loss(&participant, &receipt),
				 CLUSTER_CTRC_LOSS_BLOCKED);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_BLOCKED);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);
	UT_ASSERT_EQ(participant.applied_count, 0);
}

/* MXA-T21/T22 current-MX half: every ACTIVE member publication reserves a
 * page/offnum receipt and converts it to the exact tuple/MX identity before
 * the descriptor can become reachable.  Zero-grant terminal proof and every
 * target identity drift remain pre-mutation refusals. */
UT_TEST(test_ctrc_current_mx_receipt_applies_only_exact_active_target)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(22, CTRC_REF_RECOMPOSED_SURVIVOR, CTRC_TARGET_PAGE_PENDING_OFFNUM);
	ClusterCtrcTargetV1 pending = test_pending_offnum_target(publication.descriptor_hash);
	ClusterCtrcTargetV1 exact = test_exact_tid_target(publication.descriptor_hash);
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	exact.block_number++;
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_PREPARED);
	exact.block_number--;
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);
	UT_ASSERT_EQ(receipt.target.kind, CTRC_TARGET_EXACT_TID);

	MemSet(&receipt, 0, sizeof(receipt));
	publication.operation_id++;
	publication.grant_generation = 0;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_REFUSED);
}

/* MXA-T23: NEEDS_CLEANOUT and skipped legacy hints are not release proof. */
UT_TEST(test_ctrc_itl_uba_registers_before_mutation_and_only_exact_projection_discharges)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(3, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcDurability durability;
	ClusterCtrcItlTargetIdentity target_identity;
	ClusterCtrcItlTargetIdentity wrong_identity;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	MemSet(&target_identity, 0, sizeof(target_identity));
	target_identity.spc_oid = exact.spc_oid;
	target_identity.db_oid = exact.db_oid;
	target_identity.rel_number = exact.rel_number;
	target_identity.fork_number = exact.fork_number;
	target_identity.block_number = exact.block_number;
	target_identity.itl_slot_index = exact.itl_slot_index;
	target_identity.itl_slot_wrap = exact.itl_slot_wrap;
	target_identity.itl_xid = exact.itl_xid;
	target_identity.itl_class = exact.itl_class;
	target_identity.needs_wal = exact.needs_wal;
	memcpy(target_identity.uba, exact.uba, sizeof(target_identity.uba));
	UT_ASSERT(cluster_ctrc_itl_target_identity_matches(&receipt.target, &target_identity));
	target_identity.uba[sizeof(target_identity.uba) - 1]++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(&receipt.target, &target_identity));
	target_identity.uba[sizeof(target_identity.uba) - 1]--;
	wrong_identity = target_identity;
	wrong_identity.block_number++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.itl_slot_index++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.itl_slot_wrap++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.itl_xid++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.itl_class = target_identity.itl_class == 1 ? 2 : 1;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.needs_wal = !target_identity.needs_wal;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(&receipt.target, &wrong_identity));
	MemSet(&durability, 0, sizeof(durability));
	durability.highest_local_lsn = 300;
	durability.local_flush_lsn = 300;

	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt, CTRC_ITL_NEEDS_CLEANOUT,
													&durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt, CTRC_ITL_HINT_SKIPPED,
													&durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	durability.local_flush_lsn = 299;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
													CTRC_ITL_TERMINAL_INDEPENDENT, &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	durability.local_flush_lsn = 300;
	durability.required_lsn[4] = 400;
	durability.durable_lsn[4] = 399;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
													CTRC_ITL_TERMINAL_INDEPENDENT, &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	durability.durable_lsn[4] = 400;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
													CTRC_ITL_TERMINAL_INDEPENDENT, &durability),
				 CLUSTER_CTRC_DISCHARGE_CLEANED);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(receipt.disposition, CTRC_RELEASE_CLEANED_TERMINAL_REWRITE);
	UT_ASSERT_EQ(receipt.highest_local_wal_lsn, durability.highest_local_lsn);
	UT_ASSERT_EQ(receipt.required_lsn[4], durability.required_lsn[4]);
	UT_ASSERT_EQ(participant.applied_count, UINT64_C(0));
	UT_ASSERT_EQ(participant.cleaned_count, UINT64_C(1));
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
													CTRC_ITL_TERMINAL_INDEPENDENT, &durability),
				 CLUSTER_CTRC_DISCHARGE_CLEANED);
	UT_ASSERT_EQ(participant.cleaned_count, UINT64_C(1));
}

/* MXA-T25/T26: a new Resource-X cleanout round may rewrite only the exact
 * frozen ITL incarnation, and the terminal state must agree with block zero. */
UT_TEST(test_ctrc_itl_cleanout_rewrites_only_exact_terminal_slot)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcTargetV1 target = test_exact_itl_target();
	ClusterItlSlotData slot;
	ClusterItlSlotData before;

	MemSet(&slot, 0, sizeof(slot));
	slot.xid = target.itl_xid;
	slot.wrap = target.itl_slot_wrap;
	slot.flags = ITL_FLAG_ACTIVE;
	memcpy(&slot.undo_segment_head, target.uba, sizeof(slot.undo_segment_head));
	UT_ASSERT_EQ(cluster_ctrc_itl_cleanout_slot(&key, &target, CTRC_TERMINAL_COMMITTED,
												UINT64_C(900), &slot),
				 CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN);
	UT_ASSERT_EQ(slot.flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(slot.commit_scn, UINT64_C(900));
	UT_ASSERT_EQ(cluster_ctrc_itl_cleanout_slot(&key, &target, CTRC_TERMINAL_COMMITTED,
												UINT64_C(900), &slot),
				 CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL);

	before = slot;
	slot.wrap++;
	UT_ASSERT_EQ(cluster_ctrc_itl_cleanout_slot(&key, &target, CTRC_TERMINAL_COMMITTED,
												UINT64_C(900), &slot),
				 CLUSTER_CTRC_ITL_CLEANOUT_RETAIN);
	UT_ASSERT_EQ(slot.flags, before.flags);
	slot = before;
	slot.flags = ITL_FLAG_ACTIVE;
	slot.commit_scn = InvalidScn;
	UT_ASSERT_EQ(
		cluster_ctrc_itl_cleanout_slot(&key, &target, CTRC_TERMINAL_ABORTED, InvalidScn, &slot),
		CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN);
	UT_ASSERT_EQ(slot.flags, ITL_FLAG_ABORTED);
	UT_ASSERT_EQ(slot.commit_scn, InvalidScn);
}

/* MXA-T24: CLOSE racing an old PREPARED receipt either observes exact APPLY
 * or keeps the range blocked; time and backend loss never cancel it. */
/* Real cleaner image transition; page admission, WAL and lock services are
 * boundary seams. The slot proof and tuple rewrite are the product C. */
static int image_finishes, image_aborts, image_unlocks, image_leaves;

static bool
test_cleaner_terminal_image(Page image, const ClusterCtrcReceipt *receipt,
							ClusterCtrcTerminalStatus terminal_status, SCN commit_scn)
{
	ClusterCtrcItlCleanoutApplyResult apply_result;
	XLogRecPtr cleanout_lsn;
	const char *retain_stage = NULL;

#define GenericXLogAbort(state) ((void)image_aborts++)
#define UnlockReleaseBuffer(buffer) ((void)image_unlocks++)
#define cluster_semantic_activation_leave(admission) ((void)image_leaves++)
#define GenericXLogFinish(state) (image_finishes++, (XLogRecPtr)100)
#include "test_cluster_ctrc_terminal_image.inc"
	UT_ASSERT_EQ(cleanout_lsn, 100);
	return true;

/* The extracted image transition now uses the cleaner's common release
 * boundary. The complete cleaner fixture separately executes that body. */
itl_retain_locked:
	UT_ASSERT_STR_EQ(retain_stage, "SLOT_REVALIDATE");
	UnlockReleaseBuffer(buffer);
	cluster_semantic_activation_leave(&admission);
	return false;
#undef GenericXLogAbort
#undef UnlockReleaseBuffer
#undef cluster_semantic_activation_leave
#undef GenericXLogFinish
}

UT_TEST(test_real_cleaner_terminal_image_closes_plain_lock_reference)
{
	int leg;

	for (leg = 0; leg < 4; leg++) {
		PGAlignedBlock storage = { 0 };
		Page image = (Page)storage.data;
		PageHeader header = (PageHeader)image;
		ClusterCtrcReceipt receipt = { 0 };
		ClusterItlSlotData *slot;
		HeapTupleHeader tuple;
		bool applied;

		receipt.key = test_key();
		receipt.target = test_exact_itl_target();
		receipt.target.itl_class = 2;
		header->pd_flags = PD_HAS_ITL;
		header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
		header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
		header->pd_upper = header->pd_special - MAXALIGN(SizeofHeapTupleHeader);
		PageSetPageSizeAndVersion(image, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
		ItemIdSetNormal(PageGetItemId(image, FirstOffsetNumber), header->pd_upper,
						SizeofHeapTupleHeader);
		tuple = (HeapTupleHeader)PageGetItem(image, PageGetItemId(image, FirstOffsetNumber));
		tuple->t_hoff = SizeofHeapTupleHeader;
		tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
		HeapTupleHeaderSetXmin(tuple, 800);
		HeapTupleHeaderSetXmax(tuple, receipt.target.itl_xid);
		slot = &ClusterPageGetItlSlots(image)[receipt.target.itl_slot_index];
		slot->xid = receipt.target.itl_xid;
		slot->wrap = receipt.target.itl_slot_wrap + (leg == 3 ? 1 : 0);
		slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		memcpy(&slot->undo_segment_head, receipt.target.uba, sizeof(slot->undo_segment_head));
		image_finishes = image_aborts = image_unlocks = image_leaves = 0;
		applied = test_cleaner_terminal_image(
			image, &receipt, leg == 2 ? CTRC_TERMINAL_ABORTED : CTRC_TERMINAL_COMMITTED,
			leg == 0 || leg == 2 ? InvalidScn : 900);
		UT_ASSERT_EQ(applied, leg == 1 || leg == 2);
		UT_ASSERT_EQ((tuple->t_infomask & HEAP_XMAX_INVALID) != 0, applied);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), 800);
		UT_ASSERT_EQ(image_finishes, applied ? 1 : 0);
		UT_ASSERT_EQ(image_aborts, applied ? 0 : 1);
		UT_ASSERT_EQ(image_unlocks, applied ? 0 : 1);
		UT_ASSERT_EQ(image_leaves, applied ? 0 : 1);
	}
}

UT_TEST(test_ctrc_close_race_drains_prepared_without_timeout_cancellation)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcPublicationIdV1 publication
		= test_publication(4, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcReceipt late_receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcDurability durability;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_PENDING_DRAIN);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT_EQ(cluster_ctrc_participant_note_owner_loss(&participant, &receipt),
				 CLUSTER_CTRC_LOSS_BLOCKED);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_BLOCKED);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);

	/* If APPLY wins before CLOSE, CLOSE drains that APPLIED receipt through
	 * exact cleanout and only then freezes the local range. */
	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	publication.operation_id++;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_PENDING_DRAIN);
	MemSet(&durability, 0, sizeof(durability));
	durability.highest_local_lsn = 300;
	durability.local_flush_lsn = 300;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
													CTRC_ITL_TERMINAL_INDEPENDENT, &durability),
				 CLUSTER_CTRC_DISCHARGE_CLEANED);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_ACK_READY);

	/* A frozen close never reopens for a late proof/prepare. */
	MemSet(&late_receipt, 0, sizeof(late_receipt));
	publication.operation_id++;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &late_receipt),
				 CLUSTER_CTRC_PREPARE_REFUSED);
}

/* MXA-T25: only exact target absence under the censused transfer contract is
 * a discharge; ambiguity or changed page authority retains. */
UT_TEST(test_ctrc_exact_target_absence_and_ambiguity_cleanout_table)
{
	ClusterCtrcCleanReferenceInput input;

	MemSet(&input, 0, sizeof(input));
	input.target_state = CTRC_TARGET_ABSENT;
	input.source_transition_censused = true;
	input.page_authority_exact = true;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEANED_ABSENT);
	input.source_transition_censused = false;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.source_transition_censused = true;
	input.page_authority_exact = false;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.page_authority_exact = true;
	input.target_state = CTRC_TARGET_AMBIGUOUS;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
}

/* MXA-T26: committed updater semantics cannot be erased while any ACTIVE or
 * UNKNOWN companion remains.  Terminal lockers and aborted updaters clean. */
UT_TEST(test_ctrc_terminal_member_cleanout_semantics_cross_product)
{
	ClusterCtrcCleanReferenceInput input;

	MemSet(&input, 0, sizeof(input));
	input.page_authority_exact = true;
	input.target_state = CTRC_TARGET_TERMINAL_LOCK_ONLY;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEANED_TERMINAL_REWRITE);
	input.target_state = CTRC_TARGET_ABORTED_UPDATER;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEANED_TERMINAL_REWRITE);
	input.active_companions = 1;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.successor_topology_exact = true;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEANED_SUCCESSOR_REPLACED);
	input.active_companions = 0;
	input.successor_topology_exact = false;
	input.target_state = CTRC_TARGET_COMMITTED_UPDATER;
	input.active_companions = 1;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.active_companions = 0;
	input.unknown_companions = 1;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.unknown_companions = 0;
	input.successor_topology_exact = true;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEANED_TERMINAL_REWRITE);
	input.target_state = CTRC_TARGET_ACTIVE_SURVIVOR;
	input.successor_topology_exact = false;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.successor_topology_exact = true;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEANED_SUCCESSOR_REPLACED);
	input.target_state = UINT8_MAX;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
}

/* MXA-T26: the pure cleanout verdict is not itself a release.  The exact
 * current-MX receipt may become CLEANED only after the matching target and
 * the page/dependency durability frontier are supplied to the journal FSM. */
UT_TEST(test_ctrc_current_mx_discharge_requires_exact_target_and_durability)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(26, CTRC_REF_CURRENT_MX_LOCKER, CTRC_TARGET_PAGE_PENDING_OFFNUM);
	ClusterCtrcTargetV1 pending = test_pending_offnum_target(publication.descriptor_hash);
	ClusterCtrcTargetV1 exact = test_exact_tid_target(publication.descriptor_hash);
	ClusterCtrcTargetV1 wrong;
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcDurability durability;

	pending.predecessor_page_lsn = 200;
	exact.predecessor_page_lsn = 200;
	pending.predecessor_page_lsn_origin_node_id = 1;
	exact.predecessor_page_lsn_origin_node_id = 1;
	pending.predecessor_page_scn = scn_encode(1, 100);
	exact.predecessor_page_scn = scn_encode(1, 100);
	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);

	MemSet(&durability, 0, sizeof(durability));
	/* The cleanout WAL belongs to the cleaner's local stream and need not be
	 * numerically beyond the foreign predecessor's LSN. */
	durability.highest_local_lsn = 100;
	durability.local_flush_lsn = 99;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_current_mx(
					 &participant, &receipt, &exact, CTRC_CLEANED_TERMINAL_REWRITE, &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	wrong = exact;
	wrong.offset_number++;
	durability.local_flush_lsn = 100;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_current_mx(
					 &participant, &receipt, &wrong, CTRC_CLEANED_TERMINAL_REWRITE, &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_current_mx(&participant, &receipt, &exact,
														   CTRC_CLEAN_RETAIN, &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);

	durability.required_lsn[3] = 400;
	durability.durable_lsn[3] = 399;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_current_mx(
					 &participant, &receipt, &exact, CTRC_CLEANED_TERMINAL_REWRITE, &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	durability.durable_lsn[3] = 400;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_current_mx(
					 &participant, &receipt, &exact, CTRC_CLEANED_TERMINAL_REWRITE, &durability),
				 CLUSTER_CTRC_DISCHARGE_CLEANED);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(receipt.disposition, CTRC_RELEASE_CLEANED_TERMINAL_REWRITE);
	UT_ASSERT_EQ(receipt.highest_local_wal_lsn, UINT64_C(100));
	UT_ASSERT_EQ(receipt.required_lsn[3], UINT64_C(400));
	UT_ASSERT_EQ(participant.applied_count, UINT64_C(0));
	UT_ASSERT_EQ(participant.cleaned_count, UINT64_C(1));
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_current_mx(
					 &participant, &receipt, &exact, CTRC_CLEANED_TERMINAL_REWRITE, &durability),
				 CLUSTER_CTRC_DISCHARGE_CLEANED);
	UT_ASSERT_EQ(participant.cleaned_count, UINT64_C(1));
}

/* The four WAL streams used by MXA-T26 are independent address spaces.  A
 * numerically smaller LSN is a regression only inside the same origin; SCN
 * supplies the cross-origin order. */
UT_TEST(test_ctrc_page_lsn_regression_is_origin_qualified)
{
	SCN predecessor_scn = scn_encode(0, 100);
	SCN newer_scn = scn_encode(1, 101);
	SCN older_scn = scn_encode(1, 99);

	UT_ASSERT_EQ(cluster_ctrc_page_version_order(0, 500, predecessor_scn, 0, 501, newer_scn),
				 CTRC_PAGE_VERSION_CURRENT);
	UT_ASSERT_EQ(cluster_ctrc_page_version_order(0, 500, predecessor_scn, 0, 499, newer_scn),
				 CTRC_PAGE_VERSION_REGRESSED);
	UT_ASSERT_EQ(cluster_ctrc_page_version_order(0, 500, predecessor_scn, 1, 20, newer_scn),
				 CTRC_PAGE_VERSION_CURRENT);
	UT_ASSERT_EQ(cluster_ctrc_page_version_order(0, 500, predecessor_scn, 1, 600, older_scn),
				 CTRC_PAGE_VERSION_REGRESSED);
	UT_ASSERT_EQ(cluster_ctrc_page_version_order(CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID, 500,
												 predecessor_scn, 1, 600, newer_scn),
				 CTRC_PAGE_VERSION_UNKNOWN);
	UT_ASSERT_EQ(cluster_ctrc_page_version_order(CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID,
												 InvalidXLogRecPtr, InvalidScn, 1, 20, newer_scn),
				 CTRC_PAGE_VERSION_CURRENT);
}

/* An initdb/base-backup page predates R4 activation: its PostgreSQL LSN is
 * real, but node_id=-1 means that it has neither a cluster WAL origin nor a
 * block SCN.  It is a one-way bootstrap predecessor, never an origin-local
 * LSN coordinate. */
UT_TEST(test_ctrc_bootstrap_page_version_is_one_way_baseline)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication
		= test_publication(121, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;
	SCN current_scn = scn_encode(1, 101);

	pending.predecessor_page_lsn = 500;
	pending.predecessor_page_lsn_origin_node_id = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
	pending.predecessor_page_scn = InvalidScn;
	exact.predecessor_page_lsn = pending.predecessor_page_lsn;
	exact.predecessor_page_lsn_origin_node_id = pending.predecessor_page_lsn_origin_node_id;
	exact.predecessor_page_scn = pending.predecessor_page_scn;
	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication, &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	MemSet(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt, &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_ctrc_page_version_order(CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID, 500,
												 InvalidScn, 1, 20, current_scn),
				 CTRC_PAGE_VERSION_CURRENT);
	UT_ASSERT_EQ(cluster_ctrc_page_version_order(CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID, 500,
												 InvalidScn, CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID,
												 500, InvalidScn),
				 CTRC_PAGE_VERSION_UNKNOWN);
}

/* MXA-I01/I02/I17: the participant consumes the member origin's already
 * validated terminal proof batch.  A receipt may belong to a foreign origin;
 * cleanout must not route it through the participant-local block-0 sampler. */
UT_TEST(test_ctrc_remote_current_mx_cleanout_consumes_exact_origin_proof)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcPublicationIdV1 publication
		= test_publication(26, CTRC_REF_CURRENT_MX_LOCKER, CTRC_TARGET_PAGE_PENDING_OFFNUM);
	ClusterCurrentMxKey descriptor_key;
	ClusterCurrentMxMemberDesc member;
	ClusterCurrentMemberProof proof;
	ClusterCtrcTerminalStatus terminal_status = CTRC_TERMINAL_UNKNOWN;
	SCN commit_scn = InvalidScn;

	MemSet(&descriptor_key, 0, sizeof(descriptor_key));
	descriptor_key.origin_node_id = 1;
	descriptor_key.multixact_id = (MultiXactId)81;
	descriptor_key.cluster_epoch = key.cluster_epoch;
	MemSet(&member, 0, sizeof(member));
	member.xid = key.xid;
	member.member_status = MultiXactStatusForShare;
	MemSet(&proof, 0, sizeof(proof));
	proof.member_xid = member.xid;
	proof.member_ordinal = 0;
	proof.member_status = member.member_status;
	proof.state = CCM_COMMITTED;
	proof.commit_scn = UINT64_C(701);
	publication.member_ordinal = 0;
	publication.member_role = member.member_status + 1;

	UT_ASSERT(cluster_ctrc_current_mx_terminal_proof_exact(
		&key, &descriptor_key, &publication, &member, &proof, 1, &terminal_status, &commit_scn));
	UT_ASSERT_EQ(terminal_status, CTRC_TERMINAL_COMMITTED);
	UT_ASSERT_EQ(commit_scn, UINT64_C(701));

	proof.state = CCM_ABORTED;
	proof.commit_scn = InvalidScn;
	UT_ASSERT(cluster_ctrc_current_mx_terminal_proof_exact(
		&key, &descriptor_key, &publication, &member, &proof, 1, &terminal_status, &commit_scn));
	UT_ASSERT_EQ(terminal_status, CTRC_TERMINAL_ABORTED);
	UT_ASSERT_EQ(commit_scn, InvalidScn);

	proof.state = CCM_ACTIVE;
	ClusterCurrentMemberProofSetCtrcGrant(&proof, TEST_GRANT);
	UT_ASSERT(!cluster_ctrc_current_mx_terminal_proof_exact(
		&key, &descriptor_key, &publication, &member, &proof, 1, &terminal_status, &commit_scn));
	UT_ASSERT_EQ(terminal_status, CTRC_TERMINAL_UNKNOWN);
	UT_ASSERT_EQ(commit_scn, InvalidScn);

	proof.state = CCM_ABORTED;
	ClusterCurrentMemberProofSetCtrcGrant(&proof, TEST_GRANT);
	UT_ASSERT(!cluster_ctrc_current_mx_terminal_proof_exact(&key, &descriptor_key, &publication,
															&member, &proof, 1, NULL, NULL));
	ClusterCurrentMemberProofSetCtrcGrant(&proof, 0);
	proof.member_ordinal = 1;
	UT_ASSERT(!cluster_ctrc_current_mx_terminal_proof_exact(&key, &descriptor_key, &publication,
															&member, &proof, 1, NULL, NULL));
	proof.member_ordinal = 0;
	descriptor_key.cluster_epoch++;
	UT_ASSERT(!cluster_ctrc_current_mx_terminal_proof_exact(&key, &descriptor_key, &publication,
															&member, &proof, 1, NULL, NULL));

	UT_ASSERT_EQ(source_file_occurrences("src/backend/cluster/cluster_terminal_ref_census.c",
										 "ctrc_cleaner_terminal_sample_exact("),
				 3);
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "cluster_ctrc_current_mx_terminal_proof_exact("));
}

/* MXA-T26/T27: classify the complete immutable descriptor before any page
 * rewrite.  Active survivors require a successor descriptor; a committed
 * updater is retained only as the terminal single-XID projection, and an
 * unknown proof makes the whole plan retain. */
UT_TEST(test_ctrc_current_mx_rewrite_plan_preserves_updater_and_survivors)
{
	ClusterCtrcPublicationIdV1 publication
		= test_publication(27, CTRC_REF_CURRENT_MX_LOCKER, CTRC_TARGET_PAGE_PENDING_OFFNUM);
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCurrentMxMemberDesc members[3];
	ClusterCurrentMemberProof proofs[3];
	ClusterCtrcCurrentMxRewritePlan plan;

	MemSet(members, 0, sizeof(members));
	MemSet(proofs, 0, sizeof(proofs));
	members[0].xid = key.xid;
	members[0].member_status = MultiXactStatusForShare;
	members[1].xid = key.xid + 1;
	members[1].member_status = MultiXactStatusNoKeyUpdate;
	members[2].xid = key.xid + 2;
	members[2].member_status = MultiXactStatusForKeyShare;
	for (uint16 i = 0; i < lengthof(members); i++) {
		proofs[i].member_xid = members[i].xid;
		proofs[i].member_ordinal = i;
		proofs[i].member_status = members[i].member_status;
	}
	publication.member_ordinal = 0;
	publication.member_role = members[0].member_status + 1;
	proofs[0].state = CCM_COMMITTED;
	proofs[0].commit_scn = 701;
	proofs[1].state = CCM_ACTIVE;
	proofs[2].state = CCM_ACTIVE;
	ClusterCurrentMemberProofSetCtrcGrant(&proofs[1], 8);
	ClusterCurrentMemberProofSetCtrcGrant(&proofs[2], 9);
	UT_ASSERT(cluster_ctrc_current_mx_rewrite_plan(&key, &publication, members, proofs,
												   lengthof(members), &plan));
	UT_ASSERT_EQ(plan.kind, CTRC_CURRENT_MX_REWRITE_SUCCESSOR);
	UT_ASSERT_EQ(plan.clean_result, CTRC_CLEANED_SUCCESSOR_REPLACED);
	UT_ASSERT_EQ(plan.survivor_count, 2);
	UT_ASSERT_EQ(plan.survivors[0].xid, members[1].xid);
	UT_ASSERT_EQ(plan.survivors[1].xid, members[2].xid);

	/* A committed updater may not be erased or coexist with a replacement
	 * descriptor while any companion is still ACTIVE. */
	publication.member_ordinal = 1;
	publication.member_role = members[1].member_status + 1;
	publication.reference_kind = CTRC_REF_CURRENT_MX_UPDATER;
	key.xid = members[1].xid;
	proofs[1].state = CCM_COMMITTED;
	proofs[1].commit_scn = 702;
	ClusterCurrentMemberProofSetCtrcGrant(&proofs[1], 0);
	UT_ASSERT(!cluster_ctrc_current_mx_rewrite_plan(&key, &publication, members, proofs,
													lengthof(members), &plan));
	UT_ASSERT_EQ(plan.kind, CTRC_CURRENT_MX_REWRITE_RETAIN);

	/* Once every companion is terminal, retain the updater as PostgreSQL's
	 * native committed single-XID form. */
	proofs[2].state = CCM_ABORTED;
	ClusterCurrentMemberProofSetCtrcGrant(&proofs[2], 0);
	UT_ASSERT(cluster_ctrc_current_mx_rewrite_plan(&key, &publication, members, proofs,
												   lengthof(members), &plan));
	UT_ASSERT_EQ(plan.kind, CTRC_CURRENT_MX_REWRITE_COMMITTED_UPDATER);
	UT_ASSERT_EQ(plan.committed_updater_xid, members[1].xid);
	UT_ASSERT_EQ(plan.committed_updater_scn, UINT64_C(702));

	proofs[1].state = CCM_ABORTED;
	proofs[1].commit_scn = InvalidScn;
	publication.member_ordinal = 0;
	publication.member_role = members[0].member_status + 1;
	publication.reference_kind = CTRC_REF_CURRENT_MX_LOCKER;
	key.xid = members[0].xid;
	UT_ASSERT(cluster_ctrc_current_mx_rewrite_plan(&key, &publication, members, proofs,
												   lengthof(members), &plan));
	UT_ASSERT_EQ(plan.kind, CTRC_CURRENT_MX_REWRITE_INVALIDATE);

	proofs[2].state = CCM_UNKNOWN;
	UT_ASSERT(!cluster_ctrc_current_mx_rewrite_plan(&key, &publication, members, proofs,
													lengthof(members), &plan));
	UT_ASSERT_EQ(plan.kind, CTRC_CURRENT_MX_REWRITE_RETAIN);
}

/* MXA-T27: deleting the predecessor first must fail even when the eventual
 * successor is byte-identical. */
UT_TEST(test_ctrc_successor_receipts_and_descriptor_precede_predecessor_removal)
{
	ClusterCtrcTransferState transfer;

	MemSet(&transfer, 0, sizeof(transfer));
	transfer.active_survivor_count = 2;
	UT_ASSERT_EQ(cluster_ctrc_transfer_remove_predecessor(&transfer),
				 CLUSTER_CTRC_TRANSFER_REFUSED);
	UT_ASSERT_EQ(cluster_ctrc_transfer_note_successor_receipt(&transfer),
				 CLUSTER_CTRC_TRANSFER_PENDING_DESCRIPTOR);
	UT_ASSERT_EQ(cluster_ctrc_transfer_note_successor_receipt(&transfer),
				 CLUSTER_CTRC_TRANSFER_PENDING_DESCRIPTOR);
	UT_ASSERT_EQ(cluster_ctrc_transfer_note_descriptor_durable(&transfer),
				 CLUSTER_CTRC_TRANSFER_READY);
	UT_ASSERT_EQ(cluster_ctrc_transfer_note_descriptor_durable(&transfer),
				 CLUSTER_CTRC_TRANSFER_READY);
	UT_ASSERT_EQ(cluster_ctrc_transfer_remove_predecessor(&transfer),
				 CLUSTER_CTRC_TRANSFER_REMOVED);
	UT_ASSERT_EQ(cluster_ctrc_transfer_note_successor_receipt(&transfer),
				 CLUSTER_CTRC_TRANSFER_REFUSED);
	UT_ASSERT_EQ(cluster_ctrc_transfer_remove_predecessor(&transfer),
				 CLUSTER_CTRC_TRANSFER_REMOVED);

	MemSet(&transfer, 0, sizeof(transfer));
	UT_ASSERT_EQ(cluster_ctrc_transfer_remove_predecessor(&transfer),
				 CLUSTER_CTRC_TRANSFER_REMOVED);
}

/* MXA-T27: native prune/freeze cannot replace a current-MX predecessor in
 * peer mode.  The CTRC cleaner is the only owner that can first register a
 * surviving descriptor, so native code must leave the tuple unchanged. */
UT_TEST(test_ctrc_native_prune_freeze_refuses_current_mx_predecessor)
{
	UT_ASSERT(cluster_ctrc_native_current_mx_mutation_allowed(false, 0));
	UT_ASSERT(cluster_ctrc_native_current_mx_mutation_allowed(true, -1));
	UT_ASSERT(!cluster_ctrc_native_current_mx_mutation_allowed(true, 0));
	UT_ASSERT(!cluster_ctrc_native_current_mx_mutation_allowed(true, 15));
}

/* MXA-T35: DROP/TRUNCATE/KO may remove a physical target only after every
 * matching journal row is already terminal.  The relation gate deliberately
 * over-drains all forks/blocks; unrelated relations do not block it. */
UT_TEST(test_ctrc_relation_removal_waits_for_every_matching_receipt)
{
	ClusterCtrcReceipt receipts[3];
	ClusterCtrcTargetV1 target = test_exact_itl_target();

	MemSet(receipts, 0, sizeof(receipts));
	receipts[0].target = target;
	receipts[0].state = CTRC_RECEIPT_PREPARED;
	UT_ASSERT(!cluster_ctrc_relation_removal_ready_from_snapshot(
		receipts, lengthof(receipts), target.spc_oid, target.db_oid, target.rel_number));

	receipts[0].state = CTRC_RECEIPT_CANCELLED;
	receipts[0].disposition = CTRC_RELEASE_CANCELLED_PREMUTATION;
	UT_ASSERT(cluster_ctrc_relation_removal_ready_from_snapshot(
		receipts, lengthof(receipts), target.spc_oid, target.db_oid, target.rel_number));

	receipts[1].target = test_exact_tid_target(UINT64_C(0x9988776655443322));
	receipts[1].state = CTRC_RECEIPT_CLEANED;
	receipts[1].disposition = CTRC_RELEASE_CLEANED_TERMINAL_REWRITE;
	UT_ASSERT(cluster_ctrc_relation_removal_ready_from_snapshot(
		receipts, lengthof(receipts), target.spc_oid, target.db_oid, target.rel_number));

	receipts[1].state = CTRC_RECEIPT_ACK_FROZEN;
	UT_ASSERT(cluster_ctrc_relation_removal_ready_from_snapshot(
		receipts, lengthof(receipts), target.spc_oid, target.db_oid, target.rel_number));

	receipts[2].target = target;
	receipts[2].target.rel_number++;
	receipts[2].state = CTRC_RECEIPT_APPLIED;
	UT_ASSERT(cluster_ctrc_relation_removal_ready_from_snapshot(
		receipts, lengthof(receipts), target.spc_oid, target.db_oid, target.rel_number));

	receipts[0].state = CTRC_RECEIPT_BLOCKED;
	UT_ASSERT(!cluster_ctrc_relation_removal_ready_from_snapshot(
		receipts, lengthof(receipts), target.spc_oid, target.db_oid, target.rel_number));

	receipts[0].state = CTRC_RECEIPT_ACK_FROZEN;
	receipts[0].disposition = CTRC_RELEASE_NONE;
	UT_ASSERT(!cluster_ctrc_relation_removal_ready_from_snapshot(
		receipts, lengthof(receipts), target.spc_oid, target.db_oid, target.rel_number));
}

/* MXA-T24: the seal selector is a byte codec, not a native struct image.
 * Every fixed routing field and the split TT key must fail closed. */
UT_TEST(test_ctrc_seal_request_codec_roundtrip_and_strict_rejection)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcTxnKeyV1 zero_epoch_key;
	ClusterCtrcTxnKeyV1 decoded_key;
	ClusterCtrcSealRequestV1 decoded;
	uint8 request[CLUSTER_CTRC_SEAL_REQUEST_BYTES];
	uint8 saved[CLUSTER_CTRC_SEAL_REQUEST_BYTES];
	static const uint8 reject_offsets[] = { 8, 32, 36, 40, 55, 60, 62, 63, 64, 68, 70 };
	size_t i;

	UT_ASSERT_EQ(GCS_BLOCK_FORWARD_KIND_CURRENT_MX_CTRC_SEAL, 11);
	UT_ASSERT_EQ(GCS_BLOCK_REPLY_CURRENT_MX_CTRC_SEAL_RESULT, 30);
	UT_ASSERT(cluster_ctrc_seal_request_encode(&key, UINT64_C(501), TEST_GRANT, TEST_SEAL,
											   TEST_CAPABILITY, CTRC_SEAL_CLOSE_AND_CLEAN, request,
											   sizeof(request)));
	UT_ASSERT_EQ(request[16], 0);
	UT_ASSERT_EQ(request[18], key.segment_id);
	UT_ASSERT_EQ(request[20], key.slot_offset + 1);
	UT_ASSERT_EQ(request[40], 0xfe);
	UT_ASSERT_EQ(request[41], 0xff);
	UT_ASSERT_EQ(request[42], 0xff);
	UT_ASSERT_EQ(request[43], 0xff);
	UT_ASSERT_EQ(request[63], 11);
	UT_ASSERT_EQ(request[128], TEST_CAPABILITY);
	UT_ASSERT_EQ(request[132], 0);
	UT_ASSERT(cluster_ctrc_seal_request_decode(request, sizeof(request), key.system_identifier,
											   key.origin_node_id, 2, key.cluster_epoch, &decoded,
											   &decoded_key));
	UT_ASSERT_EQ(memcmp(&decoded_key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(decoded.request_id, UINT64_C(501));
	UT_ASSERT_EQ(decoded.requester_backend_id, CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT);
	UT_ASSERT_EQ(decoded.grant_generation, TEST_GRANT);
	UT_ASSERT_EQ(decoded.seal_generation, TEST_SEAL);
	UT_ASSERT_EQ(decoded.participant_capability_record_generation, TEST_CAPABILITY);
	UT_ASSERT_EQ(decoded.suboperation, CTRC_SEAL_CLOSE_AND_CLEAN);
	zero_epoch_key = key;
	zero_epoch_key.cluster_epoch = 0;
	zero_epoch_key.formation_epoch = 0;
	UT_ASSERT(cluster_ctrc_seal_request_encode(
		&zero_epoch_key, UINT64_C(502), TEST_GRANT, TEST_SEAL, TEST_CAPABILITY,
		CTRC_SEAL_CLOSE_AND_CLEAN, request, sizeof(request)));
	UT_ASSERT(cluster_ctrc_seal_request_decode(
		request, sizeof(request), zero_epoch_key.system_identifier, zero_epoch_key.origin_node_id,
		2, 0, &decoded, &decoded_key));
	UT_ASSERT_EQ(memcmp(&decoded_key, &zero_epoch_key, sizeof(zero_epoch_key)), 0);
	UT_ASSERT(cluster_ctrc_seal_request_encode(&key, UINT64_C(501), TEST_GRANT, TEST_SEAL,
											   TEST_CAPABILITY, CTRC_SEAL_CLOSE_AND_CLEAN, request,
											   sizeof(request)));

	memcpy(saved, request, sizeof(saved));
	for (i = 0; i < lengthof(reject_offsets); i++) {
		memcpy(request, saved, sizeof(request));
		request[reject_offsets[i]] ^= 1;
		UT_ASSERT(!cluster_ctrc_seal_request_decode(request, sizeof(request), key.system_identifier,
													key.origin_node_id, 2, key.cluster_epoch,
													&decoded, &decoded_key));
	}
	memcpy(request, saved, sizeof(request));
	MemSet(request + 48, 0, 4);
	UT_ASSERT(!cluster_ctrc_seal_request_decode(request, sizeof(request), key.system_identifier,
												key.origin_node_id, 2, key.cluster_epoch, &decoded,
												&decoded_key));
	memcpy(request, saved, sizeof(request));
	MemSet(request + 120, 0, 8);
	UT_ASSERT(!cluster_ctrc_seal_request_decode(request, sizeof(request), key.system_identifier,
												key.origin_node_id, 2, key.cluster_epoch, &decoded,
												&decoded_key));
	memcpy(request, saved, sizeof(request));
	MemSet(request + 128, 0, 4);
	UT_ASSERT(!cluster_ctrc_seal_request_decode(request, sizeof(request), key.system_identifier,
												key.origin_node_id, 2, key.cluster_epoch, &decoded,
												&decoded_key));
	memcpy(request, saved, sizeof(request));
	request[132] = 1;
	UT_ASSERT(!cluster_ctrc_seal_request_decode(request, sizeof(request), key.system_identifier,
												key.origin_node_id, 2, key.cluster_epoch, &decoded,
												&decoded_key));
	UT_ASSERT(!cluster_ctrc_seal_request_decode(saved, sizeof(saved) - 1, key.system_identifier,
												key.origin_node_id, 2, key.cluster_epoch, &decoded,
												&decoded_key));
	UT_ASSERT(!cluster_ctrc_seal_request_decode(saved, sizeof(saved), key.system_identifier,
												key.origin_node_id + 1, 2, key.cluster_epoch,
												&decoded, &decoded_key));
	UT_ASSERT(!cluster_ctrc_seal_request_decode(saved, sizeof(saved), key.system_identifier,
												key.origin_node_id, 16, key.cluster_epoch, &decoded,
												&decoded_key));
	UT_ASSERT(!cluster_ctrc_seal_request_decode(saved, sizeof(saved), key.system_identifier,
												key.origin_node_id, 2, key.cluster_epoch + 1,
												&decoded, &decoded_key));

	UT_ASSERT(cluster_ctrc_seal_request_encode(&key, UINT64_C(502), TEST_GRANT, TEST_SEAL,
											   TEST_CAPABILITY, CTRC_SEAL_CERTIFICATE_COMMITTED,
											   request, sizeof(request)));
	UT_ASSERT(cluster_ctrc_seal_request_decode(request, sizeof(request), key.system_identifier,
											   key.origin_node_id, 2, key.cluster_epoch, &decoded,
											   &decoded_key));
	UT_ASSERT_EQ(decoded.suboperation, CTRC_SEAL_CERTIFICATE_COMMITTED);
}

/* MXA-T24/T28: the block reply validates the request digest, header CRC,
 * optional ACK CRC, result/length polarity and the entire zero tail. */
UT_TEST(test_ctrc_seal_reply_codec_binds_request_ack_and_zero_tail)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcLocalReleaseAckV1 ack = test_zero_range_ack();
	ClusterCtrcLocalReleaseAckV1 decoded_ack;
	ClusterCtrcSealReplyHeaderV1 header;
	uint8 request[CLUSTER_CTRC_SEAL_REQUEST_BYTES];
	uint8 certificate_request[CLUSTER_CTRC_SEAL_REQUEST_BYTES];
	uint8 page[BLCKSZ];
	uint8 saved[BLCKSZ];
	size_t i;

	UT_ASSERT(cluster_ctrc_seal_request_encode(&key, UINT64_C(601), TEST_GRANT, TEST_SEAL,
											   TEST_CAPABILITY, CTRC_SEAL_CLOSE_AND_CLEAN, request,
											   sizeof(request)));
	UT_ASSERT(cluster_ctrc_seal_reply_encode(request, sizeof(request), 2, key.origin_node_id,
											 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK, 0, &ack, page,
											 sizeof(page)));
	for (i = CLUSTER_CTRC_REPLY_TAIL_OFFSET; i < sizeof(page); i++)
		UT_ASSERT_EQ(page[i], 0);
	UT_ASSERT(cluster_ctrc_seal_reply_decode(page, sizeof(page), request, sizeof(request), 2,
											 key.origin_node_id, &header, &decoded_ack));
	UT_ASSERT_EQ(header.result, CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
	UT_ASSERT_EQ(header.ack_length, CLUSTER_CTRC_LOCAL_ACK_BYTES);
	UT_ASSERT_EQ(header.body_length, BLCKSZ);
	UT_ASSERT_EQ(memcmp(&decoded_ack.transaction_key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ(decoded_ack.participant_node_id, 2);
	UT_ASSERT_NE(decoded_ack.crc32c, 0);

	memcpy(saved, page, sizeof(saved));
	page[60] ^= 1;
	UT_ASSERT(!cluster_ctrc_seal_reply_decode(page, sizeof(page), request, sizeof(request), 2,
											  key.origin_node_id, &header, &decoded_ack));
	memcpy(page, saved, sizeof(page));
	page[64 + 412] ^= 1;
	UT_ASSERT(!cluster_ctrc_seal_reply_decode(page, sizeof(page), request, sizeof(request), 2,
											  key.origin_node_id, &header, &decoded_ack));
	memcpy(page, saved, sizeof(page));
	page[CLUSTER_CTRC_REPLY_TAIL_OFFSET] = 1;
	UT_ASSERT(!cluster_ctrc_seal_reply_decode(page, sizeof(page), request, sizeof(request), 2,
											  key.origin_node_id, &header, &decoded_ack));
	memcpy(page, saved, sizeof(page));
	page[48] ^= 1;
	test_reseal_crc32c(page, CLUSTER_CTRC_REPLY_HEADER_BYTES, 60);
	UT_ASSERT(!cluster_ctrc_seal_reply_decode(page, sizeof(page), request, sizeof(request), 2,
											  key.origin_node_id, &header, &decoded_ack));
	memcpy(page, saved, sizeof(page));
	page[40] = CTRC_SEAL_REPLY_PENDING_DRAIN;
	test_reseal_crc32c(page, CLUSTER_CTRC_REPLY_HEADER_BYTES, 60);
	UT_ASSERT(!cluster_ctrc_seal_reply_decode(page, sizeof(page), request, sizeof(request), 2,
											  key.origin_node_id, &header, &decoded_ack));
	memcpy(page, saved, sizeof(page));
	page[64 + 392] = 1;
	test_reseal_crc32c(page + 64, CLUSTER_CTRC_LOCAL_ACK_BYTES, 412);
	UT_ASSERT(!cluster_ctrc_seal_reply_decode(page, sizeof(page), request, sizeof(request), 2,
											  key.origin_node_id, &header, &decoded_ack));

	UT_ASSERT(!cluster_ctrc_seal_reply_encode(request, sizeof(request), 2, key.origin_node_id,
											  CTRC_SEAL_REPLY_PENDING_DRAIN, 0, NULL, page,
											  sizeof(page)));
	UT_ASSERT(cluster_ctrc_seal_reply_encode(request, sizeof(request), 2, key.origin_node_id,
											 CTRC_SEAL_REPLY_PENDING_DRAIN, 7, NULL, page,
											 sizeof(page)));
	UT_ASSERT(cluster_ctrc_seal_reply_decode(page, sizeof(page), request, sizeof(request), 2,
											 key.origin_node_id, &header, &decoded_ack));
	UT_ASSERT_EQ(header.ack_length, 0);
	UT_ASSERT_EQ(memcmp(&decoded_ack, &(ClusterCtrcLocalReleaseAckV1){ 0 }, sizeof(decoded_ack)),
				 0);

	UT_ASSERT(cluster_ctrc_seal_request_encode(&key, UINT64_C(602), TEST_GRANT, TEST_SEAL,
											   TEST_CAPABILITY, CTRC_SEAL_CERTIFICATE_COMMITTED,
											   certificate_request, sizeof(certificate_request)));
	UT_ASSERT(cluster_ctrc_seal_reply_encode(
		certificate_request, sizeof(certificate_request), 2, key.origin_node_id,
		CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED, 0, NULL, page, sizeof(page)));
	UT_ASSERT(cluster_ctrc_seal_reply_decode(page, sizeof(page), certificate_request,
											 sizeof(certificate_request), 2, key.origin_node_id,
											 &header, &decoded_ack));
	UT_ASSERT_EQ(header.result, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
	UT_ASSERT(!cluster_ctrc_seal_reply_encode(certificate_request, sizeof(certificate_request), 2,
											  key.origin_node_id, CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK,
											  0, &ack, page, sizeof(page)));
}

/* MXA-T28: a touched participant is immutable for one grant. */
UT_TEST(test_ctrc_participant_boot_capability_and_membership_loss_retain)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);

	test_open_participant(&participant);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_ACK_READY);
	UT_ASSERT_EQ(participant.seal_generation, TEST_SEAL);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);

	test_open_participant(&participant);
	identity.boot_incarnation++;
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);

	identity = test_participant_identity(2);
	test_open_participant(&participant);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL + 1),
				 CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);
}

/* MXA-T24/T28: CLOSE may precede the participant's first local proof.  The
 * exact request identity creates the sole legal N=0 tombstone and becomes
 * immutable; no local capability resampling may rewrite it. */
UT_TEST(test_ctrc_close_before_proof_creates_exact_zero_range_tombstone)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);

	MemSet(&participant, 0, sizeof(participant));
	UT_ASSERT_EQ(cluster_ctrc_participant_close_or_tombstone(&participant, &key, &identity,
															 TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_ACK_READY);
	UT_ASSERT_EQ(participant.receipt_count, 0);
	UT_ASSERT_EQ(participant.next_key_sequence, 1);
	UT_ASSERT_EQ(participant.identity.capability_record_generation, TEST_CAPABILITY);
	UT_ASSERT_EQ(cluster_ctrc_participant_close_or_tombstone(&participant, &key, &identity,
															 TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);

	identity.capability_record_generation++;
	UT_ASSERT_EQ(cluster_ctrc_participant_close_or_tombstone(&participant, &key, &identity,
															 TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);
}

/* MXA-T24: a zero-range ACK is frozen byte-for-byte across duplicate CLOSE,
 * and only its exact certificate notification may reclaim the tombstone. */
UT_TEST(test_ctrc_zero_range_request_is_idempotent_until_exact_certificate)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcLocalReleaseAckV1 summary;
	ClusterCtrcLocalReleaseAckV1 first;
	ClusterCtrcLocalReleaseAckV1 duplicate;
	uint16 reason = UINT16_MAX;

	MemSet(&participant, 0, sizeof(participant));
	MemSet(&summary, 0, sizeof(summary));
	UT_ASSERT_EQ(cluster_ctrc_participant_request_apply(&participant, &summary, &key, &identity,
														TEST_GRANT, TEST_SEAL,
														CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &first),
				 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
	UT_ASSERT_EQ(reason, 0);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_ACK_FROZEN);
	UT_ASSERT_EQ(cluster_ctrc_participant_request_apply(
					 &participant, &summary, &key, &identity, TEST_GRANT, TEST_SEAL,
					 CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &duplicate),
				 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
	UT_ASSERT_EQ(memcmp(&first, &duplicate, sizeof(first)), 0);

	UT_ASSERT_EQ(cluster_ctrc_participant_request_apply(
					 &participant, &summary, &key, &identity, TEST_GRANT, TEST_SEAL + 1,
					 CTRC_SEAL_CERTIFICATE_COMMITTED, &reason, &duplicate),
				 CTRC_SEAL_REPLY_BLOCKED_RETAIN);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_ACK_FROZEN);
	UT_ASSERT_EQ(cluster_ctrc_participant_request_apply(
					 &participant, &summary, &key, &identity, TEST_GRANT, TEST_SEAL,
					 CTRC_SEAL_CERTIFICATE_COMMITTED, &reason, &duplicate),
				 CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
	UT_ASSERT_EQ(reason, 0);
	UT_ASSERT_EQ(memcmp(&participant, &(ClusterCtrcParticipantEntry){ 0 }, sizeof(participant)), 0);
	UT_ASSERT_EQ(memcmp(&summary, &(ClusterCtrcLocalReleaseAckV1){ 0 }, sizeof(summary)), 0);
}

/* MXA-T24/T30: an ACK is accepted only for the request identity actually
 * dispatched to that frozen touched participant. */
UT_TEST(test_ctrc_origin_ack_lands_only_exact_dispatched_touch)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcOriginEntry origin;
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcLocalReleaseAckV1 participant_summary;
	ClusterCtrcLocalReleaseAckV1 ack;
	ClusterCtrcLocalReleaseAckV1 landed;
	uint32 grant = 0;
	uint16 reason = 0;

	MemSet(&origin, 0, sizeof(origin));
	MemSet(&participant, 0, sizeof(participant));
	MemSet(&participant_summary, 0, sizeof(participant_summary));
	MemSet(&landed, 0, sizeof(landed));
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&origin, &key, TEST_GRANT),
				 CLUSTER_CTRC_ORIGIN_OPENED);
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &identity, CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(grant, TEST_GRANT);
	origin.state = CTRC_ORIGIN_SEALING;
	origin.seal_generation = TEST_SEAL;
	origin.close_dispatched_bitmap = UINT32_C(1) << identity.node_id;
	origin.close_request_id[identity.node_id] = UINT64_C(901);
	UT_ASSERT_EQ(cluster_ctrc_participant_request_apply(&participant, &participant_summary, &key,
														&identity, TEST_GRANT, TEST_SEAL,
														CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
				 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);

	UT_ASSERT(!cluster_ctrc_origin_ack_land_entry(&origin, UINT64_C(900), &ack, &landed));
	UT_ASSERT_EQ(origin.ack_bitmap, 0);
	UT_ASSERT(cluster_ctrc_origin_ack_land_entry(&origin, UINT64_C(901), &ack, &landed));
	UT_ASSERT_EQ(origin.ack_bitmap, UINT32_C(1) << identity.node_id);
	UT_ASSERT_EQ(memcmp(&ack, &landed, sizeof(ack)), 0);
	UT_ASSERT(cluster_ctrc_origin_ack_land_entry(&origin, UINT64_C(901), &ack, &landed));

	ack.crc32c++;
	UT_ASSERT(!cluster_ctrc_origin_ack_land_entry(&origin, UINT64_C(901), &ack, &landed));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_BLOCKED);
}

/* MXA-T24: dispatch publication, participant close confirmation and the
 * final immutable ACK are three distinct facts.  A PENDING_DRAIN reply closes
 * one participant but cannot stand in for its release ACK. */
UT_TEST(test_ctrc_origin_seal_fsm_requires_every_exact_close_confirmation)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcOriginEntry origin;
	ClusterCtrcParticipantIdentity first = test_participant_identity(1);
	ClusterCtrcParticipantIdentity second = test_participant_identity(2);
	uint32 grant = 0;

	MemSet(&origin, 0, sizeof(origin));
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&origin, &key, TEST_GRANT),
				 CLUSTER_CTRC_ORIGIN_OPENED);
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &first, CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &second, CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT(cluster_ctrc_origin_begin_seal_entry(&origin, TEST_SEAL));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_SEALING);
	UT_ASSERT_EQ(origin.close_dispatched_bitmap, 0);
	UT_ASSERT_EQ(origin.close_confirmed_bitmap, 0);

	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(&origin, first.node_id, 501));
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(&origin, second.node_id, 502));
	UT_ASSERT(!cluster_ctrc_origin_note_close_reply_entry(&origin, first.node_id, 999,
														  CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT_EQ(origin.close_confirmed_bitmap, 0);
	UT_ASSERT(cluster_ctrc_origin_note_close_reply_entry(&origin, first.node_id, 501,
														 CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_SEALING);
	UT_ASSERT_EQ(origin.close_confirmed_bitmap, UINT32_C(1) << first.node_id);
	UT_ASSERT(cluster_ctrc_origin_note_close_reply_entry(&origin, second.node_id, 502,
														 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_SEALED);
	UT_ASSERT_EQ(origin.close_confirmed_bitmap, origin.touched_bitmap);
	UT_ASSERT_EQ(origin.ack_bitmap, 0);
	UT_ASSERT(cluster_ctrc_origin_begin_cleaning_entry(&origin));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_CLEANING);
}

/* MXA-K13/T24: the existing undo cleaner is the only progress process.  It
 * samples the canonical terminal under block-zero SCUR before beginning the
 * seal, then dispatches through the existing GCS DATA continuation. */
UT_TEST(test_ctrc_existing_undo_cleaner_owns_terminal_seal_progress)
{
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_undo_cleaner.c",
								   "cluster_ctrc_cleaner_run_pass()"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "CLUSTER_UNDO_BLOCK0_SCUR"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "cluster_ctrc_origin_begin_seal_shared(&key)"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "cluster_gcs_ctrc_dispatch_batch(dispatches, dispatch_count)"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "cluster_undo_block0_current_acquire_begin_ctrc_release("));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "cluster_undo_xlog_insert_tt_ctrc_release(&record)"));
	/* The certificate keeps its EXCLUSIVE resident pin through the final
	 * durability recheck.  Re-entering the sampling API there would acquire
	 * the same non-reentrant ClusterUndoBuf lock a second time. */
	UT_ASSERT_EQ(
		source_file_occurrences("src/backend/cluster/cluster_terminal_ref_census.c",
								"cluster_undo_block0_current_sample_generation_exclusive("),
		1);
	UT_ASSERT(source_file_contains(
		"src/backend/cluster/cluster_terminal_ref_census.c",
		"cluster_undo_block0_generation_matches(&pin.observed_generation, &generation)"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "ctrc_participant_ack_entries()"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "ctrc_origin_ack_entries()"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "CTRC_SEAL_CERTIFICATE_COMMITTED"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_tt_slot.c",
								   "cluster_undo_cleaner_wakeup()"));
}

/* A terminal transition wakes the first pass, but the CTRC close/ACK/
 * certificate chain can span later passes.  Every durable shared-state edge
 * must therefore re-arm the sole cleaner; a publisher waiting for an old
 * RELEASE_PROVEN row polls only the CTRC row and never churns Resource-X. */
UT_TEST(test_ctrc_release_overlap_progress_is_event_driven_without_xcur_churn)
{
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "cluster_ctrc_origin_release_overlap_pending("));
	UT_ASSERT(
		source_file_contains("src/backend/cluster/cluster_tt_durable.c",
							 "while (cluster_ctrc_origin_release_overlap_pending(&ctrc_key))"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_tt_durable.c",
								   "cluster_undo_cleaner_wakeup();"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "progressed = landed && ack_before != origin->ack_bitmap;"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "confirmed_before != matched->close_confirmed_bitmap"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_NONE);\n"
								   "\t\t\t\tcluster_undo_cleaner_wakeup();"));
	UT_ASSERT(
		source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
							 "RELEASE_PROVEN still needs the participant-summary notification"));
	UT_ASSERT(source_file_occurrences("src/backend/cluster/cluster_terminal_ref_census.c",
									  "ctrc_semantic_progress(true);")
			  >= 4);
	/* Latches coalesce notifications, so one wake cannot stand in for an
	 * unbounded number of queued local state edges.  A completed local edge
	 * keeps the sole cleaner running immediately; a remote transport enqueue
	 * remains merely in flight and must wait for its reply wakeup. */
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_undo_cleaner.c",
								   "ctrc_local_progress = cluster_ctrc_cleaner_run_pass();"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_undo_cleaner.c",
								   "*out_work_remaining = ctrc_local_progress;"));
	UT_ASSERT(source_file_contains("src/backend/cluster/cluster_terminal_ref_census.c",
								   "CtrcLocalProgress != progress_before"));
	UT_ASSERT(!source_file_contains(
		"src/backend/cluster/cluster_terminal_ref_census.c",
		"cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS) != progress_before"));
	UT_ASSERT(!source_file_contains(
		"src/backend/cluster/cluster_terminal_ref_census.c",
		"progressed = cluster_gcs_ctrc_dispatch_close(&dispatch) || progressed;"));
}

/* MXA-T36: the undo cleaner is an auxiliary process, so a normal pre-mutation
 * Resource-X wait must return to its next pass instead of escaping as ERROR
 * and taking down the instance.  The storage boundary may soften
 * BAD_STATE/NOT_FOUND and stale authority before content-X is held; every
 * caller-owned pin and semantic admission is then released explicitly. */
UT_TEST(test_ctrc_cleaner_resource_x_not_ready_is_a_clean_retry)
{
	UT_ASSERT(
		source_file_contains("src/include/storage/bufmgr.h",
							 "extern bool ClusterLockBufferExclusiveRetryAware(Buffer buffer);"));
	UT_ASSERT(source_file_contains(
		"src/backend/storage/buffer/bufmgr.c",
		"result == RESOURCE_X_APPLY_BAD_STATE || result == RESOURCE_X_APPLY_NOT_FOUND\n"
		"\t\t\t\t || (result == RESOURCE_X_APPLY_STALE && aux_context == NULL)"));
	UT_ASSERT(source_file_contains("src/backend/storage/buffer/bufmgr.c",
								   "*pcm_x_transient_refused = true;"));
	UT_ASSERT(source_file_contains("src/backend/storage/buffer/bufmgr.c",
								   "ClusterLockBufferExclusiveRetryAware(Buffer buffer)"));
	UT_ASSERT_EQ(source_file_occurrences("src/backend/cluster/cluster_terminal_ref_census.c",
										 "ClusterLockBufferExclusiveRetryAware(buffer)"),
				 5);
	UT_ASSERT_EQ(source_file_occurrences("src/backend/cluster/cluster_terminal_ref_census.c",
										 "LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);"),
				 0);
	UT_ASSERT_EQ(source_file_occurrences("src/backend/cluster/cluster_terminal_ref_census.c",
										 "if (!ClusterLockBufferExclusiveRetryAware(buffer)) {\n"
										 "\t\tReleaseBuffer(buffer);\n"
										 "\t\tcluster_semantic_activation_leave(&admission);\n"
										 "\t\treturn false;\n"
										 "\t}"),
				 4);
	UT_ASSERT_EQ(source_file_occurrences("src/backend/cluster/cluster_terminal_ref_census.c",
										 "if (!ClusterLockBufferExclusiveRetryAware(buffer)) {\n"
										 "\t\t\tReleaseBuffer(buffer);\n"
										 "\t\t\tcluster_semantic_activation_leave(&admission);\n"
										 "\t\t\treturn false;\n"
										 "\t\t}"),
				 1);
}

/* MXA-T29: inter-key global gaps do not affect the exact local 1..N range;
 * local gaps, non-durable dependencies and non-tombstone N=0 all refuse. */
UT_TEST(test_ctrc_local_release_ack_range_digest_and_dependency_durability)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcLocalReleaseAckV1 ack;
	ClusterCtrcDurability durability;

	test_open_participant(&participant);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	MemSet(&durability, 0, sizeof(durability));
	UT_ASSERT_EQ(cluster_ctrc_participant_build_ack(&participant, &durability, &ack),
				 CLUSTER_CTRC_ACK_RELEASED);
	UT_ASSERT_EQ(sizeof(ack), CLUSTER_CTRC_LOCAL_ACK_BYTES);
	UT_ASSERT_EQ(ack.total_receipt_count, 0);
	UT_ASSERT((ack.flags & CTRC_ACK_FLAG_ZERO_RANGE) != 0);
	UT_ASSERT_EQ(
		memcmp(ack.row_digest_sha256, cluster_ctrc_empty_sha256, sizeof(ack.row_digest_sha256)), 0);

	test_open_participant(&participant);
	participant.last_key_sequence = 2;
	participant.receipt_count = 1;
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity, TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN);
}

/* MXA-T29: nonempty rows are sorted by key-local sequence, encoded through
 * the frozen TLVs, and frozen only after every local/foreign dependency is
 * durably covered. */
UT_TEST(test_ctrc_nonempty_ack_uses_canonical_rows_and_durable_vector)
{
	static const uint8 expected_digest[32]
		= { 0x27, 0x6a, 0x51, 0x38, 0x1d, 0x08, 0xab, 0x3d, 0x60, 0xc0, 0x54,
			0x04, 0xb5, 0x14, 0x31, 0x24, 0x71, 0x02, 0x76, 0x1f, 0x41, 0x5a,
			0xd3, 0xf5, 0xa3, 0xdf, 0xcd, 0xf5, 0x36, 0x7e, 0x58, 0xad };
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcReceipt receipts[2];
	ClusterCtrcDurability durability;
	ClusterCtrcLocalReleaseAckV1 ack;
	ClusterCtrcLocalReleaseAckV1 summary;
	uint16 first_reason = 0;

	test_open_participant(&participant);
	participant.state = CTRC_PARTICIPANT_ACK_READY;
	participant.seal_generation = TEST_SEAL;
	participant.next_key_sequence = 3;
	participant.last_key_sequence = 2;
	participant.receipt_count = 2;
	participant.cancelled_count = 1;
	participant.cleaned_count = 1;
	MemSet(receipts, 0, sizeof(receipts));

	/* Deliberately place key sequence 2 first: digest order is 1..N. */
	receipts[0].key = participant.key;
	receipts[0].publication
		= test_publication(72, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	receipts[0].publication.journal_sequence = 29;
	receipts[0].publication.key_sequence = 2;
	receipts[0].publication.journal_slot_generation = 102;
	receipts[0].target = test_exact_itl_target();
	receipts[0].state = CTRC_RECEIPT_CLEANED;
	receipts[0].disposition = CTRC_RELEASE_CLEANED_TERMINAL_REWRITE;
	receipts[0].highest_local_wal_lsn = 300;
	receipts[0].required_lsn[4] = 400;

	receipts[1].key = participant.key;
	receipts[1].publication
		= test_publication(71, CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	receipts[1].publication.journal_sequence = 11;
	receipts[1].publication.key_sequence = 1;
	receipts[1].publication.journal_slot_generation = 101;
	receipts[1].target = test_pending_itl_target();
	receipts[1].state = CTRC_RECEIPT_CANCELLED;
	receipts[1].disposition = CTRC_RELEASE_CANCELLED_PREMUTATION;
	MemSet(&summary, 0, sizeof(summary));
	UT_ASSERT_EQ(cluster_ctrc_participant_request_apply(
					 &participant, &summary, &participant.key, &participant.identity, TEST_GRANT,
					 TEST_SEAL, CTRC_SEAL_CLOSE_AND_CLEAN, &first_reason, &ack),
				 CTRC_SEAL_REPLY_PENDING_DRAIN);
	UT_ASSERT_EQ(first_reason, CTRC_SEAL_REASON_CLEANOUT);

	MemSet(&durability, 0, sizeof(durability));
	durability.local_flush_lsn = 300;
	durability.durable_lsn[4] = 399;
	UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&participant, receipts,
															lengthof(receipts), &durability, &ack),
				 CLUSTER_CTRC_ACK_DENIED);
	durability.durable_lsn[4] = 400;
	UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&participant, receipts,
															lengthof(receipts), &durability, &ack),
				 CLUSTER_CTRC_ACK_RELEASED);
	UT_ASSERT_EQ(receipts[0].publication.key_sequence, UINT64_C(1));
	UT_ASSERT_EQ(receipts[1].publication.key_sequence, UINT64_C(2));
	UT_ASSERT_EQ(ack.first_key_sequence, UINT64_C(1));
	UT_ASSERT_EQ(ack.last_key_sequence, UINT64_C(2));
	UT_ASSERT_EQ(ack.minimum_journal_sequence, UINT64_C(11));
	UT_ASSERT_EQ(ack.maximum_journal_sequence, UINT64_C(29));
	UT_ASSERT_EQ(ack.total_receipt_count, UINT64_C(2));
	UT_ASSERT_EQ(ack.cancelled_count, UINT64_C(1));
	UT_ASSERT_EQ(ack.cleaned_count, UINT64_C(1));
	UT_ASSERT_EQ(ack.ack_frozen_count, UINT64_C(2));
	UT_ASSERT_EQ(ack.highest_local_cleanout_lsn, UINT64_C(300));
	UT_ASSERT_EQ(ack.required_lsn_vector[4], UINT64_C(400));
	UT_ASSERT_EQ(memcmp(ack.row_digest_sha256, expected_digest, sizeof(expected_digest)), 0);
	UT_ASSERT_NE(ack.crc32c, 0);

	/* A CLEANED disposition can never hide an unfinalized target. */
	receipts[1].target = test_pending_itl_target();
	receipts[1].state = CTRC_RECEIPT_CLEANED;
	receipts[1].disposition = CTRC_RELEASE_CLEANED_ABSENT;
	UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&participant, receipts,
															lengthof(receipts), &durability, &ack),
				 CLUSTER_CTRC_ACK_DENIED);
}

/* MXA-T30: the certificate input is a set, not a quorum. */
UT_TEST(test_ctrc_origin_certificate_requires_exact_complete_sorted_ack_set)
{
	static const uint8 expected_digest[32]
		= { 0x44, 0x99, 0x91, 0xe3, 0x0b, 0x72, 0xd5, 0x0e, 0x94, 0xb5, 0xbb,
			0x9f, 0x8d, 0xe3, 0x5b, 0xca, 0x17, 0x76, 0x9e, 0x0f, 0x49, 0xf9,
			0x01, 0x01, 0x4c, 0x2e, 0xf3, 0x9e, 0x9b, 0xdf, 0xa3, 0x9c };
	ClusterCtrcCertificateInput input;
	ClusterCtrcLocalReleaseAckV1 acks[2];
	ClusterCtrcLocalReleaseAckV1 ack_slots[CLUSTER_CTRC_MAX_PARTICIPANTS];
	ClusterCtrcOriginCertificateSnapshot snapshot;
	ClusterCtrcOriginEntry origin;
	ClusterCtrcParticipantIdentity identity;
	ClusterCtrcLocalReleaseAckV1 saved;
	uint8 digest[32];
	uint32 grant = 0;

	MemSet(&input, 0, sizeof(input));
	acks[0] = test_zero_range_ack_for_node(2);
	acks[1] = test_zero_range_ack_for_node(1);
	input.acks = acks;
	input.ack_count = 1;
	input.frozen_touched_bitmap = UINT64_C(0x6);
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input), CLUSTER_CTRC_CERTIFICATE_RETAIN);
	input.ack_count = 2;
	input.seal_generation = TEST_SEAL;
	input.block0_terminal_exact = true;
	input.all_dependencies_durable = true;
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input), CLUSTER_CTRC_CERTIFICATE_READY);
	UT_ASSERT(cluster_ctrc_origin_certificate_digest(&input, digest));
	UT_ASSERT_EQ(memcmp(digest, expected_digest, sizeof(digest)), 0);

	/* The lock-free certificate phase carries an immutable exact origin/ACK
	 * snapshot; only that same origin image may publish RELEASE_PROVEN. */
	test_open_origin(&origin);
	identity = test_participant_identity(1);
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &identity, CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_RECORDED);
	identity = test_participant_identity(2);
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &identity, CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT(cluster_ctrc_origin_begin_seal_entry(&origin, TEST_SEAL));
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(&origin, 1, 101));
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(&origin, 2, 102));
	UT_ASSERT(
		cluster_ctrc_origin_note_close_reply_entry(&origin, 1, 101, CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT(
		cluster_ctrc_origin_note_close_reply_entry(&origin, 2, 102, CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT(cluster_ctrc_origin_begin_cleaning_entry(&origin));
	MemSet(ack_slots, 0, sizeof(ack_slots));
	UT_ASSERT(cluster_ctrc_origin_ack_land_entry(&origin, 101, &acks[1], &ack_slots[1]));
	UT_ASSERT(cluster_ctrc_origin_ack_land_entry(&origin, 102, &acks[0], &ack_slots[2]));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_CERTIFYING);
	UT_ASSERT(cluster_ctrc_origin_certificate_snapshot_entry(&origin, ack_slots, 7, &snapshot));
	UT_ASSERT_EQ(snapshot.origin_index, UINT64_C(7));
	UT_ASSERT_EQ(snapshot.ack_count, 2);
	UT_ASSERT_EQ(snapshot.acks[0].participant_node_id, 1);
	UT_ASSERT_EQ(snapshot.acks[1].participant_node_id, 2);
	UT_ASSERT(cluster_ctrc_origin_certificate_commit_entry(&origin, &snapshot));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT_EQ(origin.close_dispatched_bitmap, 0);
	UT_ASSERT_EQ(origin.close_confirmed_bitmap, 0);
	UT_ASSERT_EQ(memcmp(origin.close_request_id, (uint64[CLUSTER_CTRC_MAX_PARTICIPANTS]){ 0 },
						sizeof(origin.close_request_id)),
				 0);
	UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(&origin, 1, 201));
	UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(&origin, 2, 202));
	UT_ASSERT(!cluster_ctrc_origin_note_certificate_reply_entry(&origin, 1, 201,
																CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_entry(
		&origin, 1, 201, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	UT_ASSERT_EQ(origin.close_confirmed_bitmap, UINT32_C(1) << 1);
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_entry(
		&origin, 2, 202, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	UT_ASSERT_EQ(origin.close_confirmed_bitmap, origin.touched_bitmap);

	saved = acks[1];
	acks[1] = acks[0];
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input), CLUSTER_CTRC_CERTIFICATE_RETAIN);
	acks[1] = saved;
	input.frozen_touched_bitmap |= UINT64_C(1) << 3;
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input), CLUSTER_CTRC_CERTIFICATE_RETAIN);
	input.frozen_touched_bitmap = UINT64_C(0x6);
	acks[1].transaction_key.slot_wrap++;
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input), CLUSTER_CTRC_CERTIFICATE_RETAIN);
	acks[1] = saved;
	acks[1].crc32c++;
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input), CLUSTER_CTRC_CERTIFICATE_RETAIN);
	acks[1] = saved;
	input.all_dependencies_durable = false;
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input), CLUSTER_CTRC_CERTIFICATE_RETAIN);
}

/* MXA-T31: COMMITTED retains the independent SCN horizon; ABORTED does not
 * invent one, but both require the canonical release bit. */
UT_TEST(test_ctrc_release_certificate_enables_exact_l11_l12_and_ack_reclaim)
{
	ClusterCtrcRecycleInput input;

	MemSet(&input, 0, sizeof(input));
	input.status = CTRC_TERMINAL_COMMITTED;
	input.release_proven = true;
	input.commit_scn = 100;
	input.horizon_valid = true;
	input.horizon_scn = 99;
	UT_ASSERT(!cluster_ctrc_terminal_recyclable(&input));
	input.horizon_scn = 100;
	UT_ASSERT(cluster_ctrc_terminal_recyclable(&input));
	/* Node-id encoding is not temporal order: compare local SCN time. */
	input.commit_scn = scn_encode(7, 100);
	input.horizon_scn = scn_encode(1, 100);
	UT_ASSERT(cluster_ctrc_terminal_recyclable(&input));
	input.release_proven = false;
	UT_ASSERT(!cluster_ctrc_terminal_recyclable(&input));

	MemSet(&input, 0, sizeof(input));
	input.status = CTRC_TERMINAL_ABORTED;
	input.release_proven = true;
	input.durable_aborted = true;
	UT_ASSERT(cluster_ctrc_terminal_recyclable(&input));
	input.release_proven = false;
	UT_ASSERT(!cluster_ctrc_terminal_recyclable(&input));
}

/* MXA-T32: only the durable certificate cut releases.  Earlier cuts retain,
 * and the later participant notification affects reclamation only. */
UT_TEST(test_ctrc_crash_cut_matrix_releases_only_after_durable_certificate)
{
	int cut;

	for (cut = CTRC_CRASH_BEFORE_TOUCH; cut <= CTRC_CRASH_ORIGIN_PRECERT_LOSS; cut++) {
		ClusterCtrcCrashDisposition expected
			= cut == CTRC_CRASH_DURABLE_CERTIFICATE_BEFORE_NOTIFICATION
				  ? CLUSTER_CTRC_CRASH_RELEASE_PROVEN
				  : CLUSTER_CTRC_CRASH_RETAIN;

		UT_ASSERT_EQ(cluster_ctrc_crash_cut_disposition((ClusterCtrcCrashCut)cut), expected);
	}
}

/* MXA-T33: the release WAL is a field-coded 96-byte certificate and may
 * change only the exact terminal predecessor's release bit. */
UT_TEST(test_ctrc_release_wal_codec_and_exact_redo_matrix)
{
	xl_undo_tt_slot_ctrc_release_v1 record;
	xl_undo_tt_slot_ctrc_release_v1 decoded;
	uint8 bytes[CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES];
	TTSlot slot;

	MemSet(&record, 0, sizeof(record));
	record.segment_id = 17;
	record.segment_generation = 5;
	record.xid = 7001;
	record.cluster_epoch = 13;
	record.root_id = 37;
	record.root_generation = 43;
	record.formation_epoch = TEST_FORMATION;
	record.admission_record_generation = TEST_ADMISSION;
	record.seal_generation = TEST_SEAL;
	record.touched_nodes_low = UINT64_C(0x6);
	MemSet(record.ack_set_digest, 0x5a, sizeof(record.ack_set_digest));
	record.slot_offset = 3;
	record.slot_wrap = 9;
	record.owner_instance = 1;
	record.terminal_status = TT_SLOT_COMMITTED;
	record.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
	record.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
	UT_ASSERT(cluster_undo_tt_ctrc_release_encode(&record, bytes));
	UT_ASSERT_EQ(sizeof(record), CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES);
	UT_ASSERT(cluster_undo_tt_ctrc_release_decode(bytes, &decoded));
	UT_ASSERT_EQ(memcmp(&record, &decoded, sizeof(record)), 0);
	record.cluster_epoch = 0;
	record.formation_epoch = 0;
	record.segment_generation = 0;
	UT_ASSERT(cluster_undo_tt_ctrc_release_encode(&record, bytes));
	UT_ASSERT(cluster_undo_tt_ctrc_release_decode(bytes, &decoded));
	UT_ASSERT_EQ(memcmp(&record, &decoded, sizeof(record)), 0);
	record.cluster_epoch = 13;
	record.formation_epoch = TEST_FORMATION;
	record.segment_generation = 5;
	UT_ASSERT(cluster_undo_tt_ctrc_release_encode(&record, bytes));
	bytes[95] |= UINT8_C(0x80);
	UT_ASSERT(!cluster_undo_tt_ctrc_release_decode(bytes, &decoded));

	MemSet(&slot, 0, sizeof(slot));
	slot.xid = record.xid;
	slot.wrap = record.slot_wrap;
	slot.status = record.terminal_status;
	slot.commit_scn = 101;
	UT_ASSERT_EQ(cluster_undo_tt_ctrc_release_redo_decide(5, &slot, &record),
				 CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_APPLY);
	slot.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	UT_ASSERT_EQ(cluster_undo_tt_ctrc_release_redo_decide(5, &slot, &record),
				 CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_IDEMPOTENT);
	slot.flags = 0;
	UT_ASSERT_EQ(cluster_undo_tt_ctrc_release_redo_decide(6, &slot, &record),
				 CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_SKIP_STALE);
	slot.wrap++;
	UT_ASSERT_EQ(cluster_undo_tt_ctrc_release_redo_decide(5, &slot, &record),
				 CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_SKIP_STALE);
	slot.wrap = record.slot_wrap;
	slot.xid++;
	UT_ASSERT_EQ(cluster_undo_tt_ctrc_release_redo_decide(5, &slot, &record),
				 CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_CONFLICT);
}

/* MXA-T34: integer overflow or a table smaller than the frozen formula must
 * block activation; runtime full never authorizes eviction. */
UT_TEST(test_ctrc_capacity_is_activation_sized_and_runtime_full_refuses_before_mutation)
{
	ClusterCtrcCapacity capacity;
	ClusterCtrcCapacity smaller;
	Size node_delta;

	UT_ASSERT(cluster_ctrc_capacity_compute(131072, 512, 4, &capacity));
	UT_ASSERT_EQ(capacity.origin_key_entries,
				 CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(capacity.participant_key_entries, capacity.origin_key_entries * 4);
	UT_ASSERT_EQ(capacity.receipt_entries,
				 UINT64_C(131072) + UINT64_C(512) * CLUSTER_CURRENT_MX_MAX_MEMBERS);
	UT_ASSERT_EQ(capacity.participant_ack_summary_entries, capacity.participant_key_entries);
	UT_ASSERT_EQ(capacity.origin_ack_inbox_entries, capacity.participant_key_entries);
	UT_ASSERT(cluster_ctrc_capacity_compute(131072, 512, 3, &smaller));
	node_delta
		= (Size)capacity.origin_key_entries
		  * (sizeof(ClusterCtrcParticipantEntry) + sizeof(uint64) /* local receipt-chain head */
			 + 2 * sizeof(ClusterCtrcLocalReleaseAckV1));
	UT_ASSERT_EQ(capacity.total_bytes - smaller.total_bytes, node_delta);
	UT_ASSERT(!cluster_ctrc_capacity_compute(SIZE_MAX, INT_MAX, 16, &capacity));
	UT_ASSERT_EQ(cluster_ctrc_runtime_full_disposition(),
				 CLUSTER_CTRC_CAPACITY_REFUSE_BEFORE_MUTATION);
}

UT_TEST(test_ctrc_participant_index_uses_participant_not_origin_node)
{
	uint64 index = UINT64_MAX;

	/* One origin row owns one independent participant row per node. */
	UT_ASSERT(cluster_ctrc_participant_index_compute(7, 32, 128, 0, &index));
	UT_ASSERT_EQ(index, UINT64_C(28));
	UT_ASSERT(cluster_ctrc_participant_index_compute(7, 32, 128, 3, &index));
	UT_ASSERT_EQ(index, UINT64_C(31));

	UT_ASSERT(!cluster_ctrc_participant_index_compute(32, 32, 128, 0, &index));
	UT_ASSERT(!cluster_ctrc_participant_index_compute(7, 32, 128, 4, &index));
	UT_ASSERT(!cluster_ctrc_participant_index_compute(7, 32, 127, 0, &index));
	UT_ASSERT(!cluster_ctrc_participant_index_compute(7, 0, 0, 0, &index));
}

static bool
source_class_known(const char *source_class)
{
	static const char *const known[] = {
		"REGISTERED_REFERENCE",	   "TERMINAL_PROJECTION_DISCHARGE", "SUCCESSOR_BEFORE_PREDECESSOR",
		"PROVEN_LOCAL_NONCLUSTER", "FAIL_CLOSED_UNREACHABLE",
	};
	size_t i;

	for (i = 0; i < lengthof(known); i++)
		if (strcmp(source_class, known[i]) == 0)
			return true;
	return false;
}

static bool
source_scan_category_known(const char *category)
{
	static const char *const known[] = {
		"HEAP_PRODUCER_ENTRYPOINT", "HEAP_RECEIPT_BOUNDARY",   "HEAP_ITL_ALLOC_REUSE",
		"HEAP_ITL_PUBLISH",			"HEAP_ITL_REGISTER",	   "CURRENT_MX_HEAP_PREPARE",
		"CURRENT_MX_HEAP_PUBLISH",	"CURRENT_MX_RECOMPOSE",	   "MULTIXACT_PUBLISHER",
		"HOT_PRUNE_FREEZE_REWRITE", "KO_PHYSICAL_REMOVAL",	   "ITL_TERMINAL_DISCHARGE",
		"ITL_STATUS_WRITER",		"TT_STATUS_WRITER",		   "TT_RELEASE_FLAG_WRITER",
		"CURRENT_SLOT_GC",			"ROLLED_SEGMENT_RECYCLE",  "CURRENT_MX_PROOF_SENDER",
		"CURRENT_MX_WIRE_DECODER",	"CTRC_TOUCH_BEFORE_PROOF", "CTRC_RECEIPT_LIFECYCLE",
		"CTRC_CLOSE_WIRE",			"CTRC_RELATION_GATE",	   "CURRENT_MX_TERMINAL_CLEANOUT",
	};
	size_t i;

	for (i = 0; i < lengthof(known); i++)
		if (strcmp(category, known[i]) == 0)
			return true;
	return false;
}

static bool
source_hit_count_valid(const char *text)
{
	char *end = NULL;
	unsigned long count;

	if (text == NULL || text[0] == '\0')
		return false;
	count = strtoul(text, &end, 10);
	return count > 0 && end != NULL && *end == '\0';
}

static int
split_tsv_line(char *line, char **fields, int fields_cap)
{
	char *cursor = line;
	int field_count = 0;

	while (field_count < fields_cap) {
		fields[field_count++] = cursor;
		cursor = strchr(cursor, '\t');
		if (cursor == NULL)
			break;
		*cursor++ = '\0';
	}
	cursor = strchr(fields[field_count - 1], '\n');
	if (cursor != NULL)
		*cursor = '\0';
	return field_count;
}

static bool
source_file_contains(const char *relative_path, const char *needle)
{
	char path[MAXPGPATH * 2];
	char *contents;
	FILE *file;
	long length;
	bool found;

	if (relative_path == NULL || relative_path[0] == '\0' || needle == NULL || needle[0] == '\0'
		|| snprintf(path, sizeof(path), "%s/%s", PGRAC_SOURCE_ROOT_PATH, relative_path)
			   >= (int)sizeof(path))
		return false;
	file = fopen(path, "rb");
	if (file == NULL)
		return false;
	if (fseek(file, 0, SEEK_END) != 0) {
		fclose(file);
		return false;
	}
	length = ftell(file);
	if (length < 0 || fseek(file, 0, SEEK_SET) != 0) {
		fclose(file);
		return false;
	}
	contents = (char *)malloc((size_t)length + 1);
	if (contents == NULL) {
		fclose(file);
		return false;
	}
	if (fread(contents, 1, (size_t)length, file) != (size_t)length) {
		free(contents);
		fclose(file);
		return false;
	}
	contents[length] = '\0';
	found = strstr(contents, needle) != NULL;
	free(contents);
	fclose(file);
	return found;
}

static int
source_file_occurrences(const char *relative_path, const char *needle)
{
	char path[MAXPGPATH * 2];
	char *contents;
	char *cursor;
	FILE *file;
	long length;
	int count = 0;

	if (relative_path == NULL || relative_path[0] == '\0' || needle == NULL || needle[0] == '\0'
		|| snprintf(path, sizeof(path), "%s/%s", PGRAC_SOURCE_ROOT_PATH, relative_path)
			   >= (int)sizeof(path))
		return -1;
	file = fopen(path, "rb");
	if (file == NULL)
		return -1;
	if (fseek(file, 0, SEEK_END) != 0) {
		fclose(file);
		return -1;
	}
	length = ftell(file);
	if (length < 0 || fseek(file, 0, SEEK_SET) != 0) {
		fclose(file);
		return -1;
	}
	contents = (char *)malloc((size_t)length + 1);
	if (contents == NULL) {
		fclose(file);
		return -1;
	}
	if (fread(contents, 1, (size_t)length, file) != (size_t)length) {
		free(contents);
		fclose(file);
		return -1;
	}
	contents[length] = '\0';
	fclose(file);
	for (cursor = contents; (cursor = strstr(cursor, needle)) != NULL; cursor += strlen(needle))
		count++;
	free(contents);
	return count;
}

/* MXA-T35: every manifest row must carry one closed semantic class.  A row
 * cannot earn classification from a filename or comment alone. */
UT_TEST(test_ctrc_source_census_has_no_unclassified_reference_or_release_writer)
{
	FILE *file;
	char line[2048];
	int rows = 0;

	file = fopen(CTRC_SOURCE_MANIFEST_PATH, "r");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return;
	while (fgets(line, sizeof(line), file) != NULL) {
		char *fields[10];
		int field_count;
		int i;

		if (line[0] == '#' || line[0] == '\n')
			continue;
		field_count = split_tsv_line(line, fields, lengthof(fields));
		UT_ASSERT_EQ(field_count, 10);
		if (field_count != 10)
			continue;
		for (i = 0; i < field_count; i++)
			UT_ASSERT(fields[i][0] != '\0');
		UT_ASSERT(source_class_known(fields[2]));
		UT_ASSERT(source_file_contains(fields[1], fields[0]));
		UT_ASSERT(source_scan_category_known(fields[7]));
		UT_ASSERT(source_hit_count_valid(fields[8]));
		UT_ASSERT(strncmp(fields[9], "MXA-T", strlen("MXA-T")) == 0);
		rows++;
	}
	fclose(file);
	UT_ASSERT(rows > 0);
}

/* The ordinary heap producer set is closed over every source entrypoint that
 * can publish a tuple ITL/UBA reference.  Delegation is explicit; an old
 * touch-only or receipt-free direct producer cannot satisfy a row. */
UT_TEST(test_ctrc_heap_reference_producer_census_is_closed)
{
	static const char *const required[] = {
		"heap_insert", "heap_multi_insert", "heap_delete",
		"heap_update", "heap_lock_tuple",	"heap_lock_updated_tuple_rec",
	};
	bool seen[lengthof(required)];
	FILE *file;
	char line[4096];
	int rows = 0;

	MemSet(seen, 0, sizeof(seen));
	file = fopen(HEAP_REFERENCE_PRODUCER_MANIFEST_PATH, "r");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return;
	while (fgets(line, sizeof(line), file) != NULL) {
		char *fields[10];
		int field_count;
		size_t i;

		if (line[0] == '#' || line[0] == '\n')
			continue;
		field_count = split_tsv_line(line, fields, lengthof(fields));
		UT_ASSERT_EQ(field_count, 10);
		if (field_count != 10)
			continue;
		for (i = 0; i < lengthof(required); i++)
			if (strcmp(fields[0], required[i]) == 0) {
				UT_ASSERT(!seen[i]);
				seen[i] = true;
				break;
			}
		UT_ASSERT(i < lengthof(required));
		UT_ASSERT(strcmp(fields[2], "DIRECT_EXACT") == 0
				  || strcmp(fields[2], "PER_TUPLE_DELEGATE") == 0);
		UT_ASSERT(strncmp(fields[7], "FAIL_CLOSED", 11) == 0);
		UT_ASSERT(source_file_contains(fields[1], fields[0]));
		UT_ASSERT(source_file_contains(fields[1], fields[3]));
		UT_ASSERT(source_file_contains(fields[1], fields[4]));
		UT_ASSERT(source_file_contains(fields[1], fields[5]));
		UT_ASSERT(source_file_contains(fields[1], fields[6]));
		UT_ASSERT(source_file_contains(fields[8], fields[9]));
		rows++;
	}
	fclose(file);
	UT_ASSERT_EQ(rows, lengthof(required));
	for (size_t i = 0; i < lengthof(required); i++)
		UT_ASSERT(seen[i]);
	UT_ASSERT(!source_file_contains("src/backend/access/heap/heapam.c",
									"cluster_itl_touch_register_exact("));
	UT_ASSERT(source_file_contains("src/backend/access/heap/heapam.c",
								   "cluster_itl_touch_register_exact_ctrc("));
}

/* Every cardinality-sensitive owner is named once and linked to an actual
 * one-member behavior test.  This keeps a helper-only success from masking a
 * reject in memo, wire, remote CR, ordinary heap, or HOT consumption. */
UT_TEST(test_ctrc_current_mx_cardinality_census_allows_one_end_to_end)
{
	static const char *const required[] = {
		"PRODUCER_PLAN",
		"MATERIALIZER",
		"LOCAL_DESCRIBE",
		"REMOTE_DESCRIBE_SERVE",
		"REMOTE_DESCRIBE_FETCH",
		"WIRE_FORWARD",
		"WIRE_REPLY",
		"MEMBER_PROOF",
		"RESOLVE",
		"RECOMPOSE",
		"MEMO",
		"ORDINARY_HEAP_CONSUMER",
		"HOT_CONSUMER",
	};
	bool seen[lengthof(required)];
	FILE *file;
	char line[4096];
	int rows = 0;

	MemSet(seen, 0, sizeof(seen));
	file = fopen(CURRENT_MX_CARDINALITY_MANIFEST_PATH, "r");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return;
	while (fgets(line, sizeof(line), file) != NULL) {
		char *fields[7];
		int field_count;
		size_t i;

		if (line[0] == '#' || line[0] == '\n')
			continue;
		field_count = split_tsv_line(line, fields, lengthof(fields));
		UT_ASSERT_EQ(field_count, 7);
		if (field_count != 7)
			continue;
		for (i = 0; i < lengthof(required); i++)
			if (strcmp(fields[0], required[i]) == 0) {
				UT_ASSERT(!seen[i]);
				seen[i] = true;
				break;
			}
		UT_ASSERT(i < lengthof(required));
		UT_ASSERT_STR_EQ(fields[3], "ALLOW_ONE");
		UT_ASSERT(fields[4][0] != '\0' && strcmp(fields[4], "-") != 0);
		UT_ASSERT(source_file_contains(fields[2], fields[1]));
		UT_ASSERT(source_file_contains(fields[5], fields[6]));
		rows++;
	}
	fclose(file);
	UT_ASSERT_EQ(rows, lengthof(required));
	for (size_t i = 0; i < lengthof(required); i++)
		UT_ASSERT(seen[i]);
}

/* MXA-T15 consumes a closed, machine-readable observability alphabet.  Keep
 * names bijective with the enum so a new transition cannot silently reuse an
 * unrelated counter or cleaner reason. */
UT_TEST(test_ctrc_observability_names_are_closed_and_total)
{
	static const char *const stat_names[] = {
		"grant_issued_count",
		"grant_refused_count",
		"receipt_prepared_count",
		"receipt_applied_count",
		"receipt_cancelled_count",
		"receipt_capacity_refused_count",
		"seal_started_count",
		"seal_blocked_count",
		"target_absent_count",
		"target_rewritten_count",
		"target_retained_count",
		"ack_frozen_count",
		"ack_resent_count",
		"certificate_applied_count",
		"certificate_replayed_count",
		"l11_release_sample_count",
		"l12_recycle_count",
		"ordinary_publication_after_apply_count",
		"current_mx_publication_after_apply_count",
		"publication_order_violation_count",
		"cleaner_pass_count",
		"cleaner_attempt_count",
		"semantic_progress_count",
		"pending_duplicate_count",
		"dispatch_backlog",
		"certificate_backlog",
		"pending_observed_age_ms",
		"observed_at_monotonic_us",
		"observation_age_ms",
	};
	static const char *const reason_names[] = {
		"NONE",			  "PREPARED_DRAIN",	 "RESOURCE_X",		   "PAGE_REVALIDATE",
		"WAL_DURABILITY", "PARTICIPANT_ACK", "BLOCK0_CERTIFICATE", "BLOCKED",
	};
	size_t i;

	UT_ASSERT_EQ(lengthof(stat_names), CTRC_STAT_COUNT);
	for (i = 0; i < lengthof(stat_names); i++)
		UT_ASSERT_STR_EQ(cluster_ctrc_stat_name((ClusterCtrcStatId)i), stat_names[i]);
	UT_ASSERT(cluster_ctrc_stat_name(CTRC_STAT_COUNT) == NULL);

	UT_ASSERT_EQ(lengthof(reason_names), CTRC_CLEANER_REASON_COUNT);
	for (i = 0; i < lengthof(reason_names); i++)
		UT_ASSERT_STR_EQ(cluster_ctrc_cleaner_reason_name((ClusterCtrcCleanerReason)i),
						 reason_names[i]);
	UT_ASSERT(cluster_ctrc_cleaner_reason_name(CTRC_CLEANER_REASON_COUNT) == NULL);
}

typedef void (*CtrcTestFn)(void);

typedef struct CtrcTestCase {
	const char *name;
	CtrcTestFn function;
} CtrcTestCase;

#define CTRC_TEST_ENTRY(name)                                                                      \
	{                                                                                              \
		#name, name                                                                                \
	}

int
main(void)
{
	static const CtrcTestCase tests[] = {
		CTRC_TEST_ENTRY(test_ctrc_origin_reservation_prebind_cancel_and_postbind_block_are_exact),
		CTRC_TEST_ENTRY(test_ctrc_origin_reservation_open_duplicate_and_aba_cleanup_do_not_mutate),
		CTRC_TEST_ENTRY(test_ctrc_released_origin_conflict_is_retryable_without_state_loss),
		CTRC_TEST_ENTRY(test_ctrc_active_grant_records_touched_node_before_positive_proof),
		CTRC_TEST_ENTRY(test_ctrc_epoch_zero_identity_is_present_and_exact),
		CTRC_TEST_ENTRY(test_ctrc_delayed_positive_proof_revalidates_open_grant),
		CTRC_TEST_ENTRY(test_ctrc_receipt_prepare_apply_full_identity_cross_product),
		CTRC_TEST_ENTRY(test_ctrc_unpublished_itl_apply_accepts_forward_page_version),
		CTRC_TEST_ENTRY(test_ctrc_unpublished_itl_reacquired_current_binds_only_at_apply),
		CTRC_TEST_ENTRY(test_ctrc_unpublished_itl_version_floor_keeps_identity_and_negative_fences),
		CTRC_TEST_ENTRY(test_ctrc_offnum_still_requires_exact_predecessor_version),
		CTRC_TEST_ENTRY(test_ctrc_shared_table_exact_duplicate_is_idempotent),
		CTRC_TEST_ENTRY(test_ctrc_shared_table_same_publication_different_target_blocks),
		CTRC_TEST_ENTRY(test_ctrc_shared_table_different_publication_allocates_new_receipt),
		CTRC_TEST_ENTRY(test_ctrc_shared_table_participant_key_and_grant_drift_fail_closed),
		CTRC_TEST_ENTRY(test_ctrc_indexed_receipt_probe_and_tombstone_reclaim_are_exact),
		CTRC_TEST_ENTRY(test_ctrc_heap_apply_is_nonblocking_and_precedes_all_reference_mutation),
		CTRC_TEST_ENTRY(test_ctrc_same_itl_retarget_is_exact_nonblocking_and_count_neutral),
		CTRC_TEST_ENTRY(test_ctrc_retargeting_owner_loss_blocks_without_release),
		CTRC_TEST_ENTRY(test_ctrc_current_mx_receipt_applies_only_exact_active_target),
		CTRC_TEST_ENTRY(
			test_ctrc_itl_uba_registers_before_mutation_and_only_exact_projection_discharges),
		CTRC_TEST_ENTRY(test_ctrc_itl_cleanout_rewrites_only_exact_terminal_slot),
		CTRC_TEST_ENTRY(test_real_cleaner_terminal_image_closes_plain_lock_reference),
		CTRC_TEST_ENTRY(test_ctrc_close_race_drains_prepared_without_timeout_cancellation),
		CTRC_TEST_ENTRY(test_ctrc_exact_target_absence_and_ambiguity_cleanout_table),
		CTRC_TEST_ENTRY(test_ctrc_terminal_member_cleanout_semantics_cross_product),
		CTRC_TEST_ENTRY(test_ctrc_current_mx_discharge_requires_exact_target_and_durability),
		CTRC_TEST_ENTRY(test_ctrc_page_lsn_regression_is_origin_qualified),
		CTRC_TEST_ENTRY(test_ctrc_bootstrap_page_version_is_one_way_baseline),
		CTRC_TEST_ENTRY(test_ctrc_remote_current_mx_cleanout_consumes_exact_origin_proof),
		CTRC_TEST_ENTRY(test_ctrc_current_mx_rewrite_plan_preserves_updater_and_survivors),
		CTRC_TEST_ENTRY(test_ctrc_successor_receipts_and_descriptor_precede_predecessor_removal),
		CTRC_TEST_ENTRY(test_ctrc_native_prune_freeze_refuses_current_mx_predecessor),
		CTRC_TEST_ENTRY(test_ctrc_relation_removal_waits_for_every_matching_receipt),
		CTRC_TEST_ENTRY(test_ctrc_seal_request_codec_roundtrip_and_strict_rejection),
		CTRC_TEST_ENTRY(test_ctrc_seal_reply_codec_binds_request_ack_and_zero_tail),
		CTRC_TEST_ENTRY(test_ctrc_participant_boot_capability_and_membership_loss_retain),
		CTRC_TEST_ENTRY(test_ctrc_close_before_proof_creates_exact_zero_range_tombstone),
		CTRC_TEST_ENTRY(test_ctrc_zero_range_request_is_idempotent_until_exact_certificate),
		CTRC_TEST_ENTRY(test_ctrc_origin_ack_lands_only_exact_dispatched_touch),
		CTRC_TEST_ENTRY(test_ctrc_origin_seal_fsm_requires_every_exact_close_confirmation),
		CTRC_TEST_ENTRY(test_ctrc_existing_undo_cleaner_owns_terminal_seal_progress),
		CTRC_TEST_ENTRY(test_ctrc_release_overlap_progress_is_event_driven_without_xcur_churn),
		CTRC_TEST_ENTRY(test_ctrc_cleaner_resource_x_not_ready_is_a_clean_retry),
		CTRC_TEST_ENTRY(test_ctrc_local_release_ack_range_digest_and_dependency_durability),
		CTRC_TEST_ENTRY(test_ctrc_nonempty_ack_uses_canonical_rows_and_durable_vector),
		CTRC_TEST_ENTRY(test_ctrc_origin_certificate_requires_exact_complete_sorted_ack_set),
		CTRC_TEST_ENTRY(test_ctrc_release_certificate_enables_exact_l11_l12_and_ack_reclaim),
		CTRC_TEST_ENTRY(test_ctrc_crash_cut_matrix_releases_only_after_durable_certificate),
		CTRC_TEST_ENTRY(test_ctrc_release_wal_codec_and_exact_redo_matrix),
		CTRC_TEST_ENTRY(
			test_ctrc_capacity_is_activation_sized_and_runtime_full_refuses_before_mutation),
		CTRC_TEST_ENTRY(test_ctrc_participant_index_uses_participant_not_origin_node),
		CTRC_TEST_ENTRY(test_ctrc_source_census_has_no_unclassified_reference_or_release_writer),
		CTRC_TEST_ENTRY(test_ctrc_heap_reference_producer_census_is_closed),
		CTRC_TEST_ENTRY(test_ctrc_current_mx_cardinality_census_allows_one_end_to_end),
		CTRC_TEST_ENTRY(test_ctrc_observability_names_are_closed_and_total),
	};
	const char *filter = getenv("PGRAC_CTRC_UNIT_FILTER");
	size_t i;
	int selected = 0;

	for (i = 0; i < lengthof(tests); i++)
		if (filter == NULL || filter[0] == '\0' || strcmp(filter, tests[i].name) == 0)
			selected++;
	UT_PLAN(selected);
	for (i = 0; i < lengthof(tests); i++) {
		if (filter != NULL && filter[0] != '\0' && strcmp(filter, tests[i].name) != 0)
			continue;
		ut_test_count++;
		ut_current_failed = 0;
		tests[i].function();
		if (ut_current_failed == 0)
			printf("ok %d - %s\n", ut_test_count, tests[i].name);
		else {
			printf("not ok %d - %s\n", ut_test_count, tests[i].name);
			ut_failed_count++;
		}
	}
	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
