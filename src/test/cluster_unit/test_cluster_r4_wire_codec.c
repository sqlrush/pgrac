/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_wire_codec.c
 *    Stage 8 R4 exact request80/FORWARD96 extension codec tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_wire_codec.c
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_tx_resolve.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterTxResolution
verdict_resolution(ClusterTxOutcome outcome, ClusterTxProofKind proof_kind)
{
	ClusterTxResolution resolution;

	memset(&resolution, 0, sizeof(resolution));
	resolution.locator_echo.uba.raw[0] = UINT64_C(0x0102030405060708);
	resolution.locator_echo.uba.raw[1] = UINT64_C(0x1112131415161718);
	resolution.locator_echo.xid = (TransactionId)798;
	resolution.locator_echo.tt_wrap = 7;
	resolution.locator_echo.itl_kind = ITL_FLAG_ACTIVE;
	resolution.locator_echo.itl_slot_index = 3;
	resolution.top_xid = (TransactionId)700;
	resolution.outcome = outcome;
	resolution.proof_kind = proof_kind;
	resolution.commit_scn = outcome == CLUSTER_TX_COMMITTED ? (SCN)0x2122232425262728 : InvalidScn;
	resolution.horizon_scn = proof_kind == CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON
								 ? (SCN)0x3132333435363738
								 : InvalidScn;
	resolution.authority.origin_epoch = 0;
	resolution.authority.live_hwm_lsn = (XLogRecPtr)0x4142434445464748;
	resolution.authority.tt_generation = UINT64_C(0x5152535455565758);
	resolution.authority.authority_scn = (SCN)0x6162636465666768;
	return resolution;
}

static bool
bytes_are_zero(const uint8 *bytes, size_t size)
{
	for (size_t i = 0; i < size; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static void
run_wire_vector(int vector)
{
	ClusterR4RequestExtension request;
	ClusterR4ForwardExtension forward;
	ClusterTxLocator locator;
	ClusterTxLocator decoded_locator;
	uint64 master_generation = 0;
	uint64 transition_count = 0;
	SCN scn = InvalidScn;
	uint8 bytes[8];
	uint8 page[BLCKSZ];
	uint64 value;
	uint32 physical_generation = UINT32_MAX;
	ClusterTxResolution input;
	ClusterTxResolution output;

	memset(&request, 0, sizeof(request));
	memset(&forward, 0, sizeof(forward));
	memset(&locator, 0, sizeof(locator));
	memset(&decoded_locator, 0, sizeof(decoded_locator));
	memset(bytes, 0, sizeof(bytes));
	memset(page, 0, sizeof(page));
	memset(&input, 0, sizeof(input));
	memset(&output, 0, sizeof(output));
	locator.uba.raw[0] = UINT64_C(0x0102030405060708);
	locator.uba.raw[1] = UINT64_C(0x1112131415161718);
	locator.xid = (TransactionId)798;
	locator.tt_wrap = 7;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 1;

	switch (vector) {
	case 0:
		UT_ASSERT_EQ(sizeof(request), 16);
		break;
	case 1:
		UT_ASSERT_EQ(sizeof(forward), 32);
		break;
	case 2:
		UT_ASSERT_EQ(
			offsetof(ClusterR4ForwardExtension, kind.cr.master_resource_transition_count_le), 12);
		break;
	case 3:
		UT_ASSERT_EQ(CLUSTER_R4_WIRE_VERSION, 1);
		break;
	case 4:
		UT_ASSERT_EQ(CLUSTER_R4_FORWARD_EXTENDED, 6);
		break;
	case 5:
		UT_ASSERT_EQ(CLUSTER_R4_WIRE_CR_BUILD, 1);
		break;
	case 6:
		UT_ASSERT(ClusterR4RequestExtensionSetCr(&request, (SCN)0x1234));
		UT_ASSERT(ClusterR4RequestExtensionGetCr(&request, &scn));
		UT_ASSERT_EQ((uint64)scn, UINT64_C(0x1234));
		break;
	case 7:
		UT_ASSERT(!ClusterR4RequestExtensionSetCr(NULL, (SCN)1));
		break;
	case 8:
		UT_ASSERT(!ClusterR4RequestExtensionSetCr(&request, InvalidScn));
		break;
	case 9:
		UT_ASSERT(!ClusterR4RequestExtensionGetCr(NULL, &scn));
		break;
	case 10:
		UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, NULL));
		break;
	case 11:
		ClusterR4RequestExtensionSetCr(&request, (SCN)1);
		request.r4_version++;
		UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
		break;
	case 12:
		ClusterR4RequestExtensionSetCr(&request, (SCN)1);
		request.r4_kind = CLUSTER_R4_WIRE_TX_RESOLVE;
		UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
		break;
	case 13:
		ClusterR4RequestExtensionSetCr(&request, (SCN)1);
		request.flags_le[0] = 1;
		UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
		break;
	case 14:
		ClusterR4RequestExtensionSetCr(&request, (SCN)1);
		request.flags_le[1] = 1;
		UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
		break;
	case 15:
		ClusterR4RequestExtensionSetCr(&request, (SCN)1);
		request.reserved[3] = 1;
		UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
		break;
	case 16:
		ClusterR4ForwardExtensionSetCrProof(&forward, (UINT64_C(9) << 32) | 4, UINT64_C(7),
											(SCN)0x5678);
		UT_ASSERT(ClusterR4ForwardExtensionGetCrProof(&forward, 9, &master_generation,
													  &transition_count, &scn));
		UT_ASSERT(master_generation == ((UINT64_C(9) << 32) | 4));
		UT_ASSERT(transition_count == UINT64_C(7));
		UT_ASSERT((uint64)scn == UINT64_C(0x5678));
		break;
	case 17:
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(NULL, 0, &master_generation,
													   &transition_count, &scn));
		break;
	case 18:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, NULL, &transition_count, &scn));
		break;
	case 19:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
		UT_ASSERT(
			!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation, NULL, &scn));
		break;
	case 20:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
													   &transition_count, NULL));
		break;
	case 21:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
		forward.r4_version++;
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
													   &transition_count, &scn));
		break;
	case 22:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
		forward.r4_kind = CLUSTER_R4_WIRE_TX_RESOLVE;
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
													   &transition_count, &scn));
		break;
	case 23:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
		forward.flags_le[0] = 1;
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
													   &transition_count, &scn));
		break;
	case 24:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
		forward.subject_id_le[0] = 1;
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
													   &transition_count, &scn));
		break;
	case 25:
		ClusterR4ForwardExtensionSetCrProof(&forward, 0, 1, InvalidScn);
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
													   &transition_count, &scn));
		break;
	case 26:
		ClusterR4ForwardExtensionSetCrProof(&forward, UINT64_C(1) << 32, 1, InvalidScn);
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 1, &master_generation,
													   &transition_count, &scn));
		break;
	case 27:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, 0, InvalidScn);
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
													   &transition_count, &scn));
		break;
	case 28:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, UINT64_MAX, InvalidScn);
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
													   &transition_count, &scn));
		break;
	case 29:
		ClusterR4ForwardExtensionSetCrProof(&forward, (UINT64_C(2) << 32) | 1, 1, InvalidScn);
		UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 1, &master_generation,
													   &transition_count, &scn));
		break;
	case 30:
		ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
		UT_ASSERT(ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
													  &transition_count, &scn));
		UT_ASSERT_EQ((uint64)scn, (uint64)InvalidScn);
		break;
	case 31:
		ClusterR4ForwardExtensionSetCrProof(&forward, (UINT64_C(0xffffffff) << 32) | 1, 1,
											InvalidScn);
		UT_ASSERT(ClusterR4ForwardExtensionGetCrProof(&forward, UINT64_MAX, &master_generation,
													  &transition_count, &scn));
		break;
	case 32:
		UT_ASSERT(
			ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator));
		UT_ASSERT(ClusterR4ForwardExtensionGetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE,
													  &decoded_locator));
		UT_ASSERT_EQ(memcmp(&decoded_locator, &locator, sizeof(locator)), 0);
		break;
	case 33:
		UT_ASSERT(ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_UNDO_DATA_FETCH,
													  &locator));
		UT_ASSERT(ClusterR4ForwardExtensionGetLocator(&forward, CLUSTER_R4_WIRE_UNDO_DATA_FETCH,
													  &decoded_locator));
		UT_ASSERT_EQ(memcmp(&decoded_locator, &locator, sizeof(locator)), 0);
		break;
	case 34:
		UT_ASSERT(
			ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator));
		UT_ASSERT_EQ(ClusterR4WireReadU64(&forward.kind.locator_bytes[0]), locator.uba.raw[0]);
		UT_ASSERT_EQ(ClusterR4WireReadU64(&forward.kind.locator_bytes[8]), locator.uba.raw[1]);
		UT_ASSERT_EQ(ClusterR4WireReadU32(&forward.kind.locator_bytes[16]), (uint32)locator.xid);
		UT_ASSERT_EQ(ClusterR4WireReadU16(&forward.kind.locator_bytes[20]), locator.tt_wrap);
		UT_ASSERT_EQ(forward.kind.locator_bytes[22], locator.itl_kind);
		UT_ASSERT_EQ(forward.kind.locator_bytes[23], locator.itl_slot_index);
		break;
	case 35:
		memset(&forward, 0xa5, sizeof(forward));
		UT_ASSERT(
			!ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_CR_BUILD, &locator));
		UT_ASSERT(bytes_are_zero((const uint8 *)&forward, sizeof(forward)));
		break;
	case 36:
		memset(&forward, 0xa5, sizeof(forward));
		UT_ASSERT(!ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_MULTI_RESOLVE,
													   &locator));
		UT_ASSERT(bytes_are_zero((const uint8 *)&forward, sizeof(forward)));
		break;
	case 37:
		UT_ASSERT(!ClusterR4ForwardExtensionSetLocator(NULL, CLUSTER_R4_WIRE_TX_RESOLVE, &locator));
		memset(&forward, 0xa5, sizeof(forward));
		UT_ASSERT(!ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, NULL));
		UT_ASSERT(bytes_are_zero((const uint8 *)&forward, sizeof(forward)));
		break;
	case 38:
		ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator);
		memset(&decoded_locator, 0xa5, sizeof(decoded_locator));
		forward.flags_le[0] = 1;
		UT_ASSERT(!ClusterR4ForwardExtensionGetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE,
													   &decoded_locator));
		UT_ASSERT(bytes_are_zero((const uint8 *)&decoded_locator, sizeof(decoded_locator)));
		break;
	case 39:
		ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator);
		forward.subject_id_le[3] = 1;
		UT_ASSERT(!ClusterR4ForwardExtensionGetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE,
													   &decoded_locator));
		break;
	case 40:
		ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator);
		forward.r4_version++;
		UT_ASSERT(!ClusterR4ForwardExtensionGetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE,
													   &decoded_locator));
		break;
	case 41:
		ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator);
		UT_ASSERT(!ClusterR4ForwardExtensionGetLocator(&forward, CLUSTER_R4_WIRE_UNDO_DATA_FETCH,
													   &decoded_locator));
		break;
	case 42:
		UT_ASSERT(!ClusterR4ForwardExtensionGetLocator(NULL, CLUSTER_R4_WIRE_TX_RESOLVE,
													   &decoded_locator));
		ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator);
		UT_ASSERT(!ClusterR4ForwardExtensionGetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, NULL));
		break;
	case 43:
		ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator);
		forward.kind.locator_bytes[16] ^= 1;
		UT_ASSERT(ClusterR4ForwardExtensionGetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE,
													  &decoded_locator));
		UT_ASSERT(decoded_locator.xid != locator.xid);
		break;
	case 44:
		UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &locator, UINT32_C(0x01020304)));
		UT_ASSERT(ClusterR4ForwardExtensionGetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &decoded_locator, &physical_generation));
		UT_ASSERT_EQ(physical_generation, UINT32_C(0x01020304));
		UT_ASSERT_EQ(memcmp(&decoded_locator, &locator, sizeof(locator)), 0);
		UT_ASSERT_EQ(forward.subject_id_le[0], 0x04);
		UT_ASSERT_EQ(forward.subject_id_le[3], 0x01);
		break;
	case 45:
		UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator, 0));
		UT_ASSERT(ClusterR4ForwardExtensionGetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &decoded_locator, &physical_generation));
		UT_ASSERT_EQ(physical_generation, 0);
		break;
	case 46:
		memset(&forward, 0xa5, sizeof(forward));
		UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator, UINT32_MAX));
		UT_ASSERT_EQ(ClusterR4WireReadU32(forward.subject_id_le), UINT32_MAX);
		UT_ASSERT(bytes_are_zero(forward.flags_le, sizeof(forward.flags_le)));
		break;
	case 47:
		ClusterR4ForwardExtensionSetLocator(&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator);
		ClusterR4WireWriteU32(forward.subject_id_le, UINT32_MAX);
		memset(&decoded_locator, 0xa5, sizeof(decoded_locator));
		physical_generation = UINT32_C(0xa5a5a5a5);
		UT_ASSERT(ClusterR4ForwardExtensionGetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &decoded_locator, &physical_generation));
		UT_ASSERT_EQ(memcmp(&decoded_locator, &locator, sizeof(locator)), 0);
		UT_ASSERT_EQ(physical_generation, UINT32_MAX);
		break;
	case 48:
		UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &locator, 7));
		forward.flags_le[0] = 1;
		UT_ASSERT(!ClusterR4ForwardExtensionGetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &decoded_locator, &physical_generation));
		break;
	case 49:
		UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_TX_RESOLVE, &locator, 9));
		UT_ASSERT(!ClusterR4ForwardExtensionGetLocatorGeneration(
			&forward, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &decoded_locator, &physical_generation));
		break;
	case 64:
		UT_ASSERT_EQ(CLUSTER_R4_TX_VERDICT_VERSION, 3);
		UT_ASSERT_EQ(CLUSTER_R4_TX_VERDICT_HEADER_LEN, 80);
		break;
	case 65:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		UT_ASSERT_EQ(output.outcome, input.outcome);
		UT_ASSERT_EQ(output.proof_kind, input.proof_kind);
		UT_ASSERT_EQ((uint64)output.commit_scn, (uint64)input.commit_scn);
		break;
	case 66:
		input = verdict_resolution(CLUSTER_TX_ABORTED, CLUSTER_TX_PROOF_ITL_CLEANOUT);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		UT_ASSERT_EQ(output.outcome, CLUSTER_TX_ABORTED);
		break;
	case 67:
		input = verdict_resolution(CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		UT_ASSERT_EQ(output.outcome, CLUSTER_TX_IN_PROGRESS);
		break;
	case 68:
		input = verdict_resolution(CLUSTER_TX_PREPARED, CLUSTER_TX_PROOF_ORIGIN_TWOPHASE);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		UT_ASSERT_EQ(output.outcome, CLUSTER_TX_PREPARED);
		break;
	case 69:
		input = verdict_resolution(CLUSTER_TX_UNKNOWN, CLUSTER_TX_PROOF_NONE);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		UT_ASSERT_EQ(output.outcome, CLUSTER_TX_UNKNOWN);
		break;
	case 70:
		input = verdict_resolution(CLUSTER_TX_UNKNOWN, CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		UT_ASSERT_EQ((uint64)output.horizon_scn, (uint64)input.horizon_scn);
		break;
	case 71:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		UT_ASSERT_EQ(output.authority.origin_epoch, 0);
		break;
	case 72:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT_EQ(ClusterR4WireReadU64(&page[12]), input.locator_echo.uba.raw[0]);
		UT_ASSERT_EQ(ClusterR4WireReadU64(&page[20]), input.locator_echo.uba.raw[1]);
		break;
	case 73:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT_EQ(page[28], (uint8)(input.locator_echo.xid & 0xff));
		UT_ASSERT_EQ(page[32], 7);
		break;
	case 74:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT_EQ((uint32)ClusterR4WireReadU32(&page[36]), (uint32)input.top_xid);
		UT_ASSERT_EQ(ClusterR4WireReadU64(&page[40]), (uint64)input.commit_scn);
		break;
	case 75:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(bytes_are_zero(page + CLUSTER_R4_TX_VERDICT_HEADER_LEN,
								 BLCKSZ - CLUSTER_R4_TX_VERDICT_HEADER_LEN));
		break;
	case 76:
		input = verdict_resolution(CLUSTER_TX_PREPARED, CLUSTER_TX_PROOF_ITL_CLEANOUT);
		memset(page, 0xa5, sizeof(page));
		UT_ASSERT(!ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(bytes_are_zero(page, sizeof(page)));
		break;
	case 77:
		UT_ASSERT(!ClusterR4TxVerdictPageEncode(page, NULL));
		UT_ASSERT(!ClusterR4TxVerdictPageEncode(NULL, &input));
		break;
	case 78:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		page[0] ^= 1;
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		break;
	case 79:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		page[4]++;
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		break;
	case 80:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		page[6]++;
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		break;
	case 81:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		page[10] = 1;
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		break;
	case 82:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		page[BLCKSZ - 1] = 1;
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		break;
	case 83:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		input.locator_echo.xid++;
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		break;
	case 84:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		page[8] = CLUSTER_TX_PREPARED;
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		break;
	case 85:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		memset(&output, 0xa5, sizeof(output));
		page[9] = 0xff;
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &input.locator_echo, &output));
		UT_ASSERT(bytes_are_zero((const uint8 *)&output, sizeof(output)));
		break;
	case 86:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		UT_ASSERT_EQ(ClusterR4WireReadU64(&page[56]), input.authority.origin_epoch);
		UT_ASSERT_EQ(ClusterR4WireReadU64(&page[64]), input.authority.tt_generation);
		UT_ASSERT_EQ(ClusterR4WireReadU64(&page[72]), (uint64)input.authority.authority_scn);
		break;
	case 87:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		ClusterR4TxVerdictPageEncode(page, &input);
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(NULL, &input.locator_echo, &output));
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, NULL, &output));
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &input.locator_echo, NULL));
		break;
	case 88:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		locator = input.locator_echo;
		locator.tt_wrap = TT_WRAP_INVALID;
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(ClusterR4TxVerdictPageDecode(page, &locator, &output));
		UT_ASSERT_EQ(output.locator_echo.tt_wrap, 7);
		break;
	case 89:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		locator = input.locator_echo;
		locator.tt_wrap = TT_WRAP_INVALID;
		input.locator_echo.tt_wrap = TT_WRAP_INVALID;
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		memset(&output, 0xA5, sizeof(output));
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &locator, &output));
		UT_ASSERT(bytes_are_zero((const uint8 *)&output, sizeof(output)));
		break;
	case 90:
		input = verdict_resolution(CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
		locator = input.locator_echo;
		locator.tt_wrap = 8;
		UT_ASSERT(ClusterR4TxVerdictPageEncode(page, &input));
		UT_ASSERT(!ClusterR4TxVerdictPageDecode(page, &locator, &output));
		break;
	default:
		value = UINT64_C(0x0102030405060708) ^ ((uint64)vector << 33)
				^ ((uint64)vector * UINT64_C(0x9e3779b97f4a7c15));
		ClusterR4WireWriteU64(bytes, value);
		UT_ASSERT(ClusterR4WireReadU64(bytes) == value);
		break;
	}
}

#define DEFINE_WIRE_TEST(n)                                                                        \
	UT_TEST(test_wire_vector_##n)                                                                  \
	{                                                                                              \
		run_wire_vector(n);                                                                        \
	}

DEFINE_WIRE_TEST(0)
DEFINE_WIRE_TEST(1)
DEFINE_WIRE_TEST(2)
DEFINE_WIRE_TEST(3)
DEFINE_WIRE_TEST(4)
DEFINE_WIRE_TEST(5)
DEFINE_WIRE_TEST(6)
DEFINE_WIRE_TEST(7)
DEFINE_WIRE_TEST(8)
DEFINE_WIRE_TEST(9)
DEFINE_WIRE_TEST(10)
DEFINE_WIRE_TEST(11)
DEFINE_WIRE_TEST(12)
DEFINE_WIRE_TEST(13)
DEFINE_WIRE_TEST(14)
DEFINE_WIRE_TEST(15)
DEFINE_WIRE_TEST(16)
DEFINE_WIRE_TEST(17)
DEFINE_WIRE_TEST(18)
DEFINE_WIRE_TEST(19)
DEFINE_WIRE_TEST(20)
DEFINE_WIRE_TEST(21)
DEFINE_WIRE_TEST(22)
DEFINE_WIRE_TEST(23)
DEFINE_WIRE_TEST(24)
DEFINE_WIRE_TEST(25)
DEFINE_WIRE_TEST(26)
DEFINE_WIRE_TEST(27)
DEFINE_WIRE_TEST(28)
DEFINE_WIRE_TEST(29)
DEFINE_WIRE_TEST(30)
DEFINE_WIRE_TEST(31)
DEFINE_WIRE_TEST(32)
DEFINE_WIRE_TEST(33)
DEFINE_WIRE_TEST(34)
DEFINE_WIRE_TEST(35)
DEFINE_WIRE_TEST(36)
DEFINE_WIRE_TEST(37)
DEFINE_WIRE_TEST(38)
DEFINE_WIRE_TEST(39)
DEFINE_WIRE_TEST(40)
DEFINE_WIRE_TEST(41)
DEFINE_WIRE_TEST(42)
DEFINE_WIRE_TEST(43)
DEFINE_WIRE_TEST(44)
DEFINE_WIRE_TEST(45)
DEFINE_WIRE_TEST(46)
DEFINE_WIRE_TEST(47)
DEFINE_WIRE_TEST(48)
DEFINE_WIRE_TEST(49)
DEFINE_WIRE_TEST(50)
DEFINE_WIRE_TEST(51)
DEFINE_WIRE_TEST(52)
DEFINE_WIRE_TEST(53)
DEFINE_WIRE_TEST(54)
DEFINE_WIRE_TEST(55)
DEFINE_WIRE_TEST(56)
DEFINE_WIRE_TEST(57)
DEFINE_WIRE_TEST(58)
DEFINE_WIRE_TEST(59)
DEFINE_WIRE_TEST(60)
DEFINE_WIRE_TEST(61)
DEFINE_WIRE_TEST(62)
DEFINE_WIRE_TEST(63)
DEFINE_WIRE_TEST(64)
DEFINE_WIRE_TEST(65)
DEFINE_WIRE_TEST(66)
DEFINE_WIRE_TEST(67)
DEFINE_WIRE_TEST(68)
DEFINE_WIRE_TEST(69)
DEFINE_WIRE_TEST(70)
DEFINE_WIRE_TEST(71)
DEFINE_WIRE_TEST(72)
DEFINE_WIRE_TEST(73)
DEFINE_WIRE_TEST(74)
DEFINE_WIRE_TEST(75)
DEFINE_WIRE_TEST(76)
DEFINE_WIRE_TEST(77)
DEFINE_WIRE_TEST(78)
DEFINE_WIRE_TEST(79)
DEFINE_WIRE_TEST(80)
DEFINE_WIRE_TEST(81)
DEFINE_WIRE_TEST(82)
DEFINE_WIRE_TEST(83)
DEFINE_WIRE_TEST(84)
DEFINE_WIRE_TEST(85)
DEFINE_WIRE_TEST(86)
DEFINE_WIRE_TEST(87)
DEFINE_WIRE_TEST(88)
DEFINE_WIRE_TEST(89)
DEFINE_WIRE_TEST(90)

#define RUN_WIRE_TEST(n) UT_RUN(test_wire_vector_##n)

UT_TEST(test_r4_reply_status_abi_tail_is_exact)
{
	UT_ASSERT_EQ(GCS_BLOCK_REPLY_R4_CR_FULL, 21);
	UT_ASSERT_EQ(GCS_BLOCK_REPLY_R4_TX_RESOLVE_RESULT, 22);
	UT_ASSERT_EQ(GCS_BLOCK_REPLY_R4_MULTI_RESOLVE_RESULT, 23);
	UT_ASSERT_EQ(GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT, 24);
	UT_ASSERT_EQ(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 25);
	UT_ASSERT_EQ(GCS_BLOCK_REPLY_R4_DENIED, 26);
}

UT_TEST(test_r4_and_legacy_reply_status_domains_are_disjoint)
{
	int status;

	for (status = 0; status <= 20; status++) {
		UT_ASSERT(GcsBlockReplyStatusIsLegacy((GcsBlockReplyStatus)status));
		UT_ASSERT(!GcsBlockReplyStatusIsR4((GcsBlockReplyStatus)status));
		UT_ASSERT(!GcsBlockReplyStatusIsR4Refusal((GcsBlockReplyStatus)status));
	}
	for (status = 21; status <= 26; status++) {
		UT_ASSERT(!GcsBlockReplyStatusIsLegacy((GcsBlockReplyStatus)status));
		UT_ASSERT(GcsBlockReplyStatusIsR4((GcsBlockReplyStatus)status));
		UT_ASSERT_EQ(GcsBlockReplyStatusIsR4Refusal((GcsBlockReplyStatus)status), status >= 25);
	}
	UT_ASSERT(!GcsBlockReplyStatusIsLegacy((GcsBlockReplyStatus)-1));
	UT_ASSERT(!GcsBlockReplyStatusIsR4((GcsBlockReplyStatus)27));
}

UT_TEST(test_r4_undo_data_status_selects_existing_authenticated_reply_shape)
{
	UT_ASSERT(GcsBlockReplyStatusCarriesUndoAuthTrailer(GCS_BLOCK_REPLY_R4_UNDO_DATA_RESULT));
	UT_ASSERT_EQ(sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE
					 + sizeof(ClusterGcsUndoAuthTrailer),
				 8256);
	UT_ASSERT(!GcsBlockReplyStatusCarriesUndoAuthTrailer(GCS_BLOCK_REPLY_R4_CR_FULL));
	UT_ASSERT(
		!GcsBlockReplyStatusCarriesUndoAuthTrailer(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED));
	UT_ASSERT(!GcsBlockReplyStatusCarriesUndoAuthTrailer(GCS_BLOCK_REPLY_R4_DENIED));
}

UT_TEST(test_r4_status24_physical_generation_echo_is_exact)
{
	GcsBlockReplyHeader header;
	uint32 generation = UINT32_MAX;

	memset(&header, 0, sizeof(header));
	UT_ASSERT(GcsBlockReplyHeaderSetR4UndoGeneration(&header, UINT32_C(0x01020304)));
	UT_ASSERT(GcsBlockReplyHeaderGetR4UndoGeneration(&header, &generation));
	UT_ASSERT_EQ(generation, UINT32_C(0x01020304));
	UT_ASSERT_EQ(header.reserved_0[0], 0x04);
	UT_ASSERT_EQ(header.reserved_0[3], 0x01);
	UT_ASSERT_EQ(header.reserved_0[4], 0);
	UT_ASSERT_EQ(header.reserved_0[5], 0);

	memset(&header, 0xa5, sizeof(header));
	UT_ASSERT(!GcsBlockReplyHeaderSetR4UndoGeneration(&header, UINT32_MAX));
	UT_ASSERT(bytes_are_zero(header.reserved_0, sizeof(header.reserved_0)));

	header.reserved_0[4] = 1;
	generation = UINT32_C(0xa5a5a5a5);
	UT_ASSERT(!GcsBlockReplyHeaderGetR4UndoGeneration(&header, &generation));
	UT_ASSERT_EQ(generation, 0);
}

UT_TEST(test_kind4_generation_request_distinguishes_origin_sample_from_zero)
{
	ClusterR4ForwardExtension extension;
	ClusterTxLocator locator = { 0 };
	ClusterTxLocator decoded;
	uint32 values[] = { 0, 9, UINT32_MAX - 1, UINT32_MAX };
	uint32 generation;
	int i;

	locator.uba = uba_encode(257, 9, 3, 6);
	locator.xid = 797;
	locator.tt_wrap = TT_WRAP_INVALID;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 1;
	for (i = 0; i < lengthof(values); i++) {
		UT_ASSERT(ClusterR4ForwardExtensionSetLocatorGeneration(
			&extension, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &locator, values[i]));
		UT_ASSERT(ClusterR4ForwardExtensionGetLocatorGeneration(
			&extension, CLUSTER_R4_WIRE_UNDO_DATA_FETCH, &decoded, &generation));
		UT_ASSERT_EQ(generation, values[i]);
		UT_ASSERT_EQ(memcmp(&decoded, &locator, sizeof(locator)), 0);
	}
	UT_ASSERT(!ClusterR4ForwardExtensionGetLocatorGeneration(&extension, CLUSTER_R4_WIRE_TX_RESOLVE,
															 &decoded, &generation));
}

int
main(void)
{
	UT_PLAN(96);
	UT_RUN(test_kind4_generation_request_distinguishes_origin_sample_from_zero);
	RUN_WIRE_TEST(0);
	RUN_WIRE_TEST(1);
	RUN_WIRE_TEST(2);
	RUN_WIRE_TEST(3);
	RUN_WIRE_TEST(4);
	RUN_WIRE_TEST(5);
	RUN_WIRE_TEST(6);
	RUN_WIRE_TEST(7);
	RUN_WIRE_TEST(8);
	RUN_WIRE_TEST(9);
	RUN_WIRE_TEST(10);
	RUN_WIRE_TEST(11);
	RUN_WIRE_TEST(12);
	RUN_WIRE_TEST(13);
	RUN_WIRE_TEST(14);
	RUN_WIRE_TEST(15);
	RUN_WIRE_TEST(16);
	RUN_WIRE_TEST(17);
	RUN_WIRE_TEST(18);
	RUN_WIRE_TEST(19);
	RUN_WIRE_TEST(20);
	RUN_WIRE_TEST(21);
	RUN_WIRE_TEST(22);
	RUN_WIRE_TEST(23);
	RUN_WIRE_TEST(24);
	RUN_WIRE_TEST(25);
	RUN_WIRE_TEST(26);
	RUN_WIRE_TEST(27);
	RUN_WIRE_TEST(28);
	RUN_WIRE_TEST(29);
	RUN_WIRE_TEST(30);
	RUN_WIRE_TEST(31);
	RUN_WIRE_TEST(32);
	RUN_WIRE_TEST(33);
	RUN_WIRE_TEST(34);
	RUN_WIRE_TEST(35);
	RUN_WIRE_TEST(36);
	RUN_WIRE_TEST(37);
	RUN_WIRE_TEST(38);
	RUN_WIRE_TEST(39);
	RUN_WIRE_TEST(40);
	RUN_WIRE_TEST(41);
	RUN_WIRE_TEST(42);
	RUN_WIRE_TEST(43);
	RUN_WIRE_TEST(44);
	RUN_WIRE_TEST(45);
	RUN_WIRE_TEST(46);
	RUN_WIRE_TEST(47);
	RUN_WIRE_TEST(48);
	RUN_WIRE_TEST(49);
	RUN_WIRE_TEST(50);
	RUN_WIRE_TEST(51);
	RUN_WIRE_TEST(52);
	RUN_WIRE_TEST(53);
	RUN_WIRE_TEST(54);
	RUN_WIRE_TEST(55);
	RUN_WIRE_TEST(56);
	RUN_WIRE_TEST(57);
	RUN_WIRE_TEST(58);
	RUN_WIRE_TEST(59);
	RUN_WIRE_TEST(60);
	RUN_WIRE_TEST(61);
	RUN_WIRE_TEST(62);
	RUN_WIRE_TEST(63);
	RUN_WIRE_TEST(64);
	RUN_WIRE_TEST(65);
	RUN_WIRE_TEST(66);
	RUN_WIRE_TEST(67);
	RUN_WIRE_TEST(68);
	RUN_WIRE_TEST(69);
	RUN_WIRE_TEST(70);
	RUN_WIRE_TEST(71);
	RUN_WIRE_TEST(72);
	RUN_WIRE_TEST(73);
	RUN_WIRE_TEST(74);
	RUN_WIRE_TEST(75);
	RUN_WIRE_TEST(76);
	RUN_WIRE_TEST(77);
	RUN_WIRE_TEST(78);
	RUN_WIRE_TEST(79);
	RUN_WIRE_TEST(80);
	RUN_WIRE_TEST(81);
	RUN_WIRE_TEST(82);
	RUN_WIRE_TEST(83);
	RUN_WIRE_TEST(84);
	RUN_WIRE_TEST(85);
	RUN_WIRE_TEST(86);
	RUN_WIRE_TEST(87);
	RUN_WIRE_TEST(88);
	RUN_WIRE_TEST(89);
	RUN_WIRE_TEST(90);
	UT_RUN(test_r4_reply_status_abi_tail_is_exact);
	UT_RUN(test_r4_and_legacy_reply_status_domains_are_disjoint);
	UT_RUN(test_r4_undo_data_status_selects_existing_authenticated_reply_shape);
	UT_RUN(test_r4_status24_physical_generation_echo_is_exact);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
