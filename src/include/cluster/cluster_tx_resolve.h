/*-------------------------------------------------------------------------
 *
 * cluster_tx_resolve.h
 *	  Exact transaction identity and outcome contracts for R4 synchronous CR.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_tx_resolve.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TX_RESOLVE_H
#define CLUSTER_TX_RESOLVE_H

#include "c.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_uba.h"
#include "storage/bufpage.h"

#ifdef USE_PGRAC_CLUSTER

typedef struct ClusterSemanticAdmissionToken ClusterSemanticAdmissionToken;
typedef UBA ClusterUndoByteAddress;

StaticAssertDecl(sizeof(ClusterUndoByteAddress) == 16, "R4 UBA alias must remain 16 bytes");

typedef enum ClusterTxResolveMode {
	CLUSTER_TX_RESOLVE_VISIBILITY = 0,
	CLUSTER_TX_RESOLVE_ROW_WAIT = 1,
	CLUSTER_TX_RESOLVE_CR_BUILD = 2,
	CLUSTER_TX_RESOLVE_CLEANOUT_HINT = 3,
	CLUSTER_TX_RESOLVE_TERMINAL_CENSUS = 4
} ClusterTxResolveMode;

typedef enum ClusterTxResolveReason {
	CLUSTER_TX_RESOLVE_NONE = 0,
	CLUSTER_TX_RESOLVE_TARGET_DISABLED = 1,
	CLUSTER_TX_RESOLVE_RF_DEFERRED = 2,
	CLUSTER_TX_RESOLVE_BAD_LOCATOR = 3,
	CLUSTER_TX_RESOLVE_BAD_UBA = 4,
	CLUSTER_TX_RESOLVE_XID_MISMATCH = 5,
	CLUSTER_TX_RESOLVE_WRAP_MISMATCH = 6,
	CLUSTER_TX_RESOLVE_SLOT_MISMATCH = 7,
	CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE = 8,
	CLUSTER_TX_RESOLVE_AUTHORITY_STALE = 9,
	CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT = 10,
	CLUSTER_TX_RESOLVE_COVERAGE_GAP = 11,
	CLUSTER_TX_RESOLVE_SUBTRANS_CHANGED = 12,
	CLUSTER_TX_RESOLVE_SUBTRANS_CYCLE = 13,
	CLUSTER_TX_RESOLVE_SUBTRANS_DEPTH = 14,
	CLUSTER_TX_RESOLVE_TWOPHASE_CONFLICT = 15,
	CLUSTER_TX_RESOLVE_BAD_COMPOSITION = 16,
	CLUSTER_TX_RESOLVE_COMPOSITION_CHANGED = 17,
	CLUSTER_TX_RESOLVE_HORIZON_RECYCLED = 18,
	CLUSTER_TX_RESOLVE_HOLDER_MOVED = 19,
	CLUSTER_TX_RESOLVE_CAPACITY = 20,
	CLUSTER_TX_RESOLVE_TIMEOUT = 21,
	CLUSTER_TX_RESOLVE_CANCELLED = 22,
	CLUSTER_TX_RESOLVE_REENTRANT = 23,
	CLUSTER_TX_RESOLVE_IO_ERROR = 24,
	CLUSTER_TX_RESOLVE_PROTOCOL = 25
} ClusterTxResolveReason;

typedef enum ClusterTxOutcome {
	CLUSTER_TX_UNKNOWN = 0,
	CLUSTER_TX_IN_PROGRESS = 1,
	CLUSTER_TX_PREPARED = 2,
	CLUSTER_TX_COMMITTED = 3,
	CLUSTER_TX_ABORTED = 4
} ClusterTxOutcome;

typedef enum ClusterTxProofKind {
	CLUSTER_TX_PROOF_NONE = 0,
	CLUSTER_TX_PROOF_ITL_CLEANOUT = 1,
	CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG = 2,
	CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP = 3,
	CLUSTER_TX_PROOF_ORIGIN_TWOPHASE = 4,
	CLUSTER_TX_PROOF_ORIGIN_MULTIXACT = 5,
	CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED = 6,
	CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON = 7
} ClusterTxProofKind;

/*
 * Closed R4 outcome/proof compatibility table.  This is the common policy
 * consumed by verdict decoders and operation-local resolution consumers;
 * an enum-domain extension is invalid until this table is amended with it.
 * RECYCLED_BELOW_HORIZON proves only loss of the requested historical
 * version, never a fabricated terminal transaction outcome.
 */
static inline bool
cluster_tx_outcome_proof_is_valid(ClusterTxOutcome outcome, ClusterTxProofKind proof_kind)
{
	static const uint8 valid_proofs[5] = {
		[CLUSTER_TX_UNKNOWN]
		= (uint8)((1U << CLUSTER_TX_PROOF_NONE) | (1U << CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON)),
		[CLUSTER_TX_IN_PROGRESS] = (uint8)((1U << CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG)
										   | (1U << CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP)
										   | (1U << CLUSTER_TX_PROOF_ORIGIN_MULTIXACT)),
		[CLUSTER_TX_PREPARED] = (uint8)((1U << CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP)
										| (1U << CLUSTER_TX_PROOF_ORIGIN_TWOPHASE)
										| (1U << CLUSTER_TX_PROOF_ORIGIN_MULTIXACT)),
		[CLUSTER_TX_COMMITTED] = (uint8)((1U << CLUSTER_TX_PROOF_ITL_CLEANOUT)
										 | (1U << CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG)
										 | (1U << CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP)
										 | (1U << CLUSTER_TX_PROOF_ORIGIN_MULTIXACT)
										 | (1U << CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED)),
		[CLUSTER_TX_ABORTED] = (uint8)((1U << CLUSTER_TX_PROOF_ITL_CLEANOUT)
									   | (1U << CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG)
									   | (1U << CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP)
									   | (1U << CLUSTER_TX_PROOF_ORIGIN_MULTIXACT)
									   | (1U << CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED)),
	};

	if ((unsigned int)outcome >= lengthof(valid_proofs)
		|| (unsigned int)proof_kind > (unsigned int)CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON)
		return false;
	return (valid_proofs[outcome] & (uint8)(1U << proof_kind)) != 0;
}

typedef struct ClusterTxLocator {
	ClusterUndoByteAddress uba;
	TransactionId xid;
	uint16 tt_wrap;
	uint8 itl_kind;
	uint8 itl_slot_index;
} ClusterTxLocator;

StaticAssertDecl(sizeof(ClusterTxLocator) == 24,
				 "R4 ClusterTxLocator must remain an exact 24-byte value");

typedef struct ClusterTxResolution {
	ClusterTxLocator locator_echo;
	TransactionId top_xid;
	ClusterTxOutcome outcome;
	ClusterTxProofKind proof_kind;
	SCN commit_scn;
	SCN horizon_scn;
	ClusterLiveAuthority authority;
} ClusterTxResolution;

/* A page-derived request carries TT_WRAP_INVALID until the origin reads the
 * exact undo record under Candidate-2 SCUR.  Every other identity byte is
 * immutable; a canonical request never permits a wrap substitution. */
static inline bool
cluster_tx_locator_reply_matches(const ClusterTxLocator *request, const ClusterTxLocator *reply)
{
	if (request == NULL || reply == NULL || request->uba.raw[0] != reply->uba.raw[0]
		|| request->uba.raw[1] != reply->uba.raw[1] || request->xid != reply->xid
		|| request->itl_kind != reply->itl_kind || request->itl_slot_index != reply->itl_slot_index)
		return false;
	if (request->tt_wrap == TT_WRAP_INVALID)
		return reply->tt_wrap <= TT_WRAP_MAX;
	return request->tt_wrap == reply->tt_wrap;
}

#define CLUSTER_R4_MAX_MULTI_MEMBERS 256
#define CLUSTER_R4_SUBTRANS_MAX_DEPTH 1024

typedef struct ClusterMultiResolutionMember {
	SCN commit_scn;
	TransactionId xid;
	uint16 tt_wrap;
	uint8 native_status;
	uint8 outcome;
	uint8 proof_kind;
	uint8 flags;
	uint8 reserved[6];
} ClusterMultiResolutionMember;

StaticAssertDecl(sizeof(ClusterMultiResolutionMember) == 24,
				 "R4 MultiXact member must remain an exact 24-byte value");

typedef struct ClusterMultiResolution {
	MultiXactId mxid_echo;
	uint32 member_generation;
	uint16 member_count;
	uint16 reserved;
	ClusterMultiResolutionMember members[CLUSTER_R4_MAX_MULTI_MEMBERS];
} ClusterMultiResolution;

extern bool cluster_tx_locator_from_itl(Page page, uint8 slot_index, ClusterTxLocator *out,
										ClusterTxResolveReason *reason_out);
extern bool cluster_tx_locator_from_itl_terminal_census(Page page, uint8 slot_index,
														ClusterTxLocator *out,
														ClusterTxResolveReason *reason_out);
extern ClusterTxOutcome cluster_tx_resolve_exact(const ClusterTxLocator *locator,
												 ClusterTxResolveMode mode,
												 ClusterTxResolution *out,
												 ClusterTxResolveReason *reason_out);
extern ClusterTxOutcome
cluster_tx_resolve_exact_admitted(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
								  const ClusterSemanticAdmissionToken *admission,
								  ClusterTxResolution *out, ClusterTxResolveReason *reason_out);
extern ClusterTxOutcome cluster_tx_resolve_terminal_census_retained_admitted(
	const ClusterTxLocator *locator, SCN retained_commit_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out);
extern void cluster_tx_resolve_terminal_census_batch_preflight(void);
extern ClusterTxOutcome cluster_tx_resolve_multixact(MultiXactId mxid, ClusterMultiResolution *out,
													 ClusterTxResolveReason *reason_out);
extern const char *cluster_tx_resolve_reason_name(ClusterTxResolveReason reason);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_TX_RESOLVE_H */
