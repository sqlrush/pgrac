/*-------------------------------------------------------------------------
 *
 * cluster_terminal_authority.h
 *	  Smart Fusion terminal authority substrate (spec-6.2).
 *
 *	  This module is a small fail-closed decision layer for future Smart Fusion
 *	  data paths.  It does not ship holder-side CR serving, active-ITL transfer,
 *	  or Past Image.  It only answers whether the caller has enough explicit
 *	  evidence to trust a cross-instance terminal undo / TT outcome.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_terminal_authority.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TERMINAL_AUTHORITY_H
#define CLUSTER_TERMINAL_AUTHORITY_H

#include "access/transam.h"
#include "cluster/cluster_scn.h"

#define CLUSTER_TERMINAL_AUTH_MAX_NODES 128

typedef enum ClusterTerminalAuthorityState {
	CLUSTER_TERMINAL_AUTH_UNKNOWN = 0,
	CLUSTER_TERMINAL_AUTH_IN_PROGRESS,
	CLUSTER_TERMINAL_AUTH_COMMITTED,
	CLUSTER_TERMINAL_AUTH_ABORTED,
} ClusterTerminalAuthorityState;

typedef enum ClusterTerminalAuthorityReason {
	CLUSTER_TERMINAL_AUTH_OK = 0,
	CLUSTER_TERMINAL_AUTH_DISABLED,
	CLUSTER_TERMINAL_AUTH_INVALID_ORIGIN,
	CLUSTER_TERMINAL_AUTH_INVALID_XID,
	CLUSTER_TERMINAL_AUTH_EPOCH_UNKNOWN,
	CLUSTER_TERMINAL_AUTH_EPOCH_CHANGED,
	CLUSTER_TERMINAL_AUTH_OWNER_UNKNOWN,
	CLUSTER_TERMINAL_AUTH_OWNER_MISMATCH,
	CLUSTER_TERMINAL_AUTH_TERMINAL_UNKNOWN,
	CLUSTER_TERMINAL_AUTH_NONTERMINAL,
	CLUSTER_TERMINAL_AUTH_COMMIT_SCN_MISSING,
	CLUSTER_TERMINAL_AUTH_DURABLE_MISSING,
	CLUSTER_TERMINAL_AUTH_DURABLE_MISMATCH,
	CLUSTER_TERMINAL_AUTH_RETENTION_UNPROVEN,
	CLUSTER_TERMINAL_AUTH_REASON__COUNT
} ClusterTerminalAuthorityReason;

typedef struct ClusterTerminalAuthorityEvidence {
	bool enabled;
	int origin_node;
	TransactionId xid;

	/* Membership authority: absence or movement is fail-closed. */
	bool epoch_known;
	uint64 observed_epoch;
	uint64 current_epoch;

	/* Ownership authority: the terminal source must own the origin. */
	bool owner_known;
	int owner_node;

	/* Terminal outcome authority. */
	ClusterTerminalAuthorityState terminal_state;
	SCN terminal_scn; /* valid only for COMMITTED */

	/* Independent durable-TT commit evidence for COMMITTED outcomes. */
	bool durable_commit_required;
	bool durable_commit_resolved;
	SCN durable_commit_scn;

	/* Optional retention proof for recycling / retention decisions. */
	bool retention_required;
	bool retention_proven;
} ClusterTerminalAuthorityEvidence;

extern ClusterTerminalAuthorityReason
cluster_terminal_authority_decide(const ClusterTerminalAuthorityEvidence *ev);
extern bool cluster_terminal_authority_allows(const ClusterTerminalAuthorityEvidence *ev);
extern const char *cluster_terminal_authority_reason_name(ClusterTerminalAuthorityReason reason);

/* Counter hooks live with the durable-TT shmem region; pure tests do not link them. */
extern void cluster_terminal_authority_count_decision(ClusterTerminalAuthorityReason reason);
extern uint64 cluster_terminal_authority_check_count(void);
extern uint64 cluster_terminal_authority_ok_count(void);
extern uint64 cluster_terminal_authority_failclosed_count(void);
extern uint64 cluster_terminal_authority_epoch_failclosed_count(void);
extern uint64 cluster_terminal_authority_ownership_failclosed_count(void);
extern uint64 cluster_terminal_authority_unknown_failclosed_count(void);
extern uint64 cluster_terminal_authority_nonterminal_failclosed_count(void);
extern uint64 cluster_terminal_authority_durable_failclosed_count(void);
extern uint64 cluster_terminal_authority_retention_failclosed_count(void);

#endif /* CLUSTER_TERMINAL_AUTHORITY_H */
