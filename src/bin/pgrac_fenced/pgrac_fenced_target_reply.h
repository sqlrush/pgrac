/*-------------------------------------------------------------------------
 * pgrac_fenced_target_reply.h
 *    Exact target observations, deliberately separate from isolation proof.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_target_reply.h
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_TARGET_REPLY_H
#define PGRAC_FENCED_TARGET_REPLY_H

#include "pgrac_fenced_target_command.h"

#define PGRAC_TARGET_REPLY_MAX_BYTES 4096

typedef struct PgracFencedTargetObservation {
	uint8 target_boot_id[16];
	uint8 journal_digest[32];
	uint64 journal_sequence;
	uint32 runtime_id;
	uint32 flushed_routes;
} PgracFencedTargetObservation;

/* PGRAC: exact echoed observation only, never PROVEN or a provider readback.
 * Expectations are independent trusted inputs. Identity uses route_count=0;
 * other commands use the authenticated map's count. Out must not alias inputs
 * and is zero on every failure. Input length includes the canonical newline.
 */
extern bool pgrac_fenced_target_reply_decode(PgracFencedTargetCommand action,
											 const PgracFencedJournalRecordV1 *record,
											 const PgracFencedTargetV1 *target,
											 const uint8 target_boot[16], const uint8 challenge[16],
											 const uint8 inventory_digest[32], uint32 route_count,
											 const char *bytes, size_t length,
											 PgracFencedTargetObservation *out);

#endif
