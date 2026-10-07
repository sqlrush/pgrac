/*-------------------------------------------------------------------------
 * pgrac_fenced_target_command.h
 *    Bind existing target-channel commands to the durable C operation owner.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_target_command.h
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_TARGET_COMMAND_H
#define PGRAC_FENCED_TARGET_COMMAND_H

#include "pgrac_fenced_journal.h"
#include "pgrac_fenced_provider.h"

#define PGRAC_TARGET_COMMAND_MAX_BYTES 1024

typedef enum PgracFencedTargetCommand {
	PGRAC_TARGET_IDENTITY = 0,
	PGRAC_TARGET_PREPARE_DENY,
	PGRAC_TARGET_COMPLETE_OFF,
	PGRAC_TARGET_REJOIN_RESTORE,
	PGRAC_TARGET_REJOIN_RUNNING,
	PGRAC_TARGET_REJOIN_PREPARE_REVOKE,
	PGRAC_TARGET_REJOIN_COMPLETE_OFF
} PgracFencedTargetCommand;

/* PGRAC: trusted owner inputs only; encoding neither fsyncs nor grants authority.
 * Output is NUL-terminated canonical JSON; length excludes NUL, includes newline.
 * Failure empties output/length. Input/output must not alias.
 */
extern bool pgrac_fenced_target_command_encode(PgracFencedTargetCommand action,
											   const PgracFencedJournalRecordV1 *record,
											   const PgracFencedTargetV1 *target,
											   const uint8 target_boot[16],
											   const uint8 challenge[16], char *output,
											   size_t capacity, size_t *length);

#endif
