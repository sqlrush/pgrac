/*-------------------------------------------------------------------------
 * pgrac_fenced_target_client.h
 *    One fixed management-client invocation in the existing provider worker.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_target_client.h
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_TARGET_CLIENT_H
#define PGRAC_FENCED_TARGET_CLIENT_H

#include "pgrac_fenced_target_reply.h"

/* Trusted local owner configuration, never DB request fields. Fixed basename
 * and interpreter; this is not an arbitrary command-template interface.
 */
typedef struct PgracFencedTargetClientPaths {
	const char *bundle_directory;
	const char *config_file;
} PgracFencedTargetClientPaths;

/* Linux root, dedicated single-thread group leader inside the existing bounded
 * provider worker only. Its outer owner retains final process-group cleanup.
 * Success means exactly one bounded stdout/EOF/exit-zero, NOT isolation proof.
 * Invoke the exact reply decoder separately. Failure empties output/length.
 * Output capacity must be at least PGRAC_TARGET_REPLY_MAX_BYTES + 1, no aliasing.
 */
extern bool pgrac_fenced_target_client_exchange(const PgracFencedTargetClientPaths *paths,
												const char *command, size_t command_length,
												uint64 deadline_mono_ns, char *output,
												size_t capacity, size_t *length);

#endif
