/*-------------------------------------------------------------------------
 * pgrac_fenced_target_request.h
 *    Compose one existing worker's authenticated target observation.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_target_request.h
 * NOTES
 *    No isolation certificate or database admission is returned here.
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_TARGET_REQUEST_H
#define PGRAC_FENCED_TARGET_REQUEST_H

#include "pgrac_fenced_drain.h"
#include "pgrac_fenced_target_reply.h"

typedef struct PgracFencedTargetRequestResult {
	PgracFencedTargetObservation observation;
	/* Zero for identity-only. An expectation, not a signed proof. */
	PgracFencedDrainIdentityV1 expected;
} PgracFencedTargetRequestResult;

/* Consume the actual fork-local callback context and its original deadline.
 * Exactly one identity read, then at most one permitted action. No retry.
 * Invalid context/action or failed reply clears out. Inputs must not alias it.
 */
extern bool pgrac_fenced_target_request(PgracFencedTargetCommand action,
										const PgracFencedTargetV1 *target,
										PgracFencedTargetRequestResult *out);

#endif
