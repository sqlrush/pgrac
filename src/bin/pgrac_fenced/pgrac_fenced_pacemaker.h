/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_pacemaker.h
 *    Fixed native Pacemaker action client, separate from isolation proof.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_pacemaker.h
 *
 * NOTES
 *    PGRAC-original interface. Only the fenced owner calls this, after durable
 *    acceptance and exact node/guest mapping validation, inside its worker.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_PACEMAKER_H
#define PGRAC_FENCED_PACEMAKER_H

#include "pgrac_fenced_provider.h"

#define PGRAC_PACEMAKER_NODE_BYTES 64

typedef struct PgracFencedPacemakerActionV1 {
	char node[PGRAC_PACEMAKER_NODE_BYTES];
	uint8 operation_id[16];
	uint64 attempt;
	uint64 deadline_mono_ns;
	bool turn_on;
} PgracFencedPacemakerActionV1;

typedef struct PgracFencedPacemakerActionResultV1 {
	int32 native_status;
	int32 call_id; /* Diagnostic only; connection-local, never a proof identity. */
} PgracFencedPacemakerActionResultV1;

/* OK is only action completion. This function cannot produce a drained proof. */
extern PgracFencedProviderResult
pgrac_fenced_pacemaker_action(const PgracFencedPacemakerActionV1 *action,
							  PgracFencedPacemakerActionResultV1 *out);

#endif /* PGRAC_FENCED_PACEMAKER_H */
