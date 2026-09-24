/*-------------------------------------------------------------------------
 * pgrac_fenced_cib.h
 *    Native configuration observation, not a policy or isolation certificate.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_cib.h
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_CIB_H
#define PGRAC_FENCED_CIB_H

#include "pgrac_fenced_provider.h"

typedef struct PgracFencedCibObservation {
	uint64 admin_epoch;
	uint64 epoch;
	uint64 num_updates;
	uint8 configuration_digest[32];
} PgracFencedCibObservation;

/* Run in the existing bounded worker. Native query only, no file/shadow/CLI
 * fallback. Output is zero on failure. An observation neither prevents a
 * later configuration change nor proves that fencing has consumed an update.
 */
extern PgracFencedProviderResult pgrac_fenced_cib_observe(uint64 deadline_mono_ns,
														  PgracFencedCibObservation *out);

#endif
