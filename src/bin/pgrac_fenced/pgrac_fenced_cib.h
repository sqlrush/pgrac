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

#define PGRAC_CIB_POLICY_MAX_NODES 128
#define PGRAC_CIB_NAME_BYTES 64

typedef struct PgracFencedCibNode {
	char name[PGRAC_CIB_NAME_BYTES];
	uint8 guest_uuid[16];
} PgracFencedCibNode;

/* Immutable expectation supplied by protected, deployment-qualified management
 * configuration. Never populate it from the current observation itself.
 * The full digest pins endpoints, defaults and constraints as well as mapping.
 */
typedef struct PgracFencedCibPolicy {
	char resource[PGRAC_CIB_NAME_BYTES];
	uint8 configuration_digest[32];
	unsigned node_count;
	const PgracFencedCibNode *nodes;
} PgracFencedCibPolicy;

/* Run in the existing bounded worker. Native query only, no file/shadow/CLI
 * fallback. Output is zero on failure. An observation neither prevents a
 * later configuration change nor proves that fencing has consumed an update.
 */
extern PgracFencedProviderResult pgrac_fenced_cib_observe(uint64 deadline_mono_ns,
														  PgracFencedCibObservation *out);

/* Checks the fixed profile and pin against the same native snapshot. Like the
 * plain observation this is not a policy lock, OFF/drain proof or admission.
 * Inputs and output must not alias. All failures zero the output.
 */
extern PgracFencedProviderResult pgrac_fenced_cib_check(uint64 deadline_mono_ns,
														const PgracFencedCibPolicy *policy,
														PgracFencedCibObservation *out);

#endif
