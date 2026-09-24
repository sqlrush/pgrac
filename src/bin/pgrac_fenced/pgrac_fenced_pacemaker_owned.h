/*-------------------------------------------------------------------------
 * pgrac_fenced_pacemaker_owned.h
 *    Existing durable owner to target preparation and native power action.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_pacemaker_owned.h
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_PACEMAKER_OWNED_H
#define PGRAC_FENCED_PACEMAKER_OWNED_H

#include "pgrac_fenced_provider.h"

/* Fork-local accepted configuration/record/deadline only. OK denotes completion
 * of an action, never an OFF/drain certificate or permission to write. No retry,
 * implicit compensation, provider registration or change of management policy.
 */
extern PgracFencedProviderResult
pgrac_fenced_pacemaker_owned_actuate(const PgracFencedTargetV1 *target, bool turn_on);

#endif
