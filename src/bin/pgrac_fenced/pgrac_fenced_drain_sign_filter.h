/*-------------------------------------------------------------------------
 * pgrac_fenced_drain_sign_filter.h
 *    Restricted target owner's bounded signing filter.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_drain_sign_filter.h
 * NOTES
 *    PGRAC-original codec, not a source of native isolation facts.
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_DRAIN_SIGN_FILTER_H
#define PGRAC_FENCED_DRAIN_SIGN_FILTER_H

extern int pgrac_fenced_drain_sign_filter(int argc, char *const *argv, FILE *input, FILE *output);

#endif
