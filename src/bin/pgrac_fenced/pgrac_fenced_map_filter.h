/*-------------------------------------------------------------------------
 * pgrac_fenced_map_filter.h
 *    Bounded signed-map filter for the target-side configuration loader.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_map_filter.h
 *
 * NOTES
 *    PGRAC-original. Verification grants no storage or recovery authority.
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_MAP_FILTER_H
#define PGRAC_FENCED_MAP_FILTER_H

#include <stdio.h>

extern int pgrac_fenced_map_filter(int argc, char *const *argv, FILE *input, FILE *output);

#endif /* PGRAC_FENCED_MAP_FILTER_H */
