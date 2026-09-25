/*
 * controldata_utils.h
 *		Common code for pg_controldata output
 *
 *	Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *	Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/include/common/controldata_utils.h
 */
#ifndef COMMON_CONTROLDATA_UTILS_H
#define COMMON_CONTROLDATA_UTILS_H

#include "catalog/pg_control.h"

extern ControlFileData *get_controlfile(const char *DataDir, bool *crc_ok_p);
extern void update_controlfile(const char *DataDir,
							   ControlFileData *ControlFile, bool do_sync);

#ifdef FRONTEND
/* PGRAC: refuse unsupported native tools before their first data mutation.
 * Presence, not successful decoding, protects damaged shared-control state.
 * An unmarked directory is not admission or a concurrent-migration lock.
 * Author: SqlRush <sqlrush@gmail.com>
 */
extern void reject_pgrac_legacy_operation(const char *DataDir);
#endif

#endif							/* COMMON_CONTROLDATA_UTILS_H */
