/* Original initdb's native checkpoint readback.  No startup authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef PGRAC_INITDB_NATIVE_WAL_H
#define PGRAC_INITDB_NATIVE_WAL_H

#include "catalog/pg_control.h"
#include "port/pg_crc32c.h"

typedef struct PgracInitdbWalObservation
{
	XLogRecPtr checkpoint_start;
	XLogRecPtr checkpoint_end;
	pg_crc32c checkpoint_crc;
} PgracInitdbWalObservation;

/* The original creator holds the new WAL directory and has synced its files.
 * Decode exactly control's shutdown record, never search forward or accept an
 * incomplete tail.  This is physical evidence, not an owner/close/ROOT grant.
 * Caller still rechecks directory/control identity before publishing anything.
 * No writes, no ownership of directory_fd. Refusal clears a distinct out. */
extern bool pgrac_initdb_wal_observe(int directory_fd, const ControlFileData *control,
	uint16 thread, PgracInitdbWalObservation *out);

#endif
