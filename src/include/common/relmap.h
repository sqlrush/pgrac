/*-------------------------------------------------------------------------
 *
 * relmap.h
 *    Native PostgreSQL relation-map bytes, shared with fresh initialization.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/common/relmap.h
 *
 * NOTES
 *    Definitions extracted unchanged from utils/cache/relmapper.c.
 *-------------------------------------------------------------------------
 */
#ifndef COMMON_RELMAP_H
#define COMMON_RELMAP_H

#include "c.h"
#include "common/relpath.h"
#include "port/pg_crc32c.h"

#define RELMAPPER_FILEMAGIC 0x592717
#define MAX_MAPPINGS 64

typedef struct RelMapping {
	Oid mapoid;
	RelFileNumber mapfilenumber;
} RelMapping;

typedef struct RelMapFile {
	int32 magic;
	int32 num_mappings;
	RelMapping mappings[MAX_MAPPINGS];
	pg_crc32c crc;
} RelMapFile;

StaticAssertDecl(sizeof(RelMapFile) == 524, "native relation map size changed");
StaticAssertDecl(offsetof(RelMapFile, crc) == 520, "native relation map CRC moved");

#endif /* COMMON_RELMAP_H */
