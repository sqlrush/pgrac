/*-------------------------------------------------------------------------
 * Transient original-initdb child context.  This is not an on-disk format,
 * WAL claim, membership proof or serving authority.  Same-build processes
 * exchange this native carrier through one inherited, closed-writer pipe.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_INITDB_WAL_H
#define PGRAC_INITDB_WAL_H

#include "c.h"

#define PGRAC_INITDB_WAL_CONTEXT_ENV "PGRAC_INITDB_WAL_CONTEXT_FD"
#define PGRAC_INITDB_WAL_CONTEXT_MAGIC UINT32_C(0x50474957)
#define PGRAC_INITDB_WAL_BOOTSTRAP 1
#define PGRAC_INITDB_WAL_POSTBOOTSTRAP 2
#define PGRAC_INITDB_CONFIG_MAX_BYTES (1024 * 1024)

/* An explicit creation request, never a root-selected runtime input. */
typedef struct PgracInitdbConfigContext {
	uint64 device;
	uint64 inode;
	uint32 bytes;
	int32 fd;
	uint8 sha256[32];
} PgracInitdbConfigContext;

StaticAssertDecl(sizeof(PgracInitdbConfigContext) == 56, "initdb configuration request carrier");

typedef struct PgracInitdbWalContext
{
	uint32 magic;
	uint16 thread_id;
	uint16 phase;
	uint64 system_identifier; /* zero only when the first child selects it */
	uint64 data_device;
	uint64 data_inode;
	uint64 wal_device;
	uint64 wal_inode;
	/* Optional original founder's new shared-base target. Zero as a group
	 * outside post-bootstrap base creation; never an online writer grant. */
	uint64 database_incarnation;
	uint64 base_device;
	uint64 base_inode;
	uint8 storage_uuid[16];
	int32 base_fd;
	uint32 reserved;
	PgracInitdbConfigContext config;
} PgracInitdbWalContext;

StaticAssertDecl(sizeof(PgracInitdbWalContext) == 152, "initdb pipe carrier must have no padding");

#endif
