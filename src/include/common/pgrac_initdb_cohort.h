/* Same-build, transient original-creator request, never a disk authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef PGRAC_INITDB_COHORT_H
#define PGRAC_INITDB_COHORT_H

#include "common/pgrac_initdb_wal.h"

#define PGRAC_INITDB_COHORT_ENV "PGRAC_INITDB_COHORT_FD"
#define PGRAC_INITDB_COHORT_MAGIC UINT32_C(0x50474943)

typedef struct PgracInitdbCohortContext
{
	uint32 magic;
	uint32 segment_size;
	uint64 creator_pid;
	PgracInitdbConfigContext config;
	char cache_root[MAXPGPATH];
	char config_path[MAXPGPATH];
	char username[NAMEDATALEN];
	char encoding[32];
	char auth_local[64];
	char auth_host[64];
} PgracInitdbCohortContext;

#endif
