/*-------------------------------------------------------------------------
 * PGRAC: independently retained new-database bootstrap identity.
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_CONTROL_BINDING_H
#define PGRAC_CONTROL_BINDING_H

#include "c.h"

#define PGRAC_CONTROL_BINDING_BYTES 256
#define PGRAC_CONTROL_BINDING_MAX_NODES 128
#define PGRAC_CONTROL_BINDING_NAME "pgrac_control_binding"

/* In-memory carrier, never a native-struct disk format. */
typedef struct PgracControlBinding {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];
	uint64 database_incarnation;
	uint32 node_id;
	uint32 reserved;
	uint8 migration_round_sha256[32];
	uint8 source_wal_state_sha256[32];
	uint64 migration_prepare_generation;
	uint64 migration_transition_epoch;
} PgracControlBinding;

typedef enum PgracControlBindingResult {
	PGRAC_CONTROL_BINDING_OK = 0,
	PGRAC_CONTROL_BINDING_INVALID,
	PGRAC_CONTROL_BINDING_MISSING,
	PGRAC_CONTROL_BINDING_UNSAFE,
	PGRAC_CONTROL_BINDING_IO_ERROR,
	PGRAC_CONTROL_BINDING_UNSUPPORTED
} PgracControlBindingResult;

/*
 * Exact-size codec. Input and output must not overlap; overlap is refused.
 * Output is cleared on refusal: sizeof(*out) for decode, at most
 * Min(capacity, PGRAC_CONTROL_BINDING_BYTES) for encode.
 * CRC detects damage, not authenticity, first-OPEN completion or admission.
 */
extern bool pgrac_control_binding_encode(const PgracControlBinding *binding, uint8 *bytes,
										 size_t capacity);
extern bool pgrac_control_binding_decode(const uint8 *bytes, size_t length,
										 PgracControlBinding *out);

/*
 * Read only PGDATA/global/pgrac_control_binding. PGDATA must be absolute and
 * contain no empty, dot or dot-dot components. PGDATA, global and the leaf are
 * opened without following their final component; earlier PGDATA ancestors
 * are trusted deployment paths. All three must be owned by the effective user
 * and not group/other writable. The leaf must be a regular, exact-size file.
 *
 * No allocation, resource owner, lock, mutation or fallback. Clear out on all
 * refusals. Unsupported platforms never return a positive binding. Input and
 * output must not overlap. Success is only a local identity record: consumers
 * must still prove qualified storage, current root lineage and admission.
 */
extern PgracControlBindingResult pgrac_control_binding_read(const char *pgdata,
															PgracControlBinding *out);

#endif /* PGRAC_CONTROL_BINDING_H */
