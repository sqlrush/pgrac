/*-------------------------------------------------------------------------
 *
 * pgrac_protected_set.h
 *    Canonical identities for a declared set of protected storage routes.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/include/common/pgrac_protected_set.h
 *
 * NOTES
 *    PGRAC-original frontend/backend interface. These are in-memory inputs,
 *    not wire structs. A digest is an identity, never proof of I/O isolation.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_PROTECTED_SET_H
#define PGRAC_PROTECTED_SET_H

#include "c.h"

#define PGRAC_PROTECTED_SET_V2_MAX_ROUTES 128
#define PGRAC_PROTECTED_SET_V2_MAX_TEXT_BYTES 1024
#define PGRAC_PROTECTED_SET_V2_MAX_BYTES 65536
#define PGRAC_PROTECTED_SET_DIGEST_BYTES 32
#define PGRAC_PROTECTED_SET_UUID_BYTES 16

#define PGRAC_PROTECTED_ROUTE_ISCSI UINT32_C(1)
#define PGRAC_PROTECTED_ROUTE_HOST UINT32_C(2)
#define PGRAC_PROTECTED_MEDIA_FILESYSTEM UINT32_C(1)
#define PGRAC_PROTECTED_MEDIA_RAW UINT32_C(2)
#define PGRAC_PROTECTED_ROLE_DATA UINT32_C(1)
#define PGRAC_PROTECTED_ROLE_WAL UINT32_C(2)
#define PGRAC_PROTECTED_ROLE_UNDO UINT32_C(4)
#define PGRAC_PROTECTED_ROLE_VOTING UINT32_C(8)
#define PGRAC_PROTECTED_ROLE_ALL UINT32_C(15)

typedef struct PgracProtectedTextV2 {
	const char *data;
	uint32 length;
} PgracProtectedTextV2;

typedef struct PgracProtectedRouteV2 {
	uint32 kind;
	PgracProtectedTextV2 initiator;
	PgracProtectedTextV2 credential_ref;
	PgracProtectedTextV2 target;
	PgracProtectedTextV2 endpoint;
	uint32 tpg;
	PgracProtectedTextV2 lun_wwid;
	PgracProtectedTextV2 lun_serial;
	uint8 backstore_uuid[PGRAC_PROTECTED_SET_UUID_BYTES];
	uint32 media_kind;
	uint8 filesystem_uuid[PGRAC_PROTECTED_SET_UUID_BYTES];
	uint32 roles;
} PgracProtectedRouteV2;

typedef struct PgracProtectedSetV2 {
	uint32 backend_id;
	uint8 database_uuid[PGRAC_PROTECTED_SET_UUID_BYTES];
	uint8 storage_uuid[PGRAC_PROTECTED_SET_UUID_BYTES];
	uint8 authority_uuid[PGRAC_PROTECTED_SET_UUID_BYTES];
	PgracProtectedTextV2 certification_profile;
	uint64 mapping_generation;
	uint8 hypervisor_uuid[PGRAC_PROTECTED_SET_UUID_BYTES];
	uint8 guest_uuid[PGRAC_PROTECTED_SET_UUID_BYTES];
	uint32 route_count;
	const PgracProtectedRouteV2 *routes;
} PgracProtectedSetV2;

/* Borrowed text views require the immutable input buffer to remain alive. */
typedef struct PgracProtectedSetDecodedV2 {
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[PGRAC_PROTECTED_SET_V2_MAX_ROUTES];
} PgracProtectedSetDecodedV2;

/*
 * Hash all declared routes in canonical order without changing the input.
 * The caller must authenticate and certify completeness of the inventory.
 * References must identify credentials, never contain credential secrets.
 * Output must not alias an input; it is zeroed on failure when non-NULL.
 */
extern bool
pgrac_external_fence_protected_set_digest_v2(const PgracProtectedSetV2 *set,
											 uint8 digest[PGRAC_PROTECTED_SET_DIGEST_BYTES]);

/* Encode canonical preimage; written is zero on failure, even after partial output. */
extern bool pgrac_protected_set_v2_encode(const PgracProtectedSetV2 *set, uint8 *bytes,
										  size_t capacity, size_t *written);

/* Parse only, not authenticate. No shallow copy or alias with input is permitted. */
extern bool pgrac_protected_set_v2_decode(const uint8 *bytes, size_t length,
										  PgracProtectedSetDecodedV2 *out);

#endif /* PGRAC_PROTECTED_SET_H */
