/*-------------------------------------------------------------------------
 *
 * pgrac_protected_set.c
 *    Canonical protected-route identity shared by frontend and backend.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/common/pgrac_protected_set.c
 *
 * NOTES
 *    PGRAC-original codec. It does not certify inventory completeness or
 *    perform isolation. No struct padding or credential secret is hashed.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/cryptohash.h"
#include "common/pgrac_protected_set.h"

/* Include the domain as a length-prefixed field, without its C terminator. */
static const char protected_set_domain[] = "PGRAC-PROTECTED-SET-V2";

typedef struct ProtectedSetHash {
	pg_cryptohash_ctx *context;
	size_t bytes;
	uint8 *output;
	size_t capacity;
} ProtectedSetHash;

static bool
uuid_present(const uint8 uuid[16])
{
	static const uint8 zero[16] = { 0 };

	return memcmp(uuid, zero, sizeof(zero)) != 0;
}

static bool
text_valid(const PgracProtectedTextV2 *text)
{
	uint32 i;

	if (text->data == NULL || text->length == 0
		|| text->length > PGRAC_PROTECTED_SET_V2_MAX_TEXT_BYTES)
		return false;
	for (i = 0; i < text->length; i++) {
		if ((unsigned char)text->data[i] < 0x21 || (unsigned char)text->data[i] > 0x7e)
			return false;
	}
	return true;
}

static bool
route_valid(const PgracProtectedRouteV2 *route)
{
	if ((route->kind != PGRAC_PROTECTED_ROUTE_ISCSI && route->kind != PGRAC_PROTECTED_ROUTE_HOST)
		|| ((route->kind == PGRAC_PROTECTED_ROUTE_ISCSI) != (route->tpg != 0))
		|| (route->media_kind != PGRAC_PROTECTED_MEDIA_FILESYSTEM
			&& route->media_kind != PGRAC_PROTECTED_MEDIA_RAW)
		|| ((route->media_kind == PGRAC_PROTECTED_MEDIA_FILESYSTEM)
			!= uuid_present(route->filesystem_uuid))
		|| !uuid_present(route->backstore_uuid) || route->roles == 0
		|| (route->roles & ~PGRAC_PROTECTED_ROLE_ALL) != 0)
		return false;
	return text_valid(&route->initiator) && text_valid(&route->credential_ref)
		   && text_valid(&route->target) && text_valid(&route->endpoint)
		   && text_valid(&route->lun_wwid) && text_valid(&route->lun_serial);
}

static bool
set_valid(const PgracProtectedSetV2 *set)
{
	uint32 i;
	uint32 roles = 0;

	/* This profile certifies cluster_fs, not the raw DATA backend. */
	if (set == NULL || set->backend_id != 3 || !uuid_present(set->database_uuid)
		|| !uuid_present(set->storage_uuid) || !uuid_present(set->authority_uuid)
		|| !uuid_present(set->hypervisor_uuid) || !uuid_present(set->guest_uuid)
		|| !text_valid(&set->certification_profile) || set->mapping_generation == 0
		|| set->routes == NULL || set->route_count == 0
		|| set->route_count > PGRAC_PROTECTED_SET_V2_MAX_ROUTES)
		return false;
	for (i = 0; i < set->route_count; i++) {
		if (!route_valid(&set->routes[i]))
			return false;
		roles |= set->routes[i].roles;
	}
	return roles == PGRAC_PROTECTED_ROLE_ALL;
}

static int
compare_u32(uint32 left, uint32 right)
{
	return left < right ? -1 : left > right;
}

static int
compare_text(const PgracProtectedTextV2 *left, const PgracProtectedTextV2 *right)
{
	int cmp = memcmp(left->data, right->data, Min(left->length, right->length));

	return cmp != 0 ? cmp : compare_u32(left->length, right->length);
}

static int
compare_routes(const void *left, const void *right)
{
	const PgracProtectedRouteV2 *a = *(const PgracProtectedRouteV2 *const *)left;
	const PgracProtectedRouteV2 *b = *(const PgracProtectedRouteV2 *const *)right;
	int cmp;

	cmp = compare_u32(a->kind, b->kind);
	if (cmp != 0)
		return cmp;
	cmp = compare_text(&a->initiator, &b->initiator);
	if (cmp != 0)
		return cmp;
	cmp = compare_text(&a->credential_ref, &b->credential_ref);
	if (cmp != 0)
		return cmp;
	cmp = compare_text(&a->target, &b->target);
	if (cmp != 0)
		return cmp;
	cmp = compare_text(&a->endpoint, &b->endpoint);
	if (cmp != 0)
		return cmp;
	cmp = compare_u32(a->tpg, b->tpg);
	if (cmp != 0)
		return cmp;
	cmp = compare_text(&a->lun_wwid, &b->lun_wwid);
	if (cmp != 0)
		return cmp;
	cmp = compare_text(&a->lun_serial, &b->lun_serial);
	if (cmp != 0)
		return cmp;
	cmp = memcmp(a->backstore_uuid, b->backstore_uuid, 16);
	if (cmp != 0)
		return cmp;
	cmp = compare_u32(a->media_kind, b->media_kind);
	if (cmp != 0)
		return cmp;
	cmp = memcmp(a->filesystem_uuid, b->filesystem_uuid, 16);
	return cmp != 0 ? cmp : compare_u32(a->roles, b->roles);
}

static void
put_u32_le(uint8 bytes[4], uint32 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
	bytes[2] = (uint8)(value >> 16);
	bytes[3] = (uint8)(value >> 24);
}

static bool
hash_field(ProtectedSetHash *hash, const void *bytes, uint32 length)
{
	uint8 prefix[4];

	if (length > PGRAC_PROTECTED_SET_V2_MAX_BYTES - sizeof(prefix)
		|| hash->bytes > PGRAC_PROTECTED_SET_V2_MAX_BYTES - sizeof(prefix) - length)
		return false;
	put_u32_le(prefix, length);
	if (hash->output != NULL) {
		if (hash->bytes > hash->capacity || hash->capacity - hash->bytes < sizeof(prefix) + length)
			return false;
		memcpy(hash->output + hash->bytes, prefix, sizeof(prefix));
		memcpy(hash->output + hash->bytes + sizeof(prefix), bytes, length);
	}
	if (hash->context != NULL
		&& (pg_cryptohash_update(hash->context, prefix, sizeof(prefix)) < 0
			|| pg_cryptohash_update(hash->context, bytes, length) < 0))
		return false;
	hash->bytes += sizeof(prefix) + length;
	return true;
}

static bool
hash_u32(ProtectedSetHash *hash, uint32 value)
{
	uint8 bytes[4];

	put_u32_le(bytes, value);
	return hash_field(hash, bytes, sizeof(bytes));
}

static bool
hash_u64(ProtectedSetHash *hash, uint64 value)
{
	uint8 bytes[8];

	put_u32_le(bytes, (uint32)value);
	put_u32_le(bytes + 4, (uint32)(value >> 32));
	return hash_field(hash, bytes, sizeof(bytes));
}

static bool
hash_text(ProtectedSetHash *hash, const PgracProtectedTextV2 *text)
{
	return hash_field(hash, text->data, text->length);
}

static bool
hash_header(ProtectedSetHash *hash, const PgracProtectedSetV2 *set)
{
	return hash_field(hash, protected_set_domain, sizeof(protected_set_domain) - 1)
		   && hash_u32(hash, set->backend_id) && hash_field(hash, set->database_uuid, 16)
		   && hash_field(hash, set->storage_uuid, 16) && hash_field(hash, set->authority_uuid, 16)
		   && hash_text(hash, &set->certification_profile)
		   && hash_u64(hash, set->mapping_generation) && hash_field(hash, set->hypervisor_uuid, 16)
		   && hash_field(hash, set->guest_uuid, 16) && hash_u32(hash, set->route_count);
}

static bool
hash_route(ProtectedSetHash *hash, const PgracProtectedRouteV2 *route)
{
	return hash_u32(hash, route->kind) && hash_text(hash, &route->initiator)
		   && hash_text(hash, &route->credential_ref) && hash_text(hash, &route->target)
		   && hash_text(hash, &route->endpoint) && hash_u32(hash, route->tpg)
		   && hash_text(hash, &route->lun_wwid) && hash_text(hash, &route->lun_serial)
		   && hash_field(hash, route->backstore_uuid, 16) && hash_u32(hash, route->media_kind)
		   && hash_field(hash, route->filesystem_uuid, 16) && hash_u32(hash, route->roles);
}

static bool
write_set(ProtectedSetHash *hash, const PgracProtectedSetV2 *set)
{
	const PgracProtectedRouteV2 *ordered[PGRAC_PROTECTED_SET_V2_MAX_ROUTES];
	uint32 i;

	if (!set_valid(set))
		return false;
	for (i = 0; i < set->route_count; i++)
		ordered[i] = &set->routes[i];
	qsort(ordered, set->route_count, sizeof(ordered[0]), compare_routes);
	for (i = 1; i < set->route_count; i++) {
		if (compare_routes(&ordered[i - 1], &ordered[i]) == 0)
			return false;
	}
	if (!hash_header(hash, set))
		return false;
	for (i = 0; i < set->route_count; i++) {
		if (!hash_route(hash, ordered[i]))
			return false;
	}
	return true;
}

bool
pgrac_external_fence_protected_set_digest_v2(const PgracProtectedSetV2 *set, uint8 digest[32])
{
	ProtectedSetHash hash = { 0 };
	uint8 result[PGRAC_PROTECTED_SET_DIGEST_BYTES];
	bool ok = false;

	if (digest == NULL)
		return false;
	memset(digest, 0, PGRAC_PROTECTED_SET_DIGEST_BYTES);
	hash.context = pg_cryptohash_create(PG_SHA256);
	if (hash.context == NULL)
		return false;
	if (pg_cryptohash_init(hash.context) < 0 || !write_set(&hash, set))
		goto done;
	if (pg_cryptohash_final(hash.context, result, sizeof(result)) < 0)
		goto done;
	memcpy(digest, result, sizeof(result));
	ok = true;

done:
	pg_cryptohash_free(hash.context);
	return ok;
}

bool
pgrac_protected_set_v2_encode(const PgracProtectedSetV2 *set, uint8 *bytes, size_t capacity,
							  size_t *written)
{
	ProtectedSetHash writer = { 0 };

	if (written == NULL)
		return false;
	*written = 0;
	if (bytes == NULL)
		return false;
	writer.output = bytes;
	writer.capacity = capacity;
	if (!write_set(&writer, set))
		return false;
	*written = writer.bytes;
	return true;
}

typedef struct ProtectedSetReader {
	const uint8 *cursor;
	size_t remaining;
} ProtectedSetReader;

static uint32
get_u32_le(const uint8 *bytes)
{
	return (uint32)bytes[0] | ((uint32)bytes[1] << 8) | ((uint32)bytes[2] << 16)
		   | ((uint32)bytes[3] << 24);
}

static bool
read_field(ProtectedSetReader *reader, const uint8 **bytes, uint32 *length)
{
	uint32 size;

	if (reader->remaining < 4)
		return false;
	size = get_u32_le(reader->cursor);
	if (size > reader->remaining - 4)
		return false;
	*bytes = reader->cursor + 4;
	*length = size;
	reader->cursor += 4 + size;
	reader->remaining -= 4 + size;
	return true;
}

static bool
read_fixed(ProtectedSetReader *reader, void *out, uint32 expected)
{
	const uint8 *bytes;
	uint32 length;

	if (!read_field(reader, &bytes, &length) || length != expected)
		return false;
	memcpy(out, bytes, expected);
	return true;
}

static bool
read_u32(ProtectedSetReader *reader, uint32 *out)
{
	uint8 bytes[4];

	if (!read_fixed(reader, bytes, sizeof(bytes)))
		return false;
	*out = get_u32_le(bytes);
	return true;
}

static bool
read_u64(ProtectedSetReader *reader, uint64 *out)
{
	uint8 bytes[8];

	if (!read_fixed(reader, bytes, sizeof(bytes)))
		return false;
	*out = (uint64)get_u32_le(bytes) | ((uint64)get_u32_le(bytes + 4) << 32);
	return true;
}

static bool
read_text(ProtectedSetReader *reader, PgracProtectedTextV2 *out)
{
	const uint8 *bytes;
	uint32 length;

	if (!read_field(reader, &bytes, &length))
		return false;
	out->data = (const char *)bytes;
	out->length = length;
	return text_valid(out);
}

static bool
read_header(ProtectedSetReader *reader, PgracProtectedSetV2 *set)
{
	const uint8 *domain;
	uint32 length;

	return read_field(reader, &domain, &length) && length == sizeof(protected_set_domain) - 1
		   && memcmp(domain, protected_set_domain, length) == 0
		   && read_u32(reader, &set->backend_id) && read_fixed(reader, set->database_uuid, 16)
		   && read_fixed(reader, set->storage_uuid, 16)
		   && read_fixed(reader, set->authority_uuid, 16)
		   && read_text(reader, &set->certification_profile)
		   && read_u64(reader, &set->mapping_generation)
		   && read_fixed(reader, set->hypervisor_uuid, 16)
		   && read_fixed(reader, set->guest_uuid, 16) && read_u32(reader, &set->route_count)
		   && set->route_count > 0 && set->route_count <= PGRAC_PROTECTED_SET_V2_MAX_ROUTES;
}

static bool
read_route(ProtectedSetReader *reader, PgracProtectedRouteV2 *route)
{
	return read_u32(reader, &route->kind) && read_text(reader, &route->initiator)
		   && read_text(reader, &route->credential_ref) && read_text(reader, &route->target)
		   && read_text(reader, &route->endpoint) && read_u32(reader, &route->tpg)
		   && read_text(reader, &route->lun_wwid) && read_text(reader, &route->lun_serial)
		   && read_fixed(reader, route->backstore_uuid, 16) && read_u32(reader, &route->media_kind)
		   && read_fixed(reader, route->filesystem_uuid, 16) && read_u32(reader, &route->roles);
}

bool
pgrac_protected_set_v2_decode(const uint8 *bytes, size_t length, PgracProtectedSetDecodedV2 *out)
{
	ProtectedSetReader reader;
	uint32 i;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (bytes == NULL || length == 0 || length > PGRAC_PROTECTED_SET_V2_MAX_BYTES)
		return false;
	reader.cursor = bytes;
	reader.remaining = length;
	if (!read_header(&reader, &out->set))
		goto bad_input;
	out->set.routes = out->routes;
	for (i = 0; i < out->set.route_count; i++) {
		const PgracProtectedRouteV2 *route = &out->routes[i];

		if (!read_route(&reader, &out->routes[i]) || !route_valid(route))
			goto bad_input;
		if (i > 0) {
			const PgracProtectedRouteV2 *prior = &out->routes[i - 1];

			if (compare_routes(&prior, &route) >= 0)
				goto bad_input;
		}
	}
	if (reader.remaining != 0 || !set_valid(&out->set))
		goto bad_input;
	return true;

bad_input:
	memset(out, 0, sizeof(*out));
	return false;
}
