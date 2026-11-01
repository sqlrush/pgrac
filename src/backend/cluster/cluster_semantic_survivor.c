/*-------------------------------------------------------------------------
 * cluster_semantic_survivor.c
 *    Stateless survivor v4 syntax, separate from proof and admission.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_semantic_survivor.h"
#include "port/pg_crc32c.h"

static uint64
survivor_read(const uint8 *p, unsigned bytes)
{
	uint64 v = 0;
	for (unsigned i = 0; i < bytes; i++) v |= (uint64)p[i] << (i * 8);
	return v;
}
static void
survivor_write(uint8 *p, uint64 v, unsigned bytes)
{
	for (unsigned i = 0; i < bytes; i++) { p[i] = v & 255; v >>= 8; }
}
static bool
survivor_nonzero(const uint8 *p, Size bytes)
{
	for (Size i = 0; i < bytes; i++) if (p[i] != 0) return true;
	return false;
}
static bool
survivor_generation(uint64 value)
{
	return value != 0 && value != UINT64_MAX;
}
static bool
survivor_member(const uint64 members[2], uint32 node)
{
	return node < 128 && (members[node / 64] & (UINT64_C(1) << (node % 64))) != 0;
}
static unsigned
survivor_count(const uint64 members[2])
{
	unsigned count = 0;
	for (unsigned i = 0; i < 128; i++) if (survivor_member(members, i)) count++;
	return count;
}
static uint32
survivor_crc(const uint8 *bytes, Size length)
{
	pg_crc32c crc;
	const uint32 zero = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 12);
	COMP_CRC32C(crc, &zero, 4);
	COMP_CRC32C(crc, bytes + 16, length - 16);
	FIN_CRC32C(crc);
	return (uint32)crc;
}
static bool
survivor_identity(const uint8 *bytes, uint32 node)
{
	if (survivor_read(bytes, 4) != node || survivor_read(bytes + 4, 4) == 0)
		return false;
	for (unsigned i = 8; i < 48; i += 8)
		if (!survivor_generation(survivor_read(bytes + i, 8))) return false;
	return true;
}

/* Only structural bindings; no digest, byte count or zero count proves an
 * original owner's quiescence, authority, membership or durable result. */
static bool
survivor_value_valid(const ClusterSurvivorMessageV4 *m, const uint8 format[32])
{
	unsigned count, start = 0, rows;
	Size stride;
	const uint8 *row;

	if (m == NULL || format == NULL || !survivor_nonzero(format, 32)
		|| memcmp(m->format, format, 32) != 0 || m->body == NULL
		|| m->purpose < 1 || m->purpose > 3 || m->kind < 1 || m->kind > 2
		|| m->stage < 1 || m->stage > 4
		|| !survivor_generation(m->nonce) || !survivor_generation(m->epoch)
		|| !survivor_generation(m->formation) || !survivor_generation(m->native_freeze)
		|| !survivor_generation(m->round_generation)
		|| !survivor_member(m->survivors, m->coordinator)
		|| !survivor_member(m->survivors, m->sender)
		|| !survivor_nonzero(m->basis, 32) || !survivor_nonzero(m->proof, 32)
		|| m->body_bytes > CLUSTER_SURVIVOR_SCRATCH_BYTES - CLUSTER_SURVIVOR_HEADER_BYTES)
		return false;
	count = survivor_count(m->survivors);
	if (m->purpose != CLUSTER_SURVIVOR_SERVICE) {
		const uint8 *e = m->body;
		if (m->body_kind != CLUSTER_SURVIVOR_EXECUTOR_ROW || m->chunk_start != 0
			|| m->chunk_count != 0 || m->body_bytes != 96)
			return false;
		/* Process slot and PID remain signed-compatible; no unsigned -1. */
		return survivor_member(m->survivors, survivor_read(e, 4))
			&& survivor_read(e + 4, 4) <= INT32_MAX
			&& survivor_generation(survivor_read(e + 8, 8))
			&& survivor_read(e + 16, 4) > 0 && survivor_read(e + 16, 4) <= INT32_MAX
			&& survivor_generation(survivor_read(e + 24, 8))
			&& survivor_generation(survivor_read(e + 32, 8))
			&& survivor_nonzero(e + 40, 32)
			&& survivor_generation(survivor_read(e + 72, 8))
			&& survivor_generation(survivor_read(e + 88, 8));
	}
	if (m->kind == CLUSTER_SURVIVOR_ACK)
		return m->body_kind == CLUSTER_SURVIVOR_ACK_RECEIPT && m->body_bytes == 80
			&& m->chunk_start == 0 && m->chunk_count == 0
			&& survivor_identity(m->body, m->sender) && survivor_nonzero(m->body + 48, 32);
	if (m->sender != m->coordinator)
		return false;
	if (m->stage == CLUSTER_SURVIVOR_OPEN_INSTALLED) {
		if (m->body_kind != CLUSTER_SURVIVOR_OPEN_PROOF || m->chunk_count == 0
			|| m->chunk_count > CLUSTER_SURVIVOR_CHUNK_ROWS || m->chunk_start >= count
			|| (unsigned)m->chunk_start + m->chunk_count > count)
			return false;
		start = m->chunk_start; rows = m->chunk_count; stride = 80;
	} else {
		if (m->body_kind != CLUSTER_SURVIVOR_MANIFEST || m->chunk_start != 0
			|| m->chunk_count != 0)
			return false;
		rows = count; stride = 48;
	}
	if (m->body_bytes != 1536 + 80 + rows * stride)
		return false;
	/* Immutable basis' encoded scalar fields, not native struct memcpy. */
	if (!survivor_nonzero(m->body, 512) || !survivor_nonzero(m->body + 512, 512)
		|| memcmp(m->body + 1104, format, 32) != 0
		|| !survivor_generation(survivor_read(m->body + 1136, 8))
		|| survivor_read(m->body + 1144, 8) != m->formation
		|| survivor_read(m->body + 1152, 8) != m->native_freeze
		|| !survivor_generation(survivor_read(m->body + 1160, 8))
		|| survivor_read(m->body + 1168, 8) != m->nonce
		|| survivor_read(m->body + 1176, 8) != m->survivors[0]
		|| survivor_read(m->body + 1184, 8) != m->survivors[1]
		|| (survivor_read(m->body + 1192, 8) & m->survivors[0]) != 0
		|| (survivor_read(m->body + 1200, 8) & m->survivors[1]) != 0
		|| survivor_nonzero(m->body + 1304, 232))
		return false;
	for (unsigned i = 0; i < 3; i++) {
		bool present = survivor_nonzero(m->body + 1208 + i * 32, 32);
		if (present != (m->stage != CLUSTER_SURVIVOR_QUIESCED)) return false;
	}
	row = m->body + 1536 + 80;
	for (unsigned node = 0, index = 0; node < 128; node++) {
		if (!survivor_member(m->survivors, node)) continue;
		if (index++ < start) continue;
		if (!survivor_identity(row, node) || (stride == 80 && !survivor_nonzero(row + 48, 32)))
			return false;
		row += stride;
		if (--rows == 0) break;
	}
	return true;
}

bool
cluster_semantic_survivor_v4_encode(const ClusterSurvivorMessageV4 *message,
	const uint8 format[32], uint8 *bytes, Size capacity, Size *written)
{
	ClusterSurvivorMessageV4 m;
	Size length;

	if (bytes == NULL || written == NULL || !survivor_value_valid(message, format))
		return false;
	m = *message;
	length = CLUSTER_SURVIVOR_HEADER_BYTES + m.body_bytes;
	if (capacity < length) return false;
	/* Borrowed input may overlap output. Copy body before clearing header. */
	memmove(bytes + 192, m.body, m.body_bytes);
	memset(bytes, 0, 192);
	survivor_write(bytes, 4, 2); survivor_write(bytes + 2, m.purpose, 2);
	survivor_write(bytes + 4, m.kind, 2); survivor_write(bytes + 6, m.stage, 2);
	survivor_write(bytes + 8, length, 4);
	survivor_write(bytes + 16, m.nonce, 8); survivor_write(bytes + 24, m.epoch, 8);
	survivor_write(bytes + 32, m.formation, 8); survivor_write(bytes + 40, m.native_freeze, 8);
	survivor_write(bytes + 48, m.round_generation, 8); survivor_write(bytes + 56, m.features, 8);
	survivor_write(bytes + 64, m.coordinator, 4); survivor_write(bytes + 68, m.sender, 4);
	survivor_write(bytes + 72, m.survivors[0], 8); survivor_write(bytes + 80, m.survivors[1], 8);
	memcpy(bytes + 88, m.basis, 32); memcpy(bytes + 120, m.proof, 32);
	memcpy(bytes + 152, m.format, 32);
	survivor_write(bytes + 184, m.chunk_start, 2); survivor_write(bytes + 186, m.chunk_count, 2);
	survivor_write(bytes + 188, m.body_kind, 2);
	survivor_write(bytes + 12, survivor_crc(bytes, length), 4);
	*written = length;
	return true;
}

bool
cluster_semantic_survivor_v4_decode(const uint8 *bytes, Size length,
	const uint8 format[32], ClusterSurvivorMessageV4 *message)
{
	ClusterSurvivorMessageV4 m;

	if (bytes == NULL || message == NULL || length < CLUSTER_SURVIVOR_HEADER_BYTES
		|| length > CLUSTER_SURVIVOR_SCRATCH_BYTES || survivor_read(bytes, 2) != 4
		|| survivor_read(bytes + 8, 4) != length || survivor_read(bytes + 190, 2) != 0
		|| survivor_read(bytes + 12, 4) != survivor_crc(bytes, length))
		return false;
	memset(&m, 0, sizeof(m));
	m.purpose = survivor_read(bytes + 2, 2); m.kind = survivor_read(bytes + 4, 2);
	m.stage = survivor_read(bytes + 6, 2); m.nonce = survivor_read(bytes + 16, 8);
	m.epoch = survivor_read(bytes + 24, 8); m.formation = survivor_read(bytes + 32, 8);
	m.native_freeze = survivor_read(bytes + 40, 8); m.round_generation = survivor_read(bytes + 48, 8);
	m.features = survivor_read(bytes + 56, 8); m.coordinator = survivor_read(bytes + 64, 4);
	m.sender = survivor_read(bytes + 68, 4); m.survivors[0] = survivor_read(bytes + 72, 8);
	m.survivors[1] = survivor_read(bytes + 80, 8);
	memcpy(m.basis, bytes + 88, 32); memcpy(m.proof, bytes + 120, 32);
	memcpy(m.format, bytes + 152, 32);
	m.chunk_start = survivor_read(bytes + 184, 2); m.chunk_count = survivor_read(bytes + 186, 2);
	m.body_kind = survivor_read(bytes + 188, 2); m.body = bytes + 192; m.body_bytes = length - 192;
	if (!survivor_value_valid(&m, format)) return false;
	*message = m;
	return true;
}
