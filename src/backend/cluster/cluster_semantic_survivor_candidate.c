/*-------------------------------------------------------------------------
 * cluster_semantic_survivor_candidate.c
 *    Serialized, unconfirmed collection by the original LMON.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_semantic_survivor_candidate.h"
#include "../../common/sha2_int.h"

#define CANDIDATE_INVALID 1U
#define CANDIDATE_PENDING CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING
#define CANDIDATE_REFUSED CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED
#define CANDIDATE_STALE CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE

static ClusterSemanticAdmissionObservationResult
candidate_result(ClusterSemanticAdmissionObservationResult result, const char **out,
	const char *reason)
{
	if (out != NULL) *out = reason;
	return result;
}
static uint64
candidate_read(const uint8 *p, unsigned n)
{
	uint64 v = 0;
	for (unsigned i = 0; i < n; i++) v |= (uint64)p[i] << (8 * i);
	return v;
}
static void
candidate_write(uint8 *p, uint64 v)
{
	for (unsigned i = 0; i < 8; i++) { p[i] = v & 255; v >>= 8; }
}
static bool
candidate_zero(const void *value, Size n)
{
	const uint8 *p = value;
	for (Size i = 0; i < n; i++) if (p[i] != 0) return false;
	return true;
}
static bool
candidate_owner(void)
{
	return IsUnderPostmaster && MyBackendType == B_LMON && AmLmonProcess()
		&& cluster_enabled && cluster_shared_config
		&& MyProc != NULL && MyProcPid > 0 && MyProc->pid == MyProcPid
		&& MyProc->pgprocno >= 0 && MyProc->backendId == InvalidBackendId
		&& MyProc->cluster_grd_generation != 0
		&& MyProc->cluster_grd_generation != UINT64_MAX;
}
static bool
candidate_live(const ClusterSurvivorControl *c)
{
	return c != NULL && c->header.seq != 0 && !(c->header.seq & 1)
		&& c->header.active_table == 0 && c->header.phase == CLUSTER_SURVIVOR_QUIESCED
		&& c->header.flags == 0 && c->tables[0].header.flags == 0
		&& c->tables[0].header.seq == c->header.seq;
}
static bool
candidate_writable(const ClusterSurvivorControl *c)
{
	return c->header.seq < UINT64_MAX - 2
		&& c->header.latch_generation < UINT64_MAX - 1;
}
static void
candidate_identity(ClusterSurvivorIdentity *id, const uint8 *p)
{
	id->node = candidate_read(p, 4); id->capability_word = candidate_read(p + 4, 4);
	id->boot = candidate_read(p + 8, 8); id->admitted = candidate_read(p + 16, 8);
	id->control_generation = candidate_read(p + 24, 8);
	id->capability_generation = candidate_read(p + 32, 8);
	id->observation_generation = candidate_read(p + 40, 8);
}
static void
candidate_basis(ClusterSurvivorBasis *b, const uint8 *p)
{
	memcpy((uint8 *)b, p, 1136);
	b->event = candidate_read(p + 1136, 8); b->formation = candidate_read(p + 1144, 8);
	b->native_freeze = candidate_read(p + 1152, 8);
	b->open_generation = candidate_read(p + 1160, 8); b->nonce = candidate_read(p + 1168, 8);
	for (unsigned i = 0; i < 2; i++) {
		b->survivors[i] = candidate_read(p + 1176 + 8 * i, 8);
		b->failures[i] = candidate_read(p + 1192 + 8 * i, 8);
	}
	memcpy((uint8 *)b + 1208, p + 1208, 328);
}
static void
candidate_basis_hash(const ClusterSurvivorBasis *b, uint8 digest[32])
{
	pg_sha256_ctx hash;
	uint8 scalars[72];
	candidate_write(scalars, b->event); candidate_write(scalars + 8, b->formation);
	candidate_write(scalars + 16, b->native_freeze);
	candidate_write(scalars + 24, b->open_generation); candidate_write(scalars + 32, b->nonce);
	for (unsigned i = 0; i < 2; i++) {
		candidate_write(scalars + 40 + i * 8, b->survivors[i]);
		candidate_write(scalars + 56 + i * 8, b->failures[i]);
	}
	pg_sha256_init(&hash); pg_sha256_update(&hash, (const uint8 *)b, 1136);
	pg_sha256_update(&hash, scalars, sizeof(scalars));
	pg_sha256_update(&hash, (const uint8 *)b + 1208, 328); pg_sha256_final(&hash, digest);
}
static bool
candidate_basis_matches(const uint8 *p, const uint8 digest[32])
{
	pg_sha256_ctx hash;
	uint8 actual[32];
	pg_sha256_init(&hash); pg_sha256_update(&hash, p, 1536);
	pg_sha256_final(&hash, actual);
	return memcmp(actual, digest, 32) == 0;
}
static void
candidate_change_begin(ClusterSurvivorControl *c)
{
	c->header.seq++; c->tables[0].header.seq++;
	pg_write_barrier();
}
static void
candidate_change_end(ClusterSurvivorControl *c)
{
	c->header.latch_generation++;
	pg_write_barrier();
	c->tables[0].header.seq++; c->header.seq++;
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_candidate_begin(ClusterSurvivorControl *c,
	const uint8 *bytes, Size length, const uint8 format[32], uint32 self, const char **reason)
{
	ClusterSurvivorMessageV4 m;
	ClusterSurvivorTableHeader *t;
	const uint8 *row;

	if (!candidate_owner() || c == NULL)
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_owner_invalid");
	if (!cluster_semantic_survivor_v4_decode(bytes, length, format, &m)
		|| m.purpose != CLUSTER_SURVIVOR_SERVICE || m.kind != CLUSTER_SURVIVOR_REQUEST
		|| m.stage != CLUSTER_SURVIVOR_QUIESCED || self >= 128
		|| !(m.survivors[self / 64] & (UINT64_C(1) << (self % 64)))
		|| !candidate_basis_matches(m.body, m.basis))
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_manifest_invalid");
	/* A failed/new episode cannot erase original callbacks, jobs or rows. */
	if (!candidate_zero(c, sizeof(*c)))
		return candidate_result(CANDIDATE_PENDING, reason, "candidate_previous_object_retained");
	c->header.seq = 1;
	candidate_basis(&c->basis, m.body);
	c->header.nonce = m.nonce; c->header.epoch = m.epoch;
	c->header.formation = m.formation; c->header.native_freeze = m.native_freeze;
	c->header.round_generation = m.round_generation;
	c->header.latch_generation = 1; c->header.phase = m.stage;
	c->header.coordinator = m.coordinator; c->header.self = self;
	memcpy(c->header.root, m.body + 1536, 80);
	memcpy(c->header.candidate_proof, m.proof, 32);
	t = &c->tables[0].header;
	t->seq = 1; t->stage = m.stage; t->coordinator = m.coordinator;
	t->nonce = m.nonce; t->epoch = m.epoch; t->round_generation = m.round_generation;
	t->source_features = t->target_features = t->rollback_features = m.features;
	t->survivors[0] = m.survivors[0]; t->survivors[1] = m.survivors[1];
	row = m.body + 1536 + 80;
	for (unsigned node = 0; node < 128; node++) {
		if (!(m.survivors[node / 64] & (UINT64_C(1) << (node % 64)))) continue;
		candidate_identity(&c->tables[0].expected[node], row); row += 48;
	}
	/* No EXPECTED_VALID/observed/COMPLETE bit, capability sample or permission. */
	pg_write_barrier(); t->seq = c->header.seq = 2;
	return candidate_result(CANDIDATE_PENDING, reason, "candidate_unconfirmed");
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_candidate_stage(ClusterSurvivorControl *c,
	const uint8 *bytes, Size length, const uint8 format[32], uint64 transport_generation,
	const char **reason)
{
	ClusterSurvivorMessageV4 m;
	ClusterSurvivorObserved row;
	ClusterSurvivorIngress ingress;
	ClusterSurvivorTable *t;
	uint8 basis[32];
	uint64 bit;

	if (!candidate_owner() || c == NULL)
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_owner_invalid");
	if (!cluster_semantic_survivor_v4_decode(bytes, length, format, &m)
		|| m.purpose != CLUSTER_SURVIVOR_SERVICE || m.kind != CLUSTER_SURVIVOR_ACK
		|| m.stage != CLUSTER_SURVIVOR_QUIESCED)
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_row_invalid");
	if (!candidate_live(c))
		return candidate_result(CANDIDATE_STALE, reason, "candidate_not_live");
	t = &c->tables[0]; candidate_basis_hash(&c->basis, basis);
	if (m.nonce != c->header.nonce || m.epoch != c->header.epoch
		|| m.formation != c->header.formation || m.native_freeze != c->header.native_freeze
		|| m.round_generation != c->header.round_generation || m.coordinator != c->header.coordinator
		|| m.features != t->header.target_features
		|| memcmp(m.survivors, t->header.survivors, 16) != 0
		|| memcmp(m.format, c->basis.format, 32) != 0 || memcmp(m.basis, basis, 32) != 0
		|| memcmp(m.proof, c->header.candidate_proof, 32) != 0)
		return candidate_result(CANDIDATE_STALE, reason, "candidate_scope_changed");
	candidate_identity(&row.identity, m.body); memcpy(row.receipt, m.body + 48, 32);
	if (memcmp(&row.identity, &t->expected[m.sender], sizeof(row.identity)) != 0
		|| transport_generation == 0 || transport_generation == UINT64_MAX
		|| transport_generation != row.identity.control_generation)
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_identity_changed");
	memset(&ingress, 0, sizeof(ingress));
	ingress.node = m.sender; ingress.kind = m.kind; ingress.length = length;
	ingress.transport_generation = transport_generation; ingress.round_generation = m.round_generation;
	bit = UINT64_C(1) << (m.sender % 64);
	if (c->header.staged_chunk[m.sender / 64] & bit) {
		if (memcmp(&t->observed[m.sender], &row, sizeof(row)) != 0
			|| memcmp(&c->ingress[m.sender], &ingress, sizeof(ingress)) != 0)
			return candidate_result(CANDIDATE_REFUSED, reason, "candidate_duplicate_conflict");
		return candidate_result(CANDIDATE_PENDING, reason, "candidate_duplicate_unconfirmed");
	}
	if (!candidate_writable(c))
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_generation_exhausted");
	candidate_change_begin(c);
	t->observed[m.sender] = row; c->ingress[m.sender] = ingress;
	c->header.staged_chunk[m.sender / 64] |= bit;
	candidate_change_end(c);
	return candidate_result(CANDIDATE_PENDING, reason, "candidate_row_unconfirmed");
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_candidate_invalidate(ClusterSurvivorControl *c, const char **reason)
{
	if (!candidate_owner() || c == NULL)
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_owner_invalid");
	if (c->header.flags == CANDIDATE_INVALID && c->header.seq != 0 && !(c->header.seq & 1))
		return candidate_result(CANDIDATE_STALE, reason, "candidate_invalidated");
	if (!candidate_live(c) || !candidate_writable(c))
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_invalidation_invalid");
	candidate_change_begin(c);
	c->header.flags = CANDIDATE_INVALID;
	c->header.staged_chunk[0] = c->header.staged_chunk[1] = 0;
	candidate_change_end(c);
	return candidate_result(CANDIDATE_STALE, reason, "candidate_invalidated");
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_candidate_probe(ClusterSurvivorControl *c,
	ClusterServiceProducerCollection *out, const char **reason)
{
	ClusterSemanticAdmissionObservationResult result;
	if (!candidate_owner() || out == NULL || c == NULL)
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_owner_invalid");
	if (!candidate_live(c))
		return candidate_result(CANDIDATE_STALE, reason, "candidate_not_live");
	result = cluster_service_producer_collect(c->header.epoch, c->basis.open_generation, out);
	if (result == CANDIDATE_STALE) {
		(void)cluster_semantic_survivor_candidate_invalidate(c, NULL);
		return candidate_result(result, reason, "candidate_native_changed");
	}
	if (result != CANDIDATE_PENDING
		&& result != CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT)
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_native_refused");
	/* No receipt is manufactured from an idle point, an empty bitmap or hash. */
	return candidate_result(CANDIDATE_PENDING, reason,
		result == CANDIDATE_PENDING ? "candidate_native_pending" : "candidate_producer_closure_unproved");
}
