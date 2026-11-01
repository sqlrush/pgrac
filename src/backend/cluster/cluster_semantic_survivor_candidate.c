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
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_semantic_survivor_candidate.h"
#include "cluster/cluster_semantic_survivor_producer.h"
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
		&& MyBackendId == InvalidBackendId
		&& MyProc->cluster_grd_generation != 0
		&& MyProc->cluster_grd_generation != UINT64_MAX;
}
/* Stack-only scope fences, not an admission or producer-closure proof. */
typedef struct CandidateLocalView {
	PGPROC *proc;
	int pid;
	int pgprocno;
	uint64 proc_generation;
	ClusterSemanticWaitGraphSnapshot gate;
} CandidateLocalView;
StaticAssertDecl(sizeof(CandidateLocalView) <= 128, "candidate scope stack bound");

static bool
candidate_open_scope(const uint8 *pgsa, uint64 generation, uint64 native_epoch,
	uint64 source_features, uint64 target_features, ClusterSurvivorProducerScope *scope)
{
	ClusterSemanticActivationRecord open;

	if (!cluster_semantic_activation_record_decode(pgsa, &open, NULL)
		|| open.phase != CLUSTER_SEMANTIC_PHASE_OPEN
		|| open.record_generation == 0 || open.record_generation == UINT64_MAX
		|| open.record_generation != generation
		|| open.transition_epoch == 0 || open.transition_epoch == UINT64_MAX
		|| open.transition_epoch > native_epoch
		|| open.target_feature_bitmap != target_features
		|| open.target_feature_bitmap != source_features)
		return false;
	scope->native_epoch = native_epoch; scope->gate_epoch = open.transition_epoch;
	scope->record_generation = generation;
	return true;
}
static bool
candidate_same_owner(const CandidateLocalView *view)
{
	return candidate_owner() && MyProc == view->proc && MyProcPid == view->pid
		&& MyProc->pgprocno == view->pgprocno
		&& MyProc->cluster_grd_generation == view->proc_generation;
}
static ClusterSemanticAdmissionObservationResult
candidate_local_view(ClusterSurvivorProducerScope scope, CandidateLocalView *view)
{
	uint64 epoch_before, epoch_after;
	bool known;

	memset(view, 0, sizeof(*view));
	if (!candidate_owner()) return CANDIDATE_REFUSED;
	view->proc = MyProc; view->pid = MyProcPid; view->pgprocno = MyProc->pgprocno;
	view->proc_generation = MyProc->cluster_grd_generation;
	epoch_before = cluster_epoch_get_current();
	known = cluster_semantic_activation_wait_graph_snapshot(&view->gate);
	epoch_after = cluster_epoch_get_current();
	if (!candidate_same_owner(view)) return CANDIDATE_REFUSED;
	if (epoch_before != scope.native_epoch || epoch_after != scope.native_epoch)
		return CANDIDATE_STALE;
	if (!known || !view->gate.known || view->gate.seq == 0 || (view->gate.seq & 1))
		return CANDIDATE_PENDING;
	if (view->gate.formation_epoch != scope.gate_epoch
		|| view->gate.record_generation != scope.record_generation)
		return CANDIDATE_STALE;
	/* CURRENT is internal metadata only; every public candidate stays unconfirmed. */
	return CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT;
}
static ClusterSemanticAdmissionObservationResult
candidate_local_recheck(ClusterSurvivorProducerScope scope, const CandidateLocalView *before)
{
	CandidateLocalView after;
	ClusterSemanticAdmissionObservationResult result = candidate_local_view(scope, &after);

	if (!candidate_same_owner(before)) return CANDIDATE_REFUSED;
	if (result != CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT) return result;
	if (before->gate.seq != after.gate.seq || before->gate.active_bits != after.gate.active_bits
		|| before->gate.transition_closed != after.gate.transition_closed)
		return CANDIDATE_PENDING;
	return CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT;
}
static void
candidate_mark_invalid(ClusterSurvivorControl *c)
{
	/* Called inside the existing write bracket; original responsibility is retained. */
	c->header.flags = CANDIDATE_INVALID;
	c->header.staged_chunk[0] = c->header.staged_chunk[1] = 0;
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
	ClusterSurvivorProducerScope scope;
	CandidateLocalView view;
	ClusterSemanticAdmissionObservationResult result;
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
	if (!candidate_open_scope(m.body, candidate_read(m.body + 1160, 8), m.epoch,
		m.features, m.features, &scope))
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_open_baseline_invalid");
	result = candidate_local_view(scope, &view);
	if (result != CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT)
		return candidate_result(result, reason, "candidate_local_scope_unconfirmed");
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
	result = candidate_local_recheck(scope, &view);
	if (result == CANDIDATE_STALE || result == CANDIDATE_REFUSED) candidate_mark_invalid(c);
	pg_write_barrier(); t->seq = c->header.seq = 2;
	return candidate_result(result == CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT
		? CANDIDATE_PENDING : result, reason, "candidate_unconfirmed");
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
	ClusterSurvivorProducerScope scope;
	CandidateLocalView view;
	ClusterSemanticAdmissionObservationResult result;
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
	if (!candidate_open_scope(c->basis.pgsa, c->basis.open_generation, c->header.epoch,
		t->header.source_features, t->header.target_features, &scope))
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_open_baseline_invalid");
	result = candidate_local_view(scope, &view);
	if (result != CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT) {
		if (result == CANDIDATE_STALE) {
			/* Only a proved local drift invalidates; a foreign frame never can. */
			if (cluster_semantic_survivor_candidate_invalidate(c, NULL) != CANDIDATE_STALE)
				result = CANDIDATE_REFUSED;
		}
		return candidate_result(result, reason, "candidate_local_scope_unconfirmed");
	}
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
	result = candidate_local_recheck(scope, &view);
	if (result == CANDIDATE_STALE || result == CANDIDATE_REFUSED) candidate_mark_invalid(c);
	candidate_change_end(c);
	/* Unknown publication after a write retains raw syntax, never confirmed state. */
	return candidate_result(result == CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT
		? CANDIDATE_PENDING : result, reason, "candidate_row_unconfirmed");
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
	candidate_mark_invalid(c);
	candidate_change_end(c);
	return candidate_result(CANDIDATE_STALE, reason, "candidate_invalidated");
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_candidate_probe(ClusterSurvivorControl *c,
	ClusterServiceProducerCollection *out, const char **reason)
{
	ClusterSemanticAdmissionObservationResult result;
	ClusterSurvivorProducerObservation observation;
	ClusterSurvivorProducerScope scope;
	if (out != NULL) memset(out, 0, sizeof(*out));
	if (!candidate_owner() || out == NULL || c == NULL)
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_owner_invalid");
	if (!candidate_live(c))
		return candidate_result(CANDIDATE_STALE, reason, "candidate_not_live");
	/* Original PGSA decode binds scope, never durable/current proof. */
	if (!candidate_open_scope(c->basis.pgsa, c->basis.open_generation, c->header.epoch,
		c->tables[0].header.source_features, c->tables[0].header.target_features, &scope))
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_open_baseline_invalid");
	result = cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_NATIVE,
		scope, &observation);
	if (result == CANDIDATE_PENDING)
		*out = observation.sample.native;
	if (result == CANDIDATE_STALE) {
		(void)cluster_semantic_survivor_candidate_invalidate(c, NULL);
		return candidate_result(result, reason, "candidate_native_changed");
	}
	if (result != CANDIDATE_PENDING)
		return candidate_result(CANDIDATE_REFUSED, reason, "candidate_native_refused");
	/* No receipt is manufactured from an idle point, an empty bitmap or hash. */
	return candidate_result(CANDIDATE_PENDING, reason,
		out->reason != NULL
			&& strcmp(out->reason, "NATIVE_COLLECTION_PRODUCER_CLOSURE_UNPROVED") == 0
			? "candidate_producer_closure_unproved" : "candidate_native_pending");
}
