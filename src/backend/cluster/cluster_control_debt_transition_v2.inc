/*-------------------------------------------------------------------------
 * cluster_control_debt_transition_v2.inc
 *     Private transition snapshots in the original control owner.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/backend/cluster/cluster_control_debt_transition_v2.inc
 * NOTES
 *     No production producer until original Start delta consumers are wired.
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_control_debt_transition.h"
#include "utils/resowner.h"
#include "cluster/cluster_recovery_plan.h"

struct ClusterControlDebtTransitionV2 {
	const struct ClusterControlDebtTransitionV2 *self;
	ResourceOwner owner;
	int pid;
	bool publish_attempted;
	ClusterControlDebtTransitionViewV2 view;
	uint8 *bytes;
	Size byte_count;
	ClusterControlDebtBasisViewV2 *batches;
};

static inline bool
control_debt_transition_distinct_v2(const ClusterControlDebtTransitionV2 *t,
	const void *out, Size n)
{
	return out && !debt_overlap(t, sizeof(*t), out, n)
		&& !debt_overlap(t->bytes, t->byte_count, out, n)
		&& !debt_overlap(t->batches, t->view.batch_count * sizeof(*t->batches), out, n);
}

/* No I/O, no authority upgrade and no caller-provided success state. The view
 * is only data for the original owner's full admission/readback conjunction. */
bool
cluster_control_root_debt_transition_get_v2(const ClusterControlDebtTransitionV2 *t,
	ClusterControlDebtTransitionViewV2 *out)
{
	if (!t || t->self != t || t->owner != CurrentResourceOwner || !t->owner
		|| t->pid != MyProcPid || t->pid <= 0 || t->pid != (int)getpid()
		|| !cluster_cf_held_is_clusterwide(ExclusiveLock)
		|| t->view.cf_sequence != cluster_cf_attempt_sequence()
		|| !control_debt_transition_distinct_v2(t, out, sizeof(*out))
		|| !cluster_recovery_plan_output_distinct_v1(t->view.start, out, sizeof(*out))
		|| !cluster_control_root_relmap_selection_output_distinct_v4(
			t->view.selection, out, sizeof(*out))) return false;
	*out = t->view;
	return true;
}

static inline void
control_debt_transition_destroy_v2(ClusterControlDebtTransitionV2 **out)
{
	ClusterControlDebtTransitionV2 *t;
	if (!out || !(t = *out) || t->self != t
		|| !control_debt_transition_distinct_v2(t, out, sizeof(*out))) return;
	*out = NULL;
	t->self = NULL;
	free(t->batches); free(t->bytes); free(t);
}

/* Construct unpublished, complete immutable snapshots, not a durable receipt.
 * This helper is private to the original owner and has no production caller.
 * The future owner producer must keep it private until its original Start
 * credentials and full physical publication/readback have both succeeded;
 * failure retains original pins and any newly written debt. No success setter
 * or caller-supplied ROOT after-image is accepted here. */
static inline ClusterControlDebtTransitionV2 *
control_debt_transition_build_v2(const struct ClusterRecoveryStartV1 *start,
	const ClusterControlRelmapSelectionV2 *selection, const uint8 *before, Size length,
	const ControlDebtWireImageV1 *w, const ControlDebtWireBatchV1 *basis, uint64 nb,
	Size budget)
{
	ClusterControlDebtTransitionV2 *t = NULL;
	ClusterControlDebtTransitionViewV2 *v;
	ResourceOwner owner = CurrentResourceOwner;
	uint64 sequence = cluster_cf_attempt_sequence();
	int pid = MyProcPid;
	Size reserve = 0, bytes = 0;
	uint8 *p;
	if (!start || !selection || !before || length != 133120 || !w || !basis || !nb
		|| !owner || pid <= 0 || pid != (int)getpid() || !debt_counter(sequence)
		|| !cluster_cf_held_is_clusterwide(ExclusiveLock)
		|| nb > SIZE_MAX / sizeof(*t->batches) || w->batch_count != nb
		|| !debt_add(&reserve, 1, sizeof(*t), budget)
		|| !debt_add(&reserve, nb, sizeof(*t->batches), budget)
		|| !debt_add(&bytes, 2, length, budget)
		|| !debt_add(&bytes, w->registry_bytes, 1, budget)
		|| !debt_add(&bytes, w->page_bytes, 1, budget)) return NULL;
	for (uint64 i = 0; i < nb; ++i)
		if (!debt_add(&bytes, basis[i].basis_bytes, 1, budget)) return NULL;
	if (!debt_add(&reserve, 1, bytes, budget)) return NULL;
	t = calloc(1, sizeof(*t));
	if (!t) return NULL;
	t->self = t;
	t->bytes = malloc(bytes);
	t->batches = calloc((Size)nb, sizeof(*t->batches));
	if (!t->bytes || !t->batches) goto fail;
	t->byte_count = bytes;
	v = &t->view;
	p = t->bytes;
	memcpy(p, before, length); v->before_root = p; p += length;
	if (!control_debt_root_candidate_v4(before, length, w, basis, nb,
		budget - reserve, p, length)) goto fail;
	v->after_root = p; p += length;
	memcpy(p, w->registry, w->registry_bytes);
	v->registry = p; p += w->registry_bytes;
	memcpy(p, w->pages, w->page_bytes); v->pages = p; p += w->page_bytes;
	for (uint64 i = 0; i < nb; ++i) {
		ClusterControlDebtBasisViewV2 *b = &t->batches[i];
		b->episode = basis[i].episode; b->prepare = basis[i].prepare;
		memcpy(p, basis[i].basis, basis[i].basis_bytes);
		b->basis = p; b->basis_bytes = basis[i].basis_bytes; p += basis[i].basis_bytes;
		memcpy(b->basis_reference, basis[i].basis_ref, 128);
	}
	v->start = start; v->selection = selection; v->cf_sequence = sequence;
	v->root_pair_bytes = length;
	v->basis = v->registry + 256; v->basis_bytes = debt_get(v->registry, 104, 8);
	memcpy(v->basis_reference, before + 512, 128);
	v->batches = t->batches; v->batch_count = nb;
	v->registry_bytes = w->registry_bytes; v->page_bytes = w->page_bytes;
	memcpy(v->prepared_reference, w->reference, 128);
	t->owner = owner; t->pid = pid;
	if (owner == CurrentResourceOwner && pid == MyProcPid
		&& cluster_cf_held_is_clusterwide(ExclusiveLock)
		&& sequence == cluster_cf_attempt_sequence()) return t;
fail:
	control_debt_transition_destroy_v2(&t);
	return NULL;
}
