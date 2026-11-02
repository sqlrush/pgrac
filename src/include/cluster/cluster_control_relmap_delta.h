/* Pure observations of the original relmap creation confirmation.
 * This header intentionally depends on no recovery/control owner layout. */
#ifndef CLUSTER_CONTROL_RELMAP_DELTA_H
#define CLUSTER_CONTROL_RELMAP_DELTA_H

#include "c.h"

struct ClusterControlRelmapRecoveryCreateV2;
struct ClusterColdTypedV1;
struct ClusterRecoveryFencePlan;
struct ClusterControlRootReadToken;

/* Publishing-phase comparison only; this is never permission to install a
 * token in plan/IR/pin or to resume. The original confirm builder is the sole
 * producer: the complete before token for EACH present origin, typed/fence/id
 * and live original carrier must match. Its candidate alone cannot expose
 * after: only the complete primary-new/backup-old readback advances the phase.
 * During that writer's dynamic extent, check the original actor/held CF-X and
 * private phase only; never call current/H or perform ROOT I/O here. After
 * success this still returns a comparison fact, not the final gate below.
 * No caller-supplied new token, bool/range policy, publication or rebase.
 * False preserves output. */
extern bool cluster_control_relmap_recovery_create_root_delta_v2(
	const struct ClusterControlRelmapRecoveryCreateV2 *, const struct ClusterColdTypedV1 *,
	const struct ClusterRecoveryFencePlan *, uint64 episode_id,
	const struct ClusterControlRootReadToken *before, struct ClusterControlRootReadToken *effective);
/* Pure final-state observation for H's exact-delta install gate. True only
 * after the original confirm and native-table publication succeeded; never
 * during their active phase. Fixed original carrier/typed/fence/id only.
 * No current/H callback, ROOT I/O, token install or resume permission. */
extern bool cluster_control_relmap_recovery_create_root_delta_confirmed_v2(
	const struct ClusterControlRelmapRecoveryCreateV2 *, const struct ClusterColdTypedV1 *,
	const struct ClusterRecoveryFencePlan *, uint64 episode_id);
/* Final-token facts only after confirm AND table publication have completed,
 * with complete set and active clear. This is a separate entry, not a caller
 * mode argument to the publishing getter. Same pure storage/actor/binding
 * checks, without any current/H callback or ROOT I/O. The outer original owner
 * must still check the complete sources/cut and same episode before exact
 * plan/IR/pin custody; this getter neither installs them nor resumes. */
extern bool cluster_control_relmap_recovery_create_confirmed_root_delta_v2(
	const struct ClusterControlRelmapRecoveryCreateV2 *, const struct ClusterColdTypedV1 *,
	const struct ClusterRecoveryFencePlan *, uint64 episode_id,
	const struct ClusterControlRootReadToken *before, struct ClusterControlRootReadToken *effective);
#endif /* CLUSTER_CONTROL_RELMAP_DELTA_H */
