/*-------------------------------------------------------------------------
 *
 * cluster_undo_block0_current.c
 *	  Cooperative Candidate-2 current guard for undo block zero.
 *
 * This adapter owns no durable block0 bytes.  It holds the generation-
 * independent 0xFB current resource and delegates resident-only sampling and
 * copying to cluster_undo_block0_resident.c.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_undo_resid.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#define CLUSTER_UNDO_BLOCK0_WALR_RESID_TYPE UINT8_C(0xFA)
#define CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_INITIAL_MS 100
#define CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_MAX_MS 1600
#define CLUSTER_UNDO_BLOCK0_CURRENT_TOMBSTONE_MS 30000

StaticAssertDecl(CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "block0 current resid type must be outside the PG locktag domain");
StaticAssertDecl(CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_UNDO_RESID_TYPE,
				 "block0 current and old undo resid types must differ");
StaticAssertDecl(CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_UNDO_BLOCK0_WALR_RESID_TYPE,
				 "block0 current and WAL-retention resid types must differ");
StaticAssertDecl(CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_SQ_RESID_TYPE
					 && CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_CF_RESID_TYPE
					 && CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_HW_RESID_TYPE
					 && CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_DL_RESID_TYPE
					 && CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_TT_RESID_TYPE
					 && CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_IR_RESID_TYPE
					 && CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_KO_RESID_TYPE
					 && CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_OID_RESID_TYPE
					 && CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != CLUSTER_RELMAP_RESID_TYPE,
				 "block0 current resid type collides with a product namespace");
StaticAssertDecl(CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE != UINT8_C(0xF3),
				 "block0 current resid must avoid the known raw-layout/DL collision");

static bool
current_resid_namespace_valid(uint8 type)
{
	return type == UINT8_C(0xFB) && type > LOCKTAG_LAST_TYPE && type != CLUSTER_SQ_RESID_TYPE
		   && type != CLUSTER_CF_RESID_TYPE && type != CLUSTER_HW_RESID_TYPE
		   && type != CLUSTER_DL_RESID_TYPE && type != CLUSTER_TT_RESID_TYPE
		   && type != CLUSTER_IR_RESID_TYPE && type != CLUSTER_KO_RESID_TYPE
		   && type != CLUSTER_OID_RESID_TYPE && type != CLUSTER_RELMAP_RESID_TYPE
		   && type != CLUSTER_UNDO_RESID_TYPE && type != CLUSTER_UNDO_BLOCK0_WALR_RESID_TYPE
		   && type != UINT8_C(0xF3);
}

void
cluster_undo_block0_current_init(void)
{
	if (!current_resid_namespace_valid(CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE))
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("block0 current resource type namespace collision"),
						errdetail("The frozen resource type 0xFB must remain distinct from "
								  "PostgreSQL lock tags, product types 0xF0..0xF9, "
								  "WAL retention type 0xFA, and the known private "
								  "raw-layout/deadlock type 0xF3.")));
}

typedef enum ClusterUndoBlock0CurrentPhase {
	CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED = 0,
	CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT = 1,
	CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD = 2,
	CLUSTER_UNDO_BLOCK0_CURRENT_RELEASE_WAIT = 3,
	CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP = 4,
	CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR = 5
} ClusterUndoBlock0CurrentPhase;

typedef struct ClusterUndoBlock0CurrentGuardData {
	/* Must remain first: exit cleanup owns a list of active guards. */
	dlist_node active_node;
	ClusterResId resid;
	ClusterGrdHolderId holder;
	ClusterUndoBlock0LogicalKey logical;
	ClusterSemanticAdmissionToken admission;
	TimestampTz deadline;
	TimestampTz next_retry_at;
	uint64 reservation_generation;
	uint64 routing_generation;
	int32 master_node;
	int32 timeout_ms;
	uint16 retry_attempt;
	uint8 phase;
	uint8 mode;
	bool active_linked;
	bool reply_installed;
	bool remote_master;
	bool reservation_held;
	bool request_dispatched;
	bool grant_observed;
	uint8 reserved[15];
	uint8 reply_wait_site_plus_one;
	bool reply_wait_adapter_required;
	bool reply_wait_repoll_pending;
} ClusterUndoBlock0CurrentGuardData;

typedef struct ClusterUndoBlock0CurrentPinCleanup {
	ClusterUndoBlock0CurrentGuard *guard;
	ClusterUndoBlock0Pin *pin;
	bool local_pin_held;
} ClusterUndoBlock0CurrentPinCleanup;

typedef struct ClusterUndoBlock0LiveOwnerCleanup {
	ClusterSemanticAdmissionToken *admission;
	ClusterUndoBlock0CurrentGuard *guard;
	ClusterUndoBlock0FrameToken *frame;
	ClusterUndoBlock0Pin *pin;
	bool admission_held;
	bool current_active;
	bool pin_held;
	bool provision_held;
} ClusterUndoBlock0LiveOwnerCleanup;

#define CLUSTER_UNDO_BLOCK0_LIVE_OWNER_PUBLICATION_MAGIC UINT32_C(0x42505231)

typedef struct ClusterUndoBlock0LiveOwnerPublicationData {
	uint32 magic;
	uint32 reserved;
	ClusterUndoBlock0ResidentCensusItem item;
	ClusterSemanticAdmissionToken admission;
} ClusterUndoBlock0LiveOwnerPublicationData;

StaticAssertDecl(sizeof(ClusterUndoBlock0CurrentGuardData) == 168,
				 "private block0 current guard must fill its exact 168-byte ABI");
StaticAssertDecl(sizeof(ClusterUndoBlock0LiveOwnerPublicationData)
					 <= sizeof(ClusterUndoBlock0LiveOwnerPublication),
				 "private live-owner receipt must fit its process-local ABI");
StaticAssertDecl(offsetof(ClusterUndoBlock0CurrentGuardData, active_node) == 0,
				 "active dlist node must be the guard prefix");
StaticAssertDecl(CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED == 0
					 && CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT == 1
					 && CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD == 2
					 && CLUSTER_UNDO_BLOCK0_CURRENT_RELEASE_WAIT == 3
					 && CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP == 4
					 && CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR == 5,
				 "block0 current phase values are frozen");

static dlist_head current_active_guards = DLIST_STATIC_INIT(current_active_guards);

bool
cluster_undo_block0_current_backend_has_guards(void)
{
	return !dlist_is_empty(&current_active_guards);
}
static bool current_exit_hook_registered = false;

#define CURRENT_ADMISSION_BORROWED_INDEX 0
#define CURRENT_ADMISSION_OWNED UINT8_C(0)
#define CURRENT_ADMISSION_CENSUS UINT8_C(1)
#define CURRENT_ADMISSION_LIVE_OWNER_SOURCE UINT8_C(2)
#define CURRENT_ADMISSION_LIVE_OWNER_TARGET UINT8_C(3)
#define CURRENT_ADMISSION_CTRC_RELEASE UINT8_C(4)
#define CURRENT_RETRY_REPORTED_INDEX 1

static void current_backend_exit(int code, Datum arg);
static void current_error_cleanup(int code, Datum arg);
static void current_pin_error_cleanup(int code, Datum arg);
static void current_live_owner_error_cleanup(int code, Datum arg);

static inline ClusterUndoBlock0CurrentGuardData *
current_guard_data(ClusterUndoBlock0CurrentGuard *guard)
{
	return (ClusterUndoBlock0CurrentGuardData *)(void *)guard;
}

static inline bool
current_phase_valid(uint8 phase)
{
	return phase <= CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR;
}

static inline bool
current_admission_borrowed(const ClusterUndoBlock0CurrentGuardData *data)
{
	return data != NULL
		   && data->reserved[CURRENT_ADMISSION_BORROWED_INDEX] != CURRENT_ADMISSION_OWNED;
}

static inline bool
current_admission_recheck(const ClusterUndoBlock0CurrentGuardData *data)
{
	return data != NULL
		   && ((data->reserved[CURRENT_ADMISSION_BORROWED_INDEX] == CURRENT_ADMISSION_CENSUS
				|| data->reserved[CURRENT_ADMISSION_BORROWED_INDEX]
					   == CURRENT_ADMISSION_CTRC_RELEASE)
				   ? cluster_semantic_activation_recheck_r4_terminal_census(&data->admission)
				   : cluster_semantic_activation_recheck(&data->admission));
}

static inline void
current_set_failure(ClusterUndoBlock0Result *failure, ClusterUndoBlock0Result result)
{
	if (failure != NULL)
		*failure = result;
}

static ClusterUndoBlock0CurrentStep
current_unused_fail_detail(ClusterUndoBlock0CurrentGuardData *data, ClusterUndoBlock0Result result,
						   ClusterUndoBlock0Result *failure, unsigned int predicate,
						   unsigned int diagnostic_bit, const char *gate)
{
	static uint32 reported_predicates;

	/* One diagnostic per actual rejection predicate per process.  Preserve
	 * the failing branch before its zeroing cleanup; never log successes. */
	if (diagnostic_bit < 32 && (reported_predicates & (UINT32_C(1) << diagnostic_bit)) == 0) {
		reported_predicates |= UINT32_C(1) << diagnostic_bit;
		ereport(
			LOG,
			(errmsg_internal(
				"block0 current acquire admission refused: predicate=%u result=%u node=%d gate=%s",
				predicate, (unsigned int)result, cluster_node_id, gate)));
	}
	if (data != NULL)
		memset(data, 0, sizeof(*data));
	current_set_failure(failure, result);
	return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

static ClusterUndoBlock0CurrentStep
current_unused_fail(ClusterUndoBlock0CurrentGuardData *data, ClusterUndoBlock0Result result,
					ClusterUndoBlock0Result *failure, unsigned int predicate)
{
	return current_unused_fail_detail(data, result, failure, predicate, predicate,
									  "NOT_APPLICABLE");
}

/* Preserve the original short-circuit order. These are diagnostic labels,
 * never a second authority sample after the rejecting condition. */
static unsigned int
current_readiness_rejection(void)
{
	if (!cluster_enabled)
		return 1;
	if (cluster_node_id < 0)
		return 2;
	if (!cluster_lms_is_ready())
		return 3;
	if (cluster_lmon_status() != CLUSTER_LMON_READY)
		return 4;
	if (!cluster_qvotec_in_quorum())
		return 5;
	if (!cluster_membership_is_member(cluster_node_id))
		return 6;
	return 0;
}

static bool
current_resid_build(const ClusterUndoBlock0LogicalKey *logical, ClusterResId *resid)
{
	uint32 slot;

	if (resid == NULL || cluster_undo_block0_logical_slot(logical, &slot) != CLUSTER_UNDO_BLOCK0_OK)
		return false;

	memset(resid, 0, sizeof(*resid));
	resid->field1 = logical->segment_id;
	resid->field2 = 0;
	resid->field3 = 0;
	resid->field4 = logical->owner_instance;
	resid->type = CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE;
	resid->lockmethodid = DEFAULT_LOCKMETHOD;
	return true;
}

static bool
current_resid_equal(const ClusterResId *a, const ClusterResId *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}

static bool
current_resid_already_active(const ClusterResId *resid)
{
	dlist_iter iter;

	dlist_foreach(iter, &current_active_guards)
	{
		const ClusterUndoBlock0CurrentGuardData *other
			= dlist_container(ClusterUndoBlock0CurrentGuardData, active_node, iter.cur);

		if (other->active_linked && current_resid_equal(&other->resid, resid))
			return true;
	}
	return false;
}

static void
current_active_link(ClusterUndoBlock0CurrentGuardData *data)
{
	Assert(data != NULL && !data->active_linked);
	dlist_node_init(&data->active_node);
	dlist_push_tail(&current_active_guards, &data->active_node);
	data->active_linked = true;
}

/* Register outside every PG_ENSURE block so temporary callbacks remain LIFO. */
static void
current_ensure_exit_hook(void)
{
	if (!current_exit_hook_registered) {
		before_shmem_exit(current_backend_exit, (Datum)0);
		current_exit_hook_registered = true;
	}
}

void
cluster_undo_block0_current_ensure_exit_hooks(void)
{
	current_ensure_exit_hook();
	cluster_undo_smgr_ensure_exit_hook();
}

static void
current_active_unlink(ClusterUndoBlock0CurrentGuardData *data)
{
	if (data != NULL && data->active_linked) {
		dlist_delete_thoroughly(&data->active_node);
		data->active_linked = false;
	}
}

/* Startup alone owns this local guard lane while recovery can publish R4. */
bool
cluster_undo_block0_current_startup_fenced_begin(ClusterUndoBlock0CurrentGuard *guard)
{
	ClusterUndoBlock0CurrentGuardData *data;

	if (guard == NULL)
		return false;
	data = current_guard_data(guard);
	if (!current_phase_valid(data->phase) || data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED
		|| memcmp(guard->opaque, (const uint8[168]){ 0 }, sizeof(guard->opaque)) != 0
		|| !dlist_is_empty(&current_active_guards))
		return false;

	current_ensure_exit_hook();
	current_active_link(data);
	data->phase = CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR;
	return true;
}

bool
cluster_undo_block0_current_startup_fenced_end(ClusterUndoBlock0CurrentGuard *guard)
{
	ClusterUndoBlock0CurrentGuardData *data;

	if (guard == NULL)
		return false;
	data = current_guard_data(guard);
	if (!current_phase_valid(data->phase)
		|| data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR || !data->active_linked)
		return false;

	current_active_unlink(data);
	memset(data, 0, sizeof(*data));
	return true;
}

bool
cluster_undo_block0_current_startup_fenced_owned(void)
{
	dlist_iter iter;

	dlist_foreach(iter, &current_active_guards)
	{
		const ClusterUndoBlock0CurrentGuardData *data
			= dlist_container(ClusterUndoBlock0CurrentGuardData, active_node, iter.cur);

		if (data->active_linked && data->phase == CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR)
			return true;
	}
	return false;
}

static void
current_fill_reply_key(const ClusterUndoBlock0CurrentGuardData *data, uint32 opcode,
					   GesReplyWaitKey *key)
{
	memset(key, 0, sizeof(*key));
	key->request_id = data->holder.request_id;
	key->source_node_id = (int32)data->holder.node_id;
	key->dest_node_id = data->master_node;
	key->request_opcode = opcode;
	key->cluster_epoch = data->holder.cluster_epoch;
}

static void
current_fill_request(const ClusterUndoBlock0CurrentGuardData *data, uint32 opcode,
					 GesRequestPayload *request)
{
	memset(request, 0, sizeof(*request));
	request->opcode = opcode;
	request->lockmode = opcode == GES_REQ_OPCODE_RELEASE ? NoLock : data->mode;
	request->holder_node_id = data->holder.node_id;
	request->holder_procno = data->holder.procno;
	request->holder_cluster_epoch_lo = (uint32)(data->holder.cluster_epoch & UINT64_C(0xffffffff));
	request->holder_cluster_epoch_hi = (uint32)(data->holder.cluster_epoch >> 32);
	request->holder_request_id_lo = (uint32)(data->holder.request_id & UINT64_C(0xffffffff));
	request->holder_request_id_hi = (uint32)(data->holder.request_id >> 32);
	memcpy(request->resid, &data->resid, sizeof(data->resid));
	request->shard_master_generation_lo = (uint32)(data->routing_generation & UINT64_C(0xffffffff));
	request->shard_master_generation_hi = (uint32)(data->routing_generation >> 32);
	/* Candidate-2 worker0 waits are deliberately outside the PGPROC WFG. */
	request->waiter_xid = InvalidTransactionId;
	request->wait_seq = 0;
}

static bool
current_reply_install(ClusterUndoBlock0CurrentGuardData *data, uint32 opcode)
{
	GesReplyWaitKey key;

	current_fill_reply_key(data, opcode, &key);
	if (cluster_ges_reply_wait_insert(&key, data->deadline) == NULL)
		return false;
	data->reply_installed = true;
	return true;
}

static void
current_reply_delete(ClusterUndoBlock0CurrentGuardData *data, uint32 opcode)
{
	GesReplyWaitKey key;

	if (data == NULL || !data->reply_installed)
		return;
	current_fill_reply_key(data, opcode, &key);
	cluster_ges_reply_wait_delete(&key);
	data->reply_installed = false;
}

static bool
current_live_recheck(const ClusterUndoBlock0CurrentGuardData *data)
{
	uint64 routing_generation = 0;
	int32 master;

	if (data == NULL || !cluster_enabled || cluster_node_id < 0 || !cluster_lms_is_ready()
		|| cluster_lmon_status() != CLUSTER_LMON_READY || !cluster_qvotec_in_quorum()
		|| !cluster_membership_is_member(cluster_node_id) || !current_admission_recheck(data)
		|| cluster_epoch_get_current() != data->holder.cluster_epoch
		|| cluster_grd_shard_phase(cluster_grd_shard_for_resource(&data->resid))
			   != GRD_SHARD_NORMAL)
		return false;
	master = cluster_grd_lookup_master_gen(&data->resid, &routing_generation);
	return master == data->master_node && routing_generation == data->routing_generation;
}

static ClusterUndoBlock0Result
current_failure_from_grd(ClusterGrdEntryResult result)
{
	if (result == CLUSTER_GRD_ENTRY_FULL)
		return CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE;
	return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
}

static ClusterUndoBlock0Result
current_failure_from_reject(uint32 reason)
{
	switch (reason) {
	case GES_REJECT_REASON_WORK_QUEUE_FULL:
		return CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE;
	case GES_REJECT_REASON_EPOCH_MISMATCH:
	case GES_REJECT_REASON_SHARD_FROZEN:
	case GES_REJECT_REASON_FEATURE_NOT_SUPPORTED:
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	default:
		return CLUSTER_UNDO_BLOCK0_IO_ERROR;
	}
}

static void
current_stage_remote_release(ClusterUndoBlock0CurrentGuardData *data)
{
	GesRequestPayload release;

	current_fill_request(data, GES_REQ_OPCODE_RELEASE, &release);
	cluster_grd_outbound_enqueue_cleanup_release((uint32)data->master_node, &release,
												 sizeof(release));
}

static void
current_stage_pending_cleanup(ClusterUndoBlock0CurrentGuardData *data, bool exiting)
{
	if (data->remote_master) {
		GesReplyWaitKey key;
		TimestampTz tombstone;
		bool raced_grant = false;

		if (data->reply_installed && data->request_dispatched) {
			current_fill_reply_key(data, GES_REQ_OPCODE_REQUEST, &key);
			tombstone = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													CLUSTER_UNDO_BLOCK0_CURRENT_TOMBSTONE_MS);
			raced_grant = cluster_ges_reply_wait_mark_abandoned(&key, tombstone);
			data->reply_installed = false; /* tombstone is independently owned */
		} else if (data->reply_installed)
			current_reply_delete(data, GES_REQ_OPCODE_REQUEST);
		if (data->request_dispatched)
			cluster_ges_send_cancel_wait(data->master_node, &data->resid, &data->holder, 0, 0,
										 GES_CANCEL_WAIT_KIND_REQUEST);
		if (raced_grant || data->grant_observed || (exiting && data->request_dispatched))
			current_stage_remote_release(data);
	} else {
		current_reply_delete(data, GES_REQ_OPCODE_REQUEST);
		if (data->request_dispatched) {
			(void)cluster_grd_cancel_waiter_by_id_seq(&data->resid, &data->holder, 0);
			cluster_ges_release_and_drain_local(&data->resid, &data->holder);
		}
	}
	if (data->reservation_held) {
		(void)cluster_grd_cancel_reservation_by_id(&data->resid, &data->holder);
		data->reservation_held = false;
	}
}

static void
current_stage_no_wait_cleanup(ClusterUndoBlock0CurrentGuardData *data, bool exiting)
{
	bool admission_entered;

	if (data == NULL || !current_phase_valid(data->phase))
		return;
	admission_entered = data->admission.entered;

	switch ((ClusterUndoBlock0CurrentPhase)data->phase) {
	case CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT:
		current_stage_pending_cleanup(data, exiting);
		break;
	case CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD:
		if (data->remote_master) {
			current_stage_remote_release(data);
			(void)cluster_grd_release_holder_by_id(&data->resid, &data->holder);
		} else
			cluster_ges_release_and_drain_local(&data->resid, &data->holder);
		break;
	case CLUSTER_UNDO_BLOCK0_CURRENT_RELEASE_WAIT:
		if (data->remote_master) {
			current_reply_delete(data, GES_REQ_OPCODE_RELEASE);
			current_stage_remote_release(data);
			(void)cluster_grd_release_holder_by_id(&data->resid, &data->holder);
		} else
			cluster_ges_release_and_drain_local(&data->resid, &data->holder);
		break;
	case CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED:
	case CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP:
	case CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR:
		break;
	}

	data->phase = CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP;
	current_active_unlink(data);
	if (admission_entered && data->admission.entered && !current_admission_borrowed(data))
		cluster_semantic_activation_leave(&data->admission);
}

static void
current_backend_exit(int code, Datum arg)
{
	dlist_mutable_iter iter;

	(void)code;
	(void)arg;
	dlist_foreach_modify(iter, &current_active_guards)
	{
		ClusterUndoBlock0CurrentGuardData *data
			= dlist_container(ClusterUndoBlock0CurrentGuardData, active_node, iter.cur);

		current_stage_no_wait_cleanup(data, true);
	}
	current_exit_hook_registered = false;
}

static void
current_error_cleanup(int code, Datum arg)
{
	ClusterUndoBlock0CurrentGuard *guard = (ClusterUndoBlock0CurrentGuard *)DatumGetPointer(arg);

	(void)code;
	if (guard != NULL)
		current_stage_no_wait_cleanup(current_guard_data(guard), true);
}


static void
current_pin_error_cleanup(int code, Datum arg)
{
	volatile ClusterUndoBlock0CurrentPinCleanup *cleanup
		= (volatile ClusterUndoBlock0CurrentPinCleanup *)DatumGetPointer(arg);

	if (cleanup == NULL)
		return;
	if (cleanup->local_pin_held) {
		cluster_undo_block0_unpin(cleanup->pin);
		cleanup->local_pin_held = false;
	}
	current_error_cleanup(code, PointerGetDatum(cleanup->guard));
}

static void
current_live_owner_error_cleanup(int code, Datum arg)
{
	volatile ClusterUndoBlock0LiveOwnerCleanup *cleanup
		= (volatile ClusterUndoBlock0LiveOwnerCleanup *)DatumGetPointer(arg);

	(void)code;
	if (cleanup == NULL)
		return;
	if (cleanup->provision_held) {
		cluster_undo_block0_provision_abort((ClusterUndoBlock0Pin *)cleanup->pin);
		cleanup->provision_held = false;
	} else if (cleanup->pin_held) {
		cluster_undo_block0_unpin((ClusterUndoBlock0Pin *)cleanup->pin);
		cleanup->pin_held = false;
	}
	if (cleanup->frame != NULL && cleanup->frame->owned)
		cluster_undo_block0_frame_release((ClusterUndoBlock0FrameToken *)cleanup->frame);
	if (cleanup->current_active) {
		cluster_undo_block0_current_cancel((ClusterUndoBlock0CurrentGuard *)cleanup->guard);
		cleanup->current_active = false;
	}
	if (cleanup->admission_held) {
		cluster_semantic_activation_leave((ClusterSemanticAdmissionToken *)cleanup->admission);
		cleanup->admission_held = false;
	}
}

static ClusterUndoBlock0CurrentStep
current_fail(ClusterUndoBlock0CurrentGuardData *data, ClusterUndoBlock0Result result,
			 ClusterUndoBlock0Result *failure)
{
	if (data != NULL && current_phase_valid(data->phase)
		&& data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED
		&& data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP)
		current_stage_no_wait_cleanup(data, false);
	current_set_failure(failure, result);
	return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
}

/* A retry count is not an elapsed caller deadline or a proof of lost authority.
 * Diagnostics use the original deadline, never renew it, and do not resample
 * authority after a failed predicate.  -1 means no finite elapsed anchor. */
static long
current_wait_elapsed_ms(const ClusterUndoBlock0CurrentGuardData *data, TimestampTz now)
{
	TimestampTz began;

	if (data->deadline == 0 || data->timeout_ms < 0)
		return -1;
	began = data->deadline - (int64)data->timeout_ms * 1000;
	return now >= began ? (long)((now - began) / 1000) : -1;
}

static void
current_wait_log(const ClusterUndoBlock0CurrentGuardData *data, uint8 phase, const char *reason,
				 ClusterUndoBlock0Result result, int poll_result, uint32 reply_opcode,
				 uint32 reject_reason)
{
	TimestampTz now = GetCurrentTimestamp();
	int64 remaining_ms = data->deadline == 0 ? -1 : (data->deadline - now) / 1000;

	ereport(LOG,
			(errmsg_internal("block0 current wait: PGRAC_FAMILY=BLOCK0_CURRENT "
							 "PGRAC_REASON=%s node=%d segment=%u owner=%u phase=%u "
							 "request=" UINT64_FORMAT " epoch=" UINT64_FORMAT
							 " routing=" UINT64_FORMAT " master=%d attempts=%u "
							 "elapsed_ms=%ld remaining_ms=" INT64_FORMAT " timeout_ms=%d "
							 "result=%u poll=%d reply=%u reject=%u",
							 reason, cluster_node_id, data->logical.segment_id,
							 data->logical.owner_instance, phase, data->holder.request_id,
							 data->holder.cluster_epoch, data->routing_generation,
							 data->master_node, data->retry_attempt,
							 current_wait_elapsed_ms(data, now), remaining_ms, data->timeout_ms,
							 (unsigned)result, poll_result, reply_opcode, reject_reason)));
}

static ClusterUndoBlock0CurrentStep
current_wait_fail(ClusterUndoBlock0CurrentGuardData *data, ClusterUndoBlock0Result result,
				  ClusterUndoBlock0Result *failure, ClusterGesTimeoutSrc source, const char *reason,
				  int poll_result, uint32 reply_opcode, uint32 reject_reason)
{
	uint8 phase = data->phase;
	ClusterUndoBlock0CurrentStep step;

	cluster_ges_timeout_detail_set(source, data->master_node,
								   current_wait_elapsed_ms(data, GetCurrentTimestamp()),
								   data->retry_attempt, -1, data->timeout_ms);
	/* Cleanup precedes even LOG, so an error during formatting cannot orphan
	 * a pending request. Cleanup preserves diagnostic identity/scalars. */
	step = current_fail(data, result, failure);
	current_wait_log(data, phase, reason, result, poll_result, reply_opcode, reject_reason);
	return step;
}

static ClusterUndoBlock0CurrentStep
current_reply_fail(ClusterUndoBlock0CurrentGuardData *data, ClusterUndoBlock0Result *failure,
				   GesReplyWaitPollResult poll, const GesReplyWaitVerdict *verdict)
{
	ClusterGesTimeoutSrc source = CLUSTER_GES_TSRC_BLOCK0_REPLY_MISSING;
	ClusterUndoBlock0Result result = CLUSTER_UNDO_BLOCK0_IO_ERROR;
	const char *reason = "REPLY_MISSING";

	if (poll == GES_REPLY_WAIT_POLL_ABANDONED) {
		source = CLUSTER_GES_TSRC_BLOCK0_REPLY_ABANDONED;
		reason = "REPLY_ABANDONED";
	} else if (poll == GES_REPLY_WAIT_POLL_DELIVERED) {
		source = verdict->reject_reason == GES_REJECT_REASON_WORK_QUEUE_FULL
					 ? CLUSTER_GES_TSRC_MASTER_REJECT_QUEUE_FULL
				 : verdict->reject_reason == GES_REJECT_REASON_TIMEOUT
					 ? CLUSTER_GES_TSRC_MASTER_REJECT_TIMEOUT
					 : CLUSTER_GES_TSRC_BLOCK0_MASTER_REJECT;
		result = current_failure_from_reject(verdict->reject_reason);
		reason = "REMOTE_REJECT";
	}
	return current_wait_fail(data, result, failure, source, reason, (int)poll,
							 verdict == NULL ? 0 : verdict->reply_opcode,
							 verdict == NULL ? 0 : verdict->reject_reason);
}

static void
current_retry_threshold_note(ClusterUndoBlock0CurrentGuardData *data)
{
	if (cluster_ges_retransmit_max_attempts > 0
		&& data->retry_attempt >= (uint16)cluster_ges_retransmit_max_attempts
		&& data->reserved[CURRENT_RETRY_REPORTED_INDEX] == 0) {
		data->reserved[CURRENT_RETRY_REPORTED_INDEX] = 1;
		current_wait_log(data, data->phase, "RETRANSMIT_THRESHOLD_WAIT", CLUSTER_UNDO_BLOCK0_OK,
						 GES_REPLY_WAIT_POLL_PENDING, 0, 0);
	}
}

static ClusterUndoBlock0CurrentStep
current_promote_grant(ClusterUndoBlock0CurrentGuardData *data, ClusterUndoBlock0Result *failure)
{
	ClusterGrdEntryResult result;

	/* The master has granted; every failure from here must stage RELEASE. */
	data->grant_observed = true;
	if (!current_live_recheck(data)) {
		return current_fail(data, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED, failure);
	}
	result = data->remote_master
				 ? cluster_grd_promote_remote_grant_exact(&data->resid, &data->holder)
				 : cluster_grd_revalidate_and_promote(&data->resid, &data->holder, cluster_node_id,
													  data->reservation_generation);
	data->reservation_held = false; /* promote or the helper's failed revalidate consumed it */
	if (result != CLUSTER_GRD_ENTRY_OK)
		return current_fail(data, current_failure_from_grd(result), failure);
	data->phase = CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD;
	return CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
}

static ClusterUndoBlock0CurrentStep
current_acquire_reserve_and_dispatch(ClusterUndoBlock0CurrentGuardData *data,
									 ClusterUndoBlock0Result *failure)
{
	ClusterGrdEntryResult reserve_result;
	ClusterGrdGrantAction action;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nconflicts = 0;
	bool fast_path = false;
	GesRequestPayload request;

	reserve_result
		= cluster_grd_try_reserve(&data->resid, &data->holder, data->mode, cluster_node_id,
								  &fast_path, &data->reservation_generation);
	if (reserve_result == CLUSTER_GRD_ENTRY_FULL) {
		cluster_ges_timeout_detail_set(CLUSTER_GES_TSRC_MASTER_WAIT_QUEUE_FULL, data->master_node,
									   0, 0, -1, data->timeout_ms);
		data->next_retry_at = TimestampTzPlusMilliseconds(
			GetCurrentTimestamp(), CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_INITIAL_MS);
		return CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
	}
	if (reserve_result != CLUSTER_GRD_ENTRY_OK)
		return current_fail(data, current_failure_from_grd(reserve_result), failure);
	data->reservation_held = true;

	/* Correlation exists before either master can complete a grant decision. */
	if (!current_reply_install(data, GES_REQ_OPCODE_REQUEST)) {
		cluster_ges_timeout_detail_set(CLUSTER_GES_TSRC_REPLY_WAIT_TABLE_FULL, data->master_node, 0,
									   0, -1, data->timeout_ms);
		return current_fail(data, CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE, failure);
	}

	if (!data->remote_master) {
		action = cluster_grd_entry_enqueue_or_grant(
			&data->resid, &data->holder, cluster_node_id, data->holder.request_id,
			data->routing_generation, GES_REQ_OPCODE_REQUEST, data->mode, conflicts, &nconflicts);
		if (action == CLUSTER_GRD_GRANT_NOW) {
			data->request_dispatched = true;
			data->grant_observed = true;
			current_reply_delete(data, GES_REQ_OPCODE_REQUEST);
			return current_promote_grant(data, failure);
		}
		if (action != CLUSTER_GRD_ENQUEUED_WAITER) {
			cluster_ges_timeout_detail_set(CLUSTER_GES_TSRC_MASTER_WAIT_QUEUE_FULL,
										   data->master_node, 0, 0, nconflicts, data->timeout_ms);
			return current_fail(data, CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE, failure);
		}
		data->request_dispatched = true;
		if (nconflicts > 0)
			cluster_ges_send_bast_targeted(&data->resid, data->mode, conflicts, nconflicts);
	} else {
		current_fill_request(data, GES_REQ_OPCODE_REQUEST, &request);
		if (!cluster_grd_outbound_enqueue_backend_request((uint32)data->master_node, &request,
														  sizeof(request))) {
			cluster_ges_timeout_detail_set(CLUSTER_GES_TSRC_OUTBOUND_RING_FULL, data->master_node,
										   0, 0, -1, data->timeout_ms);
			return current_fail(data, CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE, failure);
		}
		data->request_dispatched = true;
	}
	data->next_retry_at = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													  CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_INITIAL_MS);
	return CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
}

static ClusterUndoBlock0CurrentStep
current_acquire_begin(const ClusterUndoBlock0LogicalKey *key, ClusterUndoBlock0CurrentMode mode,
					  int timeout_ms, const ClusterSemanticAdmissionToken *caller_admission,
					  uint8 caller_admission_class, ClusterUndoBlock0CurrentGuard *guard,
					  ClusterUndoBlock0Result *failure)
{
	ClusterUndoBlock0CurrentGuardData *data;
	ClusterUndoBlock0CurrentStep step = CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	ClusterSemanticAdmissionResult admission_result;
	int effective_timeout_ms;
	uint64 epoch;
	uint64 routing_generation = 0;
	int32 master;

	if (guard == NULL || key == NULL
		|| (mode != CLUSTER_UNDO_BLOCK0_SCUR && mode != CLUSTER_UNDO_BLOCK0_XCUR)) {
		current_set_failure(failure, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	cluster_ges_timeout_detail_reset();
	data = current_guard_data(guard);
	/* Only the all-zero public initializer is a legal UNUSED representation. */
	if (!current_phase_valid(data->phase) || data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED
		|| memcmp(guard->opaque, (const uint8[168]){ 0 }, sizeof(guard->opaque)) != 0
		|| !current_resid_build(key, &data->resid)) {
		current_set_failure(failure, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	if (current_resid_already_active(&data->resid)) {
		return current_unused_fail(data, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED, failure, 1);
	}
	{
		static const char *const gates[]
			= { "READY",		  "CLUSTER_DISABLED", "NODE_INVALID",	 "LMS_NOT_READY",
				"LMON_NOT_READY", "QUORUM_DENIED",	  "LOCAL_NOT_MEMBER" };
		unsigned int rejection = current_readiness_rejection();

		if (rejection != 0)
			return current_unused_fail_detail(data, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED, failure,
											  2, 8 + rejection, gates[rejection]);
	}
	if (caller_admission != NULL) {
		bool census = caller_admission_class == CURRENT_ADMISSION_CENSUS
					  && mode == CLUSTER_UNDO_BLOCK0_SCUR
					  && cluster_semantic_activation_recheck_r4_terminal_census(caller_admission);
		bool ctrc_release
			= caller_admission_class == CURRENT_ADMISSION_CTRC_RELEASE
			  && mode == CLUSTER_UNDO_BLOCK0_XCUR
			  && caller_admission->side == CLUSTER_SEMANTIC_TARGET_SIDE
			  && cluster_semantic_activation_recheck_r4_terminal_census(caller_admission);
		bool live_owner_source = caller_admission_class == CURRENT_ADMISSION_LIVE_OWNER_SOURCE
								 && mode == CLUSTER_UNDO_BLOCK0_XCUR
								 && caller_admission->side == CLUSTER_SEMANTIC_SOURCE_SIDE
								 && cluster_semantic_activation_recheck(caller_admission);
		bool live_owner_target = caller_admission_class == CURRENT_ADMISSION_LIVE_OWNER_TARGET
								 && mode == CLUSTER_UNDO_BLOCK0_XCUR
								 && caller_admission->side == CLUSTER_SEMANTIC_TARGET_SIDE
								 && cluster_semantic_activation_recheck(caller_admission);

		if (!census && !ctrc_release && !live_owner_source && !live_owner_target)
			return current_unused_fail(data, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED, failure, 3);
	} else if (caller_admission_class != CURRENT_ADMISSION_OWNED) {
		return current_unused_fail(data, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED, failure, 4);
	}

	effective_timeout_ms
		= (timeout_ms == -1 || (timeout_ms == 0 && cluster_ges_request_timeout_ms == -1))
			  ? -1
			  : (timeout_ms > 0 ? timeout_ms : cluster_ges_request_timeout_ms);
	if (timeout_ms < -1 || effective_timeout_ms == 0 || effective_timeout_ms < -1) {
		return current_unused_fail(data, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH, failure, 5);
	}
	epoch = cluster_epoch_get_current();
	master = cluster_grd_lookup_master_gen(&data->resid, &routing_generation);
	if (master < 0
		|| cluster_grd_shard_phase(cluster_grd_shard_for_resource(&data->resid))
			   != GRD_SHARD_NORMAL) {
		return current_unused_fail(data, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED, failure, 6);
	}

	memset(&data->holder, 0, sizeof(data->holder));
	data->holder.node_id = (uint32)cluster_node_id;
	data->holder.procno = (uint32)(MyProc != NULL ? MyProc->pgprocno : 0);
	data->holder.cluster_epoch = epoch;
	data->holder.request_id = cluster_ges_reply_wait_next_request_id();
	if (data->holder.request_id == 0) {
		return current_unused_fail(data, CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE, failure, 7);
	}
	data->logical = *key;
	data->routing_generation = routing_generation;
	data->master_node = master;
	data->timeout_ms = effective_timeout_ms;
	data->mode = (uint8)mode;
	data->remote_master = master != cluster_node_id;
	data->deadline = effective_timeout_ms < 0
						 ? 0
						 : TimestampTzPlusMilliseconds(GetCurrentTimestamp(), effective_timeout_ms);
	data->next_retry_at = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													  CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_INITIAL_MS);

	if (caller_admission != NULL) {
		data->admission = *caller_admission;
		data->reserved[CURRENT_ADMISSION_BORROWED_INDEX] = caller_admission_class;
	} else {
		admission_result = cluster_semantic_activation_enter(
			CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, CLUSTER_SEMANTIC_TARGET_SIDE, &data->admission);
		if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
			memset(data, 0, sizeof(*data));
			current_set_failure(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
			return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
		}
	}
	cluster_undo_block0_current_ensure_exit_hooks();
	current_active_link(data);
	data->phase = CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT;

	PG_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	{
		step = current_acquire_reserve_and_dispatch(data, failure);
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	return step;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin(const ClusterUndoBlock0LogicalKey *key,
										  ClusterUndoBlock0CurrentMode mode, int timeout_ms,
										  ClusterUndoBlock0CurrentGuard *guard,
										  ClusterUndoBlock0Result *failure)
{
	return current_acquire_begin(key, mode, timeout_ms, NULL, CURRENT_ADMISSION_OWNED, guard,
								 failure);
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_admitted(const ClusterUndoBlock0LogicalKey *key,
												   ClusterUndoBlock0CurrentMode mode,
												   int timeout_ms,
												   const ClusterSemanticAdmissionToken *admission,
												   ClusterUndoBlock0CurrentGuard *guard,
												   ClusterUndoBlock0Result *failure)
{
	if (admission == NULL) {
		current_set_failure(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	return current_acquire_begin(key, mode, timeout_ms, admission, CURRENT_ADMISSION_CENSUS, guard,
								 failure);
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_ctrc_release(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	ClusterUndoBlock0Result *failure)
{
	uint32 logical_slot;

	if (admission == NULL || admission->side != CLUSTER_SEMANTIC_TARGET_SIDE || cluster_node_id < 0
		|| key == NULL || key->owner_instance != (uint8)(cluster_node_id + 1)
		|| cluster_undo_block0_logical_slot(key, &logical_slot) != CLUSTER_UNDO_BLOCK0_OK) {
		current_set_failure(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	return current_acquire_begin(key, CLUSTER_UNDO_BLOCK0_XCUR, timeout_ms, admission,
								 CURRENT_ADMISSION_CTRC_RELEASE, guard, failure);
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_live_owner_source(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	ClusterUndoBlock0Result *failure)
{
	uint32 logical_slot;

	if (admission == NULL || admission->side != CLUSTER_SEMANTIC_SOURCE_SIDE || cluster_node_id < 0
		|| key == NULL || key->owner_instance != (uint8)(cluster_node_id + 1)
		|| cluster_undo_block0_logical_slot(key, &logical_slot) != CLUSTER_UNDO_BLOCK0_OK) {
		current_set_failure(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	return current_acquire_begin(key, CLUSTER_UNDO_BLOCK0_XCUR, timeout_ms, admission,
								 CURRENT_ADMISSION_LIVE_OWNER_SOURCE, guard, failure);
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_live_owner_target(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms,
	const ClusterSemanticAdmissionToken *admission, ClusterUndoBlock0CurrentGuard *guard,
	ClusterUndoBlock0Result *failure)
{
	uint32 logical_slot;

	if (admission == NULL || admission->side != CLUSTER_SEMANTIC_TARGET_SIDE || cluster_node_id < 0
		|| key == NULL || key->owner_instance != (uint8)(cluster_node_id + 1)
		|| cluster_undo_block0_logical_slot(key, &logical_slot) != CLUSTER_UNDO_BLOCK0_OK) {
		current_set_failure(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	return current_acquire_begin(key, CLUSTER_UNDO_BLOCK0_XCUR, timeout_ms, admission,
								 CURRENT_ADMISSION_LIVE_OWNER_TARGET, guard, failure);
}

static void
current_reply_poll_note(ClusterUndoBlock0CurrentGuardData *data)
{
	ClusterUndoBlock0ReplyWaitSite site;

	/* Nonblocking owners never enter the adapter, so remain unlabelled. */
	if (data->reply_wait_site_plus_one == 0
		|| data->reply_wait_site_plus_one > CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT)
		return;
	site = (ClusterUndoBlock0ReplyWaitSite)(data->reply_wait_site_plus_one - 1);
	if (data->reply_wait_adapter_required && data->reply_installed)
		cluster_undo_block0_reply_wait_metric_add(site, CLUSTER_UNDO_BLOCK0_WAIT_POLL_VIOLATION);
	if (data->reply_wait_repoll_pending)
		cluster_undo_block0_reply_wait_metric_add(site, CLUSTER_UNDO_BLOCK0_WAIT_WAKE_REPOLL);
	data->reply_wait_repoll_pending = false;
	data->reply_wait_adapter_required = true;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(ClusterUndoBlock0CurrentGuard *guard,
										 ClusterUndoBlock0Result *failure)
{
	ClusterUndoBlock0CurrentGuardData *data;
	GesReplyWaitKey key;
	GesReplyWaitVerdict verdict;
	GesReplyWaitPollResult poll_result;
	TimestampTz now;

	if (guard == NULL) {
		current_set_failure(failure, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	data = current_guard_data(guard);
	if (!current_phase_valid(data->phase))
		return current_fail(NULL, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH, failure);
	if (data->phase == CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD)
		return CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
	if (data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT)
		return current_fail(data, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH, failure);
	current_reply_poll_note(data);
	if (!data->reservation_held && !data->reply_installed && !data->request_dispatched) {
		ClusterUndoBlock0CurrentStep step;

		if (!current_live_recheck(data))
			return current_fail(data, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED, failure);
		now = GetCurrentTimestamp();
		if (data->deadline != 0 && now >= data->deadline)
			return current_wait_fail(data, CLUSTER_UNDO_BLOCK0_IO_ERROR, failure,
									 CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT,
									 "RESERVATION_DEADLINE_EXPIRED", -1, 0, 0);
		if (now < data->next_retry_at)
			return CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
		PG_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
		{
			step = current_acquire_reserve_and_dispatch(data, failure);
		}
		PG_END_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
		return step;
	}
	if (!data->reservation_held || !data->reply_installed || !data->request_dispatched)
		return current_wait_fail(data, CLUSTER_UNDO_BLOCK0_IO_ERROR, failure,
								 CLUSTER_GES_TSRC_BLOCK0_GUARD_INCONSISTENT,
								 "GUARD_FLAGS_INCONSISTENT", -1, 0, 0);

	memset(&verdict, 0, sizeof(verdict));
	current_fill_reply_key(data, GES_REQ_OPCODE_REQUEST, &key);
	PG_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	{
		poll_result = cluster_ges_reply_wait_poll_consume(&key, &verdict);
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	if (poll_result == GES_REPLY_WAIT_POLL_DELIVERED) {
		data->reply_installed = false;
		if (verdict.reply_opcode == GES_REPLY_OPCODE_GRANT
			&& verdict.reject_reason == GES_REJECT_REASON_NONE) {
			data->grant_observed = true;
			return current_promote_grant(data, failure);
		}
		return current_reply_fail(data, failure, poll_result, &verdict);
	}
	if (poll_result != GES_REPLY_WAIT_POLL_PENDING)
		return current_reply_fail(data, failure, poll_result, NULL);
	if (!current_live_recheck(data))
		return current_fail(data, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED, failure);

	now = GetCurrentTimestamp();
	if (data->deadline != 0 && now >= data->deadline)
		return current_wait_fail(data, CLUSTER_UNDO_BLOCK0_IO_ERROR, failure,
								 CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT, "ACQUIRE_DEADLINE_EXPIRED",
								 (int)poll_result, 0, 0);
	if (data->remote_master && now >= data->next_retry_at) {
		GesRequestPayload request;
		int backoff_ms;

		current_retry_threshold_note(data);
		current_fill_request(data, GES_REQ_OPCODE_REQUEST, &request);
		if (!cluster_grd_outbound_enqueue_backend_request((uint32)data->master_node, &request,
														  sizeof(request))) {
			cluster_ges_timeout_detail_set(CLUSTER_GES_TSRC_OUTBOUND_RING_FULL, data->master_node,
										   0, data->retry_attempt, -1, data->timeout_ms);
			return current_fail(data, CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE, failure);
		}
		if (data->retry_attempt < UINT16_MAX)
			data->retry_attempt++;
		backoff_ms = CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_INITIAL_MS << Min(data->retry_attempt, 4);
		if (backoff_ms > CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_MAX_MS)
			backoff_ms = CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_MAX_MS;
		data->next_retry_at = TimestampTzPlusMilliseconds(now, backoff_ms);
	}
	return CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
}

#ifdef USE_ASSERT_CHECKING
static void
current_reply_wait_check_lock(LWLock *lock, LWLockMode mode, void *context)
{
	bool *safe = (bool *)context;

	(void)mode;
	switch (lock->tranche) {
	case LWTRANCHE_LOCK_MANAGER:
	case LWTRANCHE_CLUSTER_LMON:
	case LWTRANCHE_CLUSTER_GES_REPLY_WAIT:
	case LWTRANCHE_CLUSTER_PCM:
	case LWTRANCHE_CLUSTER_TT_STATUS:
	case LWTRANCHE_CLUSTER_TT_SLOT:
	case LWTRANCHE_CLUSTER_UNDO_CLEANER:
	case LWTRANCHE_CLUSTER_UNDO_BUF:
		*safe = false;
		break;
	default:
		break;
	}
}

static bool
current_reply_wait_locks_safe(void)
{
	bool safe = true;

	ForEachLWLockHeldByMe(current_reply_wait_check_lock, &safe);
	return safe;
}
#endif

bool
cluster_undo_block0_current_wait_reply(ClusterUndoBlock0CurrentGuard *guard,
									   ClusterUndoBlock0ReplyWaitSite site)
{
	ClusterUndoBlock0CurrentGuardData *data;
	GesReplyWaitKey key;
	uint32 opcode;
	TimestampTz now;
	bool registered;

	if (guard == NULL || (unsigned)site >= CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT)
		return false;
	data = current_guard_data(guard);
	data->reply_wait_site_plus_one = (uint8)site + 1;
	data->reply_wait_adapter_required = false;
	if (!current_phase_valid(data->phase) || !data->active_linked) {
		cluster_undo_block0_reply_wait_metric_add(site, CLUSTER_UNDO_BLOCK0_WAIT_FALLBACK);
		return false;
	}
	if (data->phase == CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT)
		opcode = GES_REQ_OPCODE_REQUEST;
	else if (data->phase == CLUSTER_UNDO_BLOCK0_CURRENT_RELEASE_WAIT)
		opcode = GES_REQ_OPCODE_RELEASE;
	else {
		cluster_undo_block0_reply_wait_metric_add(site, CLUSTER_UNDO_BLOCK0_WAIT_FALLBACK);
		return false;
	}

	Assert(current_reply_wait_locks_safe());

	/* The CV API takes whole milliseconds.  With less than one quantum left,
	 * return to poll rather than rounding up or taking the caller's fallback.
	 * Neither this path nor a CV wake grants authority or consumes a reply. */
	now = GetCurrentTimestamp();
	if (data->deadline != 0 && (now >= data->deadline || data->deadline - now < 1000)) {
		cluster_undo_block0_reply_wait_metric_add(site, CLUSTER_UNDO_BLOCK0_WAIT_BUDGET_REPOLL);
		return true;
	}
	if (!data->reply_installed) {
		cluster_undo_block0_reply_wait_metric_add(site, CLUSTER_UNDO_BLOCK0_WAIT_FALLBACK);
		return false;
	}

	current_fill_reply_key(data, opcode, &key);
	cluster_undo_block0_reply_wait_metric_add(site, CLUSTER_UNDO_BLOCK0_WAIT_ELIGIBLE);
	registered = cluster_ges_reply_wait_sleep_exact(&key, 1, WAIT_EVENT_CLUSTER_GES_REPLY_WAIT);
	cluster_undo_block0_reply_wait_metric_add(site, registered ? CLUSTER_UNDO_BLOCK0_WAIT_CV
															   : CLUSTER_UNDO_BLOCK0_WAIT_FALLBACK);
	data->reply_wait_repoll_pending = registered;
	return registered;
}

void
cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard)
{
	ClusterUndoBlock0CurrentGuardData *data;
	bool admission_entered;

	if (guard == NULL)
		return;
	data = current_guard_data(guard);
	if (!current_phase_valid(data->phase))
		return;
	admission_entered = data->admission.entered;
	if (data->phase == CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT)
		current_stage_pending_cleanup(data, false);
	else if (data->phase == CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD
			 || data->phase == CLUSTER_UNDO_BLOCK0_CURRENT_RELEASE_WAIT)
		current_stage_no_wait_cleanup(data, false);
	data->phase = CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP;
	current_active_unlink(data);
	if (admission_entered && data->admission.entered && !current_admission_borrowed(data))
		cluster_semantic_activation_leave(&data->admission);
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(ClusterUndoBlock0CurrentGuard *guard,
										  ClusterUndoBlock0Result *failure)
{
	ClusterUndoBlock0CurrentGuardData *data;
	GesRequestPayload release;

	if (guard == NULL) {
		current_set_failure(failure, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	data = current_guard_data(guard);
	if (!current_phase_valid(data->phase) || data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD)
		return current_fail(data, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH, failure);
	if (!current_live_recheck(data))
		return current_fail(data, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED, failure);

	/* The release leg gets its own site label at its first adapter call. */
	data->reply_wait_site_plus_one = 0;
	data->reply_wait_adapter_required = false;
	data->reply_wait_repoll_pending = false;
	data->reserved[CURRENT_RETRY_REPORTED_INDEX] = 0;
	if (!data->remote_master) {
		cluster_ges_release_and_drain_local(&data->resid, &data->holder);
		data->phase = CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP;
		current_active_unlink(data);
		if (data->admission.entered && !current_admission_borrowed(data))
			cluster_semantic_activation_leave(&data->admission);
		return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
	}

	data->deadline = data->timeout_ms < 0
						 ? 0
						 : TimestampTzPlusMilliseconds(GetCurrentTimestamp(), data->timeout_ms);
	data->next_retry_at = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													  CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_INITIAL_MS);
	data->retry_attempt = 0;
	if (!current_reply_install(data, GES_REQ_OPCODE_RELEASE))
		return current_fail(data, CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE, failure);
	data->phase = CLUSTER_UNDO_BLOCK0_CURRENT_RELEASE_WAIT;
	current_fill_request(data, GES_REQ_OPCODE_RELEASE, &release);
	if (!cluster_grd_outbound_enqueue_backend_request((uint32)data->master_node, &release,
													  sizeof(release))) {
		/* Reliable queue owns delivery, while the mirror remains until ACK/cleanup. */
		cluster_grd_outbound_enqueue_cleanup_release((uint32)data->master_node, &release,
													 sizeof(release));
	}
	return CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
}

ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(ClusterUndoBlock0CurrentGuard *guard,
										 ClusterUndoBlock0Result *failure)
{
	ClusterUndoBlock0CurrentGuardData *data;
	GesReplyWaitKey key;
	GesReplyWaitVerdict verdict;
	GesReplyWaitPollResult poll_result;
	TimestampTz now;

	if (guard == NULL) {
		current_set_failure(failure, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
		return CLUSTER_UNDO_BLOCK0_CURRENT_FAILED;
	}
	data = current_guard_data(guard);
	if (!current_phase_valid(data->phase)
		|| data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_RELEASE_WAIT)
		return current_fail(data, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH, failure);
	current_reply_poll_note(data);
	memset(&verdict, 0, sizeof(verdict));
	current_fill_reply_key(data, GES_REQ_OPCODE_RELEASE, &key);
	PG_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	{
		poll_result = cluster_ges_reply_wait_poll_consume(&key, &verdict);
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	if (poll_result == GES_REPLY_WAIT_POLL_DELIVERED) {
		data->reply_installed = false;
		if (verdict.reply_opcode != GES_REPLY_OPCODE_GRANT
			|| verdict.reject_reason != GES_REJECT_REASON_NONE) {
			return current_reply_fail(data, failure, poll_result, &verdict);
		}
		(void)cluster_grd_release_holder_by_id(&data->resid, &data->holder);
		data->phase = CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP;
		current_active_unlink(data);
		if (data->admission.entered && !current_admission_borrowed(data))
			cluster_semantic_activation_leave(&data->admission);
		return CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED;
	}
	if (poll_result != GES_REPLY_WAIT_POLL_PENDING)
		return current_reply_fail(data, failure, poll_result, NULL);
	now = GetCurrentTimestamp();
	if (data->deadline != 0 && now >= data->deadline) {
		return current_wait_fail(data, CLUSTER_UNDO_BLOCK0_IO_ERROR, failure,
								 CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT, "RELEASE_DEADLINE_EXPIRED",
								 (int)poll_result, 0, 0);
	}
	if (now >= data->next_retry_at) {
		GesRequestPayload release;
		int backoff_ms;

		current_retry_threshold_note(data);
		current_fill_request(data, GES_REQ_OPCODE_RELEASE, &release);
		if (!cluster_grd_outbound_enqueue_backend_request((uint32)data->master_node, &release,
														  sizeof(release)))
			current_stage_remote_release(data);
		if (data->retry_attempt < UINT16_MAX)
			data->retry_attempt++;
		backoff_ms = CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_INITIAL_MS << Min(data->retry_attempt, 4);
		if (backoff_ms > CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_MAX_MS)
			backoff_ms = CLUSTER_UNDO_BLOCK0_CURRENT_RETRY_MAX_MS;
		data->next_retry_at = TimestampTzPlusMilliseconds(now, backoff_ms);
	}
	return CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
}

static ClusterUndoBlock0Result
current_held_scur_proof(ClusterUndoBlock0CurrentGuard *guard,
						ClusterUndoBlock0CurrentGuardData **data_out,
						ClusterUndoBlock0AuthorityProof *proof)
{
	ClusterUndoBlock0CurrentGuardData *data;

	if (guard == NULL || data_out == NULL || proof == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	data = current_guard_data(guard);
	if (!current_phase_valid(data->phase) || data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD
		|| data->mode != CLUSTER_UNDO_BLOCK0_SCUR)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	if (!current_live_recheck(data))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	memset(proof, 0, sizeof(*proof));
	proof->kind = CLUSTER_UNDO_BLOCK0_LIVE_OWNER;
	proof->owner_instance = data->logical.owner_instance;
	proof->cluster_epoch_present = true;
	proof->cluster_epoch = data->holder.cluster_epoch;
	*data_out = data;
	return CLUSTER_UNDO_BLOCK0_OK;
}


static ClusterUndoBlock0Result
current_held_xcur_proof(ClusterUndoBlock0CurrentGuard *guard,
						ClusterUndoBlock0CurrentGuardData **data_out,
						ClusterUndoBlock0AuthorityProof *proof)
{
	ClusterUndoBlock0CurrentGuardData *data;

	if (guard == NULL || data_out == NULL || proof == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	data = current_guard_data(guard);
	if (!current_phase_valid(data->phase) || data->phase != CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD
		|| data->mode != CLUSTER_UNDO_BLOCK0_XCUR)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	if (!current_live_recheck(data))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	memset(proof, 0, sizeof(*proof));
	proof->kind = CLUSTER_UNDO_BLOCK0_LIVE_OWNER;
	proof->owner_instance = data->logical.owner_instance;
	proof->cluster_epoch_present = true;
	proof->cluster_epoch = data->holder.cluster_epoch;
	*data_out = data;
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_recheck_exclusive(ClusterUndoBlock0CurrentGuard *guard)
{
	ClusterUndoBlock0CurrentGuardData *data;
	ClusterUndoBlock0AuthorityProof proof;

	return current_held_xcur_proof(guard, &data, &proof);
}

ClusterUndoBlock0Result
cluster_undo_block0_current_sample_generation(ClusterUndoBlock0CurrentGuard *guard,
											  const ClusterUndoBlock0ResolvedRoot *root,
											  ClusterUndoBlock0Generation *observed)
{
	ClusterUndoBlock0CurrentGuardData *data;
	ClusterUndoBlock0AuthorityProof proof;
	ClusterUndoBlock0Generation sampled;
	ClusterUndoBlock0Result result;

	if (root == NULL || observed == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	result = current_held_scur_proof(guard, &data, &proof);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	PG_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	{
		result = cluster_undo_block0_sample_resident_generation(&data->logical, root, &proof,
																&sampled);
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	if (!sampled.known || sampled.value == UINT32_MAX)
		return CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
	if (!current_live_recheck(data))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	*observed = sampled;
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_sample_generation_exclusive(ClusterUndoBlock0CurrentGuard *guard,
														const ClusterUndoBlock0ResolvedRoot *root,
														ClusterUndoBlock0Generation *observed)
{
	ClusterUndoBlock0CurrentGuardData *data;
	ClusterUndoBlock0AuthorityProof proof;
	ClusterUndoBlock0Generation sampled;
	ClusterUndoBlock0Result result;

	if (root == NULL || observed == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	result = current_held_xcur_proof(guard, &data, &proof);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	PG_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	{
		result = cluster_undo_block0_sample_resident_generation(&data->logical, root, &proof,
																&sampled);
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	if (!sampled.known || sampled.value == UINT32_MAX)
		return CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
	if (!current_live_recheck(data))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	*observed = sampled;
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_prove_strict_empty_exclusive(ClusterUndoBlock0CurrentGuard *guard)
{
	ClusterUndoBlock0CurrentGuardData *data;
	ClusterUndoBlock0AuthorityProof proof;
	ClusterUndoBlock0Result result;

	result = current_held_xcur_proof(guard, &data, &proof);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	if (data->reserved[CURRENT_ADMISSION_BORROWED_INDEX] != CURRENT_ADMISSION_LIVE_OWNER_SOURCE
		&& data->reserved[CURRENT_ADMISSION_BORROWED_INDEX] != CURRENT_ADMISSION_LIVE_OWNER_TARGET)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	PG_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	{
		result = cluster_undo_block0_prove_strict_empty(&data->logical, &proof);
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	if (!current_live_recheck(data))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_copy_resident(ClusterUndoBlock0CurrentGuard *guard,
										  const ClusterUndoBlock0ResolvedRoot *root,
										  const ClusterUndoBlock0Generation *expected,
										  char private_page[BLCKSZ])
{
	ClusterUndoBlock0CurrentGuardData *data;
	ClusterUndoBlock0AuthorityProof proof;
	ClusterUndoBlock0Generation observed;
	ClusterUndoBlock0Result result;
	char image[BLCKSZ];

	if (root == NULL || expected == NULL || !expected->known || expected->value == UINT32_MAX
		|| private_page == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	result = current_held_scur_proof(guard, &data, &proof);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	PG_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	{
		result = cluster_undo_block0_copy_resident(&data->logical, root, expected, &proof, image,
												   &observed);
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_error_cleanup, PointerGetDatum(guard));
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;
	if (!observed.known || observed.value == UINT32_MAX || observed.value != expected->value)
		return CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
	if (!current_live_recheck(data))
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	memcpy(private_page, image, BLCKSZ);
	return CLUSTER_UNDO_BLOCK0_OK;
}


ClusterUndoBlock0Result
cluster_undo_block0_current_pin_exclusive(ClusterUndoBlock0CurrentGuard *guard,
										  const ClusterUndoBlock0ResolvedRoot *root,
										  const ClusterUndoBlock0Generation *expected,
										  ClusterUndoBlock0Pin *pin, char **page)
{
	ClusterUndoBlock0CurrentGuardData *data;
	ClusterUndoBlock0AuthorityProof proof;
	ClusterUndoBlock0Pin original_pin;
	volatile ClusterUndoBlock0CurrentPinCleanup cleanup;
	ClusterUndoBlock0Result result;
	char *private_page = NULL;

	if (root == NULL || expected == NULL || !expected->known || expected->value == UINT32_MAX
		|| pin == NULL || page == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	result = current_held_xcur_proof(guard, &data, &proof);
	if (result != CLUSTER_UNDO_BLOCK0_OK)
		return result;

	original_pin = *pin;
	cleanup.guard = guard;
	cleanup.pin = pin;
	cleanup.local_pin_held = false;
	PG_ENSURE_ERROR_CLEANUP(current_pin_error_cleanup,
							PointerGetDatum((ClusterUndoBlock0CurrentPinCleanup *)&cleanup));
	{
		result = cluster_undo_block0_pin(&data->logical, root, expected,
										 CLUSTER_UNDO_BLOCK0_EXCLUSIVE, &proof, pin, &private_page);
		if (result == CLUSTER_UNDO_BLOCK0_OK) {
			cleanup.local_pin_held = true;
			if (!current_live_recheck(data)) {
				cluster_undo_block0_unpin(pin);
				cleanup.local_pin_held = false;
				result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
			}
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_pin_error_cleanup,
								PointerGetDatum((ClusterUndoBlock0CurrentPinCleanup *)&cleanup));
	if (result != CLUSTER_UNDO_BLOCK0_OK) {
		*pin = original_pin;
		return result;
	}
	*page = private_page;
	return CLUSTER_UNDO_BLOCK0_OK;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_ensure_resident_exact(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms,
	ClusterUndoBlock0LiveOwnerPublication *publication)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0FrameToken frame = { UINT32_MAX, false };
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation generation = { false, 0 };
	ClusterUndoBlock0AuthorityProof proof;
	ClusterUndoBlock0CurrentGuardData *data = NULL;
	ClusterUndoBlock0LiveOwnerPublicationData candidate;
	volatile ClusterUndoBlock0LiveOwnerCleanup cleanup;
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterUndoBlock0Result current_failure = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterSemanticAdmissionResult admission_result;
	uint32 logical_slot;
	char *page = NULL;
	bool creator = false;

	if (publication != NULL)
		memset(publication, 0, sizeof(*publication));

	if (key == NULL || cluster_node_id < 0 || key->owner_instance != (uint8)(cluster_node_id + 1)
		|| cluster_undo_block0_logical_slot(key, &logical_slot) != CLUSTER_UNDO_BLOCK0_OK)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;

	memset(&admission, 0, sizeof(admission));
	memset(&pin, 0, sizeof(pin));
	pin.slot = -1;
	memset(&root, 0, sizeof(root));
	memset(&final_root, 0, sizeof(final_root));
	memset(&proof, 0, sizeof(proof));
	memset(&candidate, 0, sizeof(candidate));
	memset((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup, 0, sizeof(cleanup));
	cleanup.admission = &admission;
	cleanup.guard = &guard;
	cleanup.frame = &frame;
	cleanup.pin = &pin;

	cluster_undo_block0_current_ensure_exit_hooks();
	/*
	 * Residency is a normal live-owner mutation prerequisite.  Borrow the
	 * currently admitted R4 modifier polarity so the same producer remains
	 * reachable across SOURCE -> TARGET cutover; CLOSED admits neither side.
	 */
	admission_result = cluster_semantic_activation_modifier_enter(true, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	cleanup.admission_held = true;

	PG_ENSURE_ERROR_CLEANUP(current_live_owner_error_cleanup,
							PointerGetDatum((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup));
	{
		if (!(admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
				  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
						&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
						key->segment_id, &root)
			  : admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
				  ? cluster_semantic_activation_resolve_shared_undo_root(
						&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
						key->segment_id, &root)
				  : false))
			goto ensure_done;

		step = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
				   ? cluster_undo_block0_current_acquire_begin_live_owner_source(
						 key, timeout_ms, &admission, &guard, &current_failure)
				   : cluster_undo_block0_current_acquire_begin_live_owner_target(
						 key, timeout_ms, &admission, &guard, &current_failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
			result = current_failure;
			goto ensure_done;
		}
		cleanup.current_active = true;
		while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
			CHECK_FOR_INTERRUPTS();
			step = cluster_undo_block0_current_acquire_poll(&guard, &current_failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
				&& !cluster_undo_block0_current_wait_reply(
					&guard, CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_ACQUIRE))
				pg_usleep(1000L);
		}
		if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
			cleanup.current_active = false;
			result = current_failure;
			goto ensure_done;
		}

		result = current_held_xcur_proof(&guard, &data, &proof);
		if (result != CLUSTER_UNDO_BLOCK0_OK)
			goto ensure_done;
		result = cluster_undo_block0_sample_resident_generation(key, &root, &proof, &generation);
		if (result == CLUSTER_UNDO_BLOCK0_OK) {
			if (!generation.known || generation.value == UINT32_MAX)
				result = CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
		} else if (result == CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED) {
			result = cluster_undo_block0_frame_reserve_batch(1, &frame);
			if (result != CLUSTER_UNDO_BLOCK0_OK)
				goto ensure_done;
			result = cluster_undo_block0_provision_begin(key, &root, &proof, &frame, &pin, &page,
														 &creator);
			if (result != CLUSTER_UNDO_BLOCK0_OK)
				goto ensure_done;
			if (creator) {
				cleanup.provision_held = true;
				result = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
				goto ensure_done;
			}
			cleanup.pin_held = true;
			generation = pin.observed_generation;
			if (!generation.known || generation.value == UINT32_MAX) {
				result = CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
				goto ensure_done;
			}
		} else {
			goto ensure_done;
		}
		if (result != CLUSTER_UNDO_BLOCK0_OK)
			goto ensure_done;

		if (!(admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
				  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
						&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
						key->segment_id, &final_root)
			  : admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
				  ? cluster_semantic_activation_resolve_shared_undo_root(
						&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
						key->segment_id, &final_root)
				  : false)
			|| !cluster_undo_block0_root_matches(&root, &final_root)
			|| !current_live_recheck(data)) {
			result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
			goto ensure_done;
		}
		candidate.magic = CLUSTER_UNDO_BLOCK0_LIVE_OWNER_PUBLICATION_MAGIC;
		candidate.item.logical = *key;
		candidate.item.resolved_root = final_root;
		candidate.item.generation = generation;
		candidate.item.proof = proof;
		candidate.admission = admission;
		result = CLUSTER_UNDO_BLOCK0_OK;

	ensure_done:
		if (cleanup.provision_held) {
			cluster_undo_block0_provision_abort(&pin);
			cleanup.provision_held = false;
		} else if (cleanup.pin_held) {
			cluster_undo_block0_unpin(&pin);
			cleanup.pin_held = false;
		}
		if (frame.owned)
			cluster_undo_block0_frame_release(&frame);
		if (cleanup.current_active) {
			step = cluster_undo_block0_current_release_begin(&guard, &current_failure);
			while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
				CHECK_FOR_INTERRUPTS();
				step = cluster_undo_block0_current_release_poll(&guard, &current_failure);
				if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
					&& !cluster_undo_block0_current_wait_reply(
						&guard, CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_RELEASE))
					pg_usleep(1000L);
			}
			cleanup.current_active = false;
			if (step != CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED && result == CLUSTER_UNDO_BLOCK0_OK)
				result = current_failure;
		}
		cluster_semantic_activation_leave(&admission);
		cleanup.admission_held = false;
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_live_owner_error_cleanup,
								PointerGetDatum((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup));
	if (result == CLUSTER_UNDO_BLOCK0_OK && publication != NULL)
		memcpy(publication, &candidate, sizeof(candidate));
	return result;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_ensure_resident(const ClusterUndoBlock0LogicalKey *key,
													   int timeout_ms)
{
	return cluster_undo_block0_current_live_owner_ensure_resident_exact(key, timeout_ms, NULL);
}

static bool
current_reuse_page_identity(const char page[BLCKSZ], const ClusterUndoBlock0LogicalKey *key,
							uint32 generation, uint8 expected_state)
{
	const PageHeader ph = (const PageHeader)page;
	const UndoSegmentHeaderData *header = (const UndoSegmentHeaderData *)page;

	return page != NULL && key != NULL && (ph->pd_flags & PD_UNDO_SEG_HEADER) != 0
		   && PageGetPageSize((Page)page) == BLCKSZ
		   && PageGetPageLayoutVersion((Page)page) == PG_PAGE_LAYOUT_VERSION
		   && header->segment_id == key->segment_id
		   && header->segment_size_bytes == UNDO_SEGMENT_SIZE_BYTES
		   && header->owner_instance == key->owner_instance
		   && header->tt_slots_count == TT_SLOTS_PER_SEGMENT && header->wrap_count == generation
		   && header->segment_state == expected_state;
}

static bool
current_reuse_predecessor_exact(const char disk_page[BLCKSZ], const char resident_page[BLCKSZ],
								const ClusterUndoBlock0LogicalKey *key, uint32 generation)
{
	const UndoSegmentHeaderData *disk = (const UndoSegmentHeaderData *)disk_page;
	uint32 i;

	if (!current_reuse_page_identity(disk_page, key, generation, SEGMENT_RECYCLABLE)
		|| !current_reuse_page_identity(resident_page, key, generation, SEGMENT_RECYCLABLE)
		|| memcmp(disk_page, resident_page, BLCKSZ) != 0)
		return false;
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		if (disk->tt_slots[i].status == TT_SLOT_ACTIVE
			|| disk->tt_slots[i].status == TT_SLOT_INVALID)
			return false;
	}
	return true;
}

static bool
current_reuse_successor_exact(const char successor_page[BLCKSZ],
							  const ClusterUndoBlock0LogicalKey *key, uint32 successor_generation)
{
	PGAlignedBlock fresh;
	UndoSegmentHeaderData *fresh_header = (UndoSegmentHeaderData *)fresh.data;

	if (successor_page == NULL || key == NULL)
		return false;
	cluster_undo_segment_make_header_bytes(key->segment_id, key->owner_instance, fresh.data);
	fresh_header->wrap_count = successor_generation;
	return memcmp(successor_page, fresh.data, BLCKSZ) == 0;
}

/* A normal live lifecycle transition may change only the monotonic metadata
 * owned by the segment lifecycle.  Canonical TT bytes, generation, page
 * identity and all unnamed/reserved bytes remain byte-identical. */
static bool
current_lifecycle_successor_exact(const char predecessor_page[BLCKSZ],
								  const char successor_page[BLCKSZ],
								  const ClusterUndoBlock0LogicalKey *key, uint32 generation)
{
	const UndoSegmentHeaderData *before = (const UndoSegmentHeaderData *)predecessor_page;
	const UndoSegmentHeaderData *after = (const UndoSegmentHeaderData *)successor_page;
	PGAlignedBlock allowed;
	UndoSegmentHeaderData *allowed_header = (UndoSegmentHeaderData *)allowed.data;
	uint32 i;

	if (predecessor_page == NULL || successor_page == NULL || key == NULL
		|| !current_reuse_page_identity(predecessor_page, key, generation, before->segment_state)
		|| !current_reuse_page_identity(successor_page, key, generation, after->segment_state))
		return false;
	if (!(after->segment_state == before->segment_state
		  || (before->segment_state == SEGMENT_ALLOCATED && after->segment_state == SEGMENT_ACTIVE)
		  || (before->segment_state == SEGMENT_ACTIVE
			  && after->segment_state == SEGMENT_COMMITTED)))
		return false;
	if ((after->segment_flags & before->segment_flags) != before->segment_flags
		|| (after->segment_flags & ~UNDO_SEGMENT_FLAG_FULL) != 0)
		return false;
	if (after->tail_block != before->tail_block
		&& !(before->tail_block == 0 && after->tail_block == 1))
		return false;
	if (UndoSegmentHeader_record_seal_upper_scn(after)
			!= UndoSegmentHeader_record_seal_upper_scn(before)
		&& !(SCN_VALID(UndoSegmentHeader_record_seal_upper_scn(after))
			 && !SCN_VALID(UndoSegmentHeader_record_seal_upper_scn(before))))
		return false;
	if (UndoSegmentHeader_record_drain_upper_scn(after)
			!= UndoSegmentHeader_record_drain_upper_scn(before)
		&& !(before->segment_state == SEGMENT_ACTIVE && after->segment_state == SEGMENT_COMMITTED
			 && UndoSegmentHeader_record_drain_upper_scn(before) == InvalidScn
			 && SCN_VALID(UndoSegmentHeader_record_drain_upper_scn(after))))
		return false;
	if (before->segment_state == SEGMENT_ACTIVE && after->segment_state == SEGMENT_COMMITTED
		&& (after->tail_block != 0 || SCN_VALID(UndoSegmentHeader_record_seal_upper_scn(after)))
		&& (!SCN_VALID(UndoSegmentHeader_record_seal_upper_scn(after))
			|| !SCN_VALID(UndoSegmentHeader_record_drain_upper_scn(after))
			|| scn_time_cmp(UndoSegmentHeader_record_drain_upper_scn(after),
							UndoSegmentHeader_record_seal_upper_scn(after))
				   < 0))
		return false;
	for (i = 0; i < UNDO_FREE_BITMAP_BYTES; i++) {
		if ((after->free_block_bitmap[i] & before->free_block_bitmap[i])
			!= before->free_block_bitmap[i])
			return false;
	}

	memcpy(allowed.data, predecessor_page, BLCKSZ);
	allowed_header->segment_state = after->segment_state;
	allowed_header->segment_flags = after->segment_flags;
	allowed_header->tail_block = after->tail_block;
	UndoSegmentHeader_set_record_seal_upper_scn(allowed_header,
												UndoSegmentHeader_record_seal_upper_scn(after));
	UndoSegmentHeader_set_record_drain_upper_scn(allowed_header,
												 UndoSegmentHeader_record_drain_upper_scn(after));
	memcpy(allowed_header->free_block_bitmap, after->free_block_bitmap,
		   sizeof(allowed_header->free_block_bitmap));
	return memcmp(allowed.data, successor_page, BLCKSZ) == 0;
}

/* Apply one byte-exact, same-generation lifecycle successor under the sole
 * live 0xFB XCUR/content-X domain.  Callers freeze both images with no local
 * lifecycle/content lock held.  A byte-identical already-applied successor is
 * idempotent; every other predecessor drift fails closed. */
ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_mutate_exact(const ClusterUndoBlock0LogicalKey *key,
													const ClusterUndoBlock0Generation *expected,
													const char predecessor_page[BLCKSZ],
													const char successor_page[BLCKSZ],
													int timeout_ms)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation observed = { false, 0 };
	PGAlignedBlock disk_block;
	PGAlignedBlock rebased_block;
	UndoSegmentHeaderData *rebased = (UndoSegmentHeaderData *)rebased_block.data;
	const UndoSegmentHeaderData *disk = (const UndoSegmentHeaderData *)disk_block.data;
	const char *publish_page = successor_page;
	char *resident_page = NULL;
	volatile ClusterUndoBlock0LiveOwnerCleanup cleanup;
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterUndoBlock0Result current_failure = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterSemanticAdmissionResult admission_result;
	bool root_ok;

	if (key == NULL || expected == NULL || !expected->known || expected->value == UINT32_MAX
		|| predecessor_page == NULL || successor_page == NULL || timeout_ms <= 0
		|| cluster_node_id < 0 || key->owner_instance != (uint8)(cluster_node_id + 1)
		|| !current_lifecycle_successor_exact(predecessor_page, successor_page, key,
											  expected->value))
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;

	memset(&admission, 0, sizeof(admission));
	memset(&pin, 0, sizeof(pin));
	pin.slot = -1;
	memset(&root, 0, sizeof(root));
	memset(&final_root, 0, sizeof(final_root));
	memset((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup, 0, sizeof(cleanup));
	cleanup.admission = &admission;
	cleanup.guard = &guard;
	cleanup.pin = &pin;

	cluster_undo_block0_current_ensure_exit_hooks();
	admission_result = cluster_semantic_activation_modifier_enter(true, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	cleanup.admission_held = true;

	PG_ENSURE_ERROR_CLEANUP(current_live_owner_error_cleanup,
							PointerGetDatum((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup));
	{
		root_ok = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &root)
				  : admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &root)
					  : false;
		if (!root_ok)
			goto mutate_done;

		step = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
				   ? cluster_undo_block0_current_acquire_begin_live_owner_source(
						 key, timeout_ms, &admission, &guard, &current_failure)
				   : cluster_undo_block0_current_acquire_begin_live_owner_target(
						 key, timeout_ms, &admission, &guard, &current_failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
			result = current_failure;
			goto mutate_done;
		}
		cleanup.current_active = true;
		while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
			CHECK_FOR_INTERRUPTS();
			step = cluster_undo_block0_current_acquire_poll(&guard, &current_failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
				&& !cluster_undo_block0_current_wait_reply(
					&guard, CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_MUTATE))
				pg_usleep(1000L);
		}
		if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
			result = current_failure;
			goto mutate_done;
		}

		result = cluster_undo_block0_current_sample_generation_exclusive(&guard, &root, &observed);
		if (result != CLUSTER_UNDO_BLOCK0_OK)
			goto mutate_done;
		if (!cluster_undo_block0_generation_matches(&observed, expected)) {
			result = CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
			goto mutate_done;
		}
		result = cluster_undo_block0_current_pin_exclusive(&guard, &root, expected, &pin,
														   &resident_page);
		if (result != CLUSTER_UNDO_BLOCK0_OK || resident_page == NULL)
			goto mutate_done;
		cleanup.pin_held = true;

		if (!cluster_undo_smgr_read_block(root.intent, key->segment_id, key->owner_instance, 0,
										  disk_block.data)) {
			result = CLUSTER_UNDO_BLOCK0_IO_ERROR;
			goto mutate_done;
		}
		if (memcmp(disk_block.data, resident_page, BLCKSZ) != 0) {
			result = CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
			goto mutate_done;
		}
		if (memcmp(disk_block.data, successor_page, BLCKSZ) == 0
			&& memcmp(resident_page, successor_page, BLCKSZ) == 0) {
			result = CLUSTER_UNDO_BLOCK0_OK;
			goto mutate_done;
		}
		if (memcmp(disk_block.data, predecessor_page, BLCKSZ) != 0) {
			/* A canonical TT publish may have landed after the lifecycle
			 * caller froze its predecessor.  Rebase only the requested,
			 * monotonic lifecycle fields onto the exact current page; the
			 * validator below rejects every non-TT or non-monotonic drift. */
			memcpy(rebased_block.data, successor_page, BLCKSZ);
			memcpy(rebased->tt_slots, disk->tt_slots, sizeof(rebased->tt_slots));
			if (!current_lifecycle_successor_exact(disk_block.data, rebased_block.data, key,
												   expected->value)) {
				result = CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
				goto mutate_done;
			}
			publish_page = rebased_block.data;
		}

		memset(&final_root, 0, sizeof(final_root));
		root_ok = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &final_root)
				  : admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &final_root)
					  : false;
		if (!root_ok || !cluster_undo_block0_root_matches(&root, &final_root)
			|| !cluster_semantic_activation_modifier_recheck(&admission, true)
			|| cluster_undo_block0_current_recheck_exclusive(&guard) != CLUSTER_UNDO_BLOCK0_OK) {
			result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
			goto mutate_done;
		}

		cluster_undo_block0_flush_sync(&pin, publish_page, InvalidXLogRecPtr, false);
		result = CLUSTER_UNDO_BLOCK0_OK;

	mutate_done:
		if (cleanup.pin_held) {
			cluster_undo_block0_unpin(&pin);
			cleanup.pin_held = false;
		}
		if (cleanup.current_active) {
			cluster_undo_block0_current_cancel(&guard);
			cleanup.current_active = false;
		}
		cluster_semantic_activation_leave(&admission);
		cleanup.admission_held = false;
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_live_owner_error_cleanup,
								PointerGetDatum((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup));
	return result;
}

/*
 * Whole-segment reuse is the only live runtime transition that replaces the
 * complete block-zero image.  Serialize that exact old-generation -> next-
 * generation edge through the existing generation-independent 0xFB XCUR and
 * the resident content-X pin, then let the resident flush primitive publish
 * identical durable and resident bytes.  The lifecycle caller must invoke
 * this with no lifecycle/content lock held.
 */
ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_reuse_exact(const ClusterUndoBlock0LogicalKey *key,
												   const ClusterUndoBlock0Generation *expected,
												   const char successor_page[BLCKSZ],
												   int timeout_ms)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation observed = { false, 0 };
	ClusterUndoBlock0Generation successor_generation = { false, 0 };
	PGAlignedBlock disk_block;
	char *resident_page = NULL;
	volatile ClusterUndoBlock0LiveOwnerCleanup cleanup;
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterUndoBlock0Result current_failure = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterSemanticAdmissionResult admission_result;
	XLogRecPtr reuse_lsn;
	bool root_ok;

	if (key == NULL || expected == NULL || !expected->known || expected->value == UINT32_MAX
		|| successor_page == NULL || timeout_ms <= 0 || cluster_node_id < 0
		|| key->owner_instance != (uint8)(cluster_node_id + 1)
		|| !cluster_undo_block0_generation_advance(expected, &successor_generation)
		|| !current_reuse_successor_exact(successor_page, key, successor_generation.value))
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;

	memset(&admission, 0, sizeof(admission));
	memset(&pin, 0, sizeof(pin));
	pin.slot = -1;
	memset(&root, 0, sizeof(root));
	memset(&final_root, 0, sizeof(final_root));
	memset((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup, 0, sizeof(cleanup));
	cleanup.admission = &admission;
	cleanup.guard = &guard;
	cleanup.pin = &pin;

	cluster_undo_block0_current_ensure_exit_hooks();
	admission_result = cluster_semantic_activation_modifier_enter(true, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	cleanup.admission_held = true;

	PG_ENSURE_ERROR_CLEANUP(current_live_owner_error_cleanup,
							PointerGetDatum((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup));
	{
		root_ok = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &root)
				  : admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &root)
					  : false;
		if (!root_ok)
			goto reuse_done;

		step = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
				   ? cluster_undo_block0_current_acquire_begin_live_owner_source(
						 key, timeout_ms, &admission, &guard, &current_failure)
				   : cluster_undo_block0_current_acquire_begin_live_owner_target(
						 key, timeout_ms, &admission, &guard, &current_failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED) {
			result = current_failure;
			goto reuse_done;
		}
		cleanup.current_active = true;
		while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
			CHECK_FOR_INTERRUPTS();
			step = cluster_undo_block0_current_acquire_poll(&guard, &current_failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
				&& !cluster_undo_block0_current_wait_reply(
					&guard, CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_REUSE))
				pg_usleep(1000L);
		}
		if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD) {
			result = current_failure;
			goto reuse_done;
		}

		result = cluster_undo_block0_current_sample_generation_exclusive(&guard, &root, &observed);
		if (result != CLUSTER_UNDO_BLOCK0_OK)
			goto reuse_done;
		if (!cluster_undo_block0_generation_matches(&observed, expected)) {
			result = CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
			goto reuse_done;
		}
		result = cluster_undo_block0_current_pin_exclusive(&guard, &root, expected, &pin,
														   &resident_page);
		if (result != CLUSTER_UNDO_BLOCK0_OK || resident_page == NULL)
			goto reuse_done;
		cleanup.pin_held = true;

		if (!cluster_undo_smgr_read_block(root.intent, key->segment_id, key->owner_instance, 0,
										  disk_block.data)) {
			result = CLUSTER_UNDO_BLOCK0_IO_ERROR;
			goto reuse_done;
		}
		if (!current_reuse_page_identity(disk_block.data, key, expected->value, SEGMENT_RECYCLABLE)
			|| ((UndoSegmentHeaderData *)resident_page)->wrap_count != expected->value) {
			result = CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH;
			goto reuse_done;
		}
		if (!current_reuse_predecessor_exact(disk_block.data, resident_page, key,
											 expected->value)) {
			result = CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
			goto reuse_done;
		}

		memset(&final_root, 0, sizeof(final_root));
		root_ok = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &final_root)
				  : admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &final_root)
					  : false;
		if (!root_ok || !cluster_undo_block0_root_matches(&root, &final_root)
			|| !cluster_semantic_activation_modifier_recheck(&admission, true)
			|| cluster_undo_block0_current_recheck_exclusive(&guard) != CLUSTER_UNDO_BLOCK0_OK) {
			result = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
			goto reuse_done;
		}

		reuse_lsn
			= cluster_undo_emit_segment_reuse(key->owner_instance, key->segment_id, expected->value,
											  successor_generation.value, successor_page);
		if (XLogRecPtrIsInvalid(reuse_lsn)) {
			result = CLUSTER_UNDO_BLOCK0_IO_ERROR;
			goto reuse_done;
		}
		cluster_undo_block0_flush_sync(&pin, successor_page, reuse_lsn, false);
		result = CLUSTER_UNDO_BLOCK0_OK;

	reuse_done:
		if (cleanup.pin_held) {
			cluster_undo_block0_unpin(&pin);
			cleanup.pin_held = false;
		}
		if (cleanup.current_active) {
			cluster_undo_block0_current_cancel(&guard);
			cleanup.current_active = false;
		}
		cluster_semantic_activation_leave(&admission);
		cleanup.admission_held = false;
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_live_owner_error_cleanup,
								PointerGetDatum((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup));
	return result;
}

static bool
current_recycle_resident_identity(const char page[BLCKSZ], const ClusterUndoBlock0LogicalKey *key,
								  uint32 generation)
{
	const UndoSegmentHeaderData *header = (const UndoSegmentHeaderData *)page;

	return header != NULL
		   && (header->segment_state == SEGMENT_ACTIVE || header->segment_state == SEGMENT_COMMITTED
			   || header->segment_state == SEGMENT_RECYCLABLE)
		   && current_reuse_page_identity(page, key, generation, header->segment_state);
}

ClusterUndoBlock0RecycleResult
cluster_undo_block0_current_live_owner_recycle_exact(const ClusterUndoBlock0LogicalKey *key,
													 SCN horizon, uint64 expected_epoch,
													 int timeout_ms)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation observed = { false, 0 };
	PGAlignedBlock disk_block;
	PGAlignedBlock successor_block;
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)disk_block.data;
	UndoSegmentHeaderData *successor = (UndoSegmentHeaderData *)successor_block.data;
	char *resident_page = NULL;
	volatile ClusterUndoBlock0LiveOwnerCleanup cleanup;
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result current_failure = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	ClusterUndoBlock0Result result;
	ClusterSemanticAdmissionResult admission_result;
	ClusterUndoBlock0RecycleResult recycle_result = CLUSTER_UNDO_BLOCK0_RECYCLE_FAILED;
	XLogRecPtr recycle_lsn;
	bool root_ok;

	if (key == NULL || !SCN_VALID(horizon)
		|| (expected_epoch == 0 && cluster_conf_node_count() != 4)
		|| cluster_epoch_get_current() != expected_epoch || timeout_ms <= 0 || cluster_node_id < 0
		|| key->owner_instance != (uint8)(cluster_node_id + 1))
		return CLUSTER_UNDO_BLOCK0_RECYCLE_FAILED;

	memset(&admission, 0, sizeof(admission));
	memset(&pin, 0, sizeof(pin));
	pin.slot = -1;
	memset(&root, 0, sizeof(root));
	memset(&final_root, 0, sizeof(final_root));
	memset((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup, 0, sizeof(cleanup));
	cleanup.admission = &admission;
	cleanup.guard = &guard;
	cleanup.pin = &pin;

	cluster_undo_block0_current_ensure_exit_hooks();
	admission_result = cluster_semantic_activation_modifier_enter(true, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return CLUSTER_UNDO_BLOCK0_RECYCLE_FAILED;
	cleanup.admission_held = true;

	PG_ENSURE_ERROR_CLEANUP(current_live_owner_error_cleanup,
							PointerGetDatum((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup));
	{
		if (admission.formation_epoch != expected_epoch)
			goto recycle_done;
		root_ok = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &root)
				  : admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &root)
					  : false;
		if (!root_ok)
			goto recycle_done;

		step = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
				   ? cluster_undo_block0_current_acquire_begin_live_owner_source(
						 key, timeout_ms, &admission, &guard, &current_failure)
				   : cluster_undo_block0_current_acquire_begin_live_owner_target(
						 key, timeout_ms, &admission, &guard, &current_failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED)
			goto recycle_done;
		cleanup.current_active = true;
		while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING) {
			CHECK_FOR_INTERRUPTS();
			step = cluster_undo_block0_current_acquire_poll(&guard, &current_failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
				&& !cluster_undo_block0_current_wait_reply(
					&guard, CLUSTER_UNDO_BLOCK0_WAIT_LIVE_OWNER_RECYCLE))
				pg_usleep(1000L);
		}
		if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
			goto recycle_done;

		result = cluster_undo_block0_current_sample_generation_exclusive(&guard, &root, &observed);
		if (result != CLUSTER_UNDO_BLOCK0_OK || !observed.known || observed.value == UINT32_MAX)
			goto recycle_done;
		result = cluster_undo_block0_current_pin_exclusive(&guard, &root, &observed, &pin,
														   &resident_page);
		if (result != CLUSTER_UNDO_BLOCK0_OK || resident_page == NULL)
			goto recycle_done;
		cleanup.pin_held = true;

		if (!cluster_undo_smgr_read_block(root.intent, key->segment_id, key->owner_instance, 0,
										  disk_block.data))
			goto recycle_done;
		if (!current_recycle_resident_identity(resident_page, key, observed.value)
			|| memcmp(disk->tt_slots, ((UndoSegmentHeaderData *)resident_page)->tt_slots,
					  sizeof(disk->tt_slots))
				   != 0)
			goto recycle_done;

		/* Parallel certificate workers can have durable flags while their
		 * exact shared-origin handoff is still pending. Never expose the
		 * segment for generation reuse across that publication window. */
		if (cluster_peer_mode_enabled()
			&& !cluster_ctrc_reuse_handoff_complete(disk, INVALID_TT_SLOT_OFFSET)) {
			recycle_result = CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED;
			goto recycle_done;
		}
		if (current_reuse_page_identity(disk_block.data, key, observed.value, SEGMENT_RECYCLABLE)) {
			if (memcmp(disk_block.data, resident_page, BLCKSZ) != 0)
				cluster_undo_block0_flush_sync(&pin, disk_block.data, InvalidXLogRecPtr, false);
			recycle_result = CLUSTER_UNDO_BLOCK0_RECYCLE_ALREADY;
			goto recycle_done;
		}
		if (!current_reuse_page_identity(disk_block.data, key, observed.value, SEGMENT_COMMITTED)) {
			recycle_result = CLUSTER_UNDO_BLOCK0_RECYCLE_NOT_COMMITTED;
			goto recycle_done;
		}
		if (!cluster_undo_segment_recyclable_for_mode(disk, horizon, cluster_peer_mode_enabled())) {
			recycle_result = CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED;
			goto recycle_done;
		}

		memcpy(successor_block.data, disk_block.data, BLCKSZ);
		successor->segment_state = SEGMENT_RECYCLABLE;
		memset(&final_root, 0, sizeof(final_root));
		root_ok = admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &final_root)
				  : admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
					  ? cluster_semantic_activation_resolve_shared_undo_root(
							&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED, key->owner_instance,
							key->segment_id, &final_root)
					  : false;
		if (!root_ok || !cluster_undo_block0_root_matches(&root, &final_root)
			|| !cluster_semantic_activation_modifier_recheck(&admission, true)
			|| cluster_epoch_get_current() != expected_epoch
			|| cluster_undo_block0_current_recheck_exclusive(&guard) != CLUSTER_UNDO_BLOCK0_OK)
			goto recycle_done;

		recycle_lsn = cluster_undo_emit_segment_recycle(key->owner_instance, key->segment_id,
														observed.value, (uint8)SEGMENT_COMMITTED,
														(uint8)SEGMENT_RECYCLABLE);
		if (XLogRecPtrIsInvalid(recycle_lsn))
			goto recycle_done;
		cluster_undo_block0_flush_sync(&pin, successor_block.data, recycle_lsn, false);
		recycle_result = CLUSTER_UNDO_BLOCK0_RECYCLE_ADVANCED;

	recycle_done:
		if (cleanup.pin_held) {
			cluster_undo_block0_unpin(&pin);
			cleanup.pin_held = false;
		}
		if (cleanup.current_active) {
			cluster_undo_block0_current_cancel(&guard);
			cleanup.current_active = false;
		}
		cluster_semantic_activation_leave(&admission);
		cleanup.admission_held = false;
	}
	PG_END_ENSURE_ERROR_CLEANUP(current_live_owner_error_cleanup,
								PointerGetDatum((ClusterUndoBlock0LiveOwnerCleanup *)&cleanup));
	return recycle_result;
}

bool
cluster_undo_block0_current_live_owner_publication_recheck(
	const ClusterUndoBlock0LiveOwnerPublication *publication)
{
	const ClusterUndoBlock0LiveOwnerPublicationData *data;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0Generation generation = { false, 0 };
	ClusterUndoBlock0Result result;
	bool root_resolved;

	if (publication == NULL)
		return false;
	data = (const ClusterUndoBlock0LiveOwnerPublicationData *)(const void *)publication;
	if (data->magic != CLUSTER_UNDO_BLOCK0_LIVE_OWNER_PUBLICATION_MAGIC || data->reserved != 0
		|| !cluster_semantic_activation_recheck(&data->admission))
		return false;

	memset(&root, 0, sizeof(root));
	root_resolved
		= data->admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
			  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
					&data->admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
					data->item.logical.owner_instance, data->item.logical.segment_id, &root)
		  : data->admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
			  ? cluster_semantic_activation_resolve_shared_undo_root(
					&data->admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
					data->item.logical.owner_instance, data->item.logical.segment_id, &root)
			  : false;
	if (!root_resolved || !cluster_undo_block0_root_matches(&root, &data->item.resolved_root))
		return false;

	result = cluster_undo_block0_sample_resident_generation(
		&data->item.logical, &data->item.resolved_root, &data->item.proof, &generation);
	if (result != CLUSTER_UNDO_BLOCK0_OK
		|| !cluster_undo_block0_generation_matches(&generation, &data->item.generation))
		return false;

	return cluster_semantic_activation_recheck(&data->admission);
}


bool
cluster_undo_block0_current_live_owner_publication_recheck_conditional(
	const ClusterUndoBlock0LiveOwnerPublication *publication)
{
	const ClusterUndoBlock0LiveOwnerPublicationData *data;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0Generation generation = { false, 0 };
	ClusterUndoBlock0Result result;
	bool root_resolved;

	if (publication == NULL)
		return false;
	data = (const ClusterUndoBlock0LiveOwnerPublicationData *)(const void *)publication;
	if (data->magic != CLUSTER_UNDO_BLOCK0_LIVE_OWNER_PUBLICATION_MAGIC || data->reserved != 0
		|| !cluster_semantic_activation_recheck(&data->admission))
		return false;

	memset(&root, 0, sizeof(root));
	root_resolved
		= data->admission.side == CLUSTER_SEMANTIC_SOURCE_SIDE
			  ? cluster_semantic_activation_resolve_shared_undo_root_live_owner_source(
					&data->admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
					data->item.logical.owner_instance, data->item.logical.segment_id, &root)
		  : data->admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
			  ? cluster_semantic_activation_resolve_shared_undo_root(
					&data->admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
					data->item.logical.owner_instance, data->item.logical.segment_id, &root)
			  : false;
	if (!root_resolved || !cluster_undo_block0_root_matches(&root, &data->item.resolved_root))
		return false;

	result = cluster_undo_block0_sample_resident_generation_conditional(
		&data->item.logical, &data->item.resolved_root, &data->item.proof, &generation);
	if (result != CLUSTER_UNDO_BLOCK0_OK
		|| !cluster_undo_block0_generation_matches(&generation, &data->item.generation))
		return false;
	return cluster_semantic_activation_recheck(&data->admission);
}
