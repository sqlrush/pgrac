/*-------------------------------------------------------------------------
 * cluster_control_request.c -- shared control-request cleanup responsibility.
 * This registry never grants a lock. The GRD remains the sole authority.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_control_request.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_retention.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

typedef struct ControlRequestShared {
	LWLock lock;
	int tranche;
	uint64 sequence;
	uint64 driver;
	uint64 revision;
	uint64 registered;
	uint64 abandoned;
	uint64 claimed;
	uint64 acknowledged;
	uint64 forgotten;
	uint64 refused;
	uint64 stale;
	ClusterControlRequestView records[CLUSTER_CONTROL_REQUEST_CAPACITY];
} ControlRequestShared;

static ControlRequestShared *control_requests;

bool
cluster_control_request_resid_valid(const ClusterResId *resid)
{
	if (resid == NULL || resid->lockmethodid != DEFAULT_LOCKMETHOD || resid->field4 != 0)
		return false;
	if (resid->type == CLUSTER_CF_RESID_TYPE)
		return resid->field1 == 0 && resid->field2 == 0 && resid->field3 == 0;
	if (resid->type == CLUSTER_WAL_RETENTION_RESID_TYPE)
		return resid->field1 > 0 && resid->field1 <= CLUSTER_WAL_RETENTION_MAX_THREADS
			   && resid->field2 == 0 && resid->field3 == 0;
	if (resid->type == CLUSTER_IR_RESID_TYPE)
		return resid->field1 > 0 && resid->field1 <= CLUSTER_WAL_RETENTION_MAX_THREADS
			   && (resid->field2 != 0 || resid->field3 != 0);
	return false;
}

static bool
control_key_valid(const ClusterControlRequestKey *key)
{
	return key != NULL && cluster_control_request_resid_valid(&key->resid)
		   && key->holder.node_id < CLUSTER_MAX_NODES && key->holder.cluster_epoch != 0
		   && key->holder.request_id != 0;
}

static bool
control_key_equal(const ClusterControlRequestKey *a, const ClusterControlRequestKey *b)
{
	return memcmp(&a->resid, &b->resid, sizeof(a->resid)) == 0
		   && a->holder.node_id == b->holder.node_id && a->holder.procno == b->holder.procno
		   && a->holder.cluster_epoch == b->holder.cluster_epoch
		   && a->holder.request_id == b->holder.request_id;
}

static bool
control_previous_valid(uint64 request, uint64 previous, LOCKMODE mode)
{
	return previous == 0
			   ? mode == NoLock
			   : previous != request && mode >= AccessShareLock && mode <= AccessExclusiveLock;
}

static bool
control_message_valid(const ClusterControlRetireMessage *message)
{
	return message != NULL && control_key_valid(&message->key) && message->exchange_id != 0
		   && message->cleanup_epoch >= message->key.holder.cluster_epoch
		   && message->verb >= CLUSTER_CONTROL_RETIRE
		   && message->verb <= CLUSTER_CONTROL_RETIRE_INVALID
		   && control_previous_valid(message->key.holder.request_id, message->previous_request,
									 message->previous_mode);
}

static void
control_put_le(uint8 *out, uint64 value, int nbytes)
{
	int i;

	for (i = 0; i < nbytes; i++)
		out[i] = (uint8)(value >> (8 * i));
}

static uint64
control_get_le(const uint8 *in, int nbytes)
{
	uint64 value = 0;
	int i;

	for (i = 0; i < nbytes; i++)
		value |= (uint64)in[i] << (8 * i);
	return value;
}

bool
cluster_control_retire_encode(const ClusterControlRetireMessage *message,
							  uint8 out[CLUSTER_CONTROL_RETIRE_BYTES])
{
	const ClusterGrdHolderId *holder;
	const ClusterResId *resid;

	if (out == NULL || !control_message_valid(message))
		return false;
	holder = &message->key.holder;
	resid = &message->key.resid;
	memset(out, 0, CLUSTER_CONTROL_RETIRE_BYTES);
	control_put_le(out, CLUSTER_CONTROL_RETIRE_OPCODE, 4);
	control_put_le(out + 4, message->verb, 4);
	control_put_le(out + 8, holder->node_id, 4);
	control_put_le(out + 12, holder->procno, 4);
	control_put_le(out + 16, holder->cluster_epoch, 8);
	control_put_le(out + 24, holder->request_id, 8);
	control_put_le(out + 32, resid->field1, 4);
	control_put_le(out + 36, resid->field2, 4);
	control_put_le(out + 40, resid->field3, 4);
	control_put_le(out + 44, resid->field4, 2);
	out[46] = resid->type;
	out[47] = resid->lockmethodid;
	control_put_le(out + 48, message->cleanup_epoch, 8);
	control_put_le(out + 56, message->exchange_id, 8);
	control_put_le(out + 64, message->previous_request, 8);
	control_put_le(out + 72, message->previous_mode, 4);
	control_put_le(out + 76, 1, 4);
	return true;
}

bool
cluster_control_retire_decode(const void *bytes, Size length, ClusterControlRetireMessage *out)
{
	const uint8 *in = bytes;
	ClusterControlRetireMessage message;

	if (bytes == NULL || out == NULL || length != CLUSTER_CONTROL_RETIRE_BYTES
		|| control_get_le(in, 4) != CLUSTER_CONTROL_RETIRE_OPCODE
		|| control_get_le(in + 76, 4) != 1)
		return false;
	memset(&message, 0, sizeof(message));
	message.verb = (ClusterControlRetireVerb)control_get_le(in + 4, 4);
	message.key.holder.node_id = (uint32)control_get_le(in + 8, 4);
	message.key.holder.procno = (uint32)control_get_le(in + 12, 4);
	message.key.holder.cluster_epoch = control_get_le(in + 16, 8);
	message.key.holder.request_id = control_get_le(in + 24, 8);
	message.key.resid.field1 = (uint32)control_get_le(in + 32, 4);
	message.key.resid.field2 = (uint32)control_get_le(in + 36, 4);
	message.key.resid.field3 = (uint32)control_get_le(in + 40, 4);
	message.key.resid.field4 = (uint16)control_get_le(in + 44, 2);
	message.key.resid.type = in[46];
	message.key.resid.lockmethodid = in[47];
	message.cleanup_epoch = control_get_le(in + 48, 8);
	message.exchange_id = control_get_le(in + 56, 8);
	message.previous_request = control_get_le(in + 64, 8);
	message.previous_mode = (LOCKMODE)control_get_le(in + 72, 4);
	if (!control_message_valid(&message))
		return false;
	*out = message;
	return true;
}

Size
cluster_control_request_shmem_size(void)
{
	return sizeof(ControlRequestShared);
}

void
cluster_control_request_shmem_init(void)
{
	bool found;

	control_requests
		= ShmemInitStruct("pgrac control requests", cluster_control_request_shmem_size(), &found);
	if (!found) {
		memset(control_requests, 0, sizeof(*control_requests));
		control_requests->revision = 1;
		control_requests->tranche = LWLockNewTrancheId();
		LWLockInitialize(&control_requests->lock, control_requests->tranche);
	}
	LWLockRegisterTranche(control_requests->tranche, "ClusterControlRequest");
}

void
cluster_control_request_shmem_register(void)
{
	static const ClusterShmemRegion region = { "pgrac control requests",
											   cluster_control_request_shmem_size,
											   cluster_control_request_shmem_init,
											   1,
											   "ges",
											   0 };

	cluster_shmem_register_region(&region);
}

/* All helpers below operate under the one registry lock. No GRD, network,
 * callbacks, allocations or other lock acquisitions are permitted inside it. */
static uint64
control_next_id(void)
{
	if (control_requests->sequence == UINT64_MAX)
		return 0;
	return ++control_requests->sequence;
}

/* Registry lock held. Zero is a permanently unprovable wrap, never ABA. */
static void
control_changed(void)
{
	if (control_requests->revision != 0)
		control_requests->revision++;
}

static bool
control_owner_valid(const ClusterControlRequestOwner *owner)
{
	return owner != NULL && owner->incarnation != 0 && owner->pid > 0;
}

static bool
control_owner_equal(const ClusterControlRequestOwner *a, const ClusterControlRequestOwner *b)
{
	return a != NULL && b != NULL && a->incarnation == b->incarnation && a->procno == b->procno
		   && a->pid == b->pid;
}

static bool
control_cut_valid(const ClusterControlRequestCut *cut)
{
	return cut != NULL && cut->epoch != 0 && cut->generation != 0 && cut->master >= 0
		   && cut->master < CLUSTER_MAX_NODES;
}

static bool
control_cut_equal(const ClusterControlRequestCut *a, const ClusterControlRequestCut *b)
{
	return a->epoch == b->epoch && a->generation == b->generation && a->master == b->master;
}

static ClusterControlRequestView *
control_record(const ClusterControlRequestHandle *handle)
{
	ClusterControlRequestView *record;

	if (handle == NULL || handle->generation == 0
		|| handle->slot >= CLUSTER_CONTROL_REQUEST_CAPACITY)
		return NULL;
	record = &control_requests->records[handle->slot];
	return record->state != CLUSTER_CONTROL_REQUEST_FREE
				   && record->handle.generation == handle->generation
			   ? record
			   : NULL;
}

static ClusterControlRequestView *
control_find(const ClusterControlRequestKey *key)
{
	uint32 i;

	for (i = 0; i < CLUSTER_CONTROL_REQUEST_CAPACITY; i++) {
		ClusterControlRequestView *record = &control_requests->records[i];

		if (record->state != CLUSTER_CONTROL_REQUEST_FREE
			&& control_key_equal(&record->message.key, key))
			return record;
	}
	return NULL;
}

bool
cluster_control_request_owner_init(uint32 procno, int32 pid, ClusterControlRequestOwner *out)
{
	uint64 incarnation;

	if (control_requests == NULL || out == NULL || pid <= 0)
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	incarnation = control_next_id();
	LWLockRelease(&control_requests->lock);
	if (incarnation == 0)
		return false;
	out->incarnation = incarnation;
	out->procno = procno;
	out->pid = pid;
	return true;
}

bool
cluster_control_request_register(const ClusterControlRequestKey *key, LOCKMODE mode,
								 const ClusterControlRequestOwner *owner, uint64 previous_request,
								 LOCKMODE previous_mode, ClusterControlRequestHandle *out)
{
	ClusterControlRequestView *record = NULL;
	uint32 i;
	bool result = false;

	if (control_requests == NULL || out == NULL || !control_key_valid(key)
		|| !control_owner_valid(owner) || key->holder.procno != owner->procno
		|| mode < AccessShareLock || mode > AccessExclusiveLock
		|| !control_previous_valid(key->holder.request_id, previous_request, previous_mode))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	/* One identity has one creator and one cleanup owner. No private copy
	 * can re-register an abandoned identity back into ACTIVE. */
	if (control_find(key) == NULL) {
		for (i = 0; i < CLUSTER_CONTROL_REQUEST_CAPACITY; i++) {
			if (control_requests->records[i].state == CLUSTER_CONTROL_REQUEST_FREE) {
				record = &control_requests->records[i];
				break;
			}
		}
	}
	if (record != NULL) {
		uint64 generation = control_next_id();

		if (generation != 0) {
			memset(record, 0, sizeof(*record));
			record->handle.generation = generation;
			record->handle.slot = i;
			record->owner = *owner;
			record->mode = mode;
			record->message.key = *key;
			record->message.previous_request = previous_request;
			record->message.previous_mode = previous_mode;
			record->message.verb = CLUSTER_CONTROL_RETIRE;
			record->state = CLUSTER_CONTROL_REQUEST_ACTIVE;
			*out = record->handle;
			control_requests->registered++;
			control_changed();
			result = true;
		}
	}
	if (!result)
		control_requests->refused++;
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_mark_held(const ClusterControlRequestHandle *handle,
								  const ClusterControlRequestOwner *owner)
{
	ClusterControlRequestView *record;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	record = control_record(handle);
	if (record != NULL && control_owner_equal(&record->owner, owner)
		&& (record->state == CLUSTER_CONTROL_REQUEST_ACTIVE
			|| record->state == CLUSTER_CONTROL_REQUEST_HELD)) {
		record->state = CLUSTER_CONTROL_REQUEST_HELD;
		control_changed();
		result = true;
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

static void
control_declare_debt(ClusterControlRequestView *record)
{
	record->state = CLUSTER_CONTROL_REQUEST_ABANDONED;
	record->driver = 0;
	record->message.exchange_id = 0;
	record->message.cleanup_epoch = 0;
	memset(&record->cut, 0, sizeof(record->cut));
	control_requests->abandoned++;
	control_changed();
}

bool
cluster_control_request_abandon(const ClusterControlRequestHandle *handle,
								const ClusterControlRequestOwner *owner)
{
	ClusterControlRequestView *record;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	record = control_record(handle);
	if (record != NULL && control_owner_equal(&record->owner, owner)) {
		if (record->state == CLUSTER_CONTROL_REQUEST_ACTIVE
			|| record->state == CLUSTER_CONTROL_REQUEST_HELD
			|| record->state == CLUSTER_CONTROL_REQUEST_QUIESCED)
			control_declare_debt(record);
		result = true;
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_pause_upgrade(const ClusterControlRequestHandle *original,
									  const ClusterControlRequestOwner *owner)
{
	ClusterControlRequestView *record;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	record = control_record(original);
	if (record != NULL && control_owner_equal(&record->owner, owner) && !record->owner_exited
		&& record->message.key.resid.type == CLUSTER_WAL_RETENTION_RESID_TYPE
		&& record->state == CLUSTER_CONTROL_REQUEST_HELD && record->mode == ShareLock
		&& record->message.previous_request == 0) {
		record->state = CLUSTER_CONTROL_REQUEST_QUIESCED;
		control_changed();
		result = true;
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

/* The original known S and this X attempt are distinct, exact identities.
 * Called only with the fixed registry lock held. No GRD or I/O in this lock. */
static bool
control_upgrade_pair(const ClusterControlRequestView *old, const ClusterControlRequestView *next,
					 const ClusterControlRequestOwner *owner)
{
	return old != NULL && next != NULL && old != next && control_owner_equal(&old->owner, owner)
		   && control_owner_equal(&next->owner, owner) && !old->owner_exited && !next->owner_exited
		   && old->state == CLUSTER_CONTROL_REQUEST_QUIESCED && old->mode == ShareLock
		   && next->mode == ExclusiveLock
		   && old->message.key.resid.type == CLUSTER_WAL_RETENTION_RESID_TYPE
		   && memcmp(&old->message.key.resid, &next->message.key.resid, sizeof(ClusterResId)) == 0
		   && old->message.key.holder.node_id == next->message.key.holder.node_id
		   && old->message.key.holder.procno == next->message.key.holder.procno
		   && old->message.key.holder.cluster_epoch == next->message.key.holder.cluster_epoch
		   && next->message.previous_request == old->message.key.holder.request_id
		   && next->message.previous_mode == ShareLock;
}

bool
cluster_control_request_rebuild_upgrade_begin(const ClusterControlRequestHandle *original,
											  const ClusterControlRequestHandle *failed,
											  const ClusterControlRequestOwner *owner)
{
	ClusterControlRequestView *old, *next;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	old = control_record(original);
	next = control_record(failed);
	if (control_upgrade_pair(old, next, owner)
		&& (next->state == CLUSTER_CONTROL_REQUEST_ABANDONED
			|| next->state == CLUSTER_CONTROL_REQUEST_TERMINAL
			|| next->state == CLUSTER_CONTROL_REQUEST_INVALID)) {
		/* Invalidate an old restore-S exchange. Keep S QUIESCED while the
		 * failed X is retired without creating S on an empty new master.
		 * The driver may already have received INVALID from that empty new
		 * directory. It is not terminal: the frozen-cut caller changes the
		 * intent and must obtain a new exact, no-restore retirement ACK. */
		next->rebuild_previous_request = old->message.key.holder.request_id;
		next->message.previous_request = 0;
		next->message.previous_mode = NoLock;
		control_declare_debt(next);
		result = true;
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_rebuild_upgrade_finish(const ClusterControlRequestHandle *original,
											   const ClusterControlRequestHandle *failed,
											   const ClusterControlRequestOwner *owner,
											   const ClusterControlRequestCut *cut)
{
	ClusterControlRequestView *old, *next;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner) || !control_cut_valid(cut))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	old = control_record(original);
	next = control_record(failed);
	if (old != NULL && next != NULL && old != next && control_owner_equal(&old->owner, owner)
		&& control_owner_equal(&next->owner, owner) && !old->owner_exited && !next->owner_exited
		&& old->state == CLUSTER_CONTROL_REQUEST_QUIESCED && old->mode == ShareLock
		&& old->message.key.resid.type == CLUSTER_WAL_RETENTION_RESID_TYPE
		&& next->mode == ExclusiveLock && next->message.previous_request == 0
		&& next->message.previous_mode == NoLock && next->state == CLUSTER_CONTROL_REQUEST_TERMINAL
		&& next->rebuild_previous_request == old->message.key.holder.request_id
		&& memcmp(&old->message.key.resid, &next->message.key.resid, sizeof(ClusterResId)) == 0
		&& old->message.key.holder.node_id == next->message.key.holder.node_id
		&& old->message.key.holder.procno == next->message.key.holder.procno
		&& old->message.key.holder.cluster_epoch == next->message.key.holder.cluster_epoch
		&& control_cut_equal(&next->cut, cut)) {
		/* This is the original known S at its ORIGINAL epoch. The stable
		 * owner must REDECLARE it before either use or census can succeed. */
		old->state = CLUSTER_CONTROL_REQUEST_HELD;
		memset(next, 0, sizeof(*next));
		control_requests->forgotten++;
		control_changed();
		result = true;
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_confirm_upgrade(const ClusterControlRequestHandle *original,
										const ClusterControlRequestHandle *replacement,
										const ClusterControlRequestOwner *owner,
										const ClusterControlRequestCut *cut)
{
	ClusterControlRequestView *old, *next;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner) || !control_cut_valid(cut))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	old = control_record(original);
	next = control_record(replacement);
	if (control_upgrade_pair(old, next, owner) && next->state == CLUSTER_CONTROL_REQUEST_HELD
		&& next->message.key.holder.cluster_epoch == cut->epoch) {
		/* The successful X now owns protection. Normal release/exit must not
		 * resurrect its superseded S; native S bookkeeping is separate. */
		next->message.previous_request = 0;
		next->message.previous_mode = NoLock;
		control_declare_debt(old);
		result = true;
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_restore_upgrade(const ClusterControlRequestHandle *original,
										const ClusterControlRequestHandle *failed,
										const ClusterControlRequestOwner *owner,
										const ClusterControlRequestCut *cut)
{
	ClusterControlRequestView *old, *next;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner) || !control_cut_valid(cut))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	old = control_record(original);
	next = control_record(failed);
	if (control_upgrade_pair(old, next, owner) && next->state == CLUSTER_CONTROL_REQUEST_TERMINAL
		&& control_cut_equal(&next->cut, cut)
		&& old->message.key.holder.cluster_epoch == cut->epoch) {
		old->state = CLUSTER_CONTROL_REQUEST_HELD;
		/* Consume the exact terminal receipt atomically with restoring use.
		 * The caller already reconciled its nondraining GRD shadow. There
		 * must be no interval where a second cleanup revokes this proof. */
		memset(next, 0, sizeof(*next));
		control_requests->forgotten++;
		control_changed();
		result = true;
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_surrender_upgrade(const ClusterControlRequestHandle *original,
										  const ClusterControlRequestHandle *replacement,
										  const ClusterControlRequestOwner *owner)
{
	ClusterControlRequestView *old, *next;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	old = control_record(original);
	next = control_record(replacement);
	if (control_upgrade_pair(old, next, owner)) {
		/* The caller gives up BOTH holds, not just the failed X. Revoke
		 * every previous preserve-S exchange before either record retires.
		 * Accepted old frames precede the new retirements in CONTROL FIFO. */
		next->message.previous_request = 0;
		next->message.previous_mode = NoLock;
		control_declare_debt(old);
		control_declare_debt(next);
		result = true;
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_begin_downgrade(const ClusterControlRequestHandle *handle,
										const ClusterControlRequestOwner *owner)
{
	ClusterControlRequestView *record;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	record = control_record(handle);
	if (record != NULL && control_owner_equal(&record->owner, owner) && !record->owner_exited
		&& record->message.key.resid.type == CLUSTER_WAL_RETENTION_RESID_TYPE
		&& record->state == CLUSTER_CONTROL_REQUEST_HELD && record->mode == ExclusiveLock
		&& record->message.previous_request == 0) {
		/* Accepted old frames precede CONVERT in CONTROL FIFO. Queued old
		 * X frames are rejected by the final identity-and-mode send gate. */
		record->mode = ShareLock;
		record->state = CLUSTER_CONTROL_REQUEST_ACTIVE;
		control_changed();
		result = true;
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

void
cluster_control_request_owner_exit(const ClusterControlRequestOwner *owner)
{
	uint32 i;

	if (control_requests == NULL || !control_owner_valid(owner))
		return;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	for (i = 0; i < CLUSTER_CONTROL_REQUEST_CAPACITY; i++) {
		ClusterControlRequestView *record = &control_requests->records[i];

		if (record->state == CLUSTER_CONTROL_REQUEST_FREE
			|| !control_owner_equal(&record->owner, owner) || record->owner_exited)
			continue;
		record->owner_exited = true;
		record->message.previous_request = 0;
		record->message.previous_mode = NoLock;
		/* Revoke any in-flight preserve-S certificate. All of this creator's
		 * separately registered identities now belong to exit cleanup. */
		control_declare_debt(record);
	}
	LWLockRelease(&control_requests->lock);
}

static bool
control_send_allowed(const ClusterControlRequestKey *key, LOCKMODE mode)
{
	ClusterControlRequestView *record;
	bool allowed;

	if (control_requests == NULL || !control_key_valid(key))
		return false;
	LWLockAcquire(&control_requests->lock, LW_SHARED);
	record = control_find(key);
	allowed = record != NULL && (mode == NoLock || record->mode == mode)
			  && (record->state == CLUSTER_CONTROL_REQUEST_ACTIVE
				  || record->state == CLUSTER_CONTROL_REQUEST_HELD);
	LWLockRelease(&control_requests->lock);
	return allowed;
}

bool
cluster_control_request_send_allowed(const ClusterControlRequestKey *key)
{
	return control_send_allowed(key, NoLock);
}

bool
cluster_control_request_send_mode_allowed(const ClusterControlRequestKey *key, LOCKMODE mode)
{
	return mode >= AccessShareLock && mode <= AccessExclusiveLock
		   && control_send_allowed(key, mode);
}

uint64
cluster_control_request_driver_start(void)
{
	uint64 driver;

	if (control_requests == NULL)
		return 0;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	driver = control_next_id();
	/* Exhaustion revokes the previous claim too; it cannot preserve an old
	 * actor's ability to accept ACKs after a failed restart. */
	control_requests->driver = driver;
	LWLockRelease(&control_requests->lock);
	return driver;
}

bool
cluster_control_request_claim(const ClusterControlRequestHandle *handle, uint64 driver,
							  const ClusterControlRequestCut *cut, ClusterControlRetireMessage *out)
{
	ClusterControlRequestView *record;
	bool result = false;

	if (control_requests == NULL || driver == 0 || out == NULL || !control_cut_valid(cut))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	record = control_record(handle);
	if (record == NULL || control_requests->driver != driver
		|| cut->epoch < record->message.key.holder.cluster_epoch)
		goto done;
	if (record->state == CLUSTER_CONTROL_REQUEST_TERMINAL && !control_cut_equal(&record->cut, cut))
		control_declare_debt(record);
	if (record->state != CLUSTER_CONTROL_REQUEST_ABANDONED)
		goto done;
	if (record->driver != driver || !control_cut_equal(&record->cut, cut)
		|| record->message.exchange_id == 0) {
		uint64 exchange = control_next_id();

		if (exchange == 0)
			goto done;
		record->driver = driver;
		record->cut = *cut;
		record->message.cleanup_epoch = cut->epoch;
		record->message.exchange_id = exchange;
		control_requests->claimed++;
	}
	*out = record->message;
	result = true;
done:
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_ack_owner(const ClusterControlRetireMessage *message, int32 source,
								  uint64 driver, const ClusterControlRequestCut *cut,
								  ClusterControlRequestOwner *notify_owner)
{
	ClusterControlRequestView *record;
	bool result = false;

	if (notify_owner != NULL)
		memset(notify_owner, 0, sizeof(*notify_owner));
	if (control_requests == NULL || !control_message_valid(message)
		|| message->verb == CLUSTER_CONTROL_RETIRE || !control_cut_valid(cut) || driver == 0
		|| source != cut->master)
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	record = control_find(&message->key);
	if (record != NULL && control_requests->driver == driver && record->driver == driver
		&& record->state == CLUSTER_CONTROL_REQUEST_ABANDONED
		&& control_cut_equal(&record->cut, cut)
		&& message->cleanup_epoch == record->message.cleanup_epoch
		&& message->exchange_id == record->message.exchange_id
		&& message->previous_request == record->message.previous_request
		&& message->previous_mode == record->message.previous_mode) {
		if (message->verb == CLUSTER_CONTROL_RETIRED)
			record->state = CLUSTER_CONTROL_REQUEST_TERMINAL;
		else if (message->verb == CLUSTER_CONTROL_RETIRE_INVALID)
			record->state = CLUSTER_CONTROL_REQUEST_INVALID;
		control_requests->acknowledged++;
		control_changed();
		if (notify_owner != NULL && !record->owner_exited
			&& message->verb == CLUSTER_CONTROL_RETIRED)
			*notify_owner = record->owner;
		result = true;
	} else
		control_requests->stale++;
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_ack(const ClusterControlRetireMessage *message, int32 source, uint64 driver,
							const ClusterControlRequestCut *cut)
{
	return cluster_control_request_ack_owner(message, source, driver, cut, NULL);
}

bool
cluster_control_request_forget(const ClusterControlRequestHandle *handle,
							   const ClusterControlRequestOwner *owner,
							   const ClusterControlRequestCut *cut)
{
	ClusterControlRequestView *record;
	bool result = false;

	if (control_requests == NULL || !control_owner_valid(owner) || !control_cut_valid(cut))
		return false;
	LWLockAcquire(&control_requests->lock, LW_EXCLUSIVE);
	record = control_record(handle);
	if (record != NULL && control_owner_equal(&record->owner, owner)
		&& record->state == CLUSTER_CONTROL_REQUEST_TERMINAL) {
		if (control_cut_equal(&record->cut, cut)) {
			memset(record, 0, sizeof(*record));
			control_requests->forgotten++;
			control_changed();
			result = true;
		} else
			control_declare_debt(record);
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_snapshot(const ClusterControlRequestHandle *handle,
								 ClusterControlRequestView *out)
{
	ClusterControlRequestView *record;
	bool result;

	if (control_requests == NULL || out == NULL)
		return false;
	LWLockAcquire(&control_requests->lock, LW_SHARED);
	record = control_record(handle);
	result = record != NULL;
	if (result)
		*out = *record;
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_next(uint32 *cursor, ClusterControlRequestView *out)
{
	bool result = false;

	if (control_requests == NULL || cursor == NULL || out == NULL)
		return false;
	LWLockAcquire(&control_requests->lock, LW_SHARED);
	while (*cursor < CLUSTER_CONTROL_REQUEST_CAPACITY) {
		ClusterControlRequestView *record = &control_requests->records[(*cursor)++];

		if (record->state != CLUSTER_CONTROL_REQUEST_FREE) {
			*out = *record;
			result = true;
			break;
		}
	}
	LWLockRelease(&control_requests->lock);
	return result;
}

bool
cluster_control_request_census(uint64 epoch, uint64 *version)
{
	bool ready = true;
	uint32 i;

	if (version != NULL)
		*version = 0;
	if (control_requests == NULL || version == NULL)
		return false;
	LWLockAcquire(&control_requests->lock, LW_SHARED);
	if (control_requests->revision == 0)
		ready = false;
	for (i = 0; ready && i < CLUSTER_CONTROL_REQUEST_CAPACITY; ++i) {
		const ClusterControlRequestView *record = &control_requests->records[i];

		/* Initial recovery formation uses epoch zero. It may prove an empty
		 * registry, never a HELD request: control_key_valid still forbids
		 * acquiring at epoch zero. Keep the revision proof across PGPROC. */
		if (record->state != CLUSTER_CONTROL_REQUEST_FREE
			&& (epoch == 0 || record->owner_exited || record->state != CLUSTER_CONTROL_REQUEST_HELD
				|| record->message.key.holder.cluster_epoch != epoch))
			ready = false;
	}
	if (ready)
		*version = control_requests->revision;
	LWLockRelease(&control_requests->lock);
	return ready;
}

bool
cluster_control_request_census_unchanged(uint64 epoch, uint64 version)
{
	uint64 current;

	return version != 0 && cluster_control_request_census(epoch, &current) && current == version;
}

bool
cluster_control_request_empty(void)
{
	bool empty = true;
	uint32 i;

	if (control_requests == NULL)
		return false;
	LWLockAcquire(&control_requests->lock, LW_SHARED);
	for (i = 0; i < CLUSTER_CONTROL_REQUEST_CAPACITY; ++i)
		if (control_requests->records[i].state != CLUSTER_CONTROL_REQUEST_FREE) {
			empty = false;
			break;
		}
	LWLockRelease(&control_requests->lock);
	return empty;
}
