/* Control-selected shared identities; local borrowing never selects a file. */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "storage/lwlock.h"
#include "cluster/cluster_control_registry_batch.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_recovery_format.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_wal_thread.h"
#include "common/pgrac_initdb_recovery.h"

typedef struct NativeFileBank {
	ClusterControlNativeTableInfoV2 info;
	ClusterRecoveryFileIdentityV2 files[CLUSTER_NATIVE_FILES_CAPACITY_V2];
	ClusterRecoveryFileReservationV1 reservations[CLUSTER_NATIVE_RESERVATIONS_CAPACITY_V2];
} NativeFileBank;
typedef struct NativeFileShared {
	slock_t lock;
	bool publishing;
	unsigned active;
	uint64 borrowed;
	NativeFileBank banks[2];
} NativeFileShared;
static NativeFileShared *native_files;

static bool
native_distinct(const void *p, Size n)
{
	uintptr_t a = (uintptr_t)p, b = (uintptr_t)native_files;
	return p && a <= UINTPTR_MAX - n
		   && (!native_files || a + n <= b || b + sizeof(*native_files) <= a);
}
static int
native_key_compare(const void *left, const void *right)
{
	const ClusterRecoveryFileIdentityV2 *a = left, *b = right;
#define CMP(f)                                                                                     \
	do {                                                                                           \
		if (a->f != b->f)                                                                          \
			return a->f < b->f ? -1 : 1;                                                           \
	} while (0)
	CMP(kind);
	CMP(origin);
	CMP(database_oid);
	CMP(tablespace_oid);
	CMP(object_id);
#undef CMP
	return 0;
}
static bool
native_same(const ClusterRecoveryFileIdentityV2 *a, const ClusterRecoveryFileIdentityV2 *b)
{
	uint8 wire[128];
	return cluster_recovery_file_identity_encode_v2(a, wire, sizeof(wire))
		   && cluster_recovery_file_identity_matches_v2(wire, sizeof(wire), b);
}
static int
native_find(const NativeFileBank *bank, const ClusterRecoveryFileIdentityV2 *key)
{
	uint64 lo = 0, hi = bank->info.count;
	while (lo < hi) {
		uint64 mid = lo + (hi - lo) / 2;
		int cmp = native_key_compare(key, &bank->files[mid]);
		if (!cmp)
			return (int)mid;
		if (cmp < 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	return -1;
}
static int
native_reserved_find(const NativeFileBank *bank, const ClusterRecoveryFileIdentityV2 *key)
{
	uint64 lo = 0, hi = bank->info.reservation_count;
	while (lo < hi) {
		uint64 mid = lo + (hi - lo) / 2;
		int cmp = native_key_compare(key, &bank->reservations[mid].expected);
		if (!cmp)
			return (int)mid;
		if (cmp < 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	return -1;
}
static bool
native_reservation_same(const ClusterRecoveryFileReservationV1 *a,
						const ClusterRecoveryFileReservationV1 *b)
{
	return native_same(&a->expected, &b->expected) && !memcmp(a->batch_uuid, b->batch_uuid, 16)
		   && cluster_control_root_identity_equal(&a->creation_owner, &b->creation_owner)
		   && a->owner_generation == b->owner_generation && a->state == b->state;
}
static bool
native_canonical(NativeFileBank *bank)
{
	uint64 count = 0;
	qsort(bank->files, bank->info.count, sizeof(bank->files[0]), native_key_compare);
	for (uint64 i = 0; i < bank->info.count; i++) {
		uint8 wire[128];
		if (!cluster_recovery_file_identity_encode_v2(&bank->files[i], wire, sizeof(wire)))
			return false;
		if (count && !native_key_compare(&bank->files[count - 1], &bank->files[i])) {
			if (!native_same(&bank->files[count - 1], &bank->files[i]))
				return false;
		} else
			bank->files[count++] = bank->files[i];
	}
	bank->info.count = count;
	return count != 0;
}
static ClusterControlNativeResultV2
native_range(const NativeFileBank *bank, uint16 origin, uint64 first, uint64 last)
{
	ClusterRecoveryFileIdentityV2 key = { 0 };
	if (bank->info.mode == CLUSTER_NATIVE_FILES_UNKNOWN_V2 || origin >= 128 || last < first)
		return CLUSTER_NATIVE_FILES_REFUSED_V2;
	if (last - first >= CLUSTER_NATIVE_FILES_CAPACITY_V2)
		return CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2;
	key.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	key.origin = origin;
	for (uint64 i = first;; i++) {
		key.object_id = i;
		if (native_find(bank, &key) < 0 && native_reserved_find(bank, &key) < 0)
			return CLUSTER_NATIVE_FILES_NEEDS_REGISTRATION_V2;
		if (i == last)
			break;
	}
	return CLUSTER_NATIVE_FILES_OK_V2;
}

/* The inactive bank is private to this control attempt. Readers continue using
 * the active bank. Publication only swaps a bank index under the short lock;
 * no vector copy, ROOT I/O, allocation or family-lock wait occurs there. */
static ClusterControlNativeResultV2
native_install(NativeFileBank *next, ClusterControlRelmapSelectionV2 *selection,
			   const PgracInitdbRecoveryContext *creation)
{
	NativeFileBank *old;
	volatile ClusterControlNativeResultV2 result = CLUSTER_NATIVE_FILES_REFUSED_V2;
	unsigned target;
	uint64 generation;
	if (!native_files)
		return result;
	SpinLockAcquire(&native_files->lock);
	if (native_files->publishing) {
		SpinLockRelease(&native_files->lock);
		return CLUSTER_NATIVE_FILES_BUSY_V2;
	}
	native_files->publishing = true;
	old = &native_files->banks[native_files->active];
	target = 1 - native_files->active;
	generation = old->info.table_generation;
	SpinLockRelease(&native_files->lock);
	PG_TRY();
	{
		bool same
			= generation && old->info.mode == next->info.mode
			  && old->info.startup_generation == next->info.startup_generation
			  && old->info.registry_byte_budget == next->info.registry_byte_budget
			  && !memcmp(&old->info.control, &next->info.control, sizeof(next->info.control))
			  && !memcmp(&old->info.creation, &next->info.creation, sizeof(next->info.creation))
			  && old->info.count == next->info.count
			  && old->info.reservation_count == next->info.reservation_count;
		bool compatible = generation != UINT64_MAX;
		if (generation
			&& (old->info.mode != next->info.mode
				|| old->info.startup_generation != next->info.startup_generation))
			compatible = false;
		/* Reidentity/deletion needs the original cross-instance stop/drain
		 * transition. Until connected, neither an epoch bump nor local zero
		 * borrowers can authorize it. Distinct additions remain possible. */
		for (uint64 i = 0; compatible && i < old->info.count; i++) {
			int at = native_find(next, &old->files[i]);
			if (at < 0 || !native_same(&old->files[i], &next->files[at]))
				compatible = false;
		}
		/* Local zero borrowers alone does not authorize removal/reidentity. An
   * original same-identity COMPLETE may move a reservation to AVAILABLE. */
		for (uint64 i = 0; compatible && i < old->info.reservation_count; i++) {
			const ClusterRecoveryFileReservationV1 *r = &old->reservations[i];
			int at = native_reserved_find(next, &r->expected);
			if (at >= 0) {
				if (!native_reservation_same(r, &next->reservations[at]))
					compatible = false;
			} else {
				at = native_find(next, &r->expected);
				if (at < 0 || !native_same(&r->expected, &next->files[at]))
					compatible = false;
			}
		}

		if (compatible) {
			next->info.table_generation = same ? generation : generation + 1;
			next->info.capacity = CLUSTER_NATIVE_FILES_CAPACITY_V2;
			next->info.shared_bytes = sizeof(*native_files);
			native_files->banks[target] = *next;
			if ((selection
				 && cluster_control_root_relmap_selection_recheck_locked_v4(selection)
						== CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				|| (creation && creation == cluster_wal_thread_initdb_recovery_context())) {
				SpinLockAcquire(&native_files->lock);
				if (same)
					result = CLUSTER_NATIVE_FILES_OK_V2;
				else if (native_files->borrowed)
					result = CLUSTER_NATIVE_FILES_BUSY_V2;
				else {
					native_files->active = target;
					result = CLUSTER_NATIVE_FILES_OK_V2;
				}
				SpinLockRelease(&native_files->lock);
			}
		}
	}
	PG_FINALLY();
	{
		SpinLockAcquire(&native_files->lock);
		native_files->publishing = false;
		SpinLockRelease(&native_files->lock);
	}
	PG_END_TRY();
	return result;
}
static Size
native_size(void)
{
	return MAXALIGN(sizeof(NativeFileShared));
}
static void
native_init(void)
{
	bool found;
	const PgracInitdbRecoveryContext *creation;
	native_files = ShmemInitStruct("pgrac native file identities", native_size(), &found);
	if (!found) {
		memset(native_files, 0, sizeof(*native_files));
		SpinLockInit(&native_files->lock);
	}
	/* The getter exists only after the ORIGINAL closed creator pipe has been
	 * consumed and checked. A copied request or an unpublished ROOT is not a
	 * production table source. This child retains its original batch custody. */
	creation = cluster_wal_thread_initdb_recovery_context();
	if (creation) {
		NativeFileBank *next = palloc0(sizeof(*next));
		PG_TRY();
		{
			unsigned relmaps = 4;
			if ((creation->cohort[0] || creation->cohort[1]) && creation->cohort_origin != 0)
				relmaps = 0;
			next->info.mode = CLUSTER_NATIVE_FILES_CREATION_V2;
			next->info.startup_generation = creation->creator_pid;
			next->info.creation = *creation;
			next->info.registry_byte_budget = CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2;
			next->info.count = relmaps + creation->boot_file_count;
			if ((relmaps && !pgrac_initdb_recovery_relmaps_v2(creation, next->files)) ||
				!pgrac_initdb_recovery_boot_v2(creation, next->files + relmaps) ||
				!native_canonical(next) || native_install(next, NULL, creation) != 0)
				ereport(FATAL, (errmsg("cannot install original native creation file identities")));
		}
		PG_FINALLY();
		{
			pfree(next);
		}
		PG_END_TRY();
	}
}
static const ClusterShmemRegion native_region = { .name = "pgrac native file identities",
												  .size_fn = native_size,
												  .init_fn = native_init,
												  .lwlock_count = 0,
												  .owner_subsys = "cluster_control_native_files",
												  .reserved_flags = 0 };
void
cluster_control_native_files_shmem_register(void)
{
	cluster_shmem_register_region(&native_region);
}

/* The original initdb child skips the online cluster registry. Its closed
 * creator request is already authenticated before PG sizes shared memory.
 * Reserve/init only this existing table; never enable an online node here. */
static bool
native_creation_child(void)
{
	if (cluster_wal_thread_initdb_recovery_context() == NULL)
		return false;
	if (IsUnderPostmaster || IsPostmasterEnvironment
		|| MyBackendType != B_STANDALONE_BACKEND || cluster_node_id != -1
		|| !(IsBootstrapProcessingMode() || IsInitProcessingMode()))
		ereport(FATAL, (errmsg("native creation table requires the original standalone child")));
	return true;
}

Size
cluster_control_native_files_creation_shmem_size_v2(void)
{
	return native_creation_child() ? native_size() : 0;
}

void
cluster_control_native_files_creation_shmem_init_v2(void)
{
	if (native_creation_child())
		native_init();
}

ClusterControlNativeModeV2
cluster_control_native_files_mode_v2(void)
{
	ClusterControlNativeModeV2 mode;
	if (!native_files)
		return CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	SpinLockAcquire(&native_files->lock);
	mode = native_files->banks[native_files->active].info.mode;
	SpinLockRelease(&native_files->lock);
	return mode;
}
bool
cluster_control_native_files_info_v2(ClusterControlNativeTableInfoV2 *out)
{
	if (!native_files || !native_distinct(out, sizeof(*out)))
		return false;
	SpinLockAcquire(&native_files->lock);
	*out = native_files->banks[native_files->active].info;
	out->borrowed = native_files->borrowed;
	SpinLockRelease(&native_files->lock);
	return true;
}
static bool
native_inventory_size(const ClusterControlNativeTableInfoV2 *info, Size *bytes, Size *offset)
{
	Size prefix = MAXALIGN(sizeof(ClusterControlNativeInventoryV2));
	if (info->count > CLUSTER_NATIVE_FILES_CAPACITY_V2
		|| info->reservation_count > CLUSTER_NATIVE_RESERVATIONS_CAPACITY_V2)
		return false;
	*offset = prefix + MAXALIGN(info->count * sizeof(ClusterRecoveryFileIdentityV2));
	*bytes = *offset + info->reservation_count * sizeof(ClusterRecoveryFileReservationV1);
	return *bytes <= CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2;
}
/* Caller holds the original short table lock, not CF or a family I/O lock. */
static bool
native_inventory_info_same(const ClusterControlNativeTableInfoV2 *saved)
{
	ClusterControlNativeTableInfoV2 current
		= native_files->banks[native_files->active].info;
	current.borrowed = native_files->borrowed;
	return !native_files->publishing && !memcmp(saved, &current, sizeof(current));
}
ClusterControlNativeResultV2
cluster_control_native_files_inventory_v2(Size budget, ClusterControlNativeInventoryV2 **out)
{
	ClusterControlNativeTableInfoV2 info;
	ClusterControlNativeInventoryV2 *value;
	ClusterControlNativeResultV2 result = CLUSTER_NATIVE_FILES_REFUSED_V2;
	Size bytes, offset;
	if (!native_files || !native_distinct(out, sizeof(*out)) || *out)
		return result;
	SpinLockAcquire(&native_files->lock);
	if (native_files->publishing) {
		SpinLockRelease(&native_files->lock);
		return CLUSTER_NATIVE_FILES_BUSY_V2;
	}
	info = native_files->banks[native_files->active].info;
	info.borrowed = native_files->borrowed;
	SpinLockRelease(&native_files->lock);
	if (info.mode != CLUSTER_NATIVE_FILES_MANAGED_V2 || !info.table_generation
		|| info.table_generation == UINT64_MAX || !info.startup_generation
		|| info.startup_generation == UINT64_MAX)
		return result;
	if (!native_inventory_size(&info, &bytes, &offset) || bytes > budget
		|| budget > CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2)
		return CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2;
	/* No allocation while holding the shared lock. Publication between these
	 * two observations refuses; it does not restart against the new table. */
	value = palloc0(bytes);
	value->info = info;
	value->private_bytes = bytes;
	value->private_self = (uintptr_t)value;
	value->private_pid = MyProcPid;
	value->files = (void *)((char *)value + MAXALIGN(sizeof(*value)));
	value->reservations = (void *)((char *)value + offset);
	SpinLockAcquire(&native_files->lock);
	if (native_inventory_info_same(&info)) {
		NativeFileBank *bank = &native_files->banks[native_files->active];
		memcpy((void *)value->files, bank->files, info.count * sizeof(*value->files));
		memcpy((void *)value->reservations, bank->reservations,
			   info.reservation_count * sizeof(*value->reservations));
		result = CLUSTER_NATIVE_FILES_OK_V2;
	}
	SpinLockRelease(&native_files->lock);
	if (result == CLUSTER_NATIVE_FILES_OK_V2)
		*out = value;
	else
		pfree(value);
	return result;
}
bool
cluster_control_native_files_inventory_current_v2(const ClusterControlNativeInventoryV2 *value)
{
	Size bytes, offset;
	bool current = false;
	if (!native_files || !native_distinct(value, sizeof(*value))
		|| value->private_self != (uintptr_t)value || value->private_pid != MyProcPid
		|| value->info.mode != CLUSTER_NATIVE_FILES_MANAGED_V2
		|| !native_inventory_size(&value->info, &bytes, &offset)
		|| value->private_bytes != bytes || (uintptr_t)value > UINTPTR_MAX - bytes
		|| value->files != (void *)((char *)value + MAXALIGN(sizeof(*value)))
		|| value->reservations != (void *)((char *)value + offset))
		return false;
	SpinLockAcquire(&native_files->lock);
	if (native_inventory_info_same(&value->info)) {
		NativeFileBank *bank = &native_files->banks[native_files->active];
		current = !memcmp(value->files, bank->files, value->info.count * sizeof(*value->files))
			&& !memcmp(value->reservations, bank->reservations,
					   value->info.reservation_count * sizeof(*value->reservations));
	}
	SpinLockRelease(&native_files->lock);
	return current;
}
void
cluster_control_native_inventory_destroy_v2(ClusterControlNativeInventoryV2 **out)
{
	ClusterControlNativeInventoryV2 *value;
	if (!native_distinct(out, sizeof(*out)) || !(value = *out)
		|| !native_distinct(value, sizeof(*value)) || value->private_self != (uintptr_t)value
		|| value->private_pid != MyProcPid || value->private_bytes < sizeof(*value)
		|| value->private_bytes > CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2
		|| (uintptr_t)value > UINTPTR_MAX - value->private_bytes
		|| ((uintptr_t)out < (uintptr_t)value + value->private_bytes
			&& (uintptr_t)value < (uintptr_t)out + sizeof(*out)))
		return;
	*out = NULL;
	pfree(value);
}
static bool
native_borrow_valid(const ClusterControlNativeFileBorrowV2 *b)
{
	NativeFileBank *bank = &native_files->banks[native_files->active];
	uint64 at = b->private_index;
	if (b->private_self != (uintptr_t)b || b->private_pid != MyProcPid || !b->table_generation
		|| b->table_generation != bank->info.table_generation
		|| b->startup_generation != bank->info.startup_generation || !native_files->borrowed)
		return false;
	if (at < CLUSTER_NATIVE_FILES_CAPACITY_V2)
		return at < bank->info.count && native_same(&b->expected, &bank->files[at]);
	at -= CLUSTER_NATIVE_FILES_CAPACITY_V2;
	return at < bank->info.reservation_count
		   && bank->reservations[at].state == CLUSTER_RECOVERY_FILE_RESERVED_V1
		   && native_same(&b->expected, &bank->reservations[at].expected);
}

bool
cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
									   ClusterControlNativeFileBorrowV2 *out)
{
	ClusterControlNativeFileBorrowV2 zero = { 0 }, value = { 0 };
	ClusterRecoveryFileIdentityV2 wanted = { 0 };
	NativeFileBank *bank;
	int at;
	bool valid = false;
	if (!native_files || !native_distinct(key, sizeof(*key)) || !native_distinct(out, sizeof(*out))
		|| memcmp(out, &zero, sizeof(zero))
		|| ((uintptr_t)key < (uintptr_t)out + sizeof(*out)
			&& (uintptr_t)out < (uintptr_t)key + sizeof(*key)))
		return false;
	wanted.kind = key->kind;
	wanted.origin = key->origin;
	wanted.database_oid = key->database_oid;
	wanted.tablespace_oid = key->tablespace_oid;
	wanted.object_id = key->object_id;
	SpinLockAcquire(&native_files->lock);
	bank = &native_files->banks[native_files->active];
	at = native_find(bank, &wanted);
	if (bank->info.mode && at >= 0 && native_files->borrowed != UINT64_MAX) {
		value.expected = bank->files[at];
		value.table_generation = bank->info.table_generation;
		value.startup_generation = bank->info.startup_generation;
		value.private_self = (uintptr_t)out;
		value.private_pid = MyProcPid;
		value.private_index = at;
		++native_files->borrowed;
		*out = value;
		valid = true;
	}
	SpinLockRelease(&native_files->lock);
	return valid;
}
ClusterControlNativeResultV2
cluster_control_native_file_borrow_by_address_v2(ClusterRecoveryFileKindV2 kind, Oid db, Oid ts,
											   uint64 object, ClusterControlNativeFileBorrowV2 *out)
{
	ClusterControlNativeFileBorrowV2 zero = { 0 }, value = { 0 };
	ClusterControlNativeResultV2 result = CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2;
	NativeFileBank *bank;
	int at = -1;
	if (!native_distinct(out, sizeof(*out)) || memcmp(out, &zero, sizeof(zero))
		|| kind < CLUSTER_RECOVERY_FILE_CLOG_V2 || kind > CLUSTER_RECOVERY_FILE_DEBT_V2)
		return CLUSTER_NATIVE_FILES_REFUSED_V2;
	if (!native_files)
		return result;
	SpinLockAcquire(&native_files->lock);
	bank = &native_files->banks[native_files->active];
	if (bank->info.mode == CLUSTER_NATIVE_FILES_MANAGED_V2
		&& bank->info.count <= CLUSTER_NATIVE_FILES_CAPACITY_V2) {
		result = CLUSTER_NATIVE_FILES_NOT_FOUND_V2;
		/* One snapshot traversal, never an origin-by-origin probe/reselection. */
		for (uint64 i = 0; i < bank->info.count; i++) {
			const ClusterRecoveryFileIdentityV2 *id = &bank->files[i];
			if (id->kind != kind || id->database_oid != db || id->tablespace_oid != ts
				|| id->object_id != object)
				continue;
			if (at >= 0) {
				result = CLUSTER_NATIVE_FILES_AMBIGUOUS_V2;
				break;
			}
			at = (int)i;
		}
		if (at < 0 && result == CLUSTER_NATIVE_FILES_NOT_FOUND_V2) {
			for (uint64 i = 0; i < bank->info.reservation_count; i++) {
				const ClusterRecoveryFileIdentityV2 *id = &bank->reservations[i].expected;
				if (id->kind == kind && id->database_oid == db && id->tablespace_oid == ts
					&& id->object_id == object) {
					result = CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2;
					break;
				}
			}
		}
		if (at >= 0 && result != CLUSTER_NATIVE_FILES_AMBIGUOUS_V2) {
			result = CLUSTER_NATIVE_FILES_BUSY_V2;
			if (native_files->borrowed != UINT64_MAX) {
				value.expected = bank->files[at];
				value.table_generation = bank->info.table_generation;
				value.startup_generation = bank->info.startup_generation;
				value.private_self = (uintptr_t)out;
				value.private_pid = MyProcPid;
				value.private_index = at;
				++native_files->borrowed;
				*out = value;
				result = CLUSTER_NATIVE_FILES_OK_V2;
			}
		}
	}
	SpinLockRelease(&native_files->lock);
	return result;
}
bool
cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *b)
{
	bool valid;
	if (!native_files || !native_distinct(b, sizeof(*b)))
		return false;
	SpinLockAcquire(&native_files->lock);
	valid = native_borrow_valid(b);
	SpinLockRelease(&native_files->lock);
	return valid;
}
bool
cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *b)
{
	bool valid;
	if (!native_files || !native_distinct(b, sizeof(*b)))
		return false;
	SpinLockAcquire(&native_files->lock);
	valid = native_borrow_valid(b);
	if (valid) {
		--native_files->borrowed;
		memset(b, 0, sizeof(*b));
	}
	SpinLockRelease(&native_files->lock);
	return valid;
}
ClusterControlNativeResultV2
cluster_control_native_files_clog_range_v2(uint16 origin, uint64 first, uint64 last)
{
	ClusterControlNativeResultV2 result;
	if (!native_files)
		return CLUSTER_NATIVE_FILES_REFUSED_V2;
	SpinLockAcquire(&native_files->lock);
	result = native_range(&native_files->banks[native_files->active], origin, first, last);
	SpinLockRelease(&native_files->lock);
	return result;
}
static bool
native_overlap(const void *a, Size an, const void *b, Size bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return x < y + bn && y < x + an;
}
ClusterControlNativeResultV2
cluster_control_native_file_reserved_acquire_locked_v2(
	const ClusterSharedConfigRef *config, Size budget, uint64 startup, const uint8 batch[16],
	const ClusterRecoveryFileIdentityV2 *expected, ClusterControlNativeFileBorrowV2 *out)
{
	struct NativeReservedWork {
		ClusterControlRelmapSelectionV2 *selection;
		ClusterControlRelmapReferenceV2 control;
		ClusterRecoveryFileIdentityV2 expected, verified;
		ClusterSharedConfigRef config;
		uint8 batch[16];
		uint64 cf_sequence;
		bool qualified;
	} *work;
	ClusterControlNativeFileBorrowV2 zero = { 0 }, value = { 0 };
	ClusterControlNativeResultV2 result = CLUSTER_NATIVE_FILES_REFUSED_V2;
	if (!native_files || !startup || startup == UINT64_MAX
		|| budget > CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2
		|| !native_distinct(config, sizeof(*config)) || !native_distinct(batch, 16)
		|| !native_distinct(expected, sizeof(*expected)) || !native_distinct(out, sizeof(*out))
		|| native_overlap(config, sizeof(*config), out, sizeof(*out))
		|| native_overlap(batch, 16, out, sizeof(*out))
		|| native_overlap(expected, sizeof(*expected), out, sizeof(*out))
		|| memcmp(out, &zero, sizeof(zero)) || !cluster_cf_held_is_clusterwide(ExclusiveLock)
		|| CritSectionCount || LWLockHeldByMe(ControlFileLock)
		|| LWLockHeldByMe(RelationMappingLock) || LWLockHeldByMe(XidGenLock)
		|| LWLockHeldByMe(XactSLRULock))
		return result;
	work = palloc0(sizeof(*work));
	work->config = *config;
	work->expected = *expected;
	memcpy(work->batch, batch, 16);
	work->cf_sequence = cluster_cf_attempt_sequence();
	PG_TRY();
	{
		work->qualified = cluster_control_root_relmap_selection_read_locked_v4(
							  &work->config, budget, &work->selection)
							  == 0
						  && cluster_control_root_reserved_file_locked_v4(
							  work->selection, work->batch, &work->expected, &work->verified)
						  && cluster_control_root_relmap_selection_reference_v4(work->selection,
																				&work->control);
		/* Finish every potentially failing descriptor cleanup before a borrow. */
		cluster_control_root_relmap_selection_destroy_v4(&work->selection);
	}
	PG_CATCH();
	{
		cluster_control_root_relmap_selection_destroy_v4(&work->selection);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (work->qualified && cluster_cf_held_is_clusterwide(ExclusiveLock)
		&& cluster_cf_attempt_sequence() == work->cf_sequence
		&& !memcmp(config, &work->config, sizeof(*config)) && !memcmp(batch, work->batch, 16)
		&& native_same(expected, &work->expected) && !memcmp(out, &zero, sizeof(zero))) {
		NativeFileBank *bank;
		int at;
		SpinLockAcquire(&native_files->lock);
		bank = &native_files->banks[native_files->active];
		at = native_reserved_find(bank, &work->expected);
		if (bank->info.mode == CLUSTER_NATIVE_FILES_MANAGED_V2
			&& bank->info.startup_generation == startup && bank->info.registry_byte_budget == budget
			&& !memcmp(&bank->info.control, &work->control, sizeof(work->control)) && at >= 0
			&& bank->reservations[at].state == CLUSTER_RECOVERY_FILE_RESERVED_V1
			&& !memcmp(bank->reservations[at].batch_uuid, work->batch, 16)
			&& native_same(&bank->reservations[at].expected, &work->expected)
			&& native_files->borrowed != UINT64_MAX) {
			value.expected = work->expected;
			value.table_generation = bank->info.table_generation;
			value.startup_generation = startup;
			value.private_self = (uintptr_t)out;
			value.private_pid = MyProcPid;
			value.private_index = CLUSTER_NATIVE_FILES_CAPACITY_V2 + at;
			++native_files->borrowed;
			*out = value;
			result = CLUSTER_NATIVE_FILES_OK_V2;
		}
		SpinLockRelease(&native_files->lock);
	}
	pfree(work);
	return result;
}
static ClusterControlNativeResultV2
native_publish(const ClusterSharedConfigRef *expected, Size budget, uint64 startup, bool range,
			   uint16 origin, uint64 first, uint64 last)
{
	struct NativePublishWork {
		NativeFileBank next;
		ClusterControlRelmapSelectionV2 *selection;
	} *work;
	NativeFileBank *next;
	volatile ClusterControlNativeResultV2 result = CLUSTER_NATIVE_FILES_REFUSED_V2;
	if (budget > CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2)
		return CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2;
	if (!native_files || !native_distinct(expected, sizeof(*expected)) || !startup
		|| startup == UINT64_MAX || !cluster_cf_held_is_clusterwide(ExclusiveLock))
		return result;
	work = palloc0(sizeof(*work));
	next = &work->next;
	PG_TRY();
	{
		if (cluster_control_root_relmap_selection_read_locked_v4(expected, budget, &work->selection)
				== 0
			&& cluster_control_root_relmap_selection_reference_v4(work->selection,
																  &next->info.control)
			&& cluster_control_root_native_files_copy_v4(
				work->selection, next->files, CLUSTER_NATIVE_FILES_CAPACITY_V2, &next->info.count)
			&& native_canonical(next)) {
			next->info.reservation_count
				= cluster_control_root_registry_reservation_count_v4(work->selection);
			if (next->info.reservation_count > CLUSTER_NATIVE_RESERVATIONS_CAPACITY_V2
				|| next->info.reservation_count
					   > CLUSTER_NATIVE_FILES_CAPACITY_V2 - next->info.count)
				result = CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2;
			else {
				bool complete = true;
				for (uint64 i = 0; i < next->info.reservation_count; i++)
					if (!cluster_control_root_registry_reservation_at_v4(work->selection, i,
																		 &next->reservations[i]))
						complete = false;
				if (!complete) {
					cluster_control_root_relmap_selection_destroy_v4(&work->selection);
				} else {
					qsort(next->reservations, next->info.reservation_count,
						  sizeof(next->reservations[0]), native_key_compare);
					next->info.mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
					next->info.startup_generation = startup;
					next->info.registry_byte_budget = budget;
					result = range ? native_range(next, origin, first, last)
								   : CLUSTER_NATIVE_FILES_OK_V2;
					if (result == 0)
						result = native_install(next, work->selection, NULL);
				}
			}
		}
	}
	PG_FINALLY();
	{
		cluster_control_root_relmap_selection_destroy_v4(&work->selection);
		pfree(work);
	}
	PG_END_TRY();
	return result;
}
ClusterControlNativeResultV2
cluster_control_native_files_publish_locked_v2(const ClusterSharedConfigRef *expected, Size budget,
											   uint64 startup)
{
	return native_publish(expected, budget, startup, false, 0, 0, 0);
}
ClusterControlNativeResultV2
cluster_control_native_files_preregister_locked_v2(const ClusterSharedConfigRef *expected,
												   Size budget, uint64 startup, uint16 origin,
												   uint64 first, uint64 last)
{
	return native_publish(expected, budget, startup, true, origin, first, last);
}

/* Control producer alias check; no table state or authority is observed. */
bool
cluster_control_native_files_output_distinct_v2(const void *p, Size n)
{
	return native_distinct(p, n);
}
void
cluster_control_native_clog_range_destroy_v2(ClusterControlNativeClogRangeV2 **out)
{
	ClusterControlNativeClogRangeV2 *value;
	if (!native_distinct(out, sizeof(*out)) || !(value = *out))
		return;
	*out = NULL;
	pfree(value);
}
void
cluster_control_native_relmap_create_destroy_v2(ClusterControlNativeRelmapCreateV2 **out)
{
	ClusterControlNativeRelmapCreateV2 *value;
	if (!native_distinct(out, sizeof(*out)) || !(value = *out)
		|| value->private_self != (uintptr_t)value || value->private_pid != MyProcPid
		|| native_overlap(out, sizeof(*out), value, sizeof(*value)))
		return;
	*out = NULL;
	pfree(value);
}
