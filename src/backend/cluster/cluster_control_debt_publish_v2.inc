/*-------------------------------------------------------------------------
 * cluster_control_debt_publish_v2.inc
 *     Private protective ROOT file publication in the original control owner.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/backend/cluster/cluster_control_debt_publish_v2.inc
 * NOTES
 *     Physical component only. No production caller until the original Start
 *     credentials and all ROOT delta consumers are connected. A true result
 *     does not authorize protection transfer, retirement, completion or OPEN.
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
typedef struct ControlDebtPublishWorkV2 {
	ControlRootCaptureWorkV4 capture;
	ControlRootImage metadata;
	uint8 pair[133120], readback[66560];
	char directory[MAXPGPATH], primary[MAXPGPATH], backup[MAXPGPATH];
	struct stat original_directory;
	ControlDebtWireBatchV1 *basis;
	uint8 *scratch;
	int immutable;
} ControlDebtPublishWorkV2;

static void
control_debt_held_lwlock_v2(LWLock *lock, LWLockMode mode, void *held)
{
	(void)lock; (void)mode;
	*(bool *)held = true;
}

/* This narrow check cannot call the original Start's full Current: that
 * consumer still expects before ROOT until its complete delta is accepted. */
static inline bool
control_debt_publish_current_v2(ClusterControlDebtTransitionV2 *t,
	int global, ControlDebtPublishWorkV2 *w)
{
	ClusterControlDebtTransitionViewV2 view;
	struct stat held, named;
	return cluster_control_root_debt_transition_get_v2(t, &view)
		&& !fstat(global, &held) && !lstat(w->directory, &named)
		&& S_ISDIR(named.st_mode) && debt_store_directory_safe(global)
		&& held.st_dev == w->original_directory.st_dev
		&& held.st_ino == w->original_directory.st_ino
		&& named.st_dev == held.st_dev && named.st_ino == held.st_ino;
}

static inline bool
control_debt_publish_capture_v2(ClusterControlDebtTransitionV2 *t,
	int global, ControlDebtPublishWorkV2 *w)
{
	return control_debt_publish_current_v2(t, global, w)
		&& cluster_control_root_registry_capture_reserved_v4(global,
			t->view.registry + 32, debt_get(t->view.registry, 16, 8),
			w->pair, &w->metadata, &w->capture)
		&& control_debt_publish_current_v2(t, global, w);
}

/* No new authorization entry point. Its future caller is the original owner,
 * after full original Start/selection admission and with their lifetime held.
 * The immutable transition is captured by that same owner, not caller input.
 * Once entered, refusal or ERROR retains all physical effects and is once-only.
 * The original Start caller must retain its existing failure duty on false;
 * the transition getter continues to expose data only, never a success flag. */
static inline bool
control_debt_transition_publish_files_v2(ClusterControlDebtTransitionV2 *t,
	int global, const char *primary, const char *backup, Size budget)
{
	ClusterControlDebtTransitionViewV2 v;
	ControlDebtPublishWorkV2 *w;
	ControlDebtWireImageV1 image;
	ControlDebtStoreAttemptV1 attempt = {0};
	Size reserve = 0, scratch = 33024;
	bool held = false;
	volatile bool ok = false;
	const char *name;
	uint8 reference[128];
	if (!t || !primary || !backup || CritSectionCount
		|| !cluster_control_root_debt_transition_get_v2(t, &v)
		|| t->publish_attempted) return false;
	ForEachLWLockHeldByMe(control_debt_held_lwlock_v2, &held);
	if (held) return false;
	t->publish_attempted = true;
	if (v.batch_count > SIZE_MAX / sizeof(*w->basis)
		|| !debt_add(&reserve, 1, sizeof(*w), budget)
		|| !debt_add(&reserve, v.batch_count, sizeof(*w->basis), budget)
		|| !debt_add(&reserve, 1, sizeof(*t), budget)
		|| !debt_add(&reserve, v.batch_count, sizeof(*t->batches), budget)
		|| !debt_add(&reserve, t->byte_count, 1, budget)) return false;
	for (uint64 i = 0; i < v.batch_count; i++)
		scratch = Max(scratch, v.batches[i].basis_bytes);
	scratch = Max(scratch, v.registry_bytes);
	if (!debt_add(&reserve, scratch, 1, budget)) return false;
	w = calloc(1, sizeof(*w));
	if (!w) return false;
	w->immutable = -1;
	PG_TRY();
	{
		w->basis = calloc((Size)v.batch_count, sizeof(*w->basis));
		w->scratch = malloc(scratch);
		name = strrchr(primary, '/');
		if (w->basis && w->scratch && name && name != primary
			&& !strcmp(name + 1, "pgrac_control_root")
			&& strlen(primary) < sizeof(w->primary) && strlen(backup) < sizeof(w->backup)
			&& name - primary < sizeof(w->directory)) {
			memcpy(w->directory, primary, name - primary);
			memcpy(w->primary, primary, strlen(primary) + 1);
			snprintf(w->backup, sizeof(w->backup), "%s/pgrac_control_root.bak", w->directory);
			if (!strcmp(w->backup, backup) && !fstat(global, &w->original_directory)) {
				for (uint64 i = 0; i < v.batch_count; i++) {
					const ClusterControlDebtBasisViewV2 *b = v.batches + i;
					ControlDebtWireBatchV1 *to = w->basis + i;
					to->episode = b->episode; to->prepare = b->prepare;
					to->basis = b->basis; to->basis_bytes = b->basis_bytes;
					memcpy(to->basis_ref, b->basis_reference, 128);
					to->entry_count = debt_get(v.registry,
						debt_get(v.registry, 112, 8) + i * 384 + 56, 8);
				}
				memset(&image, 0, sizeof(image));
				image.registry = (uint8 *)v.registry; image.registry_bytes = v.registry_bytes;
				image.pages = (uint8 *)v.pages; image.page_bytes = v.page_bytes;
				image.batch_count = v.batch_count;
				memcpy(image.reference, v.prepared_reference, 128);
				/* Validation and every allocation finish before first write. */
				if (control_debt_root_candidate_v4(v.before_root, v.root_pair_bytes,
						&image, w->basis, v.batch_count, budget - reserve,
						w->pair, sizeof(w->pair))
					&& !memcmp(w->pair, v.after_root, sizeof(w->pair))
					&& control_debt_publish_capture_v2(t, global, w)
					&& !memcmp(w->pair, v.before_root, sizeof(w->pair))
					&& control_debt_store_create_v1(global, &image, w->basis,
						v.batch_count, budget - reserve, &attempt, reference)
					&& !memcmp(reference, v.prepared_reference, 128)
					&& control_debt_publish_capture_v2(t, global, w)
					&& !memcmp(w->pair, v.before_root, sizeof(w->pair))
					&& write_durable_image_size_reserved(w->backup, v.before_root, 66560, w->readback)
					&& control_debt_publish_capture_v2(t, global, w)
					&& !memcmp(w->pair, v.before_root, 66560)
					&& !memcmp(w->pair + 66560, v.before_root, 66560)
					&& write_durable_image_size_reserved(w->primary, v.after_root, 66560, w->readback)
					&& control_debt_publish_capture_v2(t, global, w)
					&& !memcmp(w->pair, v.after_root, sizeof(w->pair))) {
					char file[96];
					w->immutable = openat(global, "pgrac_recovery_registry",
						O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
					ok = w->immutable >= 0 && debt_store_directory_current(global, w->immutable);
					for (Size at = 0; ok && at < v.page_bytes; at += 33024) {
						uint8 hash[32]; debt_sha(v.pages + at, 33024, hash);
						debt_store_name(hash, false, file);
						ok = debt_store_object(w->immutable, file, image.pages + at, 33024,
							w->scratch, false, true);
					}
					debt_store_name(v.prepared_reference + 64, true, file);
					ok = ok && debt_store_object(w->immutable, file, image.registry,
						v.registry_bytes, w->scratch, false, true);
					for (uint64 i = 0; ok && i < v.batch_count; i++)
						ok = debt_store_basis_matches(w->immutable, w->basis + i, w->scratch);
					ok = ok && debt_store_directory_current(global, w->immutable)
						&& control_debt_publish_capture_v2(t, global, w)
						&& !memcmp(w->pair, v.after_root, sizeof(w->pair));
				}
			}
		}
	}
	PG_FINALLY();
	{
		if (w->immutable >= 0 && close(w->immutable)) ok = false;
		ok = ok && control_debt_publish_current_v2(t, global, w);
		free(w->scratch); free(w->basis); free(w);
	}
	PG_END_TRY();
	return ok;
}

/* This independent observer is deliberately separate from the publisher's
 * result. Candidate bytes alone, or the old backup alone, never select debt.
 * All original snapshots stay private and unchanged even when I/O refuses. */
bool
cluster_control_root_debt_transition_readback_v2(const ClusterControlDebtTransitionV2 *t,
	int global, Size budget)
{
	ClusterControlDebtTransitionViewV2 v, confirmed;
	struct stat directory, final_directory;
	ControlDebtWireImageV1 image;
	struct ReadbackWork {
		ControlDebtWireImageV1 *loaded;
		ControlDebtWireBatchV1 *basis;
		uint8 candidate[133120];
	} *w;
	Size reserve = 0;
	bool held = false;
	volatile bool ok = false;

	if (global < 0 || CritSectionCount
		|| !cluster_control_root_debt_transition_get_v2(t, &v))
		return false;
	ForEachLWLockHeldByMe(control_debt_held_lwlock_v2, &held);
	if (held || !debt_add(&reserve, 1, sizeof(*w), budget)
		|| !debt_add(&reserve, 1, sizeof(*t), budget)
		|| !debt_add(&reserve, t->byte_count, 1, budget)
		|| !debt_add(&reserve, v.batch_count, sizeof(*t->batches), budget)
		|| !debt_add(&reserve, v.batch_count, sizeof(*w->basis), budget)
		|| fstat(global, &directory) || !S_ISDIR(directory.st_mode)
		|| !debt_store_directory_safe(global))
		return false;
	w = calloc(1, sizeof(*w));
	if (!w)
		return false;
	PG_TRY();
	{
		w->basis = calloc((Size)v.batch_count, sizeof(*w->basis));
		if (w->basis) {
			for (uint64 i = 0; i < v.batch_count; i++) {
				const ClusterControlDebtBasisViewV2 *b = v.batches + i;
				ControlDebtWireBatchV1 *to = w->basis + i;

				to->episode = b->episode; to->prepare = b->prepare;
				to->basis = b->basis; to->basis_bytes = b->basis_bytes;
				memcpy(to->basis_ref, b->basis_reference, 128);
				to->entry_count = debt_get(v.registry,
					debt_get(v.registry, 112, 8) + i * 384 + 56, 8);
			}
			memset(&image, 0, sizeof(image));
			image.registry = (uint8 *)v.registry; image.registry_bytes = v.registry_bytes;
			image.pages = (uint8 *)v.pages; image.page_bytes = v.page_bytes;
			image.batch_count = v.batch_count;
			memcpy(image.reference, v.prepared_reference, 128);
			ok = control_debt_root_candidate_v4(v.before_root, v.root_pair_bytes,
					&image, w->basis, v.batch_count, budget - reserve,
					w->candidate, sizeof(w->candidate))
				&& !memcmp(w->candidate, v.after_root, sizeof(w->candidate))
				&& control_debt_store_load_selected_v4(global, v.after_root,
					v.root_pair_bytes, v.registry + 32, debt_get(v.registry, 16, 8),
					budget - reserve, &w->loaded)
				&& w->loaded->batch_count == v.batch_count
				&& w->loaded->registry_bytes == v.registry_bytes
				&& w->loaded->page_bytes == v.page_bytes
				&& !memcmp(w->loaded->reference, v.prepared_reference, 128)
				&& !memcmp(w->loaded->registry, v.registry, v.registry_bytes)
				&& !memcmp(w->loaded->pages, v.pages, v.page_bytes)
				&& !fstat(global, &final_directory)
				&& directory.st_dev == final_directory.st_dev
				&& directory.st_ino == final_directory.st_ino
				&& debt_store_directory_safe(global)
				&& cluster_control_root_debt_transition_get_v2(t, &confirmed)
				&& !memcmp(&v, &confirmed, sizeof(v));
		}
	}
	PG_FINALLY();
	{
		control_debt_wire_destroy_v1(&w->loaded);
		free(w->basis); free(w);
	}
	PG_END_TRY();
	return ok;
}
