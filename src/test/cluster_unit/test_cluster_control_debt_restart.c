/*-------------------------------------------------------------------------
 *
 * test_cluster_control_debt_restart.c
 *     Interrupted immutable debt storage and restart-style whole reload.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_debt_restart.c
 *
 * NOTES
 *     The product codec, immutable store, candidate builder and selected
 *     loader are real, as are all files.  A forked writer is killed without
 *     any cleanup; the surviving process then forgets every input vector and
 *     learns the selection only from the two ROOT files on disk.  Only the
 *     lock-presence and attempt-sequence leaves are controlled.
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *
 *-------------------------------------------------------------------------
 */
#define DEBT_WIRE_ROOT_FIXTURE 1
#define DEBT_STORE_TEST_MAIN debt_restart_old_store_main
int			debt_restart_old_store_main(void);
int			debt_store_old_wire_main(void);
static void restart_read_hook(void);
#define CONTROL_DEBT_STORE_TEST_READ_HOOK restart_read_hook
#include "test_cluster_control_debt_store.c"
#include <signal.h>

static void pg_attribute_noreturn()
restart_fixture_abort(int line)
{
	fprintf(stderr, "Bail out! restart fixture setup failed at line %d\n", line);
	abort();
}

#define RESTART_BUDGET ((Size) 24 * 1024 * 1024)
#define RESTART_MAX_ENTRIES (65 * 64)

/* Where the interrupted incarnation dies; it never cleans up at any of them. */
#define RESTART_DIE_AFTER_OBJECTS 1
#define RESTART_DIE_AFTER_BACKUP 2
#define RESTART_DIE_BEFORE_PRIMARY_RENAME 3
#define RESTART_DIE_AFTER_PRIMARY 4

static ControlDebtWireEntryV1 restart_entries[RESTART_MAX_ENTRIES];
static unsigned restart_nb;
static unsigned restart_die_at_read;

/* Optional shapes, chosen before restart_begin and cleared by restart_end. */
static bool restart_distinct_bases;
static bool restart_tt_binding;
static uint8 *restart_older_basis;
static Size restart_older_bytes;
static uint8 restart_older_ref[128];
static uint64 restart_cf_sequence = 1;

/* Expected bytes for assertions only; never handed to the restarted loader. */
static uint8 restart_before[133120], restart_after[133120];
static uint8 *restart_registry, *restart_pages;
static Size restart_registry_bytes, restart_page_bytes;
static uint8 restart_reference[128];
static uint8 restart_storage[16];
static uint64 restart_system;

#ifndef CONTROL_DEBT_SHARED_ACTOR_TEST
uint64
cluster_cf_attempt_sequence(void)
{
	return restart_cf_sequence;
}
#endif

/* Armed in the forked writer only: die right after the Nth store read. */
static void
restart_read_hook(void)
{
	if (restart_die_at_read != 0 && reads == restart_die_at_read)
		kill(getpid(), SIGKILL);
#ifdef CONTROL_DEBT_RESTART_READ_HOOK
	CONTROL_DEBT_RESTART_READ_HOOK();
#endif
}

/*
 * restart_begin -- complete old selection plus an unpublished PREPARED batch.
 *
 * Creates the real basis registry object and the old ROOT pair on disk and
 * keeps the complete debt image and its adjacent candidate pair in memory.
 */
static void
restart_begin(uint64 count, unsigned nb)
{
	uint64		per;
	uint64		i;
	unsigned	b;

	if (count == 0 || nb == 0 || nb > lengthof(debt_batches)
		|| count > RESTART_MAX_ENTRIES || count % nb != 0)
		restart_fixture_abort(__LINE__);
	per = count / nb;
	cf_held = true;
	restart_cf_sequence = 1;
	restart_die_at_read = 0;
	restart_nb = nb;
	store_begin(1);
	for (i = 0; i < count; i++)
	{
		ControlDebtWireEntryV1 *e = &restart_entries[i];

		*e = debt_entries[0];
		e->entry.ordinal = i % per;
		e->entry.top_full_xid = FullTransactionIdFromU64(UINT64_C(0x200000003) + i);
		e->entry.object_generation = 100 + i;
		e->pin.key.generation = 100 + i;
		e->pin.key.top_full_xid = U64FromFullTransactionId(e->entry.top_full_xid);
	}
	if (restart_tt_binding)
	{
		restart_entries[0].pin.has_tt_binding = true;
		restart_entries[0].pin.binding = (ClusterCanonicalTxnBinding) {
			.segment_id = 4,.segment_generation = 9,.xid = 3,.slot_offset = 2,
			.slot_wrap = 1,.origin_instance = 1,
			.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED,.active_lsn = 130
		};
	}
	for (b = 0; b < nb; b++)
	{
		debt_batches[b] = debt_batches[0];
		debt_batches[b].episode = 7 + b;
		debt_batches[b].prepare = 15;
		debt_batches[b].entries = restart_entries + (Size) b * per;
		debt_batches[b].entry_count = per;
	}
	if (restart_distinct_bases)
	{
		/* The last batch keeps an older, separately stored original basis. */
		ClusterRecoveryRegistryV3 older = debt_registry;
		uint8		reference[128];

		older.available.coverage.control_generation = 13;
		/* Immutable basis names use registration/checkpoint/commit, not
		 * control_generation. This older checkpoint needs its own name. */
		older.available.coverage.checkpoint_generation = 15;
		if (nb < 2 || !cluster_recovery_registry_size_v3(&older, RESTART_BUDGET,
														  &restart_older_bytes))
			restart_fixture_abort(__LINE__);
		restart_older_basis = malloc(restart_older_bytes);
		if (restart_older_basis == NULL
			|| !cluster_recovery_registry_encode_v3(&older, restart_older_basis,
													restart_older_bytes, restart_older_ref)
			|| !cluster_recovery_registry_create_v3(store_global, &older, RESTART_BUDGET,
													reference)
			|| memcmp(reference, restart_older_ref, 128) != 0)
			restart_fixture_abort(__LINE__);
		debt_batches[nb - 1].basis = restart_older_basis;
		debt_batches[nb - 1].basis_bytes = restart_older_bytes;
		memcpy(debt_batches[nb - 1].basis_ref, restart_older_ref, 128);
	}
	control_debt_wire_destroy_v1(&image);
	if (!control_debt_wire_encode_v1(debt_original, debt_original_bytes, debt_original_ref,
									 15, 17, debt_batches, nb, RESTART_BUDGET, &image)
		|| !cluster_control_root_reservations_encode_v4(&root, &debt_registry, debt_original_ref,
														restart_before, 66560))
		restart_fixture_abort(__LINE__);
	memcpy(restart_before + 66560, restart_before, 66560);
	if (!control_debt_root_candidate_v4(restart_before, sizeof(restart_before), image,
										debt_batches, nb, RESTART_BUDGET, restart_after,
										sizeof(restart_after)))
		restart_fixture_abort(__LINE__);
	restart_registry_bytes = image->registry_bytes;
	restart_page_bytes = image->page_bytes;
	restart_registry = malloc(restart_registry_bytes);
	restart_pages = malloc(restart_page_bytes);
	if (restart_registry == NULL || restart_pages == NULL)
		restart_fixture_abort(__LINE__);
	memcpy(restart_registry, image->registry, restart_registry_bytes);
	memcpy(restart_pages, image->pages, restart_page_bytes);
	memcpy(restart_reference, image->reference, 128);
	memcpy(restart_storage, reg.storage_uuid, 16);
	restart_system = reg.system_identifier;
	write_root(store_global, "pgrac_control_root", restart_before);
	write_root(store_global, "pgrac_control_root.bak", restart_before + 66560);
}

/* Remove this fixture's own ROOT files and leftovers, then the store itself. */
static void
restart_end(void)
{
	int			fd = openat(store_global, ".", O_RDONLY | O_DIRECTORY);
	DIR		   *entries = fd < 0 ? NULL : fdopendir(fd);
	struct dirent *entry;

	if (entries == NULL)
		restart_fixture_abort(__LINE__);
	while ((entry = readdir(entries)) != NULL)
		if (strncmp(entry->d_name, "pgrac_control_root", 18) == 0
			&& unlinkat(store_global, entry->d_name, 0) != 0)
			restart_fixture_abort(__LINE__);
	closedir(entries);
	free(restart_registry);
	free(restart_pages);
	free(restart_older_basis);
	restart_registry = restart_pages = restart_older_basis = NULL;
	restart_distinct_bases = restart_tt_binding = false;
	debt_fixture_v4 = false;
	cf_held = true;
	store_end();
}

/* Fixture-owned ROOT replacement: complete temporary file, sync, rename. */
static void
restart_install_root(const char *name, const uint8 *bytes)
{
	char		temporary[64];
	int			fd;

	snprintf(temporary, sizeof(temporary), "%s.tmp", name);
	fd = openat(store_global, temporary, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0 || write(fd, bytes, 66560) != 66560 || fsync(fd) != 0 || close(fd) != 0
		|| renameat(store_global, temporary, store_global, name) != 0
		|| fsync(store_global) != 0)
		restart_fixture_abort(__LINE__);
}

/*
 * restart_interrupt -- run the interrupted incarnation and reap it.
 *
 * The child creates every immutable object through the product store, then
 * replaces ROOT in the original backup-old, primary-new order as far as the
 * requested stage.  It always dies by SIGKILL, so nothing is flushed, undone
 * or removed on its way out.  Returns the raw wait status.
 */
static int
restart_interrupt(unsigned stage, unsigned die_at_read)
{
	pid_t		child;
	int			status = 0;

	fflush(stdout);
	child = fork();
	if (child < 0)
		restart_fixture_abort(__LINE__);
	if (child == 0)
	{
		ControlDebtStoreAttemptV1 attempt = {0};
		uint8		reference[128];

		reset_faults();
		restart_die_at_read = die_at_read;
		if (!control_debt_store_create_v1(store_global, image, debt_batches, restart_nb,
										  RESTART_BUDGET, &attempt, reference))
			_exit(3);
		restart_die_at_read = 0;
		if (stage >= RESTART_DIE_AFTER_BACKUP)
			restart_install_root("pgrac_control_root.bak", restart_after + 66560);
		if (stage == RESTART_DIE_BEFORE_PRIMARY_RENAME)
		{
			/* Complete, synced new primary bytes that were never renamed. */
			int			fd = openat(store_global, "pgrac_control_root.tmp",
									O_WRONLY | O_CREAT | O_EXCL, 0600);

			if (fd < 0 || write(fd, restart_after, 66560) != 66560 || fsync(fd) != 0
				|| close(fd) != 0)
				restart_fixture_abort(__LINE__);
		}
		if (stage >= RESTART_DIE_AFTER_PRIMARY)
			restart_install_root("pgrac_control_root", restart_after);
		kill(getpid(), SIGKILL);
		_exit(4);
	}
	if (waitpid(child, &status, 0) != child)
		restart_fixture_abort(__LINE__);
	return status;
}

static bool
restart_killed(int status)
{
	return WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
}

/* Drop every vector the interrupted incarnation held in memory. */
static void
restart_forget(void)
{
	control_debt_wire_destroy_v1(&image);
	memset(restart_entries, 0, sizeof(restart_entries));
	memset(debt_entries, 0, sizeof(debt_entries));
	memset(debt_batches, 0, sizeof(debt_batches));
}

/* A restarted process opens the directory again instead of inheriting it. */
static int
restart_open(void)
{
	int			fd = open(store_path, O_RDONLY | O_DIRECTORY);

	if (fd < 0)
		restart_fixture_abort(__LINE__);
	return fd;
}

/*
 * restart_reload -- what a restarted owner can do with disk contents alone.
 *
 * Captures the complete physical pair first and hands exactly that pair to
 * the selected loader; no remembered candidate, reference or vector is used.
 */
static bool
restart_reload(int global, uint8 pair[133120], ControlDebtWireImageV1 **out)
{
	ControlRootImage *metadata = malloc(sizeof(*metadata));
	bool		ok;

	if (metadata == NULL)
		restart_fixture_abort(__LINE__);
	ok = cluster_control_root_registry_capture_v4(global, restart_storage, restart_system,
												  pair, metadata)
		&& control_debt_store_load_selected_v4(global, pair, 133120, restart_storage,
											   restart_system, RESTART_BUDGET, out);
	free(metadata);
	return ok;
}

/* The loaded image must be the complete original, never a readable prefix. */
static bool
restart_image_complete(const ControlDebtWireImageV1 *w)
{
	return w != NULL && w->registry_bytes == restart_registry_bytes
		&& w->page_bytes == restart_page_bytes && w->batch_count == restart_nb
		&& memcmp(w->registry, restart_registry, restart_registry_bytes) == 0
		&& memcmp(w->pages, restart_pages, restart_page_bytes) == 0
		&& memcmp(w->reference, restart_reference, 128) == 0;
}

/*
 * The basis object a ROOT image selects is still the complete original:
 * load it through the original loader and re-encode it to the same bytes.
 */
static bool
restart_basis_complete(int global, const uint8 *root_bytes)
{
	ClusterRecoveryRegistryDecodedV3 *decoded = NULL;
	uint8	   *bytes = malloc(debt_original_bytes);
	uint8		reference[128];
	bool		ok;

	if (bytes == NULL)
		restart_fixture_abort(__LINE__);
	ok = memcmp(root_bytes + 512, debt_original_ref, 128) == 0
		&& cluster_recovery_registry_load_selected_v3(global, root_bytes + 512,
													  CATALOG_VERSION_NO, RESTART_BUDGET,
													  &decoded)
		&& cluster_recovery_registry_encode_v3(cluster_recovery_registry_view_v3(decoded),
											   bytes, debt_original_bytes, reference)
		&& memcmp(reference, debt_original_ref, 128) == 0
		&& memcmp(bytes, debt_original, debt_original_bytes) == 0;
	cluster_recovery_registry_destroy_v3(&decoded);
	free(bytes);
	return ok;
}

/* Fold one directory into an order-independent digest of names and bytes. */
static void
restart_fold_directory(int parent, const char *name, uint8 digest[32], uint64 *count)
{
	int			fd = openat(parent, name, O_RDONLY | O_DIRECTORY);
	DIR		   *entries = fd < 0 ? NULL : fdopendir(fd);
	struct dirent *entry;

	if (entries == NULL)
		restart_fixture_abort(__LINE__);
	while ((entry = readdir(entries)) != NULL)
	{
		struct stat st;
		pg_sha256_ctx ctx;
		uint8		one[32];
		uint8		meta[48];
		uint8	   *bytes;
		int			file;
		unsigned	i;

		if (fstatat(fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0)
			restart_fixture_abort(__LINE__);
		if (!S_ISREG(st.st_mode))
			continue;
		file = openat(fd, entry->d_name, O_RDONLY | O_NOFOLLOW);
		bytes = malloc(st.st_size + 1);
		if (file < 0 || bytes == NULL || pread(file, bytes, st.st_size + 1, 0) != st.st_size
			|| close(file) != 0)
			restart_fixture_abort(__LINE__);
		debt_put(meta, 0, st.st_ino, 8);
		debt_put(meta, 8, st.st_size, 8);
		debt_put(meta, 16, st.st_mode, 8);
		debt_put(meta, 24, st.st_nlink, 8);
		debt_put(meta, 32, st.st_mtime, 8);
		debt_put(meta, 40, st.st_ctime, 8);
		pg_sha256_init(&ctx);
		pg_sha256_update(&ctx, (const uint8 *) entry->d_name, strlen(entry->d_name) + 1);
		pg_sha256_update(&ctx, meta, sizeof(meta));
		pg_sha256_update(&ctx, bytes, st.st_size);
		pg_sha256_final(&ctx, one);
		for (i = 0; i < 32; i++)
			digest[i] ^= one[i];
		(*count)++;
		free(bytes);
	}
	closedir(entries);
}

/* Every file identity, size, mode, link count, time and byte under the store. */
static uint64
restart_tree_digest(uint8 digest[32])
{
	uint64		count = 0;

	memset(digest, 0, 32);
	restart_fold_directory(store_global, ".", digest, &count);
	restart_fold_directory(store_global, "pgrac_recovery_registry", digest, &count);
	return count;
}

/* Open one immutable page object of the expected image by its own reference. */
static int
restart_open_page(int global, uint64 page, int flags)
{
	char		name[96];
	int			dir = openat(global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
	int			fd;

	if (dir < 0)
		restart_fixture_abort(__LINE__);
	debt_store_name(restart_registry + debt_get(restart_registry, 136, 8) + (Size) page * 128 + 64,
					false, name);
	fd = openat(dir, name, flags);
	close(dir);
	return fd;
}

/* Publish through a killed writer, forget everything, prove a sound start. */
static int
restart_begin_published(uint64 count, unsigned nb)
{
	ControlDebtWireImageV1 *loaded = NULL;
	uint8		pair[133120];
	int			global;

	restart_begin(count, nb);
	UT_ASSERT(restart_killed(restart_interrupt(RESTART_DIE_AFTER_PRIMARY, 0)));
	restart_forget();
	global = restart_open();
	UT_ASSERT(restart_reload(global, pair, &loaded));
	UT_ASSERT(restart_image_complete(loaded));
	control_debt_wire_destroy_v1(&loaded);
	return global;
}

static void
restart_expect_refusal(int global)
{
	ControlDebtWireImageV1 *loaded = NULL;
	uint8		pair[133120];

	UT_ASSERT(!restart_reload(global, pair, &loaded));
	UT_ASSERT(loaded == NULL);
	control_debt_wire_destroy_v1(&loaded);
}

UT_TEST(restart_primary_new_backup_old_reloads_complete_basis_and_every_page)
{
	static const struct
	{
		uint64		count;
		unsigned	nb;
		bool		v4;
		bool		tt;
	}			shapes[] = {
		{65, 1, false, false}, {130, 1, true, false}, {130, 2, false, true},
		{64 * 64 + 1, 1, false, false}
	};
	unsigned	shape;

	for (shape = 0; shape < lengthof(shapes); shape++)
	{
		ControlDebtWireImageV1 *loaded = NULL;
		ClusterRecoveryRegistryDecodedV3 *embedded = NULL;
		uint8		pair[133120];
		uint64		pages = (shapes[shape].count / shapes[shape].nb + 63) / 64 * shapes[shape].nb;
		int			global;
		unsigned	b;

		debt_fixture_v4 = shapes[shape].v4;
		restart_tt_binding = shapes[shape].tt;
		restart_begin(shapes[shape].count, shapes[shape].nb);
		UT_ASSERT_EQ(restart_page_bytes, pages * 33024);
		UT_ASSERT(restart_killed(restart_interrupt(RESTART_DIE_AFTER_PRIMARY, 0)));
		restart_forget();
		global = restart_open();
		memset(pair, 0x5c, sizeof(pair));
		UT_ASSERT(restart_reload(global, pair, &loaded));
		/* The physical cut itself: PREPARED primary over the previous primary. */
		UT_ASSERT(memcmp(pair, restart_after, 66560) == 0);
		UT_ASSERT(memcmp(pair + 66560, restart_before, 66560) == 0);
		UT_ASSERT(restart_image_complete(loaded));
		if (loaded != NULL)
		{
			const uint8 *rows = loaded->registry + debt_get(loaded->registry, 112, 8);

			UT_ASSERT_EQ(debt_get(loaded->registry, 144, 8), pages);
			UT_ASSERT_EQ(debt_get(loaded->registry, 152, 8), shapes[shape].count);
			/* The complete previous basis travels inside the debt registry. */
			UT_ASSERT_EQ(debt_get(loaded->registry, 104, 8), debt_original_bytes);
			UT_ASSERT(memcmp(loaded->registry + 256, debt_original, debt_original_bytes) == 0);
			UT_ASSERT(debt_old_open(loaded->registry + 256, debt_original_bytes,
									debt_original_ref, RESTART_BUDGET, &embedded));
			if (embedded != NULL)
			{
				const ClusterRecoveryRegistryV3 *r = cluster_recovery_registry_view_v3(embedded);

				UT_ASSERT_EQ(r->reservation_count, 2);
				UT_ASSERT_EQ(r->available.coverage.source_count, 2);
				UT_ASSERT_EQ(r->available.coverage.file_count, 2);
				UT_ASSERT_EQ(r->available.coverage.control_generation, 14);
				UT_ASSERT_EQ(r->available.coverage_format_version, shapes[shape].v4 ? 4 : 2);
			}
			/* A published TT binding survives field by field; absence stays zero. */
			UT_ASSERT_EQ(debt_get(loaded->pages + 256 + 352, 88, 4), shapes[shape].tt ? 4 : 0);
			UT_ASSERT_EQ(debt_get(loaded->pages + 256 + 352, 112, 8), shapes[shape].tt ? 130 : 0);
			for (b = 0; b < shapes[shape].nb; b++)
				UT_ASSERT(memcmp(rows + (Size) b * 384 + 184, debt_original_ref, 128) == 0);
			/* Last entry of the last page keeps its own exact key. */
			UT_ASSERT_EQ(debt_get(loaded->pages + (Size) (pages - 1) * 33024 + 256
								  + (Size) ((shapes[shape].count / shapes[shape].nb - 1) % 64) * 512,
								  288, 8),
						 100 + shapes[shape].count - 1);
		}
		/* Both ROOT copies still lead to the complete previous basis object. */
		UT_ASSERT(restart_basis_complete(global, pair + 66560));
		cluster_recovery_registry_destroy_v3(&embedded);
		control_debt_wire_destroy_v1(&loaded);
		UT_ASSERT(close(global) == 0);
		restart_end();
	}
}

UT_TEST(restart_reload_is_repeatable_and_never_writes)
{
	ControlDebtWireImageV1 *first = NULL, *second = NULL;
	uint8		pair[133120], before[32], after[32];
	uint64		files;
	int			global = restart_begin_published(130, 2);

	files = restart_tree_digest(before);
	/* Two ROOT copies, four pages, the debt registry and the basis object. */
	UT_ASSERT_EQ(files, 2 + 4 + 1 + 1);
	UT_ASSERT(restart_reload(global, pair, &first));
	UT_ASSERT(restart_reload(global, pair, &second));
	UT_ASSERT(restart_image_complete(first) && restart_image_complete(second));
	UT_ASSERT_EQ(restart_tree_digest(after), files);
	UT_ASSERT(memcmp(before, after, 32) == 0);
	control_debt_wire_destroy_v1(&first);
	control_debt_wire_destroy_v1(&second);
	UT_ASSERT(close(global) == 0);
	restart_end();
}

UT_TEST(restart_killed_during_object_creation_never_exposes_a_prefix)
{
	ControlDebtStoreAttemptV1 attempt = {0};
	uint8		reference[128];
	unsigned	total, at, refused = 0, complete = 0;

	/* Count the store reads of one whole undisturbed creation. */
	restart_begin(130, 1);
	reset_faults();
	UT_ASSERT(control_debt_store_create_v1(store_global, image, debt_batches, restart_nb,
										   RESTART_BUDGET, &attempt, reference));
	total = reads;
	restart_end();
	UT_ASSERT(total >= 8);
	for (at = 1; at <= total; at++)
	{
		ControlDebtWireImageV1 *loaded = NULL;
		uint8		pair[133120];
		ControlRootImage *metadata = malloc(sizeof(*metadata));
		int			global;

		restart_begin(130, 1);
		UT_ASSERT(restart_killed(restart_interrupt(RESTART_DIE_AFTER_OBJECTS, at)));
		restart_forget();
		global = restart_open();
		/* ROOT was never touched: both copies remain the previous selection. */
		UT_ASSERT(metadata != NULL
				  && cluster_control_root_registry_capture_v4(global, restart_storage,
															  restart_system, pair, metadata));
		UT_ASSERT(memcmp(pair, restart_before, sizeof(pair)) == 0);
		UT_ASSERT(restart_basis_complete(global, pair));
		UT_ASSERT(!control_debt_store_load_selected_v4(global, pair, sizeof(pair),
													   restart_storage, restart_system,
													   RESTART_BUDGET, &loaded));
		UT_ASSERT(!control_debt_store_load_selected_v4(global, restart_after,
													   sizeof(restart_after), restart_storage,
													   restart_system, RESTART_BUDGET, &loaded));
		UT_ASSERT(loaded == NULL);
		/* By exact reference the unselected batch is whole or absent. */
		if (control_debt_store_load_v1(global, restart_reference, RESTART_BUDGET, &loaded))
		{
			UT_ASSERT(restart_image_complete(loaded));
			complete++;
		}
		else
		{
			UT_ASSERT(loaded == NULL);
			refused++;
		}
		control_debt_wire_destroy_v1(&loaded);
		free(metadata);
		UT_ASSERT(close(global) == 0);
		restart_end();
	}
	/* The sweep really crossed from nothing written to everything written. */
	UT_ASSERT(refused > 0 && complete > 0);
	UT_ASSERT_EQ(refused + complete, total);
}

UT_TEST(restart_killed_before_primary_keeps_previous_selection_and_written_debt)
{
	unsigned	stage;

	for (stage = RESTART_DIE_AFTER_OBJECTS; stage <= RESTART_DIE_BEFORE_PRIMARY_RENAME; stage++)
	{
		ControlDebtWireImageV1 *loaded = NULL;
		ControlRootImage *metadata = malloc(sizeof(*metadata));
		uint8		pair[133120];
		int			global;

		restart_begin(130, 2);
		UT_ASSERT(restart_killed(restart_interrupt(stage, 0)));
		restart_forget();
		global = restart_open();
		UT_ASSERT(metadata != NULL
				  && cluster_control_root_registry_capture_v4(global, restart_storage,
															  restart_system, pair, metadata));
		UT_ASSERT(memcmp(pair, restart_before, 66560) == 0);
		UT_ASSERT(memcmp(pair + 66560, restart_before, 66560) == 0);
		UT_ASSERT(restart_basis_complete(global, pair));
		UT_ASSERT(restart_basis_complete(global, pair + 66560));
		restart_expect_refusal(global);
		UT_ASSERT(!control_debt_store_load_selected_v4(global, restart_after,
													   sizeof(restart_after), restart_storage,
													   restart_system, RESTART_BUDGET, &loaded));
		UT_ASSERT(loaded == NULL);
		/* Written debt is retained whole although nothing selects it. */
		UT_ASSERT(control_debt_store_load_v1(global, restart_reference, RESTART_BUDGET, &loaded));
		UT_ASSERT(restart_image_complete(loaded));
		control_debt_wire_destroy_v1(&loaded);
		free(metadata);
		UT_ASSERT(close(global) == 0);
		restart_end();
	}
}

UT_TEST(restart_truncated_or_resized_page_refuses_whole_reload)
{
	static const off_t sizes[] = {0, 17, 33023, 33025};
	unsigned	page, size;

	for (page = 0; page < 3; page++)
		for (size = 0; size < lengthof(sizes); size++)
		{
			int			global = restart_begin_published(130, 1);
			int			fd = restart_open_page(global, page, O_RDWR);

			UT_ASSERT(fd >= 0 && ftruncate(fd, sizes[size]) == 0 && close(fd) == 0);
			restart_expect_refusal(global);
			UT_ASSERT(close(global) == 0);
			restart_end();
		}
}

UT_TEST(restart_missing_or_damaged_page_and_registry_refuse_whole_reload)
{
	unsigned	fault;

	for (fault = 0; fault < 4; fault++)
	{
		int			global = restart_begin_published(130, 2);
		int			dir = openat(global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
		char		name[96];
		uint8		byte = 0;
		int			fd;

		UT_ASSERT(dir >= 0);
		if (fault == 0)
		{
			/* Last page of the last batch is gone. */
			debt_store_name(restart_registry + debt_get(restart_registry, 136, 8)
							+ 3 * 128 + 64, false, name);
			UT_ASSERT(unlinkat(dir, name, 0) == 0);
		}
		else if (fault == 1)
		{
			/* Same size, one payload bit of a middle page differs. */
			fd = restart_open_page(global, 1, O_RDWR);
			UT_ASSERT(fd >= 0 && pread(fd, &byte, 1, 300) == 1);
			byte ^= 1;
			UT_ASSERT(pwrite(fd, &byte, 1, 300) == 1 && close(fd) == 0);
		}
		else
		{
			debt_store_name(restart_reference + 64, true, name);
			if (fault == 2)
				UT_ASSERT(unlinkat(dir, name, 0) == 0);
			else
			{
				fd = openat(dir, name, O_RDWR);
				UT_ASSERT(fd >= 0
						  && ftruncate(fd, (off_t) restart_registry_bytes - 1) == 0
						  && close(fd) == 0);
			}
		}
		UT_ASSERT(close(dir) == 0);
		restart_expect_refusal(global);
		UT_ASSERT(close(global) == 0);
		restart_end();
	}
}

UT_TEST(restart_missing_or_damaged_basis_refuses_whole_reload)
{
	unsigned	fault;

	for (fault = 0; fault < 4; fault++)
	{
		int			global = restart_begin_published(130, 2);
		int			dir = openat(global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
		char		name[96];
		uint8		byte = 0;
		int			fd;

		UT_ASSERT(dir >= 0);
		debt_store_basis_name(debt_original_ref, name);
		if (fault == 0)
			UT_ASSERT(unlinkat(dir, name, 0) == 0);
		else
		{
			fd = openat(dir, name, O_RDWR);
			UT_ASSERT(fd >= 0);
			if (fault == 1)
				UT_ASSERT(ftruncate(fd, (off_t) debt_original_bytes - 1) == 0);
			else if (fault == 2)
				UT_ASSERT(pwrite(fd, "x", 1, (off_t) debt_original_bytes) == 1);
			else
			{
				/* Same size, one reservation byte differs from the debt's copy. */
				UT_ASSERT(pread(fd, &byte, 1, (off_t) debt_original_bytes - 100) == 1);
				byte ^= 1;
				UT_ASSERT(pwrite(fd, &byte, 1, (off_t) debt_original_bytes - 100) == 1);
			}
			UT_ASSERT(close(fd) == 0);
		}
		UT_ASSERT(close(dir) == 0);
		restart_expect_refusal(global);
		UT_ASSERT(close(global) == 0);
		restart_end();
	}
}

UT_TEST(restart_every_batch_basis_is_required_not_only_the_first)
{
	unsigned	fault;

	for (fault = 0; fault < 3; fault++)
	{
		ControlDebtWireImageV1 *loaded = NULL;
		uint8		pair[133120];
		char		name[96];
		int			global, dir;

		restart_distinct_bases = true;
		global = restart_begin_published(130, 2);
		UT_ASSERT(restart_reload(global, pair, &loaded));
		if (loaded != NULL)
		{
			const uint8 *rows = loaded->registry + debt_get(loaded->registry, 112, 8);

			/* Each batch row names its own original basis generation. */
			UT_ASSERT(memcmp(rows + 184, debt_original_ref, 128) == 0);
			UT_ASSERT(memcmp(rows + 384 + 184, restart_older_ref, 128) == 0);
			UT_ASSERT_EQ(debt_get(rows, 24, 8), 14);
			UT_ASSERT_EQ(debt_get(rows + 384, 24, 8), 13);
		}
		control_debt_wire_destroy_v1(&loaded);
		dir = openat(global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
		UT_ASSERT(dir >= 0);
		if (fault == 0)
			debt_store_basis_name(restart_older_ref, name);
		else
			debt_store_basis_name(debt_original_ref, name);
		if (fault < 2)
			UT_ASSERT(unlinkat(dir, name, 0) == 0);
		else
		{
			int			fd = openat(dir, name, O_RDWR);

			UT_ASSERT(fd >= 0 && ftruncate(fd, 511) == 0 && close(fd) == 0);
		}
		UT_ASSERT(close(dir) == 0);
		restart_expect_refusal(global);
		UT_ASSERT(close(global) == 0);
		restart_end();
	}
}

/*
 * A separately valid normal ROOT image of the same fixture at another
 * generation or lineage.  Its own registry object need not exist: the pair
 * rules are decided on the two ROOT images alone.
 */
static void
restart_other_root(uint64 generation, bool other_lineage, uint8 out[66560])
{
	ControlRootImage *metadata = malloc(sizeof(*metadata));
	uint8	   *bytes;
	uint8		reference[128];
	Size		n;

	root_fixture();
	root.header.file_txn_seq = generation;
	if (other_lineage)
		root.header.migration_round_sha256[0] ^= 1;
	debt_registry.available = reg;
	debt_registry.available.coverage.control_generation = generation;
	if (metadata == NULL
		|| !cluster_recovery_registry_size_v3(&debt_registry, RESTART_BUDGET, &n))
		restart_fixture_abort(__LINE__);
	bytes = malloc(n);
	if (bytes == NULL
		|| !cluster_recovery_registry_encode_v3(&debt_registry, bytes, n, reference)
		|| !cluster_control_root_reservations_encode_v4(&root, &debt_registry, reference,
														out, 66560)
		|| !cluster_control_root_registry_decode_v4(out, 66560, restart_storage,
													restart_system, metadata))
		restart_fixture_abort(__LINE__);
	free(bytes);
	free(metadata);
}

UT_TEST(restart_wrong_backup_refuses_whole_reload)
{
	unsigned	fault;

	for (fault = 0; fault < 7; fault++)
	{
		int			global = restart_begin_published(130, 1);
		uint8	   *other = malloc(66560);

		UT_ASSERT(other != NULL);
		if (fault == 0)
		{
			/* Individually valid, but from another creation lineage. */
			restart_other_root(14, true, other);
			write_root(global, "pgrac_control_root.bak", other);
		}
		else if (fault == 1)
		{
			/* Individually valid, but newer than the PREPARED primary. */
			restart_other_root(16, false, other);
			write_root(global, "pgrac_control_root.bak", other);
		}
		else if (fault == 2)
		{
			/* Same generation as the primary with different contents. */
			restart_other_root(15, false, other);
			write_root(global, "pgrac_control_root.bak", other);
		}
		else if (fault == 3)
			UT_ASSERT(unlinkat(global, "pgrac_control_root.bak", 0) == 0);
		else if (fault == 4)
		{
			/* One file under both names is not an independent copy. */
			UT_ASSERT(unlinkat(global, "pgrac_control_root.bak", 0) == 0);
			UT_ASSERT(linkat(global, "pgrac_control_root", global,
							 "pgrac_control_root.bak", 0) == 0);
		}
		else
		{
			int			fd = openat(global, "pgrac_control_root.bak", O_RDWR);

			UT_ASSERT(fd >= 0);
			if (fault == 5)
				UT_ASSERT(ftruncate(fd, 66559) == 0);
			else
				UT_ASSERT(pwrite(fd, "\377", 1, 2048) == 1);
			UT_ASSERT(close(fd) == 0);
		}
		restart_expect_refusal(global);
		free(other);
		UT_ASSERT(close(global) == 0);
		restart_end();
	}
}

UT_TEST(restart_identical_primary_copy_as_backup_still_reloads)
{
	ControlDebtWireImageV1 *loaded = NULL;
	uint8		pair[133120];
	int			global = restart_begin_published(130, 1);

	/* A later replacement that stopped after its own backup-old step. */
	write_root(global, "pgrac_control_root.bak", restart_after);
	UT_ASSERT(restart_reload(global, pair, &loaded));
	UT_ASSERT(memcmp(pair, restart_after, 66560) == 0);
	UT_ASSERT(memcmp(pair + 66560, restart_after, 66560) == 0);
	UT_ASSERT(restart_image_complete(loaded));
	control_debt_wire_destroy_v1(&loaded);
	UT_ASSERT(close(global) == 0);
	restart_end();
}

UT_TEST(restart_identical_recreation_never_overwrites_interrupted_objects)
{
	ControlDebtStoreAttemptV1 attempt = {0};
	uint8		reference[128], before[32], after[32];
	uint64		files;
	unsigned	first_page_reads;

	/* Reads up to and including the first created page's own readback. */
	restart_begin(130, 1);
	reset_faults();
	UT_ASSERT(control_debt_store_create_v1(store_global, image, debt_batches, restart_nb,
										   RESTART_BUDGET, &attempt, reference));
	first_page_reads = 2 + 2;
	UT_ASSERT(reads > first_page_reads);
	restart_end();

	restart_begin(130, 1);
	UT_ASSERT(restart_killed(restart_interrupt(RESTART_DIE_AFTER_OBJECTS, first_page_reads)));
	files = restart_tree_digest(before);
	/* Two ROOT copies, the basis object and exactly the first page. */
	UT_ASSERT_EQ(files, 2 + 1 + 1);
	/* The very same complete vectors, a fresh latch, a later incarnation. */
	memset(&attempt, 0, sizeof(attempt));
	memset(reference, 0x6e, sizeof(reference));
	reset_faults();
	UT_ASSERT(!control_debt_store_create_v1(store_global, image, debt_batches, restart_nb,
											RESTART_BUDGET, &attempt, reference));
	UT_ASSERT(attempt.attempted && reference[0] == 0x6e);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(restart_tree_digest(after), files);
	UT_ASSERT(memcmp(before, after, 32) == 0);
	restart_end();
}

#ifndef CONTROL_DEBT_RESTART_TEST_MAIN
#define CONTROL_DEBT_RESTART_TEST_MAIN main
#endif
int
CONTROL_DEBT_RESTART_TEST_MAIN(void)
{
	UT_PLAN(14);
	/* The shared ROOT fixture's own premises hold in this binary too. */
	UT_RUN(test_original_codec_v4_extent_and_exact_reference);
	UT_RUN(test_v4_refuses_forged_reference_and_record);
	UT_RUN(test_selected_root_registry_real_files_and_late_change);
	UT_RUN(restart_primary_new_backup_old_reloads_complete_basis_and_every_page);
	UT_RUN(restart_reload_is_repeatable_and_never_writes);
	UT_RUN(restart_killed_during_object_creation_never_exposes_a_prefix);
	UT_RUN(restart_killed_before_primary_keeps_previous_selection_and_written_debt);
	UT_RUN(restart_truncated_or_resized_page_refuses_whole_reload);
	UT_RUN(restart_missing_or_damaged_page_and_registry_refuse_whole_reload);
	UT_RUN(restart_missing_or_damaged_basis_refuses_whole_reload);
	UT_RUN(restart_every_batch_basis_is_required_not_only_the_first);
	UT_RUN(restart_wrong_backup_refuses_whole_reload);
	UT_RUN(restart_identical_primary_copy_as_backup_still_reloads);
	UT_RUN(restart_identical_recreation_never_overwrites_interrupted_objects);
	UT_DONE();
	return ut_failed_count != 0;
}
