/* PGRAC: actual ExportSnapshot registration -> original H sampler.
 * Snapshot fields use the existing producer; native CopySnapshot, pairing
 * heap, exported list, serialization and EOXact retirement execute unchanged.
 * File namespace/visibility, transaction metadata and interrupts are leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
int MyProcPid = 1234;
bool XactReadOnly;

static unsigned export_fault, export_file_opens, export_file_closes;
static unsigned export_renames, export_unlinks;
static SCN export_expected_h;
static bool export_h_at_file;
static char export_text[4096];

TransactionId
GetTopTransactionIdIfAny(void)
{
	return InvalidTransactionId;
}

bool
IsSubTransaction(void)
{
	return false;
}

int
xactGetCommittedChildren(TransactionId **children)
{
	*children = NULL;
	return 0;
}

char *
pstrdup(const char *s)
{
	Size length = strlen(s) + 1;
	char *copy = palloc(length);
	memcpy(copy, s, length);
	return copy;
}

void *
repalloc(void *pointer, Size size)
{
	void *resized = realloc(pointer, size);
	Assert(resized != NULL);
	return resized;
}

MemoryContext
GetMemoryChunkContext(void *pointer)
{
	/* All test allocations have the single controlled transaction context. */
	return TopTransactionContext;
}

int
errcode_for_file_access(void)
{
	/* SQLSTATE mapping is outside this publication/lifetime test. */
	return errcode(ERRCODE_IO_ERROR);
}

FILE *
AllocateFile(const char *path, const char *mode)
{
	ClusterUndoRetentionSample sample;

	UT_ASSERT(strncmp(path, SNAPSHOT_EXPORT_DIR "/", strlen(SNAPSHOT_EXPORT_DIR) + 1) == 0);
	UT_ASSERT(strstr(path, ".tmp") != NULL);
	UT_ASSERT(strcmp(mode, PG_BINARY_W) == 0);
	UT_ASSERT_EQ(list_length(exportedSnapshots), 1);
	UT_ASSERT(!retention_lock);
	export_file_opens++;
	export_h_at_file = catalog_history_sample(&sample)
		&& sample.horizon_scn == export_expected_h;
	if (export_fault == 4)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("export cancel fixture")));
	if (export_fault == 1)
	{
		errno = ENOSPC;
		return NULL;
	}
	/* Only this anonymous temporary stream is real I/O. No PostgreSQL
	 * snapshot-directory file is created or renamed by this fixture. */
	return tmpfile();
}

int
FreeFile(FILE *file)
{
	long size;
	int result;

	export_file_closes++;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	size = ftell(file);
	UT_ASSERT(size > 0 && size < sizeof(export_text));
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	UT_ASSERT_EQ(fread(export_text, 1, size, file), size);
	export_text[size] = '\0';
	result = fclose(file);
	if (export_fault == 2)
	{
		errno = EIO;
		return EOF;
	}
	return result;
}

int
rename(const char *from, const char *to)
{
	UT_ASSERT(strncmp(from, SNAPSHOT_EXPORT_DIR "/", strlen(SNAPSHOT_EXPORT_DIR) + 1) == 0);
	UT_ASSERT(strncmp(to, SNAPSHOT_EXPORT_DIR "/", strlen(SNAPSHOT_EXPORT_DIR) + 1) == 0);
	export_renames++;
	if (export_fault == 3)
	{
		errno = EIO;
		return -1;
	}
	return 0;
}

int
unlink(const char *path)
{
	UT_ASSERT(strncmp(path, SNAPSHOT_EXPORT_DIR "/", strlen(SNAPSHOT_EXPORT_DIR) + 1) == 0);
	export_unlinks++;
	return 0;
}

static SnapshotData
snapshot_export_source(void)
{
	SnapshotData source;

	snapshot_clock = 40;
	(void) GetSnapshotData(&source);
	UT_ASSERT_EQ(source.cluster_source, SNAPSHOT_SOURCE_CLUSTER);
	UT_ASSERT_EQ(source.read_scn, 40);
	UT_ASSERT_EQ(proc.xmin, source.xmin);
	snapshot_clock = 100;
	export_fault = export_file_opens = export_file_closes = export_renames = export_unlinks = 0;
	export_h_at_file = false;
	export_text[0] = '\0';
	return source;
}

static void
snapshot_export_cleanup(void)
{
	List *owned = exportedSnapshots;
	ListCell *cell;
	ClusterUndoRetentionSample sample;

	UT_ASSERT_EQ(list_length(owned), 1);
	AtEOXact_Snapshot(false, true);
	UT_ASSERT_EQ(export_unlinks, 1);
	UT_ASSERT(exportedSnapshots == NIL && pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
	UT_ASSERT_EQ(proc.xmin, InvalidTransactionId);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, snapshot_clock);
	/* Native EOXact has retired references; release the allocations that a
	 * real TopTransactionContext would reclaim, without redoing its cleanup. */
	foreach(cell, owned)
	{
		ExportedSnapshot *saved = lfirst(cell);
		pfree(saved->snapshot);
		pfree(saved->snapfile);
		pfree(saved);
	}
	list_free(owned);
	export_fault = 0;
}

UT_TEST(export_original_registration_publishes_h_before_file_visibility)
{
	SnapshotData source = snapshot_export_source();
	ClusterUndoRetentionSample sample;
	char *name;
	char expected_epoch[64];

	UT_ASSERT(!catalog_history_sample(&sample));
	export_expected_h = 40;
	name = ExportSnapshot(&source);
	UT_ASSERT(export_h_at_file);
	UT_ASSERT_EQ(export_file_opens, 1);
	UT_ASSERT_EQ(export_file_closes, 1);
	UT_ASSERT_EQ(export_renames, 1);
	UT_ASSERT(strncmp(name, SNAPSHOT_EXPORT_DIR, strlen(SNAPSHOT_EXPORT_DIR)) != 0);
	UT_ASSERT(strstr(export_text, "clu_scn:40\n") != NULL);
	snprintf(expected_epoch, sizeof(expected_epoch), "clu_epoch:" UINT64_FORMAT "\n", source.read_epoch);
	UT_ASSERT(strstr(export_text, expected_epoch) != NULL);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 40);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	pfree(name);
	snapshot_export_cleanup();
}

UT_TEST(export_original_registration_preserves_other_older_snapshot)
{
	Snapshot older = registered(20);
	SnapshotData source = snapshot_export_source();
	ClusterUndoRetentionSample sample;
	char *name;

	export_expected_h = 20;
	name = ExportSnapshot(&source);
	UT_ASSERT(export_h_at_file);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 20);
	UnregisterSnapshot(older);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 40);
	pfree(name);
	snapshot_export_cleanup();
}

UT_TEST(export_errors_retain_original_h_until_transaction_cleanup)
{
	for (unsigned fault = 1; fault <= 4; fault++)
	{
		SnapshotData source = snapshot_export_source();
		ClusterUndoRetentionSample sample;
		volatile bool caught = false;

		export_fault = fault;
		export_expected_h = 40;
		error_code = 0;
		PG_TRY();
		{
			(void) ExportSnapshot(&source);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught && export_h_at_file);
		UT_ASSERT_EQ(error_code, fault == 4 ? ERRCODE_QUERY_CANCELED : ERRCODE_IO_ERROR);
		UT_ASSERT(!retention_lock);
		UT_ASSERT(catalog_history_sample(&sample));
		UT_ASSERT_EQ(sample.horizon_scn, 40);
		snapshot_export_cleanup();
	}
}
