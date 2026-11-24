/* Finite original child authentication/table observations. No pipe, process
 * creation or native initdb success is simulated by this provider. */
BackendType MyBackendType;
#ifndef CLOG_CREATION_HAVE_POSTMASTER_ENV
bool IsPostmasterEnvironment;
#endif
static bool creation_has_request, creation_has_table;
static unsigned creation_info_reads, creation_drift_at;
static PgracInitdbRecoveryContext creation_context;
static ClusterControlNativeTableInfoV2 creation_info;

const PgracInitdbRecoveryContext *
cluster_wal_thread_initdb_recovery_context(void)
{
	return creation_has_request ? &creation_context : NULL;
}

bool
cluster_control_native_files_info_v2(ClusterControlNativeTableInfoV2 *out)
{
	creation_info_reads++;
	if (!creation_has_table) return false;
	*out = creation_info;
	if (creation_drift_at && creation_info_reads >= creation_drift_at)
		out->table_generation++;
	return true;
}

static void
creation_support_reset(void)
{
	creation_has_request = creation_has_table = false;
	creation_info_reads = creation_drift_at = 0;
	memset(&creation_context, 0, sizeof(creation_context));
	memset(&creation_info, 0, sizeof(creation_info));
	MyBackendType = B_BACKEND;
	IsUnderPostmaster = IsPostmasterEnvironment = false;
}

static void
creation_support_select(const ClusterRecoveryFileIdentityV2 *file,
	uint64 generation, uint64 creator)
{
	creation_support_reset();
	creation_has_request = creation_has_table = true;
	creation_context.magic = PGRAC_INITDB_RECOVERY_MAGIC;
	creation_context.version = 2;
	creation_context.creator_pid = creator;
	creation_context.boot_file_count = 1;
	creation_context.boot_files[0] = *file;
	creation_info.mode = CLUSTER_NATIVE_FILES_CREATION_V2;
	creation_info.table_generation = generation;
	creation_info.startup_generation = creator;
	creation_info.creation = creation_context;
	MyBackendType = B_STANDALONE_BACKEND;
	cluster_enabled = false;
	cluster_node_id = -1;
}
