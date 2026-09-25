/* PGRAC: immutable history IO with native file/crypto/resource contracts.
 * Only the disposable directory and held-CF fact are test-local; this cannot
 * publish a root or admit a live writer. Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include <sys/stat.h>
#include "access/xlog.h"
#include "fmgr.h"
#include "miscadmin.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_guc.h"
#include "../../../backend/cluster/cluster_control_root_private.h"

static char *history_native_root;
static bool
history_native_cf(LOCKMODE mode)
{
	return mode == ExclusiveLock;
}

ClusterControlRootResult native_history_prepare(const ControlRootImage *, uint32,
												const ClusterWalHistoryImage *, uint64,
												const uint8[16], ClusterWalHistoryStage *);
ClusterControlRootResult native_history_install(ClusterWalHistoryStage *);
ClusterControlRootResult native_history_discard(ClusterWalHistoryStage *);
#define cluster_shared_data_dir history_native_root
#define cluster_cf_held_is_clusterwide history_native_cf
#define cluster_wal_history_prepare native_history_prepare
#define cluster_wal_history_install native_history_install
#define cluster_wal_history_discard native_history_discard
#include "../../../backend/cluster/cluster_wal_history.c"
#undef cluster_shared_data_dir
#endif

PG_FUNCTION_INFO_V1(test_pgrac_wal_history_native);
Datum
test_pgrac_wal_history_native(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("native history test requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		char scratch[MAXPGPATH], path[MAXPGPATH];
		const char *dirs[] = { "global", "global/wal_history", "global/wal_history/thread_1",
							   "global/wal_history/thread_1/.staging" };
		ControlRootImage *root = palloc0(sizeof(*root));
		ClusterWalHistoryImage *history = palloc0(sizeof(*history));
		ClusterControlRootSnapshot *record = &root->records[0];
		ClusterWalHistoryStage stage;
		uint8 uuid[16] = { 1 };
		ClusterControlRootResult result;

		snprintf(scratch, sizeof(scratch), "%s/native-history-XXXXXX", DataDir);
		if (!mkdtemp(scratch))
			elog(ERROR, "native history fixture mkdtemp failed");
		history_native_root = scratch;
		for (unsigned i = 0; i < lengthof(dirs); i++) {
			snprintf(path, sizeof(path), "%s/%s", scratch, dirs[i]);
			if (mkdir(path, 0700))
				elog(ERROR, "native history fixture mkdir failed");
		}
		root->header.format_version = 2;
		root->header.system_identifier = GetSystemIdentifier();
		memset(root->header.storage_uuid, 3, 16);
		memset(root->header.authority_uuid, 5, 16);
		root->present[0] = true;
		record->identity.system_identifier = root->header.system_identifier;
		memcpy(record->identity.storage_uuid, root->header.storage_uuid, 16);
		memcpy(record->identity.authority_uuid, root->header.authority_uuid, 16);
		record->identity.origin_thread_id = 1;
		record->identity.origin_owner_incarnation = 99;
		record->identity.root_lineage_seq = 9;
		record->identity.thread_claim_created_at = 23;
		record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
		record->root_publish_seq = 1;
		record->root_flags
			= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
		record->checkpoint_tli = 1;
		record->checkpoint_lower_lsn = 0x1000000;
		record->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
		record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE;

		result = native_history_prepare(root, 0, history, 1, uuid, &stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			elog(ERROR, "native history preparation refused: %d", result);
		for (int i = 0; i < 2; i++) {
			result = native_history_install(&stage);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				elog(ERROR, "native history installation refused: %d", result);
		}
		result = native_history_discard(&stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			elog(ERROR, "native history discard refused: %d", result);
		/* The disposable cluster owns fixture cleanup; production discard must
		 * never remove a formal history object, even after root CAS refusal. */
		history_native_root = NULL;
		pfree(history);
		pfree(root);
		PG_RETURN_BOOL(true);
	}
#else
	ereport(ERROR, (errmsg("PGRAC cluster build required")));
	PG_RETURN_NULL();
#endif
}
