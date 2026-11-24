/*
 * Read the original PCM statistics without cluster_dump_state's undo ensure.
 * Test module only; never injects state or grants resource authority.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors
 */
#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "cluster/cluster_pcm_lock.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_pgrac_pcm_capacity_stats);
Datum test_pgrac_pcm_capacity_stats(PG_FUNCTION_ARGS);

Datum
test_pgrac_pcm_capacity_stats(PG_FUNCTION_ARGS)
{
	PcmGrdLifecycleStats stats;
	StringInfoData out;

	cluster_pcm_grd_lifecycle_stats_snapshot(&stats);
	initStringInfo(&out);
	appendStringInfo(&out, "{\"capacity\":%d", cluster_pcm_grd_capacity());
	appendStringInfo(&out, ",\"live_entries\":\"" UINT64_FORMAT "\"", stats.live_entries);
	appendStringInfo(&out, ",\"peak_live_entries\":\"" UINT64_FORMAT "\"", stats.peak_live_entries);
	appendStringInfo(&out, ",\"binding_generation\":\"" UINT64_FORMAT "\"", stats.binding_generation);
	appendStringInfo(&out, ",\"capacity_wait_count\":\"" UINT64_FORMAT "\"", stats.capacity_wait_count);
	appendStringInfo(&out, ",\"reclaim_success_count\":\"" UINT64_FORMAT "\"}", stats.reclaim_success_count);
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
}
