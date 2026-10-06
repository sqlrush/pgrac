/*-------------------------------------------------------------------------
 *
 * cluster_storage_quorum_backend.c
 *    Stop SQL service when the local storage observation is no longer valid.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_storage_quorum_backend.c
 *
 * NOTES
 *    PGRAC-original code; exported symbols use the cluster_ prefix.
 *    Spec: spec-s9p2-06-online-membership.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_storage_quorum.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

static TimeoutId storage_backend_timeout = MAX_TIMEOUTS;
static volatile sig_atomic_t storage_backend_check_pending;

/* PG invokes timeout callbacks in SIGALRM context. No catalog, provider,
 * shared lock, or error reporting is permitted here. */
static void
storage_backend_timeout_handler(void)
{
	storage_backend_check_pending = true;
	InterruptPending = true;
	SetLatch(MyLatch);
}

static void
storage_backend_require_eligibility(void)
{
	if (!cluster_storage_quorum_allows_node(cluster_node_id))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_QUORUM_LOST_BACKEND),
						errmsg("terminating connection: storage quorum eligibility is unavailable"),
						errhint("Reconnect to an admitted cluster member.")));
}

/*
 * cluster_storage_quorum_check_sql -- Admit command and executor entry.
 * Inputs: current storage observation, including in parallel workers.
 * Returns: void on admission; FATAL when shared eligibility is unavailable.
 * Side Effects: arms one periodic backend timeout; native mode is unchanged.
 * Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_storage_quorum_check_sql(void)
{
	if (!cluster_shared_config)
		return;
	storage_backend_require_eligibility();
	if (!IsUnderPostmaster)
		return;
	if (storage_backend_timeout == MAX_TIMEOUTS)
		storage_backend_timeout = RegisterTimeout(USER_TIMEOUT, storage_backend_timeout_handler);
	if (!get_timeout_active(storage_backend_timeout)) {
		int period_ms = Max(1, Min(cluster_quorum_poll_interval_ms, 1000));
		enable_timeout_every(storage_backend_timeout,
							 TimestampTzPlusMilliseconds(GetCurrentTimestamp(), period_ms),
							 period_ms);
	}
}

/*
 * cluster_storage_quorum_check_interrupts -- Check active SQL at a safe point.
 * Inputs: timeout reminder; caller has passed ProcessInterrupts holdoff guards.
 * Returns: void, or FATAL if an active transaction has lost eligibility.
 * Side Effects: consumes the reminder. Idle sessions recheck at command entry.
 * Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_storage_quorum_check_interrupts(void)
{
	if (!storage_backend_check_pending)
		return;
	storage_backend_check_pending = false;
	if (cluster_shared_config && IsUnderPostmaster && IsTransactionState())
		storage_backend_require_eligibility();
}
