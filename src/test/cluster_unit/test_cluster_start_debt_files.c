/*-------------------------------------------------------------------------
 *
 * test_cluster_start_debt_files.c
 *    Original control publisher and reader linked to the original Start actor.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_start_debt_files.c
 *
 * NOTES
 *    Files, complete codecs, publisher and reader are real. The linked Start
 *    fixture supplies the shared actor, CF and exception stack. Its process
 *    identity is controlled; this is not a backend recovery execution.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "miscadmin.h"
#include <unistd.h>
#include "unit_test.h"
#include "test_cluster_start_debt_files.h"
#undef UT_DEFINE_GLOBALS
#define UT_DEFINE_GLOBALS() /* The original Start fixture owns TAP bookkeeping. */

/* The original Start fixture uses PID 81 throughout its complete inputs. */
static pid_t start_debt_files_process(void) { return 81; }
#define getpid start_debt_files_process
#define CONTROL_DEBT_SHARED_ACTOR_TEST 1
#define CONTROL_DEBT_RESTART_READ_HOOK start_debt_files_read_step
#define CONTROL_DEBT_PUBLISH_TEST_MAIN start_debt_files_old_main
int start_debt_files_old_main(void);
#define cluster_control_root_debt_transition_readback_v2 start_debt_files_readback_v2
#include "test_cluster_control_debt_publish.c"
#undef cluster_control_root_debt_transition_readback_v2
#undef getpid

bool
cluster_control_root_debt_transition_get_v2(const ClusterControlDebtTransitionV2 *t,
	ClusterControlDebtTransitionViewV2 *out)
{
	return publisher_transition_get_v2(t, out);
}

int
start_debt_files_before(uint8 *pair, Size bytes)
{
	if (bytes != sizeof(restart_before)) abort();
	restart_distinct_bases = true;
	restart_begin(130, 2);
	if (snprintf(publisher_primary, sizeof(publisher_primary), "%s/pgrac_control_root", store_path) >= sizeof(publisher_primary)
		|| snprintf(publisher_backup, sizeof(publisher_backup), "%s/pgrac_control_root.bak", store_path) >= sizeof(publisher_backup)) abort();
	memcpy(pair, restart_before, bytes);
	return store_global;
}

ClusterControlDebtTransitionV2 *
start_debt_files_build(const struct ClusterRecoveryStartV1 *start,
	const ClusterControlRelmapSelectionV2 *selection)
{
	return control_debt_transition_build_v2(start, selection, restart_before,
		sizeof(restart_before), image, debt_batches, restart_nb, START_DEBT_FILES_BUDGET);
}

bool start_debt_files_publish(ClusterControlDebtTransitionV2 *t) { return publisher_run(t); }
void start_debt_files_reset(void) { reset_faults(); }
void
start_debt_files_counts(unsigned *r, unsigned *w, unsigned *s)
{
	*r = reads; *w = writes; *s = syncs;
}
uint64 start_debt_files_digest(uint8 digest[32]) { return restart_tree_digest(digest); }

void
start_debt_files_damage(unsigned kind)
{
	int fd, dir;
	uint8 byte;
	char name[96];
	if (kind < 2) {
		fd = openat(store_global, kind ? "pgrac_control_root.bak" : "pgrac_control_root", O_RDWR);
		if (fd < 0 || pread(fd, &byte, 1, 1400) != 1) abort();
		byte ^= 1;
		if (pwrite(fd, &byte, 1, 1400) != 1 || close(fd)) abort();
		return;
	}
	dir = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
	if (dir < 0) abort();
	if (kind == 2) {
		uint8 hash[32];
		debt_sha(image->pages + image->page_bytes - 33024, 33024, hash);
		debt_store_name(hash, false, name);
	} else if (kind == 3) debt_store_name(image->reference + 64, true, name);
	else if (kind == 4) debt_store_basis_name(restart_older_ref, name);
	else abort();
	if (unlinkat(dir, name, 0) || close(dir)) abort();
}

void
start_debt_files_end(ClusterControlDebtTransitionV2 **t)
{
	control_debt_transition_destroy_v2(t);
	restart_end();
}
