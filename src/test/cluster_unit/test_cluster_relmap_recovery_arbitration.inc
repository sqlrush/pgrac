/* PGRAC: original arbitration + authority pair I/O, with controlled opaque
 * terminal/round/merger/namespace-X providers. Not native recovery authority.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-11-pre2-p0-m5-e-relmap-retention.md, section 4.
 */
#include "../../backend/cluster/cluster_relmap_arb.c"

bool cluster_shared_catalog = true;
static ClusterConf arbitration_topology;
ClusterConf *ClusterConfShmem = &arbitration_topology;
static bool arbitration_claim, arbitration_terminal_ok, arbitration_current;
static bool arbitration_drop_claim, arbitration_drop_x;
static XidStatus arbitration_status;
static unsigned arbitration_reads, arbitration_rechecks, arbitration_releases;
static unsigned arbitration_invalidations, arbitration_acks;
static unsigned arbitration_error;
static unsigned char arbitration_cookie;
static ClusterRelmapV2Namespace arbitration_identity;
static ClusterRelmapV2View arbitration_pending;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
cluster_recovery_merge_claim_is_held(void)
{
	return arbitration_claim;
}

bool
cluster_relmap_recovery_terminal_read(ClusterRelmapRecoveryHandle *handle,
									  const ClusterRelmapV2Namespace *ns,
									  const ClusterRelmapV2View *view,
									  ClusterRelmapRecoveryTerminalV1 *out)
{
	arbitration_reads++;
	if (arbitration_error == 1)
		siglongjmp(*PG_exception_stack, 1);
	UT_ASSERT(handle == (ClusterRelmapRecoveryHandle *)&arbitration_cookie);
	UT_ASSERT(!retention_held);
	memset(out, 0, sizeof(*out));
	if (!arbitration_terminal_ok || ns == NULL || view == NULL
		|| memcmp(ns, &arbitration_identity, sizeof(*ns)) != 0
		|| memcmp(view, &arbitration_pending, sizeof(*view)) != 0)
		return false;
	out->status = arbitration_status;
	out->top_full_xid = view->pending_owner.top_full_xid;
	if (arbitration_drop_claim)
		arbitration_claim = false;
	if (arbitration_drop_x)
		relmap_x_held = false;
	return true;
}

bool
cluster_relmap_recovery_handle_matches_v1(ClusterRelmapRecoveryHandle *handle,
										  const ClusterRelmapV2Namespace *ns,
										  const ClusterRelmapV2View *view)
{
	arbitration_rechecks++;
	if (arbitration_error == 2)
		siglongjmp(*PG_exception_stack, 1);
	UT_ASSERT(handle == (ClusterRelmapRecoveryHandle *)&arbitration_cookie);
	UT_ASSERT(!retention_held);
	return arbitration_current && memcmp(ns, &arbitration_identity, sizeof(*ns)) == 0
		   && memcmp(view, &arbitration_pending, sizeof(*view)) == 0;
}

void
cluster_relmap_recovery_handle_release_v1(ClusterRelmapRecoveryHandle **handle)
{
	UT_ASSERT(handle != NULL && *handle == (ClusterRelmapRecoveryHandle *)&arbitration_cookie);
	arbitration_releases++;
	*handle = NULL;
}

void
SendSharedInvalidMessages(const SharedInvalidationMessage *msgs, int n)
{
	ClusterRelmapV2View view;

	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(msgs[0].rm.id, SHAREDINVALRELMAP_ID);
	UT_ASSERT_EQ(msgs[0].rm.dbId, arbitration_identity.dbid);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&arbitration_identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(!view.has_pending && view.pending_complete);
	UT_ASSERT_EQ(view.committed_generation, 8);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40001);
	UT_ASSERT(arbitration_rechecks != 0 && file_syncs == 2 && dir_syncs == 2);
	arbitration_invalidations++;
	if (arbitration_error == 3)
		siglongjmp(*PG_exception_stack, 1);
}

ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *msgs, int n)
{
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(msgs[0].rm.dbId, arbitration_identity.dbid);
	UT_ASSERT_EQ(arbitration_invalidations, 1);
	arbitration_acks++;
	return CLUSTER_SINVAL_ACK_DONE;
}

static ClusterRelmapRecoveryHandle *
arbitration_setup(bool shared, bool already_published)
{
	ClusterRelmapV2Image image = staged_image();
	unsigned char bytes[2176];

	arbitration_identity = identity;
	arbitration_identity.shared = shared;
	arbitration_identity.dbid = shared ? InvalidOid : 42;
	setup(&arbitration_identity);
	UT_ASSERT(stage(&arbitration_identity));
	if (already_published) {
		image.identity = arbitration_identity;
		image.committed = image.pending;
		image.committed_generation = image.pending_generation;
		image.pending_generation = 0;
		memset(&image.pending_owner, 0, sizeof(image.pending_owner));
		memset(&image.pending, 0, sizeof(image.pending));
		UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)),
					 CLUSTER_RELMAP_V2_VALID);
		write_copy(0, bytes, sizeof(bytes));
	}
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&arbitration_identity, &arbitration_pending),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	reset_io();
	relmap_x_held = arbitration_claim = arbitration_terminal_ok = arbitration_current = true;
	arbitration_drop_claim = arbitration_drop_x = false;
	cluster_shared_catalog = true;
	cluster_node_id = 2; /* The pending owner is foreign; no local status fallback. */
	arbitration_topology.node_count = 2;
	arbitration_status = TRANSACTION_STATUS_COMMITTED;
	arbitration_reads = arbitration_rechecks = arbitration_releases = 0;
	arbitration_invalidations = arbitration_acks = 0;
	arbitration_error = 0;
	return (ClusterRelmapRecoveryHandle *)&arbitration_cookie;
}

static void
arbitration_check_pin(const ClusterRelmapRetentionDirectory *before)
{
	UT_ASSERT_EQ(memcmp(before, &writer_retention, sizeof(*before)), 0);
	UT_ASSERT_EQ(arbitration_releases, 1);
	UT_ASSERT_EQ(io_under_retention_lock, 0);
}

UT_TEST(test_recovery_arbitration_clears_both_namespaces_from_native_outcome)
{
	for (int shared = 0; shared <= 1; shared++)
		for (int committed = 0; committed <= 1; committed++) {
			ClusterRelmapRecoveryHandle *handle = arbitration_setup(shared, false);
			ClusterRelmapRetentionDirectory before = writer_retention;
			ClusterRelmapV2View view;

			arbitration_status
				= committed ? TRANSACTION_STATUS_COMMITTED : TRANSACTION_STATUS_ABORTED;
			UT_ASSERT_EQ(cluster_relmap_arbitrate_pending_v2(&handle, &arbitration_identity,
															 &arbitration_pending),
						 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
			UT_ASSERT(handle == NULL);
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&arbitration_identity, &view),
						 CLUSTER_RELMAP_V2_PAIR_VALID);
			UT_ASSERT(view.pending_complete && !view.has_pending);
			UT_ASSERT_EQ(view.committed_generation, committed ? 8 : 7);
			UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, committed ? 40001 : 40000);
			UT_ASSERT_EQ(arbitration_reads, 1);
			UT_ASSERT_EQ(arbitration_invalidations, committed);
			UT_ASSERT_EQ(arbitration_acks, committed);
			arbitration_check_pin(&before);
		}
}

UT_TEST(test_recovery_arbitration_preserves_pending_original_base_at_publish_cut)
{
	for (int shared = 0; shared <= 1; shared++) {
		ClusterRelmapRecoveryHandle *handle = arbitration_setup(shared, true);
		ClusterRelmapRetentionDirectory before = writer_retention;

		UT_ASSERT_EQ(arbitration_pending.committed_generation, 8);
		UT_ASSERT_EQ(arbitration_pending.pending_committed_generation, 7);
		UT_ASSERT_EQ(cluster_relmap_arbitrate_pending_v2(&handle, &arbitration_identity,
														 &arbitration_pending),
					 CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT_EQ(arbitration_invalidations, 1);
		arbitration_check_pin(&before);
	}
}

UT_TEST(test_recovery_arbitration_requires_original_merger_and_namespace_x)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterRelmapRecoveryHandle *handle = arbitration_setup(true, false);
		ClusterRelmapRetentionDirectory before = writer_retention;

		if (fault == 0)
			arbitration_claim = false;
		if (fault == 1)
			relmap_x_held = false;
		if (fault == 2)
			cluster_shared_catalog = false;
		UT_ASSERT_EQ(cluster_relmap_arbitrate_pending_v2(&handle, &arbitration_identity,
														 &arbitration_pending),
					 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT(handle == NULL);
		UT_ASSERT_EQ(arbitration_reads, 0);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(arbitration_invalidations, 0);
		arbitration_check_pin(&before);
	}
	cluster_shared_catalog = true;
}

UT_TEST(test_recovery_arbitration_unknown_never_becomes_abort)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterRelmapRecoveryHandle *handle = arbitration_setup(true, false);
		ClusterRelmapRetentionDirectory before = writer_retention;

		arbitration_terminal_ok = fault != 0;
		arbitration_status
			= fault == 1 ? TRANSACTION_STATUS_IN_PROGRESS : TRANSACTION_STATUS_SUB_COMMITTED;
		UT_ASSERT_EQ(cluster_relmap_arbitrate_pending_v2(&handle, &arbitration_identity,
														 &arbitration_pending),
					 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(arbitration_invalidations, 0);
		arbitration_check_pin(&before);
	}
}

UT_TEST(test_recovery_arbitration_full_view_or_namespace_drift_refuses)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterRelmapRecoveryHandle *handle = arbitration_setup(true, false);
		ClusterRelmapRetentionDirectory before = writer_retention;
		ClusterRelmapV2Namespace ns = arbitration_identity;
		ClusterRelmapV2View view = arbitration_pending;

		if (fault == 0)
			ns.database_incarnation++;
		if (fault == 1)
			view.pending_owner.top_full_xid += UINT64CONST(0x100000000);
		if (fault == 2)
			view.pending_committed.mappings[0].mapfilenumber++;
		UT_ASSERT_EQ(cluster_relmap_arbitrate_pending_v2(&handle, &ns, &view),
					 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(writes, 0);
		arbitration_check_pin(&before);
	}
}

UT_TEST(test_recovery_arbitration_rechecks_claim_and_x_after_terminal_io)
{
	for (int fault = 0; fault < 2; fault++) {
		ClusterRelmapRecoveryHandle *handle = arbitration_setup(true, false);
		ClusterRelmapRetentionDirectory before = writer_retention;

		arbitration_drop_claim = fault == 0;
		arbitration_drop_x = fault == 1;
		UT_ASSERT_EQ(cluster_relmap_arbitrate_pending_v2(&handle, &arbitration_identity,
														 &arbitration_pending),
					 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(writes, 0);
		arbitration_check_pin(&before);
	}
}

UT_TEST(test_recovery_arbitration_foreign_pair_replacement_is_not_cleared)
{
	ClusterRelmapRecoveryHandle *handle = arbitration_setup(true, false);
	ClusterRelmapRetentionDirectory before = writer_retention;
	ClusterRelmapV2Image image = staged_image();
	unsigned char bytes[2176], after[2176];

	image.pending_owner.boot_incarnation++;
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
	write_copy(1, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_relmap_arbitrate_pending_v2(&handle, &arbitration_identity, &arbitration_pending),
		CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(writes, 0);
	read_copy(1, after);
	UT_ASSERT_EQ(memcmp(bytes, after, sizeof(bytes)), 0);
	arbitration_check_pin(&before);
}

UT_TEST(test_recovery_arbitration_io_unknown_keeps_pin_and_consumes_custody)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterRelmapRecoveryHandle *handle = arbitration_setup(true, false);
		ClusterRelmapRetentionDirectory before = writer_retention;

		if (fault == 0)
			fail_file_sync = 1;
		if (fault == 1)
			fail_rename = 2;
		if (fault == 2)
			fail_directory_close = true;
		UT_ASSERT_EQ(cluster_relmap_arbitrate_pending_v2(&handle, &arbitration_identity,
														 &arbitration_pending),
					 CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT(handle == NULL);
		UT_ASSERT(writes > 0);
		UT_ASSERT_EQ(arbitration_invalidations, 0);
		arbitration_check_pin(&before);
	}
}

UT_TEST(test_recovery_arbitration_postwrite_source_drift_cannot_acknowledge)
{
	ClusterRelmapRecoveryHandle *handle = arbitration_setup(true, false);
	ClusterRelmapRetentionDirectory before = writer_retention;

	arbitration_current = false;
	UT_ASSERT_EQ(
		cluster_relmap_arbitrate_pending_v2(&handle, &arbitration_identity, &arbitration_pending),
		CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT(writes > 0 && clean_pair(true)); /* UNKNOWN does not undo durable bytes. */
	UT_ASSERT(handle == NULL);
	UT_ASSERT_EQ(arbitration_invalidations, 0);
	arbitration_check_pin(&before);
}

UT_TEST(test_recovery_arbitration_error_consumes_only_local_custody)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		ClusterRelmapRecoveryHandle *volatile handle = arbitration_setup(true, false);
		ClusterRelmapRetentionDirectory before = writer_retention;
		volatile bool caught = false;

		arbitration_error = fault;
		PG_TRY();
		{
			(void)cluster_relmap_arbitrate_pending_v2((ClusterRelmapRecoveryHandle **)&handle,
													  &arbitration_identity, &arbitration_pending);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught && handle == NULL);
		UT_ASSERT_EQ(arbitration_invalidations, fault == 3);
		UT_ASSERT_EQ(writes > 0, fault > 1);
		arbitration_check_pin(&before);
	}
}
