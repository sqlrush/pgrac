# 270_cluster_4_12a_undo_record_segment_reclaim.pl
#
# spec-4.12a D4 -- single-node undo record-segment reclaim, reproduce +
# regression.  The leak (spec-4.13 finding): record (undo data) segments only
# advanced ACTIVE->COMMITTED at TT-slot rollover, never at record-cursor
# rollover, so a few-transactions / heavy-undo workload left them ACTIVE
# forever and the cleaner (which only does COMMITTED->RECYCLABLE) could never
# reclaim them -- the pool grew monotonically to the hard cap (53R9E).
#
# Determinism: cluster_undo_test_force_segment_end() drives a record-cursor
# rollover on the NEXT undo write without producing 64 MB of undo (mirrors
# t/222), and autovacuum=off removes background undo writers, so the only undo
# producers are this test's foreground INSERTs.
#
#   L1  fix OFF -> record segments never drain -> pool fills to the hard cap
#       (the leak is reproduced; segment_hard_cap_fail_count > 0).
#   L2  fix ON  -> rollover drain + cleaner reclaim recycle the pool, so
#       sustained rollovers never hit the hard cap and the allocator reuses
#       reclaimed segments (segment_reuse_count > 0).
#   L3  quiesce -> after writes stop (registry empty => boundary infinite),
#       further rollovers keep REUSING the recycled pool rather than creating
#       new files, i.e. the pool stays bounded with no new transactions.
#
# Spec: spec-4.12a-undo-record-segment-reclaim.md
use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;

# value of a pg_cluster_state counter in the 'undo' category.
sub undo_counter
{
	my ($node, $key) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state
		    WHERE category = 'undo' AND key = '$key'});
}

# number of on-disk record-segment files for instance 0.
sub seg_files
{
	my ($node) = @_;
	my $dir = $node->data_dir . '/pg_undo/instance_0';
	opendir(my $dh, $dir) or return -1;
	my @f = grep { /^seg_\d+\.dat$/ } readdir($dh);
	closedir($dh);
	return scalar @f;
}

# number of on-disk record segments in SEGMENT_ACTIVE state.  The segment
# header (block 0) stores segment_state at byte offset 40 (UndoSegmentHeaderData);
# SEGMENT_ACTIVE == 1.  Counting ACTIVE segments directly measures the leak: a
# fair cleaner drains the whole inventory at quiesce, leaving only the current
# active record/TT segment ACTIVE, whereas the batch-window starvation leaves a
# large stuck ACTIVE backlog.
sub active_segments
{
	my ($node) = @_;
	my $dir = $node->data_dir . '/pg_undo/instance_0';
	opendir(my $dh, $dir) or return -1;
	my @f = grep { /^seg_\d+\.dat$/ } readdir($dh);
	closedir($dh);
	my $active = 0;
	for my $name (@f)
	{
		open(my $fh, '<', "$dir/$name") or next;
		binmode($fh);
		my $buf;
		sysseek($fh, 40, 0);
		my $n = sysread($fh, $buf, 1);
		close($fh);
		$active++ if defined($n) && $n == 1 && ord($buf) == 1; # SEGMENT_ACTIVE
	}
	return $active;
}

# one deterministic record-cursor rollover: arm the autoextend, then a single
# undo-producing INSERT trips it.  Returns ($rc, $stderr) of the INSERT.
sub roll
{
	my ($node, $id) = @_;
	$node->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()});
	my ($rc, undef, $err) = $node->psql('postgres', "INSERT INTO t270 VALUES ($id)");
	return ($rc, $err);
}

sub common_conf
{
	my ($cap, $rollover) = @_;
	return
		  "log_min_messages = warning\n"
		. "cluster.enabled = on\n"
		. "cluster.node_id = 0\n"
		. "cluster.allow_single_node = on\n"
		. "autovacuum = off\n"
		. "cluster.undo_cleaner_interval_ms = 200\n"
		. "cluster.undo_segments_max_per_instance = $cap\n"
		. "cluster.undo_record_segment_commit_on_rollover = $rollover\n";
}

# ===========================================================================
# L1 -- fix OFF reproduces the leak: the record-segment pool fills to the cap.
# ===========================================================================
{
	my $na = PgracClusterNode->new('reclaim_off');
	$na->init;
	$na->append_conf('postgresql.conf', common_conf(16, 'off'));
	$na->start;
	$na->safe_psql('postgres', 'CREATE TABLE t270 (id int)');
	$na->safe_psql('postgres', 'INSERT INTO t270 VALUES (0)'); # first active segment

	my $hard_cap_hit = 0;
	for my $i (1 .. 40)
	{
		my ($rc, $err) = roll($na, $i);
		if ($rc != 0)
		{
			like($err, qr/hard cap reached/,
				"L1 cycle $i: INSERT fails with the undo segment pool hard cap (53R9E)");
			$hard_cap_hit = 1;
			last;
		}
	}
	ok($hard_cap_hit,
		'L1 fix OFF reproduces the leak: record-segment pool reaches the hard cap');
	cmp_ok(undo_counter($na, 'segment_hard_cap_fail_count'), '>', 0,
		'L1 segment_hard_cap_fail_count > 0 (hard cap actually hit, not just a slow run)');

	$na->stop;
}

# ===========================================================================
# L2 -- fix ON keeps the pool bounded under sustained rollovers: each rollover
#       is lock-stepped to a cleaner reclaim, so the pool stays far below the
#       cap and far MORE rollovers than the cap never trip 53R9E (contrast L1,
#       which fills the same cap with the fix off).
# L3 -- that paced loop reclaims with the registry empty between autocommits
#       (boundary infinite), and the allocator reuses the recycled files in
#       place rather than growing the pool.
# ===========================================================================
{
	my $nb = PgracClusterNode->new('reclaim_on');
	$nb->init;
	$nb->append_conf('postgresql.conf', common_conf(16, 'on'));
	$nb->start;
	$nb->safe_psql('postgres', 'CREATE TABLE t270 (id int)');
	$nb->safe_psql('postgres', 'INSERT INTO t270 VALUES (0)');

	# Warm past the fixed first segment (which never drains), then drive far more
	# rollovers than the cap in small batches, each followed by a short quiesce
	# so the 200ms cleaner drains (ACTIVE->COMMITTED) and reclaims
	# (COMMITTED->RECYCLABLE) the batch before the next one -- keeping the live
	# pool well under the cap.  Every rollover retains its old segment at
	# rollover time (the rolling INSERT is itself in-flight), so the drains here
	# come entirely from the cleaner's quiesce pass (boundary infinite once the
	# writer commits) -- the spec L3 "falls back with no new transactions" path.
	$nb->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()});
	$nb->safe_psql('postgres', 'INSERT INTO t270 VALUES (1)');
	my $id = 1;
	for my $batch (1 .. 5)
	{
		for (1 .. 6)
		{
			$id++;
			$nb->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()});
			$nb->safe_psql('postgres', "INSERT INTO t270 VALUES ($id)");
		}
		sleep 1; # ~5 cleaner passes, no writer in flight: drain + reclaim the batch
	}
	# 30 rollovers >> cap 16: with the fix the pool recycles and never exhausts.
	is(undo_counter($nb, 'segment_hard_cap_fail_count'), '0',
		'L2 fix ON: 30 rollovers (~2x cap) recycle the pool, never trip the hard cap (vs L1 leak)');
	cmp_ok(undo_counter($nb, 'record_segments_committed'), '>', 0,
		'L2 record segments advance ACTIVE->COMMITTED (the leak-fix drain fires)');
	cmp_ok(undo_counter($nb, 'cleaner_segments_marked_recyclable'), '>', 0,
		'L3 cleaner reclaims drained segments COMMITTED->RECYCLABLE on the quiesce pass (boundary infinite)');
	# The hard cap physically bounds the file count; 30 rollovers staying within
	# it (with no hard-cap failure above) is the reuse-in-place proof.
	cmp_ok(seg_files($nb), '<=', 20,
		'L3 on-disk record-segment pool stays bounded near the cap (reuse in place, not the L1 runaway)');

	$nb->stop;
}

# ===========================================================================
# L4 -- a segment holding an in-flight (uncommitted) transaction's undo is
#       never mismarked COMMITTED: the in-flight guard retains it
#       (record_seg_commit_skipped_inflight increments), and the transaction
#       still commits correctly afterwards (its undo segment was retained).
# ===========================================================================
{
	my $nc = PgracClusterNode->new('inflight_guard');
	$nc->init;
	$nc->append_conf('postgresql.conf', common_conf(64, 'on'));
	$nc->start;
	$nc->safe_psql('postgres', 'CREATE TABLE t270 (id int)');
	$nc->safe_psql('postgres', 'INSERT INTO t270 VALUES (0)');

	my $skip_before = undo_counter($nc, 'record_seg_commit_skipped_inflight');

	# Session A: an in-flight writer that registers its first_undo_scn and stays
	# uncommitted, so the active-write boundary is finite at its SCN.
	my $a = $nc->background_psql('postgres', on_error_die => 1);
	$a->query_safe('BEGIN');
	$a->query_safe('INSERT INTO t270 VALUES (9999)');

	# Session B rolls the cursor away from the segment that still holds A's
	# in-flight undo.  The drain gate sees the segment sealed at-or-after the
	# boundary and RETAINS it -> the skip counter increments (guard fired).
	for my $i (1 .. 4)
	{
		$nc->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()});
		$nc->safe_psql('postgres', "INSERT INTO t270 VALUES ($i)");
	}
	cmp_ok(undo_counter($nc, 'record_seg_commit_skipped_inflight'), '>', $skip_before,
		'L4 in-flight segment is retained, not mismarked COMMITTED (8.A guard fires)');

	# A still commits correctly: its undo segment was retained, so the row lands.
	$a->query_safe('COMMIT');
	$a->quit;
	is($nc->safe_psql('postgres', 'SELECT count(*) FROM t270 WHERE id = 9999'), '1',
		'L4 the in-flight transaction commits correctly after its segment was retained');

	$nc->stop;
}

# ===========================================================================
# L5 -- crash-restart while the pool is churning (a crash-SAFETY test, not the
#       leak's root-cause evidence -- that is L7, which reproduces the leak with
#       no crash).  After a crash the in-memory active-write registry is gone, so
#       the boundary degrades to {infinite}; D3 keeps this safe because the
#       cleaner only starts at PM_RUN (after recovery) and there are no
#       unresolved prepared / in-flight writers, so reclaim resumes without false
#       reclaim or PANIC and committed data is intact.
# ===========================================================================
{
	my $nd = PgracClusterNode->new('crash_restart');
	$nd->init;
	# cap=128 >> the rollover counts below, so the pool never approaches the cap
	# even with zero cleaner help -- the crash-safety behaviour under test is
	# deterministic without pacing.
	$nd->append_conf('postgresql.conf', common_conf(128, 'on'));
	$nd->start;
	$nd->safe_psql('postgres', 'CREATE TABLE t270 (id int)');

	# Churn the pool (COMMITTED -> RECYCLABLE -> reuse in flight) right up to
	# the crash, with no clean shutdown checkpoint.
	for my $i (1 .. 40)
	{
		$nd->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()});
		$nd->safe_psql('postgres', "INSERT INTO t270 VALUES ($i)");
	}
	my $committed = $nd->safe_psql('postgres', 'SELECT count(*) FROM t270');

	$nd->stop('immediate'); # crash
	$nd->start;             # crash recovery

	my $log = slurp_file($nd->logfile);
	unlike($log, qr/PANIC/, 'L5 no PANIC recovering the undo pool after a crash');
	is($nd->safe_psql('postgres', 'SELECT count(*) FROM t270'), $committed,
		'L5 committed rows intact after crash-restart (no false reclaim of live undo)');

	# Post-restart the reclaim path is healthy: registry rebuilt empty (boundary
	# infinite), cleaner spawns at PM_RUN, more rollovers drain + reclaim again.
	# Driven in small batches with a settle (like L2) so the cleaner gets quiet
	# windows to drain the retained segments.
	my $committed0 = undo_counter($nd, 'record_segments_committed');
	my $rid = 99;
	for my $batch (1 .. 4)
	{
		for (1 .. 6) { $rid++; $nd->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()}); $nd->safe_psql('postgres', "INSERT INTO t270 VALUES ($rid)"); }
		sleep 1;
	}
	note("L5 diag post-restart: committed0=$committed0 committed="
		  . undo_counter($nd, 'record_segments_committed') . " marked="
		  . undo_counter($nd, 'cleaner_segments_marked_recyclable') . " skip="
		  . undo_counter($nd, 'record_seg_commit_skipped_inflight'));
	is(undo_counter($nd, 'segment_hard_cap_fail_count'), '0',
		'L5 post-restart: no hard cap (D3 boundary-infinite reclaim path is safe)');
	cmp_ok(undo_counter($nd, 'record_segments_committed'), '>', $committed0,
		'L5 post-restart: record-segment drains resume after crash recovery');

	$nd->stop;
}

# ===========================================================================
# L6 -- a prepared transaction's undo must never be reclaimed (it can still be
#       consumed by ROLLBACK PREPARED).  While any cluster-TT prepared xact is
#       unresolved the prepared guard (Q11-A minimal-safe) retains ALL
#       record-segment drains, and that retention survives a crash because
#       RecoverPreparedTransactions rebuilds the protected-slot view BEFORE the
#       cleaner starts at PM_RUN (D3).
# ===========================================================================
{
	my $ne = PgracClusterNode->new('prepared_guard');
	$ne->init;
	# cap=128 >> the rollover counts, so the pool never approaches the cap; the
	# prepared-guard retention + crash recovery under test stay deterministic.
	$ne->append_conf('postgresql.conf',
		common_conf(128, 'on') . "max_prepared_transactions = 10\n");
	$ne->start;
	$ne->safe_psql('postgres', 'CREATE TABLE t270 (id int)');
	$ne->safe_psql('postgres', 'INSERT INTO t270 VALUES (0)');

	# A prepared transaction whose INSERT undo lives in a record segment.
	$ne->safe_psql('postgres', q{
		BEGIN;
		INSERT INTO t270 VALUES (7777);
		PREPARE TRANSACTION 'gx_undo';
	});

	# While it is unresolved, drive rollovers: the prepared guard must retain
	# EVERY record-segment drain (none advance to COMMITTED).
	my $committed_before = undo_counter($ne, 'record_segments_committed');
	for my $i (1 .. 20)
	{
		$ne->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()});
		$ne->safe_psql('postgres', "INSERT INTO t270 VALUES ($i)");
	}
	sleep 1; # >= 3 cleaner passes at 200ms -- cleaner must also retain
	is(undo_counter($ne, 'record_segments_committed'), $committed_before,
		'L6 unresolved prepared xact: prepared guard retains ALL record-segment drains');
	cmp_ok(undo_counter($ne, 'record_seg_commit_skipped_inflight'), '>', 0,
		'L6 the prepared guard actually fires (drain attempts retained)');

	# Crash with the prepared xact still pending.  The in-memory active-write
	# registry is lost, but RecoverPreparedTransactions rebuilds TwoPhaseState
	# (numPrepXacts) before the cleaner starts at PM_RUN, so the drain gate still
	# sees an unresolved prepared xact and retains the prepared undo segment.
	$ne->stop('immediate');
	$ne->start;
	is($ne->safe_psql('postgres',
			q{SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'gx_undo'}),
		'1', 'L6 prepared xact recovered after crash');
	sleep 1; # let the post-restart cleaner run a few passes with boundary infinite

	# ROLLBACK PREPARED must still find its undo: a false reclaim across the
	# crash (D3 bug) would lose it.  Success + the row gone proves it survived.
	$ne->safe_psql('postgres', q{ROLLBACK PREPARED 'gx_undo'});
	is($ne->safe_psql('postgres', q{SELECT count(*) FROM t270 WHERE id = 7777}),
		'0', 'L6 ROLLBACK PREPARED consumes the retained undo across the crash (D3)');

	# Once the prepared xact resolves (no unresolved prepared xact remains),
	# the drain gate stops retaining and record-segment drains resume.
	note("L6 diag pre-resume: prepared="
		  . $ne->safe_psql('postgres', q{SELECT count(*) FROM pg_prepared_xacts})
		  . " committed=" . undo_counter($ne, 'record_segments_committed')
		  . " skip=" . undo_counter($ne, 'record_seg_commit_skipped_inflight')
		  . " marked=" . undo_counter($ne, 'cleaner_segments_marked_recyclable'));
	my $rid6 = 199;
	for my $batch (1 .. 4)
	{
		for (1 .. 6) { $rid6++; $ne->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()}); $ne->safe_psql('postgres', "INSERT INTO t270 VALUES ($rid6)"); }
		sleep 1;
	}
	note("L6 diag post-resume: prepared="
		  . $ne->safe_psql('postgres', q{SELECT count(*) FROM pg_prepared_xacts})
		  . " committed=" . undo_counter($ne, 'record_segments_committed')
		  . " skip=" . undo_counter($ne, 'record_seg_commit_skipped_inflight')
		  . " marked=" . undo_counter($ne, 'cleaner_segments_marked_recyclable'));
	cmp_ok(undo_counter($ne, 'record_segments_committed'), '>', $committed_before,
		'L6 after prepared resolution: record-segment drains resume (guard releases on resolution)');

	$ne->stop;
}

# ===========================================================================
# L7 -- Finding C decisive regression (NO crash): the cleaner must reclaim the
#       WHOLE segment inventory, not just the first batch window.  The leak's
#       true root cause is cleaner scan-window starvation, not a post-crash bug:
#       undo_cleaner_run_pass restarted every pass at the first segment and
#       decremented its batch budget (cluster.undo_cleaner_batch_segments,
#       default 8) for every readable header, so once the inventory grew past
#       the batch window the high-id ACTIVE segments were NEVER scanned ->
#       never drained -> the pool grew to the hard cap and never fell back even
#       at full quiesce.  This reproduces it with NO crash at all (the L5/L6
#       crash cases below only made the backlog larger / front-loaded; they are
#       crash-SAFETY tests, not the leak's root-cause evidence).  The
#       round-robin scan cursor makes the cleaner resume across passes so every
#       segment is eventually visited regardless of batch size.
# ===========================================================================
{
	my $nf = PgracClusterNode->new('finding_c_fairness');
	$nf->init;
	# Default batch window (8) is the whole point; cap large so the leak shows
	# as a stuck ACTIVE backlog that never falls back, not a hard-cap abort.
	$nf->append_conf('postgresql.conf', common_conf(128, 'on'));
	$nf->start;
	$nf->safe_psql('postgres', 'CREATE TABLE t270 (id int)');
	$nf->safe_psql('postgres', 'INSERT INTO t270 VALUES (0)');

	# Unpaced burst: 40 rollovers >> the batch window (8).  The cleaner cannot
	# keep up during the burst, so a backlog of ACTIVE record segments
	# accumulates well beyond the first 8 segments of the inventory.
	for my $i (1 .. 40)
	{
		$nf->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()});
		$nf->safe_psql('postgres', "INSERT INTO t270 VALUES ($i)");
	}
	my $active_mid = active_segments($nf);
	# The inventory must exceed the batch window for this test to exercise the
	# fairness path (holds with or without the fix: the unpaced burst grows the
	# pool faster than reuse reclaims it).
	cmp_ok(seg_files($nf), '>', 8,
		"L7 burst grows the inventory past the batch window (files > 8); ACTIVE backlog mid-burst = $active_mid");

	# QUIESCE: stop writing entirely.  With no in-flight writer the active-write
	# boundary is infinite, so EVERY sealed rolled-away ACTIVE segment is
	# drainable.  A fair cleaner sweeps the whole inventory across passes and the
	# ACTIVE backlog falls to the only segments the 8.A drain gate permanently
	# excludes: the spec-3.4b fixed-first segment (guard 2), the current active
	# record segment (guard 3), and the current active TT segment (guard 4) -- at
	# most three, all correctly retained.  Poll instead of a fixed sleep
	# (condition-based wait): break once the backlog clears; the batch-window-
	# starved cleaner never clears it (it stays ~= the mid-burst backlog).
	my $excluded_floor = 3;
	my $active_after = $active_mid;
	for (1 .. 15)
	{
		sleep 1;    # 200ms cleaner interval => ~5 passes per poll
		$active_after = active_segments($nf);
		last if $active_after <= $excluded_floor;
	}
	cmp_ok($active_after, '<=', $excluded_floor,
		"L7 at quiesce the cleaner reclaims the WHOLE inventory, not just the batch window "
		  . "(ACTIVE $active_mid -> $active_after; only the <=3 drain-excluded segments remain)");
	is(undo_counter($nf, 'segment_hard_cap_fail_count'), '0',
		'L7 the pool never runs away to the hard cap when the cleaner scan is fair');
	cmp_ok(undo_counter($nf, 'cleaner_segments_marked_recyclable'), '>', 8,
		'L7 the cleaner reclaims past the batch window (marked > 8: high-id segments reached)');

	$nf->stop;
}

# ===========================================================================
# L8 -- 8.A x round-robin: an in-flight writer pins the active-write boundary,
#       so every segment sealed at/after it must be RETAINED even while the
#       round-robin cleaner sweeps the whole (large) inventory.  L4 proves the
#       in-flight guard with a tiny inventory and L7 sweeps with no in-flight
#       writer; this is the intersection -- a buggy sweep (e.g. mishandling the
#       cursor wrap) could falsely reclaim a buried in-flight segment.  The
#       drain gate is unchanged by the fairness fix, so the sweep must visit
#       everything yet drain nothing while the writer is live, then drain the
#       whole inventory once it commits (liveness resumes, no undo lost).
# ===========================================================================
{
	my $ng = PgracClusterNode->new('roundrobin_inflight_8a');
	$ng->init;
	$ng->append_conf('postgresql.conf', common_conf(128, 'on'));
	$ng->start;
	$ng->safe_psql('postgres', 'CREATE TABLE t270 (id int)');

	# Session A: an in-flight writer. Its INSERT undo pins the active-write
	# boundary at its first_undo_scn until it commits, so NOTHING sealed after it
	# may drain (Q2-A' conservative boundary).
	my $a = $ng->background_psql('postgres', on_error_die => 1);
	$a->query_safe('BEGIN');
	$a->query_safe('INSERT INTO t270 VALUES (8888)');

	# Large burst (>> batch window) so the round-robin cleaner sweeps the whole
	# inventory across passes while A stays in-flight.
	for my $i (1 .. 40)
	{
		$ng->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()});
		$ng->safe_psql('postgres', "INSERT INTO t270 VALUES ($i)");
	}
	sleep 2;    # ~10 round-robin cleaner passes sweep the inventory
	cmp_ok(active_segments($ng), '>', 8,
		'L8 while the writer is in-flight the round-robin sweep RETAINS the inventory '
		  . '(8.A in-flight guard holds across the full sweep; no false reclaim)');

	note("L8 diag pre-commit: active=" . active_segments($ng)
		  . " committed=" . undo_counter($ng, 'record_segments_committed')
		  . " skip=" . undo_counter($ng, 'record_seg_commit_skipped_inflight')
		  . " marked=" . undo_counter($ng, 'cleaner_segments_marked_recyclable'));

	# A commits -> boundary infinite. Quiesce; the round-robin cleaner now drains
	# the whole inventory (the in-flight segment was retained, not lost).
	$a->query_safe('COMMIT');
	$a->quit;
	my $active_after = 99;
	for (1 .. 15)
	{
		sleep 1;
		$active_after = active_segments($ng);
		last if $active_after <= 3;
	}
	note("L8 diag post-commit+quiesce: active=$active_after"
		  . " committed=" . undo_counter($ng, 'record_segments_committed')
		  . " skip=" . undo_counter($ng, 'record_seg_commit_skipped_inflight')
		  . " marked=" . undo_counter($ng, 'cleaner_segments_marked_recyclable'));
	cmp_ok($active_after, '<=', 3,
		"L8 after the writer commits the round-robin cleaner drains the whole inventory "
		  . "(liveness resumes; ACTIVE -> $active_after)");
	is($ng->safe_psql('postgres', 'SELECT count(*) FROM t270 WHERE id = 8888'),
		'1', 'L8 the in-flight writer commits correctly (its retained undo was never falsely reclaimed)');

	$ng->stop;
}

done_testing();
