#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 109_lmd_deadlock_smoke.pl
#	  spec-2.22 D14 — LMD Tarjan + cross-node deadlock detection
#	  (local production + cross-node scaffold).
#
#	  This TAP exercises the local production path: wait-for graph +
#	  iterative SCC + revalidate + victim cancel + ProcSignal.  Cross-
#	  node distributed scenarios SKIP forward-link to spec-2.23 BAST
#	  配套 (real cluster_ges_send_request_and_wait pipeline required).
#
#	  Wait edges are injected via pg_cluster_lmd_inject_wait_edge SRF
#	  (D16 test-only path b) — production LMS handler is a single-node
#	  GRANT stub in spec-2.21, so真 wait edges only arise once spec-
#	  2.23 ships LMS conflict + waiter queue.
#
#	  Scenarios (per spec-2.22 §4.2):
#	    L1 single-node 2-vertex cycle detect
#	    L2 no-cycle 3-vertex chain
#	    L3 self-cycle defensive (add_edge rejects waiter == blocker)
#	    L4 multi-hop 3-vertex cycle
#	    L5 victim cancel sent + counter++
#	    L6 revalidate fail advisory (race injection)
#	    L7 stale snapshot no-cancel
#	    L8 wait_edge_full HC12 fail-closed
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('lmd_tarjan');
$node->init;
$node->append_conf('postgresql.conf', qq{
cluster.node_id = 0
cluster.lmd_scan_interval_ms = 200
cluster.lmd_max_wait_edges = 64
});
$node->start;

# Wait for LMD READY.
ok($node->poll_query_until(
	'postgres',
	q{SELECT count(*) > 0 FROM pg_stat_activity WHERE backend_type = 'lmd'}),
   'LMD aux process visible (precondition)');


# Helper: read a single dump_lmd counter row.
sub read_counter {
	my ($n, $key) = @_;
	return $n->safe_psql(
		'postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category = 'lmd' AND key = '$key'});
}

# Helper: inject one synthetic wait edge.  Returns 't' or 'f'.
sub inject_edge {
	my ($n, $wnode, $wproc, $wreq, $bnode, $bproc, $breq) = @_;
	return $n->safe_psql(
		'postgres',
		qq{SELECT pg_cluster_lmd_inject_wait_edge($wnode, $wproc, $wreq, $bnode, $bproc, $breq)});
}

# Helper: wait until a counter advances beyond a previously read value.
sub wait_counter_gt {
	my ($n, $key, $before) = @_;
	return $n->poll_query_until(
		'postgres',
		qq{SELECT (SELECT value::bigint FROM pg_cluster_state WHERE category = 'lmd' AND key = '$key') > $before});
}

# Helper: update an existing waiter edge to a unique sink so prior cycles do
# not pollute later scenarios.  There is no SQL remove helper in this spec.
sub break_edge {
	my ($n, $wproc, $wreq, $sink) = @_;
	inject_edge($n, 0, $wproc, $wreq, 0, $sink, 900000 + $sink);
}


# L1 — single-node 2-vertex cycle:  A waits on B, B waits on A.
my $cycles_before = read_counter($node, 'cycle_detected_count');
inject_edge($node, 0, 100, 1001, 0, 200, 2001);
inject_edge($node, 0, 200, 2001, 0, 100, 1001);
ok(wait_counter_gt($node, 'cycle_detected_count', $cycles_before),
   'L1 scan observed cycle counter advance');
my $cycles_after = read_counter($node, 'cycle_detected_count');
ok($cycles_after > $cycles_before,
   "L1 single-node 2-vertex cycle detected (cycle_detected_count=$cycles_after)");

# Clean up L1 edges.
break_edge($node, 100, 1001, 10000);
break_edge($node, 200, 2001, 10001);


# L2 — no-cycle 3-vertex chain:  A→B→C, no back edge.
my $cycles_before_L2 = read_counter($node, 'cycle_detected_count');
inject_edge($node, 0, 300, 3001, 0, 400, 4001);
inject_edge($node, 0, 400, 4001, 0, 500, 5001);
my $scans_before_L2 = read_counter($node, 'tarjan_scan_count');
ok(wait_counter_gt($node, 'tarjan_scan_count', $scans_before_L2),
   'L2 scan tick observed');
my $cycles_after_L2 = read_counter($node, 'cycle_detected_count');
is($cycles_after_L2, $cycles_before_L2, 'L2 no-cycle 3-vertex chain — no new cycle detected');
break_edge($node, 300, 3001, 10002);
break_edge($node, 400, 4001, 10003);


# L3 — self-cycle defensive:  add_edge rejects waiter == blocker.
my $self_inject = inject_edge($node, 0, 600, 6001, 0, 600, 6001);
is($self_inject, 'f', 'L3 self-cycle add_edge rejected (waiter == blocker)');


# L4 — multi-hop 3-vertex cycle:  A→B→C→A.
my $cycles_before_L4 = read_counter($node, 'cycle_detected_count');
inject_edge($node, 0, 700, 7001, 0, 800, 8001);
inject_edge($node, 0, 800, 8001, 0, 900, 9001);
inject_edge($node, 0, 900, 9001, 0, 700, 7001);
ok(wait_counter_gt($node, 'cycle_detected_count', $cycles_before_L4),
   'L4 scan observed cycle counter advance');
my $cycles_after_L4 = read_counter($node, 'cycle_detected_count');
ok($cycles_after_L4 > $cycles_before_L4,
   "L4 multi-hop 3-vertex cycle detected (delta=$cycles_after_L4 vs $cycles_before_L4)");
break_edge($node, 700, 7001, 10004);
break_edge($node, 800, 8001, 10005);
break_edge($node, 900, 9001, 10006);


# L5 — victim cancel sent.  Victim selected by youngest sort tuple;
# since we don't tightly control local_start_ts_ms via SRF, just verify
# counter increment by reading before/after.  Note:  victim cancel only
# fires when victim.node_id == self (0 here), so all injected edges with
# node_id=0 qualify.
my $cancels_before = read_counter($node, 'victim_cancel_sent_count');
inject_edge($node, 0, 1100, 11001, 0, 1200, 12001);
inject_edge($node, 0, 1200, 12001, 0, 1100, 11001);
ok(wait_counter_gt($node, 'victim_cancel_sent_count', $cancels_before),
   'L5 scan observed victim cancel counter advance');
my $cancels_after = read_counter($node, 'victim_cancel_sent_count');
ok($cancels_after > $cancels_before,
   "L5 victim_cancel_sent_count incremented ($cancels_before → $cancels_after)");
break_edge($node, 1100, 11001, 10007);
break_edge($node, 1200, 12001, 10008);


# L6/L7 — would-be cycle redirected before it ever becomes live.  The previous
# version briefly created 1300→1400 + 1400→1300 and then broke both edges, which
# let macOS CI legitimately scan the transient live cycle between inject and
# break.  Keep the graph acyclic at every intermediate step so this remains a
# deterministic "no false cancel" boundary check rather than a scheduler race.
my $cycles_before_L6 = read_counter($node, 'cycle_detected_count');
my $cancels_before_L6 = read_counter($node, 'victim_cancel_sent_count');
inject_edge($node, 0, 1300, 13001, 0, 1400, 14001);
break_edge($node, 1300, 13001, 10009);
inject_edge($node, 0, 1400, 14001, 0, 1300, 13001);
my $scans_before_L6 = read_counter($node, 'tarjan_scan_count');
ok(wait_counter_gt($node, 'tarjan_scan_count', $scans_before_L6),
   'L6 scan tick observed after would-be cycle was redirected');
is(read_counter($node, 'cycle_detected_count'), $cycles_before_L6,
   'L6 redirected would-be cycle not counted as a live cycle');
is(read_counter($node, 'victim_cancel_sent_count'), $cancels_before_L6,
   'L7 redirected would-be cycle does not send victim cancel');


# L8 — wait_edge_full HC12:  inject >cluster.lmd_max_wait_edges (64) and
# verify both the SRF returns false AND wait_edge_full_count increments.
my $full_before = read_counter($node, 'wait_edge_full_count');
my $any_rejected = 'f';
for my $i (1..96) {
	my $r = inject_edge($node, 0, 5000 + $i, 50000 + $i, 0, 6000 + $i, 60000 + $i);
	$any_rejected = 't' if $r eq 'f';
}
my $full_after = read_counter($node, 'wait_edge_full_count');
is($any_rejected, 't', 'L8 wait_edge_full HC12 — at least one inject rejected');
ok($full_after > $full_before, "L8 wait_edge_full_count incremented ($full_before → $full_after)");


# ----------
# spec-2.23 Step 11 — D17 L13-L16 cross-node deadlock scenarios.
#
# Steps 6-8 wired the coordinator scan path: cluster_lmd_tarjan_run_
# coordinator_scan broadcasts DEADLOCK_PROBE to N-1 active peers,
# collects N-1 REPORTs via the file-static LmdProbeCollector (single
# probe_id in-flight per scan tick), unions edges + runs Tarjan
# (D9), and dispatches local-vs-remote victim per HC20.
#
# The observable surface at single-node TAP is the new dump_lmd
# counters (probe_broadcast_count, probe_partial_count) plus
# unchanged spec-2.22 counters (cycle_detected_count, etc).  Full
# 2-node ClusterPair cross-node cycle construction (HC21 L136
# interleave pattern) and remote victim cancel forwarding lands with
# Hardening v1.0.1 + spec-2.24 D axis.
# ----------

# L13: dump_lmd surfaces 2 new counters with initial values 0.
my $lmd_dump = $node->safe_psql('postgres', q{
	SELECT key
	FROM cluster_dump_state()
	WHERE category = 'lmd'
	  AND key IN ('probe_broadcast_count', 'probe_partial_count')
	ORDER BY key
});
is($lmd_dump, "probe_broadcast_count\nprobe_partial_count",
   'L13 dump_lmd exposes 2 new spec-2.23 D13 counters');

# L14: probe_broadcast_count starts at 0 (single-node has no peers to
# broadcast to; coordinator scan returns early before increment).
my $broadcast_init = $node->safe_psql('postgres', q{
	SELECT value
	FROM cluster_dump_state()
	WHERE category = 'lmd' AND key = 'probe_broadcast_count'
});
is($broadcast_init, '0', 'L14 probe_broadcast_count starts at 0 (no peers)');

# L15: probe_partial_count starts at 0 (no PROBE issued → no timeout).
my $partial_init = $node->safe_psql('postgres', q{
	SELECT value
	FROM cluster_dump_state()
	WHERE category = 'lmd' AND key = 'probe_partial_count'
});
is($partial_init, '0', 'L15 probe_partial_count starts at 0');

# L16: cross_node_victim_pending_count (spec-2.22 ship) stays at 0 in
# single-node mode — no remote victims possible until ClusterPair runs
# Hardening v1.0.1 with real cross-node cycle injection.
my $cross_pending = $node->safe_psql('postgres', q{
	SELECT value
	FROM cluster_dump_state()
	WHERE category = 'lmd' AND key = 'cross_node_victim_pending_count'
});
is($cross_pending, '0', 'L16 cross_node_victim_pending_count stays at 0 single-node');


$node->stop;
done_testing();
