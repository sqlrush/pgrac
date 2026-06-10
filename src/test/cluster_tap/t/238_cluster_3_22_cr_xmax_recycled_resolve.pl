#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 238_cluster_3_22_cr_xmax_recycled_resolve.pl
#	  spec-3.22 -- CR xmax recycled-slot resolve via the retention invariant.
#
#	  spec-3.21 closed correctness but left a liveness debt: a committed deleter
#	  whose ITL slot was recycled fails closed 53R9F because the exact-key
#	  resolver finds no commit_scn source.  spec-3.22 turns the COMMON case of
#	  that 53R9F into a sound INVISIBLE: a durable by-xid 0-match proves (via the
#	  spec-3.12 retention invariant) that the deleter committed below the horizon,
#	  hence before any current snapshot -- the delete is visible -> the CR tuple
#	  is invisible.  The shortcut is taken ONLY when the retention proof holds
#	  (own-instance, GUC on, no ungated recycle this incarnation, valid horizon,
#	  horizon <= read_scn); otherwise it stays fail-closed (规则 8.A, no Option A).
#
#	  L1  gate-on recycle-heavy workload: the balance invariant holds (no
#	      false-invisible from the RECYCLED->invisible shortcut) AND the shortcut
#	      actually fires (cr_xmax_recycled_invisible_count > 0) -- the spec-3.22
#	      liveness win is real, not a no-op.
#	  L2  gate-off control: the SAME workload stays clean and resolves nothing on
#	      the CR xmax path (native visibility).
#	  L3  retention-off soundness: with retention disabled, an ungated COMMITTED
#	      recycle is recorded (retention_off_recycle_count > 0) and the shortcut is
#	      disabled this incarnation (sticky fail-closed) -- the invariant STILL
#	      holds (no false-invisible despite the off-window), proving the proof
#	      gate, not luck, is what keeps it sound.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.22-cr-xmax-recycled-slot-resolve.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $clients = $ENV{S322_CLIENTS} // 8;
my $jobs    = $ENV{S322_JOBS}    // 4;
my $seconds = $ENV{S322_SECONDS} // 6;

sub _new_node
{
	my ($name, $gate) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf('postgresql.conf',
		    "shared_buffers = 128MB\n"
		  . "cluster.enabled = on\n"
		  . "cluster.node_id = 0\n"
		  . "cluster.allow_single_node = on\n"
		  . "cluster.interconnect_tier = stub\n"
		  # spec-3.24 D1: pin the no-peer CR-gate fast path OFF so this CR-path
		  # test exercises the CR/SCN path (default is now ON; t/239 covers the
		  # fast path + the differential equivalence).
		  . "cluster.cr_gate_no_peer_fastpath = off\n"
		  . "cluster.cr_mvcc_gate = $gate\n"
		  . "cluster.undo_buffers = 128\n"
		  . "autovacuum = off\n");
	$node->start;
	return $node;
}

# plpgsql TPC-B body with a ROW_COUNT assert: a false-invisible (a row the gate
# wrongly hides) surfaces as a distinct 'UPDATE0' error rather than a silently
# lost update, so the balance invariant + this assert together fence soundness.
my $tpcb_fn = q{
	CREATE OR REPLACE FUNCTION tpcb_txn(p_aid int, p_bid int, p_tid int, p_delta int)
	RETURNS void LANGUAGE plpgsql AS $$
	DECLARE rc int;
	BEGIN
	  UPDATE pgbench_accounts SET abalance = abalance + p_delta WHERE aid = p_aid;
	  GET DIAGNOSTICS rc = ROW_COUNT;
	  IF rc <> 1 THEN RAISE EXCEPTION 'UPDATE0 accounts aid=% rc=%', p_aid, rc; END IF;
	  PERFORM abalance FROM pgbench_accounts WHERE aid = p_aid;
	  UPDATE pgbench_tellers SET tbalance = tbalance + p_delta WHERE tid = p_tid;
	  GET DIAGNOSTICS rc = ROW_COUNT;
	  IF rc <> 1 THEN RAISE EXCEPTION 'UPDATE0 tellers tid=% rc=%', p_tid, rc; END IF;
	  UPDATE pgbench_branches SET bbalance = bbalance + p_delta WHERE bid = p_bid;
	  GET DIAGNOSTICS rc = ROW_COUNT;
	  IF rc <> 1 THEN RAISE EXCEPTION 'UPDATE0 branches bid=% rc=%', p_bid, rc; END IF;
	  INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
	    VALUES (p_tid, p_bid, p_aid, p_delta, now());
	END $$;
};

sub _write_script
{
	my ($node) = @_;
	my $script = $node->basedir . '/s322_tpcb.sql';
	open(my $fh, '>', $script) or die $!;
	print $fh "\\set aid random(1, 100000 * :scale)\n"
		. "\\set bid random(1, 1 * :scale)\n"      # ONE hot branch row -> recycle pressure
		. "\\set tid random(1, 10 * :scale)\n"
		. "\\set delta random(-5000, 5000)\n"
		. "SELECT tpcb_txn(:aid, :bid, :tid, :delta);\n";
	close($fh);
	return $script;
}

sub _invariant
{
	my ($node) = @_;
	return $node->safe_psql('postgres', q{
		SELECT (SELECT coalesce(sum(abalance),0) FROM pgbench_accounts)
		         = (SELECT coalesce(sum(delta),0) FROM pgbench_history)
		   AND (SELECT coalesce(sum(tbalance),0) FROM pgbench_tellers)
		         = (SELECT coalesce(sum(delta),0) FROM pgbench_history)
		   AND (SELECT coalesce(sum(bbalance),0) FROM pgbench_branches)
		         = (SELECT coalesce(sum(delta),0) FROM pgbench_history);});
}

sub _counter
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return ($v eq '') ? 0 : $v + 0;
}

# Gate-ON tolerated retryable errors: a recycled deleter that cannot be proven
# (delayed cleanout / wrap / retention-off / no-proof) -> 53R9F snapshot-too-old.
# Safe: aborts/retries, never a wrong/lost result (spec-3.22 acceptance #3/#4).
my $tolerated_on =
  qr{cluster\ CR\ cannot\ resolve\ commit_scn
	|cluster\ CR\ cannot\ resolve\ commit_scn\ for\ recycled
	|cluster\ CR\ durable\ scan\ unavailable
	|cluster\ CR\ xmax\ visibility\ unresolved
	|cluster\ CR\ cannot\ reconstruct\ block
	|cluster\ undo\ record\ alloc\ failed
	|ITL\ slot\ overflow
	|deadlock\ detected
	|could\ not\ serialize\ access
	|canceling\ statement\ due\ to\ lock\ timeout}xi;

sub _run_tpcb
{
	my ($node, $secs) = @_;
	$secs //= $seconds;
	my $script = _write_script($node);
	my ($out, $err);
	$node->run_log(
		[ 'pgbench', '-n', '-f', $script, '-c', $clients, '-j', $jobs, '-T', $secs,
			'-p', $node->port, '-h', $node->host, 'postgres' ],
		'>', \$out, '2>', \$err);
	return ($err // '') . ($out // '');
}

sub _unexpected_lines
{
	my ($log, $tolerated) = @_;
	my @bad;
	for my $line (split /\n/, $log) {
		next unless $line =~ /(?:ERROR|FATAL|PANIC):|pgbench:\s+error:/i;
		next if $line =~ /pgbench:\s+error:\s+Run was aborted/i;
		next if $line =~ $tolerated;
		push @bad, $line;
	}
	return @bad;
}

# ----------------------------------------------------------------------------
# L1: gate ON -- the spec-3.22 liveness win + soundness.  The recycle-heavy hot-
# branch workload makes committed deleters' slots recycle; the CR xmax gate must
# resolve them to INVISIBLE via the retention proof (no 53R9F flood) AND keep the
# balance invariant (no false-invisible).
# ----------------------------------------------------------------------------
my $on = _new_node('s322_on', 'on');
$on->command_ok(
	[ 'pgbench', '-i', '-s', '1', '-q', '-p', $on->port, '-h', $on->host, 'postgres' ],
	'L1 pgbench init');
$on->safe_psql('postgres', $tpcb_fn);
my $log_on = _run_tpcb($on);
my @bad_on = _unexpected_lines($log_on, $tolerated_on);
is(scalar @bad_on, 0, 'L1 (gate on): no unexpected errors (only 53R9F-class tolerated)')
  or diag("unexpected:\n" . join("\n", @bad_on));
is(_invariant($on), 't',
	'L1 (gate on): committed balance invariant holds (no false-invisible from RECYCLED->invisible)');
my $rinv = _counter($on, 'cr', 'cr_xmax_recycled_invisible_count');
cmp_ok($rinv, '>', 0,
	"L1: RECYCLED->invisible shortcut fired ($rinv) -- spec-3.22 removes the recycled-slot 53R9F");
# every recycled-invisible decision was proof-gated: an ungated recycle would
# have disqualified the shortcut, so this stays 0 in the all-on incarnation.
is(_counter($on, 'undo', 'retention_off_recycle_count'), 0,
	'L1: no ungated recycle (retention stayed on) -- every shortcut had a valid proof');

# ----------------------------------------------------------------------------
# L2: gate OFF control -- SAME workload, native visibility.  Invariant holds and
# the CR xmax buckets stay zero (the CR gate is the only thing under test).
# ----------------------------------------------------------------------------
my $off = _new_node('s322_off', 'off');
$off->command_ok(
	[ 'pgbench', '-i', '-s', '1', '-q', '-p', $off->port, '-h', $off->host, 'postgres' ],
	'L2 pgbench init');
$off->safe_psql('postgres', $tpcb_fn);
my $log_off = _run_tpcb($off);
# ITL-slot overflow / lock contention is a pre-existing cluster-mode artifact of
# the single hot-branch row (INITRANS=8 < concurrent writers); it is retryable and
# orthogonal to the CR gate (gate=off still runs the ITL).  The CR xmax path NOT
# being taken on gate-off is enforced by the bucket==0 assertion below, not here.
my @bad_off = _unexpected_lines($log_off, $tolerated_on);
is(scalar @bad_off, 0, 'L2 (gate off control): only retryable contention, no hard errors')
  or diag("unexpected:\n" . join("\n", @bad_off));
is(_invariant($off), 't', 'L2 (gate off control): committed balance invariant holds');
is(_counter($off, 'cr', 'cr_xmax_recycled_invisible_count'), 0,
	'L2 (gate off): CR xmax recycled path never taken (native visibility)');

# ----------------------------------------------------------------------------
# L3: retention-off soundness -- the off-window guard.  Warm up with retention ON
# (shortcut fires), then disable retention: ungated COMMITTED recycles are
# recorded and the shortcut is disabled for this incarnation (sticky).  The
# invariant must STILL hold -- the proof gate, not luck, keeps it sound.
# ----------------------------------------------------------------------------
my $ret = _new_node('s322_retoff', 'on');
$ret->command_ok(
	[ 'pgbench', '-i', '-s', '1', '-q', '-p', $ret->port, '-h', $ret->host, 'postgres' ],
	'L3 pgbench init');
$ret->safe_psql('postgres', $tpcb_fn);
_run_tpcb($ret);    # warm up with retention ON: the shortcut fires
my $shortcut_before = _counter($ret, 'cr', 'cr_xmax_recycled_invisible_count');
cmp_ok($shortcut_before, '>', 0, 'L3 warm-up (retention on): shortcut fired before the off-window');

$ret->safe_psql('postgres', 'ALTER SYSTEM SET cluster.undo_retention_horizon_enabled = off');
$ret->safe_psql('postgres', 'SELECT pg_reload_conf()');
my $log_ret = _run_tpcb($ret);    # ungated recycles happen here
my @bad_ret = _unexpected_lines($log_ret, $tolerated_on);
is(scalar @bad_ret, 0, 'L3 (retention off): no unexpected errors (53R9F-class tolerated)')
  or diag("unexpected:\n" . join("\n", @bad_ret));

cmp_ok(_counter($ret, 'undo', 'retention_off_recycle_count'), '>', 0,
	'L3: ungated COMMITTED recycle recorded (retention_off_recycle_count > 0)');
is(_invariant($ret), 't',
	'L3 (retention off): invariant holds -- no false-invisible despite the off-window');
is(_counter($ret, 'cr', 'cr_xmax_recycled_invisible_count'), $shortcut_before,
	'L3: RECYCLED->invisible shortcut disabled while retention is off');

# L3b: the sticky guard OUTLIVES the off-window.  Re-enable retention -- now the
# GUC leg (b) passes again, so the ONLY thing still disabling the shortcut is the
# sticky retention_off_recycle_count (leg c).  Re-run the recycle-heavy workload:
# the RECYCLED path IS exercised (the no-proof bucket grows), but the shortcut must
# STAY disabled (recycled_invisible flat).  This isolates the sticky counter from
# the live-GUC leg -- if the counter were cleared on re-enable, recycled_invisible
# would resume growing here.  L3 alone could not prove this (GUC-off masks leg c).
$ret->safe_psql('postgres', 'ALTER SYSTEM SET cluster.undo_retention_horizon_enabled = on');
$ret->safe_psql('postgres', 'SELECT pg_reload_conf()');
my $shortcut_pre_reenable = _counter($ret, 'cr', 'cr_xmax_recycled_invisible_count');
my $noproof_pre_reenable  = _counter($ret, 'cr', 'cr_xmax_scan_unavail_or_no_proof_count');
_run_tpcb($ret);    # retention back ON, but this incarnation already saw an off-window

cmp_ok(_counter($ret, 'undo', 'retention_off_recycle_count'), '>', 0,
	'L3b: retention_off_recycle_count stays set after re-enable (sticky, not cleared)');
cmp_ok(_counter($ret, 'cr', 'cr_xmax_scan_unavail_or_no_proof_count'), '>', $noproof_pre_reenable,
	'L3b: recycled deleters were exercised and fail-closed (no-proof bucket grew)');
is(_counter($ret, 'cr', 'cr_xmax_recycled_invisible_count'), $shortcut_pre_reenable,
	'L3b: shortcut STAYS disabled after retention re-enable (sticky guard leg c, not the live GUC)');
is(_invariant($ret), 't', 'L3b (retention re-enabled): invariant still holds');

$on->stop;
$off->stop;
$ret->stop;

done_testing();
