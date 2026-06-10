#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 237_cluster_3_21_cr_xmax_visibility.pl
#	  spec-3.21 -- CR image xmax-side visibility resolve.
#
#	  Root cause (D0.6): cluster_visibility_decide_cr_tuple treated ANY valid
#	  xmax on a CR image as "deleted" -> invisible.  But a valid xmax only proves
#	  some xact wrote a delete mark, not that the delete COMMITTED at/before
#	  read_scn.  When the deleter is in-progress / aborted / committed-after-
#	  read_scn, the row was LIVE at read_scn and must be VISIBLE.  Mis-hiding it
#	  produced a silent hot-row UPDATE 0 -> lost update (the pgbench TPC-B balance
#	  invariant broke under cluster.cr_mvcc_gate=on while gate=off stayed clean).
#	  Fix (D1/D2): resolve the deleter's commit-state vs read_scn; in-progress/
#	  aborted/post-read -> visible; committed-pre-read -> invisible; an unresolved
#	  committed commit_scn fails closed 53R9F (never silently invisible).
#
#	  Three gates (user 3-tier):
#	  L1  gate-on hot-row concurrency: UPDATE 0 == 0, the committed balance
#	      invariant holds, only the 53R9F retryable fail-closed is tolerated.
#	  L2  gate-off control: the SAME workload stays clean (no UPDATE 0, invariant
#	      holds, no errors) -- proves the break is the CR gate, not the workload.
#	  L9  deterministic: B holds an in-progress xmax on the hot row; A must SEE it
#	      (RR snapshot) and A's UPDATE must NOT return UPDATE 0 -- it blocks on B's
#	      row lock (the scan found the row) and EPQ-applies on B's committed value.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.21-cr-image-xmax-visibility-resolve.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";
use Time::HiRes qw(usleep);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $clients = $ENV{S321_CLIENTS} // 8;
my $jobs    = $ENV{S321_JOBS}    // 4;
my $seconds = $ENV{S321_SECONDS} // 8;

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

# The plpgsql TPC-B body: identical statements to the builtin tpcb-like, plus a
# ROW_COUNT assert so a scan-skip (the spec-3.21 false-invisible) surfaces as a
# distinct 'UPDATE0' error instead of a silently lost update.
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
	my $script = $node->basedir . '/s321_tpcb.sql';
	open(my $fh, '>', $script) or die $!;
	print $fh "\\set aid random(1, 100000 * :scale)\n"
		. "\\set bid random(1, 1 * :scale)\n"      # ONE hot branch row
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

# Tolerated retryable fail-closed (gate ON only): a recycled committed deleter
# whose commit_scn is unresolvable -> 53R9F snapshot-too-old.  Safe: it aborts/
# retries the txn, never a wrong/lost result (spec-3.21 acceptance #1).
my $tolerated_on =
  qr{cluster\ CR\ cannot\ resolve\ commit_scn
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
	my ($node) = @_;
	my $script = _write_script($node);
	my ($out, $err);
	$node->run_log(
		[ 'pgbench', '-n', '-f', $script, '-c', $clients, '-j', $jobs, '-T', $seconds,
			'-p', $node->port, '-h', $node->host, 'postgres' ],
		'>', \$out, '2>', \$err);
	return ($err // '') . ($out // '');
}

sub _unexpected_lines
{
	my ($all, $tolerated) = @_;
	my @bad;
	for my $line (split /\n/, $all)
	{
		next if defined $tolerated && $line =~ $tolerated;
		next if $line =~ /pgbench:\s+error:\s+Run was aborted/i;
		push @bad, $line if $line =~ /(?:ERROR|FATAL|PANIC):|pgbench:\s+error:/i;
	}
	return @bad;
}

# ----------------------------------------------------------------------
# L1: gate ON, single hot branch row -- the spec-3.21 reproducer.
# ----------------------------------------------------------------------
my $on = _new_node('s321_l1', 'on');
$on->command_ok([ 'pgbench', '-i', '-s', '1', '-q', '-p', $on->port, '-h', $on->host, 'postgres' ],
	'L1 pgbench init');
$on->safe_psql('postgres', $tpcb_fn);
my $all_on = _run_tpcb($on);

my @bad_on = _unexpected_lines($all_on, $tolerated_on);
diag("L1 first 2KB:\n" . substr($all_on, 0, 2048)) if ($all_on =~ /UPDATE0 / || @bad_on);
ok($all_on !~ /UPDATE0 /, 'L1 (gate on): no hot-row UPDATE 0 [spec-3.21 false-invisible fix]');
ok($all_on !~ /(?:FATAL|PANIC):/, 'L1 (gate on): no FATAL/PANIC');
ok(!@bad_on, 'L1 (gate on): no unexpected ERROR (only 53R9F-class retryable tolerated)');
is(_invariant($on), 't', 'L1 (gate on): committed balance invariant holds (no lost update)');
$on->stop;

# ----------------------------------------------------------------------
# L2: gate OFF control -- SAME workload must be perfectly clean.  This proves
# the L1 break (pre-3.21) was the CR gate, not the workload/harness.
# ----------------------------------------------------------------------
my $off = _new_node('s321_l2', 'off');
$off->command_ok([ 'pgbench', '-i', '-s', '1', '-q', '-p', $off->port, '-h', $off->host, 'postgres' ],
	'L2 pgbench init');
$off->safe_psql('postgres', $tpcb_fn);
my $all_off = _run_tpcb($off);

# With the gate OFF the cluster still writes undo/ITL (cluster.enabled=on), so
# native + cluster-resource retryable contention errors are expected under -c8 on
# one hot row.  The control proves two CR-gate-specific things are ABSENT: the
# spec-3.21 false-invisible (no UPDATE 0) and any CR-gate visibility error (the
# gate short-circuits off, so cluster_cr_satisfies_mvcc never runs).
my $tolerated_off =
  qr{deadlock\ detected
	|could\ not\ serialize\ access
	|canceling\ statement\ due\ to\ lock\ timeout
	|cluster\ undo\ record\ alloc\ failed
	|ITL\ slot\ overflow}xi;
my @bad_off = _unexpected_lines($all_off, $tolerated_off);
my $cr_off = $all_off =~
  /cluster CR cannot resolve commit_scn|cluster CR durable scan unavailable|cluster CR xmax visibility|cluster CR cannot reconstruct/i;
diag("L2 first 2KB:\n" . substr($all_off, 0, 2048)) if ($all_off =~ /UPDATE0 / || @bad_off || $cr_off);
ok($all_off !~ /UPDATE0 /, 'L2 (gate off control): no UPDATE 0 (no false-invisible without the gate)');
ok(!$cr_off, 'L2 (gate off control): no CR-gate visibility error (the gate is off)');
ok(!@bad_off, 'L2 (gate off control): only native/cluster-resource retryable contention errors');
is(_invariant($off), 't', 'L2 (gate off control): committed balance invariant holds');
$off->stop;

# ----------------------------------------------------------------------
# L9: deterministic in-progress-xmax visibility + UPDATE wait/EPQ.
# ----------------------------------------------------------------------
my $n = _new_node('s321_l9', 'on');
$n->safe_psql('postgres', q{
	CREATE TABLE hot (id int primary key, v int);
	INSERT INTO hot VALUES (1, 100);
	UPDATE hot SET v = v + 1 WHERE id = 1;
	UPDATE hot SET v = v + 1 WHERE id = 1;});   # hot.v = 102 (committed)

# A_rr fixes read_scn with a repeatable-read snapshot (sees 102).
my $arr = $n->background_psql('postgres', on_error_die => 1);
$arr->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
is($arr->query_safe('SELECT v FROM hot WHERE id = 1'), '102', 'L9 A_rr snapshot reads hot.v = 102');

# B updates the hot row and HOLDS (uncommitted xmax = B); block_scn moves past
# A_rr's read_scn, so A_rr's next read takes the CR gate.
my $b = $n->background_psql('postgres', on_error_die => 1);
$b->query_safe('BEGIN');
$b->query_safe('UPDATE hot SET v = v + 1000 WHERE id = 1');

# Core fix: in-progress xmax on the CR image must NOT be treated as deleted.
is($arr->query_safe('SELECT v FROM hot WHERE id = 1'), '102',
	'L9 A_rr still sees hot.v = 102 (in-progress xmax NOT deleted) [spec-3.21]');

# A read-committed UPDATE must FIND the row (block on B's row lock), not return
# UPDATE 0.  A bounded lock_timeout makes this deterministic without the fragile
# background-psql fire/drain: a blocked UPDATE is canceled (proving the scan
# found the row -> wait/EPQ semantics engage); a false-invisible would instead
# return "UPDATE 0" with no wait.  (The full EPQ no-lost-update path under
# concurrency is covered end-to-end by L1's invariant.)
my ($rc, $aout, $aerr) = $n->psql('postgres',
	'SET lock_timeout = 2000; UPDATE hot SET v = v + 1 WHERE id = 1;');
like($aerr, qr/canceling statement due to lock timeout/i,
	'L9 A_upd UPDATE found the row and blocked on B (not UPDATE 0) [spec-3.21]');
unlike(($aout // '') . ($aerr // ''), qr/UPDATE 0/,
	'L9 A_upd did not return UPDATE 0 (no false-invisible)');

# B commits its update AFTER A_rr's read_scn.
$b->query_safe('COMMIT');
$b->quit;

# A_rr's RR snapshot must STILL see 102 (xmax committed after read_scn -> the row
# was live as of the snapshot).
is($arr->query_safe('SELECT v FROM hot WHERE id = 1'), '102',
	'L9 A_rr still sees 102 (xmax committed AFTER read_scn -> visible) [spec-3.21]');
$arr->query_safe('COMMIT');
$arr->quit;

# A fresh snapshot sees B's committed value (A_upd was canceled, so 1102).
is($n->safe_psql('postgres', 'SELECT v FROM hot WHERE id = 1'), '1102',
	'L9 fresh snapshot sees committed hot.v = 1102');
$n->stop;

done_testing();
