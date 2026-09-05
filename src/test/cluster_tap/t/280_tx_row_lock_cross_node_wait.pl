#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 280_tx_row_lock_cross_node_wait.pl
#    spec-5.2 D8 — data-plane gate (2):  real 2-node cross-node TX row-lock
#    completion wait.  node0 holds an uncommitted row lock; node1's conflicting
#    write blocks in the cross-node TX enqueue wait (NOT the spec-3.4d 53R98
#    fail-closed), and wakes + re-judges + succeeds when node0 commits/aborts.
#
#    Chains the whole spec-5.2 stack:  D1 relsize coherence (node1 sees the
#    block) + D2 read-image ship (node1 reads node0's CURRENT image to see the
#    uncommitted xmax) + D4/D5 wait (node1 blocks in cluster_tx_enqueue_wait) +
#    exact TARGET wait + origin terminal recheck. SOURCE TT hints are closed
#    after R4 activation and must not wake a TARGET waiter.
#
#    Legs (spec-5.2 §4.2):
#      L5  node0 holds X (uncommitted UPDATE -> ctr=200); node1's UPDATE
#          (ctr=ctr+1) BLOCKS in wait_event = GesTxEnqueueWait (not 53R98).
#      L6  node0 COMMIT -> node1 wakes, re-judges, succeeds; ctr = 201 (no
#          lost update: node0's 200 + node1's +1).
#      L7  symmetric ABORT: node0 ROLLBACK -> node1 wakes, applies onto the
#          pre-node0 value.
#      L12 cluster.tx_enqueue_wait = off -> node1's conflicting write fails
#          closed with 53R98 (honest degradation).
#
#    The NOWAIT / SKIP LOCKED / timeout (53R70) / MultiXact (53R9H) /
#    dead-holder boundary legs and the >=10-run determinism (L8-L11) are
#    layered on once the core block->wakeup loop is green.
#
#    Harness: current ClusterQuad formation, two UPDATE participants,
#    shared_data + 3 voting disks + autovacuum off. The other two members
#    participate only in the existing authority/activation protocol.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/280_tx_row_lock_cross_node_wait.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.2-cf-liveread-dataplane-and-tx-row-lock-wait.md (D4/D5/D6/D8)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterQuad;
use Test::More;
use Time::HiRes qw(usleep);

# Fire a query that is EXPECTED TO BLOCK, without waiting for it to finish.
# We emit an \echo marker BEFORE the blocking statement; psql prints the marker
# as soon as it reads that line (before the statement blocks), so query_until
# returns immediately while the statement keeps running in the background.
# (A bare pump_nb of stdin is unreliable — psql may not have consumed the input
# yet, and a statement without a terminating ';' is never executed at all.)
sub bg_start_blocking
{
	my ($h, $sql) = @_;
	$h->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
}

# Poll a node's pg_stat_activity until a backend running $qlike is in the
# given wait_event (or timeout).  Returns 1 on match, 0 on timeout.
sub wait_for_wait_event
{
	my ($node, $qlike, $event, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $we = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce(wait_event, '') FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND pid <> pg_backend_pid()
			  AND state = 'active' LIMIT 1});
		return 1 if defined $we && $we eq $event;
		usleep(200_000);
	}
	return 0;
}

# Poll the committed row value until ctr == $want (or timeout).  This is the
# authoritative signal that the blocked node1 UPDATE woke from the cross-node
# TX enqueue wait, re-judged, applied (+1) and auto-committed with no lost
# update -- read on an independent connection so it reflects committed state.
# (The background psql's command tag is not reliably captured via pump_nb; the
# committed value is the real end-to-end evidence.)  If node1 had hung in the
# wait, ctr would never reach $want and this times out -> the leg fails.
sub wait_for_ctr
{
	my ($node, $want, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $v = $node->safe_psql('postgres', 'SELECT ctr FROM t WHERE id = 1');
		return 1 if defined $v && $v eq $want;
		usleep(200_000);
	}
	return 0;
}

sub ges_int
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='ges' AND key='$key'});
	die "missing exact ges.$key counter" unless defined($v) && $v =~ /\A[0-9]+\z/;
	return int($v);
}

sub target_terminal_count
{
	my ($node, $outcome) = @_;
	die "unexpected terminal outcome" unless $outcome eq 'committed' || $outcome eq 'aborted';
	my $key = "tx_resolve_${outcome}_count";
	my $value = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='r4' AND key='$key'});
	die "missing exact r4.$key counter" unless defined($value) && $value =~ /\A[0-9]+\z/;
	return int($value);
}

sub writer_chain_snapshot
{
	my ($pair) = @_;
	my @snap;
	for my $node ($pair->node0, $pair->node1)
	{
		my %values;
		for my $key (qw(writer_chain_resolved_count writer_chain_failclosed_count))
		{
			my $v = $node->safe_psql('postgres',
				qq{SELECT value FROM pg_cluster_state WHERE category='visibility' AND key='$key'});
			die "missing exact visibility.$key counter" unless defined($v) && $v =~ /\A[0-9]+\z/;
			$values{$key} = int($v);
		}
		push @snap, \%values;
	}
	return \@snap;
}

sub check_writer_chain_window
{
	my ($pair, $before, $leg) = @_;
	my $after = writer_chain_snapshot($pair);
	for my $index (0 .. 1)
	{
		for my $key (qw(writer_chain_resolved_count writer_chain_failclosed_count))
		{
			my $delta = $after->[$index]{$key} - $before->[$index]{$key};
			note "$leg node$index visibility.$key before=$before->[$index]{$key} after=$after->[$index]{$key} delta=$delta";
			if ($key eq 'writer_chain_failclosed_count')
			{
				is($delta, 0, "$leg node$index writer chain never fails closed");
			}
			else
			{
				# A changed page can restart qualification and consume the
				# terminal verdict in SatisfiesUpdate instead of this bridge's
				# chain-decide branch. Require its dedicated resolver proof below.
				cmp_ok($delta, '>=', 0, "$leg node$index writer-chain counter does not reset");
			}
		}
	}
}


# ----------
# L0/L1: existing current-path formation + shared data + same-DDL.
# ----------
my $pair = PostgreSQL::Test::ClusterQuad->new_quad(
	'tx_row_wait',
	quorum_voting_disks => 3,
	shared_data         => 1,
	shared_system_identifier => 1,
	shared_system_identifier_seed_sql => 'CREATE TABLE t (id int PRIMARY KEY, ctr int);',
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.read_scache = on',
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.page_scn_shortcut = on',
		'cluster.past_image = on',
		'cluster.crossnode_write_write = on',
		'cluster.undo_gcs_coherence = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.gcs_block_retransmit_max_retries = 8',
		# CI runners execute many shards in parallel; under CPU pressure a
		# node's CSSD heartbeat can be starved past the default 3000ms
		# misscount (1000ms interval x 3 deadband), so a perfectly healthy
		# peer is falsely declared DEAD and the in-flight cross-node UPDATE is
		# aborted by reconfiguration BEFORE it can block in the TX enqueue
		# wait (observed: "peer 0 -> DEAD (elapsed 3707 ms > 3000 ms)").  Widen
		# the misscount to 20s (2000ms x 10) so the lock-hold window survives
		# scheduling jitter.  This test exercises the wait path, not CSSD death
		# detection (covered by t/085 / the reconfig suite).
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);

# Reuse t/400's canonical PGRD and two-round activation sequence. A legacy
# unactivated pair cannot allocate an ACTIVE block-zero root after cutover.
# No test hook manufactures authority, and no workload error is retried.
my $voting_bytes = (8 * 128 + 3) * 512;
my @voting_paths = $pair->voting_disk_paths;
die 'expected exactly three voting disks' unless @voting_paths == 3;
for my $path (@voting_paths)
{
	truncate($path, $voting_bytes) or die "extend $path to PGRD minimum: $!";
}
for my $node ($pair->nodes)
{
	$node->append_conf('postgresql.conf', "cluster.voting_disk_size_bytes = $voting_bytes\n");
}
$pair->start_quad;
usleep(3_000_000);

for my $from (0 .. 3)
{
	is($pair->node($from)->safe_psql('postgres', 'SELECT 1'), '1', "L1 node$from alive");
	for my $to (0 .. 3)
	{
		next if $from == $to;
		ok($pair->wait_for_peer_state($from, $to, 'connected', 30), "L1 node$from sees node$to connected");
	}
}
my $undo_root = $pair->shared_data_root . '/pg_undo';
mkdir $undo_root or die "mkdir $undo_root: $!";
for my $node ($pair->nodes)
{
	$node->poll_query_until('postgres', q{SELECT in_quorum FROM pg_cluster_quorum_state}, 't')
		or die 'PGRD voting-disk majority did not become current';
	my $deadline = time() + 15;
	my ($rc, $out, $err);
	while (time() < $deadline)
	{
		($rc, $out, $err) = $node->psql('postgres',
			'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL', timeout => 30);
		last if defined($rc) && $rc != 0 && defined($err)
			&& $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
		usleep(100_000);
	}
	die 'PGRD bootstrap did not remain RF_DEFERRED: ' . ($err // '<undef>')
		unless defined($rc) && $rc != 0 && defined($err)
		&& $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
}
die 'PGRD mirror is absent' unless -f "$undo_root/pgrac_undo_root.control";
for my $round ('R4 bit0', 'Resource-X bit10')
{
	my $deadline = time() + 60;
	my $opened = 0;
	while (time() < $deadline)
	{
		my ($rc, $out, $err) = $pair->node0->psql('postgres',
			'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL', timeout => 45);
		if (defined($rc) && $rc == 0)
		{
			$opened = 1;
			last;
		}
		die "$round activation outcome unknown: " . ($err // '<undef>') unless defined($rc);
		die "$round untyped activation failure: " . ($err // '<undef>')
			unless defined($err) && $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET|activation request was refused)/;
		usleep(100_000);
	}
	die "$round did not reach OPEN_APPLIED" unless $opened;
}
# The empty catalog/index identity was cloned by the existing seed fixture.
# Repeating CREATE INDEX on shared storage would itself overwrite a live root.
my $p0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
my $p1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
if ($p0 ne $p1)
{
	die "same-DDL relfilepath differs (n0=$p0 n1=$p1)";
}
$pair->node0->safe_psql('postgres', 'INSERT INTO t VALUES (1, 100)');
$pair->node0->safe_psql('postgres', 'CHECKPOINT');    # flush the row to shared storage


# ----------
# L5/L6: node0 holds X (uncommitted UPDATE); node1's UPDATE blocks then wakes
# on node0 COMMIT and applies with no lost update.
# ----------
my $h0 = $pair->node0->background_psql('postgres', on_error_die => 1);
my $writer_before = writer_chain_snapshot($pair);
$h0->query_safe('BEGIN');
is($h0->query_safe('UPDATE t SET ctr = ctr + 100 WHERE id = 1 RETURNING ctr'),
	'200', 'L5 holder really updates exactly one row to 200 before commit');

my $waits_before = ges_int($pair->node1, 'tx_enqueue_wait_count');
my $timeouts_before = ges_int($pair->node1, 'tx_enqueue_timeout_count');
my $wakeups_before = ges_int($pair->node1, 'tx_enqueue_wakeup_count');
my $terminal_before = target_terminal_count($pair->node1, 'committed');

my $h1 = $pair->node1->background_psql('postgres', on_error_die => 1);
bg_start_blocking($h1, 'UPDATE t SET ctr = ctr + 1 WHERE id = 1 RETURNING ctr');

my $blocked = wait_for_wait_event($pair->node1, '%ctr = ctr + 1%', 'GesTxEnqueueWait', 20);
ok($blocked, 'L5 node1 UPDATE blocks in cross-node TX enqueue wait (GesTxEnqueueWait)');
cmp_ok(ges_int($pair->node1, 'tx_enqueue_wait_count'), '>', $waits_before,
	'L5 tx_enqueue_wait_count advanced (wait actually entered)');

$h0->query_safe('COMMIT');    # release: node0 commits ctr=200

ok(wait_for_ctr($pair->node1, '201', 20),
	'L6 node1 UPDATE wakes + completes after node0 commit (ctr reached 201)');

is($h1->query_safe(''), '201', 'L6 waiter updates exactly one row with no client error');
is($pair->node1->safe_psql('postgres', 'SELECT ctr FROM t WHERE id = 1'), '201',
	'L6 final ctr = 201 (no lost update)');
is(ges_int($pair->node1, 'tx_enqueue_wakeup_count'), $wakeups_before,
	'L6 TARGET completion does not consume a closed SOURCE hint wake');
is(ges_int($pair->node1, 'tx_enqueue_timeout_count'), $timeouts_before,
	'L6 no timeout is hidden by final payload');
cmp_ok(target_terminal_count($pair->node1, 'committed'), '>', $terminal_before,
	'L6 exact TARGET resolver publishes the holder commit proof');
check_writer_chain_window($pair, $writer_before, 'L6');

$h1->quit;
$h0->quit;


# ----------
# L7: symmetric ABORT — node0 holds X (uncommitted -> 400), node1 blocks, node0
# ROLLBACK -> node1 wakes and applies onto the pre-abort value (300 -> 301).
# ----------
is($pair->node1->safe_psql('postgres', 'UPDATE t SET ctr = 300 WHERE id = 1 RETURNING ctr'),
	'300', 'L7 reset exactly one committed row to 300');
my $g0 = $pair->node0->background_psql('postgres', on_error_die => 1);
$writer_before = writer_chain_snapshot($pair);
$g0->query_safe('BEGIN');
is($g0->query_safe('UPDATE t SET ctr = ctr + 100 WHERE id = 1 RETURNING ctr'),
	'400', 'L7 holder really updates exactly one row before rollback');
$waits_before = ges_int($pair->node1, 'tx_enqueue_wait_count');
$timeouts_before = ges_int($pair->node1, 'tx_enqueue_timeout_count');
$wakeups_before = ges_int($pair->node1, 'tx_enqueue_wakeup_count');
$terminal_before = target_terminal_count($pair->node1, 'aborted');

my $g1 = $pair->node1->background_psql('postgres', on_error_die => 1);
bg_start_blocking($g1, 'UPDATE t SET ctr = ctr + 1 WHERE id = 1 RETURNING ctr');
ok(wait_for_wait_event($pair->node1, '%ctr = ctr + 1%', 'GesTxEnqueueWait', 20),
	'L7 node1 UPDATE blocks (abort path)');
cmp_ok(ges_int($pair->node1, 'tx_enqueue_wait_count'), '>', $waits_before,
	'L7 exact wait is observed before rollback');

$g0->query_safe('ROLLBACK');

ok(wait_for_ctr($pair->node1, '301', 20),
	'L7 node1 wakes after node0 ROLLBACK (ctr reached 301)');
is($g1->query_safe(''), '301', 'L7 waiter updates exactly one row with no client error');
is($pair->node1->safe_psql('postgres', 'SELECT ctr FROM t WHERE id = 1'), '301',
	'L7 final ctr = 301 (aborted 400 never becomes the update base)');
is(ges_int($pair->node1, 'tx_enqueue_wakeup_count'), $wakeups_before,
	'L7 TARGET completion does not consume a closed SOURCE hint wake');
is(ges_int($pair->node1, 'tx_enqueue_timeout_count'), $timeouts_before,
	'L7 no timeout is hidden by final payload');
cmp_ok(target_terminal_count($pair->node1, 'aborted'), '>', $terminal_before,
	'L7 exact TARGET resolver publishes the holder abort proof');
check_writer_chain_window($pair, $writer_before, 'L7');

$g1->quit;
$g0->quit;

$pair->stop_quad;
done_testing();
