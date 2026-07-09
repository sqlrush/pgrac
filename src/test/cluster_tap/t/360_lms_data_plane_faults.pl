#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 360_lms_data_plane_faults.pl
#	  spec-7.2 D7 — LMS DATA-plane value gate + fault-leg coverage.
#	  Complements t/358 (flip core L1-L5) with the quantitative value
#	  gate and documents the fault legs that are KNOWN-BLOCKED on a
#	  pre-existing substrate wall.
#
#	  REACHABLE legs (real assertions):
#	    L1  flip fact: block_family_plane = 1 on both nodes; DATA-plane
#	        listeners bound (log evidence).
#	    L2  value gate: ping-pong the shared table X between the nodes,
#	        then the requester ship-latency histogram must show
#	        p50 < 5ms and p99 < 20ms (裁决④ loopback口径).  Actual
#	        percentiles are diag'd.
#	    L3  hygiene: zero plane misroutes + no plane-gate errors.
#
#	  KNOWN-BLOCKED legs (honest SKIP; see docs/stage7-substrate-
#	  findings.md — Stage-7 substrate follow-ups, user ruling 2026-07-07):
#	    L4  kill -9 LMS x3 crash recovery — F1-8:  a SIGKILL of the LMS
#	        triggers a node crash-restart, but block_device crash-
#	        recovery replays a relation create/extend that needs the
#	        raw_layout_lock via GES (cluster_lock_acquire_seven_step),
#	        and GES is unavailable before the node re-joins the cluster
#	        -> recovery FATAL "could not acquire raw layout lock".  This
#	        is a recovery-vs-membership ordering gap in the spec-6.0a
#	        block_device backend, orthogonal to the spec-7.2 DATA plane.
#	    L5  conn-reset injection reset->reconnect — F6-1:  the only arm
#	        path is the process-local injection GUC, which is persistent
#	        and storms a reset every LMS tick (not the one-shot epoch
#	        bump it models); the storm crashes the passive LMS with
#	        kevent() EBADF and then hits the same F1-8 recovery wall.
#	        The real single-bump reset path (cur_epoch != dp_last_epoch)
#	        is clean (rebuild-before-wait within the tick).
#	    L6  data-dispatch injection reachability — F6-2:  the
#	        cluster-lms-data-dispatch point sits on the LMS async recv
#	        pump's CONNECTED&READABLE branch, which the actual block
#	        REPLY recv does not traverse (hits stay 0), so it needs
#	        repositioning before it can be armed as a reachability leg.
#
# Spec: spec-7.2-ic-data-plane-decoupling.md §4 / D7
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/360_lms_data_plane_faults.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'dpfault',
	quorum_voting_disks => 3,
	shared_data         => 1,
	storage_backend     => 'block_device',
	extra_conf          => [
		'autovacuum = off',
		'fsync = off',
		'shared_buffers = 64MB',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.online_join = on',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.block_self_contained = on',
	]);
$pair->start_pair;
usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'CONTROL peers up 0->1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'CONTROL peers up 1->0');
my ($node0, $node1) = ($pair->node0, $pair->node1);

sub gcs_int
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='gcs' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

sub poll_write_ok
{
	my ($node, $sql, $timeout_s, $label) = @_;
	$timeout_s //= 60;
	my $deadline = time + $timeout_s;
	while (time < $deadline)
	{
		return 1 if eval { $node->safe_psql('postgres', $sql); 1 };
		usleep(500_000);
	}
	diag("poll_write_ok timeout ($label)");
	return 0;
}

sub timed_update_retry
{
	my ($node, $sql, $tries) = @_;
	$tries //= 30;
	for my $attempt (1 .. $tries)
	{
		return 1 if eval { $node->safe_psql('postgres', $sql); 1 };
		usleep(500_000);
	}
	return 0;
}

# ============================================================
# L1 — flip fact + DATA listeners (reachable).
# ============================================================
is(gcs_int($node0, 'block_family_plane'), 1, 'L1 node0 block family on DATA plane');
is(gcs_int($node1, 'block_family_plane'), 1, 'L1 node1 block family on DATA plane');

my $log0 = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
my $log1 = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
like($log0, qr/DATA-plane listener bound/, 'L1 node0 DATA listener bound');
like($log1, qr/DATA-plane listener bound/, 'L1 node1 DATA listener bound');

# ============================================================
# shared table (relfilenode coincidence, t/347 OID-align pattern).
# ============================================================
ok(poll_write_ok($node0, 'CREATE TABLE fg0 (x int)', 90, 'node0 gate'),
	'node0 write gate open (join admitted)');
ok(poll_write_ok($node1, 'CREATE TABLE fg1 (x int)', 90, 'node1 gate'),
	'node1 write gate open (join admitted)');

my ($p0, $p1) = ('', '');
for my $attempt (1 .. 8)
{
	$node0->safe_psql('postgres',
		'CREATE TABLE fault_t (aid int, bal int) WITH (fillfactor = 50)');
	$node1->safe_psql('postgres',
		'CREATE TABLE fault_t (aid int, bal int) WITH (fillfactor = 50)');
	$p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('fault_t')});
	$p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('fault_t')});
	last if $p0 eq $p1;
	my ($n0) = $p0 =~ /(\d+)$/;
	my ($n1) = $p1 =~ /(\d+)$/;
	my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
	$lag->safe_psql('postgres',
		"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
	$node0->safe_psql('postgres', 'DROP TABLE fault_t');
	$node1->safe_psql('postgres', 'DROP TABLE fault_t');
}
is($p0, $p1, 'shared-table coincidence');
$node0->safe_psql('postgres',
	'INSERT INTO fault_t SELECT g, 0 FROM generate_series(1, 200) g');
$node0->safe_psql('postgres', 'CHECKPOINT');
$node1->safe_psql('postgres', 'CHECKPOINT');

# ============================================================
# L2 — value gate: ping-pong the table X, then p50 < 5ms, p99 < 20ms.
# ============================================================
my $swaps = 0;
for my $r (1 .. 20)
{
	last unless timed_update_retry($node0, 'UPDATE fault_t SET bal = bal + 1', 20);
	usleep(100_000);
	last unless timed_update_retry($node1, 'UPDATE fault_t SET bal = bal + 1', 20);
	usleep(100_000);
	$swaps++;
}
ok($swaps > 0, "L2 completed $swaps X ping-pong swaps over the DATA plane");

my @bounds_us = (500, 1000, 2000, 5000, 10000, 20000, 50000, 100000,
	200000, 500000, 1000000, 2000000, 5000000, 10000000, 30000000);
my @counts;
my $total = 0;
for my $b (0 .. $#bounds_us)
{
	my $c = gcs_int($node0, "ship_hist_us_le_$bounds_us[$b]")
	  + gcs_int($node1, "ship_hist_us_le_$bounds_us[$b]");
	$counts[$b] = $c;
	$total += $c;
}
my $inf = gcs_int($node0, 'ship_hist_us_inf') + gcs_int($node1, 'ship_hist_us_inf');
$total += $inf;

sub pct_bound
{
	my ($pct, $tot, $counts_ref, $bounds_ref) = @_;
	my $target = $pct * $tot;
	my $cum = 0;
	for my $b (0 .. $#$bounds_ref)
	{
		$cum += $counts_ref->[$b];
		return $bounds_ref->[$b] if $cum >= $target;
	}
	return undef;    # +inf overflow
}

my $p50 = pct_bound(0.50, $total, \@counts, \@bounds_us);
my $p99 = pct_bound(0.99, $total, \@counts, \@bounds_us);
diag(sprintf('L2 value gate: ships=%d  p50<=%s us  p99<=%s us  (+inf=%d)',
		$total, defined($p50) ? $p50 : 'inf', defined($p99) ? $p99 : 'inf', $inf));

ok($total > 0, "L2 requester histogram recorded ship samples ($total)");
ok(defined($p50) && $p50 <= 5000,
	sprintf('L2 value gate p50 < 5ms (p50 <= %s us)', defined($p50) ? $p50 : 'inf'));
ok(defined($p99) && $p99 <= 20000,
	sprintf('L2 value gate p99 < 20ms (p99 <= %s us)', defined($p99) ? $p99 : 'inf'));

# ============================================================
# L3 — hygiene: zero plane misroutes.
# ============================================================
is(gcs_int($node0, 'plane_misroute_reject'), 0, 'L3 node0 zero plane misroutes');
is(gcs_int($node1, 'plane_misroute_reject'), 0, 'L3 node1 zero plane misroutes');

# ============================================================
# L4 — KNOWN-BLOCKED: kill -9 LMS x3 crash recovery.
# (F1-8: block_device crash-recovery raw_layout_lock needs GES, which is
# unavailable before the crash-restarting node re-joins -> recovery FATAL.
# Orthogonal to the spec-7.2 DATA plane; see docs/stage7-substrate-
# findings.md.  The SIGKILL is NOT issued here -- it would crash the node
# into an unrecoverable state.)
# ============================================================
SKIP: {
	skip 'F1-8 KNOWN-BLOCKED: block_device crash-recovery raw_layout_lock '
	  . 'depends on GES, unavailable before a crash-restarting node re-joins '
	  . '(recovery FATAL "could not acquire raw layout lock"); recovery-vs-'
	  . 'membership substrate gap, orthogonal to the spec-7.2 DATA plane', 3;
	ok(0, 'L4.1 LMS crash-restart recovers + DATA listener rebinds');
	ok(0, 'L4.2 CONTROL membership reconnects after LMS crash');
	ok(0, 'L4.3 cross-node block transfer recovers after LMS crash (x3)');
}

# ============================================================
# L5 — KNOWN-BLOCKED: conn-reset injection reset -> reconnect.
# (F6-1: the injection GUC arm is persistent and storms a reset every LMS
# tick, crashing the passive LMS with kevent() EBADF, then hitting the F1-8
# wall.  The injection cannot be single-fired via the GUC; the real single-
# bump epoch reset path is clean.  Not armed here.)
# ============================================================
SKIP: {
	skip 'F6-1 KNOWN-BLOCKED: cluster-lms-conn-reset GUC arm is persistent '
	  . '(storms every tick, not the one-shot epoch bump it models) and '
	  . 'crashes the passive LMS with kevent() EBADF, then hits F1-8; needs '
	  . 'one-shot arm + WES fd-lifecycle hardening', 2;
	ok(0, 'L5.1 conn-reset injection resets the DATA mesh once');
	ok(0, 'L5.2 mesh reconnects + block transfer converges after reset');
}

# ============================================================
# L6 — KNOWN-BLOCKED: data-dispatch injection reachability.
# (F6-2: the cluster-lms-data-dispatch point is on the LMS async recv pump's
# CONNECTED&READABLE branch, which the actual block REPLY recv does not
# traverse -- hits stay 0 even armed at startup.  Needs repositioning onto
# the real block recv path before it can be a reachability leg.)
# ============================================================
SKIP: {
	skip 'F6-2 KNOWN-BLOCKED: cluster-lms-data-dispatch sits off the actual '
	  . 'block REPLY recv path (hits stay 0 even armed at startup); needs '
	  . 'repositioning', 1;
	ok(0, 'L6 cluster-lms-data-dispatch fires on the live block recv path');
}

# ============================================================
# hygiene tail: no plane-gate errors anywhere in the run.
# ============================================================
$pair->stop_pair;
$log0 = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
$log1 = PostgreSQL::Test::Utils::slurp_file($node1->logfile);
unlike($log0, qr/cannot send from plane|plane mismatch/,
	'L3 node0 log free of plane-gate errors');
unlike($log1, qr/cannot send from plane|plane mismatch/,
	'L3 node1 log free of plane-gate errors');

done_testing();
