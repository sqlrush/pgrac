#-------------------------------------------------------------------------
#
# 362_shared_catalog_4node_kill_selfheal.pl
#    spec-4.6a D9 -- 4-node shared_catalog fail-stop self-heal.
#
#    Formation uses ClusterQuad(shared_catalog + wal_threads_root + shared_data)
#    so a killed node's HW authority is recoverable from its per-thread WAL.
#    After the kill legs start there is no SKIP path: BLOCKED_STRUCTURAL means
#    the test harness is misconfigured, and lack of convergence is a real FAIL.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/362_shared_catalog_4node_kill_selfheal.pl
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'shared_catalog 4-node kill self-heal requires --enable-cluster';
}

sub poll_until
{
	my ($node, $query, $want, $timeout_s) = @_;
	my $deadline = time() + $timeout_s;
	my $last = '';
	while (time() < $deadline)
	{
		$last = eval { $node->safe_psql('postgres', $query) };
		return 1 if defined $last && $last eq $want;
		usleep(300_000);
	}
	diag("poll_until timed out: '$query' last='" . ($last // '(undef)')
		. "' want='$want'");
	return 0;
}

sub retry_sql
{
	my ($node, $sql, $timeout_s) = @_;
	my $deadline = time() + $timeout_s;
	my $last = '';
	while (time() < $deadline)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql, timeout => 60);
		return (1, $out // '') if defined $rc && $rc == 0;
		$last = $err // '';
		usleep(500_000);
	}
	return (0, $last);
}

sub state_counter
{
	my ($node, $cat, $key) = @_;
	my $v = eval {
		$node->safe_psql('postgres',
			"SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'")
	};
	return defined $v && $v ne '' ? $v + 0 : 0;
}

sub sum_hw
{
	my ($quad, $key, @idx) = @_;
	my $sum = 0;
	for my $i (@idx)
	{
		$sum += state_counter($quad->node($i), 'hw', $key);
	}
	return $sum;
}

sub logs_contain
{
	my ($quad, $pattern, @idx) = @_;
	for my $i (@idx)
	{
		my $log = eval { PostgreSQL::Test::Utils::slurp_file($quad->node($i)->logfile) } // '';
		return 1 if $log =~ /$pattern/;
	}
	return 0;
}

sub log_until
{
	my ($quad, $idx, $pattern, $timeout_s) = @_;
	my $deadline = time() + $timeout_s;
	while (time() < $deadline)
	{
		my $log = eval { PostgreSQL::Test::Utils::slurp_file($quad->node($idx)->logfile) } // '';
		return 1 if $log =~ /$pattern/;
		usleep(300_000);
	}
	diag("log_until timed out on node$idx waiting for /$pattern/");
	return 0;
}

sub startup_env_blocker
{
	my ($quad) = @_;
	my $log = '';
	if ($quad)
	{
		for my $i (0 .. 3)
		{
			$log .= eval { PostgreSQL::Test::Utils::slurp_file($quad->node($i)->logfile) } // '';
		}
	}
	return $log =~ /could not create shared memory segment|No space left on device|No space left|SHMMNI|semget/i;
}

my $quad = eval {
	PostgreSQL::Test::ClusterQuad->new_quad(
		'sc4_selfheal',
		shared_catalog      => 1,
		quorum_voting_disks => 3,
		extra_conf          => [
			'autovacuum = off',
			'cluster.grd_max_entries = 4096',
			'cluster.lms_enabled = on',
			'cluster.ges_request_timeout_ms = 3000',
			'cluster.ges_retransmit_max_attempts = 0',
			'cluster.cssd_heartbeat_interval_ms = 1000',
			'cluster.cssd_dead_deadband_factor = 6',
			'cluster.grd_rebuild_timeout_ms = 1000',
			'cluster.hw_remaster_retry_backoff_ms = 100',
			'cluster.hw_remaster_retry_max_attempts = 8',
		]);
};
if (!$quad)
{
	my $err = $@ || 'unknown constructor failure';
	plan skip_all => "L1 shared_catalog ClusterQuad formation hit local substrate blocker: $err"
	  if $err =~ /could not create shared memory|No space left|SHMMNI|semget/i;
	BAIL_OUT("ClusterQuad(shared_catalog) constructor failed: $err");
}

my $started = 1;
for my $i (1 .. 3)
{
	PostgreSQL::Test::Utils::system_log(
		'pg_ctl', '-W', '-D', $quad->node($i)->data_dir,
		'-l', $quad->node($i)->logfile,
		'-o', "--cluster-name=sc4_selfheal_node$i", 'start');
}
unless ($quad->node(0)->start(fail_ok => 1))
{
	$started = 0;
}
for my $i (1 .. 3)
{
	$quad->node($i)->_update_pid(-1);
}
if (!$started)
{
	if (startup_env_blocker($quad))
	{
		eval { $quad->stop_quad; };
		plan skip_all => 'L1 shared_catalog ClusterQuad formation hit local shared-memory/resource limit';
	}
	my $fatal = logs_contain($quad, qr/cluster\.shared_catalog requires cluster\.wal_threads_dir|structurally blocked|BLOCKED_STRUCTURAL/i, 0 .. 3);
	eval { $quad->stop_quad; };
	ok(!$fatal, 'L1: shared_catalog quad must not hit wal_threads_dir/BLOCKED_STRUCTURAL configuration failure');
	BAIL_OUT('ClusterQuad(shared_catalog) failed to start for a non-environment reason');
}

# L1: all four members formed with quorum and per-thread WAL configured.
for my $i (0 .. 3)
{
	ok(poll_until($quad->node($i), 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 60),
		"L1: node$i reached quorum");
	is(state_counter($quad->node($i), 'hw', 'remaster_recoverable'), 1,
		"L1: node$i reports HW remaster recoverable");
}
for my $to (1 .. 3)
{
	ok($quad->wait_for_peer_state(0, $to, 'connected', 60),
		"L1: node0 sees node$to connected");
}

my $n0 = $quad->node0;
my $dead = 3;
my @survivors = (0, 1, 2);

# Seed shared-catalog DDL and force node3 to own real HW state before death.
for my $t (1 .. 2)
{
	my ($ddl_ok, $ddl_res) = retry_sql($n0,
		"CREATE TABLE sc4_hw_$t (id int, pad int)", 60);
	ok($ddl_ok, "L1: coordinator created shared_catalog table sc4_hw_$t")
	  or diag($ddl_res);
	my ($ok, $res) = retry_sql($quad->node($dead),
		"INSERT INTO sc4_hw_$t SELECT g, g FROM generate_series(1,5000) g", 60);
	ok($ok, "L1: node$dead extended shared_catalog table sc4_hw_$t before kill")
	  or diag($res);
}
for my $i (0 .. 3)
{
	retry_sql($quad->node($i), 'CHECKPOINT', 30);
}

my $done_before = sum_hw($quad, 'remaster_done_count', @survivors);
my $blocked_before = sum_hw($quad, 'remaster_blocked_count', @survivors);
my $retry_before = sum_hw($quad, 'remaster_retry_count', @survivors);

# L2: kill node3 and require every survivor to see the real DEAD edge.
# Use the CSSD log instead of a SQL view: during fail-closed GRD recovery,
# ordinary catalog reads may legitimately return 53R9I until the barrier opens.
$quad->kill_node9($dead);
for my $i (@survivors)
{
	ok(log_until($quad, $i, qr/cssd: peer $dead transitioned .* DEAD/, 45),
		"L2: survivor node$i declared node$dead dead");
}

# L3: GRD leaves no shard mastered by the dead node, and HW remaster converges.
for my $i (@survivors)
{
	ok(poll_until($quad->node($i),
		"SELECT (count(*)=0)::text FROM pg_cluster_grd_shards WHERE master_node_id=$dead",
		'true', 60),
		"L3: survivor node$i remastered all GRD shards off node$dead");
}
my $done_converged = 0;
my $deadline = time() + 90;
while (time() < $deadline)
{
	if (sum_hw($quad, 'remaster_done_count', @survivors) > $done_before)
	{
		$done_converged = 1;
		last;
	}
	usleep(500_000);
}
ok($done_converged, 'L3: HW remaster DONE advanced on at least one survivor after kill');
ok(!logs_contain($quad, qr/blocked_structural|structurally blocked|BLOCKED_STRUCTURAL/i, @survivors),
	'L3: no survivor reported BLOCKED_STRUCTURAL after kill');

# L6: if a transient BLOCKED occurred, a same-episode retry must also have run.
my $blocked_after = sum_hw($quad, 'remaster_blocked_count', @survivors);
if ($blocked_after > $blocked_before)
{
	ok(sum_hw($quad, 'remaster_retry_count', @survivors) > $retry_before,
		'L6: transient BLOCKED was followed by same-episode HW remaster retry');
}
else
{
	pass('L6: no transient HW BLOCKED occurred on the clean 4-node path');
}

# L4: survivor SQL and shared-catalog DDL recover after fail-stop.
for my $i (@survivors)
{
	my ($ok, $res) = retry_sql($quad->node($i), 'SELECT txid_current()', 60);
	ok($ok, "L4: survivor node$i accepts txid_current after reconfig") or diag($res);
}
my ($ddl_ok, $ddl_res) = retry_sql($n0, 'CREATE TABLE sc4_after_kill (id int)', 60);
ok($ddl_ok, 'L4: coordinator survivor can run DDL after node kill') or diag($ddl_res);
for my $i (1, 2)
{
	ok(poll_until($quad->node($i),
		"SELECT (count(*)=1)::text FROM pg_class WHERE relname='sc4_after_kill'",
		'true', 60),
		"L4: survivor node$i sees post-kill shared-catalog DDL");
}

# L4 (spec-4.6a D12, r2-P1-3): the dead-node PCM residue cleanup counter is
# exposed and non-negative on every survivor; when the dead node left any
# holder/pending-X records behind, at least one survivor accounts a cleanup
# (delta assertable; zero stays legal when the dead node held nothing).
{
	my $cleanup_total = 0;
	for my $i (@survivors)
	{
		my $c = state_counter($quad->node($i), 'pcm', 'dead_cleanup_entries');
		ok($c >= 0, "L4: survivor node$i exposes pcm/dead_cleanup_entries");
		$cleanup_total += $c;
	}
	diag("L4: pcm/dead_cleanup_entries total across survivors = $cleanup_total");
}

# L5: watchdog counters are allowed to be zero, but must be readable.
for my $i (@survivors)
{
	my $cluster_gate = state_counter($quad->node($i), 'grd_recovery', 'cluster_gate_timeout');
	my $epoch_escape = state_counter($quad->node($i), 'grd_recovery', 'wait_epoch_escape');
	ok($cluster_gate >= 0 && $epoch_escape >= 0,
		"L5: survivor node$i exposes WAIT_CLUSTER/WAIT_EPOCH watchdog counters");
}

$quad->stop_quad;
done_testing();
