#-------------------------------------------------------------------------
#
# 362_shared_catalog_4node_kill_selfheal.pl
#    spec-4.6a D9 -- 4-node shared_catalog fail-stop self-heal.
#
#    Formation uses ClusterQuad(shared_catalog + wal_threads_root + shared_data)
#    so a killed node's HW authority is recoverable from its per-thread WAL.
#    Amendment v1.2 (R1): in this out-of-scope deployment (4 nodes, online
#    thread recovery off) the dead master's PRE-EXISTING writes stay
#    fail-closed until the failed node restarts — its unproven blocks (bounded
#    53R9L) and equally its transaction verdicts (TT status unknown for its
#    xids, the visibility door).  Reads and DDL are asserted on the honest
#    two-outcome contract (success or explicit SQLSTATE, never a hang);
#    new-object DDL/writes prove P7 unfreeze.  L4h (r3-P1-1) is the serve-gate
#    LOCK: a fresh backend's wide catalog scan on a cold survivor MUST fail
#    53R9L (no success arm) while the dead node is down.
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

# Amendment v1.2 (R1): after the kill, a NEW backend's parse may cold-read a
# dead-master catalog page and honestly fail closed (53R9L) until the failed
# node returns.  The convergence observability therefore rides PRE-WARMED
# long-lived psql sessions (catcache hot; pg_cluster_* reads touch shmem, not
# dead-master blocks), while fresh-connection legs assert the two-outcome
# contract.  bg_query returns (1, value) or (0, error-ish) without dying.
sub bg_query
{
	my ($bg, $sql) = @_;
	my $out = eval { $bg->query($sql) };
	return (0, $@ // 'query failed') if $@;
	return (1, $out // '');
}

sub bg_counter
{
	my ($bg, $cat, $key) = @_;
	my ($ok, $v) = bg_query($bg,
		"SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'");
	return ($ok && defined $v && $v ne '') ? $v + 0 : 0;
}

sub bg_sum
{
	my ($bgs, $cat, $key, @idx) = @_;
	my $sum = 0;
	$sum += bg_counter($bgs->{$_}, $cat, $key) for @idx;
	return $sum;
}

sub bg_poll_until
{
	my ($bg, $sql, $want, $timeout_s) = @_;
	my $deadline = time() + $timeout_s;
	my $last = '';
	while (time() < $deadline)
	{
		my ($ok, $out) = bg_query($bg, $sql);
		$last = $out;
		return 1 if $ok && defined $out && $out eq $want;
		usleep(500_000);
	}
	diag("bg_poll_until timed out: '$sql' last='$last' want='$want'");
	return 0;
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

# Amendment v1.2 (R1): pre-warmed observability sessions (see helper note).
my %bg;
for my $i (@survivors)
{
	$bg{$i} = $quad->node($i)->background_psql('postgres', on_error_stop => 0);
	bg_query($bg{$i}, "SELECT value FROM pg_cluster_state WHERE category='hw' AND key='remaster_done_count'");
	bg_query($bg{$i}, "SELECT count(*) FROM pg_cluster_grd_shards WHERE master_node_id=$dead");
	bg_query($bg{$i}, 'SELECT txid_current()');
}

my $done_before = bg_sum(\%bg, 'hw', 'remaster_done_count', @survivors);
my $blocked_before = bg_sum(\%bg, 'hw', 'remaster_blocked_count', @survivors);
my $retry_before = bg_sum(\%bg, 'hw', 'remaster_retry_count', @survivors);
my $cleanup_before = bg_sum(\%bg, 'pcm', 'dead_cleanup_entries', @survivors);
my %grd_done_before;
$grd_done_before{$_} = bg_counter($bg{$_}, 'grd_recovery', 'remaster_done')
  for @survivors;

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
# The map read is hard-asserted on the warm COORDINATOR session only: the view
# scan on a cold survivor deterministically cold-reads a dead-master catalog
# page out of the small test buffer pool and honestly fails closed (53R9L)
# until node3 returns — the map itself is a deterministic recompute barriered
# by P6, and each cold survivor is instead hard-asserted on its own HW
# remaster worker DONE log line below.
ok(bg_poll_until($bg{0},
	"SELECT (count(*)=0)::text FROM pg_cluster_grd_shards WHERE master_node_id=$dead",
	'true', 60),
	'L3: coordinator remastered all GRD shards off node3 (warm session)');
for my $i (@survivors)
{
	ok(log_until($quad, $i, qr/HW remaster worker: dead node $dead -> done/, 90),
		"L3: survivor node$i HW remaster worker reported done");
}
my $done_converged = 0;
my $deadline = time() + 90;
while (time() < $deadline)
{
	if (bg_sum(\%bg, 'hw', 'remaster_done_count', @survivors) > $done_before)
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
my $blocked_after = bg_sum(\%bg, 'hw', 'remaster_blocked_count', @survivors);
if ($blocked_after > $blocked_before)
{
	ok(bg_sum(\%bg, 'hw', 'remaster_retry_count', @survivors) > $retry_before,
		'L6: transient BLOCKED was followed by same-episode HW remaster retry');
}
else
{
	pass('L6: no transient HW BLOCKED occurred on the clean 4-node path');
}

# L4h (r3-P1-1 hard leg): with the serve gate in place, an out-of-scope
# formation (4 nodes / online_thread_recovery off) NEVER publishes the dead
# node's materialization proof, so EVERY node3-mastered page stays RECOVERING
# until node3 returns — and node3 is never restarted in this test.  A wide
# catalog scan from a FRESH backend on a cold survivor therefore MUST fail
# closed with 53R9L: pg_class + pg_attribute span dozens of pages hashed ~1/4
# to node3, and a fresh backend's full scan cold-reads pages no prior session
# pulled into the 16MB test pool.  Deliberately NO success arm — a NORMAL
# serve of a dead-master page here is the exact 8.A regression this leg locks
# (reverting the serve-gate fix turns this leg red while the two-outcome legs
# stay green).  Gate on the survivor's OWN episode reaching IDLE first so the
# failure surface is deterministically the post-episode materialization proof
# (53R9L), never the in-episode shard freeze (53R9I).  The scan touches no
# node3-written rows (node3 ran only heap INSERTs, no DDL), so the TT
# visibility door cannot fire first: 53R9L is the ONLY legal outcome.
{
	my $cold = 2;

	# Compare in Perl, not SQL: the warm session's catcache covers exactly the
	# bg_counter query shape it ran pre-kill; a cast/operator added in SQL
	# would itself cold-read a dead-master catalog page and trip 53R9L.
	my $idle = 0;
	my $idle_deadline = time() + 90;
	while (time() < $idle_deadline)
	{
		if (bg_counter($bg{$cold}, 'grd_recovery', 'remaster_done')
			> $grd_done_before{$cold})
		{
			$idle = 1;
			last;
		}
		usleep(500_000);
	}
	ok($idle, "L4h: survivor node$cold GRD recovery episode reached IDLE (warm session)");

	my $t0 = time();
	my ($rc, $out, $err) = $quad->node($cold)->psql('postgres',
		'SELECT count(*) FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid',
		timeout => 60);
	my $elapsed = time() - $t0;
	isnt(defined $rc ? $rc : 0, 0,
		"L4h: fresh wide catalog scan on survivor node$cold MUST fail while node$dead is down");
	like($err // '',
		qr/block-level cache protocol state is being rebuilt after reconfiguration/,
		'L4h: the failure is the explicit 53R9L dead-master fail-closed gate');
	cmp_ok($elapsed, '<', 60, 'L4h: fail-closed scan returned bounded (no hang)');
	diag("L4h: scan failed closed as required: " . ($err // '(no stderr)'))
	  if defined $err && $err ne '';
}

# L4a (Amendment v1.2 R1): reading the dead master's PRE-EXISTING blocks in an
# out-of-scope deployment (online_thread_recovery off / 4 nodes) must be a
# BOUNDED, EXPLICIT fail-closed error — never a silent stale read, never a
# hang.  sc4_hw_* spans ~20+ pages, so some page's static master is node3 with
# overwhelming probability; the redo-coverage proof cannot be published without
# online thread recovery, so the scan hits 53R9L (resource recovering).  If by
# hash luck no scanned page is node3-mastered the scan succeeds — both are the
# honest two-outcome contract; only a hang/timeout or an unexpected error fails.
for my $t (1 .. 2)
{
	my $t0 = time();
	my ($rc, $out, $err) = $quad->node(1)->psql('postgres',
		"SELECT count(*) FROM sc4_hw_$t", timeout => 60);
	my $elapsed = time() - $t0;
	if (defined $rc && $rc == 0)
	{
		ok(1, "L4a: dead-master table sc4_hw_$t read succeeded (no node$dead-mastered page scanned)");
		diag("L4a: sc4_hw_$t count=$out");
	}
	else
	{
		like($err // '',
			qr/being rebuilt after reconfiguration|resource recovering|53R9L|cluster TT status unknown/i,
			"L4a: dead-master table sc4_hw_$t read fail-closed with an explicit SQLSTATE");
	}
	cmp_ok($elapsed, '<', 60, "L4a: sc4_hw_$t read returned bounded (no hang)");
}

# L4: survivor SQL recovers after fail-stop — hard-asserted on the pre-warmed
# sessions; a FRESH connection is the honest two-outcome contract (v1.2 R1):
# success, or an explicit fail-closed SQLSTATE on a dead-master catalog page —
# never a hang.
for my $i (@survivors)
{
	my $got = 0;
	my $dl = time() + 60;
	while (time() < $dl)
	{
		my ($ok, $out) = bg_query($bg{$i}, 'SELECT txid_current()');
		if ($ok && defined $out && $out =~ /^\d+$/) { $got = 1; last; }
		usleep(500_000);
	}
	ok($got, "L4: survivor node$i accepts txid_current after reconfig (warm session)");

	my $t0 = time();
	my ($rc, $out, $err) = $quad->node($i)->psql('postgres', 'SELECT txid_current()',
		timeout => 60);
	if (defined $rc && $rc == 0)
	{
		ok(1, "L4: fresh connection txid_current succeeded on node$i");
	}
	else
	{
		like($err // '',
			qr/being rebuilt after reconfiguration|resource recovering|53R9L|being remastered|53R9I|cluster TT status unknown|write-fenced/i,
			"L4: fresh connection on node$i fail-closed with an explicit SQLSTATE");
	}
	cmp_ok(time() - $t0, '<', 60, "L4: fresh connection probe on node$i returned bounded");
}
my ($ddl_rc, $ddl_out, $ddl_err) = $n0->psql('postgres',
	'CREATE TABLE sc4_after_kill (id int)', timeout => 60);
my $ddl_ok = defined $ddl_rc && $ddl_rc == 0;
if ($ddl_ok)
{
	ok(1, 'L4: coordinator survivor DDL succeeded after node kill');
}
else
{
	# write-fenced (53R51, spec-4.12): a transient lease/epoch fence on the
	# writer right after the reconfig — an explicit, retryable fail-closed
	# rejection, squarely inside the honest two-outcome contract.
	like($ddl_err // '',
		qr/being rebuilt after reconfiguration|resource recovering|53R9L|being remastered|53R9I|cluster TT status unknown|write-fenced/i,
		'L4: coordinator DDL fail-closed with an explicit SQLSTATE (dead-master catalog block)');
	diag("L4: DDL fail-closed honestly: $ddl_err");
}
for my $i (1, 2)
{
	if ($ddl_ok)
	{
		ok(poll_until($quad->node($i),
			"SELECT (count(*)=1)::text FROM pg_class WHERE relname='sc4_after_kill'",
			'true', 60),
			"L4: survivor node$i sees post-kill shared-catalog DDL");
	}
	else
	{
		ok(1, "L4: survivor node$i visibility leg not applicable (DDL fail-closed honestly)");
	}
}

# L4 (spec-4.6a D12, Amendment v1.2 R3): node3 held real PCM/HW state at kill
# time (it extended sc4_hw_1/2 with 5000 rows each), so the dead-node residue
# cleanup MUST account a positive delta across the survivors — a >= 0
# assertion would stay green with D12 entirely disabled.
{
	my $cleanup_total = 0;
	for my $i (@survivors)
	{
		my ($ok, $n) = bg_query($bg{$i},
			"SELECT count(*) FROM pg_cluster_state WHERE category='pcm' AND key='dead_cleanup_entries'");
		ok($ok && defined $n && $n eq '1',
			"L4: survivor node$i exposes the pcm/dead_cleanup_entries key");
		$cleanup_total += bg_counter($bg{$i}, 'pcm', 'dead_cleanup_entries');
	}
	cmp_ok($cleanup_total - $cleanup_before, '>', 0,
		'L4: dead-node PCM residue cleanup accounted a positive delta after kill');
	diag("L4: pcm/dead_cleanup_entries delta across survivors = "
		  . ($cleanup_total - $cleanup_before));
}

# L5: watchdog counters are allowed to be zero, but the KEYS must exist
# (state_counter reads a missing key as 0, so presence is the real assertion).
for my $i (@survivors)
{
	my ($ok1, $n1) = bg_query($bg{$i},
		"SELECT count(*) FROM pg_cluster_state WHERE category='grd_recovery' AND key='cluster_gate_timeout'");
	my ($ok2, $n2) = bg_query($bg{$i},
		"SELECT count(*) FROM pg_cluster_state WHERE category='grd_recovery' AND key='wait_epoch_escape'");
	ok($ok1 && $ok2 && defined $n1 && $n1 eq '1' && defined $n2 && $n2 eq '1',
		"L5: survivor node$i exposes WAIT_CLUSTER/WAIT_EPOCH watchdog counter keys");
}

$bg{$_}->quit for @survivors;

$quad->stop_quad;
done_testing();
