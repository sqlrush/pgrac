#-------------------------------------------------------------------------
#
# 301_hang_manager.pl
#    spec-5.11 Hang Manager skeleton end-to-end (single node).
#
#    Drives the DIAG-hosted long-wait sampler against a real server and
#    asserts the diagnostic surface is genuinely populated (not a hollow
#    dump): per-row long-wait samples with real pid / wait_event / duration
#    / blocker, LOG-once de-duplication, the ProcSignal backend self-dump,
#    the idle-blocker exclusion, and the disabled-switch.
#
#    Test matrix (spec-5.11 §4.2):
#      L1  injected long lock-wait appears as a per-row hang sample with
#          real values (pid / wait_event / wait_ms), quality=complete, and
#          blocker_pid pointing at the lock holder
#      L2  LOG-once: the long wait logs exactly once across rounds
#      L3  pg_cluster_hang_dump(pid) makes the target backend log its own
#          wait state; proc_signal_dump_count increments
#      L4  the idle-in-transaction lock HOLDER is NOT counted as a hang
#          waiter (only the blocked waiter is)
#      L6  cluster.hang_manager_enabled = off stops sampling; the hang
#          category freezes with cumulative counters retained (not zeroed)
#      L7  the `hang` category is present in pg_cluster_state
#      L9  cluster.hang_dump_enabled = off does NOT suppress the
#          pg_cluster_state hang category; it only freezes the dumps_emitted
#          accounting + the long-wait LOG-once (Hardening v1.2 / F3)
#
#    Forward-marked (NOT run here, honestly deferred — see body):
#      L5  confirmed-deadlock-waiter exclusion: shipped spec-5.8 exposes
#          deadlock confirmation only as aggregate counters + WFG
#          membership, not a per-proc confirmed-cycle flag, so the live
#          per-proc exclusion is forward to a 5.9 per-proc victim/confirmed
#          signal (D0 re-ground).  The exclusion LOGIC is unit-tested
#          (test_cluster_hang U2/U6).
#      L8  cross-node REMOTE_BOUNDARY: needs a 2-node cluster; the local
#          boundary marking is exercised by the cluster-wait code path but
#          the 2-node e2e is forward (spec §4.3).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/301_hang_manager.pl
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
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
# spec-5.11 Hang Manager fast-sampling test knobs.
cluster.diag_main_loop_interval = 200
cluster.hang_manager_enabled = on
cluster.hang_sample_interval_ms = 200
cluster.hang_threshold_ms = 1000
cluster.hang_dump_enabled = on
log_min_messages = log
});
$node->start;


# Fire a query expected to BLOCK without waiting for it to finish.
sub bg_start_blocking
{
	my ($h, $sql) = @_;
	$h->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
}

# A single `hang` category value.
sub hang_val
{
	my ($key) = @_;
	return $node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='hang' AND key='$key'");
}

# Count of `hang` per-row keys matching a POSIX regex + value (robust to sample
# ordering).  A regex (not LIKE) is required so 'hang_sampleN_pid' does not also
# match 'hang_sampleN_blocker_pid'.
sub hang_row_match
{
	my ($key_re, $value) = @_;
	return $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_cluster_state WHERE category='hang' "
		. "AND key ~ '$key_re' AND value = '$value'");
}

# Poll until a `hang` sample row reports the given waiter pid, or time out.
sub wait_for_hang_sample
{
	my ($waiter_pid, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		return 1 if hang_row_match('^hang_sample[0-9]+_pid$', $waiter_pid) eq '1';
		usleep(200_000);
	}
	return 0;
}

# Poll pg_stat_activity until a backend running $qlike waits on a heavyweight lock.
sub wait_for_lock_wait
{
	my ($qlike, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $we = $node->safe_psql('postgres', qq{
			SELECT coalesce(wait_event_type,'') FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND pid <> pg_backend_pid()
			  AND state = 'active' LIMIT 1});
		return 1 if defined $we && $we eq 'Lock';
		usleep(200_000);
	}
	return 0;
}


# ----------
# L7: the `hang` category exists and reports the GUC state.
# ----------
is(hang_val('hang_manager_enabled'), 't',
   'L7 hang category present; hang_manager_enabled reflects GUC (on)');
is(hang_val('hang_available'), 't', 'L7 hang_available true (DIAG region attached)');


# ----------
# Set up a blocking lock wait: holder A holds ACCESS EXCLUSIVE; waiter B blocks.
# ----------
$node->safe_psql('postgres', 'CREATE TABLE hangt (i int)');

my $ha = $node->background_psql('postgres', on_error_die => 1);
$ha->query_safe('BEGIN');
$ha->query_safe('LOCK TABLE hangt IN ACCESS EXCLUSIVE MODE');
my $apid = $ha->query_safe('SELECT pg_backend_pid()');

my $hb = $node->background_psql('postgres', on_error_die => 1);
my $bpid = $hb->query_safe('SELECT pg_backend_pid()');
# Waiter B must be inside a transaction block: LOCK TABLE outside a txn errors
# immediately ("can only be used in transaction blocks") instead of blocking.
bg_start_blocking($hb, 'BEGIN; LOCK TABLE hangt IN ACCESS EXCLUSIVE MODE');

ok(wait_for_lock_wait('%LOCK TABLE hangt%', 20),
   'waiter B is blocked on the heavyweight relation lock');


# ----------
# L1: the long lock-wait shows up as a real per-row hang sample.
# ----------
ok(wait_for_hang_sample($bpid, 25),
   'L1 blocked waiter B appears as a per-row hang sample (real shmem row)');

cmp_ok(hang_val('hang_long_wait_count'), '>=', '1',
	   'L1 hang_long_wait_count >= 1');
is(hang_row_match('^hang_sample[0-9]+_blocker_pid$', $apid), '1',
   'L1 sampled blocker_pid points at the lock holder A (hard blocker resolved)');
cmp_ok(hang_row_match('^hang_sample[0-9]+_quality$', 'complete'), '>=', '1',
	   'L1 at least one sample is quality=complete (true heavyweight wait-start)');


# ----------
# L4: the idle-in-transaction HOLDER A is not itself a hang waiter.
# ----------
is(hang_row_match('^hang_sample[0-9]+_pid$', $apid), '0',
   'L4 idle-in-tx lock holder A is excluded from hang samples');


# ----------
# L3: ProcSignal backend self-dump.
# ----------
my $before_dumps = hang_val('hang_proc_signal_dump_count');
$node->safe_psql('postgres', "SELECT pg_cluster_hang_dump($bpid)");
my $got_dump = 0;
my $dl = time() + 10;
while (time() < $dl)
{
	my $log = slurp_file($node->logfile);
	if ($log =~ /cluster hang dump: pid $bpid /)
	{
		$got_dump = 1;
		last;
	}
	usleep(200_000);
}
ok($got_dump, 'L3 pg_cluster_hang_dump made backend B log its own wait state');
cmp_ok(hang_val('hang_proc_signal_dump_count'), '>', $before_dumps,
	   'L3 proc_signal_dump_count incremented');


# ----------
# L2: LOG-once — the long wait logs exactly once across several rounds.
# ----------
usleep(1_500_000);	  # let a few more sampling rounds run
my $log = slurp_file($node->logfile);
my @hits = ($log =~ /cluster hang manager: backend pid $bpid waiting/g);
is(scalar(@hits), 1,
   'L2 long wait logged exactly once across rounds (LOG-once de-dup)');


# ----------
# L9 (Hardening v1.2): cluster.hang_dump_enabled=off does NOT suppress the
# pg_cluster_state hang category -- it only freezes the dumps_emitted /
# last_dump_emitted_at accounting and the long-wait LOG-once.  The on-demand
# diagnostic view stays available (sampling itself is gated by
# hang_manager_enabled, not by this knob).
# ----------
$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.hang_dump_enabled = off");
$node->reload;
usleep(1_500_000);	  # let the off setting take effect + a round pass
my $dumps_off1 = hang_val('hang_dumps_emitted');
usleep(1_500_000);	  # several more sampling rounds with dump_enabled=off
my $dumps_off2 = hang_val('hang_dumps_emitted');
is(hang_val('hang_dump_enabled'), 'f', 'L9 hang_dump_enabled reflects off');
is(hang_val('hang_available'), 't',
   'L9 pg_cluster_state hang category still present when dump_enabled=off');
cmp_ok(hang_row_match('^hang_sample[0-9]+_pid$', $bpid), '>=', '1',
	   'L9 per-row hang sample for B still visible when dump_enabled=off');
is($dumps_off1, $dumps_off2,
   'L9 hang_dumps_emitted frozen while dump_enabled=off (accounting suppressed)');
# restore dump accounting before the manager-off test
$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.hang_dump_enabled = on");
$node->reload;


# ----------
# L6: disabling the manager stops sampling; the hang category then freezes at
# the last sampled state (cumulative counters retained, NOT zeroed -- Hardening
# v1.2 / F3 contract).
# ----------
$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.hang_manager_enabled = off");
$node->reload;
usleep(800_000);	  # let the off setting take effect on the next DIAG tick
my $taken_off1 = hang_val('hang_samples_taken');
usleep(1_500_000);	  # several DIAG ticks with sampling disabled
my $taken_off2 = hang_val('hang_samples_taken');
is($taken_off1, $taken_off2,
   'L6 hang_manager_enabled=off stops sampling (samples_taken frozen)');
is(hang_val('hang_manager_enabled'), 'f',
   'L6 hang category reflects disabled state');
cmp_ok(hang_val('hang_samples_taken'), '>', '0',
	   'L6 cumulative samples_taken retained (frozen, not zeroed) when manager off');
is(hang_val('hang_available'), 't',
   'L6 hang category still present (frozen) when manager off');


# Clean up the blocked sessions.
$ha->query_safe('ROLLBACK') if defined $ha;
$ha->quit if defined $ha;
$hb->quit if defined $hb;

$node->stop;

done_testing();
