#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Ordinary top-transaction undo: exact-zero retry and native ERROR cleanup.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;

# Bind native trace evidence to the tested backend and its retry sample.
sub exact_zero_retarget_retry_seen
{
	my ($log, $pid) = @_;

	return 0 unless defined($pid);
	return $log =~ qr/PGRAC_FAMILY=CTRC_CONSUME[^\n]* pid=\Q$pid\E [^\n]*consumer_sequence=\d+ attempt=\d+ target_ordinal=0 route_retarget=1 [^\n]*apply_call_count=2 /;
}

# The real fixed cohort must reach native OPEN. There is no test adapter,
# fabricated admission, forced receipt, file edit or success-producing hook.
my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('undo_exact_zero',
	nodes => 4, blackbox => 1,
	extra_conf => [ 'autovacuum = off', 'cluster.trace_normal_stop = on',
		'cluster.cf_enqueue_timeout_ms = 30000' ]);
$cluster->start_cluster;
my $creator = $cluster->node0;
my $node = $cluster->node1;
my $deny = 'cluster-undo-itl-apply-zero-deny';
my $returned = 'cluster-undo-ctrc-apply-return';
is($node->safe_psql('postgres',
	"SELECT count(*) FROM pg_stat_cluster_injections WHERE name IN ('$deny','$returned')"),
	'2', 'both native points are registered')
	or BAIL_OUT('native exact-zero injection prerequisites are missing');
is($node->safe_psql('postgres', 'SHOW cluster.trace_normal_stop'), 'on',
	'diagnostic opt-in is active');
$creator->safe_psql('postgres', 'CREATE TABLE undo_exact_zero (id int, value int)');
my $started = $node->safe_psql('postgres', 'SELECT pg_postmaster_start_time()');

# SKIP_N refuses exactly one real shared APPLY before target/CAS/count writes.
# Exhaustion allows the next original APPLY; the hook never returns APPLIED.
my ($out, $err);
my $rc = $node->psql('postgres',
	"SELECT cluster_inject_fault('$deny','skipn',1);\n"
	  . "INSERT INTO undo_exact_zero VALUES (1,10);\n"
	  . "SELECT 'ZERO_HITS=' || hits FROM pg_stat_cluster_injections WHERE name='$deny';\n"
	  . "SELECT cluster_inject_fault('$deny','none',0);\n"
	  . "SELECT 'ROW=' || count(*) || ':' || min(value) FROM undo_exact_zero WHERE id=1;",
	stdout => \$out, stderr => \$err, timeout => 30);
is($rc, 0, 'one denied first APPLY requalifies and the INSERT commits');
diag($err) if $rc;
like($out, qr/^ZERO_HITS=2$/m, 'real shared denial is followed by the original shared retry');
like($out, qr/^ROW=1:10$/m, 'the retried INSERT publishes exactly one row');

# Reuse the same top owner's ITL reference, including a real retarget refusal.
# COMMIT drains this UPDATE's original passive consumer trace. No later UPDATE
# may supply the retarget evidence for the injected statement.
my $log_start = length(slurp_file($node->logfile));
($out, $err) = ('', '');
$rc = $node->psql('postgres',
	"SELECT 'PID=' || pg_backend_pid();\n"
	  . "BEGIN; INSERT INTO undo_exact_zero VALUES (2,20);\n"
	  . "SELECT cluster_inject_fault('$deny','skipn',1);\n"
	  . "UPDATE undo_exact_zero SET value=21 WHERE id=2;\n"
	  . "SELECT 'REUSE_HITS=' || hits FROM pg_stat_cluster_injections WHERE name='$deny';\n"
	  . "SELECT cluster_inject_fault('$deny','none',0);\n"
	  . "COMMIT;\n"
	  . "SELECT 'REUSE_ROW=' || count(*) || ':' || min(value) FROM undo_exact_zero WHERE id=2;",
	stdout => \$out, stderr => \$err, timeout => 30);
is($rc, 0, 'borrowed ITL refusal preserves its original owner through retry');
diag($err) if $rc;
like($out, qr/^REUSE_HITS=2$/m, 'retarget denial is bounded to one call');
like($out, qr/^REUSE_ROW=1:21$/m, 'the original top transaction continues and commits');
my ($reuse_pid) = $out =~ /^PID=(\d+)$/m;
ok(defined($reuse_pid), 'borrowed-owner backend identity was captured');
if (defined($reuse_pid))
{
	ok(exact_zero_retarget_retry_seen(substr(slurp_file($node->logfile), $log_start), $reuse_pid),
		'the actual backend took the borrowed retarget retry route');
}

# Raise ERROR only after the owner binds the real ZERO return to its original
# preflight. ON_ERROR_STOP is disabled solely to prove native abort cleanup
# returns to this same backend twice. Exact error count and final rows are
# asserted; a crash, missing cut or an unrelated error cannot pass this leg.
my $sql = "SELECT 'BEFORE_PID=' || pg_backend_pid();\n";
for my $attempt (1 .. 2)
{
	my $id = 10 + $attempt;
	$sql .= "SELECT cluster_inject_fault('$deny','skipn',1);\n"
	  . "SELECT cluster_inject_fault('$returned','error',0);\n"
	  . "INSERT INTO undo_exact_zero VALUES ($id,99);\n"
	  . "SELECT cluster_inject_fault('$returned','none',0);\n"
	  . "SELECT cluster_inject_fault('$deny','none',0);\n"
	  . "SELECT 'ABSENT_$attempt=' || count(*) FROM undo_exact_zero WHERE id=$id;\n"
	  . "INSERT INTO undo_exact_zero VALUES ($id,$attempt);\n";
}
$sql .= "SELECT 'RETURN_HITS=' || hits FROM pg_stat_cluster_injections WHERE name='$returned';\n"
	. "SELECT 'AFTER_PID=' || pg_backend_pid();\n"
	. "SELECT 'RECOVERED=' || string_agg(value::text,',' ORDER BY id) FROM undo_exact_zero WHERE id IN (11,12);";
($out, $err) = ('', '');
$rc = $node->psql('postgres', $sql, on_error_stop => 0,
	stdout => \$out, stderr => \$err,
	extra_params => [ '-v', 'VERBOSITY=verbose' ], timeout => 30);
is($rc, 0, 'the same psql connection survives both injected SQL errors');
my @errors = $err =~ /^.*ERROR:.*$/mg;
is(scalar(@errors), 2, 'exactly the two intended SQL errors were reported');
for my $error (@errors)
{
	like($error, qr/XX000: cluster injection point "\Q$returned\E" armed with ERROR/,
		'the native return point is the original SQL error');
}
like($out, qr/^ABSENT_1=0$/m, 'first failed statement installs no row');
like($out, qr/^ABSENT_2=0$/m, 'second failed statement installs no row');
like($out, qr/^RETURN_HITS=2$/m, 'both errors traversed the intended return point');
my ($before_pid) = $out =~ /^BEFORE_PID=(\d+)$/m;
my ($after_pid) = $out =~ /^AFTER_PID=(\d+)$/m;
ok(defined($before_pid) && defined($after_pid), 'both native backend identities were captured');
is($after_pid, $before_pid, 'ERROR cleanup kept the original backend');
like($out, qr/^RECOVERED=1,2$/m, 'unarmed writes work after both real ERROR cleanups');
is($node->safe_psql('postgres', 'SELECT pg_postmaster_start_time()'), $started,
	'the postmaster did not restart');
for my $member ($cluster->nodes)
{
	unlike(slurp_file($member->logfile), qr/TRAP:|PANIC:|terminated by signal/,
		$member->name . ' has no assertion failure or uncertain-effect PANIC');
}
$cluster->stop_cluster;
done_testing();
