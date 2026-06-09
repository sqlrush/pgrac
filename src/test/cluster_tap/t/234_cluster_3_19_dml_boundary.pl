#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 234_cluster_3_19_dml_boundary.pl
#	  spec-3.19 D0 -- deterministic-ish reproducer for the pre-existing
#	  "attempted to update invisible tuple" cluster MVCC/DML boundary bug.
#
#	  Root-cause classification (D0, grep-confirmed against perf CI logs):
#	    The single-node perf baseline (cluster.cr_mvcc_gate defaults ON)
#	    crashes concurrent pgbench UPDATE with
#	        ERROR: attempted to update invisible tuple   (heapam.c:3905)
#	    while t/227 -- the SAME pgbench full-r/w workload but with
#	    cluster.cr_mvcc_gate = off -- stays clean.  So the disagreement is
#	    between the cluster CR MVCC scan short-circuit
#	    (cluster_cr_satisfies_mvcc: judges a tuple visible from the read_scn
#	    historical image) and HeapTupleSatisfiesUpdate (single-node PG-native:
#	    rejects the live tuple as TM_Invisible).  It is a VERDICT
#	    DISAGREEMENT, not a CR-image-as-DML-target leak (the CR gate returns a
#	    bool; the executor mutates the live tuple).  See
#	    pgrac:specs/spec-3.19-...md §2.2 + docs/spec-3.19-visibility-verdict-audit.md.
#
#	  This file pins the differentiator as an executable gate:
#	    L1 (control)  cr_mvcc_gate = off  -> MUST stay clean (today: GREEN).
#	    L2 (repro)    cr_mvcc_gate = on   -> MUST stay clean (today: RED until
#	                  the D3 reconciliation fix; this is the spec-3.19 target).
#
#	  规則 8.A:  "attempted to update invisible tuple" is a hard PG guard
#	  against false-/wrong-version DML.  A RED L2 is a real correctness bug
#	  (the scan selected a tuple the update path cannot mutate) and blocks
#	  ship.
#
#	  Status: GREEN after the D3 fix (the CR-gate live-xmin guard in
#	  cluster_cr_satisfies_mvcc).  Locally this aborted at round 2-8 of an
#	  extreme-contention loop (S319_HOT_ROWS=2 S319_CLIENTS=16) before the fix
#	  and ran 40 clean rounds (200s) after.  L2's hard assertion is the
#	  regression sentinel; the retryable CR-reconstruct fail-closed is tolerated.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.19-minimal-pg-touch-mvcc-boundary.md (D0/D3)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Knobs (CI-tunable): a small HOT set + many clients maximises same-page
# concurrent UPDATE contention, which is what makes the cluster CR scan
# short-circuit fire against a live tuple a concurrent writer just changed.
# Defaults are moderate for CI stability; the bug was originally hammered out
# at S319_HOT_ROWS=2 S319_CLIENTS=16 (which also floods the *retryable* CR
# fail-closed path — see _run_workload's tolerated-error handling).
my $clients  = $ENV{S319_CLIENTS}  // 8;
my $jobs     = $ENV{S319_JOBS}     // 4;
my $seconds  = $ENV{S319_SECONDS}  // 10;
my $hot_rows = $ENV{S319_HOT_ROWS} // 4;

# Custom pgbench script: every client hammers the SAME tiny set of aids, so
# almost every UPDATE races a concurrent committed/in-progress writer on the
# same heap page (maximal CR-gate firing).
my $script = qq{\\set aid random(1, $hot_rows)
UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = :aid;
};

sub _run_workload
{
	my ($label, $cr_gate) = @_;

	my $node = PostgreSQL::Test::Cluster->new("s319_$label");
	$node->init;
	$node->append_conf('postgresql.conf',
		    "shared_buffers = 128MB\n"
		  . "cluster.enabled = on\n"
		  . "cluster.node_id = 0\n"
		  . "cluster.allow_single_node = on\n"
		  . "cluster.interconnect_tier = stub\n"
		  . "cluster.cr_mvcc_gate = $cr_gate\n"
		  . "autovacuum = off\n");
	$node->start;

	# Tiny scale: pgbench_accounts still has 100k rows at scale 1, but the
	# custom script only ever touches aids 1..$hot_rows.
	$node->command_ok(
		[ 'pgbench', '-i', '-s', '1', '-q', '-p', $node->port, '-h', $node->host,
			'postgres' ],
		"$label: pgbench init");

	my $script_path = $node->basedir . "/s319_$label.sql";
	open(my $fh, '>', $script_path) or die "cannot write $script_path: $!";
	print $fh $script;
	close($fh);

	my ($out, $err);
	my $ok = $node->run_log(
		[ 'pgbench', '-n', '-f', $script_path,
			'-c', $clients, '-j', $jobs, '-T', $seconds,
			'-p', $node->port, '-h', $node->host, 'postgres' ],
		'>', \$out, '2>', \$err);

	my $all = ($err // '') . ($out // '');

	# The spec-3.19 bug: the hard PG guard against false-/wrong-version DML.
	my $invisible = $all =~ /attempted to (?:update|delete|lock) invisible tuple/i;

	# Correct, retryable fail-closed under extreme ITL-slot-reuse contention
	# (53R9x with a "retry" hint).  NOT the bug -- pgbench without retry logic
	# just aborts the client; tolerated here, only counted.
	my $cr_retry = () = $all =~ /cluster CR cannot reconstruct block/gi;

	# Anything genuinely unexpected: a FATAL/PANIC, or a backend ERROR/pgbench
	# error that is neither the invisible-tuple bug nor the tolerated retryable
	# CR fail-closed.  We must check this explicitly: IPC::Run::run returns
	# false on pgbench abort, but this test intentionally tolerates the retryable
	# CR abort path for the high-contention L2 workload.
	my @unexpected_errors;
	for my $line (split /\n/, $all)
	{
		next if $line =~ /cluster CR cannot reconstruct block/i;
		next if $cr_retry
			&& $line =~ /pgbench:\s+error:\s+Run was aborted; the above results are incomplete\./i;
		push @unexpected_errors, $line
			if $line =~ /(?:ERROR|FATAL|PANIC):|pgbench:\s+error:/i;
	}

	my $failed = ($out // '') =~ /number of failed transactions: (\d+)/m ? $1 + 0 : 0;
	my $processed =
	  ($out // '') =~ /number of transactions actually processed: (\d+)/m ? $1 + 0 : 0;

	if ($invisible || @unexpected_errors)
	{
		my $stub = substr(($err // ''), 0, 2048);
		diag("$label pgbench stderr (first 2KB):\n$stub");
		diag("$label unexpected errors:\n" . join("\n", @unexpected_errors))
			if @unexpected_errors;
	}

	$node->stop;

	return {
		ok                => $ok ? 1 : 0,
		invisible         => $invisible ? 1 : 0,
		cr_retry          => $cr_retry,
		unexpected_errors => scalar @unexpected_errors,
		failed            => $failed,
		processed         => $processed,
	};
}


# ----------------------------------------------------------------------
# L1 control: cr_mvcc_gate = off.  The CR short-circuit is disabled, so the
# workload must be fully clean (mirrors t/227).  Isolates the CR gate as the
# locus.
# ----------------------------------------------------------------------
my $off = _run_workload('croff', 'off');
ok(!$off->{invisible},
	"L1 control (cr_mvcc_gate=off): no invisible-tuple error");
ok(!$off->{unexpected_errors},
	"L1 control (cr_mvcc_gate=off): no unexpected ERROR/FATAL/PANIC");
ok($off->{ok}, "L1 control (cr_mvcc_gate=off): pgbench exits cleanly");
is($off->{failed}, 0,
	"L1 control (cr_mvcc_gate=off): zero failed transactions");
cmp_ok($off->{processed}, '>', 0,
	"L1 control (cr_mvcc_gate=off): workload processed transactions");

# ----------------------------------------------------------------------
# L2 / spec-3.19 D3 regression gate: cr_mvcc_gate = on.  THE assertion is
# "no invisible-tuple": before the D3 CR-gate live-xmin guard this aborted
# with "attempted to update invisible tuple" (the cluster CR scan reported a
# live, still-in-ProcArray version visible that HeapTupleSatisfiesUpdate then
# rejected).  The retryable CR-reconstruct fail-closed (cr_retry) is the
# CORRECT, separate behavior under extreme ITL-slot reuse and is tolerated
# (pgbench has no retry loop here, so it can inflate aborts; not a bug).
# ----------------------------------------------------------------------
my $on = _run_workload('cron', 'on');
ok(!$on->{invisible},
	"L2 (cr_mvcc_gate=on): no invisible-tuple error [spec-3.19 D3 fix]");
ok(!$on->{unexpected_errors},
	"L2 (cr_mvcc_gate=on): no unexpected ERROR/FATAL/PANIC");
cmp_ok($on->{processed}, '>', 0,
	"L2 (cr_mvcc_gate=on): workload processed transactions");
diag("L2 tolerated retryable CR-reconstruct fail-closed count: $on->{cr_retry} "
	. "(correct behavior, not the bug)");

done_testing();
