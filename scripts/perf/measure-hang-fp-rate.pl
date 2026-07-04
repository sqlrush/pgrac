#!/usr/bin/perl
#-------------------------------------------------------------------------
# measure-hang-fp-rate.pl -- spec-5.20 D6 (MG-A): Hang Manager false-positive
# disposition rate measurement (report-only, measure-and-decide).
#
# Spins a single --enable-cluster node, runs a mixed chaos campaign (real hangs
# + healthy-slow active queries + idle backends) under ADVISORY mode, and
# measures the spurious-but-safe recommendation rate
#   (advisory_recommendations / resolve_evaluations)
# alongside the HARD invariant that ZERO genuinely-unsafe dispositions occurred
# (no healthy / idle backend was ever killed -- advisory issues no signals).
#
# The FP rate is a tuning reference (< 1% target), NOT a ship bar; any
# genuinely-unsafe disposition is a P0 (HG#2), reported as unsafe_dispositions.
# Soundness is MEASURE_ONLY here: a single-node synthetic campaign is not a
# production-diverse wait population (spec §3.2 MG-A soundness gate).
#
# Emits a spec-5.20 acceptance-report JSON fragment to --results-dir.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
# Spec: spec-5.20-hang-manager-acceptance.md (§1.2 D6, §3.2 MG-A)
# IDENTIFICATION
#    scripts/perf/measure-hang-fp-rate.pl
#-------------------------------------------------------------------------

use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../src/test/perl";
use lib "$FindBin::RealBin/../../src/test/cluster_tap/lib";

use Getopt::Long;
use Time::HiRes qw(usleep);

our ($have_mods);
BEGIN {
	eval {
		require PostgreSQL::Test::Cluster;
		require PostgreSQL::Test::Utils;
		require PostgreSQL::Test::HangChaos;
		require PostgreSQL::Test::HangManagerAcceptanceReport;
		require PgracClusterNode;
		$have_mods = 1;
	};
}

my $results_dir    = '.';
my $timestamp      = 'manual';
my $window         = 20;		# campaign seconds
my $seed           = 1;
my $enable_install = $ENV{PGRAC_ENABLE_INSTALL};
GetOptions(
	'results-dir=s'    => \$results_dir,
	'timestamp=s'      => \$timestamp,
	'window=i'         => \$window,
	'seed=i'           => \$seed,
	'enable-install=s' => \$enable_install,
) or die "usage: $0 [--results-dir=DIR --timestamp=TS --window=SECS --seed=N --enable-install=DIR]\n";

die "PostgreSQL::Test modules not loadable (run via measure-hang-fp-rate.sh)\n"
	unless $have_mods;

# HangChaos pulls in Test::More; this is a report script, not a TAP test, so
# disable Test::More's end-of-run "no plan" exit-code manipulation.
require Test::More;
Test::More->builder->no_ending(1);

my @new_args = ("fpmeasure_$timestamp");
push @new_args, (install_path => $enable_install) if $enable_install;
my $node = PgracClusterNode->new(@new_args);
$node->init;
my $chaos = PostgreSQL::Test::HangChaos->new($node);
# advisory mode by default; lower the threshold so a campaign round's real hangs
# actually cross it (and get sampled + evaluated) within the measurement window.
$chaos->apply_fast_conf('cluster.hang_threshold_ms = 500');
$node->start;

my $base = $chaos->counters;

# Mixed workload, kept deliberately simple + robust: each round injects a real
# idle-in-tx hang (a blocker + waiter that crosses the lowered threshold and is
# sampled/evaluated) alongside a healthy-slow CPU decoy (must never be disposed).
# Hold each round PAST the threshold + a couple of sample/resolve rounds so the
# FP rate measures real evaluations, THEN tear it down.  (A heavier chain/convoy
# campaign churns many background psql handles and is unnecessary for the rate.)
my $end = time() + $window;
my $rounds = 0;
while (time() < $end)
{
	my $root = $chaos->idle_in_tx_blocker;
	$chaos->waiter_on($root->{table});
	$chaos->healthy_slow_query(30_000_000);
	usleep(1_500_000);	# > threshold(500) + a few 200ms sample/resolve rounds
	$rounds++;
	$chaos->cleanup;
}

my $evals = $chaos->delta($base, 'hang_resolve_evaluations');
my $recs  = $chaos->delta($base, 'hang_advisory_recommendations');
# HG#2 hard invariant: advisory issues no signals -> no unsafe disposition.
my $terms  = $chaos->delta($base, 'hang_terminates_issued');
my $cancs  = $chaos->delta($base, 'hang_soft_cancels_issued');
my $unsafe = $terms + $cancs;	# in advisory this MUST be 0

my $rate = ($evals > 0) ? sprintf('%.4f', $recs / $evals) : undef;

$node->stop;

# Build the report fragment.
my $r = PostgreSQL::Test::HangManagerAcceptanceReport::new_report();
$r->{false_positive}{value} = {
	rate => $rate, evaluations => $evals, recommendations => $recs,
};
$r->{false_positive}{soundness}           = 'MEASURE_ONLY';
$r->{false_positive}{unsafe_dispositions} = $unsafe;
push @{ $r->{limitation} },
	'MG-A single-node synthetic campaign: FP rate is MEASURE_ONLY (not a '
	. 'production-diverse wait population); < 1% is a tuning reference, not a ship bar.';

my ($ok, $errs) = PostgreSQL::Test::HangManagerAcceptanceReport::validate($r);
if (!$ok)
{
	print STDERR "report validation FAILED: " . join('; ', @$errs) . "\n";
	exit 1;
}

my $json = PostgreSQL::Test::HangManagerAcceptanceReport::to_json($r);
my $out  = "$results_dir/hang-fp-rate-$timestamp.json";
open(my $fh, '>', $out) or die "open $out: $!";
print $fh $json, "\n";
close $fh;

print "MG-A false-positive rate: "
	. (defined $rate ? $rate : 'n/a')
	. " (recs=$recs / evals=$evals; unsafe_dispositions=$unsafe; rounds=$rounds)\n";
print "wrote $out\n";
# Non-zero exit ONLY on the HG#2 hard invariant (a genuinely-unsafe disposition).
exit($unsafe == 0 ? 0 : 2);
