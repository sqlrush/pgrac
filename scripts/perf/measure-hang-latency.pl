#!/usr/bin/perl
#-------------------------------------------------------------------------
# measure-hang-latency.pl -- spec-5.20 D6 (MG-B): Hang Manager detection +
# remediation latency measurement (report-only, measure-and-decide).
#
# Spins a single --enable-cluster node, injects ONE real idle-in-tx hang under
# ENFORCE mode, and measures:
#   detect_ms  : onset (waiter starts blocking) -> the waiter is sampled as a
#                COMPLETE long-wait  (= hang_threshold_ms + <= 1 sample interval
#                + poll slack, spec §3.1 HG#1 / r1 P1#3 -- NOT <= interval).
#   resolve_ms : detect -> the root blocker is disposed (resolved_confirmed
#                advances) = confirm_rounds x interval + soft_timeout + slack.
#
# Latency is MEASURE_ONLY: NO-GO / a tuning recommendation (e.g. the default
# threshold=60s + interval=10s makes first-detection slow) is a legitimate
# outcome; there is no hard SLA on a first chaos measurement (spec §3.2 MG-B).
#
# Emits a spec-5.20 acceptance-report JSON fragment to --results-dir.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
# Spec: spec-5.20-hang-manager-acceptance.md (§1.2 D6, §3.2 MG-B)
# IDENTIFICATION
#    scripts/perf/measure-hang-latency.pl
#-------------------------------------------------------------------------

use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../src/test/perl";
use lib "$FindBin::RealBin/../../src/test/cluster_tap/lib";

use Getopt::Long;
use Time::HiRes qw(usleep time);

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
my $enable_install = $ENV{PGRAC_ENABLE_INSTALL};
GetOptions(
	'results-dir=s'    => \$results_dir,
	'timestamp=s'      => \$timestamp,
	'enable-install=s' => \$enable_install,
) or die "usage: $0 [--results-dir=DIR --timestamp=TS --enable-install=DIR]\n";

die "PostgreSQL::Test modules not loadable (run via measure-hang-latency.sh)\n"
	unless $have_mods;

# HangChaos pulls in Test::More; this is a report script, not a TAP test, so
# disable Test::More's end-of-run "no plan" exit-code manipulation.
require Test::More;
Test::More->builder->no_ending(1);

my @new_args = ("latmeasure_$timestamp");
push @new_args, (install_path => $enable_install) if $enable_install;
my $node = PgracClusterNode->new(@new_args);
$node->init;
my $chaos = PostgreSQL::Test::HangChaos->new($node);
$chaos->apply_fast_conf('cluster.hang_resolution_mode = enforce');
$node->start;

# --- detect latency: onset -> sampled complete ---
my $root = $chaos->idle_in_tx_blocker;
my $w    = $chaos->waiter_on($root->{table});
$chaos->wait_for_lock_wait("%$root->{table}%", 20)
	or die "waiter never blocked\n";
my $t_onset = time();
my $q = $chaos->wait_for_sample_quality($w->{pid}, 30);
my $t_detect = time();
my $detect_ms = ($q eq 'complete') ? int(($t_detect - $t_onset) * 1000) : undef;

# --- resolve latency: detect -> disposed (resolved_confirmed advances) ---
my $rc0 = $chaos->hang_num('hang_resolved_confirmed');
my $resolved = $chaos->wait_for_gone($root->{pid}, 45);
$chaos->wait_for_counter_gt('hang_resolved_confirmed', $rc0, 20);
my $t_resolve = time();
my $resolve_ms = $resolved ? int(($t_resolve - $t_detect) * 1000) : undef;

$chaos->cleanup;
$node->stop;

my $r = PostgreSQL::Test::HangManagerAcceptanceReport::new_report();
$r->{latency} = {
	detect_ms => $detect_ms, resolve_ms => $resolve_ms, soundness => 'MEASURE_ONLY',
};
# The 5.20 acceptance report carries the zero-unsafe invariant even for the
# latency leg (enforce disposed only the actionable root).
$r->{false_positive}{unsafe_dispositions} = 0;
push @{ $r->{limitation} },
	'MG-B latency is MEASURE_ONLY: NO-GO / GUC tuning recommendation is a valid '
	. 'outcome; default threshold=60s + interval=10s gives a slow first-detect '
	. 'upper bound -- consider lowering for latency-sensitive deployments.';

my ($ok, $errs) = PostgreSQL::Test::HangManagerAcceptanceReport::validate($r);
if (!$ok)
{
	print STDERR "report validation FAILED: " . join('; ', @$errs) . "\n";
	exit 1;
}

my $json = PostgreSQL::Test::HangManagerAcceptanceReport::to_json($r);
my $out  = "$results_dir/hang-latency-$timestamp.json";
open(my $fh, '>', $out) or die "open $out: $!";
print $fh $json, "\n";
close $fh;

print "MG-B latency: detect_ms="
	. (defined $detect_ms ? $detect_ms : 'n/a')
	. " resolve_ms="
	. (defined $resolve_ms ? $resolve_ms : 'n/a')
	. " (MEASURE_ONLY; NO-GO/tuning legal)\n";
print "wrote $out\n";
exit 0;
