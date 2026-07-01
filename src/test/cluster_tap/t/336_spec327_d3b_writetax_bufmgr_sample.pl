#-------------------------------------------------------------------------
#
# 336_spec327_d3b_writetax_bufmgr_sample.pl
#	  spec-3.27 D3b — same-runner A+B vs A+B+C (bufmgr) write-tax SAMPLE.
#
#	  cluster.undo_buffer_backend is PGC_POSTMASTER, so the C lever (undo blocks
#	  served by the shared buffer manager) cannot be toggled on the same instance
#	  the way t/333's PGC_SUSET arms are.  This test runs THREE instances in the
#	  same job / same runner and interleaves them per round so runner speed +
#	  momentary load cancel in the medians:
#
#	    arm1  native                              : cluster.enabled = off
#	    arm2  cluster, undo_buffer_backend=legacy  : A + B (itl bespoke + fastpath)
#	    arm3  cluster, undo_buffer_backend=bufmgr   : A + B + C (D3b buffer-backed undo)
#
#	  REPORT-ONLY, and a *local pre-D5 comparison sample* only -- NOT a ship gate.
#	  The authoritative <= 5% decision is taken from the clean CI/nightly t/333
#	  fifth arm (D7).  Local runs fluctuate; do not read a ship verdict here.
#
# Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/336_spec327_d3b_writetax_bufmgr_sample.pl
#
# Spec: spec-3.27-undo-buffer-backed-model.md (D3b measurement sample)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $PGBENCH = $ENV{PGBENCH} // 'pgbench';
my $SCALE   = 10;
my $SECS    = $ENV{PGRAC_PGBENCH_SECS} // 8;
my $CLIENTS = 4;
my $ROUNDS  = $ENV{PGRAC_PGBENCH_ROUNDS} // 7;

sub median
{
	my @s = sort { $a <=> $b } @_;
	return undef unless @s;
	return $s[int((@s) / 2)];
}

sub pgbench_one
{
	my ($node) = @_;
	my $conn = '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
	my $out  = `$PGBENCH -n -T $SECS -c $CLIENTS -j 2 -N $conn 2>&1`;
	return $1 + 0.0 if $out =~ /tps\s*=\s*([\d.]+)\s*\(without initial/;
	return $1 + 0.0 if $out =~ /tps\s*=\s*([\d.]+)/;
	return undef;
}

sub pgbench_init
{
	my ($node) = @_;
	my $conn = '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
	system("$PGBENCH -i -s $SCALE -q $conn >/dev/null 2>&1");
	return $? == 0;
}

# fsync off removes disk variance so the measurement isolates the added CPU work.
my @perf_conf = (
	"autovacuum = off\n",
	"fsync = off\n",
	"shared_buffers = 64MB\n",
	"max_wal_size = 4GB\n",
);

sub make_cluster
{
	my ($name, $backend) = @_;
	my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
	my $node    = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
	$node->append_conf('postgresql.conf', "cluster.interconnect_tier = tier1\n");
	$node->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
	$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
	$node->append_conf('postgresql.conf', "cluster.undo_buffer_backend = $backend\n");
	$node->append_conf('postgresql.conf', $_) for @perf_conf;
	PostgreSQL::Test::Utils::append_to_file(
		$node->data_dir . '/pgrac.conf',
		"[cluster]\nname = $name\n\n[node.0]\ninterconnect_addr = 127.0.0.1:$ic_port\n\n");
	$node->start;
	return $node;
}

my $native = PostgreSQL::Test::Cluster->new('bm_native');
$native->init;
$native->append_conf('postgresql.conf', $_) for @perf_conf;
$native->start;

my $legacy = make_cluster('bm_legacy', 'legacy_pool');
my $bufmgr = make_cluster('bm_bufmgr', 'bufmgr');

# Confirm the C arm really engaged the bufmgr backend.
is($bufmgr->safe_psql('postgres', 'SHOW cluster.undo_buffer_backend'),
	'bufmgr', 'arm3 instance is running undo_buffer_backend = bufmgr');

my $init_ok =
     pgbench_init($native)
  && pgbench_init($legacy)
  && pgbench_init($bufmgr);

my (@nat, @ab, @abc, @tab, @tabc);
if ($init_ok)
{
	for my $r (1 .. $ROUNDS)
	{
		my $n  = pgbench_one($native);
		my $l  = pgbench_one($legacy);
		my $b  = pgbench_one($bufmgr);

		next
		  unless defined $n && $n > 0
		  && defined $l && $l > 0
		  && defined $b && $b > 0;

		push @nat, $n;
		push @ab,  $l;
		push @abc, $b;
		push @tab,  100.0 * (1.0 - $l / $n);
		push @tabc, 100.0 * (1.0 - $b / $n);
		note(sprintf(
			"  round %d: native=%.0f  A+B=%.0f(%.2f%%)  A+B+C=%.0f(%.2f%%)",
			$r, $n, $l, $tab[-1], $b, $tabc[-1]));
	}
}

$native->stop;
$legacy->stop;
$bufmgr->stop;

my $have = scalar(@tabc) > 0;
ok($have, "three-arm interleaved measurement produced >= 1 valid round");

SKIP:
{
	skip "no valid rounds (pgbench init/run failed)", 1 unless $have;

	my $tabm  = median(@tab);
	my $tabcm = median(@tabc);
	my $c_delta = $tabm - $tabcm;    # +pp = C helped; -pp = C hurt (local sample)

	diag("=== spec-3.27 D3b bufmgr write-tax SAMPLE (same-runner, median of "
		  . scalar(@tabc)
		  . " interleaved rounds) -- LOCAL COMPARISON ONLY, NOT A SHIP GATE ===");
	diag(sprintf("  native (cluster off) median tps          = %.0f", median(@nat)));
	diag(sprintf("  A+B    (legacy_pool) tax = %6.2f%%  (tps %.0f)", $tabm,  median(@ab)));
	diag(sprintf("  A+B+C  (bufmgr)      tax = %6.2f%%  (tps %.0f)", $tabcm, median(@abc)));
	diag(sprintf("  Lever C delta (A+B -> A+B+C)              = %+.2f pp  (+=C helped)", $c_delta));
	diag(sprintf("  A+B+C median tax = %.2f%%   <= 5%% ? %s   (LOCAL SAMPLE, authoritative = CI/nightly t/333)",
		$tabcm, ($tabcm <= 5.0 ? 'YES' : 'NO')));

	ok(1, "bufmgr write-tax sample reported (report-only; not a ship gate)");
}

done_testing();
