#-------------------------------------------------------------------------
#
# 333_spec326_writetax_lever_isolation.pl
#	  spec-3.26 — same-runner four-arm lever-isolation write-tax measurement.
#
#	  The cross-nightly comparison (A-only 5.74% vs A+B 7.56%) was confounded:
#	  the two runs landed on runners whose native (cluster=off) baseline differed
#	  2.3x (5091 vs 11573 tps), and the cluster tax % is itself runner-speed
#	  dependent (a partly-fixed per-op overhead is a larger fraction at higher
#	  TPS).  So a cross-run delta cannot prove Lever B helped, hurt, or that A+B
#	  reaches <= 5%.
#
#	  This test interleaves FOUR arms in the SAME job / SAME runner, in the same
#	  order every round, so the runner speed + momentary load cancel in the
#	  per-round ratio and the medians give clean per-lever deltas:
#
#	    arm1  native                          : cluster.enabled = off
#	    arm2  cluster + generic + no-fastpath : pre-A/B baseline
#	    arm3  cluster + bespoke + no-fastpath : Lever A only
#	    arm4  cluster + bespoke + fastpath    : Lever A + Lever B
#
#	  arms 2-4 toggle on the SAME cluster instance via ALTER SYSTEM +
#	  pg_reload_conf() of cluster.itl_finish_wal_mode {generic,bespoke} and
#	  cluster.undo_buf_pin_fastpath {off,on} (both PGC_SUSET), verified through a
#	  fresh connection before each pgbench.
#
#	  REPORT-ONLY (measure-first): prints native tps, each arm's median tax %,
#	  the Lever A delta, the Lever B delta, and whether A+B median <= 5%.  It does
#	  NOT gate -- the <= 5% ship decision is taken from this clean isolated data,
#	  not from a cross-run difference.
#
# Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/333_spec326_writetax_lever_isolation.pl
#
# Spec: spec-3.26-single-node-write-tax-cpu-closure.md
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

# One pgbench TPC-B (-N) run against a node; returns tps (float) or undef.
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

sub poll_sql_eq
{
	my ($node, $sql, $want, $timeout_s) = @_;
	$timeout_s //= 15;
	my $deadline = time + $timeout_s;
	while (time < $deadline)
	{
		my $got = eval { $node->safe_psql('postgres', $sql); };
		return 1 if defined $got && $got eq $want;
		select(undef, undef, undef, 0.25);
	}
	return 0;
}

# Switch the cluster instance to (itl_finish_wal_mode, undo_buf_pin_fastpath) and
# confirm a fresh connection observes the reloaded values.
sub set_arm
{
	my ($node, $mode, $fast) = @_;
	$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.itl_finish_wal_mode = '$mode'");
	$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.undo_buf_pin_fastpath = '$fast'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	return 0 unless poll_sql_eq($node, 'SHOW cluster.itl_finish_wal_mode', $mode, 10);
	return 0 unless poll_sql_eq($node, 'SHOW cluster.undo_buf_pin_fastpath', $fast, 10);
	return 1;
}

# fsync off removes disk variance so the measurement isolates the cluster
# machinery's added CPU work (the per-op overhead the levers target).
my @perf_conf = (
	"autovacuum = off\n",
	"fsync = off\n",
	"shared_buffers = 64MB\n",
	"max_wal_size = 4GB\n",
);

my $native = PostgreSQL::Test::Cluster->new('lev_native');
$native->init;
$native->append_conf('postgresql.conf', $_) for @perf_conf;
$native->start;

my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
my $clu     = PostgreSQL::Test::Cluster->new('lev_cluster');
$clu->init;
$clu->append_conf('postgresql.conf', "cluster.enabled = on\n");
$clu->append_conf('postgresql.conf', "cluster.interconnect_tier = tier1\n");
$clu->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$clu->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$clu->append_conf('postgresql.conf', $_) for @perf_conf;
PostgreSQL::Test::Utils::append_to_file(
	$clu->data_dir . '/pgrac.conf',
	"[cluster]\nname = lev_cluster\n\n[node.0]\ninterconnect_addr = 127.0.0.1:$ic_port\n\n");
$clu->start;

my $init_ok = pgbench_init($native) && pgbench_init($clu);

my (@nat, @gen, @a, @ab, @tgen, @ta, @tab);
if ($init_ok)
{
	for my $r (1 .. $ROUNDS)
	{
		my $n = pgbench_one($native);

		my $cg = set_arm($clu, 'generic', 'off') ? pgbench_one($clu) : undef;
		my $ca = set_arm($clu, 'bespoke', 'off') ? pgbench_one($clu) : undef;
		my $cb = set_arm($clu, 'bespoke', 'on')  ? pgbench_one($clu) : undef;

		next
		  unless defined $n  && $n > 0
		  && defined $cg && $cg > 0
		  && defined $ca && $ca > 0
		  && defined $cb && $cb > 0;

		push @nat, $n;
		push @gen, $cg;
		push @a,   $ca;
		push @ab,  $cb;
		push @tgen, 100.0 * (1.0 - $cg / $n);
		push @ta,   100.0 * (1.0 - $ca / $n);
		push @tab,  100.0 * (1.0 - $cb / $n);
		note(sprintf(
			"  round %d: native=%.0f  gen=%.0f(%.2f%%)  A=%.0f(%.2f%%)  A+B=%.0f(%.2f%%)",
			$r, $n, $cg, $tgen[-1], $ca, $ta[-1], $cb, $tab[-1]));
	}
}

$native->stop;
$clu->stop;

my $have = scalar(@tab) > 0;
ok($have, "four-arm interleaved measurement produced >= 1 valid round");

SKIP:
{
	skip "no valid rounds (pgbench init/run failed)", 1 unless $have;

	my $tg  = median(@tgen);
	my $taa = median(@ta);
	my $tbb = median(@tab);
	my $a_delta = $tg - $taa;     # pp the bespoke record removed vs generic
	my $b_delta = $taa - $tbb;    # pp the undo-pin fast path removed vs A-only

	# diag() (STDERR) so the medians reach the captured CI console log even
	# without prove -v (note() STDOUT comments are suppressed; mirrors t/328).
	diag("=== spec-3.26 lever isolation (same-runner, median of "
		  . scalar(@tab)
		  . " interleaved rounds) ===");
	diag(sprintf("  native (cluster off) median tps        = %.0f", median(@nat)));
	diag(sprintf("  arm2 generic + no-fastpath  tax = %6.2f%%  (tps %.0f)  [pre-A/B baseline]",
		$tg, median(@gen)));
	diag(sprintf("  arm3 bespoke + no-fastpath  tax = %6.2f%%  (tps %.0f)  [Lever A only]",
		$taa, median(@a)));
	diag(sprintf("  arm4 bespoke + fastpath     tax = %6.2f%%  (tps %.0f)  [Lever A + B]",
		$tbb, median(@ab)));
	diag(sprintf("  Lever A delta (generic -> bespoke)     = %+.2f pp", $a_delta));
	diag(sprintf("  Lever B delta (no-fastpath -> fastpath)= %+.2f pp", $b_delta));
	diag(sprintf("  A+B median tax = %.2f%%   <= 5%% ? %s   (REPORT-ONLY, not a gate)",
		$tbb, ($tbb <= 5.0 ? 'YES' : 'NO')));

	ok(1, "lever-isolation measurement reported (report-only; ship decision is manual)");
}

done_testing();
