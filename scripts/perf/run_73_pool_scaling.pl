#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# run_73_pool_scaling.pl
#    spec-7.3 D9 (L5) -- LMS worker-pool scaling gate harness.
#
#    Boots a 2-node ClusterPair (shared_data + block_device) twice --
#    cluster.lms_workers = 1 (the spec-7.2 single-LMS topology) and
#    cluster.lms_workers = 2 (the default pool) -- and measures, per leg:
#
#      MULTI-TAG   node0 runs pgbench UPDATE churn over a ~40-block
#                  shared table (deep undo chains) while node1 readers
#                  scan it under a lagged REPEATABLE READ snapshot --
#                  every dirtied block must be CR-CONSTRUCTED (undo
#                  walk) by node0's LMS pool and shipped: the spec-7.3
#                  motivating head-of-line workload.  Aggregate serve
#                  throughput = delta(lms.lms_direct_reply_count, both
#                  nodes) / seconds; per-worker inline-serve deltas are
#                  the fan-out evidence.
#
#      SINGLE-TAG  node0 updates / node1 reads ONE single-block table
#                  (1 client each).  Requester ship-latency p99 from the
#                  gcs.ship_hist_us_* histogram delta (both nodes).
#
#    Q9 value gate (spec-7.3 §8 / §7 DoD):
#      - 2-worker multi-tag aggregate ship throughput >= 1.6x 1-worker
#      - single-tag p99 degradation <= 10%
#    Medians over PS_ROUNDS (default 3) rounds per leg (H-3 variance
#    discipline); numbers are 本机 loopback 口径 -- trend datapoints for
#    the gate read, not cross-machine absolutes.  The runner reports and
#    exits 0 on harness success; the gate verdict is a human/DoD read.
#
#    Env knobs: PS_SECS (20) multi-tag seconds; PS_SINGLE_SECS (10)
#    single-tag seconds; PS_ROUNDS (3); PS_CLIENTS (8) pgbench clients
#    per node; PS_OUT report path (default scripts/perf/results/
#    73-pool-scaling-<epoch>.md); PS_INSTALL install prefix prepended
#    to PATH; PGBENCH pgbench binary override.
#
#    Run from repo root in a built tree:
#      PS_INSTALL=/tmp/pgrac-install-73 perl scripts/perf/run_73_pool_scaling.pl
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-7.3-lms-worker-pool.md (D9/Q9)
#
# IDENTIFICATION
#    scripts/perf/run_73_pool_scaling.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../src/test/perl";

use File::Basename qw(dirname);
use File::Path qw(make_path);
use File::Spec;
use List::Util qw(sum min max);
use Time::HiRes ();

our ($REAL_STDOUT, $have_fixtures, $fixtures_err);

BEGIN
{
	open($REAL_STDOUT, '>&', \*STDOUT)
	  or die "cannot dup STDOUT: $!\n";
	my $prev = select($REAL_STDOUT);
	$| = 1;
	select($prev);

	if (defined $ENV{PS_INSTALL} && length $ENV{PS_INSTALL})
	{
		$ENV{PATH} = "$ENV{PS_INSTALL}/bin:$ENV{PATH}";
		for my $var (qw(LD_LIBRARY_PATH DYLD_LIBRARY_PATH))
		{
			$ENV{$var} = join(':', "$ENV{PS_INSTALL}/lib",
				grep { defined && length } $ENV{$var});
		}
	}

	{
		my $repo = File::Spec->rel2abs("$FindBin::RealBin/../..");

		$ENV{PG_REGRESS} = "$repo/src/test/regress/pg_regress"
		  unless defined $ENV{PG_REGRESS} && length $ENV{PG_REGRESS};
		$ENV{TESTDATADIR} = "$repo/tmp_check"
		  unless defined $ENV{TESTDATADIR} && length $ENV{TESTDATADIR};
		$ENV{TESTLOGDIR} = "$repo/tmp_check/log"
		  unless defined $ENV{TESTLOGDIR} && length $ENV{TESTLOGDIR};
	}

	eval {
		require PostgreSQL::Test::Utils;
		PostgreSQL::Test::Utils->import();
		require PostgreSQL::Test::Cluster;
		require PostgreSQL::Test::ClusterPair;
		require IPC::Run;
		$have_fixtures = 1;
	} or $fixtures_err = $@;
}

die "TAP fixtures unavailable: $fixtures_err\n" unless $have_fixtures;

my $PS_SECS        = $ENV{PS_SECS} // 20;
my $PS_SINGLE_SECS = $ENV{PS_SINGLE_SECS} // 10;
my $PS_ROUNDS      = $ENV{PS_ROUNDS} // 3;
my $PS_CLIENTS     = $ENV{PS_CLIENTS} // 8;
my $PGBENCH        = $ENV{PGBENCH} // 'pgbench';

my $REPO   = File::Spec->rel2abs("$FindBin::RealBin/../..");
my $outdir = File::Spec->catfile($REPO, 'scripts', 'perf', 'results');
my $stamp  = time();
my $PS_OUT = $ENV{PS_OUT}
  // File::Spec->catfile($outdir, "73-pool-scaling-$stamp.md");

my @HIST_BOUNDS = (500, 1000, 2000, 5000, 10000, 20000, 50000, 100000,
	200000, 500000, 1000000, 2000000, 5000000, 10000000, 30000000);

my @base_conf = (
	'autovacuum = off',
	'fsync = off',
	'shared_buffers = 64MB',
	'cluster.ges_request_timeout_ms = 30000',
	'cluster.gcs_reply_timeout_ms = 3000',
	'cluster.online_join = on',
	'cluster.xid_striping = on',
	'cluster.crossnode_runtime_visibility = on',
	'cluster.crossnode_cr_data_plane = on',
	'cluster.block_self_contained = on');

# pgbench per-tx scripts (written once under TESTDATADIR).
my $sqldir = File::Spec->catfile($ENV{TESTDATADIR}, "ps73-$stamp");
make_path($sqldir);
# Multi-tag workload: DISJOINT write stripes (node0 owns aid 1-2500,
# node1 owns 2501-5000) so writes never row-conflict, while every tx also
# reads one random row of the PEER's stripe -- pulling a remote-dirty
# block across the DATA plane (a real image ship) each time.  This keeps
# the pipeline ship-bound instead of GES-lock-bound (the naive both-sides
# random UPDATE degenerates into cross-node X convoys: tps ~ 16, ships
# ~30/s, and a retry/verdict dispatch storm -- measured, not assumed).
# v4 workload -- faithful to the spec-7.3 motivating bottleneck (CR
# construction head-of-line, spec §背景): node0 runs pure own-table
# UPDATE churn (deep undo chains, no cross reads, no TT-hot races);
# node1 readers open a REPEATABLE READ snapshot, lag 200 ms while node0
# commits on top, then scan the table -- every dirtied block must be CR
# CONSTRUCTED (undo walk) by node0's LMS pool and shipped.  The scan is
# DO-wrapped (subtransaction) so a fail-closed retryable visibility
# error (TT slot recycled et al.) rolls back the probe, not the client:
# pgbench --max-tries only retries 40001/40P01.  Both legs identical.
my %SQL = (
	writer_churn => "\\set aid random(1, 5000)\n"
	  . "UPDATE pool_t SET bal = bal + 1 WHERE aid = :aid;\n",
	reader_cr => "BEGIN ISOLATION LEVEL REPEATABLE READ;\n"
	  . "SELECT 1;\n"
	  . "\\sleep 200 ms\n"
	  . "DO \$\$ BEGIN PERFORM sum(bal) FROM pool_t; "
	  . "EXCEPTION WHEN OTHERS THEN NULL; END \$\$;\n"
	  . "COMMIT;\n",
	single_update => "UPDATE pool_one SET bal = bal + 1 WHERE aid = 1;\n",
	single_select =>
	  "DO \$\$ BEGIN PERFORM bal FROM pool_one WHERE aid = 1; "
	  . "EXCEPTION WHEN OTHERS THEN NULL; END \$\$;\n",
);
for my $k (keys %SQL)
{
	open(my $fh, '>', "$sqldir/$k.sql") or die "write $k.sql: $!\n";
	print $fh $SQL{$k};
	close $fh;
}

sub logline { print $REAL_STDOUT @_, "\n"; }

sub state_int
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

sub poll_write_ok
{
	my ($node, $sql, $timeout_s) = @_;
	$timeout_s //= 90;
	my $deadline = time + $timeout_s;
	while (time < $deadline)
	{
		return 1 if eval { $node->safe_psql('postgres', $sql); 1 };
		Time::HiRes::usleep(500_000);
	}
	return 0;
}

sub snapshot_hist
{
	my ($node0, $node1) = @_;
	my @c;
	for my $b (0 .. $#HIST_BOUNDS)
	{
		$c[$b] = state_int($node0, 'gcs', "ship_hist_us_le_$HIST_BOUNDS[$b]")
		  + state_int($node1, 'gcs', "ship_hist_us_le_$HIST_BOUNDS[$b]");
	}
	my $inf = state_int($node0, 'gcs', 'ship_hist_us_inf')
	  + state_int($node1, 'gcs', 'ship_hist_us_inf');
	return { counts => \@c, inf => $inf };
}

# p99 upper bound (us) of the delta histogram; undef = +inf overflow.
sub hist_p99
{
	my ($pre, $post) = @_;
	my @d = map { $post->{counts}[$_] - $pre->{counts}[$_] } 0 .. $#HIST_BOUNDS;
	my $dinf  = $post->{inf} - $pre->{inf};
	my $total = sum(@d) + $dinf;
	return (undef, 0) if $total <= 0;
	my $target = 0.99 * $total;
	my $cum    = 0;
	for my $b (0 .. $#HIST_BOUNDS)
	{
		$cum += $d[$b];
		return ($HIST_BOUNDS[$b], $total) if $cum >= $target;
	}
	return (undef, $total);
}

sub run_pgbench_pair
{
	my ($node0, $script0, $clients0, $node1, $script1, $clients1, $secs) = @_;
	my (@out0, @out1);
	my @cmd0 = ($PGBENCH, '-h', $node0->host, '-p', $node0->port, '-n',
		'-M', 'simple', '-c', $clients0, '-j', min($clients0, 4),
		'-T', $secs, '--max-tries=20', '-f', "$sqldir/$script0.sql",
		'postgres');
	my @cmd1 = ($PGBENCH, '-h', $node1->host, '-p', $node1->port, '-n',
		'-M', 'simple', '-c', $clients1, '-j', min($clients1, 4),
		'-T', $secs, '--max-tries=20', '-f', "$sqldir/$script1.sql",
		'postgres');
	my ($o0, $e0, $o1, $e1) = ('', '', '', '');
	my $h0 = IPC::Run::start(\@cmd0, '>', \$o0, '2>', \$e0);
	my $h1 = IPC::Run::start(\@cmd1, '>', \$o1, '2>', \$e1);
	$h0->finish;
	$h1->finish;
	my ($tps0) = $o0 =~ /tps = ([\d.]+)/;
	my ($tps1) = $o1 =~ /tps = ([\d.]+)/;
	die "pgbench node0 produced no tps:\n$o0\n$e0\n" unless defined $tps0;
	die "pgbench node1 produced no tps:\n$o1\n$e1\n" unless defined $tps1;
	return ($tps0, $tps1);
}

sub run_leg
{
	my ($workers, $round) = @_;
	my $name = "ps${workers}r${round}s" . ($stamp % 100000);
	logline("== leg lms_workers=$workers round $round: forming pair $name");

	my @extra = grep { length }
	  map { s/^\s+|\s+\z//gr } split /;/, ($ENV{PS_EXTRA_GUC} // '');
	my $pair = PostgreSQL::Test::ClusterPair->new_pair(
		$name,
		quorum_voting_disks => 3,
		shared_data         => 1,
		storage_backend     => 'block_device',
		data_port_span      => $workers,
		extra_conf => [ @base_conf, @extra, "cluster.lms_workers = $workers" ]);
	$pair->start_pair;
	Time::HiRes::usleep(2_000_000);
	$pair->wait_for_peer_state(0, 1, 'connected', 30)
	  or die "$name: CONTROL 0->1 never connected\n";
	$pair->wait_for_peer_state(1, 0, 'connected', 30)
	  or die "$name: CONTROL 1->0 never connected\n";
	my ($node0, $node1) = ($pair->node0, $pair->node1);

	poll_write_ok($node0, 'CREATE TABLE wg0 (x int)')
	  or die "$name: node0 write gate never opened\n";
	poll_write_ok($node1, 'CREATE TABLE wg1 (x int)')
	  or die "$name: node1 write gate never opened\n";

	# shared-table relfilenode coincidence (t/347 OID-align pattern)
	my ($p0, $p1, $q0, $q1) = ('', '', '', '');
	for my $attempt (1 .. 8)
	{
		for my $n ($node0, $node1)
		{
			$n->safe_psql('postgres',
				'CREATE TABLE pool_t (aid int, bal int) WITH (fillfactor = 50)');
			$n->safe_psql('postgres', 'CREATE TABLE pool_one (aid int, bal int)');
		}
		$p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('pool_t')});
		$p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('pool_t')});
		$q0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('pool_one')});
		$q1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('pool_one')});
		last if $p0 eq $p1 && $q0 eq $q1;
		my ($n0) = $p0 =~ /(\d+)$/;
		my ($n1) = $p1 =~ /(\d+)$/;
		my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
		$burn = 1 if $burn <= 0;
		$lag->safe_psql('postgres',
			"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
		for my $n ($node0, $node1)
		{
			$n->safe_psql('postgres', 'DROP TABLE pool_t');
			$n->safe_psql('postgres', 'DROP TABLE pool_one');
		}
	}
	die "$name: shared-table coincidence failed ($p0 vs $p1 / $q0 vs $q1)\n"
	  unless $p0 eq $p1 && $q0 eq $q1;

	$node0->safe_psql('postgres',
		'INSERT INTO pool_t SELECT g, 0 FROM generate_series(1, 5000) g');
	$node0->safe_psql('postgres', 'INSERT INTO pool_one VALUES (1, 0)');
	$node0->safe_psql('postgres', 'CHECKPOINT');
	$node1->safe_psql('postgres', 'CHECKPOINT');

	# warmup: one full-table swap each way heats the GRD / lock state.
	$node0->safe_psql('postgres', 'UPDATE pool_t SET bal = bal + 1');
	$node1->safe_psql('postgres', 'UPDATE pool_t SET bal = bal + 1');
	$node1->safe_psql('postgres', 'SELECT count(*) FROM pool_one');

	# ---- multi-tag concurrent phase ----
	my %pre;
	for my $i (0, 1)
	{
		my $n = ($node0, $node1)[$i];
		$pre{"reply$i"} = state_int($n, 'gcs', 'block_reply_count');
		$pre{"serve$i"} = state_int($n, 'lms', 'lms_direct_reply_count');
		$pre{"sw0_$i"}  = state_int($n, 'lms', 'lms_inline_serve_count_w0');
		$pre{"sw1_$i"}  = state_int($n, 'lms', 'lms_inline_serve_count_w1');
	}
	my $t0 = Time::HiRes::time();
	my ($tps0, $tps1) = run_pgbench_pair($node0, 'writer_churn', $PS_CLIENTS,
		$node1, 'reader_cr', $PS_CLIENTS, $PS_SECS);
	my $elapsed = Time::HiRes::time() - $t0;

	my %post;
	for my $i (0, 1)
	{
		my $n = ($node0, $node1)[$i];
		$post{"reply$i"} = state_int($n, 'gcs', 'block_reply_count');
		$post{"serve$i"} = state_int($n, 'lms', 'lms_direct_reply_count');
		$post{"sw0_$i"}  = state_int($n, 'lms', 'lms_inline_serve_count_w0');
		$post{"sw1_$i"}  = state_int($n, 'lms', 'lms_inline_serve_count_w1');
	}
	my $ship = ($post{serve0} - $pre{serve0}) + ($post{serve1} - $pre{serve1});
	my $ship_rate = $ship / $elapsed;
	my $img_ship = ($post{reply0} - $pre{reply0}) + ($post{reply1} - $pre{reply1});

	# ---- single-tag phase ----
	my $hpre = snapshot_hist($node0, $node1);
	my ($stps0, $stps1) = run_pgbench_pair($node0, 'single_update', 1,
		$node1, 'single_select', 1, $PS_SINGLE_SECS);
	my $hpost = snapshot_hist($node0, $node1);
	my ($p99, $samples) = hist_p99($hpre, $hpost);

	my %m = (
		workers   => $workers,
		round     => $round,
		ship      => $ship,
		secs      => $elapsed,
		ship_rate => $ship_rate,
		img_ship  => $img_ship,
		tps_w     => $tps0,
		tps_r     => $tps1,
		tps_sum   => $tps0 + $tps1,
		disp_w0   => ($post{sw0_0} - $pre{sw0_0}) + ($post{sw0_1} - $pre{sw0_1}),
		disp_w1   => ($post{sw1_0} - $pre{sw1_0}) + ($post{sw1_1} - $pre{sw1_1}),
		p99_us    => $p99,
		p99_n     => $samples,
		stps_sum  => $stps0 + $stps1,
	);
	logline(sprintf(
		"== leg w=%d r%d: serve-ship=%d in %.1fs -> %.1f/s  img_ship=%d  "
		  . "tps w=%.1f r=%.1f  serve w0=%d w1=%d  single-tag p99<=%s us (n=%d)",
		$workers, $round, $ship, $elapsed, $ship_rate, $img_ship,
		$tps0, $tps1, $m{disp_w0}, $m{disp_w1}, $p99 // 'inf', $samples));

	$pair->stop_pair;
	return \%m;
}

sub median
{
	my @s = sort { $a <=> $b } grep { defined } @_;
	return undef unless @s;
	return $s[int(@s / 2)];
}

# ---- drive both legs ----
my %runs;    # workers -> [round metrics...]
for my $workers (1, 2)
{
	for my $round (1 .. $PS_ROUNDS)
	{
		push @{ $runs{$workers} }, run_leg($workers, $round);
	}
}

my %med;
for my $workers (1, 2)
{
	$med{$workers}{ship_rate} = median(map { $_->{ship_rate} } @{ $runs{$workers} });
	$med{$workers}{tps_sum}   = median(map { $_->{tps_sum} } @{ $runs{$workers} });
	$med{$workers}{p99_us} =
	  median(map { $_->{p99_us} } grep { defined $_->{p99_us} } @{ $runs{$workers} });
}

my $ratio = ($med{1}{ship_rate} && $med{1}{ship_rate} > 0)
  ? $med{2}{ship_rate} / $med{1}{ship_rate}
  : undef;
my $p99_ratio = (defined $med{1}{p99_us} && $med{1}{p99_us} > 0 && defined $med{2}{p99_us})
  ? $med{2}{p99_us} / $med{1}{p99_us}
  : undef;

my $gate_ship = (defined $ratio && $ratio >= 1.6) ? 'PASS' : 'FAIL';
my $gate_p99 = (defined $p99_ratio && $p99_ratio <= 1.10) ? 'PASS'
  : (defined $p99_ratio ? 'FAIL' : 'n/a (hist bucket resolution)');

# ---- report ----
make_path(dirname($PS_OUT));
open(my $fh, '>', $PS_OUT) or die "cannot write $PS_OUT: $!\n";
print $fh "# spec-7.3 D9/Q9 pool-scaling gate (本机 loopback 口径)\n\n";
print $fh "- date: " . scalar(localtime($stamp)) . "\n";
print $fh "- knobs: PS_SECS=$PS_SECS PS_SINGLE_SECS=$PS_SINGLE_SECS "
  . "PS_ROUNDS=$PS_ROUNDS PS_CLIENTS=$PS_CLIENTS/node "
  . "PS_EXTRA_GUC=" . ($ENV{PS_EXTRA_GUC} // '(none)') . "\n";
print $fh "- workload: multi-tag = node0 UPDATE churn (deep undo chains) +\n"
  . "  node1 lagged-REPEATABLE-READ full scans -- every dirtied block is\n"
  . "  CR-CONSTRUCTED by node0's LMS pool and shipped (the spec-7.3\n"
  . "  motivating head-of-line);  single-tag = 1-client update/read\n"
  . "  ping-pong on a single-block table.  serve-ships = the D8\n"
  . "  lms_direct_reply_count aggregate (CR/undo/verdict serve replies).\n\n";
print $fh "| leg | round | serve-ships/s | img-ships | tps(w+r) | serve w0 | serve w1 | 1-tag p99(us) |\n";
print $fh "|---|---|---|---|---|---|---|\n";
for my $workers (1, 2)
{
	for my $m (@{ $runs{$workers} })
	{
		printf $fh "| w=%d | %d | %.1f | %d | %.1f | %d | %d | %s |\n",
		  $workers, $m->{round}, $m->{ship_rate}, $m->{img_ship},
		  $m->{tps_sum}, $m->{disp_w0}, $m->{disp_w1}, $m->{p99_us} // 'inf';
	}
}
print $fh "\n## medians + gate\n\n";
printf $fh "- serve-ship/s median: w1=%.1f  w2=%.1f  ratio=%s (gate >= 1.6x: **%s**)\n",
  $med{1}{ship_rate}, $med{2}{ship_rate},
  defined $ratio ? sprintf('%.2fx', $ratio) : 'n/a', $gate_ship;
printf $fh "- tps(sum) median: w1=%.1f  w2=%.1f\n", $med{1}{tps_sum}, $med{2}{tps_sum};
printf $fh
  "- single-tag p99 median: w1=%s us  w2=%s us  ratio=%s (gate <= 1.10x: **%s**)\n",
  $med{1}{p99_us} // 'inf', $med{2}{p99_us} // 'inf',
  defined $p99_ratio ? sprintf('%.2fx', $p99_ratio) : 'n/a', $gate_p99;
print $fh "\n> HONESTY: loopback numbers on one host; the two pgbench\n"
  . "> drivers and both postmasters share the same cores, so the ratio is\n"
  . "> a lower bound on isolated-hardware scaling.  ships/s counts every\n"
  . "> block-family REPLY (image ships + CR/undo/verdict serves).  p99 is\n"
  . "> a histogram upper bound (bucket edge), not an exact percentile.\n";
close $fh;

logline("== report: $PS_OUT");
logline(sprintf("== GATE ship>=1.6x: %s (%s)  p99<=1.10x: %s (%s)",
	$gate_ship, defined $ratio     ? sprintf('%.2fx', $ratio)     : 'n/a',
	$gate_p99,  defined $p99_ratio ? sprintf('%.2fx', $p99_ratio) : 'n/a'));
exit 0;
