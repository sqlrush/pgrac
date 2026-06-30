#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 332_write_tax_profile.pl
#    spec-5.19 Thread B — single-node write-tax CPU PROFILE (nightly artifact).
#
#    The residual single-node cluster write tax (MG-B / M1 in t/328) is per-DML
#    hot-path MACHINERY CPU, not WAL bytes (a deterministic WAL-bytes breakdown
#    showed the cluster-added WAL is dominated by commit-BATCHED undo/commit-
#    stamp records that cost ~no CPU under fsync=off).  WAL bytes do NOT predict
#    the TPS tax and the box that ships this cannot be CPU-profiled reliably
#    (CPU-saturated), so this leg produces the authoritative per-function CPU
#    profile on the clean nightly runner: it `perf record`s a single-writer
#    pgbench TPC-B run against (a) a native node and (b) a cluster single-node,
#    then emits the top postgres symbols of each so the dominant cluster_* hot
#    spot (undo construct / ITL stamp / SCN / commit re-pin) can be identified
#    before any rule-8.A optimization.
#
#    This is a PROFILING ARTIFACT, never a gate: if perf is unavailable (e.g.
#    macOS local, or a runner without perf_event access) the leg records the
#    reason and PASSES -- it must never fail t/332 or block ship.  The profile
#    text reaches the captured nightly log via diag().
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/332_write_tax_profile.pl
#
#-------------------------------------------------------------------------
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $PGBENCH = $ENV{PGBENCH} // 'pgbench';
my $SCALE   = 10;
my $SECS    = $ENV{PGRAC_PROFILE_SECS} // 20;
my $TOPN    = 30;

my @perf_conf = ("autovacuum = off\n", "fsync = off\n",
				 "shared_buffers = 64MB\n", "max_wal_size = 8GB\n");

# Locate a usable `perf` (Linux).  Returns the path or undef.
sub find_perf
{
	return undef if $^O ne 'linux';
	my $p = `command -v perf 2>/dev/null`;
	chomp $p;
	return ($p && -x $p) ? $p : undef;
}

# perf-record a single-writer pgbench run against $node, then emit the top
# postgres symbols.  Returns the perf report text (or undef on failure).
sub profile_node
{
	my ($perf, $node, $label, $datadir) = @_;
	my $conn = '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
	my $data = "$datadir/perf_${label}.data";
	# -a (system wide) avoids racing for the short-lived backend pid; the runner
	# is otherwise idle so postgres dominates.  Needs kernel.perf_event_paranoid
	# <= 0 (the nightly workflow sets it).  perf wraps the pgbench run so the
	# sample window == the load window.
	my $rc = system("$perf record -g -a -o '$data' -- "
		. "$PGBENCH -n -T $SECS -c 1 -j 1 -N $conn >/dev/null 2>&1");
	if ($rc != 0 || !-s $data)
	{
		diag("[$label] perf record failed (rc=$rc) -- skipping profile");
		return undef;
	}
	my $rep = `$perf report --stdio --percent-limit 0.3 -i '$data' 2>/dev/null`;
	# Keep only the symbol lines (those with a leading percentage), top N.
	my @lines;
	for my $l (split /\n/, $rep)
	{
		next unless $l =~ /^\s*\d+\.\d+%/;
		push @lines, $l;
		last if @lines >= $TOPN;
	}
	return join("\n", @lines);
}

my $perf = find_perf();

my $native = PostgreSQL::Test::Cluster->new('wtp_native');
$native->init;
$native->append_conf('postgresql.conf', $_) for @perf_conf;
$native->start;

my $ic = PostgreSQL::Test::Cluster::get_free_port();
my $clu = PostgreSQL::Test::Cluster->new('wtp_cluster');
$clu->init;
$clu->append_conf('postgresql.conf', "cluster.enabled = on\n");
$clu->append_conf('postgresql.conf', "cluster.interconnect_tier = tier1\n");
$clu->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$clu->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$clu->append_conf('postgresql.conf', $_) for @perf_conf;
PostgreSQL::Test::Utils::append_to_file($clu->data_dir . '/pgrac.conf',
	"[cluster]\nname = wtp_cluster\n\n[node.0]\ninterconnect_addr = 127.0.0.1:$ic\n\n");
$clu->start;

system("$PGBENCH -i -s $SCALE -q -h " . $native->host . ' -p ' . $native->port
	. " postgres >/dev/null 2>&1");
system("$PGBENCH -i -s $SCALE -q -h " . $clu->host . ' -p ' . $clu->port
	. " postgres >/dev/null 2>&1");

my $datadir = $ENV{TESTDATADIR} || PostgreSQL::Test::Utils::tempdir();

if (!defined $perf)
{
	diag("write-tax CPU profile UNAVAILABLE: perf not found (OS=$^O). "
		. "This profiling artifact runs on the Linux nightly runner; "
		. "local/macOS records the reason and passes (never a gate).");
	ok(1, "P1 write-tax CPU profile unavailable (perf absent) -- artifact leg, never a gate");
}
else
{
	my $nat = profile_node($perf, $native, 'native', $datadir);
	my $clr = profile_node($perf, $clu, 'cluster', $datadir);
	if (defined $nat && defined $clr)
	{
		diag("===== write-tax CPU profile: NATIVE top $TOPN symbols =====\n$nat");
		diag("===== write-tax CPU profile: CLUSTER top $TOPN symbols =====\n$clr");
		diag("===== analysis hint: cluster_* symbols present ONLY in the CLUSTER "
			. "list (and their %) are the per-DML write-tax hot spots to target =====");
		# Surface cluster_* lines explicitly for quick scanning.
		my @hot = grep { /cluster_/ } split /\n/, $clr;
		diag("cluster_* hot symbols (cluster profile):\n" . join("\n", @hot)) if @hot;
		ok(1, "P1 write-tax CPU profile captured (native + cluster top $TOPN symbols in log)");
	}
	else
	{
		diag("write-tax CPU profile could not be captured (perf record/report "
			. "failed -- likely kernel.perf_event_paranoid too high; the nightly "
			. "workflow lowers it).  Recorded + passing (never a gate).");
		ok(1, "P1 write-tax CPU profile unavailable (perf record/report failed) -- never a gate");
	}
}

$native->stop;
$clu->stop;
done_testing();
