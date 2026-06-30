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
#    profile on the clean nightly runner so the dominant cluster_* hot spot
#    (undo construct / ITL stamp / SCN / commit re-pin) can be identified before
#    any rule-8.A optimization.
#
# NOTES
#    v2 (spec-5.19 Thread B sharpening): the v1 profile was too coarse to find
#    the lever -- `perf -a` over a single idle-heavy `-c 1` writer was dominated
#    by the idle swapper, and the shard's --enable-cassert build buried the real
#    hot path under AllocSetCheck/MemoryContextCheck.  v2 fixes all three:
#      1. saturate with `-c $CLIENTS -j $CLIENTS -N` so the runner is never idle
#         and the idle swapper stops dominating the samples;
#      2. report SELF-sorted (--no-children) so the leaf functions actually
#         burning CPU surface (a children sort floats only call-stack
#         ancestors whose self% ~ 0), and compute the native-vs-cluster
#         self-% DELTA in-process (cluster.enabled off vs on, same binary):
#         positive delta = the extra per-symbol CPU cluster carries = the
#         lever, including same-named-but-heavier shared paths, not just new
#         cluster_* symbols.  (In-process delta because `perf diff` emitted
#         nothing on the runner; the self-% maps are fully under our control.)
#      3. detect & loudly flag a cassert build (SHOW debug_assertions) -- the
#         dedicated nightly write-tax profiling job builds WITHOUT
#         --enable-cassert so the signal is trustworthy.
#    Still a PROFILING ARTIFACT, never a gate: if perf is unavailable (e.g.
#    macOS local, or a runner without perf_event access) the leg records the
#    reason and PASSES -- it must never fail t/332 or block ship.  The profile
#    and diff text reach the captured nightly log via diag().
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
my $SECS    = $ENV{PGRAC_PROFILE_SECS}    // 30;
my $CLIENTS = $ENV{PGRAC_PROFILE_CLIENTS} // 4;
my $TOPN    = 30;

# shared_buffers large enough to hold the scale-10 working set so buffer
# eviction CPU does not contaminate the per-DML write-tax signal.
my @perf_conf = ("autovacuum = off\n", "fsync = off\n",
				 "shared_buffers = 256MB\n", "max_wal_size = 8GB\n");

# Locate a usable `perf` (Linux).  Returns the path or undef.
sub find_perf
{
	return undef if $^O ne 'linux';
	my $p = `command -v perf 2>/dev/null`;
	chomp $p;
	return ($p && -x $p) ? $p : undef;
}

# Is the installed binary a --enable-cassert build?  Assertion checks
# (AllocSetCheck, MemoryContextCheck, ...) can dominate a cassert CPU profile
# and bury the real cluster_* hot path; the dedicated nightly profiling job
# builds WITHOUT cassert, but flag it either way so the log is unambiguous.
sub assertions_enabled
{
	my ($node) = @_;
	my $v = $node->safe_psql('postgres', 'SHOW debug_assertions');
	chomp $v;
	return ($v eq 'on');
}

# perf-record a SATURATED pgbench write run against $node.  Returns the perf
# data file path (or undef on failure).
sub profile_node
{
	my ($perf, $node, $label, $datadir) = @_;
	my $conn = '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
	my $data = "$datadir/perf_${label}.data";
	# -N (accounts-only) avoids hot-row lock contention on branches/tellers that
	# would show up as wait, not CPU.  $CLIENTS saturating writers keep the
	# runner busy so system-wide `perf record -a` is dominated by postgres, not
	# the idle swapper.  -a (not -p) because pgbench forks $CLIENTS backends a
	# single -p pid cannot follow; whatever per-symbol noise survives is
	# cancelled by the native-vs-cluster perf diff downstream.
	my $rc = system("$perf record -g -a -o '$data' -- "
		. "$PGBENCH -n -T $SECS -c $CLIENTS -j $CLIENTS -N $conn >/dev/null 2>&1");
	if ($rc != 0 || !-s $data)
	{
		diag("[$label] perf record failed (rc=$rc) -- skipping profile");
		return undef;
	}
	return $data;
}

# Raw SELF-sorted (--no-children) perf report text for one data file.  Self
# sort surfaces the leaf functions actually burning CPU (undo memcpy / ITL
# stamp / SCN advance) instead of the call-stack ancestors a children sort
# floats to the top with self% ~ 0.
sub raw_report
{
	my ($perf, $data) = @_;
	return `$perf report --stdio --no-children --percent-limit 0.3 -i '$data' 2>/dev/null`;
}

# Top-N symbol lines (those with a leading self percentage) of a report.
sub top_lines
{
	my ($rep) = @_;
	my @lines;
	for my $l (split /\n/, $rep)
	{
		next unless $l =~ /^\s*\d+\.\d+%/;
		push @lines, $l;
		last if @lines >= $TOPN;
	}
	return join("\n", @lines);
}

# symbol => self% map parsed from a self-sorted report line such as
#   "   12.34%  postgres  postgres  [.] heap_update".
sub parse_self
{
	my ($rep) = @_;
	my %h;
	for my $l (split /\n/, $rep)
	{
		next unless $l =~ /^\s*(\d+\.\d+)%/;
		my $pct = $1;
		next unless $l =~ /\[[.k]\]\s+(.+?)\s*$/;
		$h{$1} += $pct;
	}
	return \%h;
}

# native-vs-cluster self-% delta computed IN-PROCESS on the SAME binary
# (cluster.enabled off vs on) -- robust where `perf diff` emits nothing.  A
# positive delta on a cluster_* symbol (or on a shared heap/xlog/undo path
# called more heavily under cluster) is exactly the per-DML write-tax lever to
# target with a rule-8.A optimization.  Returns the top-$n cluster-heavier
# movers, formatted.
sub self_delta
{
	my ($nat_rep, $clr_rep, $n) = @_;
	my $nh = parse_self($nat_rep);
	my $ch = parse_self($clr_rep);
	my %all = map { $_ => 1 } (keys %$nh, keys %$ch);
	my @rows;
	for my $s (keys %all)
	{
		my $nv = $nh->{$s} // 0;
		my $cv = $ch->{$s} // 0;
		push @rows, [ $s, $cv - $nv, $cv, $nv ];
	}
	@rows = sort { $b->[1] <=> $a->[1] } @rows;
	my @out;
	for my $r (@rows)
	{
		last if @out >= $n;
		last if $r->[1] <= 0.0;	# only the cluster-heavier symbols
		push @out, sprintf("  %+6.2f%%  %-44s (cluster %5.2f%% vs native %5.2f%%)",
			$r->[1], $r->[0], $r->[2], $r->[3]);
	}
	return join("\n", @out);
}

my $perf = find_perf();

# native + cluster on the SAME installed binary; only cluster.enabled differs,
# which is what makes the perf diff symbol space comparable.
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

# Flag a polluted (cassert) build up front so the captured log is unambiguous.
my $polluted = assertions_enabled($native);
diag("write-tax CPU profile build: debug_assertions="
	. ($polluted
		? "on -- WARNING: cassert build; AllocSetCheck/MemoryContextCheck noise "
		  . "pollutes this profile.  The dedicated nightly write-tax profiling "
		  . "job builds WITHOUT --enable-cassert for a trustworthy signal."
		: "off -- clean release-like build, profile signal is trustworthy."));

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
	my $nat_data = profile_node($perf, $native, 'native', $datadir);
	my $clr_data = profile_node($perf, $clu, 'cluster', $datadir);
	if (defined $nat_data && defined $clr_data)
	{
		my $nat_rep = raw_report($perf, $nat_data);
		my $clr_rep = raw_report($perf, $clr_data);
		my $nat  = top_lines($nat_rep);
		my $clr  = top_lines($clr_rep);
		my $diff = self_delta($nat_rep, $clr_rep, 25);
		diag("===== write-tax CPU profile (-c $CLIENTS -T $SECS -N, self-sorted): "
			. "NATIVE top $TOPN symbols =====\n$nat");
		diag("===== write-tax CPU profile: CLUSTER top $TOPN symbols =====\n$clr");
		diag("===== write-tax SELF-% DELTA (cluster - native; positive = extra "
			. "cluster CPU per symbol = the lever to target) =====\n$diff");
		# Surface cluster_* delta lines explicitly for quick scanning.
		my @hot = grep { /cluster_/ } split /\n/, $diff;
		diag("cluster_* symbols in the delta (per-DML write-tax levers):\n"
			. join("\n", @hot)) if @hot;
		ok(1, "P1 write-tax CPU profile + native-vs-cluster self-delta captured"
			. ($polluted ? " (cassert-polluted -- see build warning)" : ""));
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
