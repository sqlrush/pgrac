#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 351_cluster_6_12c_vis_memo.pl
#	  spec-6.12 wave c -- resolver terminal-outcome memo behavioural TAP
#	  on a 2-node ClusterPair (phantom-shared plain-heap table, t/334
#	  pattern: identical DDL on both nodes -> coincident relfilepath;
#	  node0 writes, node1 reads cross-node).
#
#	  L1   pair boots + quorum + phantom table coincident
#	  L2   inertness: both GUCs off -> all six xnode_lever counters stay 0
#	       across a cross-node read (off-path byte-identical surface)
#	  L3   D0 measure mode: cluster.xnode_profile=on (wave off) -> node1
#	       cross-node read ticks c_resolve/c_tt_lookup; memo counters
#	       stay 0 (measure never engages the lever)
#	  L4   L-gain: cluster.page_scn_shortcut=on -> same read pattern
#	       produces memo installs + hits, and the tt_lookup delta drops
#	       below the resolve delta (the memo absorbed repeats)
#	  L5   L-8A fail-closed: an UNCOMMITTED remote transaction's rows stay
#	       invisible under the memo (nothing non-terminal replays), and
#	       after commit a NEW transaction sees them (no stale replay)
#	  L6   dump surface: all six xnode_lever keys present on both nodes
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave c)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/351_cluster_6_12c_vis_memo.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my @LEVER_KEYS = qw(
	c_resolve_count c_tt_lookup_count c_memo_hit_count
	c_memo_install_count c_stamp_cached_seen_count
	c_stamp_contradicted_count
);

sub lever_snap
{
	my ($node) = @_;
	my %snap;
	my $rows = $node->safe_psql('postgres',
		q{SELECT key, value FROM pg_cluster_state WHERE category='xnode_lever'});
	for my $line (split /\n/, ($rows // ''))
	{
		my ($k, $v) = split /\|/, $line, 2;
		$snap{$k} = ($v // 0) + 0 if defined $k && length $k;
	}
	return \%snap;
}

sub lever_delta
{
	my ($before, $after) = @_;
	my %d;
	$d{$_} = ($after->{$_} // 0) - ($before->{$_} // 0) for @LEVER_KEYS;
	return \%d;
}

# Cross-node reads can hit transient fail-closed retries (rule-8.A
# behaviour); bounded retry, return last value or undef.
sub read_count
{
	my ($node, $sql_prefix) = @_;
	for my $i (1 .. 10)
	{
		my $v = eval { $node->safe_psql('postgres', "$sql_prefix SELECT count(*) FROM xl_read"); };
		return $v if defined $v && $v ne '';
		usleep(500_000);
	}
	return undef;
}

# ============================================================
# L1: boot + phantom-shared table.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'c612_vis_memo',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		# L355: widen heartbeat deathwatch under CI load.
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;

usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

# Phantom-shared table FIRST (before any other DDL; t/334 relfilenode
# coincidence discipline).
$node0->safe_psql('postgres', 'CREATE TABLE xl_read (id int, v int)');
$node1->safe_psql('postgres', 'CREATE TABLE xl_read (id int, v int)');
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('xl_read')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('xl_read')});
is($p0, $p1, 'L1 xl_read relfilepath coincidence holds');

# node0 seeds + checkpoints so node1's reads cross-node-coordinate.
$node0->safe_psql('postgres',
	'INSERT INTO xl_read SELECT g, g FROM generate_series(1,2000) g');
$node0->safe_psql('postgres', 'CHECKPOINT');

# ============================================================
# L2: inertness -- both GUCs off, counters stay 0.
# ============================================================
my $b2 = lever_snap($node1);
my $c2 = read_count($node1, '');
ok(defined $c2, "L2 cross-node read completed (count=" . ($c2 // 'n/a') . ")");
my $d2 = lever_delta($b2, lever_snap($node1));
is($d2->{$_}, 0, "L2 $_ stays 0 with both GUCs off") for @LEVER_KEYS;

# ============================================================
# L3: D0 measure mode -- profile on, wave off.
# ============================================================
my $b3 = lever_snap($node1);
my $c3 = read_count($node1, 'SET cluster.xnode_profile = on;');
ok(defined $c3, 'L3 measure-mode cross-node read completed');
my $d3 = lever_delta($b3, lever_snap($node1));
cmp_ok($d3->{c_resolve_count}, '>', 0,
	"L3 c_resolve_count ticks in measure mode ($d3->{c_resolve_count})");
is($d3->{c_tt_lookup_count}, $d3->{c_resolve_count},
	'L3 every resolve is a TT lookup with the memo off');
is($d3->{c_memo_hit_count}, 0, 'L3 memo never engages in measure mode');
is($d3->{c_memo_install_count}, 0, 'L3 memo installs nothing in measure mode');

# ============================================================
# L4: L-gain -- memo on, repeats absorbed.
# ============================================================
my $b4 = lever_snap($node1);
my $c4 = read_count($node1, 'SET cluster.page_scn_shortcut = on;');
ok(defined $c4, 'L4 memo-mode cross-node read completed');
my $d4 = lever_delta($b4, lever_snap($node1));
cmp_ok($d4->{c_memo_install_count}, '>', 0,
	"L4 terminal outcomes installed ($d4->{c_memo_install_count})");
cmp_ok($d4->{c_memo_hit_count}, '>', 0,
	"L4 memo hits absorbed repeat resolves ($d4->{c_memo_hit_count})");
cmp_ok($d4->{c_tt_lookup_count}, '<', $d4->{c_resolve_count},
	sprintf('L4 TT lookups (%d) < resolves (%d) with the memo on',
		$d4->{c_tt_lookup_count}, $d4->{c_resolve_count}));

# ============================================================
# L5: L-8A -- nothing non-terminal replays; no stale replay after commit.
# ============================================================
my $pre_rows = read_count($node1, 'SET cluster.page_scn_shortcut = on;');
ok(defined $pre_rows, 'L5 baseline count read');

# UNCOMMITTED writer on node0 (psql session held open via background_psql)
my $bg = $node0->background_psql('postgres');
$bg->query_safe('BEGIN');
$bg->query_safe(
	'INSERT INTO xl_read SELECT g, g FROM generate_series(10001,10100) g');

# 8.A contract: with an ACTIVE remote ITL in the scanned range the read
# either resolves the writer in-progress (rows invisible, count == pre)
# or fails closed (retryable error family; read_count returns undef).
# Both are correct; the ONLY forbidden outcome is seeing the uncommitted
# rows (false-visible).
my $mid_rows = read_count($node1, 'SET cluster.page_scn_shortcut = on;');
ok(!defined($mid_rows) || $mid_rows == $pre_rows,
	'L5 uncommitted remote rows never false-visible under the memo '
	  . '(observed: ' . ($mid_rows // 'fail-closed') . ')');

$bg->query_safe('COMMIT');
$bg->quit;

my $post_rows;
for my $i (1 .. 20)
{
	$post_rows = read_count($node1, 'SET cluster.page_scn_shortcut = on;');
	last if defined $post_rows && $post_rows == $pre_rows + 100;
	usleep(500_000);
}
is($post_rows, $pre_rows + 100,
	'L5 committed rows visible in a new transaction (no stale memo replay)');

# ============================================================
# L6: dump surface complete on both nodes.
# ============================================================
for my $n ($node0, $node1)
{
	my $snap = lever_snap($n);
	my $missing = join(',', grep { !exists $snap->{$_} } @LEVER_KEYS);
	is($missing, '', 'L6 all six xnode_lever keys present (' . $n->name . ')');
}

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
