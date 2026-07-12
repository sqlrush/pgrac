# spec-6.12e2 (㉔) — master->holder BAST nudge on the live-X-holder deny.
#
# The HG7 conservative deny fires only in the THIRD-PARTY topology
# (master ∉ {requester, holder}): with two nodes the master is always one
# of the parties (path-B / local-master flows), so this is a 3-node TAP.
#
#	off leg   requester writes a block X-held by another live node whose
#	          static PCM home is the third node -> bounded fail-closed
#	          deny (today's behaviour, unchanged).
#	on leg    same topology with cluster.ges_bast=on -> the master sends
#	          the holder a fire-and-forget nudge; the holder's LMON runs
#	          the quiescent X->S self-downgrade; the requester's retry
#	          proceeds through the S-invalidate grant path.  Counters:
#	          nudge_sent on the master, nudge_yield on the holder.
#	refusal   with the holder-side injection armed the nudge is refused:
#	          the deny-retry path must stay intact (advisory contract,
#	          §3.4b: never force the holder) and nudge_refused grows.
#
# Topology probing: the tag hash spreads block homes over the three
# declared nodes; eight single-block tables make P(no third-party table)
# = (2/3)^8 ≈ 4%.  The off leg's outcome pattern LOCATES the third-party
# tables (only they fail); the on/refusal legs then drive exactly those.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterTriple;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 10)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

# ============================================================
# L1: boot the shared-data triple.
# ============================================================
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'e612_bast',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		# t/251 shape: no cluster.grd_max_entries (the GES logical-lock
		# path makes CREATE TABLE DDL time out on triples) and no custom
		# GES/GCS timeouts.
		'autovacuum = off',
	]);
$triple->start_triple;
usleep(3_000_000);

my ($node0, $node1, $node2) =
  ($triple->node(0), $triple->node(1), $triple->node(2));
for my $i (0 .. 2)
{
	is($triple->node($i)->safe_psql('postgres', 'SELECT 1'), '1',
		"L1 node$i alive");
}
# Full IC mesh before any DDL: a GES request sent before the peers are
# connected dead-letters into the 30s timeout (t/251 L0 precedent).
for my $from (0 .. 2)
{
	for my $to (0 .. 2)
	{
		next if $from == $to;
		ok($triple->wait_for_peer_state($from, $to, 'connected', 30),
			"L1 node$from sees node$to connected");
	}
}
is($node0->safe_psql('postgres', 'SHOW cluster.ges_bast'), 'on',
	'L1 cluster.ges_bast default on (round-4 FUNC-1 live-X handoff activation)');

# The L2 leg proves the DISARMED terminal-deny behaviour, so force the
# escape hatch off everywhere first; L3 re-arms it below.
for my $n ($node0, $node1, $node2)
{
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.ges_bast = off');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(1_000_000);
is($node1->safe_psql('postgres', 'SHOW cluster.ges_bast'), 'off',
	'L1b cluster.ges_bast disarmed for the off leg');

# ============================================================
# L2 (off leg): eight probe tables; node2 X-holds each (quiescent after a
# local read-back cleanout); node1 single-shot writes.  Third-party
# tables (home = node0) fail closed; the rest succeed.
# ============================================================
my (@third_party, @other);
for my $i (1 .. 8)
{
	my $t = "e_t$i";
	my $coincide = 1;
	$_->safe_psql('postgres', "CREATE TABLE $t (id int, v int)")
	  for ($node0, $node1, $node2);
	my $p0 = $node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
	for my $n ($node1, $node2)
	{
		$coincide = 0
		  if $n->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')}) ne $p0;
	}
	next unless $coincide;
	next unless write_retry($node2, "INSERT INTO $t VALUES (1, 10), (2, 20)");
	next unless write_retry($node2, 'CHECKPOINT');
	# Local read-back: lazy cleanout stamps the seed ITL so the block is
	# QUIESCENT under node2's X (a Fast-Commit unstamped slot would read
	# as ACTIVE and refuse the quiescent downgrade by design).
	$node2->safe_psql('postgres', "SELECT sum(v) FROM $t");

	my ($rc, $out, $err) = $node1->psql('postgres', "INSERT INTO $t VALUES (101, 1)");
	if ($rc != 0) { push @third_party, $t; }
	else          { push @other,       $t; }
}
cmp_ok(scalar(@third_party), '>=', 1,
	'L2 off leg: at least one third-party (master=node0) table fails closed ('
	  . scalar(@third_party) . '/8: ' . join(',', @third_party) . ')');
note('L2 non-third-party tables (succeeded): ' . join(',', @other));

# ============================================================
# L3 (on leg): arm cluster.ges_bast everywhere; the failed tables now
# converge -- nudge_sent on the master, nudge_yield on the holder.
# ============================================================
for my $n ($node0, $node1, $node2)
{
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.ges_bast = on');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(1_000_000);
is($node1->safe_psql('postgres', 'SHOW cluster.ges_bast'), 'on',
	'L3 cluster.ges_bast armed');

my $sent0  = state_val($node0, 'xnode_lever', 'e2_bast_nudge_sent_count');
my $yield0 = state_val($node2, 'xnode_lever', 'e2_bast_nudge_yield_count');

my $converged = 0;
my @on_tables = @third_party;
my $refusal_table = pop @on_tables;    # keep one for the refusal leg
for my $t (@on_tables)
{
	$converged++ if write_retry($node1, "INSERT INTO $t VALUES (102, 2)");
}
if (@on_tables)
{
	is($converged, scalar(@on_tables),
		'L3 on leg: every nudged third-party table converges ('
		  . "$converged/" . scalar(@on_tables) . ')');
	my $sent1  = state_val($node0, 'xnode_lever', 'e2_bast_nudge_sent_count');
	my $yield1 = state_val($node2, 'xnode_lever', 'e2_bast_nudge_yield_count');
	cmp_ok($sent1, '>', $sent0, 'L3 master counted nudge_sent');
	cmp_ok($yield1, '>', $yield0, 'L3 holder counted nudge_yield');
}
else
{
	note('L3 skipped: only one third-party table found; it is reserved for L4');
}

# ============================================================
# L4 (refusal leg): holder-side injection refuses the nudge -> the
# bounded deny stays (advisory contract) and nudge_refused grows.
# GUC colon-arm reaches the holder's LMON (SQL arm is per-backend);
# the armed skip survives reloads, so this leg runs LAST.
# ============================================================
{
	my $refused0 = state_val($node2, 'xnode_lever', 'e2_bast_nudge_refused_count');
	$node2->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.injection_points = 'cluster-gcs-block-bast-nudge:skip'});
	$node2->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(3_000_000);

	my ($rc, $out, $err) =
	  $node1->psql('postgres', "INSERT INTO $refusal_table VALUES (103, 3)");
	isnt($rc, 0, 'L4 refusal: single-shot write still fails closed (deny-retry intact)');
	my $refused1 = state_val($node2, 'xnode_lever', 'e2_bast_nudge_refused_count');
	cmp_ok($refused1, '>', $refused0, 'L4 holder counted nudge_refused');
}

# ============================================================
# L5: lever key surface.
# ============================================================
for my $n ($node0, $node2)
{
	my $rows = $n->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='xnode_lever' AND key IN
		     ('e2_bast_nudge_sent_count','e2_bast_nudge_yield_count',
		      'e2_bast_nudge_refused_count')});
	is($rows, '3', 'L5 wave-e2 lever keys present (' . $n->name . ')');
}

$triple->stop_triple if $triple->can('stop_triple');
done_testing();
