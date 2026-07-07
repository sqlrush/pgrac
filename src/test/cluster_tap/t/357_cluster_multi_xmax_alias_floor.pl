#-------------------------------------------------------------------------
#
# 357_cluster_multi_xmax_alias_floor.pl
#
#   Multi-xmax alias floor + spec-7.1 D3-a origin derivation: a multixact
#   id is a node-local counter, so a foreign multi decoded against the
#   LOCAL pg_multixact aliases to unrelated members once the local
#   counter passes that id -- before the floor this read answered
#   SILENTLY WRONG (the spec-7.1 census probe-F reproduction: the
#   committed update vanished and the superseded version stayed visible).
#   D3-a derives the origin from the STRIPED mxid value:
#     - a DERIVED-OWN multi decodes natively (alias-free above the
#       activation floor) -- the creator can read its own updater multi
#       again (L4), lifting the D3-0 floor's blanket cost;
#     - a DERIVED-FOREIGN updater multi is resolved through the member
#       overlay when it HITS, else fails closed 53R9C.  The proactive V4
#       overlay never covers updater-bearing multis (the updater has no
#       TT binding at heap_update compose time -- see multixact.c
#       spec-7.1 D3-a note), so cross-node positive resolution of those
#       multis is served by the member-serve slow path = spec-7.1 D3-b
#       (software-ordered behind spec-7.2 D3).  Until D3-b, foreign
#       updater multis stay fail-closed (L2/L3) -- never silently wrong.
#
#   L1  lockers-only multi, committed  -> remote read still answers (no
#       over-blocking: lock-only never needs a member decode)
#   L2  keyshare+update multi          -> remote read fails closed 53R9C
#       (derived foreign, overlay miss; positive = D3-b member serve).
#       NEVER the silent-wrong native alias (pre-floor P0)
#   L3  probe F: the reader first advances its OWN multixact counter past
#       the foreign id -> read STILL fails closed (origin derivation is
#       immune to the local counter position; the silent-wrong P0 can no
#       longer happen by construction -- the superseded row is never
#       returned)
#   L4  creator-side read of its own updater-multi OLD version resolves
#       natively -- a DERIVED-OWN multi decodes alias-free above the
#       activation floor (was: the D3-0 floor's documented availability
#       cost; this is the D3-a positive win over the blanket floor)
#
# Spec: spec-7.1-cross-instance-positive-interread.md (D3-a)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/357_cluster_multi_xmax_alias_floor.pl
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'mxfloor',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 5',
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
	]);
$pair->start_pair;
usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'node0 sees node1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'node1 sees node0');
my ($node0, $node1) = ($pair->node0, $pair->node1);

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

# Mirrored coincident CREATE (shared-data harness discipline).
sub mirrored_coincident_create
{
	my ($name, $ddl) = @_;
	my ($q0, $q1) = ('', '');
	for my $attempt (1 .. 8)
	{
		return 0 unless write_retry($node0, $ddl);
		return 0 unless write_retry($node1, $ddl);
		$q0 = $node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$name')});
		$q1 = $node1->safe_psql('postgres', qq{SELECT pg_relation_filepath('$name')});
		return 1 if $q0 eq $q1;
		my ($n0) = $q0 =~ /(\d+)$/;
		my ($n1) = $q1 =~ /(\d+)$/;
		my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
		return 0
		  unless write_retry($lag,
			"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
		return 0 unless write_retry($node0, "DROP TABLE $name");
		return 0 unless write_retry($node1, "DROP TABLE $name");
	}
	return 0;
}

sub dump_key
{
	my ($node, $key) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value FROM cluster_dump_state() WHERE key = '$key'});
}

# The mxid activation floor must be published + latched before the test
# composes the multixacts it expects to resolve positively: a multixact
# allocated before activation is a DENSE (pre-stripe) id, which is
# forever underivable and correctly fails closed (the honest floor, not
# a positive-resolution case).  Wait for both nodes to see a nonzero
# floor so the L2/L3/L5 composes land above it (striped, derivable).
sub wait_mxid_floor
{
	for my $i (1 .. 60)
	{
		my $f0 = dump_key($node0, 'mxid_stripe_activated_floor') || '0';
		my $f1 = dump_key($node1, 'mxid_stripe_activated_floor') || '0';
		return 1 if $f0 > 0 && $f1 > 0;
		usleep(1_000_000);
	}
	return 0;
}

ok(wait_mxid_floor(), 'mxid activation floor published + latched on both nodes');

ok(mirrored_coincident_create('mxf_t', 'CREATE TABLE mxf_t (aid int, v int)'),
	'mxf_t relfilepath coincidence holds');
ok(write_retry($node0, 'INSERT INTO mxf_t SELECT g, 0 FROM generate_series(1, 20) g'), 'seeded');

sub bq
{
	my ($bg, $tag, $sql) = @_;
	my $ok = eval { $bg->query_safe($sql); 1 };
	diag("step $tag FAILED: $@") if !$ok;
	return $ok;
}

# Cross-node positive reads converge once the member overlay + member
# commit hints arrive on the wire; retry to the expected answer (the
# project's "转正腿带收敛等待" idiom).
sub read_converge
{
	my ($node, $sql, $want, $tries) = @_;
	my ($rc, $out, $err) = (1, '', '');
	for my $i (1 .. $tries)
	{
		($rc, $out, $err) = $node->psql('postgres', $sql, timeout => 15);
		return ($rc, $out, $err) if $rc == 0 && $out eq $want;
		usleep(500_000);
	}
	return ($rc, $out, $err);
}

# ------------------------------------------------------------------
# L1: lockers-only multi (share + share), committed -> remote read OK.
# ------------------------------------------------------------------
{
	my $bg1 = $node0->background_psql('postgres', timeout => 25);
	my $bg2 = $node0->background_psql('postgres', timeout => 25);
	bq($bg1, 'L1-1', 'BEGIN');
	bq($bg1, 'L1-2', 'SELECT v FROM mxf_t WHERE aid = 7 FOR SHARE');
	bq($bg2, 'L1-3', 'BEGIN');
	bq($bg2, 'L1-4', 'SELECT v FROM mxf_t WHERE aid = 7 FOR SHARE');
	bq($bg1, 'L1-5', 'COMMIT');
	bq($bg2, 'L1-6', 'COMMIT');
	eval { $bg1->quit };
	eval { $bg2->quit };

	my ($rc, $out, $err) =
	  $node1->psql('postgres', 'SELECT aid, v FROM mxf_t WHERE aid = 7', timeout => 15);
	is($rc,  0,     'L1 lockers-only multi: remote read succeeds (no over-block)');
	is($out, '7|0', 'L1 lockers-only multi: remote read answers correctly');
}

# ------------------------------------------------------------------
# L2: KEY SHARE + non-key UPDATE -> updater-bearing multi, committed.
# ------------------------------------------------------------------
{
	my $bg1 = $node0->background_psql('postgres', timeout => 25);
	my $bg2 = $node0->background_psql('postgres', timeout => 25);
	bq($bg1, 'L2-1', 'BEGIN');
	bq($bg1, 'L2-2', 'SELECT v FROM mxf_t WHERE aid = 11 FOR KEY SHARE');
	bq($bg2, 'L2-3', 'BEGIN');
	bq($bg2, 'L2-4', 'UPDATE mxf_t SET v = v + 100 WHERE aid = 11');
	bq($bg2, 'L2-5', 'COMMIT');
	bq($bg1, 'L2-6', 'COMMIT');
	eval { $bg1->quit };
	eval { $bg2->quit };

	my ($rc, $out, $err) =
	  $node1->psql('postgres', 'SELECT aid, v FROM mxf_t WHERE aid = 11', timeout => 15);
	isnt($rc, 0,
		'L2 updater multi: remote read fails closed (derived foreign; positive = D3-b member serve)');
	like(
		$err,
		qr/cannot be attributed to an origin node|multixact member overlay miss|TT status unknown for deleting xmax/,
		'L2 updater multi: clean 53R9C floor message (not a raw native multixact error)');
	unlike($err, qr/has not been created yet/,
		'L2 updater multi: native id-space error never surfaces');
}

# ------------------------------------------------------------------
# L3: probe F -- reader advances its OWN multi counter past the foreign id,
#     then reads.  Pre-floor this answered rc=0 with the SUPERSEDED row.
# ------------------------------------------------------------------
{
	ok(mirrored_coincident_create('mxf_local1', 'CREATE TABLE mxf_local1 (aid int, v int)'),
		'mxf_local1 relfilepath coincidence holds');
	ok(write_retry($node1, 'INSERT INTO mxf_local1 SELECT g, 0 FROM generate_series(1, 5) g'),
		'mxf_local1 seeded by node1');
	for my $round (1 .. 3)
	{
		my $bgx = $node1->background_psql('postgres', timeout => 25);
		my $bgy = $node1->background_psql('postgres', timeout => 25);
		bq($bgx, "L3-$round-1", 'BEGIN');
		bq($bgx, "L3-$round-2", "SELECT v FROM mxf_local1 WHERE aid = $round FOR SHARE");
		bq($bgy, "L3-$round-3", 'BEGIN');
		bq($bgy, "L3-$round-4", "SELECT v FROM mxf_local1 WHERE aid = $round FOR SHARE");
		bq($bgx, "L3-$round-5", 'COMMIT');
		bq($bgy, "L3-$round-6", 'COMMIT');
		eval { $bgx->quit };
		eval { $bgy->quit };
	}

	my ($rc, $out, $err) =
	  $node1->psql('postgres', 'SELECT aid, v FROM mxf_t WHERE aid = 11', timeout => 15);
	isnt($rc, 0,
		'L3 probe F: aliased read still fails closed (derivation immune to local counter position)');
	like(
		$err,
		qr/cannot be attributed to an origin node|multixact member overlay miss|TT status unknown for deleting xmax/,
		'L3 probe F: floor message');
	unlike($out, qr/11\|0/, 'L3 probe F: the superseded version is NEVER returned');
}

# ------------------------------------------------------------------
# L4: creator-side read of its own updater-multi OLD version resolves
#     natively -- a DERIVED-OWN multi decodes alias-free above the
#     activation floor (the D3-a positive win over the D3-0 blanket
#     floor, which fail-closed even the creator).
# ------------------------------------------------------------------
{
	my ($rc, $out, $err) =
	  read_converge($node0, 'SELECT aid, v FROM mxf_t WHERE aid = 11', '11|100', 10);
	is($rc, 0, 'L4 creator-local read of its own updater multi answers (derived own -> native)');
	is($out, '11|100', 'L4 creator sees the committed update');
}

# ------------------------------------------------------------------
# L5: revert valve -- cluster.multi_xmax_remote_resolve = off keeps the
#     derived-foreign fail-closed floor (the on-path for updater multis
#     is fail-closed too until D3-b, so L5 asserts off never REGRESSES
#     to the native silent-wrong alias, and the derived-own L4 win is
#     independent of the GUC).
# ------------------------------------------------------------------
{
	$node1->append_conf('postgresql.conf', 'cluster.multi_xmax_remote_resolve = off');
	$node1->reload;
	usleep(500_000);

	my ($rc, $out, $err) =
	  $node1->psql('postgres', 'SELECT aid, v FROM mxf_t WHERE aid = 11', timeout => 15);
	isnt($rc, 0, 'L5 resolve off: foreign updater multi read fails closed (floor verbatim)');
	unlike($out, qr/11\|0/, 'L5 resolve off: the superseded version is NEVER returned');

	$node1->append_conf('postgresql.conf', 'cluster.multi_xmax_remote_resolve = on');
	$node1->reload;
}

$pair->stop_pair;
done_testing();
