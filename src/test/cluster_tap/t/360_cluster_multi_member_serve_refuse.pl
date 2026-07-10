#-------------------------------------------------------------------------
#
# 360_cluster_multi_member_serve_refuse.pl
#
#   spec-7.1 D3-b origin-refuse negative leg (Rule 8.A).  The positive
#   member-serve path (a foreign updater multixact resolved by asking the
#   origin) turns t/357 L2/L3 positive; this test proves the FAIL-CLOSED
#   direction of the SAME path: when the origin DECLINES to serve the member
#   verdict, the requester must keep 53R9C and NEVER return the superseded
#   row (no false-visible, no native alias).
#
#   Unlike t/357 L5 (which turns the REQUESTER's ask GUC off so node1 never
#   asks), here node1 DOES ask and the ORIGIN refuses: cluster.crossnode_
#   runtime_visibility is turned off on node0 ONLY (asymmetric, the t/346 L4
#   idiom), so node0's LMS submit gate refuses the parked request and replies
#   an immediate DENIED -- exercising the requester fetch's DENIED handling
#   and the reader's UNKNOWN -> 53R9C.  (The positive counterpart is t/357.)
#
# Spec: spec-7.1-cross-instance-positive-interread.md (D3-b, §4 negative leg)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/360_cluster_multi_member_serve_refuse.pl
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'mmrefuse',
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

sub bq
{
	my ($bg, $tag, $sql) = @_;
	my $ok = eval { $bg->query_safe($sql); 1 };
	diag("step $tag FAILED: $@") if !$ok;
	return $ok;
}

ok(mirrored_coincident_create('mmr_t', 'CREATE TABLE mmr_t (aid int, v int)'),
	'mmr_t relfilepath coincidence holds');
ok(write_retry($node0, 'INSERT INTO mmr_t SELECT g, 0 FROM generate_series(1, 8) g'), 'seeded');

# Compose a committed foreign updater multixact on node0 (KEY SHARE locker +
# non-key UPDATE); this is node0's own write path, independent of any serve.
{
	my $bgl = $node0->background_psql('postgres', timeout => 25);
	my $bgu = $node0->background_psql('postgres', timeout => 25);
	bq($bgl, 'C-l-1', 'BEGIN');
	bq($bgl, 'C-l-2', 'SELECT v FROM mmr_t WHERE aid = 3 FOR KEY SHARE');
	bq($bgu, 'C-u-1', 'BEGIN');
	bq($bgu, 'C-u-2', 'UPDATE mmr_t SET v = v + 900 WHERE aid = 3');
	bq($bgu, 'C-u-3', 'COMMIT');
	bq($bgl, 'C-l-3', 'COMMIT');
	eval { $bgl->quit };
	eval { $bgu->quit };
}

# Control: node0 is the creator (derived-own) -- it decodes natively and sees
# the committed update regardless of the serve gate.  Read it BEFORE refusing
# so the read never rides the origin serve.
{
	my ($rc, $out, $err);
	for my $i (1 .. 20)
	{
		($rc, $out, $err) =
		  $node0->psql('postgres', 'SELECT aid, v FROM mmr_t WHERE aid = 3', timeout => 15);
		last if $rc == 0 && $out eq '3|900';
		usleep(500_000);
	}
	is($rc,  0,       'creator-local read answers (derived own -> native)');
	is($out, '3|900', 'creator sees the committed update');
}

# ------------------------------------------------------------------
# Turn the runtime-visibility serve gate OFF on node0 (the ORIGIN) only:
# its LMS submit gate now refuses the parked request and replies an immediate
# DENIED.  node1 (ask GUC still on) asks, is refused, and fails closed 53R9C
# -- never returning the superseded 3|0 row.
# ------------------------------------------------------------------
{
	$node0->safe_psql('postgres', 'ALTER SYSTEM SET cluster.crossnode_runtime_visibility = off');
	$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_000_000);

	my ($rc, $out, $err);
	for my $i (1 .. 20)
	{
		($rc, $out, $err) =
		  $node1->psql('postgres', 'SELECT aid, v FROM mmr_t WHERE aid = 3', timeout => 15);
		last if $rc != 0;    # origin refuse -> fail closed
		usleep(500_000);
	}
	isnt($rc, 0, 'origin serve refused: foreign updater multi read fails closed on the peer');
	# node1 fails closed on the origin refusal -- either the multi-xmax member
	# ask or the new version's xmin verdict ask (both refused with the origin's
	# GUC off).  Accept any CLEAN cluster fail-closed message; the point is it
	# is a cluster 53R97/53R9C refusal, NOT a raw native multixact id-space error.
	like(
		$err,
		qr/cluster TT slot recycled|cluster TT status unknown|multixact member overlay miss|TT status unknown for deleting xmax|cannot be attributed to an origin node/,
		'clean cluster fail-closed message (origin refuse, never a raw native multixact error)');
	unlike($err, qr/has not been created yet/, 'native id-space error never surfaces');
	unlike($out, qr/3\|0/, 'the superseded version is NEVER returned on a refuse (no false-visible)');

	$node0->safe_psql('postgres', 'ALTER SYSTEM SET cluster.crossnode_runtime_visibility = on');
	$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

$pair->stop_pair;
done_testing();
