#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 386_cluster_2_2_caps_reply_matrix.pl
#	  spec-2.2 additive amendment (spec-5.22e D5 prereq) — bidirectional
#	  peer capability exchange on a 3-node ClusterTriple.  Legs:
#
#	  M1  directed capability matrix: every ordered pair (i,j) holds a
#	      valid record for its peer — acceptor legs learn from the
#	      dialer's HELLO, dialer legs from the capability-gated
#	      PEER_CAPS_REPLY (the mesh dials low-id -> high-id, so node0
#	      never receives a HELLO from anyone; only the reply can teach
#	      it).  Every record carries the CAPS_REPLY_V1 meta bit (0x8)
#	      and the UNDO_HORIZON_V1 bit (0x4).
#	  C1  new dialer -> old acceptor: node1 restarts with the test-only
#	      suppression GUC (simulated old binary).  node0 redials and
#	      stays CONNECTED with zero reconnect churn, but its record for
#	      node1 stays UNKNOWN (v=0) — the old acceptor never replies;
#	      fail-closed, no transport impact.
#	  C2  old dialer -> new acceptor: the same suppressed node1 redials
#	      node2.  node2 learns node1's HELLO word WITHOUT the meta bit
#	      (the suppressed wire really lacks 0x8) and, gated on that,
#	      sends no reply — node1's record for node2 stays UNKNOWN while
#	      the connection stays CONNECTED and quiet.
#	  G1  generation binding: node1's restarts invalidate the peers'
#	      old records during the down window (v=0, matched-generation
#	      clear) and the re-learned records carry a strictly higher
#	      generation; the validation core rejects nothing end-to-end.
#
# Spec: spec-2.2-ic-tier1-tcp.md (additive amendment);
#       spec-5.22e-undo-cluster-retention-horizon.md (D5 prereq)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/386_cluster_2_2_caps_reply_matrix.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterTriple;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

use constant CAP_UNDO_HORIZON => 0x4;
use constant CAP_CAPS_REPLY   => 0x8;

# Parse node $i's "ic"/"peer_capabilities" dump for peer $j:
# "n<j>:bits=0x<hex>,gen=<gen>,v=<0|1>" -> (bits, gen, valid); (0, 0, 0)
# when the peer token is absent.
sub cap_rec
{
	my ($node, $peer) = @_;
	my $line = $node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state WHERE category='ic' AND key='peer_capabilities'});
	return (0, 0, 0) unless defined $line;
	if ($line =~ /\bn$peer:bits=0x([0-9A-Fa-f]+),gen=(\d+),v=([01])\b/)
	{
		return (hex($1), $2 + 0, $3 + 0);
	}
	return (0, 0, 0);
}

sub reject_count
{
	my ($node) = @_;
	my $v = $node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state WHERE category='ic' AND key='caps_reply_reject_count'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub reconnects
{
	my ($node, $peer) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT reconnect_count FROM pg_cluster_ic_peers WHERE node_id = $peer");
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub poll_ok
{
	my ($fn, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		return 1 if $fn->();
		usleep(300_000);
	}
	return 0;
}

# ============================================================
# Boot: plain 3-node transport mesh (no shared storage needed —
# capability exchange is pure tier1 substrate).
# ============================================================
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'spec_2_2_caps_matrix',
	extra_conf => [ 'autovacuum = off' ]);
$triple->start_triple;

my @nodes = ($triple->node0, $triple->node1, $triple->node2);

for my $i (0 .. 2)
{
	for my $j (0 .. 2)
	{
		next if $i == $j;
		ok($triple->wait_for_peer_state($i, $j, 'connected', 30),
			"boot node$i sees node$j connected");
	}
}

# ============================================================
# M1: directed capability matrix.
# ============================================================
for my $i (0 .. 2)
{
	for my $j (0 .. 2)
	{
		next if $i == $j;
		ok( poll_ok(
				sub {
					my ($bits, $gen, $v) = cap_rec($nodes[$i], $j);
					$v == 1
					  && ($bits & CAP_CAPS_REPLY)
					  && ($bits & CAP_UNDO_HORIZON);
				},
				30),
			"M1 node$i holds a valid capability record for node$j (meta + horizon bits)")
		  or diag("node$i dump: "
			  . $nodes[$i]->safe_psql('postgres',
				q{SELECT value FROM pg_cluster_state WHERE category='ic' AND key='peer_capabilities'}));
	}
}

my (undef, $gen_n0_for_1_before, undef) = cap_rec($nodes[0], 1);

# ============================================================
# C1 + C2: rolling-upgrade compat — node1 becomes the "old binary".
# node0 -> node1 exercises new dialer -> old acceptor; node1 -> node2
# exercises old dialer -> new acceptor.  One restart covers both
# directions because node1 is acceptor for node0 and dialer for node2.
# ============================================================
$nodes[1]->append_conf('postgresql.conf', 'cluster.ic_suppress_caps_reply = on');
$nodes[1]->restart;

ok($triple->wait_for_peer_state(0, 1, 'connected', 30), 'C1 node0 redialed old node1: connected');
ok($triple->wait_for_peer_state(1, 2, 'connected', 30), 'C2 old node1 redialed node2: connected');

# C2: node2 relearns node1's HELLO word and it really lacks the meta bit
# (poll on generation-bearing validity first: the record repopulates when
# the redial's HELLO verifies).
ok( poll_ok(
		sub {
			my ($bits, $gen, $v) = cap_rec($nodes[2], 1);
			$v == 1 && !($bits & CAP_CAPS_REPLY) && ($bits & CAP_UNDO_HORIZON);
		},
		30),
	'C2 node2 learned suppressed node1 HELLO: horizon bit present, meta bit absent')
  or diag('node2 dump: '
	  . $nodes[2]->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state WHERE category='ic' AND key='peer_capabilities'}));

# Let a few heartbeat rounds pass, then assert the fail-closed steady state.
usleep(4_000_000);

{
	my ($bits, $gen, $v) = cap_rec($nodes[0], 1);
	is($v, 0, 'C1 node0 record for old node1 stays UNKNOWN (acceptor never replied)');
}
{
	my ($bits, $gen, $v) = cap_rec($nodes[1], 2);
	is($v, 0, 'C2 old node1 record for node2 stays UNKNOWN (reply gated on the absent meta bit)');
}

# Zero reconnect churn in the steady state: sample, wait 2+ heartbeat
# rounds, sample again.
{
	my $rc01 = reconnects($nodes[0], 1);
	my $rc12 = reconnects($nodes[1], 2);
	usleep(5_000_000);
	is(reconnects($nodes[0], 1), $rc01, 'C1 no reconnect storm on the 0->1 link');
	is(reconnects($nodes[1], 2), $rc12, 'C2 no reconnect storm on the 1->2 link');
	ok($triple->wait_for_peer_state(0, 1, 'connected', 5), 'C1 0->1 still connected');
	ok($triple->wait_for_peer_state(1, 2, 'connected', 5), 'C2 1->2 still connected');
}

# ============================================================
# G1: generation binding across reconnects.
# ============================================================
$nodes[1]->append_conf('postgresql.conf', 'cluster.ic_suppress_caps_reply = off');
$nodes[1]->restart;

ok($triple->wait_for_peer_state(0, 1, 'connected', 30), 'G1 node0 redialed new node1: connected');
ok($triple->wait_for_peer_state(1, 2, 'connected', 30), 'G1 new node1 redialed node2: connected');

ok( poll_ok(
		sub {
			my ($bits, $gen, $v) = cap_rec($nodes[0], 1);
			$v == 1 && ($bits & CAP_CAPS_REPLY) && $gen > $gen_n0_for_1_before;
		},
		30),
	'G1 node0 relearned node1 via PEER_CAPS_REPLY with a strictly higher generation')
  or diag(sprintf('gen before=%d dump now: %s',
		$gen_n0_for_1_before,
		$nodes[0]->safe_psql('postgres',
			q{SELECT value FROM pg_cluster_state WHERE category='ic' AND key='peer_capabilities'})));

ok( poll_ok(
		sub {
			my ($bits, $gen, $v) = cap_rec($nodes[1], 2);
			$v == 1 && ($bits & CAP_CAPS_REPLY);
		},
		30),
	'G1 un-suppressed node1 relearned node2 via PEER_CAPS_REPLY');

ok( poll_ok(
		sub {
			my ($bits, $gen, $v) = cap_rec($nodes[2], 1);
			$v == 1 && ($bits & CAP_CAPS_REPLY);
		},
		30),
	'G1 node2 relearned node1 HELLO with the meta bit back');

# The validation core rejected nothing anywhere, end to end.
for my $i (0 .. 2)
{
	is(reject_count($nodes[$i]), 0, "G1 node$i PEER_CAPS_REPLY reject count is zero");
}

$triple->stop_triple;

done_testing();
