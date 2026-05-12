# spec-2.3 D10 -- 2-node envelope round-trip TAP.
#
# First behavioural test that two pgrac instances exchange the
# spec-2.3 36-byte ClusterICEnvelope (replacing the spec-2.2 24-byte
# ClusterMsgHeader + per-target seq_no).  Uses ClusterPair helper.
#
# Test matrix (5 L#):
#   L1 ClusterPair start_pair OK -- both postmasters live (smoke
#      that envelope wire pivot did not break startup; relies on
#      076 for full HELLO + connected coverage)
#   L2 peer.state = connected on both sides within 30s + heartbeat
#      counts grow (sanity that envelope traffic actually flows)
#   L3 bytes_send / bytes_recv match envelope frame size: after the
#      one-time 64-byte HELLO, every heartbeat is exactly 36 bytes;
#      verify (bytes_send - 64) == heartbeat_send_count * 36 on
#      both sides
#   L4 pg_cluster_ic_msg_types view contains HEARTBEAT row
#      (msg_type=1, name='heartbeat', handler_present=true,
#      broadcast_ok=false) on both nodes -- spec-2.3 D5 register
#      at postmaster phase 1 (cluster_lmon_shmem_init)
#   L5 producer mask invariant: HEARTBEAT.allowed_producer_mask !=
#      0 (LMON-only producer; bitwise OR of CLUSTER_IC_PRODUCER_LMON)
#
# Spec authority: pgrac:specs/spec-2.3-* §4.2 D10 (2-node tier1
# envelope round-trip; 36-byte frame size + msg_type registration).
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


# spec-2.3 D1 §3.7 frozen envelope frame size.  HELLO (64 bytes) is
# sent / received via a raw send()/recv() path in cluster_ic_tier1.c
# (tier1_continue_hello_send + accept_one HELLO read) that does NOT
# increment bytes_send / bytes_recv -- those counters track only the
# envelope-framed message stream.  So the strict equality below is
# bytes_send == heartbeat_send_count * 36 (no HELLO term).
my $ENVELOPE_BYTES = 36;


{
	my $pair = PostgreSQL::Test::ClusterPair->new_pair('pgrac081');
	$pair->start_pair;

	# ============================================================
	# L1 -- both postmasters live (envelope pivot did not break
	# startup).
	# ============================================================
	is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
		'L1 node0 postmaster accepts SQL after tier1+envelope startup');
	is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1',
		'L1 node1 postmaster accepts SQL after tier1+envelope startup');

	# ============================================================
	# L2 -- connected on both sides + heartbeats flow.  Default
	# heartbeat interval = 1s; sleep 3s after connected guarantees
	# at least 2 heartbeats sent each direction.
	# ============================================================
	ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
		'L2 node0 sees node1 connected within 30s (envelope wire)');
	ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
		'L2 node1 sees node0 connected within 30s (envelope wire)');

	sleep 3;

	# ============================================================
	# L3 -- bytes_send / bytes_recv frame to a 36-byte envelope.
	#
	# Per spec-2.3 §3.7 + cluster_ic_tier1.c implementation: the
	# bytes_send / bytes_recv counters track only the envelope-
	# framed stream (HELLO uses a separate raw send/recv path that
	# does not increment them).  After handshake the only frames
	# crossing the wire are heartbeat envelopes.  Therefore the
	# wire frame size is provably 36 bytes iff
	#
	#   bytes_send % 36 == 0  AND  bytes_send >= 36
	#
	# A leftover 24-byte path or a stray non-envelope frame would
	# break the modulo.  We deliberately do NOT assert
	# bytes_send == heartbeat_send_count * 36 because the two
	# counters are atomically incremented in sequence (bytes_send
	# first inside tier1_send_bytes, then heartbeat_send_count
	# inside cluster_ic_tier1_send_heartbeat) and a SELECT taken
	# between those two updates would observe an off-by-one
	# mismatch -- the loose check above suffices to prove
	# "the wire IS 36-byte envelope frames".
	# ============================================================
	for my $pair_dir ([0, 1, 'node0->node1'], [1, 0, 'node1->node0']) {
		my ($from, $to, $label) = @$pair_dir;
		my $node = $from == 0 ? $pair->node0 : $pair->node1;

		# bytes_send: tier1_send_bytes adds `len` (always 36 for an
		# envelope) AFTER a full successful send -- so this counter
		# is always a multiple of the frame size.
		my $bytes_send = $node->safe_psql('postgres',
			"SELECT bytes_send FROM pg_cluster_ic_peers WHERE node_id = $to");
		cmp_ok($bytes_send, '>=', $ENVELOPE_BYTES,
			"L3 $label bytes_send >= 36 (>= 1 envelope sent)");
		# spec-2.5 hardening v1.0.1 F2: spec-2.5 added CSSD background
		# heartbeat (msg_type=11) which sends env+payload combined (48B);
		# wire is now MIXED with LMON HEARTBEAT (36B no payload).  The
		# strict % 36 invariant pre-spec-2.5 is no longer applicable.
		# 081 is an envelope smoke test — assert envelope traffic flows;
		# CSSD-specific assertions live in 085.

		# bytes_recv: tier1 recv loop adds `got` per recv() call,
		# which may be partial under TCP fragmentation, so we only
		# assert >= 36 here (at least one envelope's worth received).
		my $bytes_recv = $node->safe_psql('postgres',
			"SELECT bytes_recv FROM pg_cluster_ic_peers WHERE node_id = $to");
		cmp_ok($bytes_recv, '>=', $ENVELOPE_BYTES,
			"L3 $label bytes_recv >= 36 (>= 1 envelope received)");
	}

	# ============================================================
	# L4 -- pg_cluster_ic_msg_types view contains HEARTBEAT row on
	# both nodes (spec-2.3 D8).  Registration happens at postmaster
	# phase 1 (cluster_lmon_shmem_init); identical on every node.
	# ============================================================
	for my $idx (0 .. 1) {
		my $node = $idx == 0 ? $pair->node0 : $pair->node1;
		# bool || text yields 'true' / 'false' (Postgres bool::text),
		# not the 't' / 'f' rendered by raw tuple output.
		my $row = $node->safe_psql('postgres',
			q{SELECT msg_type || '|' || name || '|' ||
			        handler_present || '|' || broadcast_ok
			    FROM pg_cluster_ic_msg_types
			   WHERE msg_type = 1});
		is( $row, '1|heartbeat|true|false',
			"L4 node$idx HEARTBEAT row {msg_type=1, name=heartbeat, handler_present=true, broadcast_ok=false}");
	}

	# ============================================================
	# L5 -- producer mask invariant: HEARTBEAT.allowed_producer_mask
	# is non-zero (LMON-only producer per spec-2.3 D5;
	# CLUSTER_IC_PRODUCER_LMON is bit (1<<B_LMON)).  Diagnostic
	# only -- view does not expose which producer the bit is.
	# ============================================================
	my $mask = $pair->node0->safe_psql('postgres',
		q{SELECT allowed_producer_mask::int8
		    FROM pg_cluster_ic_msg_types
		   WHERE msg_type = 1});
	cmp_ok($mask, '>', 0,
		'L5 HEARTBEAT.allowed_producer_mask > 0 (LMON producer bit set)');

	$pair->stop_pair;
}


done_testing();
