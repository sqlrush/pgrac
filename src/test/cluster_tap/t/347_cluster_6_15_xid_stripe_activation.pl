#-------------------------------------------------------------------------
#
# 347_cluster_6_15_xid_stripe_activation.pl
#    spec-6.15 D5b — xid stripe activation / handshake legs.
#
#    Two-node pair (strict pair + shared data + 3 voting disks, the
#    proven online-join recipe) with cluster.xid_striping = on:
#
#      L1   cold-bootstrap formation seeds the durable activation
#           record (region 5, magic "PGXA") exactly once, on the seed
#           candidate (node 0 = lowest fresh-alive declared node), and
#           both nodes open their write gates only after the record is
#           published (writes succeed on both).
#      L2   striped allocation is live: each node's next xid falls in
#           its own congruence class (node0 = 0 mod 16, node1 = 1).
#      L3   activation is idempotent across a full pair restart: the
#           record is adopted, never re-seeded (the seed-staging log
#           line appears exactly once across both boots), and writes
#           still succeed.
#      L4   stripe-mode handshake refuses a mixed-mode rejoin: node1
#           restarted with cluster.xid_striping = off is held out of
#           admission with the SQLSTATE 53RB1 refusal logged, and its
#           write gate stays closed; restoring striping = on and
#           restarting rejoins cleanly.
#
#    Writes use per-node tables (t347_n0 / t347_n1): catalogs are
#    per-node until spec-6.14 ships the shared catalog, so a table
#    created on node0 does not exist in node1's catalog.
#
#    D5c legs (slot claim / owner identity / 5.18 retired) and the D5d
#    standby leg land with their deliverables.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/347_cluster_6_15_xid_stripe_activation.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
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
use Time::HiRes qw(usleep time);

sub poll_until
{
	my ($node, $query, $expected, $timeout_s, $label) = @_;
	$expected //= 't';
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	my $last = '';
	while (time < $deadline)
	{
		$last = eval { $node->safe_psql('postgres', $query) } // '<err>';
		return 1 if defined $last && $last eq $expected;
		usleep(200_000);
	}
	diag("poll_until timeout ($label): last=$last expected=$expected");
	return 0;
}

# Poll until a write statement succeeds (write gate open) on $node.
sub poll_write_ok
{
	my ($node, $sql, $timeout_s, $label) = @_;
	$timeout_s //= 60;
	my $deadline = time + $timeout_s;
	while (time < $deadline)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 } // 0;
		return 1 if $ok;
		usleep(500_000);
	}
	diag("poll_write_ok timeout ($label)");
	return 0;
}

# Count occurrences of $pattern in a node's current log file.
sub log_count
{
	my ($node, $pattern) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	my $n = () = $log =~ /\Q$pattern\E/g;
	return $n;
}

# Read the region-5 activation record magic from a voting disk file.
# Region 5 sits at offset 4 * 128 * 512; magic "PGXA" = 0x50475841 LE.
sub read_activation_magic
{
	my ($path) = @_;
	open(my $fh, '<', $path) or return undef;
	binmode $fh;
	return undef unless seek($fh, 4 * 128 * 512, 0);
	my $buf;
	return undef unless read($fh, $buf, 4) == 4;
	close $fh;
	return unpack('V', $buf);
}

my @conf = (
	'cluster.online_join = on',
	'cluster.join_convergence_timeout_ms = 30000',
	'cluster.cssd_heartbeat_interval_ms = 500',
	'cluster.cssd_dead_deadband_factor = 6',
	'cluster.ges_request_timeout_ms = 30000',
	'cluster.xid_striping = on',
	'autovacuum = off',
	'jit = off',
);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_6_15_stripe',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [@conf]);
$pair->start_pair;
my $node0 = $pair->node0;
my $node1 = $pair->node1;

# ----------
# L1 — activation record seeded once; write gates open on both nodes.
# ----------
{
	ok(poll_write_ok($node0, 'CREATE TABLE IF NOT EXISTS t347_n0 (v int)', 90,
			'node0 write gate open'),
		'L1-i node0 forms and accepts writes (activation resolved)');
	ok(poll_write_ok($node1, 'CREATE TABLE IF NOT EXISTS t347_n1 (v int)', 90,
			'node1 write gate open'),
		'L1-ii node1 forms and accepts writes (activation resolved)');

	is(log_count($node0, 'staging activation seed')
			+ log_count($node1, 'staging activation seed'),
		1, 'L1-iii activation seed staged exactly once cluster-wide');
	cmp_ok(log_count($node0, 'activation record durable'), '==', 1,
		'L1-iv the seed candidate (node0) wrote the durable record');

	my $disks = $node0->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name = 'cluster.voting_disks'});
	my ($disk0) = split(/,/, $disks);
	is(sprintf('0x%08X', read_activation_magic($disk0) // 0), '0x50475841',
		'L1-v region-5 record carries the PGXA magic on disk');
}

# ----------
# L2 — striped allocation live: per-node congruence classes.
# ----------
{
	my $x0 = $node0->safe_psql('postgres', 'SELECT txid_current()');
	my $x1 = $node1->safe_psql('postgres', 'SELECT txid_current()');
	is($x0 % 16, 0, "L2-i node0 xid $x0 is congruent to slot 0 (mod 16)");
	is($x1 % 16, 1, "L2-ii node1 xid $x1 is congruent to slot 1 (mod 16)");
}

# ----------
# L3 — idempotent adopt across a full pair restart (no re-seed).
# ----------
{
	$pair->stop_pair;
	$pair->start_pair;

	ok(poll_write_ok($node0, 'INSERT INTO t347_n0 VALUES (3)', 90,
			'node0 write after restart'),
		'L3-i node0 reforms and accepts writes');
	ok(poll_write_ok($node1, 'INSERT INTO t347_n1 VALUES (3)', 90,
			'node1 write after restart'),
		'L3-ii node1 reforms and accepts writes');

	is(log_count($node0, 'staging activation seed')
			+ log_count($node1, 'staging activation seed'),
		1, 'L3-iii restart adopted the record — still exactly one seed ever');
}

# ----------
# L4 — mixed-mode handshake refusal (53RB1), then clean rejoin.
# ----------
{
	$node1->stop;
	$node1->append_conf('postgresql.conf', "cluster.xid_striping = off\n");
	$node1->start;

	# The refusal is logged once (either at admission finalize or at the
	# cold-bootstrap gate, depending on which proof node1 reaches).
	my $deadline = time + 60;
	my $seen = 0;
	while (time < $deadline)
	{
		if (log_count($node1, 'SQLSTATE 53RB1') > 0) { $seen = 1; last; }
		usleep(500_000);
	}
	ok($seen, 'L4-i mixed-mode rejoin refused with SQLSTATE 53RB1 logged');

	my $write_ok =
	  eval { $node1->safe_psql('postgres', 'INSERT INTO t347_n1 VALUES (4)'); 1 } // 0;
	is($write_ok, 0, 'L4-ii held node1 write gate stays closed (fail-closed)');

	$node1->stop;
	$node1->append_conf('postgresql.conf', "cluster.xid_striping = on\n");
	$node1->start;
	ok(poll_write_ok($node1, 'INSERT INTO t347_n1 VALUES (5)', 90,
			'node1 rejoin after fixing striping'),
		'L4-iii restoring cluster.xid_striping = on rejoins cleanly');

	my $x1 = $node1->safe_psql('postgres', 'SELECT txid_current()');
	is($x1 % 16, 1, "L4-iv rejoined node1 xid $x1 back in slot 1 (mod 16)");
}

$pair->stop_pair;
done_testing();
