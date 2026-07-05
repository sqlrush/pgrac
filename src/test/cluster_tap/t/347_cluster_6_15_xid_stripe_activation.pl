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
#      L5   D5c slot claim: both nodes' region-4 stripe slots carry
#           the PGXS magic durably on disk after admission.
#      L6   D5c retire-before-removal: node1 clean-leaves (5.13), node0
#           permanently removes it (5.18) — the removal commits only
#           after node1's stripe slot is durably retired (retired flag
#           byte set on disk, retire logged on the coordinator).
#      L7   the removed node's fresh boot stays fail-closed: no
#           writable transaction ever succeeds (membership 53R64 and
#           the retired stripe slot 53RB1 both bar the way).
#
#      L8   (runs before L6/L7) D5d standby skip-fill: a streaming standby of node0 (fully
#           isolated cluster plane: own pgrac.conf, own IC port, own
#           empty voting disks) learns the activation floor + active
#           slot set ONLY from replayed JOIN records (checkpoint
#           re-emission covers the backup window), sees committed
#           primary data, and its snapshot carries no x16 phantom
#           in-progress xids for the striped allocation gaps.
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

# Read {magic, retired} of a region-4 stripe slot from a voting disk
# file.  Slot N sits at ((3 * 128) + N) * 512; magic "PGXS" =
# 0x50475853 LE; the retired flag is the byte at record offset 12
# (magic 4 + version 4 + node_id 4).
sub read_stripe_slot
{
	my ($path, $node) = @_;
	open(my $fh, '<', $path) or return undef;
	binmode $fh;
	return undef unless seek($fh, (3 * 128 + $node) * 512, 0);
	my $buf;
	return undef unless read($fh, $buf, 16) == 16;
	close $fh;
	my ($magic, $retired) = (unpack('V', substr($buf, 0, 4)), ord(substr($buf, 12, 1)));
	return { magic => $magic, retired => $retired };
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
	'cluster.clean_leave_enabled = on',
	'cluster.online_node_removal = on',
	'cluster.quorum_poll_interval_ms = 500',
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

# ----------
# L5 — D5c: both region-4 stripe slots durably claimed (PGXS on disk).
# ----------
my $disk0;
{
	my $disks = $node0->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name = 'cluster.voting_disks'});
	($disk0) = split(/,/, $disks);
	for my $n (0, 1)
	{
		my $slot = read_stripe_slot($disk0, $n);
		is(sprintf('0x%08X', $slot->{magic} // 0), '0x50475853',
			"L5-i slot $n carries the PGXS magic on disk");
		is($slot->{retired}, 0, "L5-ii slot $n is not retired");
	}
}

# ----------
# L8 — D5d: standby learns the stripe state from WAL and skip-fills.
# ----------
{
	# Arm streaming on node0 only now (wal_level = replica from the start
	# perturbs the L6 leave+removal timing — a pre-existing interaction
	# unrelated to the stripe face; follow-up).  This leg runs BEFORE the
	# removal (L6): node0's restart rejoins via the live node1 coordinator
	# (the t/315 pattern); a lone-survivor restart with online_join = on
	# after the peer's removal has no coordinator left to admit it — a
	# pre-existing 5.15/5.18 interaction, follow-up.  initdb's default
	# pg_hba already trusts local replication connections.
	$node0->append_conf('postgresql.conf',
		"wal_level = replica\nmax_wal_senders = 4\nmax_replication_slots = 4\n");
	$node0->restart;
	ok(poll_write_ok($node0, 'INSERT INTO t347_n0 VALUES (8)', 90,
			'node0 write after streaming restart'),
		'L8-0 node0 rejoins writable with streaming armed');

	$node0->safe_psql('postgres',
		q{SELECT pg_create_physical_replication_slot('s347')});
	$node0->backup('b347');

	my $standby = PostgreSQL::Test::Cluster->new('stripe_standby');
	$standby->init_from_backup($node0, 'b347', has_streaming => 1);
	$standby->append_conf('postgresql.conf', "primary_slot_name = 's347'\n");

	# Fully isolate the standby's cluster plane: its own single-node
	# pgrac.conf on a fresh IC port (never fights node0's identity) and
	# its own empty voting disks (the stripe replay face must learn the
	# activation ONLY from WAL — never from disks or local config).
	my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
	my $ddir = $standby->data_dir;
	open(my $fh, '>', "$ddir/pgrac.conf") or die "pgrac.conf: $!";
	print $fh "[cluster]\nname = spec_6_15_stripe\n\n[node.0]\n"
	  . "interconnect_addr = 127.0.0.1:$ic_port\n";
	close $fh;
	my $sdisk_dir = PostgreSQL::Test::Utils::tempdir();
	my @sdisks;
	for my $i (0 .. 2)
	{
		my $path = "$sdisk_dir/sdisk$i";
		open(my $dh, '>', $path) or die "$path: $!";
		binmode $dh;
		print $dh ("\0" x (2 * 128 * 512));
		close $dh;
		push @sdisks, $path;
	}
	$standby->append_conf('postgresql.conf',
		"cluster.voting_disks = '" . join(',', @sdisks) . "'\n");
	$standby->start;

	# an open write txn (real in-progress xid) + a committed one after it,
	# leaving a striped allocation gap between their xids.
	my $bg = $node0->background_psql('postgres');
	$bg->query_safe('BEGIN');
	$bg->query_safe('INSERT INTO t347_n0 VALUES (81)');
	$node0->safe_psql('postgres', 'INSERT INTO t347_n0 VALUES (82)');
	my $marker_rows = $node0->safe_psql('postgres',
		'SELECT count(*) FROM t347_n0 WHERE v = 82');

	$node0->wait_for_catchup($standby);

	cmp_ok(log_count($standby, 'replay learned activation'), '>=', 1,
		'L8-i standby learned the activation floor from replayed JOIN records');

	is($standby->safe_psql('postgres', 'SELECT count(*) FROM t347_n0 WHERE v = 82'),
		$marker_rows, 'L8-ii committed primary rows visible on the standby');

	# snapshot phantom check: the striped gaps between node0's xids must
	# NOT be filled as in-progress foreign-class xids.  With skip-fill the
	# standby snapshot holds just the open txn (allow a little noise);
	# the vanilla dense fill would hold ~16 per allocation gap.
	my $snap = $standby->safe_psql('postgres', 'SELECT txid_current_snapshot()');
	my (undef, undef, $xip) = split(/:/, $snap);
	my $xip_count = defined($xip) && length($xip) ? scalar(split(/,/, $xip)) : 0;
	cmp_ok($xip_count, '<=', 3,
		"L8-iii standby snapshot has no x16 phantom fill (xip count $xip_count, snapshot $snap)");

	# 8.A pin: the still-open primary txn's rows must NOT be visible on
	# the standby — skip-fill may only drop classes that can never have
	# been issued, never a real in-progress xid.
	is($standby->safe_psql('postgres', 'SELECT count(*) FROM t347_n0 WHERE v = 81'),
		'0', 'L8-iii-b open txn rows are invisible on the standby (8.A)');

	$bg->query_safe('COMMIT');
	$bg->quit;
	$node0->wait_for_catchup($standby);
	is($standby->safe_psql('postgres', 'SELECT count(*) FROM t347_n0 WHERE v = 81'),
		'1', 'L8-iv the open txn becomes visible on the standby after commit');

	$standby->stop;
}

# ----------
# L6 — D5c: retire-before-removal (clean-leave then permanent removal).
# ----------
{
	my $leave = $node1->safe_psql('postgres', 'SELECT pg_cluster_clean_leave_request()');
	is($leave, 'accepted', 'L6-i node1 clean-leave accepted');
	ok(poll_until($node1,
			q{SELECT phase = 'committed' FROM pg_cluster_clean_leave_state},
			't', 40, 'node1 clean-leave committed'),
		'L6-ii node1 clean-leave commits (dormant member)');

	my $req = $node0->safe_psql('postgres', 'SELECT pg_cluster_remove_node(1)');
	is($req, 'accepted', 'L6-iii node0 pg_cluster_remove_node(1) accepted');
	ok(poll_until($node0,
			q{SELECT phase = 'committed' FROM pg_cluster_node_removal_state},
			't', 60, 'removal committed'),
		'L6-iv removal commits');

	cmp_ok(log_count($node0, 'slot 1 retired'), '>=', 1,
		'L6-v coordinator logged the durable stripe-slot retire');
	my $slot = read_stripe_slot($disk0, 1);
	is($slot->{retired}, 1, 'L6-vi region-4 slot 1 retired flag durable on disk');
	is(sprintf('0x%08X', $slot->{magic} // 0), '0x50475853',
		'L6-vii retired record still carries the PGXS magic (owner preserved)');
}

# ----------
# L7 — the removed node's fresh boot stays fail-closed (never writable).
# ----------
{
	$node1->stop;
	$node1->start;
	sleep 8; # give the joiner gate several LMON ticks to (wrongly) open

	my $write_ok =
	  eval { $node1->safe_psql('postgres', 'INSERT INTO t347_n1 VALUES (7)'); 1 } // 0;
	is($write_ok, 0, 'L7 removed node1 stays fail-closed after a fresh boot');
	$node1->stop;
}

$pair->stop_pair;
done_testing();
