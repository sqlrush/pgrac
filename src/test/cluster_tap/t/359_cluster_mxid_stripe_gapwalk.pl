#-------------------------------------------------------------------------
#
# 359_cluster_mxid_stripe_gapwalk.pl
#
#   spec-7.1 D3-a mxid stripe substrate legs:
#
#   G0  activation extension: both nodes publish a nonzero
#       mxid_stripe_activated_floor dump key (the "PGXM" record rode the
#       activation slot and latched).
#   G1  class-0 offsets page-boundary crossing: node0 (stripe slot 0)
#       burns enough lock-only multixacts to stride across an offsets
#       SLRU page boundary (2048 entries/page, divisible by 16).  The
#       burn itself is the trigger: without the gap-walk the crossing
#       dies on the missing page at RecordNewMultiXact.
#   G2  non-0-class crossing: same burn on node1 (stripe slot 1).  This
#       leg is INDEPENDENT of G1 by design (IN-8-approve): a page-first
#       id is always congruence class 0, so a naive "extend the
#       candidate" fix passes G1 and still dies here.
#   G3  restart durability: node1 restarts after the crossing and can
#       both re-trim its offsets SLRU (TrimMultiXact reads the current
#       page) and compose a fresh multixact on the striped chain.
#   G4  half-space hard limit (53RB4): the armed injection point forces
#       the refusal branch (the organic trigger needs 2^31 multixacts);
#       the guardrail counter becomes visible in the dump and creation
#       recovers after disarm.
#   G5  overlay-miss negative leg: with the origin's overlay emit
#       suppressed (member cap below the composed member count), the
#       remote read of an updater multi fails closed 53R9C -- positive
#       resolution never guesses on a miss.
#
#   The positive-resolution legs (foreign updater multi resolves through
#   the member overlay) live in t/357, whose floor legs turned positive
#   with this deliverable.
#
# Spec: spec-7.1-cross-instance-positive-interread.md (D3-a)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/359_cluster_mxid_stripe_gapwalk.pl
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'mxgapwalk',
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

sub bq
{
	my ($bg, $tag, $sql) = @_;
	my $ok = eval { $bg->query_safe($sql); 1 };
	diag("step $tag FAILED: $@") if !$ok;
	return $ok;
}

# One lock-only multixact per round on $node against $table (two
# concurrent FOR SHARE holders compose share+share).  Two persistent
# background sessions; a fresh pair every 64 rounds keeps the psql
# pipes young.
sub burn_multis
{
	my ($node, $table, $rounds, $tag) = @_;
	my $done = 0;
	while ($done < $rounds)
	{
		my $chunk = $rounds - $done > 64 ? 64 : $rounds - $done;
		my $bga   = $node->background_psql('postgres', timeout => 60);
		my $bgb   = $node->background_psql('postgres', timeout => 60);
		for my $i (1 .. $chunk)
		{
			my $aid = ($done + $i) % 20 + 1;
			return $done unless bq($bga, "$tag-$done-$i-1", 'BEGIN');
			return $done
			  unless bq($bga, "$tag-$done-$i-2",
				"SELECT v FROM $table WHERE aid = $aid FOR SHARE");
			return $done unless bq($bgb, "$tag-$done-$i-3", 'BEGIN');
			return $done
			  unless bq($bgb, "$tag-$done-$i-4",
				"SELECT v FROM $table WHERE aid = $aid FOR SHARE");
			return $done unless bq($bga, "$tag-$done-$i-5", 'COMMIT');
			return $done unless bq($bgb, "$tag-$done-$i-6", 'COMMIT');
		}
		eval { $bga->quit };
		eval { $bgb->quit };
		$done += $chunk;
	}
	return $done;
}

ok(mirrored_coincident_create('mxgw_t', 'CREATE TABLE mxgw_t (aid int, v int)'),
	'mxgw_t relfilepath coincidence holds');
ok(write_retry($node0, 'INSERT INTO mxgw_t SELECT g, 0 FROM generate_series(1, 20) g'),
	'seeded');

# ------------------------------------------------------------------
# G0: the mxid activation extension latched on both nodes.
# ------------------------------------------------------------------
{
	my $floor0 = dump_key($node0, 'mxid_stripe_activated_floor');
	my $floor1 = dump_key($node1, 'mxid_stripe_activated_floor');
	cmp_ok($floor0, '>', 0, 'G0 node0 published a nonzero mxid activation floor');
	is($floor1, $floor0, 'G0 node1 adopted the SAME mxid activation floor');
}

# ------------------------------------------------------------------
# G1: class-0 node strides across an offsets page boundary.  160 rounds
# x stride 16 = a 2560-id span from the formation-time floor (< 2048),
# guaranteed to cross at least one 2048-entry page boundary.  Without
# the gap-walk the crossing dies inside the burn (missing offsets page
# at RecordNewMultiXact).
# ------------------------------------------------------------------
is(burn_multis($node0, 'mxgw_t', 160, 'G1'),
	160, 'G1 class-0 node crossed an offsets page boundary alive (160 multis)');

# ------------------------------------------------------------------
# G2: non-0-class node does the same, INDEPENDENTLY (a page-first id is
# never class 1, so this leg catches a class-0-only fix).
# ------------------------------------------------------------------
is(burn_multis($node1, 'mxgw_t', 160, 'G2'),
	160, 'G2 class-1 node crossed an offsets page boundary alive (160 multis)');

# ------------------------------------------------------------------
# G3: restart durability -- TrimMultiXact re-reads the current offsets
# page and the striped chain continues.
# ------------------------------------------------------------------
{
	$node1->restart;
	ok($pair->wait_for_peer_state(0, 1, 'connected', 60), 'G3 node1 rejoined after restart');
	ok($pair->wait_for_peer_state(1, 0, 'connected', 60), 'G3 node1 sees node0 again');
	is(burn_multis($node1, 'mxgw_t', 4, 'G3'),
		4, 'G3 striped multixact chain continues after restart');
}

# ------------------------------------------------------------------
# G4: half-space hard limit leg -- the armed injection forces the 53RB4
# refusal; the guardrail counter shows it; disarm recovers.
# ------------------------------------------------------------------
{
	# Bare name arms the WARNING kind: enough for the armed-state peek
	# (any non-NONE type forces the branch) AND the only kind the GUC
	# disarm sweep releases -- a ':error' arm would survive the reload.
	$node0->append_conf('postgresql.conf',
		"cluster.injection_points = 'cluster-mxid-halfspace-hard-limit'");
	$node0->reload;
	usleep(500_000);

	my $bga = $node0->background_psql('postgres', timeout => 25);
	bq($bga, 'G4-1', 'BEGIN');
	bq($bga, 'G4-2', 'SELECT v FROM mxgw_t WHERE aid = 3 FOR SHARE');
	# Second locker in a plain psql so the server ERROR text is captured.
	my ($rc, $out, $err) = $node0->psql(
		'postgres',
		"BEGIN;\nSELECT v FROM mxgw_t WHERE aid = 3 FOR SHARE;\nCOMMIT;",
		timeout => 15);
	eval { $bga->quit };
	isnt($rc, 0, 'G4 second locker (multixact compose) refused under the armed half-space limit');
	like(
		$err,
		qr/cluster mxid half-space limit reached/,
		'G4 refusal is the 53RB4 half-space message');

	cmp_ok(dump_key($node0, 'mxid_stripe_halfspace_refusals'),
		'>', 0, 'G4 guardrail counter recorded the refusal');

	$node0->append_conf('postgresql.conf', "cluster.injection_points = ''");
	$node0->reload;
	usleep(500_000);
	is(burn_multis($node0, 'mxgw_t', 2, 'G4r'),
		2, 'G4 multixact creation recovers after disarm');
}

# ------------------------------------------------------------------
# G5: overlay-miss negative leg.  Cap the origin's overlay member limit
# below the composed member count so no overlay entry is installed or
# emitted; the remote read of the updater multi must fail closed 53R9C
# (positive resolution never guesses on a miss -- rule 8.A).
# ------------------------------------------------------------------
{
	$node0->append_conf('postgresql.conf', 'cluster.multixact_member_overlay_max_members = 4');
	$node0->reload;
	usleep(500_000);

	# Fresh table: the burn table's page ITLs (INITRANS = 8) are too
	# contended for a 5-way concurrent compose.
	ok(mirrored_coincident_create('mxgw_g5', 'CREATE TABLE mxgw_g5 (aid int, v int)'),
		'G5 mxgw_g5 relfilepath coincidence holds');
	ok(write_retry($node0, 'INSERT INTO mxgw_g5 VALUES (17, 0)'), 'G5 seeded');

	# 4 KEY SHARE lockers + 1 non-key updater = 5 members > 4.
	my @lockers = map { $node0->background_psql('postgres', timeout => 25) } (1 .. 4);
	my $bgu     = $node0->background_psql('postgres', timeout => 25);
	my $li      = 0;
	for my $bg (@lockers)
	{
		$li++;
		bq($bg, "G5-l$li-1", 'BEGIN');
		bq($bg, "G5-l$li-2", 'SELECT v FROM mxgw_g5 WHERE aid = 17 FOR KEY SHARE');
	}
	bq($bgu, 'G5-u-1', 'BEGIN');
	bq($bgu, 'G5-u-2', 'UPDATE mxgw_g5 SET v = v + 500 WHERE aid = 17');
	bq($bgu, 'G5-u-3', 'COMMIT');
	for my $bg (@lockers)
	{
		bq($bg, 'G5-lc', 'COMMIT');
		eval { $bg->quit };
	}
	eval { $bgu->quit };

	my ($rc, $out, $err) =
	  $node1->psql('postgres', 'SELECT aid, v FROM mxgw_g5 WHERE aid = 17', timeout => 15);
	isnt($rc, 0, 'G5 remote read of the unemitted updater multi fails closed');
	like(
		$err,
		qr/multixact member overlay miss|TT status unknown for deleting xmax/,
		'G5 clean overlay-miss fail-closed message');
	unlike($out, qr/17\|0/, 'G5 the superseded version is NEVER returned');

	$node0->append_conf('postgresql.conf', 'cluster.multixact_member_overlay_max_members = 32');
	$node0->reload;
}

$pair->stop_pair;
done_testing();
