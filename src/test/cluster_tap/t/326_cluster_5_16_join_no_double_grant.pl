# 326_cluster_5_16_join_no_double_grant.pl
#	  spec-5.16 D8 — online node-join GRD/PCM remaster, 3-node no-double-grant
#	  ship blocker.
#
#	  A survivor (node1) holds X on an advisory GES lock whose master is node2.
#	  node2 leaves the cluster online (its home shards remaster to survivors and
#	  node1's X is re-declared there), then rejoins online.  The spec-5.16 join-
#	  remaster moves node2's home-shard mastership BACK to node2 and rebuilds its
#	  GES view from the survivors' re-declarations.  The ship-blocker correctness
#	  (Rule 8.A — no split-master / no double grant):
#	    - L8 no-double-grant: after the rejoin, node1 still holds X on the
#	      node2-home key; a conflicting acquire from node0 must NOT be granted
#	      (the rebuilt view has a SINGLE X holder),
#	    - L9 continuity: once node1 releases, node0 acquires it cleanly,
#	    - L10 unaffected: a node0-home key held by node1 is undisturbed by node2's
#	      rejoin.
#
#	  Spec: spec-5.16-online-join-grd-pcm-remaster.md (D8 L8-L10 ship blockers).
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/326_cluster_5_16_join_no_double_grant.pl

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterTriple;
use Test::More;
use Time::HiRes qw(usleep);

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
	diag("poll_until timeout ($label): last='$last' expected='$expected'");
	return 0;
}

# Keys a given node currently masters (GES advisory entries with grants).
sub mastered_keys
{
	my ($node, $base, $count) = @_;
	my $out = eval {
		$node->safe_psql('postgres', qq{
			SELECT field3 FROM pg_cluster_grd_entries
			 WHERE type = 10 AND lockmethodid = 2 AND field4 = 1
			   AND field3 BETWEEN @{[$base + 1]} AND @{[$base + $count]}
			   AND ngranted > 0 ORDER BY field3});
	};
	return split(/\n/, $out // '');
}

my $KEY_BASE  = 5160000;
my $KEY_COUNT = 48;

my @conf = (
	'cluster.online_join = on',
	'cluster.join_remaster_enabled = on',
	'cluster.join_convergence_timeout_ms = 30000',
	'cluster.cssd_heartbeat_interval_ms = 500',
	'cluster.cssd_dead_deadband_factor = 6',
	'cluster.ges_request_timeout_ms = 5000',
	'cluster.grd_rebuild_timeout_ms = 15000',
	'cluster.grd_max_entries = 4096',
	'autovacuum = off',
	'jit = off',
);

my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'spec_5_16_ndg',
	quorum_voting_disks => 3,
	extra_conf          => [@conf]);
$triple->start_triple;
usleep(3_000_000);
my $node0 = $triple->node0;
my $node1 = $triple->node1;
my $node2 = $triple->node2;

# ----------
# G1 — triple up, identical maps, all members.
# ----------
for my $i (0 .. 2)
{
	is($triple->node($i)->safe_psql('postgres', 'SELECT 1'), '1', "G1 node$i alive");
	ok(poll_until($triple->node($i), 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20,
			"node$i in_quorum"),
		"G1 node$i in_quorum");
}
ok(poll_until($node0, q{SELECT count(*) = 3 FROM pg_cluster_membership WHERE state = 'member'},
		't', 30, '3 members'),
	'G1 node0 sees all 3 nodes MEMBER');

# ----------
# G2 — discover (at full 3-node) a node2-mastered key (K_home2 = the rejoiner's
#       home) and a node0-mastered key (K_un = unaffected), then RELEASE.  The
#       discovery session touches node2 but commits before node2 leaves, so it
#       is not caught by the spec-5.14 touched-abort.
# ----------
my $acquire_all = join("\n",
	map { "SELECT pg_advisory_xact_lock(" . ($KEY_BASE + $_) . ");" } (1 .. $KEY_COUNT));
my $disc = $node1->background_psql('postgres', on_error_stop => 0);
$disc->query_safe('BEGIN');
$disc->query($acquire_all);
my @home2 = mastered_keys($node2, $KEY_BASE, $KEY_COUNT);
my @home0 = mastered_keys($node0, $KEY_BASE, $KEY_COUNT);
note('G2 discovery: node2-mastered=' . scalar(@home2) . ' node0-mastered=' . scalar(@home0));
ok(scalar(@home2) >= 1, 'G2 found a node2-mastered (rejoiner-home) key');
ok(scalar(@home0) >= 1, 'G2 found a node0-mastered (unaffected) key');
BAIL_OUT('discovery premise failed') if scalar(@home2) < 1 || scalar(@home0) < 1;
my $K_home2 = $home2[0];
my $K_un    = $home0[0];
$disc->query_safe('COMMIT');
$disc->quit;

# ----------
# G3 — node2 leaves online (K_home2's home is now absent, so it reroutes to a
#       survivor).  THEN sessA (node1) takes X on K_home2 + K_un — it never
#       touches node2 (node2 is gone), so it survives the later rejoin.
# ----------
$node2->stop;
ok(poll_until($node0, q{SELECT state <> 'member' FROM pg_cluster_membership WHERE node_id = 2},
		't', 60, 'node2 not member'),
	'G3 node0 dropped node2 from the MEMBER set');

# Wait for the leave (failure) remaster to finish so K_home2's rerouted shard
# is no longer FROZEN (acquiring a mid-remaster shard fails 53R9I, retry-safe).
# Readiness probe: an autocommit acquire (released immediately) that succeeds.
ok(poll_until($node1, qq{SELECT count(*)::text FROM (SELECT pg_advisory_xact_lock($K_home2)) s},
		'1', 60, 'K_home2 acquirable after leave remaster'),
	'G3 K_home2 shard reopened on the survivor after the leave remaster');

my $sessA = $node1->background_psql('postgres', on_error_stop => 0);
$sessA->query_safe('BEGIN');
$sessA->query_safe("SELECT pg_advisory_xact_lock($K_home2)");
$sessA->query_safe("SELECT pg_advisory_xact_lock($K_un)");
is($sessA->query_safe('SELECT 1'), '1', 'G3 sessA holds X on K_home2 + K_un (2-node phase)');

# ----------
# G4 — node2 rejoins online; the spec-5.16 join-remaster moves K_home2 back to
#       node2 and rebuilds its GES view from the survivors' re-declarations
#       (sessA's X is re-declared TO node2).
# ----------
$node2->start;
ok(poll_until($node0, q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 2},
		't', 90, 'node2 CSSD alive again'),
	'G4 node0 sees node2 CSSD alive again after restart');
ok(poll_until($node0, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 2},
		't', 90, 'node2 rejoined member'),
	'G4 node0 republished node2 as MEMBER (online rejoin)');
# DIAG (fix A): does each survivor see node2 as MEMBER + run the JOIN episode?
for my $n (0, 1, 2)
{
	my $nd = $triple->node($n);
	my $ms = eval { $nd->safe_psql('postgres',
		q{SELECT state FROM pg_cluster_membership WHERE node_id = 2}) } // '<err>';
	my $js = eval { $nd->safe_psql('postgres',
		q{SELECT value FROM cluster_dump_state() WHERE category='grd_recovery' AND key='join_remaster_started'}) } // '?';
	my $jd = eval { $nd->safe_psql('postgres',
		q{SELECT value FROM cluster_dump_state() WHERE category='grd_recovery' AND key='join_remaster_done'}) } // '?';
	diag("node$n: node2_membership=$ms join_started=$js join_done=$jd");
}
# DIAG: IC link node1<->node2 — sleep, sample twice, compare send vs recv deltas.
sub ic_row { my ($nd,$peer)=@_; return eval { $nd->safe_psql('postgres',
	"SELECT state||' hb_s='||heartbeat_send_count||' hb_r='||heartbeat_recv_count"
	. "||' msg_s='||msg_send_count||' msg_r='||msg_recv_count||' rc='||reconnect_count"
	. "||' err='||coalesce(last_error,'')"
	. " FROM pg_cluster_ic_peers WHERE node_id = $peer") } // '<err>'; }
diag("IC node1->peer2: " . ic_row($node1, 2));
diag("IC node2->peer1: " . ic_row($node2, 1));
diag("IC node0->peer2: " . ic_row($node0, 2));
sleep 3;
diag("IC node1->peer2 (+3s): " . ic_row($node1, 2));
diag("IC node2->peer1 (+3s): " . ic_row($node2, 1));
# The join-remaster episode completed on a survivor (the GES view was rebuilt).
ok(poll_until($node0,
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'join_remaster_done'},
		't', 60, 'node0 join_remaster_done'),
	'G4 node0 JOIN remaster episode completed');

# Wait for K_home2 to be re-mastered back onto node2 (join rebalance) with
# sessA's X re-declared (single holder).
ok(poll_until($node2,
		qq{SELECT ngranted = 1 FROM pg_cluster_grd_entries
		    WHERE type = 10 AND lockmethodid = 2 AND field4 = 1 AND field3 = $K_home2},
		't', 60, 'sessA X rebuilt on node2'),
	'G4 K_home2 re-mastered to node2 with sessA as the single X holder');

# ----------
# G5 — L8 NO-DOUBLE-GRANT (ship blocker): node0's conflicting acquire on the
#       rebuilt K_home2 must NOT be granted while sessA still holds X.
# ----------
{
	my ($rc, $out, $err) = $node0->psql('postgres',
		"BEGIN;\nSET lock_timeout = '4s';\nSELECT pg_advisory_xact_lock($K_home2);\nCOMMIT;",
		timeout => 30);
	isnt($rc, 0,
		'G5 L8: conflicting acquire from node0 NOT granted while sessA holds X (no double grant)');
}

# ----------
# G6 — L9 continuity: sessA releases -> node0 acquires K_home2 cleanly.
# ----------
$sessA->query_safe('COMMIT');
{
	my ($rc, $out, $err) = $node0->psql('postgres',
		"BEGIN;\nSELECT pg_advisory_xact_lock($K_home2);\nCOMMIT;", timeout => 30);
	is($rc, 0, "G6 L9: node0 acquires K_home2 after sessA release (err=$err)");
}

# ----------
# G7 — L10 unaffected: a fresh acquire of K_un (node0-home, never remastered by
#       node2's rejoin) succeeds cleanly (its holder set was undisturbed; sessA
#       already released in G6).
# ----------
{
	my ($rc, $out, $err) = $node1->psql('postgres',
		"BEGIN;\nSELECT pg_advisory_xact_lock($K_un);\nCOMMIT;", timeout => 30);
	is($rc, 0, "G7 L10: node0-home unaffected key cleanly re-acquirable (err=$err)");
}

$sessA->quit;
$triple->stop_triple;
done_testing();
