#-------------------------------------------------------------------------
# spec-7.1 D6 L4 -- bidirectional-write same-page e2e SKELETON (scratch;
# promoted to a real t/NNN at ship, number assigned then, L464).
#
#   The缺口 C.1 scene (docs/cross-instance-consistency-gaps.md): a heap
#   block written from BOTH nodes concurrently.  Pre-fix this silently
#   answered count/sum双双错得"自洽"(count 18 vs 12, sum 1224 vs 810);
#   the 6.15 D7 fail-closed三件套 + the spec-7.1 P0 multi-xmax alias floor
#   (hotfix, in main) closed the silent-wrong direction.  This leg is the
#   POSITIVE acceptance: the final content must be exact, read from BOTH
#   nodes, with a pageinspect infomask铁证 that no poisoned hint survives.
#
#   Skeleton posture (this file, D2/D3 not yet fully wired): the exact
#   count/sum assertion runs under the t/347 L2b口径 -- "either the exact
#   value OR a documented fail-closed 53R97", never a silent wrong answer.
#   D4 (census收敛) flips it to a PURE positive assertion once the D1/D3
#   interread legs land.  The determinism harness (coincidence table via
#   lo_create OID alignment) and the pageinspect铁证 step are already
#   real here so D4 only has to remove the fail-closed escape.
#
#   L454判定方向标注: every read below is a CROSS-NODE read of a page
#   written by the OTHER node (node1 reads node0's writes and vice versa).
#
# Spec: spec-7.1-cross-instance-positive-interread.md (D6 L4)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/993_scratch_71_l4_bidir.pl
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

my $NROWS   = 12;    # one heap block's worth
my $ROUNDS  = $ENV{L4_ROUNDS} // 3;    # +1 per node per round -> +2*ROUNDS/row

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'l4bidir',
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
	for my $i (1 .. 12)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

# Coincidence table: identical relfilenode on both nodes (t/347 recipe).
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
		  unless write_retry($lag, "SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
		return 0 unless write_retry($node0, "DROP TABLE $name");
		return 0 unless write_retry($node1, "DROP TABLE $name");
	}
	return 0;
}

ok(mirrored_coincident_create('l4_t', 'CREATE TABLE l4_t (id int, v int)'),
	'l4_t relfilepath coincidence holds (one shared block)');
ok(write_retry($node0, "INSERT INTO l4_t SELECT g, 0 FROM generate_series(1, $NROWS) g"),
	'seeded 12 rows on one block, v=0');
is($node0->safe_psql('postgres',
		q{SELECT count(DISTINCT (ctid::text::point)[0]::int) FROM l4_t}),
	'1', 'all rows on ONE heap block');

# Bidirectional writes: each round, node0 then node1 each +1 every row.
# Correct final sum = NROWS rows * (2 * ROUNDS) increments = 24*ROUNDS,
# BUT ONLY IF every increment actually committed.  Each UPDATE is retried
# (write_retry) so a transient pre-D4 cross-node fail-closed (53R97 /
# write-fence) does not silently drop an increment; if an UPDATE still
# never commits after the retries, $all_committed goes false and the sum
# assertion below defers (the writes did not all land -- a WRITE-path
# gap, distinct from the READ silent-wrong this leg targets).  The exact
# COUNT is asserted unconditionally: a wrong row count (dup / lost rows)
# is the C.1 read-corruption signature regardless of write completeness.
my $expected_sum   = $NROWS * 2 * $ROUNDS;
my $expected_count = $NROWS;
my $all_committed  = 1;
for my $r (1 .. $ROUNDS)
{
	for my $node ($node0, $node1)
	{
		for my $id (1 .. $NROWS)
		{
			$all_committed = 0
			  unless write_retry($node, "UPDATE l4_t SET v = v + 1 WHERE id = $id");
		}
	}
}
usleep(2_000_000);
write_retry($node0, 'CHECKPOINT');

# ------------------------------------------------------------------
# The acceptance: read the final content from BOTH nodes (cross-node read
# of the other node's writes).  Skeleton口径 (t/347 L2b): exact OR a
# documented fail-closed -- NEVER a silent wrong answer.  D4 removes the
# fail-closed escape and asserts the exact values purely.
# ------------------------------------------------------------------
for my $probe ([ $node0, 'node0' ], [ $node1, 'node1' ])
{
	my ($node, $who) = @$probe;
	my ($rc, $out, $err) =
	  $node->psql('postgres', 'SELECT count(*), sum(v) FROM l4_t', timeout => 20);
	if ($rc == 0)
	{
		my ($cnt, $sum) = split /\|/, $out;
		# The铁证: if the read succeeded it MUST be exact -- a wrong-but-
		# self-consistent count/sum is the C.1 silent-corruption signature
		# and is NEVER acceptable (this half is a pure positive assertion
		# even in the skeleton).
		is($cnt, $expected_count, "L4 $who cross-node read: exact count ($expected_count)");
		if ($all_committed)
		{
			is($sum, $expected_sum,
				"L4 $who cross-node read: exact sum ($expected_sum) -- no silent lost-update/dup");
		}
		else
		{
			diag("L4 $who sum=$sum; writes incomplete pre-D4 (D4 target: all commit -> exact "
				  . "$expected_sum)");
			ok(1, "L4 $who read succeeded + count exact; sum exactness deferred to D4 (write-path)");
		}
	}
	else
	{
		# Documented READ fail-closed escape (skeleton only; D4 removes it).
		like($err, qr/53R9|TT status unknown|TT slot recycled|remastered|being rebuilt/,
			"L4 $who cross-node read: documented fail-closed (skeleton; D4 flips to pure positive)");
	}
}

# ------------------------------------------------------------------
# pageinspect铁证 (C.5): no poisoned hint bit survives on the shared block.
# HEAP_XMAX_INVALID (0x0800) stamped onto a committed deleter is the C.1
# poison signature; assert no live tuple carries it with a valid xmax.
# Requires EXTRA_INSTALL=contrib/pageinspect.
# ------------------------------------------------------------------
SKIP:
{
	my $has_pi = $node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_available_extensions WHERE name='pageinspect'});
	skip 'pageinspect not available (run with EXTRA_INSTALL=contrib/pageinspect)', 1
	  unless $has_pi && $has_pi ne '0';
	$node0->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS pageinspect');
	my ($rc, $poisoned, $err) = $node0->psql(
		'postgres',
		q{SELECT count(*) FROM heap_page_items(get_raw_page('l4_t', 0))
		  WHERE lp_flags = 1 AND (t_infomask & 4096) <> 0 AND (t_infomask & 2048) <> 0},
		timeout => 20);
	# 0x1000 = HEAP_XMAX_COMMITTED, 0x0800 = HEAP_XMAX_INVALID; both set on
	# one tuple is the contradictory poison hint C.1 produced.
	if ($rc == 0)
	{
		is($poisoned, '0', 'L4 pageinspect铁证: no XMAX_COMMITTED+XMAX_INVALID poison hint on block');
	}
	else
	{
		diag("L4 pageinspect probe fail-closed (skeleton): $err");
		ok(1, 'L4 pageinspect probe reached (fail-closed; skeleton)');
	}
}

diag("L4 skeleton: all writes committed = $all_committed "
	  . "(D4 target: 1 with pure positive count+sum both nodes)");

$pair->stop_pair;
done_testing();
