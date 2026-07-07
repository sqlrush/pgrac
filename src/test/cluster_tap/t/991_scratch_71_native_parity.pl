# spec-7.1 L6 native-parity harness (scratch skeleton; promoted to a real
# t/NNN at ship time -- number assigned then, L464).
#
#   The ultimate anti-false-positive yardstick (spec-7.1 Q6/§3.6): one
#   deterministic seeded DML workload is applied twice --
#     leg N: plain single-node PG (cluster off),
#     leg C: 2-node pair, statements alternating node0/node1 --
#   and the FINAL table contents must be byte-equal (full row set, ordered),
#   read from BOTH cluster nodes.  Single-node PG is the MVCC gold standard;
#   any silent-wrong cluster answer diverges here even when it is
#   self-consistent (the C.1 lesson: count/sum can be wrong "consistently").
#
#   Pre-D4 posture: cluster-leg statements may hit fail-closed refusals
#   (53R97 family); those are retried with a fresh transaction, which is
#   correctness-neutral (the workload is a fixed statement sequence, and a
#   refused statement changed nothing).  A statement that cannot commit in
#   MAX_TRIES is reported and fails the run honestly -- availability is part
#   of what D1-D4 must close before this harness can gate the ship.
#
# Spec: spec-7.1-cross-instance-positive-interread.md (D6 L6)

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

my $MAX_TRIES = $ENV{PARITY_MAX_TRIES} // 20;
my $NROWS     = 24;      # two heap blocks' worth; keeps ITL churn realistic
my $SEED      = 20260707;

# ------------------------------------------------------------------
# Deterministic workload: a fixed PRNG (no libc rand -- reproducible
# across runs/platforms) emits a statement sequence over aid in [1,NROWS].
# Mix: UPDATE (60%), DELETE+reINSERT (15%), SELECT FOR SHARE (15%),
# SELECT FOR KEY SHARE + UPDATE pair (10%) -- the last two shapes force
# lock-only xmax and updater-multixact evidence onto shared pages.
# ------------------------------------------------------------------
my $prng = $SEED;

sub prand
{
	my ($n) = @_;
	$prng = ($prng * 1103515245 + 12345) % 2147483648;
	return $prng % $n;
}

sub build_workload
{
	my ($nstmts) = @_;
	my @w;
	for my $i (1 .. $nstmts)
	{
		my $aid  = 1 + prand($NROWS);
		my $dice = prand(100);
		if ($dice < 60)
		{
			push @w, ["UPDATE parity_t SET v = v + 1 WHERE aid = $aid"];
		}
		elsif ($dice < 75)
		{
			push @w,
			  [ "DELETE FROM parity_t WHERE aid = $aid",
				"INSERT INTO parity_t VALUES ($aid, 1000)" ];
		}
		elsif ($dice < 90)
		{
			push @w, ["SELECT v FROM parity_t WHERE aid = $aid FOR SHARE"];
		}
		else
		{
			push @w,
			  [ "SELECT v FROM parity_t WHERE aid = $aid FOR KEY SHARE",
				"UPDATE parity_t SET v = v + 7 WHERE aid = $aid" ];
		}
	}
	return @w;
}

my $NSTMTS   = $ENV{PARITY_NSTMTS} // 120;
my @WORKLOAD = build_workload($NSTMTS);

my $SETUP = "CREATE TABLE parity_t (aid int, v int); "
  . "INSERT INTO parity_t SELECT g, 0 FROM generate_series(1, $NROWS) g";
my $FINGERPRINT =
  q{SELECT string_agg(aid || ':' || v, ',' ORDER BY aid, v) FROM parity_t};

# Apply one workload item (a list of statements = ONE transaction) with
# fail-closed retry.  Returns 1 on commit, 0 when MAX_TRIES exhausted.
sub apply_item
{
	my ($node, $stmts) = @_;
	my $sql = join(';', 'BEGIN', @$stmts, 'COMMIT');
	for my $try (1 .. $MAX_TRIES)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(250_000);
	}
	return 0;
}

# ------------------------------------------------------------------
# Leg N: plain single-node PG (cluster paths off) -- the gold standard.
# ------------------------------------------------------------------
my $native = PostgreSQL::Test::Cluster->new('parity_native');
$native->init();
$native->start;
$native->safe_psql('postgres', $SETUP);
{
	my $failed = 0;
	for my $item (@WORKLOAD) { $failed++ unless apply_item($native, $item); }
	is($failed, 0, "leg N: all $NSTMTS workload transactions committed natively");
}
my $fp_native = $native->safe_psql('postgres', $FINGERPRINT);
$native->stop;
ok(length($fp_native) > 0, 'leg N: native fingerprint captured');

# ------------------------------------------------------------------
# Leg C: 2-node pair, statements alternating node0 / node1.
# ------------------------------------------------------------------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'c71parity',
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
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'leg C: node0 sees node1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'leg C: node1 sees node0');
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

# Mirrored DDL (coincidence harness; census recipe).
{
	my ($p0, $p1) = ('', '');
	for my $attempt (1 .. 8)
	{
		last unless write_retry($node0, 'CREATE TABLE parity_t (aid int, v int)');
		last unless write_retry($node1, 'CREATE TABLE parity_t (aid int, v int)');
		$p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('parity_t')});
		$p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('parity_t')});
		last if $p0 eq $p1;
		my ($n0) = $p0 =~ /(\d+)$/;
		my ($n1) = $p1 =~ /(\d+)$/;
		my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
		write_retry($lag, "SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
		write_retry($node0, 'DROP TABLE parity_t');
		write_retry($node1, 'DROP TABLE parity_t');
	}
	is($p0, $p1, 'leg C: parity_t relfilepath coincidence holds');
}
ok(write_retry($node0, "INSERT INTO parity_t SELECT g, 0 FROM generate_series(1, $NROWS) g"),
	'leg C: seeded');

{
	my $failed = 0;
	my $i      = 0;
	for my $item (@WORKLOAD)
	{
		my $node = ($i++ % 2 == 0) ? $node0 : $node1;
		$failed++ unless apply_item($node, $item);
	}
	is($failed, 0,
		"leg C: all $NSTMTS workload transactions committed on the pair "
		  . "(alternating nodes, fail-closed retries allowed)");
}

# Quiesce, then fingerprint from BOTH nodes with read retry (pre-D4 reads
# may fail closed transiently; a read that never succeeds fails honestly).
usleep(3_000_000);

sub fingerprint_retry
{
	my ($node) = @_;
	for my $try (1 .. $MAX_TRIES)
	{
		my $fp = eval { $node->safe_psql('postgres', $FINGERPRINT) };
		return $fp if defined $fp;
		usleep(500_000);
	}
	return undef;
}
my $fp0 = fingerprint_retry($node0);
my $fp1 = fingerprint_retry($node1);

ok(defined $fp0, 'leg C: node0 fingerprint readable');
ok(defined $fp1, 'leg C: node1 fingerprint readable');
is($fp0 // '<unreadable>', $fp_native, 'L6 PARITY: node0 content == native gold standard');
is($fp1 // '<unreadable>', $fp_native, 'L6 PARITY: node1 content == native gold standard');

$pair->stop_pair;
done_testing();
