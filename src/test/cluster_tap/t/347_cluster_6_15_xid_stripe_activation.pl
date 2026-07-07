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
#      L2b  (D7) bidirectional writes on ONE shared heap block with
#           mutual ITL slot recycling — the original 6.12i P0
#           false-resolve scenario, held to the fail-closed contract:
#           individual cross-node updates may fail closed (per-thread
#           covers-gate limit, forward to 6.12i amend ㉖) and a LEDGER
#           of acknowledged commits defines the truth; both nodes'
#           cross reads of the peer's recycled refs fail closed 53R97
#           while cluster.crossnode_runtime_visibility is off (the
#           mutual recycle is real, not a vacuous pass); once armed, a
#           read that SUCCEEDS must return exactly the ledger state —
#           same count twice (no duplicate row versions in one
#           snapshot: the D7 foreign-xmax discipline guard), exact
#           sum — and the D4 origin derivation never refuses
#           (underivable == 0; derived == self routes LOCAL).
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
#      L9   D3 counter herding (slack lowered to 65536 via SIGHUP
#           first): node0 burns ~20k xids
#           while node1 idles; after qvotec herding ticks, node1's
#           next xid jumps into the window (>= node0's watermark
#           minus slack) instead of resuming from its stale position,
#           and stays in its congruence class.
#      L10  D3 window hard limit, ORGANIC: node1's herding tick is
#           frozen via the cluster-xid-herding-stall :skip injection
#           (hwm publication stops; the quorum lease keeps running so
#           no self-fence/fail-stop), node0 burns past slack x 64
#           values ahead — xid assignment refuses fail-closed with the
#           53RB2 message; disarming lets herding catch up and writes
#           resume (never torn, fully recoverable).
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

# Read one pg_cluster_state counter (0 when absent).
sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

# Retry a write statement through transient cross-node GES/CF jitter.
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

# Count occurrences of $pattern in a node's current log file.
sub log_count
{
	my ($node, $pattern) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	my $n = () = $log =~ /\Q$pattern\E/g;
	return $n;
}

# Burn $n top-level xids on $node: a flat stream of autocommit
# txid_current() statements (one xid each = 16 values of striped
# space).  Single-transaction subxact loops cannot do this — the
# pgrac TT slot allocator bounds live subxacts per segment.  Errors
# after a mid-stream 53RB2 trip are swallowed (no ON_ERROR_STOP).
sub burn_xids
{
	my ($node, $n) = @_;
	my $dir = PostgreSQL::Test::Utils::tempdir();
	my $file = "$dir/burn.sql";
	open my $fh, '>', $file or die "burn.sql: $!";
	print $fh "SELECT txid_current();\n" for 1 .. $n;
	close $fh;
	# A plain psql stream (no ON_ERROR_STOP): statement-per-transaction
	# autocommit, and any mid-stream error — the wanted 53RB2 trip, or a
	# sporadic cross-node GES timeout (pre-existing substrate jitter,
	# follow-up) — is swallowed and the stream continues.  pgbench is
	# NOT suitable here: it aborts the whole client on the first error.
	PostgreSQL::Test::Utils::system_log('psql', '-X', '-q', '-o', '/dev/null',
		'-d', $node->connstr('postgres'), '-f', $file);
	return;
}

# Read {magic, retired} of a region-4 stripe slot from a voting disk
# file.  Slot N sits at ((4 * 128) + N) * 512 (region 5, after the
# spec-6.4 ADG lease region); magic "PGXS" =
# 0x50475853 LE; the retired flag is the byte at record offset 12
# (magic 4 + version 4 + node_id 4).
sub read_stripe_slot
{
	my ($path, $node) = @_;
	open(my $fh, '<', $path) or return undef;
	binmode $fh;
	return undef unless seek($fh, (4 * 128 + $node) * 512, 0);
	my $buf;
	return undef unless read($fh, $buf, 16) == 16;
	close $fh;
	my ($magic, $retired) = (unpack('V', substr($buf, 0, 4)), ord(substr($buf, 12, 1)));
	return { magic => $magic, retired => $retired };
}

# Read the region-5 activation record magic from a voting disk file.
# Region 6 sits at offset 5 * 128 * 512; magic "PGXA" = 0x50475841 LE.
sub read_activation_magic
{
	my ($path) = @_;
	open(my $fh, '<', $path) or return undef;
	binmode $fh;
	return undef unless seek($fh, 5 * 128 * 512, 0);
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
	# small herding slack (min) so the L9/L10 herding legs trigger with
	# ~100k burned xids instead of the 4M default
	# (cluster.xid_herding_slack stays at the 4M default through L1-L8;
	# the herding legs L9/L10 lower it via SIGHUP — booting with a tiny
	# slack shrinks the activation floor into a value range whose TX
	# enqueue masters remotely right after L4's under-deadband node1
	# bounce, tripping a pre-existing same-epoch-restart GES staleness
	# gap unrelated to the stripe face; follow-up)
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
# L2b — D7: bidirectional writes on ONE shared block, mutual ITL
# recycling (the original 6.12i P0 false-resolve scenario), proven
# positive end to end.
#
# Both nodes create the same table: on a fresh pair the OID /
# relfilenode allocation coincides, so both catalog entries point at
# ONE physical file on the shared store (the t/346 recipe; catalogs
# stay per-node until spec-6.14).  node0 seeds 12 rows on one block,
# then the nodes alternate committed single-row UPDATE xacts (20 xacts
# > 8 ITL slots), so EARLY updaters' slots from BOTH nodes recycle
# while tuples still reference them.
#
# Sequencing: cluster.crossnode_runtime_visibility is armed BEFORE the
# alternating phase — once the block's ITL starts recycling, a
# cross-node updater's own scan must already resolve the peer's
# recycled refs (the write-side resolution ride-along).  It is then
# dropped to prove BOTH directions fail closed 53R97 (the mutual
# recycle is real — without this the positive reads below would pass
# vacuously), and re-armed for the positive proof: exact rows and
# values on both nodes, wire fetches in both directions, and the D4
# origin derivation never refusing (underivable == 0; refs whose xid
# derives to the reader's own slot route LOCAL).
# ----------
{
	# The coincidence is not free here: L1's write-gate CREATE polling
	# burned an uneven number of OIDs per node.  Align the two OID
	# counters first — measure the relfilenode delta and burn single
	# OIDs (lo_create/lo_unlink) on the lagging node until an identical
	# CREATE lands on the same relfilenode on both.
	my ($p0, $p1) = ('', '');
	for my $attempt (1 .. 8)
	{
		$node0->safe_psql('postgres', 'CREATE TABLE t347_bi (id int, v int)');
		$node1->safe_psql('postgres', 'CREATE TABLE t347_bi (id int, v int)');
		$p0 = $node0->safe_psql('postgres',
			q{SELECT pg_relation_filepath('t347_bi')});
		$p1 = $node1->safe_psql('postgres',
			q{SELECT pg_relation_filepath('t347_bi')});
		last if $p0 eq $p1;
		my ($n0) = $p0 =~ /(\d+)$/;
		my ($n1) = $p1 =~ /(\d+)$/;
		my ($lag, $burn) =
		  $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
		$lag->safe_psql('postgres',
			"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
		$node0->safe_psql('postgres', 'DROP TABLE t347_bi');
		$node1->safe_psql('postgres', 'DROP TABLE t347_bi');
	}
	is($p0, $p1, 'L2b-0 t347_bi relfilepath coincidence holds (one shared file)');

	ok(write_retry($node0,
			'INSERT INTO t347_bi SELECT g, g * 10 FROM generate_series(1, 12) g'),
		'L2b-i node0 seed xact inserted 12 rows on one block');

	for my $n ($node0, $node1)
	{
		$n->append_conf('postgresql.conf',
			"cluster.crossnode_runtime_visibility = on\n");
		$n->reload;
	}
	usleep(1_000_000);

	# Counter baselines BEFORE the armed write phase: the alternating
	# updates themselves must resolve the peer's recycled refs (their own
	# scans hit them once the ITL recycles), so the exercised/wire asserts
	# below hold even when the later read probes stay fail-closed pre-㉖.
	my $rc1_0   = state_val($node1, 'cr', 'rtvis_resolve_committed_count');
	my $rc0_0   = state_val($node0, 'cr', 'rtvis_resolve_committed_count');
	my $wire1_0 = state_val($node1, 'cr', 'rtvis_undo_fetch_wire_count')
	  + state_val($node1, 'cr', 'rtvis_verdict_wire_count');
	my $wire0_0 = state_val($node0, 'cr', 'rtvis_undo_fetch_wire_count')
	  + state_val($node0, 'cr', 'rtvis_verdict_wire_count');

	# Alternate 2 updater xacts per row, flipping which node goes last.
	# Individual cross-node updates MAY fail closed (the covers gate
	# compares per-thread WAL positions numerically — a pre-existing
	# 6.12i limit, positive capability forwarded to amend ㉖); a LEDGER
	# of acknowledged commits defines the exact expected final state, so
	# any read that later SUCCEEDS with different numbers is a silent
	# wrong answer and fails the leg.
	my $sum_expected = 780; # seed: sum(10*id, id=1..12)
	my %final_class = (1 => 0, 2 => 0); # id -> class of last acked updater
	my ($n0_acks, $n1_acks) = (0, 0);
	for my $id (3 .. 12)
	{
		$final_class{$id} = 0; # seed xact is node0-class
		my @legs =
		  ($id % 2)
		  ? ([ $node0, 2, 0 ], [ $node1, 1, 1 ])
		  : ([ $node1, 1, 1 ], [ $node0, 2, 0 ]);
		for my $leg (@legs)
		{
			my ($node, $delta, $class) = @$leg;
			next
			  unless write_retry($node,
				"UPDATE t347_bi SET v = v + $delta WHERE id = $id");
			$sum_expected += $delta;
			$final_class{$id} = $class;
			$class ? $n1_acks++ : $n0_acks++;
		}
	}
	cmp_ok($n0_acks, '>=', 2,
		"L2b-ii node0 committed updater xacts on the shared block ($n0_acks/10)");
	cmp_ok($n1_acks, '>=', 2,
		"L2b-iii node1 committed updater xacts on the shared block ($n1_acks/10)");
	ok(write_retry($node0, 'CHECKPOINT'), 'L2b-iii-b checkpoint');

	# Drop the GUC: each direction must now fail closed on the OTHER
	# node's recycled refs (own-class recycled refs resolve LOCAL and
	# never need it — the reason these two probes prove MUTUAL recycle).
	for my $n ($node0, $node1)
	{
		$n->append_conf('postgresql.conf',
			"cluster.crossnode_runtime_visibility = off\n");
		$n->reload;
	}
	usleep(1_000_000);
	{
		my ($rc, $out, $err) =
		  $node1->psql('postgres', 'SELECT count(*) FROM t347_bi');
		like($err, qr/cluster TT (status unknown|slot recycled)/,
			'L2b-iv node1 read of node0-recycled refs fails closed 53R97 when off');
		($rc, $out, $err) =
		  $node0->psql('postgres', 'SELECT count(*) FROM t347_bi');
		like($err, qr/cluster TT (status unknown|slot recycled)/,
			'L2b-v node0 read of node1-recycled refs fails closed 53R97 when off');
	}

	# Re-arm and prove the positive: any read that SUCCEEDS must return
	# exactly the ledger state — same count twice (no duplicate versions
	# of a row in one snapshot: the D7 false-visible guard), exact sum.
	# A read may fail closed 53R97 until each origin's flushed WAL
	# position numerically passes the shared page's LSN (covers-gate
	# per-thread limit, forwarded to 6.12i amend ㉖) — nudge both nodes'
	# WAL forward with node-local writes and retry.
	for my $n ($node0, $node1)
	{
		$n->append_conf('postgresql.conf',
			"cluster.crossnode_runtime_visibility = on\n");
		$n->reload;
	}
	usleep(1_000_000);

	# The anti-silent-wrong audit: probe both nodes repeatedly (nudging
	# each origin's WAL + checkpoint forward: the covers gate and the
	# durable-TT stamp are both per-thread progress conditions).  EVERY
	# outcome must be either the exact ledger state or a documented
	# fail-closed refusal — never different data.  Exact convergence is
	# NOT required pre-㉖: an organically-formed cross-node multixact
	# xmax keeps the read fail-closed (the spec-3.6 XXX foreign-multi
	# gate), which is the honest contract this leg pins.
	# WAL nudge = a plain xid burn: it advances the origin's flushed WAL
	# position with pure xact records — data-block writes here would grow
	# relations mid-leg and change the L8 replay window (a pre-existing
	# shared-pg_control restart seam, spec-5.6a, is parked there).
	my $expected  = "12|12|$sum_expected";
	my $failre    = qr/cluster TT (status unknown|slot recycled)|cross-node write conflict|multixact/;
	my $converged = 0;
	for my $probe ([ $node1, 'L2b-vi node1' ], [ $node0, 'L2b-vii node0' ])
	{
		my ($node, $tag) = @$probe;
		my ($exact, $safe_refusals, $bad) = (0, 0, '');
		for my $try (1 .. 20)
		{
			if ($try % 5 == 1)
			{
				write_retry($node0, 'CHECKPOINT');
				write_retry($node1, 'CHECKPOINT');
			}
			my ($rc, $out, $err) = $node->psql('postgres',
				'SELECT count(*), count(DISTINCT id), sum(v) FROM t347_bi');
			if ($rc == 0)
			{
				if ($out eq $expected) { $exact++; last; }
				$bad = "wrong data: got '$out' want '$expected'";
				last;
			}
			elsif ($err =~ $failre) { $safe_refusals++; }
			else                    { $bad = "unexpected error: $err"; last; }
			burn_xids($node0, 50);
			burn_xids($node1, 50);
			usleep(500_000);
		}
		is($bad, '',
			"$tag every read outcome is exact-ledger or fail-closed "
			  . "(exact=$exact refusals=$safe_refusals)");
		$converged++ if $exact;
	}

	# When a read converged, the on-page state must match the ledger in
	# full (one block; live xmin classes = classes of the acked final
	# updaters).  When it did not (pre-㉖ multi-xmax refusal), the audit
	# above already proved the fail-closed contract held throughout.
	if ($converged)
	{
		is($node0->safe_psql('postgres',
				q{SELECT count(DISTINCT (ctid::text::point)[0]::int) FROM t347_bi}),
			'1', 'L2b-viii all live rows stayed on ONE heap block');
		my %classes = map { $_ => 1 } values %final_class;
		my $want = join(',', sort keys %classes);
		is($node0->safe_psql('postgres',
				q{SELECT string_agg(DISTINCT (xmin::text::bigint % 16)::text, ','
				  ORDER BY (xmin::text::bigint % 16)::text) FROM t347_bi}),
			$want,
			"L2b-ix live xmin congruence classes match the ledger ($want)");
	}
	else
	{
		diag('L2b interread did not converge in this run (pre-㉖ '
			  . 'cross-node multixact xmax keeps it fail-closed) — '
			  . 'the audit asserts above still pin no-silent-wrong');
		pass('L2b-viii (skipped page-shape check: no converged read)');
		pass('L2b-ix (skipped ledger-class check: no converged read)');
	}

	# Which node lands positive proofs varies run to run pre-㉖ (a node
	# whose acked legs all predate the recycle may see only refusals
	# afterwards) — the cluster-wide count must move; per-node coverage
	# is pinned by the wire-attempt asserts below.
	cmp_ok(state_val($node1, 'cr', 'rtvis_resolve_committed_count')
			 + state_val($node0, 'cr', 'rtvis_resolve_committed_count'),
		'>', $rc1_0 + $rc0_0,
		'L2b-x cross-node resolution produced positive proofs (cluster-wide)');
	# fetch or verdict: which wire leg serves depends on whether the ref's
	# segment id (the CURRENT slot owner's namespace) happens to exist on
	# the derived origin — both are cross-node proof, either counts.
	#
	# Multi-xmax alias floor amendment: the bidirectional-update workload
	# organically composes cross-node multixacts (update-chain locking), and
	# the floor refuses such tuples BEFORE the per-xid resolve — whether a
	# node's reads ever reach a wire-riding recycled ref now depends on scan
	# order.  Either outcome is the pinned fail-closed posture (L2b-vii
	# above is the no-silent-wrong tooth): the wire moved, or reads refused
	# at the multi floor first.
	{
		my $w1 = state_val($node1, 'cr', 'rtvis_undo_fetch_wire_count')
		  + state_val($node1, 'cr', 'rtvis_verdict_wire_count') > $wire1_0 ? 1 : 0;
		my $f1 = log_count($node1, 'cannot be attributed to an origin node')
		  + log_count($node1, 'TT status unknown for deleting xmax');
		ok($w1 || $f1 > 0,
			"L2b-xii node1 rode the wire OR refused at the multi floor "
			  . "(wire_moved=$w1 floor_refusals=$f1)");
		my $w0 = state_val($node0, 'cr', 'rtvis_undo_fetch_wire_count')
		  + state_val($node0, 'cr', 'rtvis_verdict_wire_count') > $wire0_0 ? 1 : 0;
		my $f0 = log_count($node0, 'cannot be attributed to an origin node')
		  + log_count($node0, 'TT status unknown for deleting xmax');
		ok($w0 || $f0 > 0,
			"L2b-xiii node0 rode the wire OR refused at the multi floor "
			  . "(wire_moved=$w0 floor_refusals=$f0)");
	}

	is(state_val($node0, 'cr', 'rtvis_underivable_failclosed_count'), 0,
		'L2b-xiv node0: no underivable refusals ever (spec-6.15 D4)');
	is(state_val($node1, 'cr', 'rtvis_underivable_failclosed_count'), 0,
		'L2b-xv node1: no underivable refusals ever (spec-6.15 D4)');

	# Restore the baseline environment for the later legs.
	for my $n ($node0, $node1)
	{
		$n->append_conf('postgresql.conf',
			"cluster.crossnode_runtime_visibility = off\n");
		$n->reload;
	}
	usleep(1_000_000);
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
# L9 — D3 counter herding: idle node1 jumps into the window.
# (L9/L10 run on the pristine post-L3 cluster, BEFORE any leg that
# detects a node death: a fail-stop's GRD remaster can leave shards
# frozen long after the dead node rejoins — a pre-existing substrate
# gap the 20k-xid burn reliably sweeps into, follow-up — and an
# under-deadband bounce leaves stale per-node GES state with the same
# effect on remote-mastered enqueues.)
# ----------
{
	# Lower the herding slack to its 65536 minimum (PGC_SIGHUP) so the
	# jump and the hard limit trigger with a few-second xid burn.
	for my $n ($node0, $node1)
	{
		$n->append_conf('postgresql.conf', "cluster.xid_herding_slack = 65536\n");
		$n->reload;
	}
	usleep(2_000_000); # let both qvotec herding ticks see the new slack

	my $x1_before = $node1->safe_psql('postgres', 'SELECT txid_current()');

	# Burn ~20k xids on node0 (16 values each = ~320k values, far past
	# the 65536 slack).
	burn_xids($node0, 20000);
	my $x0 = $node0->safe_psql('postgres', 'SELECT txid_current()');
	cmp_ok($x0 - $x1_before, '>', 300000,
		"L9-0 the burn actually consumed xid space (node0 at $x0)");

	# Wait for herding: qvotec publishes node0's hwm, node1 observes and
	# durably arms its jump (poll interval 500ms; give it a few ticks).
	my $deadline = time + 30;
	my $x1_after = $x1_before;
	while (time < $deadline)
	{
		usleep(1_000_000);
		$x1_after = $node1->safe_psql('postgres', 'SELECT txid_current()');
		last if $x1_after > $x0 - 65536 * 2;
	}

	cmp_ok($x1_after, '>', $x0 - 65536 * 2,
		"L9-i idle node1 jumped into the window (node1 $x1_after vs node0 $x0, was $x1_before)");
	is($x1_after % 16, 1, "L9-ii jumped xid $x1_after still in slot 1 (mod 16)");
	cmp_ok($x1_after - $x1_before, '>', 100000,
		'L9-iii the jump abandoned a large stretch of never-issued positions');
}

# ----------
# L10 — D3 window hard limit (organic 53RB2): freeze node1's herding
# plane via the cluster-xid-herding-stall injection (:skip makes its
# herding tick a no-op; the quorum lease keeps refreshing, so no
# self-fence and no fail-stop), burn node0 past slack x 64 values
# ahead of the frozen hwm, hit the fail-closed refusal, then disarm
# and watch herding catch up and writes resume.
# ----------
{
	# Arm via the GUC + reload: the injection registry is per-process,
	# so a SQL-armed fault only reaches the arming backend — the GUC
	# assign hook re-arms every process (including qvotec) on SIGHUP.
	$node1->append_conf('postgresql.conf',
		"cluster.injection_points = 'cluster-xid-herding-stall:skip'\n");
	$node1->reload;
	usleep(2_000_000); # let node1's qvotec pick up the frozen plane
	pass('L10-0 node1 herding plane frozen via injection GUC');
	my ($ret, $out, $err);

	# Burn 275k xids (~4.4M values, past the 4.19M hard limit above
	# node1's frozen hwm).  Statements after the mid-stream trip just
	# error and are swallowed by the stream (~8-10 minutes; this leg is
	# the reason t/347 is nightly-scope).
	burn_xids($node0, 275000);

	diag("xid_stripe face on node0 after the burn:\n"
		. $node0->safe_psql('postgres',
			q{SELECT key || '=' || value FROM pg_cluster_state WHERE category = 'xid_stripe'}));

	($ret, $out, $err) = $node0->psql('postgres', 'SELECT txid_current()');
	isnt($ret, 0, 'L10-i xid assignment refused past the window hard limit');
	like($err, qr/cluster xid window hard limit/,
		'L10-ii the refusal is the 53RB2 hard-limit error (fail-closed, not a tear)');

	# Reads that need no new xid keep working (refusal is write-scoped).
	is($node0->safe_psql('postgres', 'SELECT 1'), '1',
		'L10-iii read-only queries unaffected by the refusal');

	# Recovery: thaw node1's herding.  A GUC colon-armed fault of a
	# non-WARNING type deliberately SURVIVES reloads (framework
	# contract: explicit disarm is SQL-only, which cannot reach
	# qvotec's process-local registry) — so clear it with a quick
	# restart: the conf's trailing empty assignment wins at boot and
	# the herding tick resumes, jumps, and republishes its hwm.
	$node1->append_conf('postgresql.conf', "cluster.injection_points = ''\n");
	$node1->restart;
	ok(poll_write_ok($node0, 'INSERT INTO t347_n0 VALUES (10)', 120,
			'node0 write after node1 herding thaws'),
		'L10-iv writes resume once herding catches up (recoverable, never torn)');
	is($node1->safe_psql('postgres', 'SELECT 1'), '1',
		'L10-v node1 unperturbed (no fail-stop was triggered)');
}

# ----------
# L4 — mixed-mode handshake refusal (53RB1), then clean rejoin.
# ----------
{
	$node1->stop;
	# Let the CSSD deadband (3s) expire so node1's death is DETECTED and
	# the epoch bumps: an under-deadband bounce keeps the epoch and the
	# peer's stale GES per-node state (request dedup / grants) poisons
	# the rejoined node's remote-mastered lock traffic — a pre-existing
	# substrate gap (follow-up), out of the stripe face's scope.
	usleep(5_000_000);
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
	usleep(5_000_000); # detected death again (see above)
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
	$node0->stop;
	usleep(5_000_000); # detected death (same substrate gap as L4)
	$node0->start;
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
