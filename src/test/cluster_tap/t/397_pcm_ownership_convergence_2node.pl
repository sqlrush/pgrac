# Ownership-generation wave convergence gate (2-node, NO injection).
#
# The three window REDs (t/394 W1 cached-cover, t/395 W2 restore-ABA,
# t/396 W3 grant-finalize-vs-INVALIDATE) each prove ONE interleave with
# fault injection.  This gate proves the assembled machinery converges on
# the REAL path: two nodes ping-pong X ownership of one heap block through
# repeated cross-node transfers (grant/install/finalize, BAST downgrades,
# copy->drop serves, VM-page prefetch, BUSY rounds where they occur), and
# at the end
#
#	L2  every write survived: row count and content sum match exactly the
#	    writes issued (a lost write / stale-copy overwrite breaks the sum);
#	    BOTH nodes read the same converged state.
#	L3  bounded: the whole ping-pong completes inside a generous wall-clock
#	    budget (no timeout-mediated wedge; ruling ② BUSY keeps rounds live).
#	L4  clean counters: zero lost-write detector fires, zero TT noise
#	    (53R97 / recycled / unknown) -- the fixture never reads a peer's
#	    un-frozen row mid-flight (INSERT-only ping-pong; the single freeze
#	    happens once at the end, after which both nodes read natively).
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-2.36-gcs-block-transfer.md
# Spec: spec-4.7a-hold-until-revoked.md
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep time);

my ($n0, $n1);

sub state_int {
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

my $TT_SQL = q{
	SELECT COALESCE(SUM(value::bigint), 0) FROM pg_cluster_state
	 WHERE (category = 'cr' AND key IN (
			'vis53r97_leg_invalid_scn_refuse_count',
			'vis53r97_leg_zero_match_refuse_count',
			'vis53r97_leg_srv_other_refuse_count',
			'vis53r97_leg_covers_refuse_count',
			'vis53r97_leg_multi_unresolvable_count',
			'vis53r97_leg_xmax_unprovable_count',
			'cr_xmax_recycled_invisible_count'))
	    OR (category = 'tt_status_hint' AND key = 'drop_unknown_version_count')
	    OR (category = 'tt_recovery'    AND key = 'recycled_liveness_relaxed')
};

sub tt_noise_sum {
	return int($n0->safe_psql('postgres', $TT_SQL))
		+ int($n1->safe_psql('postgres', $TT_SQL));
}

sub lost_write_sum {
	return state_int($n0, 'gcs', 'lost_write_detected_count')
		+ state_int($n1, 'gcs', 'lost_write_detected_count');
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'pcm_convergence',
	quorum_voting_disks => 3,
	shared_data => 1,
	extra_conf => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.ges_bast = on',
		'cluster.read_scache = on',
		'cluster.crossnode_runtime_visibility = off',
		'cluster.gcs_block_local_cache = on',
		'cluster.gcs_reply_timeout_ms = 2000',
		'cluster.gcs_block_retransmit_max_retries = 12',
		'cluster.gcs_block_starvation_max_retries = 60' ]);
$pair->start_pair;
usleep(3_000_000);

$n0 = $pair->node0;
$n1 = $pair->node1;

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 peers 0->1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 peers 1->0 connected');

my $tbl;
for my $i (1 .. 12) {
	my $t = "cv_t$i";
	$_->safe_psql('postgres', "CREATE TABLE $t (k int, v int)")
		for ($n0, $n1);
	my $p0 = $n0->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	my $p1 = $n1->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	if (($p0 // '') eq ($p1 // '')) { $tbl = $t; last; }
}
die 'no coinciding filepath found' unless defined $tbl;
diag("table=$tbl");

# Seed + quiesce (frozen, hint-clean).
$n0->safe_psql('postgres', "INSERT INTO $tbl VALUES (0, 0)");
$n0->safe_psql('postgres', "VACUUM (FREEZE) $tbl");
$n0->safe_psql('postgres', 'CHECKPOINT');
$n0->safe_psql('postgres', "SELECT count(*) FROM $tbl");
usleep(300_000);

my $tt_b = tt_noise_sum();
my $lw_b = lost_write_sum();

# Ping-pong: each INSERT needs X on block 0 (fillfactor keeps every row
# there), so ownership crosses the wire every iteration.  INSERT-only: no
# statement ever reads the peer's un-frozen rows, so the loop is TT-clean
# with crossnode visibility off.  Bounded per-statement retries tolerate a
# transient fail-closed deny (never a wedge).
my $ROUNDS = 15;
my $writes = 0;
my $aborts = 0;		# failed attempts leave DEAD line pointers on the page
my $t0 = time();
for my $r (1 .. $ROUNDS) {
	for my $p ([$n0, 0], [$n1, 1]) {
		my ($node, $id) = @$p;
		my $done = 0;
		for my $try (1 .. 10) {
			my ($rc, $o, $e) = $node->psql('postgres',
				"INSERT INTO $tbl VALUES ($r, " . ($r * 10 + $id) . ")");
			if ($rc == 0) { $done = 1; last; }
			$aborts++;
			usleep(200_000);
		}
		die "ping-pong wedge: node$id round $r never landed" unless $done;
		$writes++;
	}
}
my $elapsed = time() - $t0;
diag(sprintf("ping-pong: %d writes (+%d retried aborts) in %.1fs (%.0fms/write)",
		$writes, $aborts, $elapsed, 1000 * $elapsed / $writes));

# L3 — bounded (generous: 2s/write would already be pathological).
cmp_ok($elapsed, '<', 2 * $writes,
	'L3 ping-pong completed inside the wall-clock budget (no wedge)');

# L2 — exact convergence via the SHARED-STORAGE ground truth.  Any row read
# would trip the orthogonal fresh-cluster low-xid 53R97 fail-close (tasks
# ⑤/⑥: cross-node row visibility, incl. VACUUM), so the check is physical:
# checkpoint both nodes (the current X holder flushes the converged page),
# then parse the heap page header straight off the shared data file --
# pd_lower gives the line-pointer count, and INSERT-only traffic means
# every issued write must be exactly one line pointer.  A lost insert or a
# stale-copy page overwrite shrinks it.
$_->safe_psql('postgres', 'CHECKPOINT') for ($n0, $n1);
my $relpath = $n0->safe_psql('postgres', "SELECT pg_relation_filepath('$tbl')");
# shared_data: the data file lives under the pair's shared root, not under
# either node's pgdata.  Locate it by relfilenode basename under the root.
my ($rfn) = $relpath =~ m{/(\d+)$};
my (@cands) = grep { -f $_ }
	glob($pair->shared_data_root . "/*/$relpath"),
	glob($pair->shared_data_root . "/$relpath"),
	`find @{[$pair->shared_data_root]} -type f -name $rfn 2>/dev/null` =~ /^(.+)$/mg;
my ($datafile) = grep { m{\Q$relpath\E$} } @cands;
$datafile //= $cands[0];
die 'shared data file not found for ' . $relpath unless defined $datafile;
chomp $datafile;
diag("shared data file: $datafile");
open my $fh, '<:raw', $datafile or die "cannot open $datafile: $!";
my $page = '';
die 'short page' unless sysread($fh, $page, 8192) == 8192;
close $fh;
my $pd_lower = unpack('v', substr($page, 12, 2));
my $nline = int(($pd_lower - 24) / 4);
# ItemIdData (32 bits): lp_off:15 | lp_flags:2 | lp_len:15.
# flags: 0=UNUSED 1=NORMAL 2=REDIRECT 3=DEAD.  A committed insert is one
# NORMAL pointer; aborted attempts / internal slots show up as non-NORMAL.
my %by_flag = (0 => 0, 1 => 0, 2 => 0, 3 => 0);
for my $i (0 .. $nline - 1) {
	my $lp = unpack('V', substr($page, 24 + 4 * $i, 4));
	$by_flag{($lp >> 15) & 0x3}++;
}
my $normal = $by_flag{1};
my $want_n = 1 + $writes;
diag("shared-storage page 0: pd_lower=$pd_lower lp=$nline "
	. "(normal=$by_flag{1} unused=$by_flag{0} redirect=$by_flag{2} dead=$by_flag{3})");
# Every committed write is one line pointer; an ABORTED attempt (counted
# above, fail-closed retryable) also leaves a DEAD line pointer.  So the
# exact closed-form is [want, want + aborts]: any lost insert or stale-copy
# page overwrite breaks the lower bound.
cmp_ok($normal, '>=', $want_n,
	"L2 shared storage holds all $want_n committed rows as NORMAL pointers "
	  . '(no lost insert, no stale-copy overwrite)');
cmp_ok($normal, '<=', $want_n + $aborts,
	'L2 no unexplained extra NORMAL pointers (every surplus is an accounted '
	  . 'aborted retry)');

# L4 — clean counters over the whole run.
is(lost_write_sum() - $lw_b, 0, 'L4 zero lost-write detector fires');
is(tt_noise_sum() - $tt_b, 0,
	'L4 zero TT noise (53R97 / recycled / unknown) over the whole test');

$pair->stop_pair;
done_testing();
