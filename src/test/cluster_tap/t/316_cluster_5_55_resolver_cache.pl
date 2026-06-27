#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 316_cluster_5_55_resolver_cache.pl
#	  spec-5.55 -- shared resolver cache (CR Source 3 by-xid search-shortcut memo).
#
#	  The memo caches the position the last own-instance WRAP_ANY by-xid scan
#	  matched -- (xid_epoch, raw_xid, origin) -> {seg, slot, wrap} -- so a peer
#	  backend re-validates that one slot in O(1) instead of re-running the whole
#	  O(segments) scan.  A re-validated hit re-runs the PHYSICALLY SAME wrap_suspect
#	  acceptance gate the fresh scan runs (gate 3), so a hit is verdict-equivalent
#	  to the scan it replaces (规则 8.A: a wrong deleter scn would false-hide a live
#	  row).  The recycle-heavy hot-branch workload (INITRANS=8 < concurrent writers)
#	  forces committed deleters' ITL slots to recycle -> CR Source 3 by-xid scans,
#	  resolved redundantly across the 8 backends -> the memo's target leg.
#
#	  L1  TRUST mode: the balance invariant holds (NO false-visible from a memo
#	      hit) AND the memo actually fires (resolver_cache.hit > 0 -- the O(segments)
#	      scan was really skipped) on cross-backend redundant resolutions.
#	  L2  retention-off soundness: with retention unreliable (sticky), the acceptance
#	      gate (gate 3) is exercised on the memo path; the invariant STILL holds --
#	      the gate, run identically on a memo hit and a fresh scan, keeps it sound.
#	  L3  GUC off control: SAME workload, entries=0 -> byte-identical; the invariant
#	      holds and every resolver_cache counter stays 0 (the memo is truly off).
#	  L4  MEASURE mode (§0.6 value gate): the authoritative scan always runs and the
#	      hint is never trusted; the counters quantify the cross-backend redundancy
#	      (key_present / lookup) and re-probe hit rate (hit / key_present) that gate
#	      the trust path -- NO-GO is a legitimate outcome, measured here.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-5.55-shared-resolver-cache.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $clients = $ENV{S555_CLIENTS} // 8;
my $jobs    = $ENV{S555_JOBS}    // 4;
my $seconds = $ENV{S555_SECONDS} // 8;

# entries: 0 (off) or a memo big enough that direct-mapped eviction is rare.
sub _new_node
{
	my ($name, $enabled, $measure, $entries) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf('postgresql.conf',
		    "shared_buffers = 128MB\n"
		  . "cluster.enabled = on\n"
		  . "cluster.node_id = 0\n"
		  . "cluster.allow_single_node = on\n"
		  . "cluster.interconnect_tier = stub\n"
		  . "cluster.cr_gate_no_peer_fastpath = off\n"
		  . "cluster.cr_mvcc_gate = on\n"
		  . "cluster.resolver_cache_enabled = $enabled\n"
		  . "cluster.resolver_cache_measure = $measure\n"
		  . "cluster.resolver_cache_entries = $entries\n"
		  . "cluster.undo_buffers = 128\n"
		  . "autovacuum = off\n");
	$node->start;
	return $node;
}

# plpgsql TPC-B body with a ROW_COUNT assert: a false-invisible (a row the memo
# wrongly hides) surfaces as a distinct 'UPDATE0' error, so the balance invariant
# + this assert together fence soundness.
my $tpcb_fn = q{
	CREATE OR REPLACE FUNCTION tpcb_txn(p_aid int, p_bid int, p_tid int, p_delta int)
	RETURNS void LANGUAGE plpgsql AS $$
	DECLARE rc int;
	BEGIN
	  UPDATE pgbench_accounts SET abalance = abalance + p_delta WHERE aid = p_aid;
	  GET DIAGNOSTICS rc = ROW_COUNT;
	  IF rc <> 1 THEN RAISE EXCEPTION 'UPDATE0 accounts aid=% rc=%', p_aid, rc; END IF;
	  PERFORM abalance FROM pgbench_accounts WHERE aid = p_aid;
	  UPDATE pgbench_tellers SET tbalance = tbalance + p_delta WHERE tid = p_tid;
	  GET DIAGNOSTICS rc = ROW_COUNT;
	  IF rc <> 1 THEN RAISE EXCEPTION 'UPDATE0 tellers tid=% rc=%', p_tid, rc; END IF;
	  UPDATE pgbench_branches SET bbalance = bbalance + p_delta WHERE bid = p_bid;
	  GET DIAGNOSTICS rc = ROW_COUNT;
	  IF rc <> 1 THEN RAISE EXCEPTION 'UPDATE0 branches bid=% rc=%', p_bid, rc; END IF;
	  INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
	    VALUES (p_tid, p_bid, p_aid, p_delta, now());
	END $$;
};

sub _write_script
{
	my ($node) = @_;
	my $script = $node->basedir . '/s555_tpcb.sql';
	open(my $fh, '>', $script) or die $!;
	print $fh "\\set aid random(1, 100000 * :scale)\n"
		. "\\set bid random(1, 1 * :scale)\n"      # ONE hot branch row -> recycle pressure
		. "\\set tid random(1, 10 * :scale)\n"
		. "\\set delta random(-5000, 5000)\n"
		. "SELECT tpcb_txn(:aid, :bid, :tid, :delta);\n";
	close($fh);
	return $script;
}

sub _invariant
{
	my ($node) = @_;
	return $node->safe_psql('postgres', q{
		SELECT (SELECT coalesce(sum(abalance),0) FROM pgbench_accounts)
		         = (SELECT coalesce(sum(delta),0) FROM pgbench_history)
		   AND (SELECT coalesce(sum(tbalance),0) FROM pgbench_tellers)
		         = (SELECT coalesce(sum(delta),0) FROM pgbench_history)
		   AND (SELECT coalesce(sum(bbalance),0) FROM pgbench_branches)
		         = (SELECT coalesce(sum(delta),0) FROM pgbench_history);});
}

sub _rc
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='resolver_cache' AND key='$key'});
	return ($v eq '') ? 0 : $v + 0;
}

my $tolerated =
  qr{cluster\ CR\ cannot\ resolve\ commit_scn
	|cluster\ CR\ durable\ scan\ unavailable
	|cluster\ CR\ xmax\ visibility\ unresolved
	|cluster\ CR\ cannot\ reconstruct\ block
	|cluster\ undo\ record\ alloc\ failed
	|ITL\ slot\ overflow
	|deadlock\ detected
	|could\ not\ serialize\ access
	|canceling\ statement\ due\ to\ lock\ timeout}xi;

sub _run_tpcb
{
	my ($node, $secs) = @_;
	$secs //= $seconds;
	my $script = _write_script($node);
	my ($out, $err);
	$node->run_log(
		[ 'pgbench', '-n', '-f', $script, '-c', $clients, '-j', $jobs, '-T', $secs,
			'-p', $node->port, '-h', $node->host, 'postgres' ],
		'>', \$out, '2>', \$err);
	return ($err // '') . ($out // '');
}

sub _unexpected_lines
{
	my ($log) = @_;
	my @bad;
	for my $line (split /\n/, $log) {
		next unless $line =~ /(?:ERROR|FATAL|PANIC):|pgbench:\s+error:/i;
		next if $line =~ /pgbench:\s+error:\s+Run was aborted/i;
		next if $line =~ $tolerated;
		push @bad, $line;
	}
	return @bad;
}

sub _init_pgbench
{
	my ($node, $label) = @_;
	$node->command_ok(
		[ 'pgbench', '-i', '-s', '1', '-q', '-p', $node->port, '-h', $node->host, 'postgres' ],
		"$label pgbench init");
	$node->safe_psql('postgres', $tpcb_fn);
}

# ----------------------------------------------------------------------------
# L1: TRUST mode -- no false-visible AND the memo fires.
# ----------------------------------------------------------------------------
my $trust = _new_node('s555_trust', 'on', 'off', 8192);
is($trust->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name='cluster.resolver_cache_enabled'}),
	'on', 'L1a resolver_cache_enabled = on');
_init_pgbench($trust, 'L1');
my @bad_trust = _unexpected_lines(_run_tpcb($trust));
is(scalar @bad_trust, 0, 'L1 (trust): no unexpected errors (only 53R9F-class tolerated)')
  or diag("unexpected:\n" . join("\n", @bad_trust));
is(_invariant($trust), 't',
	'L1 (trust): balance invariant holds -- NO false-visible from a memo hit (规则 8.A)');
my $kp = _rc($trust, 'key_present');
my $hit = _rc($trust, 'hit');
cmp_ok($kp, '>', 0,
	"L1: cross-backend redundancy is real (key_present=$kp) -- the memo's target leg fires");
cmp_ok($hit, '>', 0,
	"L1: the memo fires -- $hit O(segments) by-xid scans skipped by an O(1) re-validated hit");
# acceptance fail-closed must never produce a false result; it is bounded and the
# invariant above proves no wrong verdict slipped through.
my $afc = _rc($trust, 'acceptance_failclosed');
diag("L1 resolver_cache: lookup=" . _rc($trust, 'lookup') . " key_present=$kp hit=$hit "
	. "revalidate_miss=" . _rc($trust, 'revalidate_miss')
	. " acceptance_pass=" . _rc($trust, 'acceptance_pass') . " acceptance_failclosed=$afc");

# ----------------------------------------------------------------------------
# L2: retention-off soundness -- the acceptance gate (gate 3) on the memo path.
# Disable retention (sticky unreliable): a below-horizon match is wrap-suspect, so
# the acceptance gate -- run IDENTICALLY on a memo hit and a fresh scan -- must
# fail closed.  The invariant STILL holds: the gate, not luck, keeps it sound.
# ----------------------------------------------------------------------------
$trust->safe_psql('postgres', 'ALTER SYSTEM SET cluster.undo_retention_horizon_enabled = off');
$trust->safe_psql('postgres', 'SELECT pg_reload_conf()');
my @bad_ret = _unexpected_lines(_run_tpcb($trust));
is(scalar @bad_ret, 0, 'L2 (retention off): no unexpected errors')
  or diag("unexpected:\n" . join("\n", @bad_ret));
is(_invariant($trust), 't',
	'L2 (retention off): invariant STILL holds -- the acceptance gate (gate 3) keeps the memo sound');
$trust->stop;

# ----------------------------------------------------------------------------
# L3: GUC off control -- entries=0 is byte-identical; counters stay 0.
# ----------------------------------------------------------------------------
my $off = _new_node('s555_off', 'off', 'off', 0);
_init_pgbench($off, 'L3');
my @bad_off = _unexpected_lines(_run_tpcb($off));
is(scalar @bad_off, 0, 'L3 (off control): only retryable contention, no hard errors')
  or diag("unexpected:\n" . join("\n", @bad_off));
is(_invariant($off), 't', 'L3 (off control): balance invariant holds (native by-xid path)');
is(_rc($off, 'lookup'), 0, 'L3 (off): resolver_cache.lookup stays 0 -- the memo is truly disabled');
is(_rc($off, 'install'), 0, 'L3 (off): resolver_cache.install stays 0');
$off->stop;

# ----------------------------------------------------------------------------
# L4: MEASURE mode (§0.6 value gate) -- never trusts; quantifies the value.
# ----------------------------------------------------------------------------
my $meas = _new_node('s555_measure', 'off', 'on', 8192);
_init_pgbench($meas, 'L4');
my @bad_meas = _unexpected_lines(_run_tpcb($meas));
is(scalar @bad_meas, 0, 'L4 (measure): no unexpected errors')
  or diag("unexpected:\n" . join("\n", @bad_meas));
is(_invariant($meas), 't',
	'L4 (measure): invariant holds -- measure mode never changes a verdict (the scan always runs)');
my $m_lookup = _rc($meas, 'lookup');
my $m_kp = _rc($meas, 'key_present');
my $m_hit = _rc($meas, 'hit');
cmp_ok($m_lookup, '>', 0, "L4: by-xid resolutions were probed (lookup=$m_lookup)");
cmp_ok($m_kp, '>', 0, "L4: cross-backend redundancy measured (key_present=$m_kp)");
# §0.6 value-gate observation (not a pass/fail bound -- NO-GO is legitimate).
my $redundancy = $m_lookup > 0 ? sprintf('%.1f%%', 100.0 * $m_kp / $m_lookup) : 'n/a';
my $hit_rate = $m_kp > 0 ? sprintf('%.1f%%', 100.0 * $m_hit / $m_kp) : 'n/a';
diag("L4 §0.6 value gate: lookup=$m_lookup key_present=$m_kp (redundancy=$redundancy) "
	. "hit=$m_hit (re-probe hit rate=$hit_rate) "
	. "acceptance_pass=" . _rc($meas, 'acceptance_pass'));
$meas->stop;

done_testing();
