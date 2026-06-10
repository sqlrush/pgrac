#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 236_cluster_3_20_cr_concurrent_update_rebuild.pl
#	  spec-3.20 D2 -- CR construction robustness under concurrent multi-table
#	  UPDATE workloads.
#
#	  Root cause (D0, F8): cr_walk_chain() follows the TRANSACTION-GLOBAL
#	  prev_uba undo chain, but inverse-applies every record to the single block
#	  being reconstructed using target_offset only.  A TPC-B transaction touches
#	  pgbench_accounts (121-byte) + _tellers + _branches (44-byte) + _history in
#	  one statement, so reconstructing a tellers/branches block pulled in an
#	  accounts old image and applied it at the wrong offset:
#	    - DIFFLEN -> ERROR "cluster CR update inverse-apply failed" (fail-closed);
#	    - SAMELEN -> silent wrong-image overwrite (P0/8.A silent corruption).
#	  Fix (D3.A): cr_walk_chain skips records whose (target_locator,target_fork,
#	  target_block) != the reconstructed block, while continuing prev_uba.
#	  D3.C adds a restore identity guard so a same-length foreign occupant can
#	  never be silently overwritten.
#
#	  L1  pgbench TPC-B (multi-table) with cr_mvcc_gate=on -- the F8 reproducer.
#	      Before D3 this reliably ERROR'd "inverse-apply failed"; after, none.
#	  L9  deterministic 2-relation transaction + held repeatable-read snapshot
#	      forcing CR construct on the NARROW relation: the WIDE relation's undo
#	      records must be skipped, and the snapshot must read the correct
#	      historical narrow-row value (no cross-block corruption, no error).
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.20-cr-construction-concurrent-update-robustness.md (D2/D3)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $clients = $ENV{S320_CLIENTS} // 8;
my $jobs    = $ENV{S320_JOBS}    // 4;
my $seconds = $ENV{S320_SECONDS} // 8;

sub _new_node
{
	my ($name, $gate) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf('postgresql.conf',
		    "shared_buffers = 128MB\n"
		  . "cluster.enabled = on\n"
		  . "cluster.node_id = 0\n"
		  . "cluster.allow_single_node = on\n"
		  . "cluster.interconnect_tier = stub\n"
		  # spec-3.24 D1: pin the no-peer CR-gate fast path OFF so this CR-path
		  # test exercises the CR/SCN path (default is now ON; t/239 covers the
		  # fast path + the differential equivalence).
		  . "cluster.cr_gate_no_peer_fastpath = off\n"
		  . "cluster.cr_mvcc_gate = $gate\n"
		  . "cluster.undo_buffers = 128\n"
		  . "autovacuum = off\n");
	$node->start;
	return $node;
}

# Tolerated cluster-resource fail-closed under extreme contention (safe ERROR-
# level aborts, NOT the spec-3.20 bug -- mirrors t/234).
my $tolerated =
  qr{cluster\ CR\ cannot\ reconstruct\ block
	|cluster\ undo\ record\ alloc\ failed
	|cluster\ CR\ cannot\ resolve\ commit_scn
	|cluster\ CR\ xmax\ visibility\ unresolved
	|ITL\ slot\ overflow
	|deadlock\ detected
	|could\ not\ serialize\ access
	|canceling\ statement\ due\ to\ lock\ timeout}xi;


# ----------------------------------------------------------------------
# L1: pgbench TPC-B (multi-table) with the CR gate on -- the F8 reproducer.
# ----------------------------------------------------------------------
my $on = _new_node('s320_l1', 'on');
$on->command_ok(
	[ 'pgbench', '-i', '-s', '1', '-q', '-p', $on->port, '-h', $on->host, 'postgres' ],
	'L1 pgbench init');

my ($out, $err);
$on->run_log(
	[ 'pgbench', '-n', '-c', $clients, '-j', $jobs, '-T', $seconds,
		'-p', $on->port, '-h', $on->host, 'postgres' ],
	'>', \$out, '2>', \$err);
my $all = ($err // '') . ($out // '');

# THE spec-3.20 assertions: the F8 failure + any silent-corruption surface.
my $inverse_fail = $all =~ /cluster CR update inverse-apply failed/i;
my $invalid_rel  = $all =~ /invalid target relation/i;
my $fatal        = $all =~ /(?:FATAL|PANIC):/;
my @unexpected;
for my $line (split /\n/, $all)
{
	next if $line =~ $tolerated;
	next if $line =~ /pgbench:\s+error:\s+Run was aborted/i;
	push @unexpected, $line if $line =~ /(?:ERROR|FATAL|PANIC):|pgbench:\s+error:/i;
}
diag("L1 first 2KB stderr:\n" . substr(($err // ''), 0, 2048))
	if $inverse_fail || $fatal || @unexpected;

ok(!$inverse_fail,
	"L1 (gate on): no 'cluster CR update inverse-apply failed' [spec-3.20 F8 fix]");
ok(!$invalid_rel, "L1 (gate on): no invalid-target-relation corruption");
ok(!$fatal, "L1 (gate on): no FATAL/PANIC");
ok(!@unexpected, "L1 (gate on): no unexpected ERROR (only tolerated cluster fail-closed)");

# NB: spec-3.20 is a CR-image (read-path) correctness fix; committed heap data
# is not what this spec touches, so t/236 deliberately does NOT assert a pgbench
# end-to-end balance invariant here (that would test the write path and would be
# noisy under the tolerated fail-closed aborts).  The F8 fix is verified by the
# inverse-apply-failed assertion above + the deterministic L9 CR-read below.
$on->stop;


# ----------------------------------------------------------------------
# L9: deterministic cross-block test.  A WIDE relation (forces a different
# tuple length) and a NARROW relation updated in ONE transaction share a
# transaction-global undo chain.  A repeatable-read snapshot taken before that
# commit, then forced to reconstruct the NARROW block, must skip the wide
# relation's undo records and read the correct historical narrow value.
# ----------------------------------------------------------------------
my $n = _new_node('s320_l9', 'on');
$n->safe_psql('postgres', q{
	CREATE TABLE wide   (id int primary key, v int, filler char(120));
	CREATE TABLE narrow (id int primary key, v int);
	INSERT INTO wide   SELECT g, 0, 'x' FROM generate_series(1, 10) g;
	INSERT INTO narrow SELECT g, 100   FROM generate_series(1, 10) g;
});

my $s1 = $n->background_psql('postgres', on_error_die => 1);
$s1->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
is($s1->query_safe('SELECT v FROM narrow WHERE id = 7'), '100',
	'L9 s1 RR snapshot reads narrow.v = 100');

# Three transactions, each updating the SAME wide row + narrow row in ONE txn ->
# a transaction-global undo chain whose records span the wide block (124-byte
# tuple) and the narrow block (44-byte tuple).  Only one row per relation is
# touched, so the narrow block's 8 ITL slots are not recycled and CR
# reconstruction is deterministic (no retryable slot-reuse fail-closed).
for my $i (1 .. 3)
{
	$n->safe_psql('postgres', qq{
		BEGIN;
		UPDATE wide   SET v = v + $i, filler = repeat('y', 120) WHERE id = 7;
		UPDATE narrow SET v = v + $i WHERE id = 7;
		COMMIT;});
}

# s1's repeatable-read snapshot must still see the original narrow value via a
# CR reconstruction of the post-snapshot-modified narrow block.  Pre-D3 the walk
# pulled the wide relation's 124-byte image into the narrow block (DIFFLEN ->
# "inverse-apply failed", or a same-length cross-block silent overwrite).
is($s1->query_safe('SELECT v FROM narrow WHERE id = 7'), '100',
	'L9 s1 RR snapshot STILL sees narrow.v = 100 after 3 cross-table updates '
	. '(F8: wide-relation undo records skipped; no corruption, no error)');
$s1->query_safe('COMMIT');
$s1->quit;

# Fresh snapshot sees the committed updates: 100 + (1+2+3) = 106.
is($n->safe_psql('postgres', 'SELECT v FROM narrow WHERE id = 7'), '106',
	'L9 fresh snapshot sees committed narrow.v = 106');
$n->stop;

done_testing();
