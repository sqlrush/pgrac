#!/usr/bin/perl
#-------------------------------------------------------------------------
#
# 277_ges_convert.pl
#    spec-5.1b — GES grant/convert state machine acceptance.
#
#    Live single-node checks of the spec-5.1b LIVE behaviour, plus a source
#    contract for the cross-node convert reject (which has no live producer
#    in 5.1b and is deliberately NOT faked, L341):
#
#      L1   matrix-switch is LIVE (spec-5.1b D1): the frozen matrix that now
#           drives the GRD grant decision agrees with PG's live conflict
#           table, and the node started cleanly -- the spec-5.1b D7
#           unconditional + always-FATAL self-check passed (a divergence
#           would have refused startup).  Ordinary DML still round-trips.
#      L2   spec-5.1b D7: the cluster.ges_mode_selfcheck GUC is gone (off/warn
#           had no safe meaning once the matrix drives grants).
#      L3   spec-5.1b D9: the 3 convert verdict counters are exposed in the
#           grd dump category and are 0 at a fresh start (no convert ran).
#      L4   spec-5.1b D3: an inbound cross-node opcode-2 CONVERT is an explicit
#           FEATURE_NOT_SUPPORTED reject through the reply ring (NOT a silent
#           drop, NOT an ereport on the LMS/receiving thread), and the
#           requester maps the reason to ERRCODE_FEATURE_NOT_SUPPORTED (0A000).
#
#    NOT reachable single-node (honest forward, mirrors 4.9/4.11/4.12b):
#      PG is an additive lock model with no native lock conversion, so there
#      is no live backend convert trigger in 5.1b -- the real trigger +
#      convert wire/identity model are co-designed with the first real
#      consumer (spec-5.2 TX row-lock upgrade).  The convert state machine
#      LOGIC (partial-order classification, convert-priority drain, anti-
#      starvation, self-exclusion, double-grant guard, queue-full) is fully
#      covered by the cluster_unit suite (test_cluster_grd U1-U11).  The
#      cross-node convert GRANT e2e is deliberately not faked here (rule 8 /
#      rule 18 / L341); L4 verifies the reject MECHANISM is wired, not stubbed.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/277_ges_convert.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Spec: spec-5.1b-ges-grant-convert-state-machine.md
#
#-------------------------------------------------------------------------
use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;


# ----------------------------------------------------------------------
# L1 -- matrix-switch is LIVE (D1) + node started cleanly (D7 self-check).
# ----------------------------------------------------------------------
is($node->safe_psql('postgres', q{SELECT cluster_ges_mode_matches_pg()}),
	't', 'L1: live frozen matrix == PG conflict table (now drives GRD grant, D1)');

$node->safe_psql('postgres',
	q{CREATE TABLE t277_conv (a int); INSERT INTO t277_conv VALUES (1), (2)});
is($node->safe_psql('postgres', q{SELECT count(*) FROM t277_conv}),
	'2', 'L1b: ordinary locking/DML still round-trips under the matrix-switch grant path');


# ----------------------------------------------------------------------
# L2 -- spec-5.1b D7: cluster.ges_mode_selfcheck GUC removed.
# ----------------------------------------------------------------------
my ($r2, undef, $e2) = $node->psql('postgres', q{SHOW cluster.ges_mode_selfcheck});
isnt($r2, 0, 'L2: cluster.ges_mode_selfcheck GUC removed (D7)');
like($e2, qr/unrecognized configuration parameter/,
	'L2: SHOW of the removed GUC reports an unrecognized parameter');


# ----------------------------------------------------------------------
# L3 -- spec-5.1b D9: convert verdict counters exposed + 0 at fresh start.
# ----------------------------------------------------------------------
is( $node->safe_psql('postgres', q{
		SELECT count(*) FROM pg_cluster_state
		WHERE category = 'grd'
		  AND key IN ('grd_convert_granted_inplace_count',
		              'grd_convert_enqueued_count',
		              'grd_convert_illegal_count')}),
	'3', 'L3: 3 convert verdict counters present in the grd dump (D9)');

is( $node->safe_psql('postgres', q{
		SELECT bool_and(value = '0') FROM pg_cluster_state
		WHERE category = 'grd' AND key LIKE 'grd_convert_%'}),
	't', 'L3: convert verdict counters are 0 at a fresh start (no convert ran)');


# ----------------------------------------------------------------------
# L4 -- spec-5.3 AMEND: the inbound opcode-2 CONVERT handler is now LIVE (the
# 5.1b FEATURE_NOT_SUPPORTED placeholder was replaced by the real convert
# decision when spec-5.3 activated the TM table-lock upgrade consumer).  The
# real cross-node convert GRANT / conflict / 53R74 e2e lives in
# t/283_tm_table_lock_convert.pl;  here we keep the source contract that the
# handler drives the live state machine and fail-closes with 53R74 (NOT a
# silent drop, NOT an ereport on the LMS/receiving thread -- L341).
# ----------------------------------------------------------------------
my $root = "$FindBin::RealBin/../../../..";

sub slurp_src
{
	my ($rel) = @_;
	my $path = "$root/$rel";
	open my $fh, '<', $path or die "open $path: $!";
	local $/;
	my $s = <$fh>;
	close $fh;
	return $s;
}

my $ges_c = slurp_src('src/backend/cluster/cluster_ges.c');
# A dedicated CONVERT handler block (split out of the REQUEST fallthrough).
like($ges_c, qr/case GES_REQ_OPCODE_CONVERT: \{/,
	'L4: inbound opcode-2 CONVERT has its own handler (split from REQUEST)');
# The handler drives the live 5.1b convert state machine (spec-5.3 D3).
like($ges_c, qr/cluster_grd_convert_or_enqueue\(/,
	'L4: CONVERT handler runs the live convert decision (cluster_grd_convert_or_enqueue)');
# An illegal (LATERAL / no-holder) convert fail-closes via the reply ring with
# the 53R74 reason -- NOT an ereport on the LMS/receiving thread (L341).
like($ges_c, qr/GES_REJECT_REASON_ILLEGAL_CONVERT/,
	'L4: CONVERT handler fail-closes an illegal convert with ILLEGAL_CONVERT (53R74)');

my $la_c = slurp_src('src/backend/cluster/cluster_lock_acquire.c');
like($la_c,
	qr/case GES_REJECT_REASON_ILLEGAL_CONVERT:.*?CLUSTER_LOCK_ACQUIRE_FAIL_ILLEGAL_CONVERT/s,
	'L4: requester maps ILLEGAL_CONVERT to the FAIL_ILLEGAL_CONVERT result');

my $lock_c = slurp_src('src/backend/storage/lmgr/lock.c');
like($lock_c,
	qr/CLUSTER_LOCK_ACQUIRE_FAIL_ILLEGAL_CONVERT:.*?ERRCODE_CLUSTER_GES_ILLEGAL_LOCK_CONVERSION/s,
	'L4: the FAIL_ILLEGAL_CONVERT result raises 53R74');

diag('spec-5.3: cross-node convert is now LIVE; the GRANT/conflict/53R74 e2e '
	  . 'is t/283_tm_table_lock_convert.pl; GRD-level convert logic is unit-'
	  . 'covered by test_cluster_grd (U1-U12 + spec-5.3 U14/U15/U17/U18/U22).');


$node->stop;
done_testing();
