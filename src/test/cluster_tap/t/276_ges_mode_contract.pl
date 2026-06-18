#-------------------------------------------------------------------------
#
# 276_ges_mode_contract.pl
#    GES lock-mode encoding + 8x8 compatibility matrix contract (spec-5.1a).
#
#    Backend-side coverage of the frozen GES mode contract that the
#    cluster_unit test (test_cluster_ges_mode) cannot reach:
#      - the matrix SRF dumps all 64 cells with correct content,
#      - the live self-check (cluster_ges_mode_matches_pg) confirms the
#        frozen matrix agrees with the real DoLockModesConflict table,
#      - the cluster.ges_mode_selfcheck GUC defaults to fatal and the node
#        started cleanly (the startup self-check passed),
#      - cluster_ges_mode_compat accepts only canonical PG names
#        (case-insensitive) and fails closed on DLM aliases / unknown names.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/276_ges_mode_contract.pl
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
use Test::More;
use PgracClusterNode;


my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;


# ----------------------------------------------------------------------
# L1 -- matrix SRF dumps all 64 cells with correct content.
# ----------------------------------------------------------------------
my $rows = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_ges_mode_matrix()});
is($rows, 64, 'L1: matrix SRF returns 64 rows (8x8)');

my $as_ae = $node->safe_psql('postgres',
	q{SELECT compatible FROM pg_cluster_ges_mode_matrix()
	  WHERE held = 'AccessShareLock' AND wanted = 'AccessExclusiveLock'});
is($as_ae, 'f', 'L1: AccessShare vs AccessExclusive conflict');

my $share_share = $node->safe_psql('postgres',
	q{SELECT compatible FROM pg_cluster_ges_mode_matrix()
	  WHERE held = 'ShareLock' AND wanted = 'ShareLock'});
is($share_share, 't', 'L1: Share vs Share compatible');

my $sue_sue = $node->safe_psql('postgres',
	q{SELECT compatible FROM pg_cluster_ges_mode_matrix()
	  WHERE held = 'ShareUpdateExclusiveLock' AND wanted = 'ShareUpdateExclusiveLock'});
is($sue_sue, 'f', 'L1: ShareUpdateExclusive self-conflict');

my $as_dlm = $node->safe_psql('postgres',
	q{SELECT DISTINCT held_dlm FROM pg_cluster_ges_mode_matrix()
	  WHERE held = 'AccessShareLock'});
is($as_dlm, 'CR', 'L1: AccessShareLock DLM alias is CR');

my $ae_dlm = $node->safe_psql('postgres',
	q{SELECT DISTINCT held_dlm FROM pg_cluster_ges_mode_matrix()
	  WHERE held = 'AccessExclusiveLock'});
is($ae_dlm, 'EX', 'L1: AccessExclusiveLock DLM alias is EX');


# ----------------------------------------------------------------------
# L2 -- cluster_ges_mode_compat scalar correctness + case-insensitivity.
# ----------------------------------------------------------------------
is( $node->safe_psql('postgres',
		q{SELECT cluster_ges_mode_compat('AccessShareLock', 'AccessExclusiveLock')}),
	'f', 'L2: AS vs AE incompatible');
is( $node->safe_psql('postgres',
		q{SELECT cluster_ges_mode_compat('AccessShareLock', 'ShareLock')}),
	't', 'L2: AS vs Share compatible');
is( $node->safe_psql('postgres',
		q{SELECT cluster_ges_mode_compat('accessSHArelock', 'ShareLock')}),
	't', 'L2: parser is case-insensitive');


# ----------------------------------------------------------------------
# L3 -- live self-check: frozen matrix == real DoLockModesConflict (KEY).
# ----------------------------------------------------------------------
is($node->safe_psql('postgres', q{SELECT cluster_ges_mode_matches_pg()}),
	't', 'L3: frozen matrix matches the live lock conflict table');


# ----------------------------------------------------------------------
# L4 -- GUC default is fatal; node started cleanly (startup check passed).
# ----------------------------------------------------------------------
is($node->safe_psql('postgres', q{SHOW cluster.ges_mode_selfcheck}),
	'fatal', 'L4: cluster.ges_mode_selfcheck defaults to fatal');


# ----------------------------------------------------------------------
# L5 -- parser fails closed on DLM aliases and unknown names (SQLSTATE 22023).
# ----------------------------------------------------------------------
my ($ret1, undef, $err1) = $node->psql('postgres',
	q{SELECT cluster_ges_mode_compat('CR', 'EX')});
isnt($ret1, 0, 'L5: DLM alias input is rejected');
like($err1, qr/unknown GES lock mode name/,
	'L5: DLM alias error names the offending mode');

my ($ret2, undef, $err2) = $node->psql('postgres',
	q{SELECT cluster_ges_mode_compat('bogus', 'ShareLock')});
isnt($ret2, 0, 'L5: unknown name input is rejected');

my $sqlstate = $node->safe_psql(
	'postgres',
	q{DO $$ BEGIN
		PERFORM cluster_ges_mode_compat('CR', 'EX');
	  EXCEPTION WHEN invalid_parameter_value THEN
		RAISE NOTICE 'caught 22023';
	  END $$});
# safe_psql returns empty; the NOTICE proves the SQLSTATE class.  Re-run
# capturing stderr to assert the 22023 class explicitly.
my (undef, undef, $noticeerr) = $node->psql('postgres',
	q{DO $$ BEGIN
		PERFORM cluster_ges_mode_compat('CR', 'EX');
	  EXCEPTION WHEN invalid_parameter_value THEN
		RAISE NOTICE 'caught 22023';
	  END $$});
like($noticeerr, qr/caught 22023/,
	'L5: parser raises ERRCODE_INVALID_PARAMETER_VALUE (22023)');


$node->stop;
done_testing();
