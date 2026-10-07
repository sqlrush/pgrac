#-------------------------------------------------------------------------
#
# 031_undo_publication_refusal.pl
#    PGRAC: an unadmitted undo publication must remain a SQL error, not a
#    backend assertion failure. Exercise real ERROR recovery twice so a
#    leaked allocator LWLock cannot hide behind the first refusal.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/031_undo_publication_refusal.pl
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;

# PRE2 native OPEN requires the full fixed four-member cohort. Only node0 and
# node1 perform SQL work; the original refusal assertions and budget are unchanged.
# Use the product's 30-second CF deadline during native formation. The cohort
# helper's one-second diagnostic override is unrelated to UNDO error cleanup.
my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('undo_refusal',
	nodes => 4, blackbox => 1,
	extra_conf => [ 'autovacuum = off', 'cluster.cf_enqueue_timeout_ms = 30000' ]);
# This checks the real selected ROOT, formation, PGSA/R4 and every member's
# current writer before any SQL mutation. No SOURCE_ZERO or fabricated token.
$cluster->start_cluster;
my $creator = $cluster->node0;
# Catalog DDL can already publish the creator's undo segment. Keep the tested
# writer free of prior mutations so the deny-only first-publication point is
# reached by the original INSERT, with no forced cursor or file manipulation.
my $node = $cluster->node1;

# An admitted cohort has an authoritative undo root. Refuse only its first
# live-owner publication, after real admission, using the native deny-only
# point. Its absence is a missing prerequisite, never a skipped assertion.
my $point = 'cluster-undo-first-publication-deny';
my $available = $node->safe_psql('postgres',
	"SELECT count(*) FROM pg_stat_cluster_injections WHERE name='$point'");
die "BLOCKED: native first UNDO publication denial point is unavailable\n"
	unless $available eq '1';
$creator->safe_psql('postgres', 'CREATE TABLE undo_refusal (id int)');
my $started = $node->safe_psql('postgres', 'SELECT pg_postmaster_start_time()');

# The denial must traverse real ERROR recovery twice under one postmaster.
for my $attempt (1 .. 2)
{
	my ($out, $err);
	# Injection state is backend-local. Arm in the same psql connection that
	# runs the INSERT; ON_ERROR_STOP still returns the real SQL error code.
	my $rc = $node->psql('postgres',
		"SELECT cluster_inject_fault('$point', 'skip', 0);\n"
		  . 'INSERT INTO undo_refusal VALUES (1)',
		stdout => \$out, stderr => \$err,
		extra_params => [ '-v', 'VERBOSITY=verbose' ], timeout => 15);
	is($rc, 3, "attempt $attempt returns an ordinary SQL error");
	like($err, qr/55000: undo segment first publication refused:/,
		"attempt $attempt preserves the authority refusal");
	like($out, qr/^t$/m,
		"attempt $attempt arms refusal in the INSERT backend");
	is($node->safe_psql('postgres', 'SELECT pg_postmaster_start_time()'),
		$started, "attempt $attempt does not restart the server");
	is($node->safe_psql('postgres', 'SELECT count(*) FROM undo_refusal'),
		'0', "attempt $attempt publishes no row");
}

unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal/,
	'error cleanup does not crash a backend');

# Global catalogs use the native global-tablespace/database-zero address.
# Exercise independent existing rows too: a failed CREATE must not hide an
# ALTER or shared-description failure behind a missing test role.
for my $case (
	[ 'CREATE ROLE ctrc_namespace_role NOLOGIN', 'CREATE ROLE' ],
	[ 'ALTER ROLE CURRENT_USER NOINHERIT', 'ALTER ROLE' ],
	[ "COMMENT ON DATABASE postgres IS 'ctrc namespace insert'", 'shared catalog insert' ],
	[ "COMMENT ON DATABASE postgres IS 'ctrc namespace update'", 'shared catalog update' ])
{
	my ($out, $err);
	my $rc = $creator->psql('postgres', $case->[0],
		stdout => \$out, stderr => \$err,
		extra_params => [ '-v', 'VERBOSITY=verbose' ], timeout => 15);
	is($rc, 0, "$case->[1] accepts the native shared catalog address");
	diag($err) if $rc;
}
for my $member ($cluster->nodes)
{
	is($member->safe_psql('postgres',
		"SELECT count(*) FROM pg_roles WHERE rolname='ctrc_namespace_role' AND NOT rolcanlogin"),
		'1', $member->name . ' observes CREATE ROLE');
	is($member->safe_psql('postgres',
		'SELECT rolinherit FROM pg_roles WHERE rolname=current_user'),
		'f', $member->name . ' observes ALTER ROLE');
	is($member->safe_psql('postgres',
		"SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname='postgres'"),
		'ctrc namespace update', $member->name . ' observes shared catalog update');
}
for my $sql ('DROP ROLE IF EXISTS ctrc_namespace_role',
	'ALTER ROLE CURRENT_USER INHERIT', 'COMMENT ON DATABASE postgres IS NULL')
{
	my ($out, $err);
	my $rc = $creator->psql('postgres', $sql, stdout => \$out, stderr => \$err, timeout => 15);
	is($rc, 0, 'shared catalog cleanup succeeds');
	diag($err) if $rc;
}
# Each armed INSERT connection has exited; no other backend was armed.
$cluster->stop_cluster;
done_testing();
