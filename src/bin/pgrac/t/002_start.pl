#-------------------------------------------------------------------------
#
# 002_start.pl
#    End-to-end regression for the pgrac-start Stage 0.20 wrapper.
#
#    Drives the full bootstrap-then-launch flow that a Stage 0 user
#    would run: pgrac-init creates PGDATA, pgrac-start brings the
#    server up, psql confirms cluster.node_id and pg_cluster_nodes
#    reflect the configured values.  Also verifies the preflight
#    error paths (missing PGDATA, missing cluster.node_id) and the
#    pgrac.conf / postgresql.conf mismatch warning.
#
# IDENTIFICATION
#    src/bin/pgrac/t/002_start.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ----------
# 1. CLI plumbing.
# ----------
program_help_ok('pgrac-start');
program_version_ok('pgrac-start');

# ----------
# 2. Preflight failure: PGDATA does not exist.
# ----------
my $tempdir = PostgreSQL::Test::Utils::tempdir;
command_fails(
	[ 'pgrac-start', '-D', "$tempdir/no-such-dir" ],
	'pgrac-start fails when PGDATA does not exist');

# ----------
# 3. Preflight failure: PGDATA exists but no cluster.node_id.
#
#    Build a bare PGDATA via plain initdb (skipping pgrac-init) so
#    cluster.node_id is *not* set.  pgrac-start should refuse with
#    the conf-check error.
# ----------
my $bare_dir = "$tempdir/bare";
command_ok([ 'initdb', '-D', $bare_dir, '-A', 'trust', '-N' ],
	'plain initdb succeeds (precondition for preflight test)');
command_fails(
	[ 'pgrac-start', '-D', $bare_dir ],
	'pgrac-start fails when cluster.node_id is not declared');

# ----------
# 4. Golden path: pgrac-init then pgrac-start, verify SHOW + pg_cluster_nodes.
#
#    We allocate a port via PostgreSQL::Test::Cluster->get_new_node so
#    parallel TAP runs do not collide, but drive everything else
#    through pgrac-init / pgrac-start to exercise the wrappers.
# ----------
my $node = PostgreSQL::Test::Cluster->new('pgrac_002');
my $port = $node->port;
my $datadir = "$tempdir/main";

command_ok(
	[ 'pgrac-init', '-D', $datadir, '--node-id=3', '--cluster-name=tap-002' ],
	'pgrac-init bootstraps PGDATA');

# Configure port + unix socket so we can talk to the server without
# clashing with any concurrent test cluster.
PostgreSQL::Test::Utils::append_to_file("$datadir/postgresql.conf",
	"port = $port\n"
	. "unix_socket_directories = '$tempdir'\n"
	. "listen_addresses = ''\n");

# Boot via pgrac-start (which exec's pg_ctl).
my $logfile = "$tempdir/server.log";
command_ok(
	[ 'pgrac-start', '-D', $datadir, '-l', $logfile, '-w', '-t', '60' ],
	'pgrac-start launches the server');

# Verify GUC and view via psql + the same socket / port.
my $psql_args = [
	'psql',
	'-h', $tempdir,
	'-p', $port,
	'-d', 'postgres',
	'-X',
	'-A',
	'-t',
	'-c',
];

my $cluster_node_id;
{
	my $out;
	IPC::Run::run([ @$psql_args, q{SHOW "cluster.node_id"} ], '>', \$out)
	    or die "psql SHOW failed";
	chomp $out;
	$cluster_node_id = $out;
}
is($cluster_node_id, '3',
	'SHOW cluster.node_id returns the value pgrac-init wrote');

my $self_row;
{
	my $out;
	IPC::Run::run(
		[ @$psql_args,
			q{SELECT node_id || '|' || role
			    FROM pg_cluster_nodes
			   WHERE is_self} ],
		'>', \$out)
	    or die "psql SELECT failed";
	chomp $out;
	$self_row = $out;
}
is($self_row, '3|primary',
	'pg_cluster_nodes self row matches pgrac.conf [node.3]');

# Tear down via pg_ctl (pgrac-start only handles start).
command_ok(
	[ 'pg_ctl', '-D', $datadir, '-w', 'stop' ],
	'pg_ctl stop ends the server cleanly');

# ----------
# 5. Mismatch warning: pgrac.conf [node.3] vs postgresql.conf
#    cluster.node_id = 9 should produce a WARNING (not a hard fail at
#    pgrac-start; postmaster itself FATALs).
# ----------
PostgreSQL::Test::Utils::append_to_file("$datadir/postgresql.conf",
	"\ncluster.node_id = 9\n");

my ($stdout, $stderr);
my $rc = IPC::Run::run(
	[ 'pgrac-start', '-D', $datadir, '-l', $logfile, '-W' ],
	'>', \$stdout, '2>', \$stderr);
like($stderr, qr/does not match postgresql\.conf cluster\.node_id=9/,
	'pgrac-start warns when pgrac.conf and cluster.node_id disagree');

# Best-effort cleanup; the postmaster may have started in a half-broken
# state and pg_ctl stop returns non-zero.
IPC::Run::run([ 'pg_ctl', '-D', $datadir, '-m', 'immediate', 'stop' ]);

done_testing();
