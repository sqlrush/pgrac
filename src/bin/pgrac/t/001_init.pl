#-------------------------------------------------------------------------
#
# 001_init.pl
#    File-level regression for the pgrac-init Stage 0.20 wrapper.
#
#    This TAP test exercises pgrac-init at the filesystem level (no
#    server start; that is covered by 002_start.pl):
#      - --help / --version contracts
#      - Invalid flags are rejected with a "Try --help" hint
#      - Empty PGDATA is bootstrapped end-to-end (initdb succeeds,
#        cluster.node_id appears in postgresql.conf, pgrac.conf gets
#        a [node.<id>] section)
#      - Idempotent re-run is a no-op
#      - Conflicting re-run without --force is rejected
#      - --force re-run rewrites the conf bits
#      - Out-of-range --node-id is rejected
#
# IDENTIFICATION
#    src/bin/pgrac/t/001_init.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use PostgreSQL::Test::Utils;
use Test::More;

# ----------
# 1. CLI plumbing.
# ----------
program_help_ok('pgrac-init');
program_version_ok('pgrac-init');

# Invalid option -> exit 1 with "Try --help" hint.
my ($stdout, $stderr);
my $rc = IPC::Run::run([ 'pgrac-init', '--bogus-flag' ],
	'>', \$stdout, '2>', \$stderr);
ok(!$rc, 'pgrac-init rejects unknown option');
like($stderr, qr/--help/, 'rejection message includes "--help" hint');

# Out-of-range node id is rejected.
command_fails(
	[ 'pgrac-init', '-D', '/tmp/nonexistent', '--node-id=200' ],
	'pgrac-init rejects --node-id out of [0, 127]');

# ----------
# 2. End-to-end bootstrap on a fresh PGDATA.
# ----------
my $tempdir = PostgreSQL::Test::Utils::tempdir;
my $datadir = "$tempdir/data";

command_ok(
	[ 'pgrac-init', '-D', $datadir, '--node-id=7', '--cluster-name=tap-test' ],
	'pgrac-init -D EMPTY succeeds');

ok(-d $datadir, 'pgrac-init created PGDATA');
ok(-f "$datadir/PG_VERSION",
	'pgrac-init produced a PG-shaped data directory');
ok(-f "$datadir/postgresql.conf",
	'postgresql.conf exists');
ok(-f "$datadir/pgrac.conf",
	'pgrac.conf exists');

my $pgconf = PostgreSQL::Test::Utils::slurp_file("$datadir/postgresql.conf");
like($pgconf, qr/^cluster\.node_id\s*=\s*7\s*$/m,
	'cluster.node_id = 7 appended to postgresql.conf');

my $pgrac_conf = PostgreSQL::Test::Utils::slurp_file("$datadir/pgrac.conf");
like($pgrac_conf, qr/\[cluster\][^\[]*name\s*=\s*tap-test/s,
	'pgrac.conf [cluster] section has name=tap-test');
like($pgrac_conf, qr/\[node\.7\]/,
	'pgrac.conf has [node.7] section');
like($pgrac_conf, qr/role\s*=\s*primary/,
	'pgrac.conf [node.7] has role=primary');

# ----------
# 3. Idempotent re-run on the same PGDATA with the same parameters.
# ----------
command_ok(
	[ 'pgrac-init', '-D', $datadir, '--node-id=7', '--cluster-name=tap-test' ],
	'pgrac-init re-run with same params is a no-op');

# ----------
# 4. Conflicting re-run without --force is rejected.
# ----------
command_fails(
	[ 'pgrac-init', '-D', $datadir, '--node-id=42' ],
	'pgrac-init re-run with different --node-id and no --force fails');

# ----------
# 5. --force re-run rewrites cluster.node_id.
# ----------
command_ok(
	[ 'pgrac-init', '-D', $datadir, '--node-id=42', '--force' ],
	'pgrac-init --force rewrites cluster.node_id');

$pgconf = PostgreSQL::Test::Utils::slurp_file("$datadir/postgresql.conf");
like($pgconf, qr/^cluster\.node_id\s*=\s*42\s*$/m,
	'postgresql.conf now reflects the new --node-id after --force');

done_testing();
