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
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('undo_refusal');
$node->init;
$node->append_conf('postgresql.conf',
	"cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "autovacuum = off\n");
$node->start;
$node->safe_psql('postgres', 'CREATE TABLE undo_refusal (id int)');
my $started = $node->safe_psql('postgres', 'SELECT pg_postmaster_start_time()');

# No voting-backed undo root is installed: authority rejection is required.
for my $attempt (1 .. 2)
{
	my ($out, $err);
	my $rc = $node->psql('postgres', 'INSERT INTO undo_refusal VALUES (1)',
		stdout => \$out, stderr => \$err,
		extra_params => [ '-v', 'VERBOSITY=verbose' ], timeout => 15);
	is($rc, 3, "attempt $attempt returns an ordinary SQL error");
	like($err, qr/55000: undo segment first publication refused:/,
		"attempt $attempt preserves the authority refusal");
	is($node->safe_psql('postgres', 'SELECT pg_postmaster_start_time()'),
		$started, "attempt $attempt does not restart the server");
	is($node->safe_psql('postgres', 'SELECT count(*) FROM undo_refusal'),
		'0', "attempt $attempt publishes no row");
}

unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal/,
	'error cleanup does not crash a backend');
$node->stop('fast');
done_testing();
