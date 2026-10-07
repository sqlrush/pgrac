# PGRAC: native fsync and repeated immutable history installation.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('wal_history');
$node->init;
$node->append_conf('postgresql.conf', "restart_after_crash=off\nfsync=on\n");
$node->start;
if ($node->safe_psql('postgres', q{SELECT count(*) FROM pg_settings WHERE name='cluster.shared_config'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC build required, no admission certification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my ($rc, $out, $err) = $node->psql('postgres',
	'SELECT test_pgrac_wal_history_native(); SELECT test_pgrac_wal_history_native()');
is($rc, 0, 'history install uses native fsync without a backend crash');
is($out, "t\nt", 'repeat install and discard work across native query contexts');
if ($rc == 0) { $node->stop('fast'); }
else { note($err); $node->stop('immediate', fail_ok => 1); }
done_testing();
