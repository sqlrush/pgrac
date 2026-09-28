# PGRAC: actual native memory context cleanup without granting CF authority.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('selection_memory');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_selection_cleanup()'),
	't', '1000 refused reads leave actual caller memory unchanged and no child contexts');
$node->stop('fast');
done_testing();
