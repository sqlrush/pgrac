# Copyright (c) 2026, PostgreSQL Global Development Group
# PGRAC: native shared ALTER SYSTEM routing and refusal isolation.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('shared_sql');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $auto = $node->data_dir . '/postgresql.auto.conf';
my $before = slurp_file($auto);
for my $stmt ('SET work_mem = \'12MB\'', 'RESET work_mem', 'RESET ALL',
	'SET cluster.read_scache = on', 'SET port = 6543')
{
	my ($out, $err);
	my $rc = $node->psql('postgres',
		"SELECT test_pgrac_config_sql_mode(true);\nALTER SYSTEM $stmt;",
		stdout => \$out, stderr => \$err);
	isnt($rc, 0, "$stmt refuses absent shared authority");
	like($err, qr/shared configuration|shared ALTER SYSTEM/i,
		"$stmt reports the shared route");
	is(slurp_file($auto), $before, "$stmt never falls back to local auto.conf");
}
$node->safe_psql('postgres', 'CREATE ROLE config_no_priv');
my ($out, $err);
my $rc = $node->psql('postgres',
	"SELECT test_pgrac_config_sql_mode(true);\nSET ROLE config_no_priv;\n"
	. "ALTER SYSTEM SET work_mem = '12MB';",
	stdout => \$out, stderr => \$err);
isnt($rc, 0, 'native parameter ACL still denies unprivileged callers');
like($err, qr/permission denied/, 'ACL runs before shared publication');
is(slurp_file($auto), $before, 'ACL refusal does not write local defaults');
$node->safe_psql('postgres', "ALTER SYSTEM SET work_mem = '13MB'");
like(slurp_file($auto), qr/work_mem = '13MB'/, 'non-shared SQL keeps native publication');
$node->safe_psql('postgres', 'ALTER SYSTEM RESET work_mem');
unlike(slurp_file($auto), qr/work_mem =/, 'non-shared RESET remains native');
$node->stop('fast');
done_testing();
