# Copyright (c) 2026, PostgreSQL Global Development Group
# Shared backup recovery is unavailable until retained DROP windows can be
# qualified durably. Native backup recovery remains PostgreSQL behavior.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $source = PostgreSQL::Test::Cluster->new('backup_refusal_source');
$source->init(allows_streaming => 1);
$source->append_conf('postgresql.conf', "cluster.enabled=off\nautovacuum=off\n");
$source->start;
$source->safe_psql('postgres', 'CREATE TABLE backup_payload(v int); INSERT INTO backup_payload VALUES(42)');
$source->backup('before_shared_mode');
$source->stop;

for my $mode ('catalog', 'config', 'both')
{
    my $clone = PostgreSQL::Test::Cluster->new("backup_refusal_$mode");
    $clone->init_from_backup($source, 'before_shared_mode');
    my $label = $clone->data_dir . '/backup_label';
    my $before = slurp_file($label);
    my $control = slurp_file($clone->data_dir . '/global/pg_control');
    $clone->append_conf('postgresql.conf',
        'cluster.shared_catalog=' . ($mode eq 'config' ? 'off' : 'on') . "\n"
        . 'cluster.shared_config=' . ($mode eq 'catalog' ? 'off' : 'on') . "\n");
    ok(!$clone->start(fail_ok => 1), "$mode refuses a backup_label startup");
    like(slurp_file($clone->logfile),
        qr/FATAL:.*backup_label recovery is not supported in shared mode/,
        'refusal identifies the unsupported recovery before authority migration');
    is(slurp_file($label), $before, 'refusal retains backup_label unchanged');
    is(slurp_file($clone->data_dir . '/global/pg_control'), $control,
        'refusal leaves the control file unchanged');
}
my $native = PostgreSQL::Test::Cluster->new('native_backup_recovery');
$native->init_from_backup($source, 'before_shared_mode');
$native->start;
is($native->safe_psql('postgres', 'SELECT v FROM backup_payload'), '42',
    'native backup recovery still restores the original data');
ok(!-e $native->data_dir . '/backup_label', 'native recovery consumes its backup label normally');
$native->safe_psql('postgres', q{
CREATE FUNCTION shared_drop_database_guard(bool, bool) RETURNS void
AS '$libdir/test_pgrac_shared_config', 'test_pgrac_shared_drop_database_guard' LANGUAGE C STRICT;
});
$native->safe_psql('postgres', 'CREATE DATABASE retained_database');
my $dbid = $native->safe_psql('postgres',
    q{SELECT oid FROM pg_database WHERE datname='retained_database'});
for my $config ('true', 'false')
{
  for my $direct ('false', 'true')
  {
    my ($out, $err);
    my $rc = $native->psql('postgres',
        "\\set VERBOSITY verbose\nSELECT shared_drop_database_guard($config, $direct)",
        stdout => \$out, stderr => \$err);
    is($rc, 3, "shared DROP DATABASE is refused with shared_config=$config, direct=$direct");
    like($err, qr/0A000:.*(?:CREATE\/)?DROP DATABASE is not supported in shared mode/s,
        'shared DROP DATABASE has the explicit pre-mutation refusal');
  }
}
is($native->safe_psql('postgres',
    q{SELECT oid FROM pg_database WHERE datname='retained_database'}), $dbid,
    'refused DROP keeps the same database identity');
ok(-d $native->data_dir . "/base/$dbid", 'refused DROP retains database files');
$native->stop;
done_testing();
