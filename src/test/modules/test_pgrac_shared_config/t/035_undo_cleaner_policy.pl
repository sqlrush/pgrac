#-------------------------------------------------------------------------
#
# 035_undo_cleaner_policy.pl
#    Required cleaner configuration through the native GUC and COMMON paths.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/modules/test_pgrac_shared_config/t/035_undo_cleaner_policy.pl
#
# NOTES
#    Isolated native instances exercise configuration only, without shared
#    storage, cleaner work, cluster admission or a multi-node deployment.
#-------------------------------------------------------------------------
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub quoted
{
	my ($value) = @_;
	$value =~ s/'/''/g;
	return "'$value'";
}

my $node = PostgreSQL::Test::Cluster->new('cleaner_policy');
$node->init;
$node->start;
is($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name='cluster.undo_cleaner_enabled'}),
	'1', 'cleaner policy requires the cluster build');
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $setting = q{SELECT setting,reset_val,source,pending_restart FROM pg_settings
WHERE name='cluster.undo_cleaner_enabled'};
my $before = $node->safe_psql('postgres', $setting);

for my $value ('off', 'false', '0', 'no', 'OFF', 'invalid')
{
	for my $online ('false', 'true')
	{
		like($node->safe_psql('postgres',
			"SELECT test_pgrac_config_entry(-1, 'cluster.undo_cleaner_enabled', "
			. quoted($value) . ", $online)"),
			qr/^0:8:.*:cluster\.undo_cleaner_enabled$/,
			"COMMON rejects cleaner=$value with online=$online");
	}
}
for my $value ('on', 'true', '1', 'yes')
{
	like($node->safe_psql('postgres',
		"SELECT test_pgrac_config_entry(-1, 'cluster.undo_cleaner_enabled', "
		. quoted($value) . ', true)'), qr/^1:0:/,
		"COMMON accepts cleaner=$value");
}
like($node->safe_psql('postgres',
	q{SELECT test_pgrac_config_entry(0, 'cluster.undo_cleaner_enabled', 'on', false)}),
	qr/^0:5:/, 'cleaner setting cannot be overridden per instance');
for my $prepare ('false', 'true')
{
	like($node->safe_psql('postgres',
		'SELECT test_pgrac_config_object('
		. quoted("common.cluster.undo_cleaner_enabled='off'\n") . ", $prepare)"),
		qr/^0:8:.*:cluster\.undo_cleaner_enabled$/,
		"object validation refuses off before staging=$prepare");
}

my $bootstrap = join('', map { "common.$_='on'\n" }
	qw(cluster.controlfile_shared_authority cluster.enabled cluster.merged_recovery
	cluster.shared_catalog cluster.shared_config cluster.smgr_user_relations));
$bootstrap .= "common.cluster.shared_data_dir='/srv/pgrac/data'\n"
	. "common.cluster.shared_storage_backend='cluster_fs'\n"
	. "common.cluster.shared_storage_uuid='01010101-0101-0101-0101-010101010101'\n"
	. "common.cluster.undo_tablespace_path='/srv/pgrac/undo'\n"
	. "common.cluster.wal_threads_dir='/srv/pgrac/wal'\n"
	. "node000.cluster.node_id='0'\nnode001.cluster.node_id='1'\n";
for my $value ('on', 'off')
{
	my $body = join('', sort split(/(?<=\n)/,
		$bootstrap . "common.cluster.undo_cleaner_enabled='$value'\n"));
	like($node->safe_psql('postgres', 'SELECT test_pgrac_config_bootstrap('
		. quoted($body) . q{, 0, '/srv/pgrac/data', '/srv/pgrac/wal', '/srv/pgrac/undo', false)}),
		$value eq 'on' ? qr/^1:0:/ : qr/^0:8:.*:cluster\.undo_cleaner_enabled$/,
		"bootstrap validates cleaner=$value without relying on current GUC values");
}
my $old = "common.cluster.undo_cleaner_enabled='on'\ncommon.work_mem='8MB'\n";
my $new = "common.cluster.undo_cleaner_enabled='off'\ncommon.work_mem='12MB'\n";
like($node->safe_psql('postgres', 'SELECT test_pgrac_config_reload('
	. quoted($old) . ',' . quoted($new) . q{,0,''); SHOW cluster.undo_cleaner_enabled; SHOW work_mem}),
	qr/^0:8:0:0:0:0\non\n8MB$/,
	'reload refuses off before assignment and publishes no application receipt');
is($node->safe_psql('postgres', $setting), $before,
	'policy inspection preserves the postmaster default and its source');
$node->stop('fast');

sub startup_refused
{
	my ($name, $body, $extra, $reason) = @_;
	my $bad = PostgreSQL::Test::Cluster->new($name);
	$bad->init;
	$bad->append_conf('postgresql.conf', $extra);
	if (defined $body)
	{
		$bad->append_conf('postgresql.conf',
			"shared_preload_libraries='test_pgrac_shared_config'\n"
			. "test_pgrac_shared_config.apply_node=0\n");
		open(my $fh, '>:raw', $bad->data_dir . '/test_config.input') or die $!;
		print {$fh} $body;
		close($fh) or die $!;
	}
	my $started = $bad->start(fail_ok => 1);
	ok(!$started, "$name refuses startup");
	my $log = slurp_file($bad->logfile);
	like($log, $reason, "$name reports the cleaner constraint");
	unlike($log, qr/test shared configuration applied:/, "$name has no application receipt");
	$bad->stop('fast') if $started;
}
startup_refused('cleaner_common_off', "common.cluster.undo_cleaner_enabled='off'\n", '',
	qr/shared configuration object is not applicable/);
startup_refused('cleaner_inherited_off', "common.work_mem='6MB'\n",
	"cluster.undo_cleaner_enabled=off\n",
	qr/shared configuration requires cluster.undo_cleaner_enabled=on/);
for my $order (0, 1)
{
	my @lines = ("cluster.shared_config=on\n", "cluster.undo_cleaner_enabled=off\n");
	@lines = reverse @lines if $order;
	startup_refused("cleaner_local_order_$order", undef, join('', @lines),
		qr/shared configuration requires cluster.undo_cleaner_enabled=on/);
}

my $local = PostgreSQL::Test::Cluster->new('cleaner_nonshared');
$local->init;
$local->append_conf('postgresql.conf', "cluster.undo_cleaner_enabled=off\n");
$local->start;
is($local->safe_psql('postgres', 'SHOW cluster.undo_cleaner_enabled'), 'off',
	'non-shared native startup still permits cleaner off');
$local->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
like($local->safe_psql('postgres',
	q{SELECT test_pgrac_config_reload('','',0,''); SHOW cluster.undo_cleaner_enabled}),
	qr/^0:8:0:0:0:0\noff$/,
	'shared reload checks inherited off even when both images omit the setting');
$local->append_conf('postgresql.conf', "cluster.undo_cleaner_enabled=on\n");
$local->reload;
$local->poll_query_until('postgres', q{SELECT current_setting('cluster.undo_cleaner_enabled')='on'})
	or die 'native cleaner enable reload did not arrive';
is($local->safe_psql('postgres', 'SHOW cluster.undo_cleaner_enabled'), 'on',
	'non-shared native reload can enable the cleaner');
$local->append_conf('postgresql.conf', "cluster.undo_cleaner_enabled=off\n");
$local->reload;
$local->poll_query_until('postgres', q{SELECT current_setting('cluster.undo_cleaner_enabled')='off'})
	or die 'native cleaner disable reload did not arrive';
is($local->safe_psql('postgres', 'SHOW cluster.undo_cleaner_enabled'), 'off',
	'non-shared native reload can disable the cleaner');
$local->stop('fast');
done_testing();
