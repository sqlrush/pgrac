#-------------------------------------------------------------------------
#
# 029_checkpoint_delivery.pl
#    Native checkpoint/reload interleaving, with selected input injected.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# IDENTIFICATION
#    src/test/modules/test_pgrac_shared_config/t/029_checkpoint_delivery.pl
# NOTES
#    Real checkpointer, WAL, pages and SIGHUP; not distributed admission.
#-------------------------------------------------------------------------
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('checkpoint_delivery');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\ntest_pgrac_shared_config.delivery=on\n"
	. "autovacuum=off\nshared_buffers='64MB'\nbgwriter_lru_maxpages=0\n"
	. "full_page_writes=off\n"
	. "checkpoint_timeout='30s'\ncheckpoint_completion_target=0.9\nmax_wal_size='1GB'\n");
sub body
{
	my ($value, $memory) = @_;
	return "common.cluster.read_scache='$value'\ncommon.full_page_writes='$value'\n"
		. "common.work_mem='$memory'\n";
}
append_to_file($node->data_dir . '/test_config.input', body('off', '6MB'));
$node->start;
$node->safe_psql('postgres', q{
 CREATE EXTENSION test_pgrac_shared_config;
 CREATE TABLE checkpoint_target(id int, payload text) WITH (autovacuum_enabled=false);
 ALTER TABLE checkpoint_target ALTER COLUMN payload SET STORAGE PLAIN;
 INSERT INTO checkpoint_target SELECT i, repeat(md5(i::text), 32) FROM generate_series(1,16000) i;
 CHECKPOINT;
});
my $pid = $node->safe_psql('postgres', q{SELECT pid FROM pg_stat_activity WHERE backend_type='checkpointer'});
ok($pid > 0, 'actual native checkpointer is enrolled');

sub checkpoint_window
{
	$node->safe_psql('postgres', q{
  UPDATE checkpoint_target SET payload=reverse(payload);
  SELECT test_pgrac_config_checkpoint_request(false);
 });
	ok($node->poll_query_until('postgres', qq{
  SELECT wait_event='CheckpointWriteDelay' FROM pg_stat_activity WHERE pid=$pid
 }), 'real non-immediate checkpoint reached its native write-delay window')
		or BAIL_OUT('checkpoint did not reach native delay');
}
sub publish
{
	my ($generation, $value, $memory) = @_;
	open(my $file, '>', $node->data_dir . '/test_config.reload') or die $!;
	print {$file} body($value, $memory);
	close($file) or die $!;
	$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=$generation\n");
	my $offset = -s $node->logfile;
	$node->reload;
	my $log = '';
	for (1..400) {
		$log = substr(slurp_file($node->logfile), $offset);
		last if $log =~ /test checkpoint configuration: target=$generation /;
		usleep(25000);
	}
	like($log, qr/test checkpoint configuration: target=$generation /,
		'actual checkpointer processed the reload during this checkpoint');
	return $log;
}

checkpoint_window();
my $log = publish(2, 'on', '6MB');
like($log, qr/test checkpoint configuration: target=2 before=1 after=1 owned=1/,
	'common values remain on the original checkpoint generation');
my $retained = $log =~ /target=2 before=1 after=1 owned=1/;
# Accelerate only the disposable native fixture's completion, not any product
# or acceptance timeout. Preserve the first actual RED instead of hanging.
$node->safe_psql('postgres', 'CHECKPOINT');
if (!$retained) {
	$node->stop('fast');
	done_testing();
	exit;
}
is($node->safe_psql('postgres', 'SELECT full_page_writes FROM pg_control_checkpoint()'), 't',
	'idle retry propagates full_page_writes through the native WAL/shared-memory update');
ok($node->poll_query_until('postgres', qq{
 SELECT split_part(test_pgrac_config_enrollment($pid), ':', 3)='2'
}), 'original checkpointer consumes the common target at native idle');

checkpoint_window();
$log = publish(3, 'on', '9MB');
like($log, qr/test checkpoint configuration: target=3 before=2 after=3 owned=1/,
	'ordinary defaults still apply during the real owned checkpoint');
$node->safe_psql('postgres', 'CHECKPOINT');
is($node->safe_psql('postgres', 'SELECT count(*) FROM checkpoint_target'), '16000',
	'all native checkpointed rows remain present');
is($node->safe_psql('postgres', qq{SELECT pid FROM pg_stat_activity WHERE backend_type='checkpointer'}),
	$pid, 'the original checkpointer completed both requests without replacement');
$node->stop('fast');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM checkpoint_target'), '16000',
	'native normal stop and same-data restart retain all rows');
$node->stop('fast');
done_testing();
