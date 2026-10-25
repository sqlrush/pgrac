#!/usr/bin/env perl
# Native miss while the dirty current holder owns a physical write interval.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my $cluster;
END { eval { $cluster->stop_cluster } if defined $cluster; }
sub require_true { BAIL_OUT($_[1]) unless $_[0]; }
sub wait_log {
 my ($node,$pattern,$offset)=@_;
 my $until=clock_gettime(CLOCK_MONOTONIC)+30;
 while(clock_gettime(CLOCK_MONOTONIC)<$until) {
  my $text=substr(slurp_file($node->logfile),$offset // 0);
  return $text if $text =~ $pattern;
  sleep(0.02);
 }
 BAIL_OUT('native writer did not reach its owned physical write interval');
}
$cluster=PostgreSQL::Test::ClusterPRE2->new_cluster('first_source',
 nodes=>4, blackbox=>1, required_operations=>[],
 extra_conf=>['autovacuum=off','cluster.trace_block_io=on',
              'log_error_verbosity=verbose','cluster.read_scache=off']);
$cluster->start_cluster;
my @nodes=$cluster->nodes;
my ($a,$b)=@nodes[0,1];
for my $name (qw(cached page flush)) {
 my $type=$name eq 'cached' ? 'boolean':'bytea';
 $a->safe_psql('postgres',qq{CREATE FUNCTION first_source_$name(regclass) RETURNS $type
 AS '\$libdir/test_pcm_read_source', 'test_pcm_read_source_$name'
 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE});
}
$a->safe_psql('postgres',q{CREATE TABLE first_source_data(id int, v int, pad text);
 INSERT INTO first_source_data SELECT i,1,repeat(md5(i::text),2) FROM generate_series(1,20)i});
$a->safe_psql('postgres','VACUUM(FREEZE) first_source_data');
$a->safe_psql('postgres','CHECKPOINT');
$a->safe_psql('postgres',q{UPDATE first_source_data SET v=2});
my $rel=$a->safe_psql('postgres',q{SELECT pg_relation_filenode('first_source_data')});
require_true($rel =~ /^[1-9][0-9]*$/,'exact target relfilenumber is required');
is($b->safe_psql('postgres',q{SELECT first_source_cached('first_source_data')}),'f',
 'peer has no target descriptor before the actual native miss');
my $writer=$a->background_psql('postgres',timeout=>120);
my $reader=$b->background_psql('postgres',timeout=>120);
my $wpid=$writer->query_safe('SELECT pg_backend_pid()');
my $rpid=$reader->query_safe('SELECT pg_backend_pid()');
require_true($wpid =~ /^[1-9][0-9]*$/ && $rpid =~ /^[1-9][0-9]*$/,'exact native PIDs required');
my ($ao,$bo)=(-s $a->logfile,-s $b->logfile);
$writer->query_safe(q{SELECT cluster_inject_fault('cluster-pcm-write-before-data','sleep',8000000)});
$writer->query_until(qr/FLUSH_SENT/,"\\echo FLUSH_SENT\nSELECT md5(first_source_flush('first_source_data'));\n");
wait_log($a,qr/BLOCK_IO event=WRITE_PAUSE .*pid=$wpid .*tag=\d+\/\d+\/$rel\/0\/0 /,$ao);
pass('actual dirty-current flush holds BufferIO before smgrwrite');
$reader->query_until(qr/READ_SENT/,"\\echo READ_SENT\nSELECT md5(first_source_page('first_source_data'));\n");
wait_log($b,qr/BLOCK_IO event=READ_SOURCE phase=REQUEST .*pid=$rpid .*tag=\d+\/\d+\/$rel\/0\/0 /,$bo);
unlike(substr(slurp_file($a->logfile),$ao),qr/BLOCK_IO event=WRITE_COMPLETE .*pid=$wpid .*tag=\d+\/\d+\/$rel\/0\/0 /,
 'peer entered the actual first-fill request before the writer completed IO');
my $writer_hash=$writer->query_safe("SELECT 'WRITER_DONE'");
my $reader_hash=$reader->query_safe("SELECT 'READER_DONE'");
# BackgroundPsql returns the unread first query output before these markers.
my ($wh)=$writer_hash =~ /([0-9a-f]{32})/;
my ($rh)=$reader_hash =~ /([0-9a-f]{32})/;
require_true(defined($wh) && defined($rh),'both original native page digests must be captured');
is($rh,$wh,'peer installs the complete current page, byte for byte');
my $blog=substr(slurp_file($b->logfile),$bo);
my @requests=($blog =~ /^.*BLOCK_IO event=READ_SOURCE phase=REQUEST .*pid=$rpid .*tag=\d+\/\d+\/$rel\/0\/0 .*$/mg);
is(scalar(@requests),1,'native write-busy reader retains its original request without retry flooding');
like(substr(slurp_file($a->logfile),$ao),qr/BLOCK_IO event=FIRST_FILL_HOLDER_PARK .*tag=\d+\/\d+\/$rel\/0\/0 /,
 'holder retained the request against the actual native write episode');
like($blog,qr/BLOCK_IO event=READ_SOURCE phase=DECISION .*pid=$rpid .*tag=\d+\/\d+\/$rel\/0\/0 source=2 /,
 'same original miss chose an installed GCS image');
unlike($blog,qr/BLOCK_IO event=READ_SOURCE phase=DISK_BEGIN .*pid=$rpid .*tag=\d+\/\d+\/$rel\/0\/0 /,
 'peer never entered disk IO while a foreign current/dirty source existed');
like(substr(slurp_file($a->logfile),$ao),qr/BLOCK_IO event=WRITE_COMPLETE .*pid=$wpid .*tag=\d+\/\d+\/$rel\/0\/0 /,
 'actual writer completion is distinct from the source receipt');
is($b->safe_psql('postgres','SELECT count(*),sum(v) FROM first_source_data'),'20|40',
 'ordinary native query sees all committed rows and complete values');
$writer->quit; $reader->quit;
$cluster->stop_cluster; undef $cluster;
for my $node (@nodes) {
 unlike(slurp_file($node->logfile),qr/PANIC|invalid page in block|checksum verification failed/,
  'normal closed member has no checksum error or PANIC');
}
done_testing();
