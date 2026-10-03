# Copyright (c) 2026, PostgreSQL Global Development Group
# Native SQL baseline for parallel groups with a leader-held DDL lock.
# Cross-node GES coverage needs the separately scheduled shared cluster.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use JSON::PP qw(decode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('parallel_catalog_locks');
$node->init;
$node->append_conf('postgresql.conf', qq{
cluster.enabled=off
max_worker_processes=8
max_parallel_workers=4
});
$node->start;
$node->safe_psql('postgres', q{
CREATE TABLE parallel_locked AS SELECT generate_series(1,20000) AS id;
ALTER TABLE parallel_locked SET (parallel_workers=2);
ANALYZE parallel_locked;
});
my $leader = $node->background_psql('postgres');
my $ddl = $node->background_psql('postgres');
$leader->query_safe(q{
SET statement_timeout='15s';
SET max_parallel_workers_per_gather=2;
SET min_parallel_table_scan_size=0;
SET parallel_setup_cost=0;
SET parallel_tuple_cost=0;
SET parallel_leader_participation=off;
BEGIN;
LOCK TABLE parallel_locked IN ACCESS EXCLUSIVE MODE;
});
$ddl->query_safe(q{SET application_name='parallel_group_ddl'; SET statement_timeout='20s'});
$ddl->query_until(qr/ddl_submitted/, qq{\\echo ddl_submitted
ALTER TABLE parallel_locked ADD COLUMN extra integer;
});
ok($node->poll_query_until('postgres', q{
SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
 WHERE a.application_name='parallel_group_ddl' AND NOT l.granted
 AND l.relation='parallel_locked'::regclass)
}), 'other backend DDL is queued behind the leader');

my $plan = decode_json($leader->query_safe(q{
EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, TIMING OFF)
 SELECT sum(id) FROM parallel_locked
}));
sub workers_launched
{
    my ($plan) = @_;
    my $n = $plan->{'Workers Launched'} // 0;
    $n += workers_launched($_) for @{$plan->{Plans} // []};
    return $n;
}
cmp_ok(workers_launched($plan->[0]->{Plan}), '>=', 1,
       'actual parallel workers finish while leader holds AEL and DDL waits');
is($leader->query_safe('SELECT sum(id) FROM parallel_locked'), '200010000',
   'parallel result is complete');
$leader->query_safe('COMMIT');
$ddl->query_safe('SELECT 1');
is($node->safe_psql('postgres', q{
SELECT count(*) FROM pg_attribute WHERE attrelid='parallel_locked'::regclass
 AND attname='extra' AND NOT attisdropped
}), '1', 'queued DDL completes after group releases the lock');
$leader->quit;
$ddl->quit;
$node->stop;
done_testing();
