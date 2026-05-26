#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 211_cluster_3_5_subxact_lock_activation.pl
#	  spec-3.5 D14 — subxact heap_lock_tuple cross-node lock activation
#	  smoke (spec-3.4d 53R98 family graduate).
#
#	  spec-3.4d D11 cluster_itl_lock_path_enabled previously refused
#	  subxact callers via GetCurrentTransactionNestLevel() <= 1.  spec-3.5
#	  D9 removed that barrier, so SELECT FOR UPDATE inside a savepoint
#	  now exercises the same lock-only ITL stamp + ACTIVE TT install
#	  path as top-level callers.  Real cross-node block-wait remains
#	  forward-linked to spec-5.2 GES TX.
#
#	  L1   ClusterPair startup
#	  L2   savepoint SELECT FOR UPDATE on plain (no-remote-active) row
#	       — must succeed (barrier removed)
#	  L3   savepoint nested twice — innermost SELECT FOR UPDATE OK
#	  L4   no PANIC + clean shutdown
#
#	  Spec: spec-3.5-subtrans-cross-node-visibility.md (v0.3 FROZEN 2026-05-26)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_3_5_subxact_lock',
	extra_conf => [
		'autovacuum = off',
		'cluster.pcm_grd_max_entries = 0',
	]);
$pair->start_pair;

usleep(2_000_000);

# L1
is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
	'L1 node0 alive');

$pair->node0->safe_psql('postgres', q{
	DROP TABLE IF EXISTS spec_3_5_lock_smoke;
	CREATE TABLE spec_3_5_lock_smoke(id int PRIMARY KEY, v text);
	INSERT INTO spec_3_5_lock_smoke VALUES (1, 'lockable');
});


# L2: SELECT FOR UPDATE inside savepoint must succeed
my ($rc2, $out2, $err2) = $pair->node0->psql('postgres', q{
	BEGIN;
	SAVEPOINT sp_lock;
	SELECT count(*) FROM spec_3_5_lock_smoke WHERE id = 1 FOR UPDATE;
	RELEASE SAVEPOINT sp_lock;
	COMMIT;
});
is($rc2, 0, 'L2 SELECT FOR UPDATE inside savepoint succeeds (barrier removed)');
unlike($err2 // '', qr/0A000|FEATURE_NOT_SUPPORTED/,
	'L2 no spec-3.4d nest-level barrier ERROR');


# L3: nested savepoints — innermost SELECT FOR UPDATE OK
my ($rc3, $out3, $err3) = $pair->node0->psql('postgres', q{
	BEGIN;
	SAVEPOINT sp1;
	SAVEPOINT sp2;
	SAVEPOINT sp3;
	SELECT count(*) FROM spec_3_5_lock_smoke WHERE id = 1 FOR UPDATE;
	RELEASE SAVEPOINT sp3;
	RELEASE SAVEPOINT sp2;
	RELEASE SAVEPOINT sp1;
	COMMIT;
});
is($rc3, 0, 'L3 SELECT FOR UPDATE at depth=3 nested savepoint OK');


# L4: log scrape + clean shutdown
my $log0 = $pair->node0->logfile;
my $log1 = $pair->node1->logfile;

my $panic = `grep -c PANIC $log0 $log1 2>/dev/null | awk -F: '{s+=\$2}END{print s+0}'`;
chomp $panic;
is($panic, '0', 'L4 no PANIC in either node log');

$pair->stop_pair;

done_testing();
