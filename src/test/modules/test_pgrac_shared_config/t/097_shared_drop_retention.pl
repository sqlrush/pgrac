# Copyright (c) 2026, PostgreSQL Global Development Group
# Real storage-entry regression for repeated online DROP replay. The native
# fixture does not manufacture admission or a durable recovery-window close.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use File::Path qw(make_path);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('shared_drop_retention');
$node->init;
$node->append_conf('postgresql.conf', "cluster.enabled=off\nautovacuum=off\n");
$node->start;
$node->safe_psql('postgres', q{
CREATE FUNCTION force_oid_candidate(oid) RETURNS void
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_set_next_oid' LANGUAGE C STRICT;
CREATE FUNCTION shared_relfile_candidate(text,oid DEFAULT 0) RETURNS oid
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_shared_relfile_candidate' LANGUAGE C;
CREATE FUNCTION shared_drop_replay(text,oid) RETURNS void
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_shared_drop_replay' LANGUAGE C STRICT;
});
my $db = $node->safe_psql('postgres', q{SELECT oid FROM pg_database WHERE datname='postgres'});
my $root = PostgreSQL::Test::Utils::tempdir;
my $dir = "$root/base/$db";
make_path($dir);
my $old = 3001000000;
my @forks = ('', '_fsm', '_vm', '_init', '_space');
for my $fork (@forks)
{
    append_to_file("$dir/$old$fork", "old fixture $fork\n");
}
$node->safe_psql('postgres', "SELECT shared_drop_replay('$root',$old)");
ok(-f "$dir/$old" && -s "$dir/$old" == 0,
    'shared replay keeps zero-length MAIN until its window is durably closed');
$node->safe_psql('postgres', 'CHECKPOINT');
ok(-f "$dir/$old", 'local checkpoint cannot retire an open recovery-window reservation');
my $new = $node->safe_psql('postgres', qq{
SELECT force_oid_candidate($old);
SELECT shared_relfile_candidate('$root');
});
$new =~ s/^\s+|\s+$//g;
cmp_ok($new, '>', $old, 'allocator wrapping to the replayed number must skip it');
for my $fork (@forks)
{
    append_to_file("$dir/$new$fork", "new independently created fixture $fork\n");
}
$node->safe_psql('postgres', qq{
SELECT shared_drop_replay('$root',$old);
SELECT shared_drop_replay('$root',$old);
CHECKPOINT;
});
for my $fork (@forks)
{
    ok(-f "$dir/$new$fork" &&
        slurp_file("$dir/$new$fork") eq "new independently created fixture $fork\n",
        "repeating old DROP cannot delete or alter the new object fork '$fork'");
}
ok(-f "$dir/$old", 'repeat replay still does not enqueue an ordinary checkpoint unlink');
$node->stop('immediate');
$node->start;
ok(-f "$dir/$old", 'reservation survives recovery-process restart');
$node->safe_psql('postgres', "SELECT shared_drop_replay('$root',$old); CHECKPOINT");
ok(-f "$dir/$old", 'restarted storage consumer keeps the unresolved reservation');

# An absent MAIN cannot prove a safe reservation. Until the recovery owner
# supplies identity/closed-window qualification it must block, not treat an
# old DELETE as idempotent and expose the name for a later replay to delete.
my $absent = $old + 100;
my ($out, $err);
my $rc = $node->psql('postgres', "SELECT shared_drop_replay('$root',$absent)",
    stdout => \$out, stderr => \$err);
is($rc, 3, 'missing replay reservation fails closed');
like($err, qr/shared DROP replay requires a retained MAIN file/,
    'missing reservation has an explicit refusal');
ok(!-e "$dir/$absent", 'unqualified replay does not invent a new file identity');

my $io_failure = $old + 200;
append_to_file("$dir/$io_failure", "retained old data\n");
append_to_file("$dir/${io_failure}_vm", "untouched until MAIN is reserved\n");
chmod(0400, "$dir/$io_failure") == 1 or die "protect replay fixture: $!";
$rc = $node->psql('postgres', "SELECT shared_drop_replay('$root',$io_failure)",
    stdout => \$out, stderr => \$err);
if (-e "$dir/$io_failure")
{
    chmod(0600, "$dir/$io_failure") == 1 or die "restore replay fixture: $!";
}
is($rc, 3, 'replay retention I/O failure propagates to the recovery owner');
ok(-f "$dir/$io_failure" && slurp_file("$dir/$io_failure") eq "retained old data\n",
    'failed reservation does not remove the old MAIN');
ok(-f "$dir/${io_failure}_vm", 'failed MAIN reservation stops before auxiliary fork deletion');

my $aux_failure = $old + 300;
append_to_file("$dir/$aux_failure", "old main for auxiliary failure\n");
make_path("$dir/${aux_failure}_vm");
$rc = $node->psql('postgres', "SELECT shared_drop_replay('$root',$aux_failure)",
    stdout => \$out, stderr => \$err);
is($rc, 0, 'shared filesystem auxiliary unlink failure returns normally');
like($err, qr/WARNING:.*could not unlink .*_vm/,
    'shared filesystem reports the failed auxiliary unlink as WARNING');
ok(-f "$dir/$aux_failure" && -s "$dir/$aux_failure" == 0,
    'auxiliary failure preserves the qualified MAIN reservation');
ok(-d "$dir/${aux_failure}_vm", 'unremoved auxiliary name also prevents reuse');
$node->stop;
done_testing();
