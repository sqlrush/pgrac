# Copyright (c) 2026, PostgreSQL Global Development Group
# Native administrative input and refusal checks, not shared admission tests.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use File::Spec;
use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('membership_command');
$node->init;
$node->append_conf('postgresql.conf', "cluster.enabled=off\nautovacuum=off\n");
$node->start;
my $registered = $node->safe_psql('postgres', q{
SELECT count(*) FROM pg_proc
WHERE proname='pg_cluster_membership_command' AND proargtypes='25 3802'::oidvector
});
is($registered, '1', 'the exact membership command is registered');
BAIL_OUT('native membership command is unavailable') unless $registered eq '1';

my $request = q!{"version":1,"operation_kind":"rejoin","target_node":3,
"guest_uuid":"11111111-2222-3333-4444-555555555555",
"expected_formation":9223372036854775809,"operation_generation":18446744073709551615,
"expected_old_incarnation":9223372036854775810,"reserved_new_incarnation":18446744073709551615}!;
my $json = JSON::PP->new;
my $original = $json->decode($request);
for my $action ('precheck', 'execute', 'status')
{
    my $reply = $json->decode($node->safe_psql('postgres',
        "SELECT pg_cluster_membership_command('$action', '$request'::jsonb)"));
    is_deeply([sort keys %$reply], [sort qw(version action status request reason)],
        "$action returns the exact response contract");
    is($reply->{version}, 1, 'response version is explicit');
    is($reply->{action}, $action, 'response retains the action');
    is($reply->{status}, 'blocked', 'missing authority never reports success or idle');
    is($reply->{reason}, 'cluster_disabled', 'disabled runtime has a concrete reason');
    is_deeply($reply->{request}, $original, 'the complete unsigned identity is preserved');
}
for my $empty ('NULL', q!'null'::jsonb!)
{
    my $reply = $json->decode($node->safe_psql('postgres',
        "SELECT pg_cluster_membership_command('status', $empty)"));
    is($reply->{status}, 'blocked', 'status without identity does not invent an idle operation');
    ok(!defined $reply->{request}, 'status preserves an absent request');
}

sub rejects
{
    my ($sql, $state, $label) = @_;
    my ($out, $err);
    my $rc = $node->psql('postgres', "\\set VERBOSITY verbose\n$sql", stdout => \$out, stderr => \$err);
    is($rc, 3, "$label rejects");
    like($err, qr/\Q$state\E:/, "$label has the expected SQLSTATE");
    like($err, qr/HINT:.*(?:superuser|format|integer|fields|UUID|request|action)/is,
        "$label explains how to correct the request") unless $label =~ /^PUBLIC/;
}
for my $field (sort keys %$original)
{
    rejects("SELECT pg_cluster_membership_command('execute', '$request'::jsonb - '$field')",
        '22023', "missing $field");
}
for my $change (
    [version => '2'], [version => 'true'], [operation_kind => '"join"'],
    [operation_kind => '"LEAVE"'], [target_node => '-1'], [target_node => '16'],
    [target_node => '1.5'], [target_node => 'false'],
    [guest_uuid => '"00000000-0000-0000-0000-000000000000"'],
    [guest_uuid => '"AAAAAAAA-2222-3333-4444-555555555555"'],
    [guest_uuid => '"11111111222233334444555555555555"'],
    [expected_formation => '0'], [expected_formation => 'null'],
    [expected_formation => '"91"'], [operation_generation => '18446744073709551616'],
    [expected_old_incarnation => '-1'], [reserved_new_incarnation => '9223372036854775810'],
    [operation_kind => '"leave"'])
{
    my ($field, $value) = @$change;
    rejects("SELECT pg_cluster_membership_command('precheck', jsonb_set('$request'::jsonb, '{$field}', '$value'::jsonb))",
        '22023', "invalid $field=$value");
}
for my $bad ('NULL', q!'null'::jsonb!, q!'[]'::jsonb!, q!'{}'::jsonb!,
    "'$request'::jsonb || '{\"force\":true}'::jsonb")
{
    rejects("SELECT pg_cluster_membership_command('execute', $bad)", '22023', 'invalid request shape');
}
rejects("SELECT pg_cluster_membership_command('force', '$request'::jsonb)", '22023', 'unknown action');
rejects("SELECT pg_cluster_membership_command(NULL, '$request'::jsonb)", '22023', 'NULL action');
for my $kind ('leave', 'remove')
{
    my $reply = $json->decode($node->safe_psql('postgres',
        "SELECT pg_cluster_membership_command('execute', '$request'::jsonb || '{\"operation_kind\":\"$kind\",\"reserved_new_incarnation\":0}'::jsonb)"));
    is($reply->{status}, 'blocked', "$kind also refuses before side effects");
    is($reply->{request}->{operation_kind}, $kind, 'kind is not substituted');
}

$node->safe_psql('postgres', 'CREATE ROLE member_reader');
rejects(q{SET ROLE member_reader; SELECT pg_cluster_membership_command('status', NULL)},
    '42501', 'PUBLIC has no command privilege');
$node->safe_psql('postgres', q{GRANT EXECUTE ON FUNCTION pg_cluster_membership_command(text,jsonb) TO member_reader});
rejects(q{SET ROLE member_reader; SELECT pg_cluster_membership_command('status', NULL)},
    '42501', 'the C superuser check survives an explicit function grant');

my $service = $node->basedir . '/member-service.conf';
my $manifest = $node->basedir . '/member-request.json';
append_to_file($service, "[p3b-native]\nhost=" . $node->host . "\nport=" . $node->port . "\ndbname=postgres\n");
append_to_file($manifest, $request);
local $ENV{PGSERVICEFILE} = $service;
my $client = File::Spec->catfile($FindBin::RealBin, qw(.. .. .. .. .. scripts deploy pre2 membership.py));
my ($client_out, $client_err);
my $ok = IPC::Run::run(['python3', $client, 'execute', '--service', 'p3b-native', '--request', $manifest],
    '>', \$client_out, '2>', \$client_err);
is($? >> 8, 2, 'the actual CLI receives native blocked, not a transport error');
is($client_err, '', 'the native CLI round trip has no error diagnostic');
my $reply = $json->decode($client_out);
is_deeply($reply->{request}, $original, 'actual psql round trip preserves uint64 request identity');
is($reply->{status}, 'blocked', 'actual client does not claim execution succeeded');
$node->stop;
$node->append_conf('postgresql.conf', "cluster.enabled=on\ncluster.allow_single_node=on\n");
$node->start;
is($node->safe_psql('postgres', 'SHOW cluster.enabled'), 'on', 'runtime gate is really enabled');
for my $action ('precheck', 'execute', 'status')
{
    my $reply = $json->decode($node->safe_psql('postgres',
        "SELECT pg_cluster_membership_command('$action', '$request'::jsonb)"));
    is($reply->{status}, 'blocked', "$action cannot bypass missing membership authority");
    is($reply->{reason}, 'membership_authority_unavailable', 'enabled runtime reports its real prerequisite');
    is_deeply($reply->{request}, $original, 'enabled refusal retains the exact identity');
}
$node->stop;
done_testing();
