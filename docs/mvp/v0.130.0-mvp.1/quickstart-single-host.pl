#!/usr/bin/env perl
# Linux single-host usage example for v0.130.0-mvp.1; not a production installer.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use File::Path qw(make_path);
use Fcntl qw(O_CREAT O_EXCL O_WRONLY);
use IPC::Run qw(run);
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use Time::HiRes qw(time usleep);

my $root = $ENV{PGRAC_QS_ROOT} or die "PGRAC_QS_ROOT is required\n";
die "root must be an absolute private path\n"
  unless $root =~ m{\A/[A-Za-z0-9_./-]+\z} && -d $root && !-l $root;
die "Linux and a non-root account are required\n" unless $^O eq 'linux' && $< != 0;
die "offline loop startup must be selected\n"
  unless ($ENV{PGRAC_STAGE8_HAPPY_PATH_ONLY} // '') eq '1'
  && !($ENV{PGRAC_TEST_TWO_STAGE_VOTING_LOOP} // '');
sysopen(my $once, "$root/INITIALIZED", O_WRONLY | O_CREAT | O_EXCL, 0600)
  or die "use a new example directory; this one was already initialized: $!\n";
close($once) or die $!;
my $quad = PostgreSQL::Test::ClusterQuad->new_quad(
    'quickstart', quorum_voting_disks => 3,
    shared_data => 1, shared_system_identifier => 1,
    shared_system_identifier_seed_sql =>
      'CREATE TABLE quickstart_demo(id integer PRIMARY KEY, value integer NOT NULL);',
    extra_conf => [
        'fsync = on', 'full_page_writes = on', 'synchronous_commit = on',
        'autovacuum = off', 'shared_buffers = 128MB',
        'max_connections = 32', 'max_parallel_workers_per_gather = 0',
        'cluster.clean_leave_enabled = on',
        'cluster.read_scache = on', 'cluster.online_join = on',
        'cluster.quorum_poll_interval_ms = 2000',
        'cluster.write_fence_lease_ms = 60000',
        'cluster.join_convergence_timeout_ms = 30000',
        'cluster.xid_striping = on',
        'cluster.crossnode_runtime_visibility = on',
        'cluster.page_scn_shortcut = on', 'cluster.past_image = on',
        'cluster.crossnode_write_write = on',
        'cluster.undo_gcs_coherence = on',
        'cluster.crossnode_cr_data_plane = on',
        'cluster.gcs_reply_timeout_ms = 3000',
        'cluster.gcs_block_retransmit_max_retries = 8',
        'cluster.cssd_heartbeat_interval_ms = 2000',
        'cluster.cssd_dead_deadband_factor = 10',
    ]);
note('Initializing four instances');
$quad->start_quad;
for my $from (0 .. 3) {
    for my $to (0 .. 3) {
        next if $from == $to;
        $quad->wait_for_peer_state($from, $to, 'connected', 45)
          or die "node$from cannot reach node$to\n";
    }
}
make_path($quad->shared_data_root . '/pg_undo');
for my $node ($quad->nodes) {
    $node->poll_query_until('postgres',
        'SELECT in_quorum FROM pg_cluster_quorum_state', 't')
      or die "quorum did not become ready\n";
    my ($rc, $out, $err) = $node->psql('postgres',
        'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL', timeout => 45);
    die "activation failed: $err\n" unless defined($rc) &&
      ($rc == 0 || $err =~ /RF_DEFERRED|CONDITION_NOT_YET_MET/);
}
for my $round (1 .. 2) {
    my $deadline = time() + 120;
    my $ready = 0;
    while (time() < $deadline) {
        my ($rc, $out, $err) = $quad->node0->psql('postgres',
            'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL', timeout => 45);
        if (defined($rc) && $rc == 0) { $ready = 1; last; }
        die "activation failed: $err\n" unless defined($rc) &&
          $err =~ /RF_DEFERRED|CONDITION_NOT_YET_MET|activation request was refused/;
        usleep(200_000);
    }
    die "activation round $round did not complete\n" unless $ready;
}
$quad->node0->safe_psql('postgres',
    'INSERT INTO quickstart_demo VALUES (1,0);');
for my $i (0 .. 3) {
    is($quad->node($i)->safe_psql('postgres',
        'UPDATE quickstart_demo SET value=value+1 WHERE id=1 RETURNING value;'),
       '' . ($i + 1), "node$i sees and updates the shared row") or die "SQL check failed\n";
}
open(my $env, '>', "$root/connect.env") or die $!;
print {$env} "export PGHOST='" . $quad->node0->host . "'\n";
print {$env} "export PGDATABASE=postgres\n";
for my $i (0 .. 3) {
    print {$env} "export PGPORT_$i=" . $quad->node($i)->port . "\n";
    print {$env} "export PGDATA_$i='" . $quad->node($i)->data_dir . "'\n";
    is($quad->node($i)->safe_psql('postgres',
        'SELECT value FROM quickstart_demo WHERE id=1;'), '4', "node$i reads value 4")
      or die "readback failed\n";
}
close($env) or die $!;
open(my $ready, '>', "$root/READY") or die $!;
close($ready) or die $!;
note("READY: source $root/connect.env");
usleep(200_000) until -e "$root/STOP";
my @nodes = $quad->nodes;
my @offsets = map { -s $_->logfile } @nodes;
for my $node (@nodes) {
    system('pg_ctl', '-D', $node->data_dir, '-m', 'fast', '-W', 'stop') == 0
      or die "normal shutdown request failed\n";
}
my $stop_deadline = time() + 600;
while (grep { -f $_->data_dir . '/postmaster.pid' } @nodes) {
    die "normal shutdown did not complete; retain this directory\n"
      if time() >= $stop_deadline;
    usleep(200_000);
}
for my $i (0 .. 3) {
    my ($control, $err) = ('', '');
    run(['pg_controldata', $nodes[$i]->data_dir], '>', \$control, '2>', \$err)
      or die "pg_controldata failed: $err\n";
    die "node$i was not shut down cleanly\n"
      unless $control =~ /^Database cluster state:\s+shut down\s*$/m;
    my $log = substr(slurp_file($nodes[$i]->logfile), $offsets[$i]);
    die "node$i has an abnormal shutdown log\n"
      if $log =~ /(?:FATAL:|PANIC:|abnormal database system shutdown)/;
    die "node$i normal shutdown protocol did not close\n"
      unless $log =~ /cluster normal-stop: protocol closed after shutdown checkpoint/
      && $log =~ /database system is shut down/;
    $nodes[$i]->_update_pid(0);
}
# Only after four durable clean stops may the helper detach its owned loops.
$quad->stop_quad;
open(my $stopped, '>', "$root/STOPPED") or die $!;
close($stopped) or die $!;
done_testing();
