#!/usr/bin/env perl
# New demo seed only; uses the frozen public initializer.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use JSON::PP;
use Fcntl qw(O_CREAT O_EXCL O_WRONLY);
use File::Path qw(make_path);
use PostgreSQL::Test::ClusterQuad;
use Test::More;

die "expected non-root demo account\n" unless $< == 10001;
sysopen(my $claim, '/demo/INITIALIZED', O_WRONLY | O_CREAT | O_EXCL, 0600)
  or die "seed already claimed; preserve this directory\n";
close($claim) or die $!;
open(my $pf, '<', '/demo/password') or die "password file missing\n";
my $password = <$pf>;
close($pf);
chomp($password);
die "invalid password format\n" unless $password =~ /\A[0-9a-f]{64}\z/;
open(my $settings, '<', '/demo/settings.json') or die $!;
my $config = decode_json(do { local $/; <$settings> });
close($settings);
my $base = $config->{port_base};
die "invalid port base\n" unless defined $base && $base =~ /^\d+$/ && $base >= 1024 && $base <= 65000;

my $quad;
eval {
    $quad = PostgreSQL::Test::ClusterQuad->new_quad('pgrac_demo',
        quorum_voting_disks => 3, shared_data => 1, shared_system_identifier => 1,
        shared_system_identifier_seed_sql =>
          "CREATE TABLE demo_account(id integer PRIMARY KEY, value bigint NOT NULL, pad text NOT NULL) WITH (fillfactor=70); " .
          "CREATE TABLE demo_smoke(id integer PRIMARY KEY, value bigint NOT NULL); " .
          "ALTER ROLE pgrac PASSWORD '$password';",
        extra_conf => [
            'fsync = on', 'full_page_writes = on', 'synchronous_commit = on',
            'autovacuum = off', 'shared_buffers = 128MB', 'max_connections = 32',
            'max_parallel_workers_per_gather = 0', 'log_statement = none',
            'log_min_error_statement = panic', 'restart_after_crash = off',
            'cluster.clean_leave_enabled = on', 'cluster.read_scache = on',
            'cluster.online_join = on', 'cluster.quorum_poll_interval_ms = 2000',
            'cluster.write_fence_lease_ms = 60000', 'cluster.join_convergence_timeout_ms = 30000',
            'cluster.xid_striping = on', 'cluster.crossnode_runtime_visibility = on',
            'cluster.page_scn_shortcut = on', 'cluster.past_image = on',
            'cluster.crossnode_write_write = on', 'cluster.undo_gcs_coherence = on',
            'cluster.crossnode_cr_data_plane = on', 'cluster.gcs_reply_timeout_ms = 3000',
            'cluster.gcs_block_retransmit_max_retries = 8',
            'cluster.cssd_heartbeat_interval_ms = 2000', 'cluster.cssd_dead_deadband_factor = 10',
            'cluster.undo_buffers = 8192', 'cluster.pcm_grd_max_entries = 32768',
            'cluster.ges_dedup_max_entries = 16384', 'cluster.gcs_block_dedup_max_entries = 16384',
        ]);
};
if ($@) {
    my $error = "$@";
    $error =~ s/\Q$password\E/[REDACTED]/g;
    die $error;
}
make_path('/demo/server-log', '/demo/sockets', $quad->shared_data_root . '/pg_undo');
my $peers = "[cluster]\nname = pgrac_demo\n\n";
for my $i (0 .. 3) {
    $peers .= "[node.$i]\ninterconnect_addr = 127.0.0.1:" . ($base + 100 + $i) .
      "\ndata_addr = 127.0.0.1:" . ($base + 200 + 2 * $i) . "\n\n";
}
my @nodes;
for my $i (0 .. 3) {
    my $node = $quad->node($i);
    my $dir = $node->data_dir;
    $node->append_conf('postgresql.conf',
      "port = " . ($base + $i) . "\nlisten_addresses = '127.0.0.1'\n" .
      "unix_socket_directories = '/demo/sockets'\nunix_socket_permissions = 0700\n" .
      "cluster_name = 'pgrac_demo_node$i'\n" .
      "cluster.voting_disks = '/dev/pgrac-vote0,/dev/pgrac-vote1,/dev/pgrac-vote2'\n" .
      "cluster.voting_disk_size_bytes = 525824\n");
    open(my $pc, '>', "$dir/pgrac.conf") or die $!;
    print {$pc} $peers;
    close($pc) or die $!;
    open(my $hba, '>', "$dir/pg_hba.conf") or die $!;
    print {$hba} "local all all peer\nhost all all 127.0.0.1/32 scram-sha-256\n";
    close($hba) or die $!;
    push @nodes, {id => $i, pgdata => $dir, port => $base + $i,
                  log => "/demo/server-log/node$i.log"};
}
open(my $pass, '>', '/demo/client.pgpass') or die $!;
print {$pass} "127.0.0.1:*:postgres:pgrac:$password\n";
close($pass) or die $!;
chmod(0600, '/demo/client.pgpass') or die $!;
open(my $out, '>', '/demo/bootstrap.json') or die $!;
print {$out} JSON::PP->new->canonical->pretty->encode({
    nodes => \@nodes, shared_data => $quad->shared_data_root,
    voting => [$quad->voting_disk_paths], source_revision =>
      '83d8c5a002643581f153058b7a766afe1ffa1eb0'});
close($out) or die $!;
ok(1, 'homogeneous demo seed prepared; no four-node workload started');
done_testing();
