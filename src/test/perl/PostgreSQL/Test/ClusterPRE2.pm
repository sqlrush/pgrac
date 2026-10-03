# Copyright (c) 2026, PostgreSQL Global Development Group
# Fresh shared fixture. No legacy backup/seed fallback is permitted.
package PostgreSQL::Test::ClusterPRE2;
use strict;
use warnings;
use parent 'PostgreSQL::Test::ClusterQuad';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use JSON::PP;
use IPC::Run qw(run timeout);
use File::Basename qw(dirname);
use Cwd qw(abs_path);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my $script_dir = abs_path(dirname(__FILE__) . '/../../../cluster_tap/pre2');
sub _now { return clock_gettime(CLOCK_MONOTONIC); }

sub _driver
{
	my ($self, $op, $args, $budget) = @_;
	$budget //= 120;
	die 'PRE2 operation deadline expired' unless $budget > 0;
	my $input = encode_json({version => 1, op => $op, layout => $self->{layout}, args => $args // {},
		budget_seconds => $budget});
	my ($out, $err) = ('', '');
	run($self->{driver_argv}, '<', \$input, '>', \$out, '2>', \$err, timeout($budget))
	  or die "PRE2 adapter $op failed: $err";
	my $reply = decode_json($out);
	die "invalid PRE2 adapter reply" unless ref($reply) eq 'HASH' && ($reply->{version} // 0) == 1;
	die "BLOCKED: $reply->{dependency}\n" if ($reply->{status} // '') eq 'BLOCKED' && $op eq 'describe';
	die "PRE2 adapter $op failed: " . ($reply->{reason} // 'missing OK')
	  unless ($reply->{status} // '') eq 'OK';
	return $reply;
}

sub new_cluster
{
	my ($class, $name, %opts) = @_;
	my $count = $opts{nodes} // 3;
	die "PRE2 fixture requires 2..4 nodes" unless $count =~ /^[234]$/;
	die "invalid fixture name" unless $name =~ /^[a-zA-Z0-9_]+$/;
	my $adapter = $opts{blackbox} ? undef : $ENV{PGRAC_PRE2_TEST_ADAPTER};
	die "invalid PRE2 adapter executable\n"
	  if defined($adapter) && !($adapter =~ m{^/} && -f $adapter && -x $adapter);
	my $self = bless {driver_argv => defined($adapter) ? [$adapter] :
		[$ENV{PYTHON} // 'python3', "$script_dir/blackbox.py"]}, $class;
	my $caps = $self->_driver('describe', {});
	my %caps = map { $_ => 1 } @{$caps->{capabilities} // []};
	for my $need ('fresh_init', 'shared_start', 'shared_stop', 'root_observation',
		'open_observation', 'shutdown_observation', @{$opts{required_operations} // []})
	{
		die "BLOCKED: PRE2 adapter missing $need\n" unless $caps{$need};
	}
	my $root = PostgreSQL::Test::Utils::tempdir();
	my (@nodes, @ic, @data);
	for my $i (0 .. $count - 1)
	{
		push @nodes, PostgreSQL::Test::Cluster->new("${name}_node$i");
		push @ic, PostgreSQL::Test::Cluster::get_free_port();
		push @data, PostgreSQL::Test::Cluster::get_free_port_range(2);
	}
	$self->{nodes} = \@nodes;
	$self->{cluster_name} = $name;
	$self->{ic_ports} = \@ic;
	$self->{data_ports} = \@data;
	$self->{pg_ports} = [map { $_->port } @nodes];
	$self->{shared_data_root} = "$root/shared";
	$self->{wal_threads_root} = "$root/wal";
	$self->{voting_disk_paths} = [map { "$root/vote$_" } 0 .. 2];
	$self->{layout} = {
		name => $name, root => $root, shared_data_dir => "$root/shared", wal_root => "$root/wal",
		voting_disks => $self->{voting_disk_paths}, extra_conf => $opts{extra_conf} // [],
		nodes => [map { my $i = $_; +{id => $i, data_dir => $nodes[$i]->data_dir,
			port => $nodes[$i]->port, host => $nodes[$i]->host,
			ic_port => $ic[$i], data_port => $data[$i], logfile => $nodes[$i]->logfile} } 0 .. $count - 1],
	};
	$self->_driver('fresh_init', {});
	for my $node (@nodes)
	{
		die 'fresh initializer did not create native control' unless -f $node->data_dir . '/global/pg_control';
		die 'fresh initializer created backup_label' if -e $node->data_dir . '/backup_label';
	}
	$self->{data_identity} = $self->_data_identity;
	return $self;
}

sub start_cluster
{
	my ($self) = @_;
	die 'fixture is already running' if $self->{running};
	$self->{normal_stop_proven} = 0;
	$self->{running} = 1; # A failed start still needs explicit cleanup.
	$self->_driver('shared_start', {});
	my $sysid;
	my $node_id = 0;
	for my $node (@{$self->{nodes}})
	{
		$node->_update_pid(-1);
		my $identity = $node->safe_psql('postgres',
			q{SELECT system_identifier::text || '|' || catalog_version_no FROM pg_control_system()}, timeout => 10);
		my ($current, $catalog) = split /\|/, $identity;
		die 'PRE2 format is not activated' unless ($catalog // '') eq '202609290';
		die 'invalid shared system identity' unless ($current // '') =~ /^[1-9][0-9]*$/;
		$sysid //= $current;
		die 'different system identifiers' unless $current eq $sysid;
		my $mode = $node->safe_psql('postgres', q{SELECT current_setting('cluster.enabled') || '|' ||
			current_setting('cluster.shared_catalog') || '|' || current_setting('cluster.shared_config') || '|' ||
			current_setting('cluster.controlfile_shared_authority') || '|' ||
			current_setting('cluster.merged_recovery')}, timeout => 10);
		die 'fixture is not configuration C' unless $mode eq 'on|on|on|on|on';
		my $id = $node->safe_psql('postgres', q{SHOW cluster.node_id}, timeout => 10);
		die 'configured member identity differs' unless $id eq "$node_id";
		$node_id++;
	}
	my $root = $self->_driver('root_observation', {});
	die 'missing selected current root observation' unless ($root->{system_identifier} // '') eq $sysid
	  && ($root->{root_digest} // '') =~ /^[0-9a-f]{64}$/ && ($root->{generation} // 0) > 0;
	die 'system identifier changed across restart' if defined($self->{system_identifier})
	  && $self->{system_identifier} ne $sysid;
	$self->{system_identifier} = $sysid;
	$self->{selected_root} = $root;
	$self->wait_open;
	return $self;
}

sub _data_identity
{
	my ($self) = @_;
	return join('|', map {
		my @s = stat($_);
		die "missing fixture directory: $_" unless @s;
		join(':', $_, @s[0,1]);
	} (map({ $_->data_dir } @{$self->{nodes}}), $self->shared_root, $self->wal_root));
}

sub wait_open
{
	my ($self) = @_;
	my $deadline = _now() + 60;
	my @writers;
	while (1)
	{
		@writers = ();
		my @samples;
		for my $i (0 .. $#{$self->{nodes}})
		{
			my $open = $self->_driver('open_observation', {node => $i}, $deadline - _now());
			next unless ($open->{phase} // '') eq 'OPEN';
			die 'OPEN has a different system identifier' unless
			  ($open->{system_identifier} // '') eq $self->{system_identifier};
			die 'OPEN has an incomplete member set' unless
			  join(',', @{$open->{members} // []}) eq join(',', 0 .. $#{$self->{nodes}});
			my $writer = $open->{writer};
			die 'OPEN lacks exact current writer identity' unless ref($writer) eq 'HASH'
			  && defined($writer->{node}) && $writer->{node} == $i
			  && ($writer->{boot} // '') ne '';
			for my $field ('thread', 'incarnation', 'wal_generation')
			{
				die "OPEN lacks $field" unless ($writer->{$field} // '') =~ /^[1-9][0-9]*$/;
			}
			push @writers, $writer;
			push @samples, $open;
		}
		if (@writers == @{$self->{nodes}})
		{
			my %threads;
			die 'OPEN reused one WAL thread on multiple nodes' if grep { $threads{$_->{thread}}++ } @writers;
			my $root = $self->_driver('root_observation', {}, $deadline - _now());
			die 'OPEN lacks a selected current root' unless
			  ($root->{system_identifier} // '') eq $self->{system_identifier}
			  && ($root->{root_digest} // '') =~ /^[0-9a-f]{64}$/ && ($root->{generation} // 0) > 0;
			my $same_cut = !grep {
				($_->{root_digest} // '') ne $root->{root_digest}
				|| ($_->{generation} // '') ne $root->{generation}
			} @samples;
			if ($same_cut)
			{
				die 'S16: OPEN deadline expired' if _now() >= $deadline;
				$self->{selected_root} = $root;
				last;
			}
		}
		die 'S16: not all members reached OPEN within 60 seconds' if _now() >= $deadline;
		sleep(0.1);
	}
	$self->{writers} = \@writers;
	return \@writers;
}

sub check_evidence
{
	my ($self, $check, @args) = @_;
	my $input = encode_json({check => $check, args => \@args});
	my ($out, $err) = ('', '');
	run([$ENV{PYTHON} // 'python3', "$script_dir/first_chain.py"], '<', \$input,
		'>', \$out, '2>', \$err, timeout(10)) or die "PRE2 $check assertion: $err";
	return 1;
}

sub observe { my ($self, $op, $args) = @_; return $self->_driver($op, $args // {}); }

sub restart_cluster
{
	my ($self) = @_;
	die 'restart requires a completed normal stop' if $self->{running} || !$self->{normal_stop_proven};
	die 'DATA/shared directories were replaced' unless $self->_data_identity eq $self->{data_identity};
	my @old = @{$self->{closed_writers}};
	my $generation = $self->{selected_root}{generation};
	$self->start_cluster;
	die 'ROOT generation did not advance on same-DATA restart'
	  unless $self->{selected_root}{generation} > $generation;
	for my $i (0 .. $#old)
	{
		my $new = $self->{writers}[$i];
		die 'restart reused a closed writer identity' unless $new->{thread} == $old[$i]{thread}
		  && $new->{boot} ne $old[$i]{boot} && $new->{wal_generation} > $old[$i]{wal_generation};
		for my $field ('thread', 'incarnation', 'wal_generation', 'boot')
		{
			die "restart did not consume the exact CLOSED predecessor: $field" unless
			  defined($new->{predecessor}{$field}) && $new->{predecessor}{$field} eq $old[$i]{$field};
		}
	}
	return $self;
}

sub start_quad { shift->start_cluster(@_); }
sub start_pair { shift->start_cluster(@_); }
sub stop_cluster
{
	my ($self, %opts) = @_;
	die 'fixture is not running' unless $self->{running};
	my $nodes = $opts{nodes} // [0 .. $#{$self->{nodes}}];
	my $exit = $self->_driver('shared_stop', {nodes => $nodes});
	$self->{running} = 0;
	$self->{nodes}[$_]->_update_pid(-1) for @$nodes;
	my %wanted = map { $_ => 1 } @$nodes;
	my @writers = grep { $wanted{$_->{node}} } @{$self->{writers} // []};
	my $receipt = $self->_driver('shutdown_observation', {nodes => $nodes, writers => \@writers});
	# Process exit is measured by the driver, never supplied by the product mapping.
	$receipt->{all_processes_exited} = $exit->{all_processes_exited};
	$self->check_evidence('shutdown', $receipt, $self->{system_identifier}, \@writers);
	$self->{closed_writers} = \@writers;
	$self->{normal_stop_proven} = @$nodes == @{$self->{nodes}};
	return $receipt;
}
sub stop_quad { shift->stop_cluster(@_); }
sub stop_pair { shift->stop_cluster(@_); }
sub shared_root { return $_[0]->{shared_data_root}; }
sub wal_root { return $_[0]->{wal_threads_root}; }
sub disks_csv { return join(',', @{$_[0]->{voting_disk_paths}}); }

# Existing hand-written TAP bodies can bind these names after replacing their
# old init/backup/seed block. Assertions and fault legs remain in the old file.
sub legacy_handles
{
	my ($self) = @_;
	return {nodes => $self->{nodes}, shared_root => $self->shared_root,
		wal_root => $self->wal_root, disks => $self->{voting_disk_paths},
		disks_csv => $self->disks_csv, ic_ports => $self->{ic_ports}, data_ports => $self->{data_ports}};
}

sub new_for_tap
{
	my ($class, $number, $name, %opts) = @_;
	my %counts = (337 => 2, 339 => 2, 346 => 2, 361 => 2, 362 => 4, 366 => 2, 371 => 2);
	die "unknown legacy shared TAP" unless exists $counts{$number};
	return $class->new_cluster($name, %opts, nodes => $counts{$number});
}

1;
