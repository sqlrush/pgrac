
# spec-2.2 D15 -- ClusterPair: 2-instance harness for tier1 TAP tests.
#
# Encapsulates the boilerplate that 076 (and future Stage 2.X tests) need
# to spin up two cooperating pgrac instances with mutual interconnect:
#   - allocate 4 random free ports (2 PG + 2 IC)
#   - init both nodes
#   - append cluster.* GUCs (cluster.enabled = on, tier = tier1, node_id)
#     using append_conf (per spec-2.1 v1.0.1 L59 -- adjust_conf is
#     replace-only and would no-op for first-time GUC writes)
#   - write mutually-trusting pgrac.conf to both data dirs
#   - start_pair / stop_pair / heartbeat_seen helpers
#
# Spec authority: pgrac:specs/spec-2.2-* §4.3.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

package PostgreSQL::Test::ClusterPair;

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;


#-----------------------------------------------------------------------
# new_pair($class, $cluster_name, %opts)
#
#	Allocate two PG instances that share a pgrac cluster name.
#	Optional %opts:
#	  cluster_name_override  : write a different name into the second
#	                           node's pgrac.conf (used by 076 L6 to
#	                           force HELLO mismatch; default = same)
#	  extra_conf             : arrayref of extra GUC lines appended
#	                           to BOTH nodes' postgresql.conf
#	  quorum_voting_disks    : positive integer N — opt-in spec-2.6
#	                           strict-mode harness.  Pre-allocates N
#	                           shared voting-disk files (zero-filled,
#	                           64KB each) in a tempdir and configures
#	                           BOTH nodes with cluster.allow_single_
#	                           node=off + cluster.voting_disks=CSV.
#	                           Default unset = legacy compatibility
#	                           mode (allow_single_node=on, no voting
#	                           disks); preserves behaviour for the 8
#	                           pre-existing TAPs (075/076/079/080/
#	                           081/085/090/097-style negative).  Use
#	                           the ->voting_disk_paths accessor to
#	                           recover the file list (e.g., for fault
#	                           injection in future Hardening v0.5+).
#	  wal_threads_root       : boolean — opt-in spec-4.1 per-thread WAL
#	                           layout harness.  Creates one shared
#	                           tempdir; node N's WAL is relocated to
#	                           <root>/thread_<N+1> via initdb -X and
#	                           cluster.wal_threads_dir is set on both
#	                           nodes.  Use ->wal_threads_root to reach
#	                           the root (cross-thread pg_waldump etc.).
#	  shared_data            : boolean — opt-in spec-4.5a shared-data
#	                           backend harness.  Creates one shared data
#	                           root tempdir; BOTH nodes get
#	                           cluster.shared_storage_backend=cluster_fs
#	                           + cluster.shared_data_dir=<root> +
#	                           cluster.smgr_user_relations=on, so user
#	                           relations land in ONE shared file tree
#	                           (the same-DDL/same-relfilenode harness
#	                           premise; production naming is feature
#	                           #11).  Use ->shared_data_root to reach
#	                           the root (sentinel inspection etc.).
#-----------------------------------------------------------------------
sub new_pair
{
	my ($class, $cluster_name, %opts) = @_;

	# Allocate 4 distinct free ports.  get_free_port() reserves the port
	# until the test exits, so successive calls return distinct values.
	my $pg_port_0 = PostgreSQL::Test::Cluster::get_free_port();
	my $pg_port_1 = PostgreSQL::Test::Cluster::get_free_port();
	my $ic_port_0 = PostgreSQL::Test::Cluster::get_free_port();
	my $ic_port_1 = PostgreSQL::Test::Cluster::get_free_port();

	my $node0 =
	  PostgreSQL::Test::Cluster->new("${cluster_name}_node0", port => $pg_port_0);
	my $node1 =
	  PostgreSQL::Test::Cluster->new("${cluster_name}_node1", port => $pg_port_1);

	# spec-2.6 strict-mode opt-in: pre-allocate N shared voting-disk
	# files (zero-filled, 128 slots × 512B = 64KB each) in a tempdir
	# both postmasters can read/write.  Disks list is built once and
	# written into both nodes' postgresql.conf so the same file paths
	# back the same disk_index slots from both sides.
	my $voting_disks_csv;
	my @voting_disk_paths;
	if (defined $opts{quorum_voting_disks} && $opts{quorum_voting_disks} > 0)
	{
		my $disk_dir = PostgreSQL::Test::Utils::tempdir();
		for my $i (0 .. $opts{quorum_voting_disks} - 1)
		{
			my $path = "$disk_dir/disk$i";
			open(my $fh, '>', $path) or die "open $path: $!";
			binmode $fh;
			# PGRAC spec-5.13: two 512-byte regions per node (voting slot +
			# clean-leave marker), so 2 * 128 slots = 128 KiB.
			print $fh ("\0" x (2 * 128 * 512));
			close $fh;
			push @voting_disk_paths, $path;
		}
		$voting_disks_csv = join(',', @voting_disk_paths);
	}

	# spec-4.1 opt-in: shared per-thread WAL root.  One tempdir both
	# postmasters can reach; node N's WAL stream is relocated to
	# <root>/thread_<N+1> via initdb -X (the bootstrap-managed
	# relocation pgrac-init --wal-threads-dir performs in production)
	# and cluster.wal_threads_dir points the startup validator at the
	# root (cluster_wal_thread_init).
	my $wal_threads_root;
	if ($opts{wal_threads_root})
	{
		$wal_threads_root = PostgreSQL::Test::Utils::tempdir();
	}

	# spec-4.5a opt-in: shared data root.  One tempdir both postmasters
	# write user-relation blocks into through the cluster_fs shared_fs
	# backend (cluster_smgr passthrough).  The shared-root sentinel
	# (pgrac_shared.control) is created by the first node to attach and
	# records both node_ids as participants.
	my $shared_data_root;
	my $block_device_img;
	my $backend = $opts{storage_backend} // 'cluster_fs';   # spec-6.0a opt-in
	if ($opts{shared_data})
	{
		$shared_data_root = PostgreSQL::Test::Utils::tempdir();
		if ($backend eq 'block_device')
		{
			# spec-6.0a raw block_device backend over a loopback file image
			# (t/333 pattern); ONE image, both nodes point to it.
			$block_device_img = "$shared_data_root/pair_raw_device.img";
			open(my $fh, '>', $block_device_img) or die "open img: $!";
			truncate($fh, 512 * 1024 * 1024) or die "truncate img: $!";
			close($fh);
		}
	}

	my $wal_node_index = 0;
	for my $node ($node0, $node1)
	{
		if (defined $wal_threads_root)
		{
			my $thread_id = $wal_node_index + 1;
			$node->init(extra => [ '-X', "$wal_threads_root/thread_$thread_id" ]);
			$node->append_conf('postgresql.conf',
				"cluster.wal_threads_dir = '$wal_threads_root'\n");
		}
		else
		{
			$node->init;
		}
		$wal_node_index++;

		if (defined $shared_data_root)
		{
			if ($backend eq 'block_device')
			{
				my $q = $block_device_img;
				$q =~ s/'/''/g;
				$node->append_conf('postgresql.conf',
					"cluster.shared_storage_backend = block_device\n");
				$node->append_conf('postgresql.conf',
					"cluster.block_device_path = '$q'\n");
				$node->append_conf('postgresql.conf',
					"cluster.block_device_use_odirect = off\n");
				$node->append_conf('postgresql.conf',
					"cluster.storage_fence_driver = disabled\n");
			}
			else
			{
				$node->append_conf('postgresql.conf',
					"cluster.shared_storage_backend = cluster_fs\n");
				$node->append_conf('postgresql.conf',
					"cluster.shared_data_dir = '$shared_data_root'\n");
			}
			$node->append_conf('postgresql.conf',
				"cluster.smgr_user_relations = on\n");
		}

		# spec-2.2 §3.3 -- enable cluster + tier1.  per L59 first-time
		# GUC writes use append_conf (adjust_conf is replace-only).
		$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
		$node->append_conf('postgresql.conf',
			"cluster.interconnect_tier = tier1\n");

		# spec-2.6 — default to single-node compatibility mode for
		# the harness.  ClusterPair is used by 8 TAPs (075/076/079/
		# 080/081/085/090/096) that test interconnect / cssd / smgr
		# / quorum mechanics, NOT spec-2.1 strict-mode semantics.
		# Strict mode (allow_single_node=off + cluster.voting_disks
		# CSV) triggers Q7 validator FATAL + spawns qvotec polling
		# every 2s; the polling adds background catalog activity
		# that destabilises sensitive observability assertions
		# (e.g. 090 L9c HTAB shrink check).  Tests that genuinely
		# require strict mode opt in via quorum_voting_disks =>
		# N (096 happy path) or set the GUCs explicitly via
		# extra_conf (097 does this); ClusterPair stays neutral.
		if (defined $voting_disks_csv)
		{
			$node->append_conf('postgresql.conf',
				"cluster.allow_single_node = off\n");
			$node->append_conf('postgresql.conf',
				"cluster.voting_disks = '$voting_disks_csv'\n");
		}
		else
		{
			$node->append_conf('postgresql.conf',
				"cluster.allow_single_node = on\n");
		}

		# Keep shared_buffers small so 2 postmasters fit in CI runners
		# (R8 mitigation).
		$node->append_conf('postgresql.conf', "shared_buffers = 16MB\n");

		if ($opts{extra_conf})
		{
			for my $line (@{ $opts{extra_conf} })
			{
				$node->append_conf('postgresql.conf', "$line\n");
			}
		}
	}

	$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
	$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

	# Mutual pgrac.conf for node0 -- declares both peers in the same cluster.
	my $pgrac_conf_0 = <<EOC;
[cluster]
name = $cluster_name

[node.0]
interconnect_addr = 127.0.0.1:$ic_port_0

[node.1]
interconnect_addr = 127.0.0.1:$ic_port_1
EOC

	# node1's pgrac.conf -- same except optionally a different cluster_name
	# (L6 HELLO mismatch test).
	my $alt_name = $opts{cluster_name_override} // $cluster_name;
	my $pgrac_conf_1 = <<EOC;
[cluster]
name = $alt_name

[node.0]
interconnect_addr = 127.0.0.1:$ic_port_0

[node.1]
interconnect_addr = 127.0.0.1:$ic_port_1
EOC

	PostgreSQL::Test::Utils::append_to_file(
		$node0->data_dir . '/pgrac.conf', $pgrac_conf_0);
	PostgreSQL::Test::Utils::append_to_file(
		$node1->data_dir . '/pgrac.conf', $pgrac_conf_1);

	return bless {
		node0       => $node0,
		node1       => $node1,
		cluster_name => $cluster_name,
		pg_ports    => [ $pg_port_0, $pg_port_1 ],
		ic_ports    => [ $ic_port_0, $ic_port_1 ],
		voting_disk_paths => \@voting_disk_paths,
		wal_threads_root  => $wal_threads_root,
		shared_data_root  => $shared_data_root,
	}, $class;
}


sub start_pair
{
	my ($self, %opts) = @_;
	$self->{node0}->start(%opts);
	$self->{node1}->start(%opts);

	# spec-2.13 Hardening v1.0.2 diagnostic instrumentation
	# (L66-family HELLO bad magic root-cause investigation).
	# Dump configured IC ports + PG ports + cluster_name so failure-
	# postmortem can correlate getpeername() observed in C-side dump
	# with TAP-time configured ports.  Cheap printf-class diag.
	my $name = $self->{cluster_name} // '(unknown)';
	my $pg0  = $self->{node0}->port;
	my $pg1  = $self->{node1}->port;
	my $ic0  = $self->{ic_ports}[0] // -1;
	my $ic1  = $self->{ic_ports}[1] // -1;
	Test::More::note(
		"ClusterPair started: cluster_name='$name' "
		. "node0=pg:$pg0/ic:$ic0 node1=pg:$pg1/ic:$ic1");
	return;
}

sub stop_pair
{
	my ($self) = @_;
	$self->{node0}->stop if $self->{node0};
	$self->{node1}->stop if $self->{node1};
	return;
}

#-----------------------------------------------------------------------
# kill_node9($self, $idx)
#
#	spec-4.6 D0 — hard-kill one side of the pair (SIGKILL to the
#	postmaster) to simulate real node death for recovery-remaster
#	TAPs.  Children (the CSSD heartbeat producer included) exit on
#	postmaster death, so the SURVIVOR's deadband fires the DEAD
#	edge without any cooperative shutdown handshake.  kill9 clears
#	the node's _pid, so a later stop_pair skips the dead node.
#-----------------------------------------------------------------------
sub kill_node9
{
	my ($self, $idx) = @_;
	my $node = $idx == 0 ? $self->{node0} : $self->{node1};
	Test::More::note("ClusterPair kill_node9: SIGKILL node$idx postmaster");
	$node->kill9;
	return;
}

sub node0   { return $_[0]->{node0}; }
sub node1   { return $_[0]->{node1}; }
sub ic_port { return $_[0]->{ic_ports}[ $_[1] ]; }

# spec-4.1: shared per-thread WAL root (undef unless wal_threads_root => 1).
sub wal_threads_root { return $_[0]->{wal_threads_root}; }

# spec-4.5a: shared data root (undef unless shared_data => 1).
sub shared_data_root { return $_[0]->{shared_data_root}; }

#-----------------------------------------------------------------------
# voting_disk_paths($self)
#
#	Returns the list of pre-allocated voting-disk file paths (one
#	per element).  Empty list when ClusterPair was created without
#	the quorum_voting_disks opt.  Lets future fault-injection TAPs
#	(spec-2.6 Hardening v0.5+ — 096 L8 all-disk-fail) reach into the
#	files for write/chmod/unlink scenarios.
#-----------------------------------------------------------------------
sub voting_disk_paths
{
	my ($self) = @_;
	return @{ $self->{voting_disk_paths} // [] };
}


#-----------------------------------------------------------------------
# heartbeat_seen($self, $from, $to)
#
#	Returns 't' / 'f' as a string -- whether $from instance has sent
#	at least one heartbeat to $to peer (heartbeat_send_count > 0).
#	Caller wraps in a poll loop with timeout.
#-----------------------------------------------------------------------
sub heartbeat_seen
{
	my ($self, $from, $to) = @_;
	my $node = $from == 0 ? $self->{node0} : $self->{node1};
	return $node->safe_psql(
		'postgres',
		"SELECT heartbeat_send_count > 0 FROM pg_cluster_ic_peers WHERE node_id = $to"
	);
}


#-----------------------------------------------------------------------
# wait_for_peer_state($self, $from, $to, $expected_state, $timeout_s)
#
#	Polls $from's pg_cluster_ic_peers.state for $to until it matches
#	$expected_state or $timeout_s elapses.  Returns 1 on success, 0 on
#	timeout.  Default timeout = 10 s.
#-----------------------------------------------------------------------
sub wait_for_peer_state
{
	my ($self, $from, $to, $expected_state, $timeout_s) = @_;
	$timeout_s //= 10;
	my $node = $from == 0 ? $self->{node0} : $self->{node1};
	my $deadline = time + $timeout_s;
	my $last_state = '(never-queried)';
	while (time < $deadline)
	{
		my $state = $node->safe_psql('postgres',
			"SELECT state FROM pg_cluster_ic_peers WHERE node_id = $to");
		$last_state = $state // '(null)';
		return 1 if defined $state && $state eq $expected_state;
		select(undef, undef, undef, 0.25);
	}

	# spec-2.13 Hardening v1.0.2 diagnostic instrumentation
	# (L66-family HELLO bad magic root-cause investigation).
	# On timeout, dump full pg_cluster_ic_peers from BOTH sides + tail
	# of both postmaster logs so we can correlate IC state machine
	# with kernel-level connection state.
	Test::More::diag(
		"wait_for_peer_state TIMEOUT after ${timeout_s}s: from=node$from to=$to "
		. "expected='$expected_state' last_observed='$last_state'");
	for my $n (0, 1)
	{
		my $nm = $n == 0 ? $self->{node0} : $self->{node1};
		my $peers = $nm->safe_psql(
			'postgres',
			q{SELECT node_id || '|state=' || state || }
			. q{'|hb_send=' || heartbeat_send_count || }
			. q{'|hb_recv=' || heartbeat_recv_count || }
			. q{'|last_err=' || COALESCE(last_error_msg, '-') }
			. q{FROM pg_cluster_ic_peers ORDER BY node_id});
		Test::More::diag("  node$n pg_cluster_ic_peers:\n    "
			. ($peers // '(query-failed)'));

		# Tail postmaster log (last 40 lines).  Path via $node->logfile.
		my $logfile = $nm->logfile;
		if ($logfile && -r $logfile)
		{
			my @lines;
			if (open my $fh, '<', $logfile)
			{
				while (<$fh>) { push @lines, $_; shift @lines if @lines > 40; }
				close $fh;
			}
			Test::More::diag("  node$n postmaster log tail:\n"
				. join('', map { "    $_" } @lines));
		}
	}
	return 0;
}


1;
