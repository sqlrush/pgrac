
# spec-5.19 D1 -- ClusterQuad: 4-instance harness for the Stage 5 integrated
# acceptance (4-node reconfig fault matrix + multi-node write-path workloads).
#
# Encapsulates the boilerplate that the Stage 5 integrated-acceptance TAPs
# (t/32x reconfig matrix / HW-extend workload / production-bench-subset) need
# to spin up four cooperating pgrac instances with mutual interconnect:
#   - allocate 8 random free ports (4 PG + 4 IC)
#   - init all four nodes
#   - append cluster.* GUCs (cluster.enabled = on, tier = tier1, node_id 0..3)
#   - write mutually-trusting pgrac.conf to all four data dirs
#   - start_quad / stop_quad helpers
#
# Mirrors PostgreSQL::Test::ClusterTriple (spec-2.36 D15) and
# PostgreSQL::Test::ClusterPair (spec-2.2 D15) with an extra node slot.  It is
# the substrate for the first 4-node reconfig fault matrix, and is reused by
# spec-5.20 (Hang Manager acceptance) and spec-5.21 (Stage 5 beta close-out).
#
# Reconfig-leg API (used by the fault-matrix TAPs):
#   - kill_node9($i)   : SIGKILL the postmaster (fail-stop / spec-5.14 leg).
#   - leave_node($i)   : cooperative clean-leave (spec-5.13) -- runs
#                        pg_cluster_clean_leave_request() on node $i.
#   - stop_node($i)    : graceful stop (node goes ABSENT without a marker).
#   - join_node($i)    : peer-restart rejoin (spec-5.15) -- starts a node that
#                        was previously down/absent so it re-enters the live
#                        membership via the coordinator two-phase epoch
#                        protocol.  (There is no pg_cluster_join() UDF: 5.15
#                        join is driven by the node coming back online.)
#   - remove_node($coord_i, $target_id)
#                      : permanent removal (spec-5.18) -- runs
#                        pg_cluster_remove_node($target_id) on a surviving
#                        coordinator node.
#
# NOTE: the join-remaster (spec-5.16) leg of the fault matrix is gated on
# spec-5.16 landing on origin/main;  this harness exposes the substrate but
# does not itself depend on 5.16.
#
# Spec authority: pgrac:specs/spec-5.19-stage5-integrated-acceptance.md §1.2 D1.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

package PostgreSQL::Test::ClusterQuad;

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;


our $NODES = 4;



# Relocate an init_from_backup node's copied pg_wal into its shared WAL thread
# directory, matching the layout initdb -X produces for the seed node.
sub _relocate_backup_pg_wal
{
	my ($node, $wal_threads_root, $thread_id) = @_;
	my $pgwal = $node->data_dir . '/pg_wal';
	my $wal_thread = "$wal_threads_root/thread_$thread_id";

	mkdir $wal_thread or die "mkdir $wal_thread: $!";
	opendir(my $dh, $pgwal) or die "opendir $pgwal: $!";
	for my $e (readdir $dh)
	{
		next if $e eq '.' || $e eq '..';
		rename("$pgwal/$e", "$wal_thread/$e")
		  or die "rename $pgwal/$e -> $wal_thread/$e: $!";
	}
	closedir $dh;
	rmdir $pgwal or die "rmdir $pgwal: $!";
	symlink($wal_thread, $pgwal) or die "symlink $pgwal -> $wal_thread: $!";
	return;
}

#-----------------------------------------------------------------------
# new_quad($class, $cluster_name, %opts)
#
#	Allocate four PG instances that share a pgrac cluster name.
#	Optional %opts (mirror ClusterTriple):
#	  extra_conf          : arrayref of extra GUC lines appended to ALL
#	                        nodes' postgresql.conf
#	  quorum_voting_disks : N  -> strict mode, pre-allocate N shared
#	                        voting-disk files (QVOTEC reaches quorum OK)
#	  wal_threads_root    : 1  -> shared per-thread WAL root
#	  shared_data         : 1  -> shared data root (cluster_fs backend)
#	  shared_catalog      : 1  -> t/337-style shared-catalog formation;
#	                        implies shared_data + wal_threads_root
#-----------------------------------------------------------------------
sub new_quad
{
	my ($class, $cluster_name, %opts) = @_;

	# Allocate 8 distinct free ports (4 PG + 4 IC).
	my @pg_ports;
	my @ic_ports;
	for (0 .. $NODES - 1)
	{
		push @pg_ports, PostgreSQL::Test::Cluster::get_free_port();
	}
	for (0 .. $NODES - 1)
	{
		push @ic_ports, PostgreSQL::Test::Cluster::get_free_port();
	}

	my @nodes;
	for my $i (0 .. $NODES - 1)
	{
		push @nodes,
		  PostgreSQL::Test::Cluster->new(
			"${cluster_name}_node$i", port => $pg_ports[$i]);
	}

	if ($opts{shared_catalog})
	{
		$opts{shared_data} = 1;
		$opts{wal_threads_root} = 1;
	}

	# spec-4.6: strict-mode opt-in (mirror ClusterTriple) -- pre-allocate N
	# shared voting-disk files so QVOTEC reaches quorum_state=OK and the GES
	# inbound validation (in_quorum, check 4) accepts cross-node traffic.
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
			# PGRAC spec-5.13/5.18: two 512-byte regions per node (voting slot
			# + clean-leave / removal marker), 2 * 128 slots = 128 KiB.
			print $fh ("\0" x (2 * 128 * 512));
			close $fh;
			push @voting_disk_paths, $path;
		}
		$voting_disks_csv = join(',', @voting_disk_paths);
	}

	# spec-4.1 opt-in: shared per-thread WAL root (mirror ClusterTriple).
	my $wal_threads_root;
	if ($opts{wal_threads_root})
	{
		$wal_threads_root = PostgreSQL::Test::Utils::tempdir();
	}

	# spec-4.5a opt-in: shared data root (mirror ClusterTriple).  One tempdir
	# all four postmasters write user-relation blocks into through the
	# cluster_fs shared_fs backend (cluster_smgr passthrough).
	my $shared_data_root;
	if ($opts{shared_data})
	{
		$shared_data_root = PostgreSQL::Test::Utils::tempdir();
		if ($opts{shared_catalog})
		{
			mkdir "$shared_data_root/global"
			  or die "mkdir $shared_data_root/global: $!";
		}
	}

	if ($opts{shared_catalog})
	{
		my $sc_common = <<EOC;
shared_buffers = 16MB
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared_data_root'
cluster.smgr_user_relations = on
cluster.controlfile_shared_authority = on
cluster.shared_catalog = on
cluster.merged_recovery = on
EOC

		$nodes[0]->init(allows_streaming => 1,
			extra => [ '-X', "$wal_threads_root/thread_1" ]);
		$nodes[0]->start;
		$nodes[0]->backup('clusterquad_scb');
		$nodes[0]->stop;

		for my $i (1 .. $NODES - 1)
		{
			$nodes[$i]->init_from_backup($nodes[0], 'clusterquad_scb');
			_relocate_backup_pg_wal($nodes[$i], $wal_threads_root, $i + 1);
		}

		# Seed the shared catalog/controlfile/OID authorities in a single-node era,
		# then append the real cluster config below.  Last GUC value wins.
		$nodes[0]->append_conf('postgresql.conf', $sc_common);
		$nodes[0]->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC
		$nodes[0]->start;
		die "shared_catalog seed did not create catalog authority"
		  unless -e "$shared_data_root/global/pgrac_catalog_authority";
		$nodes[0]->stop;
	}
	else
	{
		my $wal_node_index = 0;
		for my $node (@nodes)
		{
			if (defined $wal_threads_root)
			{
				my $thread_id = $wal_node_index + 1;
				$node->init(extra => [ '-X', "$wal_threads_root/thread_$thread_id" ]);
			}
			else
			{
				$node->init;
			}
			$wal_node_index++;
		}
	}

	for my $node (@nodes)
	{
		if (defined $wal_threads_root)
		{
			$node->append_conf('postgresql.conf',
				"cluster.wal_threads_dir = '$wal_threads_root'\n");
		}

		if (defined $shared_data_root)
		{
			$node->append_conf('postgresql.conf',
				"cluster.shared_storage_backend = cluster_fs\n");
			$node->append_conf('postgresql.conf',
				"cluster.shared_data_dir = '$shared_data_root'\n");
			$node->append_conf('postgresql.conf',
				"cluster.smgr_user_relations = on\n");
			if ($opts{shared_catalog})
			{
				$node->append_conf('postgresql.conf',
					"cluster.controlfile_shared_authority = on\n");
				$node->append_conf('postgresql.conf',
					"cluster.shared_catalog = on\n");
				$node->append_conf('postgresql.conf',
					"cluster.merged_recovery = on\n");
			}
		}

		# Enable cluster + tier1, same baseline as ClusterTriple (spec-2.2).
		$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
		$node->append_conf('postgresql.conf',
			"cluster.interconnect_tier = tier1\n");
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

		# Keep shared_buffers small so 4 postmasters fit in CI runners.
		$node->append_conf('postgresql.conf', "shared_buffers = 16MB\n");

		if ($opts{extra_conf})
		{
			for my $line (@{ $opts{extra_conf} })
			{
				$node->append_conf('postgresql.conf', "$line\n");
			}
		}
	}

	# Per-node identity.
	for my $i (0 .. $NODES - 1)
	{
		$nodes[$i]->append_conf('postgresql.conf', "cluster.node_id = $i\n");
	}

	# Build the shared pgrac.conf body declaring all four peers.
	my $peers_block = "";
	for my $i (0 .. $NODES - 1)
	{
		$peers_block .=
		  "[node.$i]\ninterconnect_addr = 127.0.0.1:$ic_ports[$i]\n\n";
	}

	my $pgrac_conf_body = <<EOC;
[cluster]
name = $cluster_name

$peers_block
EOC

	for my $node (@nodes)
	{
		PostgreSQL::Test::Utils::append_to_file(
			$node->data_dir . '/pgrac.conf', $pgrac_conf_body);
	}

	return bless {
		nodes             => \@nodes,
		cluster_name      => $cluster_name,
		pg_ports          => \@pg_ports,
		ic_ports          => \@ic_ports,
		voting_disk_paths => \@voting_disk_paths,
		wal_threads_root  => $wal_threads_root,
		shared_data_root  => $shared_data_root,
	}, $class;
}


sub start_quad
{
	my ($self, %opts) = @_;
	for my $node (@{ $self->{nodes} })
	{
		$node->start(%opts);
	}

	# Diagnostic note (mirrors ClusterTriple/ClusterPair pattern).
	my $name = $self->{cluster_name} // '(unknown)';
	my $msg = "ClusterQuad started: cluster_name='$name'";
	for my $i (0 .. $NODES - 1)
	{
		my $pg = $self->{nodes}[$i]->port;
		my $ic = $self->{ic_ports}[$i] // -1;
		$msg .= " node$i=pg:$pg/ic:$ic";
	}
	Test::More::note($msg);
	return;
}


sub stop_quad
{
	my ($self) = @_;
	for my $node (@{ $self->{nodes} })
	{
		$node->stop if $node;
	}
	return;
}


#-----------------------------------------------------------------------
# kill_node9($self, $idx)
#
#	spec-5.14 fail-stop leg -- hard-kill one node of the quad (SIGKILL to
#	the postmaster).  Children (the CSSD heartbeat producer included) exit
#	on postmaster death, so the survivors' deadband fires the DEAD edge
#	without any cooperative shutdown handshake.  kill9 clears the node's
#	_pid, so a later stop_quad skips the dead node.
#-----------------------------------------------------------------------
sub kill_node9
{
	my ($self, $idx) = @_;
	Test::More::note("ClusterQuad kill_node9: SIGKILL node$idx postmaster");
	$self->{nodes}[$idx]->kill9;
	return;
}


#-----------------------------------------------------------------------
# stop_node($self, $idx)
#
#	Graceful stop of one node (postmaster fast shutdown).  The node leaves
#	the live membership as ABSENT without writing a clean-leave marker --
#	the substrate for a later join_node() peer-restart rejoin (spec-5.15).
#-----------------------------------------------------------------------
sub stop_node
{
	my ($self, $idx) = @_;
	Test::More::note("ClusterQuad stop_node: graceful stop node$idx");
	$self->{nodes}[$idx]->stop;
	return;
}


#-----------------------------------------------------------------------
# leave_node($self, $idx)
#
#	spec-5.13 cooperative clean-leave leg -- ask node $idx to leave the
#	cluster.  Returns the pg_cluster_clean_leave_request() result string
#	('accepted' on success;  a 'rejected:...' reason otherwise).  The
#	caller polls pg_cluster_clean_leave_state.phase / the survivors'
#	pg_cluster_reconfig_state for convergence.
#-----------------------------------------------------------------------
sub leave_node
{
	my ($self, $idx) = @_;
	Test::More::note("ClusterQuad leave_node: clean-leave request on node$idx");
	return $self->{nodes}[$idx]->safe_psql('postgres',
		'SELECT pg_cluster_clean_leave_request()');
}


#-----------------------------------------------------------------------
# join_node($self, $idx, %opts)
#
#	spec-5.15 online join/rejoin leg -- start node $idx (which must be
#	currently down: previously kill_node9'd, stop_node'd, or never
#	started) so it re-enters the live membership via the coordinator
#	two-phase epoch protocol.  There is no pg_cluster_join() UDF;  5.15
#	join is driven by a declared node coming back online.
#-----------------------------------------------------------------------
sub join_node
{
	my ($self, $idx, %opts) = @_;
	Test::More::note("ClusterQuad join_node: peer-restart rejoin node$idx");
	$self->{nodes}[$idx]->start(%opts);
	return;
}


#-----------------------------------------------------------------------
# remove_node($self, $coord_idx, $target_id)
#
#	spec-5.18 permanent-removal leg -- ask a surviving coordinator node
#	($coord_idx) to permanently remove node $target_id from the cluster
#	(fence + cluster-wide cleanup).  Returns the pg_cluster_remove_node()
#	result.  The target must already be down/fenced.
#-----------------------------------------------------------------------
sub remove_node
{
	my ($self, $coord_idx, $target_id) = @_;
	Test::More::note(
		"ClusterQuad remove_node: node$coord_idx removes node $target_id");
	return $self->{nodes}[$coord_idx]->safe_psql('postgres',
		"SELECT pg_cluster_remove_node($target_id)");
}


sub node      { return $_[0]->{nodes}[ $_[1] ]; }
sub node0     { return $_[0]->{nodes}[0]; }
sub node1     { return $_[0]->{nodes}[1]; }
sub node2     { return $_[0]->{nodes}[2]; }
sub node3     { return $_[0]->{nodes}[3]; }
sub nodes     { return @{ $_[0]->{nodes} }; }
sub ic_port   { return $_[0]->{ic_ports}[ $_[1] ]; }
sub pg_port   { return $_[0]->{pg_ports}[ $_[1] ]; }
sub cluster_name { return $_[0]->{cluster_name}; }

# spec-4.1: shared per-thread WAL root (undef unless wal_threads_root => 1).
sub wal_threads_root { return $_[0]->{wal_threads_root}; }

# spec-4.5a: shared data root (undef unless shared_data => 1).
sub shared_data_root { return $_[0]->{shared_data_root}; }

# spec-2.6: pre-allocated voting-disk file paths (empty without strict mode).
sub voting_disk_paths { return @{ $_[0]->{voting_disk_paths} // [] }; }


#-----------------------------------------------------------------------
# wait_for_peer_state($self, $from, $to, $expected_state, $timeout_s)
#
#	Polls $from's pg_cluster_ic_peers.state for $to until it matches
#	$expected_state or $timeout_s elapses.  Returns 1 on success, 0 on
#	timeout.  Mirrors ClusterTriple::wait_for_peer_state for the 4-node
#	reconfig matrix.
#-----------------------------------------------------------------------
sub wait_for_peer_state
{
	my ($self, $from, $to, $expected_state, $timeout_s) = @_;
	$timeout_s //= 30;
	my $node = $self->{nodes}[$from];
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
	Test::More::diag(
		"ClusterQuad wait_for_peer_state TIMEOUT after ${timeout_s}s: "
		. "from=node$from to=$to expected='$expected_state' "
		. "last_observed='$last_state'");
	return 0;
}


#-----------------------------------------------------------------------
# wait_for_membership_count($self, $from, $expected_members, $timeout_s)
#
#	Polls $from's pg_cluster_membership for the number of nodes in the
#	'member' state (the membership-decision SSOT, INV-J8) until it equals
#	$expected_members or $timeout_s elapses.  Used by the reconfig matrix
#	to assert membership convergence after a fault/leave/join.  The
#	membership view's state column is one of absent/dead/joining/member/
#	rejected (spec-5.15 D6);  'member' is the live admitted state.
#	Returns 1 on success, 0 on timeout.
#-----------------------------------------------------------------------
sub wait_for_membership_count
{
	my ($self, $from, $expected_members, $timeout_s) = @_;
	$timeout_s //= 30;
	my $node = $self->{nodes}[$from];
	my $deadline = time + $timeout_s;
	my $last = '(never-queried)';
	while (time < $deadline)
	{
		my $n = $node->safe_psql('postgres',
			"SELECT count(*) FROM pg_cluster_membership "
			. "WHERE state = 'member'");
		$last = defined $n ? $n : '(null)';
		return 1 if defined $n && $n == $expected_members;
		select(undef, undef, undef, 0.25);
	}
	Test::More::diag(
		"ClusterQuad wait_for_membership_count TIMEOUT after ${timeout_s}s: "
		. "from=node$from expected_members=$expected_members last=$last");
	return 0;
}


1;
