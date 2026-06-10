#-------------------------------------------------------------------------
#
# PgracWalState.pm
#	  Raw byte-level helpers for the ClusterWalState registry
#	  (<wal_threads_dir>/pgrac_wal_state, spec-4.2 layout: 512B header
#	  + 128 x 512B slots, slot offset = 512 + (tid-1)*512).
#
#	  Extracted from t/244_wal_state_registry.pl (spec-4.3 D8) so the
#	  CRC32C implementation and slot forging logic have a single
#	  source; consumers: t/244 (registry surface), t/245 (recovery
#	  plan).
#
#	  crc32c() must match PostgreSQL's pg_crc32c (Castagnoli,
#	  reflected, poly 0x82F63B78): forged slots are only useful when
#	  they classify OK at the spec-4.2 layer.
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-4.3-recovery-coordinator-skeleton.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

package PgracWalState;

use strict;
use warnings;

use Exporter 'import';
our @EXPORT_OK = qw(crc32c slot_offset read_file_raw write_file_raw
  read_slot_raw patch_byte forge_slot_node_id);

sub crc32c
{
	my ($data) = @_;
	my $crc = 0xFFFFFFFF;
	foreach my $b (unpack('C*', $data))
	{
		$crc ^= $b;
		for (1 .. 8)
		{
			$crc = ($crc >> 1) ^ (($crc & 1) ? 0x82F63B78 : 0);
		}
	}
	return $crc ^ 0xFFFFFFFF;
}

# Single-source offset mirror of CLUSTER_WAL_STATE_SLOT_OFFSET.
sub slot_offset
{
	my ($tid) = @_;
	return 512 + ($tid - 1) * 512;
}

sub read_file_raw
{
	my ($path) = @_;
	open my $fh, '<:raw', $path or die "open $path: $!";
	local $/;
	my $data = <$fh>;
	close $fh;
	return $data;
}

sub write_file_raw
{
	my ($path, $data) = @_;
	open my $fh, '+<:raw', $path or die "open $path: $!";
	syswrite($fh, $data) == length($data) or die "write $path: $!";
	close $fh;
}

# Fixed-field peek (magic/version/thread_id/node_id/state @0..15,
# started_at @24).
sub read_slot_raw
{
	my ($regfile, $tid) = @_;
	open my $fh, '<:raw', $regfile or die "open $regfile: $!";
	sysseek($fh, slot_offset($tid), 0) or die "seek: $!";
	sysread($fh, my $buf, 512) == 512 or die "short read";
	close $fh;
	my ($magic, $version, $thread_id, $node_id, $state) = unpack('LSSlL', $buf);
	my ($started_at) = unpack('q', substr($buf, 24, 8));
	return {
		magic => $magic,
		thread_id => $thread_id,
		node_id => $node_id,
		state => $state,
		started_at => $started_at
	};
}

# XOR one byte (CRC-breaking corruption).
sub patch_byte
{
	my ($path, $off) = @_;
	open my $fh, '+<:raw', $path or die "open $path: $!";
	sysseek($fh, $off, 0) or die "seek: $!";
	sysread($fh, my $old, 1) or die "read: $!";
	sysseek($fh, $off, 0) or die "seek: $!";
	syswrite($fh, chr(ord($old) ^ 0xFF), 1) or die "write: $!";
	close $fh;
}

# Rewrite slot $tid's node_id and recompute a VALID crc, so the slot
# still classifies OK at the spec-4.2 layer (identity-invariant tests:
# spec-4.2 foreign-owner gate, spec-4.3 P2 UNKNOWN rule).
sub forge_slot_node_id
{
	my ($regfile, $tid, $node_id) = @_;
	my $image = read_file_raw($regfile);
	my $off = slot_offset($tid);
	my $slot = substr($image, $off, 512);
	substr($slot, 8, 4) = pack('l', $node_id);
	substr($slot, 504, 4) = pack('L', crc32c(substr($slot, 0, 504)));
	substr($image, $off, 512) = $slot;
	write_file_raw($regfile, $image);
	return;
}

1;
