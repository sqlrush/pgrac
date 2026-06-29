# -*- perl -*-
#
# PostgreSQL::Test::Stage5IntegratedAcceptanceReport
#
#	  spec-5.19 D7:  Stage 5 integrated-acceptance JSON report emitter.
#
#	  Standardized schema (version 1):
#	    {
#	      spec: "5.19",
#	      tag:  "v0.1??.0-stage5.19" | "unknown",
#	      timestamp: ISO8601,
#	      schema_version: 1,
#	      matrix: {
#	        reconfig_matrix:        [ { fault, phase, leg, status, required,
#	                                    counter_delta, note }, ... ],
#	        multinode_write_perf:   { value: [ { nodes, workload, write_tax_pct,
#	                                    wait_share, affinity_gain, ... }, ... ],
#	                                  soundness: { verdict, real, note } },
#	        hw_extend:              [ { id, name, status, required, note }, ... ],
#	        itl_wal_decision:       { measured: { ... }, decision, blocker,
#	                                  note },
#	        production_bench_subset:[ { workload, status, correctness, metric,
#	                                    note }, ... ],
#	        limitations:            [ { name, kind, forward, note }, ... ]
#	      }
#	    }
#
#	  The acceptance legs (t/32x reconfig matrix / HW-extend workload /
#	  production-bench-subset) and the MG-B / MG-D perf measure scripts
#	  accumulate into one report instance and emit to
#	  tmp/stage5-integrated-acceptance-<timestamp>.json.  pgrac
#	  docs/stage-5-integrated-acceptance.md + docs/perf-baseline.md §N
#	  manually import for trend tracking (linkdb CI does NOT cross-write the
#	  pgrac repo — mirrors spec-2.40 / 3.17 / 4.14 separation).
#
#	  reconfig_matrix / hw_extend rows carry an explicit status AND a
#	  'required' flag (spec-5.19 §3.1 status model):  REQUIRED cells must
#	  PASS;  PASS-or-SKIP cells (4-node faithful) may SKIP with a limitation;
#	  a cell whose mechanism is un-merged is BLOCKED (NOT a closure SKIP,
#	  rule 8.B).  A SKIP on a faithful gate is NEVER re-labelled "covered".
#
#	  multinode_write_perf / itl_wal_decision are measure-and-decide (L257):
#	  raw metric is report-only;  a RED regression / GO decision becomes a
#	  ship blocker ONLY when qualified as a perf blocker, in which case the
#	  fix closes inside 5.19.  The soundness verdict distinguishes REAL
#	  (true cross-node shared-heap write competition) from measure-only.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
package PostgreSQL::Test::Stage5IntegratedAcceptanceReport;

use strict;
use warnings;

use Scalar::Util qw(reftype);
use Time::HiRes qw(time);


# Hand-rolled minimal JSON emitter — avoids the JSON.pm dep on CI runners
# where Perl prereqs are minimal.  Uses reftype() (not ref()) so the top-level
# *blessed* report object encodes as its underlying HASH — ref() returns the
# package name for a blessed ref, which would stringify the whole report to a
# garbage scalar (L223, the latent Stage2AcceptanceReport bug spec-3.17 D7
# caught).
sub _encode
{
	my ($v) = @_;
	if (!defined $v) {
		return 'null';
	}
	my $rt = reftype($v);
	if (defined $rt && $rt eq 'HASH') {
		my @kv;
		for my $k (sort keys %$v) {
			my $ek = $k; $ek =~ s/"/\\"/g;
			push @kv, qq{"$ek":} . _encode($v->{$k});
		}
		return '{' . join(',', @kv) . '}';
	}
	if (defined $rt && $rt eq 'ARRAY') {
		return '[' . join(',', map { _encode($_) } @$v) . ']';
	}
	# Scalar:  numeric vs string detection.  Conservative — anything not a
	# pure integer/decimal we quote.
	if ($v =~ /^-?\d+(\.\d+)?$/) {
		return $v;
	}
	my $s = $v;
	$s =~ s/\\/\\\\/g;
	$s =~ s/"/\\"/g;
	$s =~ s/\n/\\n/g;
	$s =~ s/\r/\\r/g;
	$s =~ s/\t/\\t/g;
	return qq{"$s"};
}


sub new
{
	my ($class, %opts) = @_;
	my $self = {
		spec           => $opts{spec}      // '5.19',
		tag            => $opts{tag}       // 'unknown',
		timestamp      => $opts{timestamp} // _iso_now(),
		schema_version => 1,
		matrix => {
			reconfig_matrix         => [],
			multinode_write_perf    => {
				value     => [],
				soundness => { verdict => 'unknown', real => 0 },
			},
			hw_extend               => [],
			itl_wal_decision        => {
				measured => {},
				decision => 'undecided',
				blocker  => 0,
			},
			production_bench_subset => [],
			limitations             => [],
		},
	};
	return bless $self, $class;
}


sub _iso_now
{
	my @t = localtime(time);
	return sprintf "%04d-%02d-%02dT%02d:%02d:%02d",
		$t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0];
}


# D2 (t/32x):  one reconfig-matrix cell (HG#1/#2/#3).
#   fault    — clean_leave / fail_stop / join / join_remaster / leave_remove
#   phase    — idle / under_write_load / during_remaster
#   leg      — faithful / mechanism_completion
#   status   — PASS / SKIP / BLOCKED
#   required — 1 = must PASS;  0 = PASS-or-environment-SKIP (4-node faithful)
# A cell whose mechanism is un-merged (e.g. join_remaster before 5.16 lands)
# is BLOCKED — it blocks the 5.19 tag and is never a closure SKIP (rule 8.B).
sub record_reconfig_cell
{
	my ($self, $fault, $phase, %extra) = @_;
	push @{ $self->{matrix}->{reconfig_matrix} }, {
		fault    => $fault,
		phase    => $phase,
		leg      => $extra{leg}      // 'mechanism_completion',
		status   => $extra{status}   // 'PASS',
		required => $extra{required} // 1,
		%extra,
	};
}


# D3 (MG-B):  one multi-node write-path perf value row.
#   nodes        — 2 / 3 / 4
#   workload     — tpcb / hotrow
#   write_tax_pct, wait_share, affinity_gain — raw metric (report-only)
sub record_multinode_write_value
{
	my ($self, $nodes, $workload, %extra) = @_;
	push @{ $self->{matrix}->{multinode_write_perf}->{value} }, {
		nodes       => $nodes,
		workload    => $workload,
		report_only => 1,
		%extra,
	};
}


# D3 (MG-B):  the soundness-gate verdict (D0).  real=1 only when the harness
# runs true cross-node shared-heap write competition (aligned-OID relfilenode,
# GES/PCM/HW on the same block);  measure-only otherwise.
sub set_multinode_write_soundness
{
	my ($self, $verdict, %extra) = @_;
	$self->{matrix}->{multinode_write_perf}->{soundness} = {
		verdict => $verdict,
		real    => $extra{real} // 0,
		%extra,
	};
}


# D4 (t/32x HW workload):  one HW/relation-extend hard-gate row (HG#4).
#   id     — HG#4a disjoint / HG#4b crash-recovery-HWM / HG#4c fail-closed
#   status — PASS / SKIP
sub record_hw_extend
{
	my ($self, $id, $name, %extra) = @_;
	push @{ $self->{matrix}->{hw_extend} }, {
		id       => $id,
		name     => $name,
		status   => $extra{status}   // 'PASS',
		required => $extra{required} // 1,
		%extra,
	};
}


# D5 (MG-D):  the heap-ITL WAL compaction measure-and-decide record.
#   measured  — { delta_bytes_per_record, itl_share_pct, coalesce_rate_pct }
#   decision  — GO / NO-GO
#   blocker   — 1 only when qualified as a perf blocker (then closes in 5.19)
sub set_itl_wal_decision
{
	my ($self, $decision, %extra) = @_;
	$self->{matrix}->{itl_wal_decision} = {
		measured => $extra{measured} // {},
		decision => $decision,
		blocker  => $extra{blocker} // 0,
		%extra,
	};
}


# D6 (production-bench-subset):  one workload row (HG#5).
#   workload    — tpcc / ddl_oltp / crash_load / long_snap / hot_row
#   status      — PASS / SKIP
#   correctness — the 8.A verdict (no_lost_commit / no_corruption /
#                 no_false_visible)
sub record_production_bench
{
	my ($self, $workload, %extra) = @_;
	push @{ $self->{matrix}->{production_bench_subset} }, {
		workload => $workload,
		status   => $extra{status} // 'PASS',
		%extra,
	};
}


# §1.3:  one honest Stage 5 integrated-acceptance limitation registration.
# kind = availability / correctness-forward / perf-forward / substrate;
# forward = the spec/stage that owns the eventual fix.
sub record_limitation
{
	my ($self, $name, %extra) = @_;
	push @{ $self->{matrix}->{limitations} }, {
		name => $name,
		kind => $extra{kind} // 'forward',
		%extra,
	};
}


sub emit_json
{
	my ($self, $path) = @_;
	open my $fh, '>', $path or die "open $path: $!";
	print $fh _encode($self);
	close $fh;
	return $path;
}


# Test helper:  build default path
# tmp/stage5-integrated-acceptance-<timestamp>.json
sub default_path
{
	my ($self, $base) = @_;
	$base //= 'tmp';
	mkdir $base unless -d $base;
	my $ts = $self->{timestamp};
	$ts =~ s/[^0-9TZ]/_/g;
	return "$base/stage5-integrated-acceptance-$ts.json";
}


1;
