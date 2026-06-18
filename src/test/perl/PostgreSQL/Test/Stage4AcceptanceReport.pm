# -*- perl -*-
#
# PostgreSQL::Test::Stage4AcceptanceReport
#
#	  spec-4.14 D7:  Stage 4 (WAL + Recovery) acceptance JSON report emitter.
#
#	  Standardized schema (version 1):
#	    {
#	      spec: "4.14",
#	      tag:  "v0.92.0-stage4.14" | "unknown",
#	      timestamp: ISO8601,
#	      schema_version: 1,
#	      matrix: {
#	        recovery_capabilities: [ { name, status, layer, counter_delta, ... }, ... ],
#	        hard_gates:            [ { id, name, status, required, note }, ... ],
#	        storage_fencing:       [ { layer, name, status, forward }, ... ],
#	        baseline_regression:   [ { band, value_ms, baseline_ms, verdict }, ... ],
#	        demo:                  [ { name, status, note }, ... ],
#	        limitations:           [ { name, kind, forward, note }, ... ]
#	      }
#	    }
#
#	  Each test (t/273, t/274, t/275) accumulates into one report instance and
#	  emits to tmp/stage4-acceptance-<timestamp>.json.  pgrac docs/
#	  stage-4-acceptance.md + docs/perf-baseline.md §16 manually import for
#	  trend tracking (linkdb CI does NOT cross-write the pgrac repo — mirrors
#	  spec-2.40 v0.2 F4 / spec-3.17 architectural separation).
#
#	  hard_gates carry an explicit status (PASS / SKIP) AND a 'required' flag
#	  (spec-4.14 §3 finding-1 status model):  REQUIRED gates (HG#1b / HG#2a-i /
#	  HG#3 / HG#4 / HG#5) must PASS;  PASS-or-SKIP gates (HG#1a faithful-crash /
#	  HG#2a-ii thread-recovery apply-through / HG#2b cross-node) may SKIP with a
#	  limitation.  A SKIP on a faithful gate is NEVER re-labelled "covered".
#
#	  baseline_regression rows are report-only (spec-4.14 §3.3):  recovery
#	  latency is compared to the spec-4.13 §15 block-recovery baseline for
#	  trend, with no numeric perf gate — correctness is proven by the hard
#	  gates, not a perf floor.  Cross-node steady perf is forward to Stage 5.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
package PostgreSQL::Test::Stage4AcceptanceReport;

use strict;
use warnings;

use Scalar::Util qw(reftype);
use Time::HiRes qw(time);


# Hand-rolled minimal JSON emitter — avoids the JSON.pm dep on CI runners
# where Perl prereqs are minimal.  Only handles scalars / arrays / hashes
# nested to whatever depth our schema needs.
#
# Uses reftype() (not ref()) so the top-level *blessed* report object encodes
# as its underlying HASH — ref() returns the package name for a blessed ref,
# which would otherwise stringify the whole report to a garbage scalar (L223,
# the latent Stage2AcceptanceReport bug spec-3.17 D7 caught).
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
		spec           => $opts{spec}      // '4.14',
		tag            => $opts{tag}       // 'unknown',
		timestamp      => $opts{timestamp} // _iso_now(),
		schema_version => 1,
		matrix => {
			recovery_capabilities => [],
			hard_gates            => [],
			storage_fencing       => [],
			baseline_regression   => [],
			demo                  => [],
			limitations           => [],
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


# D1 (t/273):  one Stage 4 recovery capability assertion (L1-L12).  status
# PASS / SKIP;  layer hard / best_effort.  counter_delta records the live-path
# proof (L250:  a non-zero delta proves the mechanism really ran).
sub record_recovery_capability
{
	my ($self, $name, %extra) = @_;
	push @{ $self->{matrix}->{recovery_capabilities} }, {
		name   => $name,
		status => $extra{status} // 'PASS',
		layer  => $extra{layer}  // 'hard',
		%extra,
	};
}


# D2 (t/274):  one combined recovery hard-gate (spec-4.14 §3 status model).
#   id        — HG#1a / HG#1b / HG#2a-i / HG#2a-ii / HG#2b / HG#3 / HG#4 / HG#5
#   required  — 1 = must PASS;  0 = PASS-or-SKIP-with-limitation (faithful /
#               thread-recovery apply-through / cross-node forward)
#   status    — PASS / SKIP
# A SKIP on a non-required gate is honest;  a SKIP on a required gate is a
# spec failure.  The caller (t/274) enforces "required => PASS".
sub record_hard_gate
{
	my ($self, $id, $name, %extra) = @_;
	push @{ $self->{matrix}->{hard_gates} }, {
		id       => $id,
		name     => $name,
		status   => $extra{status}   // 'PASS',
		required => $extra{required} // 1,
		%extra,
	};
}


# D3 (t/275):  one storage/fencing matrix boundary row.
#   layer  — L1 (cooperative, shared-FS verifiable) / L2 (external, forward)
#   status — PASS (L1 boundary verified) / FORWARD (L2 external, no actor)
sub record_storage_fencing
{
	my ($self, $layer, $name, %extra) = @_;
	push @{ $self->{matrix}->{storage_fencing} }, {
		layer  => $layer,
		name   => $name,
		status => $extra{status} // 'PASS',
		%extra,
	};
}


# D4:  one recovery-latency regression row vs the spec-4.13 §15 baseline.
# Report-only (no numeric gate) — verdict is informational trend only.
sub record_baseline_regression
{
	my ($self, $band, %extra) = @_;
	push @{ $self->{matrix}->{baseline_regression} }, {
		band   => $band,
		report_only => 1,
		%extra,
	};
}


# D5:  one 4-node shared-storage auto-recovery demo row.  status PASS / SKIP
# (SKIP carries 'reason' when no external shared-storage 4-node cluster is
# wired — never faked, spec-4.14 §1.4 / no-ClusterQuad honest finding).
sub record_demo
{
	my ($self, $name, %extra) = @_;
	push @{ $self->{matrix}->{demo} }, {
		name   => $name,
		status => $extra{status} // 'SKIP',
		%extra,
	};
}


# §1.3:  one honest Stage 4 limitation registration (4-node demo / 2-node
# steady UNAVAILABLE / cross-node positive CR FEATURE_NOT_SUPPORTED / L2
# external fence / thread latency SKIP / PI dirty reconstruction).
# kind = availability / correctness-forward / perf-forward;  forward = the
# spec/stage that owns the eventual fix.
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


# Test helper:  build default path tmp/stage4-acceptance-<timestamp>.json
sub default_path
{
	my ($self, $base) = @_;
	$base //= 'tmp';
	mkdir $base unless -d $base;
	my $ts = $self->{timestamp};
	$ts =~ s/[^0-9TZ]/_/g;
	return "$base/stage4-acceptance-$ts.json";
}


1;
