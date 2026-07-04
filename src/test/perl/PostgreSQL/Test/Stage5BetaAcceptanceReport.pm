# -*- perl -*-
#
# PostgreSQL::Test::Stage5BetaAcceptanceReport
#
#	  spec-5.21 D4:  Stage 5 beta close-out / release-gate JSON report.
#
#	  Standardized schema (version 1):
#	    {
#	      spec: "5.21",
#	      tag:  "v0.12?.0-stage5.21" | "unknown",   # internal monotonic tag
#	      public_label: "v0.5.0-beta",              # public milestone label
#	      timestamp: ISO8601,
#	      schema_version: 1,
#	      readiness_matrix: {
#	        rac_core:          [ { id, name, present, evidence }, ... ],
#	        subspec_readiness: [ { spec, status, marker }, ... ],
#	        open_p0p1:         { count, items: [ ... ] },
#	        verdict_519:       { verdict, note },
#	        verdict_520:       { verdict, note },
#	        verdict_558:       { verdict, note }
#	      },
#	      chaos_soak: {
#	        ci_leg:  { status, cycles, faults, consistency, note },
#	        ext_leg: { status, ...EXT artifacts... },
#	        mechanism_completion: { status, note }
#	      },
#	      integrated_regression: { status, prove_tests, note },
#	      limitations: [ { name, kind, forward, note }, ... ],
#	      go_no_go: "GO" | "NO-GO" | "CHECKPOINT-ONLY" | "undecided"
#	    }
#
#	  The D1 chaos-soak leg, the D2 integrated-acceptance leg, and the D0
#	  readiness scan accumulate into one report instance and emit to
#	  tmp/stage5-beta-acceptance-<timestamp>.json.  pgrac
#	  docs/stage-5-beta-acceptance.md manually imports the go/no-go record
#	  (linkdb CI does NOT cross-write the pgrac repo — mirrors the
#	  Stage5IntegratedAcceptanceReport separation).
#
#	  readiness status enum (subspec_readiness[].status):
#	    SHIPPED_DOUBLE_GREEN  — ship tag present + fast+nightly run IDs
#	    DESIGN_AHEAD_PENDING  — spec written, mechanism un-coded (no tag)
#	    DRAFT_PENDING         — spec DRAFT, not frozen/shipped
#	    RETIRED_WITH_TOMBSTONE — tombstone present + fold-into targets shipped
#	  RETIRED_WITH_TOMBSTONE (5.17 only) is verified by tombstone + fold-into
#	  double-green, NOT by a self ship tag (there is none) — checking a 5.17
#	  tag would pin the gate red forever or invite a forged tag.
#
#	  The two 4-node faithful chaos-soak legs (release-cut evidence, spec-5.21
#	  HG#2 / R13):
#	    ci_leg  — ClusterQuad single-machine 4-node soak;  PASS or ENV_SKIP.
#	    ext_leg — external 4-node connstr manual;  PASS (with full artifacts)
#	              or ABSENT.
#	  Cutting the public v0.5.0-beta requires at least one 4-node faithful PASS
#	  (ci_leg == PASS OR ext_leg == PASS).  validate() enforces
#	  go_no_go == 'GO'  =>  (ci PASS OR ext PASS), and ext PASS => all required
#	  EXT artifacts present + final_consistency_summary all-node-consistent.
#	  A 3-node faithful + mechanism-completion result is NOT a substitute for a
#	  4-node faithful PASS (rule 8.B — no over-claim of the 4-node headline).
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
package PostgreSQL::Test::Stage5BetaAcceptanceReport;

use strict;
use warnings;

use Scalar::Util qw(reftype);
use Time::HiRes qw(time);


# Required EXT-leg artifacts.  ext_leg status may be PASS only when every one of
# these is present (spec-5.21 D4 / HG#2a-EXT — external manual is never an
# un-checkable "manual PASS").
our @EXT_REQUIRED = qw(
	commit_sha internal_tag_candidate node_identities cycle_count
	tpcb_params storage_backend fence_backend log_paths
	final_consistency_summary operator timestamp
);


# Hand-rolled minimal JSON emitter — avoids the JSON.pm dep on CI runners where
# Perl prereqs are minimal.  Uses reftype() (not ref()) so the top-level blessed
# report object encodes as its underlying HASH — ref() returns the package name
# for a blessed ref, which would stringify the whole report to garbage (L223).
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
	# Scalar:  numeric vs string detection.  Conservative — anything not a pure
	# integer/decimal we quote.
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


sub _iso_now
{
	my @t = localtime(time);
	return sprintf "%04d-%02d-%02dT%02d:%02d:%02d",
		$t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0];
}


sub new
{
	my ($class, %opts) = @_;
	my $self = {
		spec           => $opts{spec}         // '5.21',
		tag            => $opts{tag}          // 'unknown',
		public_label   => $opts{public_label} // 'v0.5.0-beta',
		timestamp      => $opts{timestamp}    // _iso_now(),
		schema_version => 1,
		readiness_matrix => {
			rac_core          => [],
			subspec_readiness => [],
			open_p0p1         => { count => 0, items => [] },
			verdict_519       => { verdict => 'unknown' },
			verdict_520       => { verdict => 'unknown' },
			verdict_558       => { verdict => 'unknown' },
		},
		chaos_soak => {
			ci_leg               => { status => 'ENV_SKIP' },
			ext_leg              => { status => 'ABSENT' },
			mechanism_completion => { status => 'unknown' },
		},
		integrated_regression => { status => 'unknown' },
		limitations           => [],
		go_no_go              => 'undecided',
	};
	return bless $self, $class;
}


# D0 (b):  one of the 7 RAC core presence rows.
#   id       — 1..7
#   present  — 1 / 0
#   evidence — grep anchor file:line + e2e reference
sub record_rac_core
{
	my ($self, $id, $name, %extra) = @_;
	push @{ $self->{readiness_matrix}->{rac_core} }, {
		id      => $id,
		name    => $name,
		present => $extra{present} // 1,
		%extra,
	};
}


# D0 (a):  one sub-spec readiness row.
#   status — one of the readiness enum values (see banner).
#   marker — the status-specific proof (ship tag / tombstone+fold-into).
sub record_subspec_readiness
{
	my ($self, $spec, $status, %extra) = @_;
	push @{ $self->{readiness_matrix}->{subspec_readiness} }, {
		spec   => $spec,
		status => $status,
		marker => $extra{marker} // '',
		%extra,
	};
}


# D0 (c):  the open-P0/P1 scan result.  count>0 forces NO-GO.
sub set_open_p0p1
{
	my ($self, $count, @items) = @_;
	$self->{readiness_matrix}->{open_p0p1} = {
		count => $count,
		items => [@items],
	};
}


# D0 (d):  ingest a sub-domain verdict.  which — '519' / '520' / '558'.
sub set_verdict
{
	my ($self, $which, $verdict, %extra) = @_;
	$self->{readiness_matrix}->{"verdict_$which"} = {
		verdict => $verdict,
		%extra,
	};
}


# D1:  the ClusterQuad single-machine 4-node faithful soak leg (HG#2a-CI).
#   status — PASS / ENV_SKIP
sub set_ci_leg
{
	my ($self, $status, %extra) = @_;
	$self->{chaos_soak}->{ci_leg} = {
		status => $status,
		%extra,
	};
}


# D1:  the external 4-node connstr manual faithful leg (HG#2a-EXT).
#   status — PASS / ABSENT
# When status is PASS, every artifact in @EXT_REQUIRED must be supplied;
# ext_artifacts_complete() / validate() enforce that (a bare "manual PASS" with
# missing artifacts is not accepted).
sub set_ext_leg
{
	my ($self, $status, %extra) = @_;
	$self->{chaos_soak}->{ext_leg} = {
		status => $status,
		%extra,
	};
}


# D1:  the deterministic mechanism-completion leg (HG#2b partner).  Honest:
# this is REAL-FSM-to-DONE, never a substitute for a faithful chaos PASS.
sub set_mechanism_completion
{
	my ($self, $status, %extra) = @_;
	$self->{chaos_soak}->{mechanism_completion} = {
		status => $status,
		%extra,
	};
}


# D2:  the release-commit integrated-regression result (HG#3).
sub set_integrated_regression
{
	my ($self, $status, %extra) = @_;
	$self->{integrated_regression} = {
		status => $status,
		%extra,
	};
}


# §1.3 / D6:  one honest beta known-limitation.
#   kind    — node-ceiling / fencing / experimental / storage / perf-forward /
#             substrate / availability
#   forward — the stage/spec that owns the eventual lift (may be omitted)
sub record_limitation
{
	my ($self, $name, %extra) = @_;
	push @{ $self->{limitations} }, {
		name => $name,
		kind => $extra{kind} // 'forward',
		%extra,
	};
}


sub set_go_no_go
{
	my ($self, $decision) = @_;
	$self->{go_no_go} = $decision;
}


# True when the ext_leg carries every required artifact (HG#2a-EXT machine
# check -- presence only).  Used by validate() and the D1/D0 EXT ingest.
sub ext_artifacts_complete
{
	my ($self) = @_;
	my $ext = $self->{chaos_soak}->{ext_leg};
	for my $k (@EXT_REQUIRED) {
		return 0 unless defined $ext->{$k};
	}
	return 1;
}


# True when a consistency summary affirms all-node consistency.  An ext PASS
# whose final_consistency_summary is empty or reports divergence/loss/corruption
# is NOT a valid release-cut evidence (spec-5.21 §3.1: ext PASS => summary
# all-node-consistent).
sub _consistency_affirmed
{
	my ($s) = @_;
	return 0 unless defined $s && length $s;
	return 0 if $s =~ /diverg|mismatch|inconsist|lost|corrupt|fail/i;
	return 1;
}


# True when the ext_leg is a VALID PASS: status PASS + all required artifacts
# present + final_consistency_summary affirms all-node consistency.
sub ext_leg_valid_pass
{
	my ($self) = @_;
	my $ext = $self->{chaos_soak}->{ext_leg};
	return 0 unless ($ext->{status} // '') eq 'PASS';
	return 0 unless $self->ext_artifacts_complete();
	return 0 unless _consistency_affirmed($ext->{final_consistency_summary});
	return 1;
}


# True when a real 4-node faithful PASS exists on either leg (release-cut
# precondition, HG#2 / R13).  ext_leg counts only as a VALID PASS (artifacts +
# affirmed consistency).
sub has_four_node_faithful_pass
{
	my ($self) = @_;
	return 1 if ($self->{chaos_soak}->{ci_leg}->{status} // '') eq 'PASS';
	return $self->ext_leg_valid_pass() ? 1 : 0;
}


# Cross-field consistency check.  Returns a list of human-readable errors
# (empty list == valid).  Enforces spec-5.21 machine gates:
#   1. go_no_go == 'GO'  =>  a real 4-node faithful PASS exists.
#   2. ext_leg PASS      =>  all required EXT artifacts present.
#   3. open_p0p1.count>0 =>  go_no_go must NOT be 'GO'.
#   4. any subspec non-shipped/non-retired => go_no_go must NOT be 'GO'.
sub validate
{
	my ($self) = @_;
	my @err;

	my $ext = $self->{chaos_soak}->{ext_leg};
	if (($ext->{status} // '') eq 'PASS') {
		if (!$self->ext_artifacts_complete()) {
			my @missing = grep { !defined $ext->{$_} } @EXT_REQUIRED;
			push @err,
				"ext_leg=PASS but missing EXT artifacts: " . join(',', @missing);
		}
		elsif (!_consistency_affirmed($ext->{final_consistency_summary})) {
			push @err,
				"ext_leg=PASS but final_consistency_summary does not affirm "
				. "all-node consistency: '"
				. ($ext->{final_consistency_summary} // '(undef)') . "'";
		}
	}

	if ($self->{go_no_go} eq 'GO' && !$self->has_four_node_faithful_pass()) {
		push @err, "go_no_go=GO but no real 4-node faithful PASS "
			. "(ci_leg != PASS and ext_leg != PASS-with-artifacts)";
	}

	if ($self->{readiness_matrix}->{open_p0p1}->{count} > 0
		&& $self->{go_no_go} eq 'GO') {
		push @err, "go_no_go=GO but open_p0p1.count > 0";
	}

	for my $r (@{ $self->{readiness_matrix}->{subspec_readiness} }) {
		next if $r->{status} eq 'SHIPPED_DOUBLE_GREEN';
		next if $r->{status} eq 'RETIRED_WITH_TOMBSTONE';
		if ($self->{go_no_go} eq 'GO') {
			push @err, "go_no_go=GO but sub-spec $r->{spec} status=$r->{status}";
		}
	}

	return @err;
}


sub emit_json
{
	my ($self, $path) = @_;
	open my $fh, '>', $path or die "open $path: $!";
	print $fh _encode($self);
	close $fh;
	return $path;
}


# Test helper:  build default path tmp/stage5-beta-acceptance-<timestamp>.json
sub default_path
{
	my ($self, $base) = @_;
	$base //= 'tmp';
	mkdir $base unless -d $base;
	my $ts = $self->{timestamp};
	$ts =~ s/[^0-9TZ]/_/g;
	return "$base/stage5-beta-acceptance-$ts.json";
}


1;
