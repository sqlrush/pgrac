
#-------------------------------------------------------------------------
#
# HangManagerAcceptanceReport.pm -- spec-5.20 D7: Hang Manager acceptance
# report schema + JSON emitter/validator.
#
# Defines the machine-readable schema the chaos-acceptance legs (t/340-343) and
# the MG-A/MG-B measure scripts (scripts/perf/measure-hang-*.pl) emit, plus a
# validator the report leg uses to prove the emitted artifact has real content
# (not merely that the file exists, L223).  Type checks use
# Scalar::Util::reftype (structural) rather than ref() (blessed-class-name),
# per lessons L223.
#
# Schema (top-level keys):
#   version                   : schema version string
#   spec                      : "spec-5.20"
#   detection_matrix          : { ht_cells => [...], faithful => N, synthetic => N }
#   false_positive            : { value => { rate, evaluations, recommendations },
#                                 soundness => REAL|MEASURE_ONLY,
#                                 unsafe_dispositions => N }   # N must be 0
#   remediation               : { terminates => N, cancels => N, aba_revalidate => N }
#   diagnostic_completeness    : { fixed_keys => N, sample_suffixes => N }
#   reconfig_noninterference   : { three_node => PASS|FAIL, four_node => PASS|SKIP }
#   latency                    : { detect_ms, resolve_ms, soundness }
#   limitation                 : [ ... honest forward/skip notes ... ]
#
# Spec authority: pgrac:specs/spec-5.20-hang-manager-acceptance.md §1.2 D7.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

package PostgreSQL::Test::HangManagerAcceptanceReport;

use strict;
use warnings;

use Scalar::Util qw(reftype looks_like_number);

our $SCHEMA_VERSION = '1.0';

# The required top-level keys and the reftype each must have ('' = scalar).
our %REQUIRED = (
	version                  => '',
	spec                     => '',
	detection_matrix         => 'HASH',
	false_positive           => 'HASH',
	remediation              => 'HASH',
	diagnostic_completeness  => 'HASH',
	reconfig_noninterference => 'HASH',
	latency                  => 'HASH',
	limitation               => 'ARRAY',
);


# new() -> a skeleton report hashref with the schema shape + safe defaults.
sub new_report
{
	return {
		version                  => $SCHEMA_VERSION,
		spec                     => 'spec-5.20',
		detection_matrix         => { ht_cells => [], faithful => 0, synthetic => 0 },
		false_positive           => {
			value     => { rate => undef, evaluations => 0, recommendations => 0 },
			soundness => 'MEASURE_ONLY',
			unsafe_dispositions => 0,
		},
		remediation              => { terminates => 0, cancels => 0, aba_revalidate => 0 },
		diagnostic_completeness  => { fixed_keys => 0, sample_suffixes => 0 },
		reconfig_noninterference => { three_node => 'PENDING', four_node => 'PENDING' },
		latency                  => { detect_ms => undef, resolve_ms => undef, soundness => 'MEASURE_ONLY' },
		limitation               => [],
	};
}


# validate($report) -> (ok, \@errors).  Structural + invariant checks:
#   - every required top-level key present with the right reftype;
#   - false_positive.unsafe_dispositions MUST be 0 (HG#2 zero-unsafe hard gate);
#   - soundness fields are one of the allowed enums.
sub validate
{
	my ($r) = @_;
	my @err;

	if (!defined $r || (reftype($r) // '') ne 'HASH')
	{
		return (0, ['report is not a HASH']);
	}

	for my $k (sort keys %REQUIRED)
	{
		if (!exists $r->{$k})
		{
			push @err, "missing key: $k";
			next;
		}
		my $want = $REQUIRED{$k};
		my $got  = reftype($r->{$k}) // '';
		if ($want eq '' && $got ne '')
		{
			push @err, "key $k should be a scalar, got reftype $got";
		}
		elsif ($want ne '' && $got ne $want)
		{
			push @err, "key $k should be reftype $want, got '$got'";
		}
	}

	# HG#2 zero-unsafe hard-gate invariant: any genuinely-unsafe disposition is
	# a P0, never a rate.  The report must record 0.
	if (reftype($r->{false_positive}) && reftype($r->{false_positive}) eq 'HASH')
	{
		my $u = $r->{false_positive}{unsafe_dispositions};
		if (!defined $u || !looks_like_number($u) || $u != 0)
		{
			push @err, "false_positive.unsafe_dispositions must be exactly 0 (HG#2 zero-unsafe)";
		}
		my $snd = $r->{false_positive}{soundness} // '';
		push @err, "false_positive.soundness must be REAL|MEASURE_ONLY"
			unless $snd eq 'REAL' || $snd eq 'MEASURE_ONLY';
	}

	if (reftype($r->{reconfig_noninterference}) && reftype($r->{reconfig_noninterference}) eq 'HASH')
	{
		my $tn = $r->{reconfig_noninterference}{three_node} // '';
		push @err, "reconfig_noninterference.three_node must be PASS (HG#5a REQUIRED)"
			unless $tn eq 'PASS' || $tn eq 'PENDING';
	}

	return (scalar(@err) == 0, \@err);
}


# to_json($report) -> a compact deterministic JSON string (no external deps:
# hand-rolled so the scripts have no CPAN requirement; keys sorted).
sub to_json
{
	my ($v) = @_;
	my $rt = reftype($v) // '';
	if ($rt eq 'HASH')
	{
		return '{' . join(',', map { _q($_) . ':' . to_json($v->{$_}) } sort keys %$v) . '}';
	}
	elsif ($rt eq 'ARRAY')
	{
		return '[' . join(',', map { to_json($_) } @$v) . ']';
	}
	elsif (!defined $v)
	{
		return 'null';
	}
	elsif (looks_like_number($v))
	{
		return $v + 0;
	}
	else
	{
		return _q($v);
	}
}

sub _q
{
	my ($s) = @_;
	$s =~ s/(["\\])/\\$1/g;
	$s =~ s/\n/\\n/g;
	return '"' . $s . '"';
}


1;
