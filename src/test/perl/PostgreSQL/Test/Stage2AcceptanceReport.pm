# -*- perl -*-
#
# PostgreSQL::Test::Stage2AcceptanceReport
#
#	  spec-2.40 D14:  Stage 2 acceptance JSON report emitter.
#
#	  Standardized schema (version 1):
#	    {
#	      spec: "2.40",
#	      tag:  "v0.46.0-stage2.40" | "unknown",
#	      timestamp: ISO8601,
#	      schema_version: 1,
#	      matrix: {
#	        capabilities: [ { name, status, layer, ... }, ... ],
#	        faults:       [ { name, recovery_us, bound_us, status }, ... ],
#	        workloads:    [ { name, single_node_off, single_node_on, two_node, ... }, ... ]
#	      }
#	    }
#
#	  Each test (t/200, t/201, t/202) accumulates into one report instance
#	  and emits to tmp/stage2-acceptance-<timestamp>.json.  pgrac docs/
#	  stage-2-acceptance.md + docs/perf-baseline.md manually import for
#	  trend tracking (linkdb CI does NOT cross-write pgrac repo — spec
#	  v0.2 F4 architectural fix).
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
package PostgreSQL::Test::Stage2AcceptanceReport;

use strict;
use warnings;

use Time::HiRes qw(time);


# Hand-rolled minimal JSON emitter — avoids the JSON.pm dep on CI runners
# where Perl prereqs are minimal.  Only handles scalars / arrays / hashes
# nested to whatever depth our schema needs.
sub _encode
{
	my ($v) = @_;
	if (!defined $v) {
		return 'null';
	}
	if (ref($v) eq 'HASH') {
		my @kv;
		for my $k (sort keys %$v) {
			my $ek = $k; $ek =~ s/"/\\"/g;
			push @kv, qq{"$ek":} . _encode($v->{$k});
		}
		return '{' . join(',', @kv) . '}';
	}
	if (ref($v) eq 'ARRAY') {
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
		spec           => $opts{spec}           // '2.40',
		tag            => $opts{tag}            // 'unknown',
		timestamp      => $opts{timestamp}      // _iso_now(),
		schema_version => 1,
		matrix => {
			capabilities => [],
			faults       => [],
			workloads    => [],
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


sub record_capability_assertion
{
	my ($self, $name, %extra) = @_;
	push @{ $self->{matrix}->{capabilities} }, {
		name   => $name,
		status => $extra{status} // 'PASS',
		layer  => $extra{layer}  // 'hard',
		%extra,
	};
}


sub record_fault_scenario
{
	my ($self, $name, %extra) = @_;
	push @{ $self->{matrix}->{faults} }, {
		name        => $name,
		recovery_us => $extra{recovery_us} // 0,
		bound_us    => $extra{bound_us}    // 30_000_000,
		status      => $extra{status}      // 'PASS',
		%extra,
	};
}


sub record_perf_workload
{
	my ($self, $name, %extra) = @_;
	push @{ $self->{matrix}->{workloads} }, {
		name => $name,
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


# Test helper:  build default path tmp/stage2-acceptance-<timestamp>.json
sub default_path
{
	my ($self, $base) = @_;
	$base //= 'tmp';
	mkdir $base unless -d $base;
	my $ts = $self->{timestamp};
	$ts =~ s/[^0-9TZ]/_/g;
	return "$base/stage2-acceptance-$ts.json";
}


1;
