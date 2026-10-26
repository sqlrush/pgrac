# Author: SqlRush <sqlrush@gmail.com>
package M2CanonicalRollover;
use strict;
use warnings;
use Exporter 'import';
our @EXPORT_OK = qw(rollover_workload check_rollover_budget);

sub rollover_workload
{
 my $rows=65536;
 my $pad_repeats=28;
 return {rows=>$rows,pad_repeats=>$pad_repeats,setup_sql=>qq{
 SET cluster.trace_tx_identity=off;
 CREATE TABLE m2_roll(id int,v int,pad text);
 ALTER TABLE m2_roll ALTER pad SET STORAGE PLAIN;
 INSERT INTO m2_roll SELECT i,0,repeat(md5(i::text),$pad_repeats)
  FROM generate_series(1,$rows)i;
 }};
}

sub check_rollover_budget
{
 my ($limits,$stats)=@_;
 die "rollover fixture facts missing\n" unless ref($limits) eq 'HASH' && ref($stats) eq 'HASH';
 for my $entry ([$limits,[qw(segment_bytes inline_capacity_bytes update_payload_header_bytes
                             record_header_bytes history_trailer_bytes record_hard_cap_bytes)]],
                [$stats,[qw(rows min_tuple_bytes max_tuple_bytes)]]) {
  my ($source,$fields)=@$entry;
  for my $field (@$fields) {
   my $value=$source->{$field};
   die "rollover fixture facts invalid: $field\n"
    unless defined($value) && !ref($value) && $value =~ /\A[0-9]+\z/ && $value > 0;
  }
 }
 die "rollover fixture facts inconsistent\n" if $stats->{min_tuple_bytes}>$stats->{max_tuple_bytes};
 my $max_payload=$limits->{update_payload_header_bytes}+$stats->{max_tuple_bytes};
 die "rollover fixture UPDATE payload $max_payload exceeds receipt $limits->{inline_capacity_bytes}\n"
  if $max_payload>$limits->{inline_capacity_bytes};
 my $overhead=$limits->{record_header_bytes}+$limits->{history_trailer_bytes};
 my $max_record=$max_payload+$overhead;
 die "rollover fixture record $max_record exceeds hard cap $limits->{record_hard_cap_bytes}\n"
  if $max_record>$limits->{record_hard_cap_bytes};
 my $minimum=$stats->{rows}*
  ($limits->{update_payload_header_bytes}+$stats->{min_tuple_bytes}+$overhead);
 die "rollover fixture record budget $minimum does not exceed segment $limits->{segment_bytes}\n"
  if $minimum<=$limits->{segment_bytes};
 # A sufficient byte budget is not a native allocation or caller witness.
 return {max_payload_bytes=>$max_payload,max_record_bytes=>$max_record,
         min_total_record_bytes=>$minimum};
}
1;
