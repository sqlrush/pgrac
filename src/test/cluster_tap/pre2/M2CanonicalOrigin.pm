# Author: SqlRush <sqlrush@gmail.com>
package M2CanonicalOrigin;
use strict;
use warnings;
use M2CanonicalCases qw(run_cases);

# Keep real same-segment records available after another cell rolls DATA.
# These are ordinary open producer transactions, with resources owned by
# their individual cells. Sampling still runs in the original matrix order.
sub run_prepared_cases
{
 my ($cases,$prepare,$body,$cleanup,$record)=@_;
 my (%prepared,%errors);
 for my $case (@$cases) {
  next unless $case->{caller} eq 'freeze_data';
  my $ok=eval { $prepared{$case->{number}}=$prepare->($case); 1; };
  $errors{$case->{number}}=$@ unless $ok;
 }
 run_cases($cases,sub {
  my ($case)=@_;
  my $id=$case->{number};
  die $errors{$id} if exists $errors{$id};
  $prepared{$id}=$prepare->($case) unless exists $prepared{$id};
  $body->($case,$prepared{$id});
 },$cleanup,$record);
}

sub origin_query
{
 my ($locator,$key,$caller)=@_;
 for my $field (qw(uba0 uba1 xid wrap itl_kind itl_slot_index data_segment tt_slot origin)) {
  my $v=$locator->{$field};
  die "invalid copied locator $field\n" unless defined($v) && $v =~ /\A[0-9]+\z/
   && length($v)<=19 && (length($v)<19 || $v le '9223372036854775807');
 }
 for my $field (qw(xid origin segment slot wrap generation root root_generation)) {
  die "missing producer identity $field\n" unless defined($key->{$field})
   && $key->{$field} =~ /\A[0-9]+\z/;
 }
 die "copied locator/producer identity mismatch\n" unless $locator->{xid} eq $key->{xid}
  && $locator->{origin} eq $key->{origin} && $locator->{tt_slot} eq $key->{slot};
 my $actual=$locator->{data_segment} == $key->{segment} ? 'freeze_data' : 'sample_canonical';
 die "caller mismatch: requested=$caller actual=$actual DATA=$locator->{data_segment} TT=$key->{segment}\n"
  unless $caller eq $actual;
 return "SELECT test_pgrac_m2_sample_locator($locator->{uba0},$locator->{uba1},"
  ."'$locator->{xid}'::xid,$locator->{wrap},$locator->{itl_kind},$locator->{itl_slot_index})";
}
1;
