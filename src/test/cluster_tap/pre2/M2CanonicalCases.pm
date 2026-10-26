# Author: SqlRush <sqlrush@gmail.com>
package M2CanonicalCases;
use strict;
use warnings;
use Exporter 'import';
use Test::More ();
our @EXPORT_OK = qw(run_cases);

# Keep each verdict and its cleanup independent of later matrix cells.
sub run_cases
{
 my ($cases,$body,$cleanup,$record)=@_;
 for my $case (@$cases) {
  my $completed=eval { $body->($case); 1; };
  my $error=$@;
  eval { $cleanup->($case); 1; };
  my $cleanup_error=$@;
  my $recorded=eval {
   $record->($case,{completed=>$completed ? 1 : 0,
    error=>$error,cleanup_error=>$cleanup_error});
   1;
  };
  if(!$recorded) {
   my $record_error=$@;
   Test::More::fail("cell $case->{number}: evidence recording failed");
   Test::More::diag($error.$cleanup_error.$record_error);
  }
 }
}
1;
