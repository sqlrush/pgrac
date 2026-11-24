#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use Test::More;

open my $file,'<',"$FindBin::RealBin/../t/482_pre2_m2_itl_capacity.pl" or die $!;
my $source=do {local $/; <$file>};
my @blocks=$source =~ /(my \@cleanup;\nfor my \$s \(\@sessions\) \{[^\n]*\n\@sessions=\(\);)/sg;
is(scalar(@blocks),1,'actual capacity client cleanup is available');
{package ExitClient; sub quit {return $_[0]->{ok}}}
for my $rc (0,1) {
 my @sessions=(bless {ok=>$rc},'ExitClient');
 my $count=eval $blocks[0].' scalar @cleanup;';
 is($@,'','actual cleanup ran');
 is($count,$rc ? 0 : 1,'nonzero client exit is retained while a clean exit succeeds');
}
done_testing();
