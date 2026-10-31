#!/usr/bin/env perl
# Execute the real preflight and reject oversized input before owner creation.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use JSON::PP qw(encode_json decode_json);
use Test::More;
use IPC::Run qw(run timeout);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

open my $source,'<',"$FindBin::RealBin/../t/482_pre2_m2_itl_capacity.pl" or die $!;
my $text=do {local $/; <$source>};
close $source or die $!;
my @sessions;
my ($owners,$error);
sub save_json {
 my ($name,$value)=@_;
 $error=$value->{error} if $name eq 'b06-pair-execution';
}
sub session {$owners++; die "unexpected owner creation\n"}
my $name='m2_b06_pair';
my $fixture={source_tag=>[1663,5,20000,0,1],destination_tag=>[1663,5,20000,0,0],
 settings=>{block_size=>8192,payload_storage=>'p',inline_max_bytes=>1024},
 before=>{id=>9,v=>0,ctid=>'(1,1)',xmin=>'1100',xmax=>'0',payload_bytes=>900,tuple_bytes=>936},
 layout=>[(map {+{id=>$_,ctid=>"(0,$_)" ,payload_bytes=>0}} 1..8),
  {id=>9,ctid=>'(1,1)',payload_bytes=>900},{id=>10,ctid=>'(0,9)',payload_bytes=>6472},
  {id=>11,ctid=>'(1,2)',payload_bytes=>6600}],
 sql=>"UPDATE m2_b06_pair SET v=v+1,pad=repeat('n',480) WHERE id=9;"};

for my $function ('judge_requalification','check_cross_page_setup','cross_page_cell') {
 my @body=$text =~ /(^sub \Q$function\E \{.*?^\})/msg;
 is(scalar(@body),1,"one actual $function boundary exists");
 if(@body!=1) {done_testing(); exit 1;}
 eval $body[0]; die $@ if $@;
}
subtest 'the real Python preflight accepts only a bounded original input' => sub {
 my $proof=check_cross_page_setup($fixture);
 is($proof->{update_payload_bytes},948,'full original tuple plus fixed undo header');
 is($proof->{inline_max_bytes},1024,'actual unchanged capacity');
 my $bad=decode_json(encode_json($fixture));
 $bad->{settings}->{inline_max_bytes}=947;
 eval {check_cross_page_setup($bad)};
 like($@,qr/preflight evidence/,'oversized input refuses');
};
{
 package SetupNode;
 sub safe_psql {
  my ($self,$database,$sql)=@_;
  return '' if $sql =~ /^(?:CREATE TABLE|VACUUM)/;
  return encode_json($fixture->{layout}) if $sql =~ /SELECT json_agg/;
  return '1663|5|20000|0|0' if $sql =~ /pg_relation_filenode/;
  return encode_json($fixture->{before}) if $sql =~ /SELECT row_to_json/;
  if($sql =~ /SELECT json_build_object/) {
   my $settings={%{$fixture->{settings}},inline_max_bytes=>947};
   return encode_json($settings);
  }
  die "unexpected query: $sql";
 }
 sub encode_json {JSON::PP::encode_json($_[0])}
}
subtest 'actual cell checks the live budget before creating any owners' => sub {
 $owners=0;
 my $result=cross_page_cell(bless({},'SetupNode'),[]);
 ok(!$result,'ineligible setup cannot run a capacity scenario');
 like($error,qr/pair preflight evidence refused/,'actual preflight is the refusal boundary');
 is($owners,0,'no owner transaction is created');
};
done_testing();
