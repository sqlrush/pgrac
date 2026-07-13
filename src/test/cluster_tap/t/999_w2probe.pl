use strict; use warnings FATAL=>'all';
use FindBin; use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPair; use Test::More; use Time::HiRes qw(usleep time);
use IPC::Run ();
my $pair = PostgreSQL::Test::ClusterPair->new_pair('w2probe',
  quorum_voting_disks=>3, shared_data=>1,
  extra_conf=>['autovacuum = off','cluster.grd_max_entries = 1024',
    'cluster.ges_bast = on','cluster.read_scache = on',
    'cluster.crossnode_runtime_visibility = off','cluster.gcs_block_local_cache = on',
    'cluster.gcs_reply_timeout_ms = 2000','cluster.gcs_block_retransmit_max_retries = 12',
    'cluster.gcs_block_starvation_max_retries = 60']);
$pair->start_pair; usleep(3_000_000);
my ($n0,$n1)=($pair->node0,$pair->node1);
$pair->wait_for_peer_state(0,1,'connected',30); $pair->wait_for_peer_state(1,0,'connected',30);
sub sint { my ($n,$c,$k)=@_; my $v=$n->safe_psql('postgres',
  "SELECT value FROM pg_cluster_state WHERE category='$c' AND key='$k'");
  return defined($v)&&$v ne ''?int($v):0; }
sub dumpst { my ($tag)=@_;
  for my $p ([0,$n0],[1,$n1]) { my ($i,$n)=@$p;
    diag(sprintf("[%s] n%d parked=%d parkexp=%d bcast=%d aba=%d", $tag,$i,
      sint($n,'gcs','invalidate_parked_count'), sint($n,'gcs','invalidate_park_expired_count'),
      sint($n,'gcs','block_invalidate_broadcast_count'), sint($n,'pcm','restore_aba_detected_count')));
    diag(sprintf("[%s] n%d pcmparked=%d", $tag,$i, sint($n,'pcm','invalidate_parked_grant_pending_count'))); } }
my $tbl;
for my $i (1..12){ my $t="ra_t$i";
  $_->safe_psql('postgres',"CREATE TABLE $t (k int, v int) WITH (fillfactor=10)") for ($n0,$n1);
  my $p0=$n0->safe_psql('postgres',"SELECT pg_relation_filepath('$t')");
  my $p1=$n1->safe_psql('postgres',"SELECT pg_relation_filepath('$t')");
  if(($p0//'') eq ($p1//'')){$tbl=$t; last;}}
die 'no tbl' unless $tbl;
$n0->safe_psql('postgres',"INSERT INTO $tbl VALUES (1,1)");
$n0->safe_psql('postgres',"VACUUM (FREEZE) $tbl"); $n0->safe_psql('postgres','CHECKPOINT');
$n0->safe_psql('postgres',"SELECT count(*) FROM $tbl");
usleep(300_000);
$n0->safe_psql('postgres',"ALTER SYSTEM SET cluster.injection_points = 'cluster-pcm-drop-prepin-window:sleep:2500000,cluster-pcm-restore-aba-force-round:skip'");
$n0->safe_psql('postgres','SELECT pg_reload_conf()'); usleep(400_000);
my ($ri,$ro,$re)=('','',''); my $reqh=IPC::Run::start(
  ['psql','-X','-q','-d',$n1->connstr('postgres'),'-c',"UPDATE $tbl SET v=v+10 WHERE k=1"],\$ri,\$ro,\$re);
usleep(800_000);
my ($pi,$po,$pe)=('','',''); my $pinh=IPC::Run::start(
  ['psql','-X','-A','-t','-q','-d',$n0->connstr('postgres'),'-c',"SELECT count(*) FROM $tbl"],\$pi,\$po,\$pe);
for (1..40){ last if sint($n0,'pcm','restore_aba_detected_count')>=1; usleep(250_000); }
$pinh->finish; $reqh->finish;
$n0->safe_psql('postgres','ALTER SYSTEM RESET cluster.injection_points');
$n0->safe_psql('postgres','SELECT pg_reload_conf()');
dumpst('post-aba');
diag("attempt1 err=[".(($re//'')=~s/\n/ | /gr)."]");
# heal
my ($hrc,$ho,$he)=$n0->psql('postgres',"UPDATE $tbl SET v=v+100 WHERE k=1");
diag("heal rc=$hrc err=[".(($he//'')=~s/\n/ | /gr)."]");
$n0->safe_psql('postgres',"VACUUM (FREEZE) $tbl"); $n0->safe_psql('postgres','CHECKPOINT');
$n0->safe_psql('postgres',"SELECT count(*) FROM $tbl");
dumpst('post-heal');
# node1 retry once + dump
my ($rc2,$o2,$e2)=$n1->psql('postgres',"UPDATE $tbl SET v=v+10 WHERE k=1 RETURNING v");
diag("n1 retry rc=$rc2 out=[".($o2//'')."] err=[".(($e2//'')=~s/\n/ | /gr)."]");
dumpst('post-retry');


sub snap { my %h;
  for my $p ([0,$n0],[1,$n1]) { my ($i,$n)=@$p;
    my $rows=$n->safe_psql('postgres',
      "SELECT category||'.'||key||'='||value FROM pg_cluster_state WHERE category IN ('gcs','pcm') AND value ~ '^[0-9]+\$'");
    for my $r (split /\n/,$rows){ my ($k,$v)=split /=/,$r,2; $h{"n$i.$k"}=$v; } }
  return \%h; }
my $s1=snap();
my ($rc3,$o3,$e3)=$n1->psql('postgres',"UPDATE $tbl SET v=v+10 WHERE k=1 RETURNING v");
diag("n1 retry2 rc=$rc3 out=[".($o3//'')."]");
my $s2=snap();
for my $k (sort keys %$s2){
  my $d=($s2->{$k}//0)-($s1->{$k}//0);
  diag("DELTA $k +$d") if $d>0; }
ok(1); $pair->stop_pair; done_testing();

