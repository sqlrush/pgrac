"""Execute the matrix exception boundary without a database or VM."""
import json
from pathlib import Path
import subprocess
import unittest

HERE = Path(__file__).resolve().parent


class CaseIsolation(unittest.TestCase):
    def test_rollover_and_cleanup_errors_preserve_all_later_cells_and_cancel(self):
        code = r'''use M2CanonicalCases qw(run_cases);
use JSON::PP;
my (@ran, @cleaned, @records);
my @cases = map { {number=>$_} } 1..9;
run_cases(\@cases,
 sub { my ($c)=@_; push @ran,$c->{number}; die "rollover 57014\n" if $c->{number}==4; },
 sub { my ($c)=@_; push @cleaned,$c->{number}; die "cleanup failed\n" if $c->{number}==5; },
 sub { my ($c,$result)=@_; push @records,{number=>$c->{number},%$result}; });
print encode_json({ran=>\@ran,cleaned=>\@cleaned,records=>\@records});
'''
        result = subprocess.run(['perl', '-I'+str(HERE), '-e', code], text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.assertEqual(data['ran'], [1,2,3,4,5,6,7,8,9])
        self.assertEqual(data['cleaned'], [1,2,3,4,5,6,7,8,9])
        self.assertEqual([r['number'] for r in data['records']], [1,2,3,4,5,6,7,8,9])
        self.assertEqual(data['records'][3]['error'], 'rollover 57014\n')
        self.assertEqual(data['records'][3]['cleanup_error'], '')
        self.assertEqual(data['records'][4]['error'], '')
        self.assertEqual(data['records'][4]['cleanup_error'], 'cleanup failed\n')
        self.assertTrue(all(r['completed'] for i,r in enumerate(data['records']) if i!=3))
        self.assertFalse(data['records'][3]['completed'])

    def test_cleanup_runs_after_body_failure_and_both_errors_survive(self):
        code = r'''use M2CanonicalCases qw(run_cases);
use JSON::PP;
my @records;
run_cases([{number=>1}],sub { die "body\n"; },sub { die "cleanup\n"; },
 sub { my ($c,$r)=@_; push @records,$r; });
print encode_json(\@records);
'''
        result = subprocess.run(['perl', '-I'+str(HERE), '-e', code], text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), [dict(completed=0, error='body\n',
                                                        cleanup_error='cleanup\n')])

    def test_recording_failure_is_tap_failure_and_keeps_later_cases(self):
        code = r'''use M2CanonicalCases qw(run_cases);
use JSON::PP;
use Test::More;
my (@ran,@records);
run_cases([map { {number=>$_} } 1..9],sub { push @ran,$_[0]{number}; },sub {},
 sub { my ($c,$r)=@_; die "record sink unavailable\n" if $c->{number}==4;
       push @records,$c->{number}; });
print "JSON ".encode_json({ran=>\@ran,records=>\@records})."\n";
done_testing();
'''
        result = subprocess.run(['perl', '-I'+str(HERE), '-e', code], text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 1, result.stderr)
        data = json.loads(next(line[5:] for line in result.stdout.splitlines()
                               if line.startswith('JSON ')))
        self.assertEqual(data['ran'], [1,2,3,4,5,6,7,8,9])
        self.assertEqual(data['records'], [1,2,3,5,6,7,8,9])
        self.assertIn('not ok 1 - cell 4: evidence recording failed', result.stdout)
        self.assertIn('record sink unavailable', result.stderr)


if __name__ == '__main__':
    unittest.main()
