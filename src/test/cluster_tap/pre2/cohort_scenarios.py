"""Run the unmodified first-chain TAPs with a live local cohort profile.

No inferred capability or successful observation is supplied by this runner.
Author: SqlRush <sqlrush@gmail.com>
"""
import hashlib
import json
import os
from pathlib import Path
import re


def run_taps(quorum, args, entry):
    if args.nodes != 4 or not args.pg_regress:
        raise ValueError('first-chain TAP requires four nodes and pg_regress')
    source = args.tap_source.resolve(strict=True)
    profile = json.loads(entry.read_text())
    results = []
    for case in ('431_pre2_start_stop_restart', '432_pre2_two_recoverers', '433_pre2_remote_x_scache'):
        root = args.output.resolve()/'tap'/case
        for path in (root, root/'log', root/'data'):
            path.mkdir(parents=True)
            os.chown(path, args.uid, args.gid)
        # A successfully executed predecessor changes runtime voting records.
        # Every next cohort requires new formatted devices, even after CLOSED.
        case_profile = json.loads(json.dumps(profile))
        votes = []
        for n in range(3):
            image = root/f'vote{n}.image'
            quorum.run(['setpriv', '--reuid', str(args.uid), '--regid', str(args.gid), '--init-groups',
                        profile['fixture']['vote_formatter'], str(image), str(n)])
            votes.append(quorum.attach_vote(image))
        case_profile['fixture']['votes'] = votes
        case_entry = root/'entry.json'
        case_entry.write_text(json.dumps(case_profile, indent=2)+'\n')
        argv = ['setpriv', '--reuid', str(args.uid), '--regid', str(args.gid), '--init-groups',
                'env', 'PATH='+str(args.install.resolve()/'bin')+':'+os.environ['PATH'],
                'PGRAC_PRE2_BINDIR='+str(args.install.resolve()/'bin'),
                'PGRAC_PRE2_ENTRY_FILE='+str(case_entry), 'PG_TEST_NOCLEAN=1',
                'TESTLOGDIR='+str(root/'log'), 'TESTDATADIR='+str(root/'data'),
                'PG_REGRESS='+str(args.pg_regress.resolve(strict=True)),
                'prove', '-v', '-I', str(source/'src/test/perl'),
                str(source/'src/test/cluster_tap/t'/(case+'.pl'))]
        answer = quorum.run(argv, check=False, cwd=root, timeout=180)
        text = answer.stdout+answer.stderr
        (root/'prove.log').write_text(text)
        passed = answer.returncode == 0 and 'Result: PASS' in text and not re.search(r'#\s*skip\b', text, re.I)
        cleanup_error = None
        for layout_file in (root/'data').rglob('blackbox-layout.json'):
            nodes = json.loads(layout_file.read_text())['nodes']
            if any((Path(n['data_dir'])/'postmaster.pid').exists() for n in nodes):
                # This is only failing-case cleanup, never normal-stop proof.
                from cohort_probe import cleanup
                passed = False
                cleanup_error = 'case left postmaster.pid; error cleanup required'
                cleanup(quorum, nodes, args.install.resolve()/'bin')
        # These absent lifecycle entries are specified by BLACKBOX.md and are
        # checked before allocating DATA. Arbitrary runtime failures stay FAIL.
        dependency = next((name for name in ('root_observation', 'open_observation', 'shutdown_observation')
            if name not in profile['operations'] and '# BLOCKED: PRE2 adapter missing '+name in text), None)
        if cleanup_error:
            dependency = None
        results.append(dict(case=case, status='PASS' if passed else 'BLOCKED' if dependency else 'FAIL',
                            raw_tap='PASS' if passed else 'FAIL', rc=answer.returncode,
                            dependency=dependency, log=str(root/'prove.log'),
                            cleanup_error=cleanup_error,
                            case_sha256=hashlib.sha256((source/'src/test/cluster_tap/t'/(case+'.pl')).read_bytes()).hexdigest(),
                            entry_sha256=hashlib.sha256(case_entry.read_bytes()).hexdigest()))
        (args.output/'tap-results.json').write_text(json.dumps(results, indent=2)+'\n')
    return results
