#!/usr/bin/env python3
"""Control the four preinstalled PGRAC instances in a single Linux VM."""
from concurrent.futures import ThreadPoolExecutor
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import pwd
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

from quorum import Quorum
from results import EVENT, score_point, shutdown_ok, startup_ready

ROOT = Path('/srv/pgrac/demo')
RUN = Path('/run/pgrac-single')
QUORUM = Path('/var/lib/pgrac-single/services')
OUT = Path('/var/log/pgrac-single')
BIN = Path('/opt/pgrac/bin')
MANIFEST = Path('/etc/pgrac-single/manifest.json')
HERE = Path(__file__).resolve().parent
SOURCE = '2d857abffc76a5cbfbc1b01c7d99b82ab0375f46'


def write_json(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, indent=2)+'\n')
    temp.replace(path)


def verify():
    if os.geteuid() != 0:
        raise RuntimeError('sudo is required')
    value = json.loads(MANIFEST.read_text())
    if value['source_commit'] != SOURCE or value['release_label'] != 'v0.135.0':
        raise RuntimeError('unexpected appliance identity')
    if hashlib.sha256((BIN/'postgres').read_bytes()).hexdigest() != value['postgres_sha256']:
        raise RuntimeError('installed postgres identity mismatch')
    for n in range(4):
        if not (ROOT/f'node{n}/data/PG_VERSION').is_file():
            raise RuntimeError('preinstalled database is missing; do not reinitialize')
    OUT.mkdir(mode=0o700, exist_ok=True)
    return value


def all_nodes(fn):
    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(fn, range(4)))


def command(argv, timeout=180):
    begin = time.monotonic()
    try:
        p = subprocess.run(list(map(str, argv)), text=True, capture_output=True, timeout=timeout)
        return dict(rc=p.returncode, stdout=p.stdout, stderr=p.stderr, seconds=time.monotonic()-begin)
    except subprocess.TimeoutExpired as e:
        def text(v):
            return v.decode(errors='replace') if isinstance(v, bytes) else v or ''
        return dict(rc=124, stdout=text(e.stdout), stderr=text(e.stderr),
                    seconds=time.monotonic()-begin, timeout=True)


def checked(argv, timeout=180):
    result = command(argv, timeout)
    if result['rc']:
        raise RuntimeError(f'command failed ({result["rc"]}): {result["stderr"]}')
    return result


def handles():
    return json.loads((QUORUM/'handles.json').read_text())


def node_command(n, argv):
    h = handles()
    entry = h['nodes'][n]
    if Path(f'/proc/{entry["pid"]}/ns/net').stat().st_ino != entry['net_inode']:
        raise RuntimeError('namespace owner changed')
    return ['nsenter', '-t', str(entry['pid']), '-n', '-m', '-i',
            'setpriv', '--reuid', str(h['uid']), '--regid', str(h['gid']), '--init-groups',
            'env', 'LD_LIBRARY_PATH=/opt/pgrac/lib', *map(str, argv)]


def sql(n, query, timeout=180):
    return checked(node_command(n, [BIN/'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
        '-U', 'pgrac', '-h', ROOT/'sockets', '-p', str(5540+n), '-d', 'postgres', '-c', query]), timeout)


def data(n):
    return ROOT/f'node{n}/data'


def logfile(n):
    return ROOT/f'node{n}/postgres.log'


def pg_state(n):
    return command(node_command(n, [BIN/'pg_ctl', '-D', data(n), 'status']), 10)


def normal_stop():
    def one(n):
        offset = logfile(n).stat().st_size
        result = command(node_command(n, [BIN/'pg_ctl', '-D', data(n), '-m', 'fast',
                                          '-w', '-t', '30', 'stop']), 35)
        with logfile(n).open() as f:
            f.seek(offset)
            result['log'] = f.read()
        result.update(node=n, stopped=pg_state(n)['rc'] == 3)
        return result
    begin = time.monotonic()
    nodes = all_nodes(one)
    return dict(status='STOPPED' if shutdown_ok(nodes) else 'NORMAL_STOP_FAILED',
                seconds=time.monotonic()-begin, nodes=nodes)


def serve():
    """Detached owner keeps its exact namespace and process handles until stop."""
    verify()
    user = pwd.getpwnam('pgrac')
    q = Quorum(QUORUM, user.pw_uid, user.pw_gid)
    started = time.monotonic()
    try:
        q.start()
        for n in range(3):
            loop = q.attach_vote(ROOT/f'vote{n}.img')
            alias = Path(f'/dev/pgrac-vote{n}')
            if os.path.lexists(alias):
                raise RuntimeError('voting alias already exists; preserve it')
            alias.symlink_to(loop)
        offsets = {n: logfile(n).stat().st_size if logfile(n).exists() else 0 for n in range(4)}
        for n in range(4):
            checked(node_command(n, [BIN/'pg_ctl', '-D', data(n), '-l', logfile(n), '-W', 'start']))
        end = time.monotonic()+180
        while True:
            ready = []
            for n in range(4):
                ready.append(startup_ready(data(n), logfile(n), offsets[n]))
            if all(ready):
                break
            if time.monotonic() >= end:
                raise RuntimeError('four-instance startup exceeded 180 seconds')
            time.sleep(.25)
        for n in range(4):
            if sql(n, 'SHOW cluster.node_id', 10)['stdout'].strip() != str(n):
                raise RuntimeError('connection reached the wrong instance')
        write_json(RUN/'state.json', dict(status='STARTED', seconds=time.monotonic()-started,
                                         pid=os.getpid(), nodes=list(range(4))))
        while not (RUN/'stop-request').exists():
            time.sleep(.2)
        result = normal_stop()
        write_json(OUT/'last-stop.json', result)
        write_json(RUN/'state.json', result)
        if result['status'] != 'STOPPED':
            # Leave membership and storage present while support inspects the failure.
            while True:
                time.sleep(30)
        q.stop()
        for n in range(3):
            Path(f'/dev/pgrac-vote{n}').unlink()
        (QUORUM/'stopped').touch()
        (RUN/'owner-done').touch()
    except Exception as error:
        write_json(RUN/'state.json', dict(status='START_FAILED', error=str(error), pid=os.getpid()))
        # Never tear down authority underneath databases after a partial startup.
        while True:
            time.sleep(30)


def start():
    if QUORUM.exists():
        if not (QUORUM/'stopped').exists() or any((data(n)/'postmaster.pid').exists() for n in range(4)):
            raise RuntimeError('previous services were not cleanly stopped; preserve their state')
        shutil.rmtree(QUORUM)  # Only completed appliance service files and disposable credentials.
    if RUN.exists():
        if (RUN/'owner-done').exists():
            if any((data(n)/'postmaster.pid').exists() for n in range(4)):
                raise RuntimeError('DATA still has a postmaster PID; preserve it')
            shutil.rmtree(RUN)  # Only this tool's completed ephemeral runtime.
        else:
            raise RuntimeError('appliance runtime already exists; inspect status')
    RUN.mkdir(mode=0o700)
    with (OUT/'owner.log').open('a') as log:
        p = subprocess.Popen([sys.executable, __file__, 'serve'], stdin=subprocess.DEVNULL,
            stdout=log, stderr=log, start_new_session=True)
    (RUN/'owner.pid').write_text(str(p.pid)+'\n')
    end = time.monotonic()+300
    while time.monotonic() < end:
        path = RUN/'state.json'
        if path.exists():
            result = json.loads(path.read_text())
            if result['status'] != 'STARTED':
                raise RuntimeError(str(result))
            return result
        if p.poll() is not None:
            raise RuntimeError('appliance owner exited; see owner.log')
        time.sleep(.25)
    raise RuntimeError('start did not finish; runtime and logs retained')


def stop():
    if (RUN/'owner-done').exists():
        return json.loads((OUT/'last-stop.json').read_text())
    state = json.loads((RUN/'state.json').read_text())
    if state['status'] != 'STARTED':
        raise RuntimeError('runtime is not in STARTED state; retain data and logs')
    (RUN/'stop-request').touch()
    end = time.monotonic()+120
    while time.monotonic() < end:
        state = json.loads((RUN/'state.json').read_text())
        if state['status'] == 'NORMAL_STOP_FAILED':
            return state
        if (RUN/'owner-done').exists():
            return state
        time.sleep(.25)
    raise RuntimeError('normal stop did not finish; VM must remain running')


def example():
    before = all_nodes(lambda n: sql(n, 'SELECT count(*),count(DISTINCT id) FROM demo.idx_lookup'))
    if any(r['stdout'].strip() != '50000|50000' for r in before):
        raise RuntimeError('example row count or uniqueness mismatch')
    tokens = [uuid.uuid4().hex.ljust(80, 'x') for _ in range(4)]
    all_nodes(lambda n: sql(n, f"BEGIN; UPDATE demo.idx_lookup SET payload='{tokens[n]}' WHERE id={n+1}; COMMIT;"))
    after = all_nodes(lambda n: sql(n, 'SELECT id,payload FROM demo.idx_lookup WHERE id BETWEEN 1 AND 4 ORDER BY id'))
    expected = '\n'.join(f'{n+1}|{tokens[n]}' for n in range(4))
    if any(r['stdout'].strip() != expected for r in after):
        raise RuntimeError('example results differ or updates were not preserved')
    return dict(status='EXAMPLE_PASS', nodes=4, rows=50000, transactions=4)


def replace_factory_data(archive, expected_sha256, root):
    """Replace a stopped example cohort from its immutable cold snapshot."""
    root, archive = Path(root), Path(archive)
    if root.is_symlink() or not root.is_dir() or any(root.glob('node*/data/postmaster.pid')):
        raise RuntimeError('example DATA must be a stopped directory')
    digest = hashlib.sha256()
    with archive.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    if digest.hexdigest() != expected_sha256:
        raise RuntimeError('factory example snapshot checksum mismatch')
    with tarfile.open(archive, 'r:gz') as source:
        members = source.getmembers()
        links = set()
        for member in members:
            name = PurePosixPath(member.name)
            if name.is_absolute() or '..' in name.parts or not name.parts or name.parts[0] != root.name:
                raise RuntimeError('factory snapshot contains an invalid path')
            if not (member.isfile() or member.isdir() or member.issym()):
                raise RuntimeError('factory snapshot contains an unsupported file type')
            if member.issym():
                target = PurePosixPath(member.linkname)
                if not target.is_absolute() or '..' in target.parts or str(root) not in map(str, target.parents):
                    raise RuntimeError('factory snapshot link leaves example DATA')
                links.add(name)
        if any(parent in links for m in members for parent in PurePosixPath(m.name).parents):
            raise RuntimeError('factory snapshot writes through a symbolic link')
    # Fully unpack and check the replacement before touching the previous DATA.
    staging = Path(tempfile.mkdtemp(prefix='.pgrac-example-', dir=root.parent))
    previous = staging / 'previous'
    try:
        subprocess.run(['tar', '-xzpf', str(archive), '-C', str(staging)], check=True)
        fresh = staging / root.name
        required = [fresh / f'node{n}/data/PG_VERSION' for n in range(4)]
        required += [fresh / f'vote{n}.img' for n in range(3)]
        directories = {fresh, fresh / 'data', fresh / 'wal'}
        for path in required:
            directories.update(p for p in path.parents if p == fresh or fresh in p.parents)
        if (not all(p.is_file() and not p.is_symlink() for p in required)
                or not all(p.is_dir() and not p.is_symlink() for p in directories)
                or any(fresh.glob('node*/data/postmaster.pid'))):
            raise RuntimeError('factory snapshot is incomplete or was not stopped')
        root.rename(previous)
        try:
            fresh.rename(root)
        except BaseException:
            previous.rename(root)
            raise
        shutil.rmtree(previous)
    finally:
        # Keep a previous DATA tree if a replacement/rollback did not complete.
        if not previous.exists():
            shutil.rmtree(staging)


def reset_example():
    manifest = verify()
    archive = HERE / 'factory-data.tar.gz'
    expected = manifest['factory_data_sha256']
    if RUN.exists():
        result = stop()
        if result['status'] != 'STOPPED':
            raise RuntimeError('normal stop failed; example DATA has been preserved')
    if command(['pgrep', '-x', 'postgres'], 10)['rc'] != 1:
        raise RuntimeError('a database process remains; example DATA has been preserved')
    dest = OUT / ('reset-' + uuid.uuid4().hex)
    dest.mkdir(mode=0o700)
    for n in range(4):
        if logfile(n).is_file():
            shutil.copyfile(logfile(n), dest / f'node{n}-before-reset.log')
    begin = time.monotonic()
    replace_factory_data(archive, expected, ROOT)
    receipt = dict(status='EXAMPLE_RESET', sha256=expected,
                   seconds=time.monotonic()-begin, output=str(dest))
    write_json(dest / 'result.json', receipt)
    return receipt


def fresh_bench(clients, seconds):
    reset = reset_example()
    start_result = start()
    if sql(0, 'SELECT count(*),count(DISTINCT id) FROM demo.idx_lookup')['stdout'].strip() != '50000|50000':
        raise RuntimeError('factory example row count or uniqueness mismatch')
    result = bench(clients, seconds)
    result.update(factory_reset=reset, startup_seconds=start_result['seconds'])
    write_json(Path(result['output']) / 'result.json', result)
    return result


def bench(clients, seconds):
    for n in range(4):
        if sql(n, 'SHOW cluster.crossnode_write_write', 10)['stdout'].strip() != 'on':
            raise RuntimeError('point-update workload requires the supplied crossnode_write_write setting')
    dest = OUT/('bench-'+time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())+'-'+uuid.uuid4().hex[:8])
    dest.mkdir(mode=0o700)
    outputs, procs, starts, ends = {}, {}, {}, {}
    offsets = {n: logfile(n).stat().st_size for n in range(4)}
    cursors, partial = dict(offsets), {n: '' for n in range(4)}
    try:
        for n in range(4):
            outputs[n] = (dest/f'node{n}.txt').open('w')
            argv = node_command(n, [BIN/'pgbench', '-n', '-M', 'prepared', '-U', 'pgrac',
                '-h', ROOT/'sockets', '-p', str(5540+n), '-c', str(clients),
                '-j', '4', '-T', str(seconds), '-P', '10', '-f', HERE/'point-update.sql', 'postgres'])
            starts[n] = time.monotonic()
            procs[n] = subprocess.Popen(argv, stdout=outputs[n], stderr=subprocess.STDOUT, start_new_session=True)
        failure = None
        deadline = time.monotonic()+seconds+30
        while len(ends) < 4:
            for n, p in procs.items():
                if p.poll() is not None and n not in ends:
                    ends[n] = time.monotonic()
                    if p.returncode and failure is None:
                        failure = f'node{n} pgbench rc{p.returncode}'
                with logfile(n).open() as f:
                    f.seek(cursors[n])
                    text = partial[n]+f.read()
                    cursors[n] = f.tell()
                    partial[n] = text.rsplit('\n', 1)[-1]
                    event = EVENT.search(text)
                    if event and failure is None:
                        failure = f'node{n}: {event.group(0)}'
            if failure or time.monotonic() > deadline:
                failure = failure or 'load did not finish within its deadline'
                break
            time.sleep(.2)
    finally:
        for n, p in procs.items():
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGINT)
        cancel_deadline = time.monotonic()+10
        for n, p in procs.items():
            if p.poll() is None:
                try:
                    p.wait(timeout=max(.01, cancel_deadline-time.monotonic()))
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid, signal.SIGTERM)
                    try:
                        p.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(p.pid, signal.SIGKILL)
                        p.wait()  # Never release ownership while a client survives.
            ends.setdefault(n, time.monotonic())
        for stream in outputs.values():
            stream.close()
    events = [failure] if failure else []
    rows = []
    for n, p in procs.items():
        with logfile(n).open() as f:
            f.seek(offsets[n])
            tail = f.read()
        (dest/f'node{n}-database.log').write_text(tail)
        events.extend(EVENT.findall(tail))
        rows.append(dict(node=n, rc=p.returncode, started=starts[n], ended=ends[n],
                         stdout=(dest/f'node{n}.txt').read_text()))
    result = dict(score_point(rows, events), clients_per_instance=clients,
                  requested_seconds=seconds, output=str(dest))
    write_json(dest/'result.json', result)
    return result


def sweep(seconds):
    rows = []
    for clients in (16, 32, 48, 64):
        value = fresh_bench(clients, seconds)
        rows.append(value)
        print(json.dumps(value), flush=True)
        if not value['valid']:
            break
    valid = [r for r in rows if r['valid']]
    best = max(valid, key=lambda r: r['common_window_tps']) if valid else None
    return dict(status='SWEEP_PASS' if len(rows) == 4 and len(valid) == 4 else 'SWEEP_INCOMPLETE',
                points=rows, best_observed_clients=best['clients_per_instance'] if best else None,
                best_observed_tps=best['common_window_tps'] if best else None)


def job_path(name):
    if not re.fullmatch('[0-9a-f]{32}', name):
        raise ValueError('invalid job identifier')
    return OUT/'jobs'/name


def launch_sweep(seconds):
    name = uuid.uuid4().hex
    path = job_path(name)
    path.mkdir(mode=0o700, parents=True)
    write_json(path/'status.json', dict(job=name, status='RUNNING', rc=None))
    with (path/'owner.log').open('w') as log:
        subprocess.Popen([sys.executable, __file__, 'sweep-job', '--job', name,
            '--seconds', str(seconds)], stdin=subprocess.DEVNULL, stdout=log, stderr=log,
            start_new_session=True)
    return dict(job=name, status='RUNNING', output=str(path))


def sweep_job(name, seconds):
    path = job_path(name)
    # The worker and pgbench processes survive loss of the caller's SSH session.
    # Each point owns its deadline and client cleanup. Do not kill the owner
    # with an outer timeout that would bypass its finally block.
    begin = time.monotonic()
    with (path/'stdout.log').open('w') as stdout, (path/'stderr.log').open('w') as stderr:
        process = subprocess.run([sys.executable, __file__, 'sweep', '--seconds', str(seconds)],
                                 stdout=stdout, stderr=stderr)
    result = dict(job=name, status='DONE', rc=process.returncode, seconds=time.monotonic()-begin)
    if result['rc'] == 0:
        result['result'] = json.loads((OUT/'last-sweep.json').read_text())
    write_json(path/'status.json', result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'serve', 'status', 'example', 'stop', 'sql',
                                         'bench', 'sweep', 'launch-sweep', 'sweep-job', 'job-status'])
    parser.add_argument('--node', type=int, choices=range(4), default=0)
    parser.add_argument('--query')
    parser.add_argument('--clients', type=int, choices=[16, 32, 48, 64], default=16)
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--job')
    args = parser.parse_args()
    verify()
    if not 1 <= args.seconds <= 180:
        parser.error('--seconds must be between 1 and 180')
    if args.action == 'serve':
        serve()
        return
    if args.action == 'sweep-job':
        sweep_job(args.job or '', args.seconds)
        return
    if args.action == 'launch-sweep':
        print(json.dumps(launch_sweep(args.seconds)))
        return
    if args.action == 'job-status':
        print((job_path(args.job or '')/'status.json').read_text())
        return
    if args.action in ('bench', 'sweep'):
        def interrupted(signum, frame):
            raise KeyboardInterrupt('measurement interrupted; cleaning up owned clients')
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
    with open('/run/pgrac-single-command.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.action == 'sql':
            if not args.query:
                parser.error('--query is required')
            result = sql(args.node, args.query)
        elif args.action == 'bench':
            result = fresh_bench(args.clients, args.seconds)
        elif args.action == 'sweep':
            result = sweep(args.seconds)
        elif args.action == 'status':
            result = json.loads((RUN/'state.json').read_text())
            if result['status'] == 'STARTED':
                result['instances'] = all_nodes(lambda n: dict(node=n, **pg_state(n)))
        else:
            result = globals()[args.action]()
        write_json(OUT/('last-'+args.action+'.json'), result)
        print(json.dumps(result, indent=2), flush=True)
        if result.get('valid') is False or result.get('status') in ('NORMAL_STOP_FAILED', 'SWEEP_INCOMPLETE'):
            raise SystemExit(1)


if __name__ == '__main__':
    main()
