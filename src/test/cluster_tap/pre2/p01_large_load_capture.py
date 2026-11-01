"""Capture four installed pgbench workers; no product or stop authority."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from wait_graph_capture import CaptureSession, capture_failed_watchdog, capture_failed_workload


def reject_server_errors(text):
    match = re.search(r'^.*\b(?:ERROR|PANIC):.*$', text, re.M)
    if match:
        raise RuntimeError('server error: ' + match.group(0)[:512])


def validate_worker(text, rc, elapsed):
    if rc != 0 or elapsed < 180:
        raise ValueError('failed or short workload')
    if re.search(r'^pgbench: (?:error|fatal):|\b(?:ERROR|FATAL|PANIC):', text, re.M | re.I):
        raise ValueError('client error or abort')

    def field(label):
        values = re.findall(r'^' + re.escape(label) + r':\s*(\d+)(?:\s|$)', text, re.M)
        if len(values) != 1:
            raise ValueError('missing or duplicate ' + label)
        return int(values[0])

    for label, wanted in [('number of clients', 4), ('number of threads', 4),
                          ('maximum number of tries', 1), ('duration', 180),
                          ('number of failed transactions', 0)]:
        if field(label) != wanted:
            raise ValueError('wrong ' + label)
    count = field('number of transactions actually processed')
    if count <= 0:
        raise ValueError('no successful transaction')
    for label in ['number of transactions retried', 'total number of retries',
                  'number of transactions skipped']:
        if re.search(r'^' + re.escape(label) + ':', text, re.M) and field(label) != 0:
            raise ValueError('retried or skipped transactions')
    return count


def run(config_path, output):
    config = json.loads(Path(config_path).read_text())
    if len(config) != 4 or {x['node'] for x in config} != set(range(4)):
        raise ValueError('exact four-node map required')
    if len({x['connstr'] for x in config}) != 4:
        raise ValueError('duplicate node connection')
    output = Path(output)
    output.mkdir(exist_ok=False)
    script = output / 'update.sql'
    script.write_text('\\set id random(1,131072)\nBEGIN;\n'
                      'UPDATE lab.stop_load SET v=v+1 WHERE id=:id;\nCOMMIT;\n')
    binary = shutil.which('pgbench')
    if binary is None:
        raise ValueError('installed pgbench missing')
    result = dict(started_unix=time.time(), status='RUNNING', workers=[],
                  keyspace=[1, 131072], clients_total=16)
    processes, streams, starts = [], [], []
    server_logs = []
    error = None
    diagnostic = None

    def save():
        path = output / 'result.json'
        tmp = output / 'result.json.tmp'
        tmp.write_text(json.dumps(result, indent=2) + '\n')
        tmp.replace(path)

    save()

    def check_server_logs():
        for entry in server_logs:
            text = entry['tail'] + entry['stream'].read()
            reject_server_errors(text)
            entry['tail'] = text[-16:]

    try:
        for node in config:
            server_logs.append(dict(stream=Path(node['logfile']).open(errors='replace'), tail=''))
        check_server_logs()
        diagnostic = CaptureSession(config, output / 'wait-graph')
        for node in sorted(config, key=lambda n: n['node']):
            n = node['node']
            command = [binary, '-n', '-c', '4', '-j', '4', '-T', '180', '-M', 'simple',
                       '--max-tries=1', '--random-seed=' + str(4180 + n),
                       '-f', str(script), node['connstr']]
            out = (output / ('node%d.stdout' % n)).open('x')
            err = (output / ('node%d.stderr' % n)).open('x')
            streams.extend([out, err])
            started = time.monotonic()
            process = subprocess.Popen(command, stdout=out, stderr=err,
                                       env=dict(os.environ, LC_ALL='C'))
            starts.append(started)
            processes.append(process)
            result['workers'].append(dict(node=n, command=command, pid=process.pid,
                                          started_unix=time.time()))
            save()
        deadline = starts[0] + 240
        while True:
            check_server_logs()
            if any(p.poll() not in (None, 0) for p in processes):
                raise RuntimeError('workload client exited nonzero')
            running = any(p.poll() is None for p in processes)
            if running and time.monotonic() >= deadline:
                result.update(status='FAIL', error='original watchdog expired')
                save()
                result['wait_graph'] = capture_failed_watchdog(diagnostic.capture)
                save()
                raise RuntimeError('workload watchdog expired; never a positive result')
            for index, process in enumerate(processes):
                if process.poll() is not None and 'elapsed_seconds' not in result['workers'][index]:
                    elapsed = time.monotonic() - starts[index]
                    result['workers'][index]['elapsed_seconds'] = elapsed
                    if elapsed < 180:
                        raise RuntimeError('workload client exited early')
            if not running:
                break
            time.sleep(.25)
        check_server_logs()
        # Validate completed output while the prepared observers still exist.
        # A simultaneous exit or an ERROR in a completed client's output must
        # reach the same first-failure cut as an error observed during polling.
        if len(result['workers']) != 4:
            raise ValueError('missing worker')
        for index, worker in enumerate(result['workers']):
            worker['rc'] = processes[index].returncode
            worker.setdefault('elapsed_seconds', time.monotonic() - starts[index])
            n = worker['node']
            text = (output / ('node%d.stdout' % n)).read_text()
            text += '\n' + (output / ('node%d.stderr' % n)).read_text()
            worker['commits'] = validate_worker(text, worker['rc'], worker['elapsed_seconds'])
            if worker.get('forced_client_cleanup'):
                raise ValueError('client was forcibly cleaned up')
    except BaseException as exc:
        error = repr(exc)
        # Freeze the original verdict before collecting and before terminating
        # any owned clients. The watchdog already took its one failure cut.
        result.update(status='FAIL', error=error)
        save()
        if 'wait_graph' not in result:
            if diagnostic is None:
                result['wait_graph'] = dict(status='FAIL', reason='original_workload_failed',
                                           capture_error='diagnostic_session_not_ready')
            else:
                result['wait_graph'] = capture_failed_workload(diagnostic.capture)
            save()
    finally:
        for index, process in enumerate(processes):
            worker = result['workers'][index]
            if process.poll() is None:
                worker['forced_client_cleanup'] = True
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            worker['rc'] = process.returncode
            worker.setdefault('elapsed_seconds', time.monotonic() - starts[index])
        if diagnostic is not None:
            try:
                diagnostic.close()
            except BaseException as exc:
                result['diagnostic_cleanup_error'] = repr(exc)
                if error is None:
                    error = repr(exc)
        for stream in streams:
            stream.close()
        for entry in server_logs:
            entry['stream'].close()
        result.update(finished_unix=time.time(), error=error,
                      status='PASS' if error is None else 'FAIL')
        if error is None:
            result['commits'] = sum(x['commits'] for x in result['workers'])
        save()
    print(json.dumps(dict(status=result['status'], commits=result.get('commits'), error=error)), flush=True)
    return 0 if error is None else 1


if __name__ == '__main__':
    sys.exit(run(*sys.argv[1:]))
