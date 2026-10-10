"""Read pgbench and normal-shutdown results without hiding failed samples."""
import re
from pathlib import Path

EVENT = re.compile(r'\b(?:ERROR|FATAL|PANIC):[^\n]*')


def startup_ready(data, logfile, offset):
    try:
        with Path(logfile).open(errors='replace') as f:
            f.seek(offset)
            text = f.read()
        if EVENT.search(text):
            raise RuntimeError('startup error; see complete database log')
        lines = (Path(data)/'postmaster.pid').read_text().splitlines()
    except FileNotFoundError:
        return False
    return (len(lines) >= 8 and lines[0].isdigit() and int(lines[0]) > 0
            and Path(lines[1]).resolve() == Path(data).resolve() and lines[7].strip() == 'ready')


def score_point(nodes, events):
    parsed = []
    for node in nodes:
        output = node.get('stdout', '')
        count = re.search(r'^number of transactions actually processed: (\d+)', output, re.M)
        failures = re.search(r'^number of failed transactions: (\d+)', output, re.M)
        tps = re.search(r'^tps = ([0-9.]+) \(without initial connection time\)', output, re.M)
        parsed.append(dict(node, transactions=int(count[1]) if count else None,
                           failures=int(failures[1]) if failures else None,
                           tps=float(tps[1]) if tps else None))
    valid = len(parsed) == 4 and not events and all(
        n.get('rc') == 0 and n['transactions'] and n['transactions'] > 0
        and n['failures'] == 0 and n['tps'] is not None
        and n['ended'] > n['started'] for n in parsed)
    total = sum(n['transactions'] or 0 for n in parsed)
    elapsed = max((n['ended'] for n in parsed), default=0) - min(
        (n['started'] for n in parsed), default=0)
    return dict(valid=bool(valid), nodes=parsed, events=events, transactions=total,
                common_window_seconds=elapsed,
                common_window_tps=total/elapsed if valid else None,
                sum_pgbench_tps=sum(n['tps'] for n in parsed) if valid else None)


def shutdown_ok(nodes):
    return len(nodes) == 4 and all(
        n.get('rc') == 0 and n.get('stopped')
        and 'database system is shut down' in n.get('log', '')
        and not EVENT.search(n['log']) for n in nodes)
