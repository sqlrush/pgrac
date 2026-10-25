"""Installed four-node HW probe; only copied native evidence can qualify."""
import json
from pathlib import Path
import sys
from wait_graph_capture import CaptureSession


def require_hw_edge(graph, holder_pid, waiter_pid, resid):
    if graph.get('complete') is not True or graph.get('unknown') != []:
        raise ValueError('native graph is incomplete')
    processes = []
    for node,pid in ((0,holder_pid),(1,waiter_pid)):
        rows = [row for row in graph['vertices'] if row['node']==node and row['pid']==pid]
        if len(rows) != 1:
            raise ValueError('native process incarnation missing or ambiguous')
        processes.append(rows[0]['id'])
    holder,waiter = processes
    edges = [edge for edge in graph['edges'] if edge['from']==waiter and edge['to']==holder and
             edge['quality']=='exact_ges_holder' and edge['resource'].get('kind')=='enqueue' and
             edge['resource'].get('resid')==resid]
    if len(edges) != 1:
        raise ValueError('expected exact native HW dependency missing or ambiguous')
    return edges[0]


def main(argv):
    config_path,output_path,holder_text,waiter_text,key_text = argv
    config = json.loads(Path(config_path).read_text())
    holder,waiter,resid = int(holder_text),int(waiter_text),json.loads(key_text)
    if (not 0 < holder < 2**31 or not 0 < waiter < 2**31 or
            not isinstance(resid,list) or len(resid)!=6 or
            any(type(value) is not int or not 0 <= value < 2**32 for value in resid) or
            resid[3:] != [0,242,1]):
        raise ValueError('invalid independently observed native HW identity')
    output = Path(output_path)
    diagnostic = None
    result = dict(status='FAIL',holder_pid=holder,waiter_pid=waiter,resid=resid)
    try:
        diagnostic = CaptureSession(config,output)
        result['capture'] = diagnostic.capture()
        evidence = json.loads((output/'wait-for-graph.json').read_text())
        result['edge'] = require_hw_edge(evidence,holder,waiter,resid)
        result['status'] = 'PASS'
    except Exception as exc:
        result['error'] = repr(exc)
    finally:
        if diagnostic is not None:
            try:
                diagnostic.close()
            except Exception as exc:
                result.update(status='FAIL',cleanup_error=repr(exc))
    if output.is_dir():
        (output/'probe-result.json').write_text(json.dumps(result,indent=2)+'\n')
    return 0 if result['status']=='PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
