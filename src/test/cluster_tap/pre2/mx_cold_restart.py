#!/usr/bin/env python3
"""Run the cleared-vote rejection only against an isolated cold copy.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import argparse
import json
from pathlib import Path
import re
import shutil
import time

from mx_cold_copy import sha

IMAGE_NAMES = ('pgrac_control_root', 'pgrac_control_root.bak',
               'pgrac_xid_authority', 'pgrac_xid_authority.bak')


def prepare(capture_path, destination, votes, logdir):
    capture = json.loads(Path(capture_path).read_text())
    destination, logdir = Path(destination).resolve(), Path(logdir).resolve()
    original = Path(capture['original_data_root']).resolve(strict=True)
    saved = Path(capture['data']).resolve(strict=True)
    if destination.exists() or destination.is_relative_to(original) or destination.is_relative_to(saved):
        raise ValueError('negative DATA must be a new, independent directory')
    if len(votes) != 3 or len(set(votes)) != 3:
        raise ValueError('negative fixture requires three copied voting devices')
    layout = capture['layout']
    for node in layout['nodes']:
        if (Path(node['data_dir'])/'postmaster.pid').exists():
            raise ValueError('original cohort must remain stopped')
    destination.mkdir(mode=0o700)
    shutil.copytree(saved, destination/'data', symlinks=True)
    logdir.mkdir(mode=0o700, exist_ok=True)
    bindings = [dict(original=str(original), copy=str(destination/'data'))]
    for alias, device in zip(layout['voting_disks'], votes):
        target = str(Path(alias).resolve(strict=True))
        if target == str(Path(device).resolve(strict=True)):
            raise ValueError('negative voting device aliases the original')
        bindings.append(dict(original=target, copy=str(device)))
    # Native COMMON configuration contains absolute discovery paths. Preserve
    # every config and link byte; the root test controller binds these copies
    # over the same names inside its private mount namespaces only.
    expected = []
    for node in layout['nodes']:
        copied = destination/'data'/Path(node['data_dir']).resolve(strict=True).relative_to(original)
        info = copied.stat()
        expected.append(dict(node=node['id'], device=info.st_dev, inode=info.st_ino))
        node['logfile'] = str(logdir/f'node{node["id"]}.log')
    layout_path = destination/'layout.json'
    layout_path.write_text(json.dumps(layout, indent=2)+'\n')
    (destination/'mount-view.json').write_text(json.dumps(dict(
        bindings=bindings, expected_nodes=expected), indent=2)+'\n')
    return layout_path


def images(layout, directory):
    directory.mkdir(mode=0o700)
    result = {}
    shared = Path(layout['shared_data_dir'])
    for name in IMAGE_NAMES:
        relative = Path('global')/name
        path = shared/relative
        if path.is_symlink() or not path.is_file():
            raise ValueError('missing native control/authority image: ' + name)
        saved = directory/relative
        saved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, saved)
        result[str(relative)] = sha(saved)
    return result


def isolated_layout(layout_path):
    """Require the controller to see the copied DATA, never the original."""
    layout = json.loads(Path(layout_path).read_text())
    root = Path(layout_path).resolve(strict=True).parent
    view = json.loads((root/'mount-view.json').read_text())
    expected = {item['node']: item for item in view['expected_nodes']}
    for node in layout['nodes']:
        current = Path(node['data_dir']).stat()
        if (current.st_dev, current.st_ino) != (expected[node['id']]['device'], expected[node['id']]['inode']):
            raise ValueError('controller is not in the isolated cold-copy mount view')
    return layout


def stop_copy(layout_path, entry_path, bindir):
    from cohort_entry import CohortEntry
    layout = isolated_layout(layout_path)
    driver = CohortEntry(layout, bindir, json.loads(Path(entry_path).read_text()))
    driver.deadline = time.monotonic() + 30
    cleanup = []
    for node in layout['nodes']:
        if not driver.is_stopped(node):
            reply = driver.command('pg_ctl', ['stop', '-D', node['data_dir'],
                '-m', 'immediate', '-w', '-t', '10'], check=False, seconds=12)
            cleanup.append(dict(node=node['id'], rc=reply.returncode))
    return dict(cleanup=cleanup,
                all_stopped=all(driver.is_stopped(node) for node in layout['nodes']))


def verify(result):
    assert result['refused'], 'cleared votes unexpectedly reached OPEN'
    assert not any(node['configuration_mismatch'] for node in result['nodes']), 'copy changed native configuration identity'
    assert result['all_stopped'], 'negative fixture left a live process'
    assert any(node['fatal'] and node['limit_refusal'] for node in result['nodes']), 'no actual MultiXact limit FATAL'
    assert not any(node['proven'] or node['ready'] for node in result['nodes']), 'cold copy admitted'
    for phase in ('before', 'after'):
        assert set(result[phase]) == {'global/' + name for name in IMAGE_NAMES}, 'incomplete native images'
    for name in IMAGE_NAMES:
        if name.startswith('pgrac_xid_authority'):
            key = 'global/' + name
            assert result['after'][key] == result['before'][key], 'authority changed during refusal'


def reject(layout_path, entry_path, bindir, output):
    from cohort_entry import CohortEntry
    layout = isolated_layout(layout_path)
    root = Path(layout_path).resolve(strict=True).parent
    output = Path(output)
    driver = CohortEntry(layout, bindir, json.loads(Path(entry_path).read_text()))
    before = images(layout, root/'before')
    result = dict(before=before, refused=False, cleanup=[], nodes=[])
    try:
        driver.phase_budget(60)
        try:
            driver.shared_start()
        except RuntimeError as error:
            result.update(refused=True, error=str(error))
        if not result['refused']:
            raise AssertionError('cleared votes unexpectedly reached OPEN')
    finally:
        # Expected failed startup cannot be reported as a normal stop. Signal
        # only the new copy through the same namespace-aware native controller.
        result.update(stop_copy(layout_path, entry_path, bindir))
        result['after'] = images(layout, root/'after')
        for node in layout['nodes']:
            text = Path(node['logfile']).read_text() if Path(node['logfile']).exists() else ''
            result['nodes'].append(dict(node=node['id'], log=node['logfile'],
                fatal=len(re.findall(r'FATAL:', text)),
                proven=text.count('NATIVE_EMPTY_BOOT_PROVEN'),
                ready='database system is ready to accept connections' in text,
                limit_refusal='shared MultiXact member retention limit is not proven' in text,
                configuration_mismatch='native bootstrap profile is not applicable' in text))
        output.write_text(json.dumps(result, indent=2)+'\n')
    verify(result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('layout')
    parser.add_argument('entry')
    parser.add_argument('bindir')
    parser.add_argument('output')
    args = parser.parse_args()
    print(json.dumps(reject(args.layout, args.entry, args.bindir, args.output)))


if __name__ == '__main__':
    main()
