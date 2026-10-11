#!/usr/bin/env python3
"""Capture stopped fixture DATA without modifying the original cohort.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil


def sha(path):
    with Path(path).open('rb') as source:
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def capture(layout_path, data_root, destination):
    layout_path = layout_path.resolve(strict=True)
    data_root = data_root.resolve(strict=True)
    destination = destination.resolve()
    if not layout_path.is_relative_to(data_root):
        raise ValueError('layout is outside fixture DATA')
    if destination.is_relative_to(data_root) or destination.exists():
        raise ValueError('cold copy destination must be new and outside DATA')
    layout = json.loads(layout_path.read_text())
    if [node['id'] for node in layout['nodes']] != list(range(4)):
        raise ValueError('cold copy requires the original four nodes')
    for path in [layout['root'], layout['shared_data_dir'], layout['wal_root'],
                 *(node['data_dir'] for node in layout['nodes'])]:
        if not Path(path).resolve(strict=True).is_relative_to(data_root):
            raise ValueError('cohort path is outside fixture DATA')
    for node in layout['nodes']:
        if (Path(node['data_dir'])/'postmaster.pid').exists():
            raise ValueError('cold copy requires every postmaster stopped')
    destination.mkdir(mode=0o700)
    # Preserve links for relocation in the isolated negative-test fixture.
    shutil.copytree(data_root, destination/'data', symlinks=True)
    votes = []
    for index, path in enumerate(layout['voting_disks']):
        target = destination/f'vote{index}.image'
        with Path(path).open('rb') as source, target.open('xb') as output:
            shutil.copyfileobj(source, output)
        votes.append(dict(path=str(target), sha256=sha(target),
                          size=target.stat().st_size))
    result = dict(original_data_root=str(data_root), layout=layout,
                  data=str(destination/'data'), votes=votes)
    (destination/'capture.json').write_text(json.dumps(result, indent=2)+'\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['capture'])
    parser.add_argument('layout', type=Path)
    parser.add_argument('data', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    print(json.dumps(capture(args.layout, args.data, args.destination)))


if __name__ == '__main__':
    main()
