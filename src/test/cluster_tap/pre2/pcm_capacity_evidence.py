#!/usr/bin/env python3
"""Validate fixed-capacity PCM counter cuts, without granting runtime authority.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import json
import re
import sys


def uint64(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError('counter is not an unsigned decimal')
    if not re.fullmatch(r'0|[1-9][0-9]*', str(value)) or int(value) >= 2**64:
        raise ValueError('counter is outside uint64')
    return int(value)


def judge(sample):
    try:
        before, after = sample['before'], sample['after']
        for rows in (before, after):
            if (not isinstance(rows, list) or len(rows) != 4
                    or [row['node'] for row in rows] != list(range(4))
                    or any(type(row['node']) is not int for row in rows)):
                raise ValueError('four ordered original members are required')
        wait, reclaimed = 0, 0
        full = []
        for node, (old, new) in enumerate(zip(before, after)):
            if (not isinstance(old['postmaster_start'], str)
                    or not old['postmaster_start']
                    or old['postmaster_start'] != new['postmaster_start']):
                raise ValueError('postmaster lifetime changed or is unknown')
            previous, current = old['stats'], new['stats']
            for stats in (previous, current):
                if type(stats['capacity']) is not int or stats['capacity'] != 2048:
                    raise ValueError('original 2048 capacity is required')
                if (uint64(stats['live_entries']) > 2048
                        or uint64(stats['peak_live_entries']) > 2048):
                    raise ValueError('entry counts exceed original capacity')
            for field in ('binding_generation', 'peak_live_entries',
                          'capacity_wait_count', 'reclaim_success_count'):
                if uint64(current[field]) < uint64(previous[field]):
                    raise ValueError('counter reset or generation regression')
            wait += uint64(current['capacity_wait_count']) - uint64(previous['capacity_wait_count'])
            reclaimed += uint64(current['reclaim_success_count']) - uint64(previous['reclaim_success_count'])
            if uint64(current['peak_live_entries']) == 2048:
                full.append(node)
        if not full or wait <= 0 or reclaimed <= 0:
            raise ValueError('full capacity and increasing wait/reclaim counters are required')
        return dict(passed=True, wait_delta=str(wait), reclaim_delta=str(reclaimed),
                    full_members=full, scope='counter-evidence-only')
    except (KeyError, TypeError, ValueError) as error:
        return dict(passed=False, reason=str(error), scope='counter-evidence-only')


if __name__ == '__main__':
    print(json.dumps(judge(json.load(sys.stdin)), sort_keys=True))
