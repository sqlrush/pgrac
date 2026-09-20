#!/usr/bin/env python3
"""Author: SqlRush. Attribute admitted CR slot lifetime, not client wall time.

Only existing requester/backend/request identities join to UPDATEs. Source
durations are measured on that source's monotonic clock; source and requester
timestamps are never subtracted. Slot seconds must not be added to foreground
wait seconds. An unadmitted/refused request has no slot episode.
"""
from collections import Counter, defaultdict
from bisect import bisect_right


def request_key(e):
    return e['requester'], e['backend'], e['request']


def load_events(paths):
    import json
    events = []
    metadata = {}
    for path in paths:
        meta = {}
        for row in json.loads(path.read_text()):
            if row.get('category') != 'update_trace_event':
                continue
            if not row['key'].startswith('event.'):
                meta[row['key']] = int(row['value'])
                continue
            events.append({key: int(value) for key, value in
                           (part.split('=', 1) for part in row['value'].split(';'))})
        metadata[str(path)] = meta
    return events, metadata


def analyze_events(events, selected_ops=None):
    links = {}
    episodes = defaultdict(list)
    gates = defaultdict(list)
    serving = defaultdict(list)
    intervals = defaultdict(list)
    serve_totals = Counter()
    dependencies = {}
    origin_rows = []
    origin_lost = []
    unmatched_ends = 0
    for e in sorted(events, key=lambda row: row['stamp_ns']):
        if e['kind'] == 1:
            key, op = request_key(e), (e['node'], e['op'])
            if key in links and links[key] != op:
                raise ValueError('conflicting physical request linkage')
            links[key] = op
        elif e['kind'] == 6:
            gates[e['node']].append(e)
        elif e['kind'] in (7, 8):
            key = (e['node'], e['pid'], request_key(e), e['value'])
            if e['kind'] == 7:
                serving[key].append(e['stamp_ns'])
            elif serving[key]:
                start = serving[key].pop()
                intervals[key[:2]].append((start, e['stamp_ns']))
                serve_totals[str(e['value'])] += e['stamp_ns']-start
            else:
                unmatched_ends += 1
        elif e['kind'] == 10:
            # The dependency requester is the CR builder, not the original
            # frontend. No clock ordering between these nodes is assumed.
            # value is exported as uint32; requester_backend as int32.
            endpoint = e['value'] if e['value'] < (1 << 31) else e['value']-(1 << 32)
            key = (e['node'], endpoint, e['dependency'])
            parent = request_key(e)
            if key in dependencies and dependencies[key] != parent:
                raise ValueError('conflicting origin dependency linkage')
            dependencies[key] = parent
        elif e['kind'] == 9:
            if not 0 <= e['work_ns'] <= e['duration_ns']:
                raise ValueError('origin work exceeds phase residence')
            origin_rows.append(e)
        elif e['kind'] == 11:
            origin_lost.append(e)
        else:
            episodes[(e['node'], e['slot'], e['generation'], request_key(e))].append(e)
    gate_starts = {}
    for node, rows in gates.items():
        rows.sort(key=lambda e: e['stamp_ns'])
        gate_starts[node] = [e['stamp_ns'] for e in rows]
    interval_starts = {}
    for key, rows in intervals.items():
        merged = []
        for a, b in sorted(rows):
            if merged and a <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(b, merged[-1][1]))
            else:
                merged.append((a, b))
        intervals[key] = merged
        interval_starts[key] = [a for a, b in merged]

    def serve_overlap(key, start, end):
        rows = intervals[key]
        if not rows:
            return 0
        index = max(0, bisect_right(interval_starts[key], start)-1)
        overlap = 0
        while index < len(rows) and rows[index][0] < end:
            a, b = rows[index]
            overlap += max(0, min(end, b)-max(start, a))
            index += 1
        return overlap

    def blocked_overlap(node, start, end):
        rows = gates[node]
        if not rows:
            return 0
        index = max(0, bisect_right(gate_starts[node], start)-1)
        overlap = 0
        while index < len(rows) and rows[index]['stamp_ns'] < end:
            a = max(start, rows[index]['stamp_ns'])
            b = min(end, rows[index+1]['stamp_ns'] if index+1 < len(rows) else end)
            if rows[index]['value'] == 1:
                overlap += max(0, b-a)
            index += 1
        return overlap

    names = {5: 'queued', 6: 'claimed_wait', 7: 'undo_send_wait',
             8: 'undo_inflight', 9: 'undo_ready_wait', 10: 'terminal_wait',
             11: 'terminal_wait', 12: 'terminal_wait', 13: 'terminal_wait',
             14: 'shipping'}
    totals = Counter()
    complete = incomplete = unselected = unlinked = 0
    wall = origin_overlap = legacy_overlap = 0
    linked_requests = set()
    for key, rows in episodes.items():
        op = links.get(key[3])
        if op is None:
            unlinked += 1
        if selected_ops is not None and op not in selected_ops:
            unselected += 1
            continue
        rows.sort(key=lambda e: e['stamp_ns'])
        if (rows[0]['kind'] != 2 or rows[0]['value'] != 5 or rows[-1]['kind'] != 5
                or sum(e['kind'] == 5 for e in rows) != 1
                or sum(e['kind'] == 2 and e['value'] == 5 for e in rows) != 1):
            incomplete += 1
            continue
        phase = 'queued'
        build = False
        after_build = 'post_build'
        builder_pid = next((e['pid'] for e in rows if e['kind'] == 3), -1)
        local = Counter()
        overlap = local_legacy = 0
        prior = rows[0]['stamp_ns']
        valid = True
        for e in rows[1:]:
            elapsed = e['stamp_ns']-prior
            local[phase] += elapsed
            if phase in ('queued', 'claimed_wait', 'undo_ready_wait', 'undo_send_wait', 'terminal_wait'):
                overlap += blocked_overlap(key[0], prior, e['stamp_ns'])
                local_legacy += serve_overlap((key[0], builder_pid), prior, e['stamp_ns'])
            prior = e['stamp_ns']
            if e['kind'] == 2:
                state_phase = names.get(e['value'], 'unknown_state')
                if build:
                    after_build = state_phase
                else:
                    phase = state_phase
            elif e['kind'] == 3:
                if build:
                    valid = False
                build = True
                after_build = 'post_build'
                phase = 'build_call'
            elif e['kind'] == 4:
                if not build:
                    valid = False
                build = False
                phase = after_build
        duration = rows[-1]['stamp_ns']-rows[0]['stamp_ns']
        if not valid or build or sum(local.values()) != duration:
            incomplete += 1
            continue
        complete += 1
        wall += duration
        totals.update(local)
        origin_overlap += overlap
        legacy_overlap += local_legacy
        linked_requests.add(key[3])
    selected_links = {key for key, op in links.items()
                      if selected_ops is None or op in selected_ops}
    selected_dependencies = {key for key, parent in dependencies.items()
                             if selected_ops is None or links.get(parent) in selected_ops}
    origin_phases, origin_work = Counter(), Counter()
    all_origin_phases, all_origin_work = Counter(), Counter()
    origin_complete, origin_seen = set(), set()
    for e in origin_rows:
        phase = f"{e['value'] >> 8}:{e['value'] & 255}"
        all_origin_phases[phase] += e['duration_ns']
        all_origin_work[phase] += e['work_ns']
        key = request_key(e)
        if key not in selected_dependencies:
            continue
        origin_seen.add(key)
        origin_phases[phase] += e['duration_ns']
        origin_work[phase] += e['work_ns']
        if e['value'] & 255 == 16:
            origin_complete.add(key)
    return dict(complete_episodes=complete, incomplete_episodes=incomplete,
                unselected_episodes=unselected, unlinked_episodes=unlinked,
                selected_physical_requests=len(selected_links),
                requests_without_complete_episode=len(selected_links-linked_requests),
                slot_wall_ns=wall, phases_ns=dict(totals),
                eligible_wait_under_last_observed_origin_gate_ns=origin_overlap,
                eligible_wait_inside_legacy_serve_ns=legacy_overlap,
                legacy_serve_total_ns_by_kind=dict(serve_totals),
                legacy_incomplete_calls=sum(map(len, serving.values()))+unmatched_ends,
                origin_phase_ns=dict(origin_phases), origin_work_ns=dict(origin_work),
                all_origin_phase_ns=dict(all_origin_phases),
                all_origin_work_ns=dict(all_origin_work),
                origin_dependency_requests=len(selected_dependencies),
                origin_observed_requests=len(origin_seen),
                origin_completed_requests=len(origin_complete),
                origin_dependencies_without_send=len(selected_dependencies-origin_complete),
                origin_incomplete_intervals=sum(request_key(e) in selected_dependencies
                                                for e in origin_lost),
                all_origin_incomplete_intervals=len(origin_lost),
                origin_time_basis='phase first step to transition on origin clock; work is '
                                  'time inside step calls, not CPU time; SEND observation '
                                  'is not proof of successful reply delivery',
                gate_basis='last worker drain return predicate; not SCUR hold ownership',
                time_basis='source slot lifetime; not additive to client wall time')
