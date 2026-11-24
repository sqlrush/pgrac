# -*- coding: utf-8 -*-
"""Render the one-page Chinese summary of a judged long run.

Pure formatting of the judge's result document; it decides nothing.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

from typing import Dict, List, Mapping, Sequence

TITLES = {
    'P0-1': '正常全停失败', 'P0-2': '冻结/回收推不动（回卷保护）', 'P0-3': 'LMS 多于 2 个时启停失败',
    'P0-4': '启动与崩溃恢复', 'P0-5': '历史事务终态不可证明', 'P0-6': '成员失联/隔离后整库不可用',
    'P0-7': 'undo 段池耗尽', 'P0-8': 'ITL 溢出或等待失败', 'P0-9': '单段 48 个事务槽满',
    'P0-10': '读镜像失败', 'P0-11': '收据准备失败', 'P0-12': 'HOT 链无法证明',
    'P0-13': '子事务中的 DML', 'P0-14': 'DROP/TRUNCATE 之后集群异常',
    'HOT-TX-01': '等待远端写入事务超时（候选，未定级）'}
VERDICTS = {'PASS': '通过', 'FAIL': '未通过', 'SELFTEST_OK': '驱动器自检正常（不是验收结果）',
            'SELFTEST_FAILED': '驱动器自检失败（不是验收结果）'}
FAILURES = {
    'EVIDENCE_MISSING': '证据缺失：{what}', 'MEMBER_COUNT': '成员数不对：应为 {expected}，实际 {actual}',
    'STEP_NOT_PASSED': '步骤 {step} = {status}：{error}', 'FORCED_CLEANUP': '控制器不得不强制清理数据库进程（不是正常停机）',
    'LEFT_RUNNING': '本轮结束时集群仍在运行', 'ERRORS_NOT_ALLOWED': '{count} 条未被允许的错误',
    'NODE_WITHOUT_COMMITS': '节点 {node} 没有任何成功事务', 'WRONG_CLIENT_ANSWERS': '节点 {node} 有 {count} 次错误应答（UPDATE/COMMIT 结果不符）',
    'HUNG_CONNECTIONS': '{count} 个连接在宽限期后仍未返回（挂起）', 'WORKLOAD_STALLED': '负载停滞后提前中止：{detail}',
    'LEDGER_MISMATCH': '步骤 {step}：节点 {members} 的表内容与提交账本不一致', 'CONTROLLER': '控制器：{detail}'}
STEP_STATUS = {'PASS': '通过', 'FAIL': '失败', 'NOT_RUN': '未执行', 'RUNNING': '中断'}
SECTIONS = {'NOT_RUN': '未执行', 'NOT_IMPLEMENTED': '未实现', 'PASS': '通过', 'FAIL': '失败',
            'RUN': '已执行'}


def _table(header: Sequence[str], rows: Sequence[Sequence[object]]) -> List[str]:
    lines = ['| ' + ' | '.join(header) + ' |', '|' + '---|' * len(header)]
    return lines + ['| ' + ' | '.join(str(cell) for cell in row) + ' |' for row in rows]


def _failure(failure: Mapping[str, object]) -> str:
    template = FAILURES.get(str(failure['code']), str(failure['code']))
    fields: Dict[str, object] = dict(what='', expected='', actual='', step='', status='',
                                    error='', count='', node='', detail='', members='')
    fields.update(failure)
    fields['status'] = STEP_STATUS.get(str(fields['status']), fields['status'])
    prefix = '第 %s 轮：' % failure['cycle'] if 'cycle' in failure else ''
    return prefix + template.format(**fields)


def _brief(raw: str) -> str:
    """Primary line and DETAIL of a retained record; result.json has all of it."""
    lines = raw.split('\n')
    kept = lines[:1] + [line for line in lines[1:] if 'DETAIL:  ' in line]
    return '\n'.join(line[:400] for line in kept)


def _step(cycle: Mapping[str, object], name: str) -> str:
    for step in cycle['steps']:
        if step['name'] == name:
            return STEP_STATUS.get(step['status'], step['status'])
    return '—'


def _reconciled(cycle: Mapping[str, object]) -> str:
    answers = cycle.get('reconciliations') or []
    if not answers:
        return '未执行'
    bad = sum(1 for a in answers for m in a['members'] if m['status'] != 'PASS')
    return '一致' if not bad else '%d 个节点不一致' % bad


def render(result: Mapping[str, object]) -> str:
    identity = result.get('identity') or {}
    totals = result['totals']
    out = ['# P0 长跑验收摘要', '',
           '**结论：%s（%s）**　零错误：%s' % (result['verdict'], VERDICTS[str(result['verdict'])],
                                         '是' if result['zero_errors'] else '否'), '',
           '- 模式：%s%s' % (result['mode'], '' if result['acceptance_result']
                           else '（单个普通实例，仅检查驱动器本身）'),
           '- postgres sha256：%s　源码提交：%s' % (str(identity.get('postgres_sha256'))[:16],
                                             identity.get('source_commit')),
           '- 分类规则：%d 条（sha256 %s）　判官：未被允许的错误一律算失败，默认没有允许名单'
           % (result['rule_table']['rules'], result['rule_table']['sha256'][:12]),
           '- 未被允许的错误：服务端日志 %d 条，客户端 %d 条；其中未分类 %d 条；已允许 %d 条'
           % (totals['failing_server_records'], totals['failing_client_records'],
              totals['unclassified'], totals['allowed_records']), '', '## 各轮', '']
    rows = []
    for cycle in result['cycles']:
        nodes = cycle.get('nodes') or []
        rows.append([cycle['cycle'], cycle['clients_per_node'], cycle['seconds'],
                     ' / '.join(str(n['commits']) for n in nodes) or '—',
                     cycle.get('commits', '—'), cycle.get('failing_records', '—'),
                     len(cycle.get('hangs') or []), _reconciled(cycle), _step(cycle, 'stop')])
    out += _table(['轮', '每实例连接', '时长(秒)', '各节点成功事务', '合计', '未允许错误', '挂起连接',
                   '账本对账', '正常停机'], rows)
    out += ['', '## 按编号的错误计数', '']
    rows = [[name, TITLES[name], b['server'], b['client'], b['allowed'],
             '；'.join('%s×%d' % item for item in sorted(b['rules'].items())),
             '、'.join(sorted(b['steps']))]
            for name, b in result['buckets'].items() if b['server'] or b['client'] or b['allowed']]
    out += _table(['编号', '含义', '服务端日志', '客户端', '已允许', '命中规则', '出现步骤'], rows) \
        if rows else ['（没有任何错误命中分类规则）']
    out += ['', '没有报错原文规则的编号（只能靠错误码或所在步骤归类，否则落入未分类）：'
            + '、'.join(result['buckets_without_text_rule']), '',
            '## 未分类错误：%d 条' % result['unclassified']['count'], '']
    if result['unclassified']['count']:
        out += _table(['次数', '归并后的形态（级别|SQLSTATE|消息）'],
                      [[s['count'], '`%s`' % s['signature'].replace('|', '\\|')]
                       for s in result['unclassified']['signatures'][:10]])
        out += ['', '前 %d 条原文：' % len(result['unclassified']['first_raw']), '', '```']
        out += [_brief(raw) for raw in result['unclassified']['first_raw']] + ['```']
    else:
        out.append('无。')
    out += ['', '## 失败原因', '']
    out += ['- ' + _failure(f) for f in result['failures']] or ['无。']
    sections = result['sections']
    stop = sections['normal_stop']
    out += ['', '## 环节状态', '']
    out += _table(['环节', '状态'], [
        ['负载', SECTIONS[sections['workload']]],
        ['负载后正常停机（四实例正常关闭并取得关闭证明）', '%d / %d 轮通过' % (stop['passed'],
                                                         stop['requested'])],
        ['kill -9 全部实例后重启并按账本对账', SECTIONS[sections['crash_restart_reconcile']]
         + ('（本次未请求）' if sections['crash_restart_reconcile'] == 'NOT_RUN' else '')],
        ['单节点被隔离后其余节点继续服务', SECTIONS[sections['service_after_member_isolation']]
         + '（需要多机和真实隔离，本驱动器不执行）'],
        ['容量类定向复测（段池、单段槽、ITL）', SECTIONS[sections['directed_capacity_cases']]
         + '（由定向脚本另行执行）']])
    out += ['', '## 允许名单', '']
    allow = result['allow_list']
    if allow['entries']:
        out.append('文件 %s（sha256 %s）' % (allow['file'], str(allow['sha256'])[:12]))
        out += _table(['条目', '理由', '匹配次数'],
                      [[e['id'], e['reason'], e['matched']] for e in allow['entries']])
    else:
        out.append('无（所有错误都算失败）。')
    return '\n'.join(out) + '\n'
