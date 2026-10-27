#!/usr/bin/env python3
"""Verify an isolated Kubernetes fixture's completed replay through the local API."""
import argparse
import json
import math
from pathlib import Path
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--recording-summary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    url = urllib.parse.urlparse(args.url)
    if url.hostname not in ('127.0.0.1', 'localhost', '::1'):
        parser.error('qualification uses only the local fixture viewer')

    def get(route, **query):
        with urllib.request.urlopen(args.url.rstrip('/') + route + '?' + urllib.parse.urlencode(query), timeout=60) as response:
            return json.load(response)

    def rank(scope, start=0, end=None, **query):
        return get('/api/rankings', **{'from': start, 'to': info['window_count'] if end is None else end, 'scope': scope, 'metric': 'writes', 'limit': 100, **query})

    report = json.loads((args.run / 'report.json').read_text())
    recording = json.loads(args.recording_summary.read_text())
    assert report['logical_sha256'] == recording['expected_logical_sha256']
    assert report['final_revision'] == recording['expected_revision']
    info = get('/api/info')
    assert 'tenant' in info['label_keys']
    total = report['analysis']['metrics']['value_write_bytes']
    results = {}
    physical_totals = {}
    for scope in ('resource', 'namespace', 'api-group', 'kind', 'label', 'namespace-label', 'controller', 'manager'):
        result = rank(scope, label_key='tenant')
        assert result['total'] <= 100, 'fixture unexpectedly exceeds one page'
        assert sum(row['bytes'] for row in result['items']) == total, scope
        for row in result['items']:
            group = row.get('group')
            if group:
                child = rank('object', label_key='tenant', group_scope=group['scope'], group_value=group['key'])
                assert child['denominator_bytes'] == row['bytes'], (scope, group)
        results[scope] = result
        physical_totals[scope] = {}
        for metric, field in (('db-growth', 'estimated_db_growth_bytes'), ('free-growth', 'estimated_free_growth_bytes'), ('compacted', 'compacted_mvcc_bytes')):
            physical = rank(scope, label_key='tenant', metric=metric)
            assert physical['total'] <= 100
            amount = sum(row['bytes'] for row in physical['items'])
            assert amount == report['analysis']['metrics'][field], (scope, metric, amount)
            physical_totals[scope][metric] = amount
        print(f'PASS {scope}: {result["total"]} groups, conserved bytes and drill-down totals', flush=True)

    phases = []
    window = info['window_ns']
    # Use interior windows so phase-boundary polling/anchoring jitter cannot
    # decide the expected leader. Short smoke fixtures skip this long-run check.
    if recording['workload_duration_ns'] >= 60_000_000_000:
        for index, phase in enumerate(recording['phases']):
            end = recording['phases'][index + 1]['start_ns'] if index + 1 < len(recording['phases']) else recording['workload_duration_ns']
            first, last = math.ceil(phase['start_ns'] / window) + 1, math.floor(end / window) - 1
            assert first < last
            by_ns = rank('namespace', first, last)
            by_tenant = rank('namespace-label', first, last, label_key='tenant')
            assert by_ns['items'][0]['group']['value'] == phase['dominant_namespace'], (index + 1, by_ns['items'][0]['group'], phase)
            expected = phase['dominant_namespace'].removeprefix('tenant-')
            if index == 4:
                expected = 'alpha-migrated'
            assert by_tenant['items'][0]['group']['value'] == expected, (index + 1, by_tenant['items'][0]['group'], expected)
            phases.append({'phase': index + 1, 'from': first, 'to': last, 'namespace_leader': by_ns['items'][0], 'namespace_label_leader': by_tenant['items'][0]})
            print(f'PASS phase {index + 1}: namespace={phase["dominant_namespace"]}, tenant={expected}', flush=True)

    quiet = get('/api/objects', search='/registry/configmaps/tenant-alpha/quiet')['items'][0]
    quiet_detail = get('/api/object', id=quiet['id'], **{'from': info['window_count'] - 1, 'to': info['window_count']})
    assert quiet_detail['start'] == quiet_detail['end']
    assert all(value == 0 for value in quiet_detail['metrics'].values())
    assert quiet_detail['metadata']['labels']['tenant'] == 'alpha'
    encrypted = get('/api/objects', search='/registry/secrets/tenant-delta/opaque')['items'][0]
    detail = get('/api/object', id=encrypted['id'])
    assert detail['metadata']['status'] == 'encrypted'
    assert not detail['metadata'].get('labels')
    args.out.write_text(json.dumps({'status': 'passed', 'info': info, 'logical_state_matches_source': True, 'conserved_write_bytes': total, 'conserved_physical_and_compaction_bytes': physical_totals, 'groupings': results, 'phases': phases, 'quiet_object': quiet_detail, 'encrypted_object_status': 'encrypted'}, indent=2) + '\n')
    print(json.dumps({'status': 'passed', 'windows': info['window_count'], 'duration_seconds': info['duration_ns'] / 1e9, 'write_bytes_conserved': total, 'verified_phase_leaders': len(phases)}))


if __name__ == '__main__':
    main()
