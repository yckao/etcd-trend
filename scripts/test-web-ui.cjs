// DOM behavior tests; jsdom is a test-only dependency installed outside the repo.
// NODE_PATH=/tmp/etcd-trend-ui-tests/node_modules node scripts/test-web-ui.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');

const assets = path.join(__dirname, '../internal/webui/assets');
const html = fs.readFileSync(path.join(assets, 'index.html'), 'utf8');
const script = fs.readFileSync(path.join(assets, 'app.js'), 'utf8');
const stock = { live_value_bytes: 32, live_record_bytes: 60, retained_mvcc_bytes: 120, history_bytes: 60 };
const metrics = { puts: 2, deletes: 0, value_write_bytes: 64, compacted_mvcc_bytes: 30, estimated_db_growth_bytes: 4096, estimated_free_growth_bytes: 2048, estimated_free_reuse_bytes: 512 };
const identity = { key: '/registry/pods/alpha/pod-a', resource: 'pods', namespace: 'alpha', name: 'pod-a', confidence: 'path-layout' };
const metadata = { status: 'decoded-json', api_version: 'v1', kind: 'Pod', namespace: 'alpha', uid: 'pod-uid', labels: { tenant: '<script>bad()</script>' }, controller: { api_version: 'apps/v1', kind: 'ReplicaSet', name: 'rs-a', uid: 'owner-uid' }, managers: ['kubelet', 'controller'] };
const totals = { physical_start: { db_size: 4096, db_size_in_use: 4096 }, physical_end: { db_size: 8192, db_size_in_use: 4096, pending_pages: 0 }, free_growth_bytes: 4096, free_reuse_bytes: 0 };
const token = '["label","present","tenant","alpha"]';
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check) {
  for (let n = 0; n < 1500; n++) { if (check()) return; await delay(10); }
  throw new Error('DOM condition did not become true');
}

async function run(labelKeys) {
  const dom = new JSDOM(html, { url: 'http://127.0.0.1:8080', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document, queries = [];
  // Node's native fetch requires its own AbortSignal brand across DOM realms.
  w.AbortController = AbortController;
  let holdNext = false, rejectHeld;
  w.fetch = async raw => {
    const url = new URL(raw, w.location.href), q = url.searchParams;
    let data;
    switch (url.pathname) {
      case '/api/info': data = { window_count: 2, window_ns: 1e9, duration_ns: 2e9, label_keys: labelKeys, source_version: '3.6.0', engine: 'test', model: 'test', final_revision: 4 }; break;
      case '/api/curve': data = [0, 1, 2].map(n => ({ time_ns: n * 1e9, window: n - 1, db_size: 4096 * (n + 1), db_size_in_use: 4096, free_bytes: 4096 * n, fragmentation_percent: n * 25 })); break;
      case '/api/rankings': {
        queries.push(Object.fromEntries(q));
        if (holdNext) { holdNext = false; await new Promise((_, reject) => { rejectHeld = reject; }); }
        const grouped = !['resource', 'object'].includes(q.get('scope'));
        data = { total: 1, offset: 0, limit: 10, denominator_bytes: 64, denominator_label: 'matching rows', totals, attribution: 'Metadata at each window endpoint.', items: [{ id: 'object-id', identity, bytes: 64, share_percent: 100, group: grouped ? { scope: q.get('scope'), key: token, value: 'alpha', label_key: 'tenant', status: 'present' } : undefined }] };
        break;
      }
      case '/api/objects': data = { items: [{ id: 'object-id', identity }], total: 1, offset: 0, limit: 8 }; break;
      case '/api/object': data = { id: 'object-id', identity, metadata, metadata_start: { ...metadata, labels: { tenant: 'old' } }, from: 0, to: 2, start: stock, end: stock, baseline: stock, final: stock, metrics, first_index: 2, last_index: 4, points: [{ time_ns: 0, stock, metrics: {}, changed: false }, { time_ns: 2e9, stock, metrics, changed: true }], total_points: 2 }; break;
      default: throw new Error(url.pathname);
    }
    return { ok: true, json: async () => data };
  };
  w.eval(script);
  const el = id => d.getElementById(id);
  const change = (id, value) => { el(id).value = value; el(id).dispatchEvent(new w.Event('change')); };
  await until(() => el('rank-body').children.length === 1);
  if (!labelKeys.length) {
    assert.equal(el('label-key').disabled, true);
    assert.equal([...el('group-scope').options].find(o => o.value === 'label').disabled, true);
    w.close(); return;
  }
  change('group-scope', 'namespace-label');
  await until(() => el('rank-body').textContent.includes('alpha') && queries.at(-1).scope === 'namespace-label');
  el('rank-search').value = 'alpha';
  el('rank-search').dispatchEvent(new w.Event('input'));
  await until(() => queries.at(-1).search === 'alpha' && el('rank-body').children.length === 1);
  el('rank-body').children[0].click();
  await until(() => queries.at(-1).scope === 'object' && el('rank-body').textContent.includes('pod-a'));
  assert.equal(queries.at(-1).search || '', '', 'group display search must not filter the child objects');
  assert.equal(el('rank-search').value, '');
  assert.equal(queries.at(-1).group_scope, 'namespace-label');
  assert.equal(queries.at(-1).group_value, token);
  assert.equal(queries.at(-1).label_key, 'tenant');
  el('namespace-filter').value = 'alpha';
  el('namespace-filter').dispatchEvent(new w.Event('input'));
  await until(() => queries.at(-1).namespace === 'alpha');
  el('label-filter-enabled').checked = true;
  el('label-filter-enabled').dispatchEvent(new w.Event('change'));
  await until(() => queries.at(-1).label_filter === 'true' && el('rank-body').children.length === 1);
  // Empty-value equality is represented explicitly; omission of label_value is okay.
  assert.equal(queries.at(-1).label_value || '', '');
  el('label-value').value = 'alpha';
  el('label-value').dispatchEvent(new w.Event('input'));
  await until(() => queries.at(-1).label_value === 'alpha');
  el('label-filter-enabled').checked = false;
  el('label-filter-enabled').dispatchEvent(new w.Event('change'));
  await until(() => queries.at(-1).label_filter === 'false' && el('rank-body').children.length === 1);
  assert.equal(queries.at(-1).label_value || '', '');
  assert.equal(el('label-value').value, 'alpha', 'keep the text for re-enabling the filter');
  el('label-filter-enabled').checked = true;
  el('label-filter-enabled').dispatchEvent(new w.Event('change'));
  await until(() => queries.at(-1).label_filter === 'true' && queries.at(-1).label_value === 'alpha' && el('rank-body').children.length === 1);
  el('rank-body').children[0].click();
  await until(() => !el('detail-content').classList.contains('hidden'));
  assert.match(el('object-metadata').textContent, /pod-uid/);
  assert.match(el('object-metadata').textContent, /ReplicaSet\/rs-a/);
  assert.match(el('object-metadata').textContent, /tenant=<script>bad\(\)<\/script>/);
  assert.equal(el('object-metadata').querySelector('script'), null);
  assert.match(el('metadata-note').textContent, /differs from the interval start/);
  el('close-drawer').click();
  holdNext = true;
  change('group-scope', 'namespace');
  await until(() => rejectHeld !== undefined);
  assert.equal(el('rank-body').children.length, 0);
  assert.equal(el('stat-db').textContent, '—');
  change('group-scope', 'kind');
  await until(() => el('rank-body').children.length === 1 && queries.at(-1).scope === 'kind');
  rejectHeld(new Error('superseded network failure'));
  await delay(20);
  assert.equal(el('rank-error').textContent, '');
  assert.equal(el('rank-body').children.length, 1);
  w.close();
}
async function live(base) {
  assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(new URL(base).hostname), 'live tests require loopback');
  const dom = new JSDOM(html, { url: base, runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document, queries = [];
  w.AbortController = AbortController;
  w.fetch = (raw, options) => {
    const url = new URL(raw, base);
    if (url.pathname === '/api/rankings') queries.push(Object.fromEntries(url.searchParams));
    return fetch(url, options);
  };
  w.eval(script);
  const el = id => d.getElementById(id);
  const ready = () => el('rank-body').getAttribute('aria-busy') === 'false' && el('rank-body').children.length > 0;
  try { await until(ready); } catch (err) {
    console.error('Initial UI errors:', el('fatal').textContent, el('rank-error').textContent, queries);
    w.close(); throw err;
  }
  el('label-key').value = 'tenant'; el('label-key').dispatchEvent(new w.Event('change'));
  await until(ready);
  for (const scope of ['namespace', 'api-group', 'kind', 'label', 'namespace-label', 'controller', 'manager']) {
    el('group-scope').value = scope; el('group-scope').dispatchEvent(new w.Event('change'));
    await until(() => ready() && queries.at(-1).scope === scope);
    assert.equal(el('rank-error').textContent, '');
  }
  el('rank-search').value = 'controller';
  el('rank-search').dispatchEvent(new w.Event('input'));
  await until(() => ready() && queries.at(-1).search === 'controller');
  el('rank-body').children[0].click();
  await until(() => ready() && queries.at(-1).scope === 'object');
  assert.equal(queries.at(-1).group_scope, 'manager');
  assert.ok(queries.at(-1).group_value);
  assert.equal(queries.at(-1).search || '', '');
  el('rank-body').children[0].click();
  await until(() => !el('detail-content').classList.contains('hidden') && el('object-metadata').textContent.includes('decoded-json'));
  assert.match(el('object-metadata').textContent, /tenant=/);
  assert.equal(el('detail-error').textContent, '');
  w.close();
  console.log('PASS: live local API -> DOM aggregation for seven dimensions -> objects -> Kubernetes metadata');
}
(async () => {
  await run(['tenant', 'team']); await run([]);
  console.log('PASS: tenant controls, namespace-label drill, empty label filtering, metadata text safety, stale query rejection, legacy outputs');
  if (process.argv[2]) await live(process.argv[2]);
})().catch(err => { console.error(err); process.exitCode = 1; });
