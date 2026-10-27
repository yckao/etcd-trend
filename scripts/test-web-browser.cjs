// Optional local browser QA. Playwright is a test dependency, not an app runtime.
// PLAYWRIGHT_MODULE=/path/to/playwright-core node scripts/test-web-browser.cjs URL OUT
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const base = process.argv[2], out = process.argv[3];
  assert.ok(base && out, 'usage: test-web-browser.cjs LOOPBACK_URL OUTPUT_DIRECTORY');
  assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(new URL(base).hostname));
  fs.mkdirSync(out, { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1080 } });
  page.setDefaultTimeout(30000);
  const errors = [];
  page.on('pageerror', error => errors.push(String(error)));
  const ready = () => page.waitForSelector('#rank-body[aria-busy="false"] tr');
  async function queryAfter(action, matches) {
    const response = page.waitForResponse(r => {
      const u = new URL(r.url());
      return u.pathname === '/api/rankings' && matches(u.searchParams);
    });
    await action();
    const result = await response;
    assert.equal(result.status(), 200);
    const body = await result.json();
    await ready();
    return body;
  }
  try {
    await page.goto(base);
    await ready();
    await queryAfter(() => page.selectOption('#label-key', 'tenant'), q => q.get('label_key') === 'tenant');
    await queryAfter(() => page.selectOption('#group-scope', 'namespace-label'), q => q.get('scope') === 'namespace-label');
    await page.screenshot({ path: path.join(out, 'desktop.png'), fullPage: true });

    await page.fill('#label-value', 'alpha');
    const filtered = await queryAfter(() => page.check('#label-filter-enabled'), q => q.get('label_filter') === 'true' && q.get('label_value') === 'alpha');
    const all = await queryAfter(() => page.uncheck('#label-filter-enabled'), q => q.get('label_filter') === 'false' && !q.get('label_value'));
    assert.ok(filtered.denominator_bytes > 0 && all.denominator_bytes > filtered.denominator_bytes);
    assert.equal(await page.inputValue('#label-value'), 'alpha');

    await queryAfter(() => page.selectOption('#group-scope', 'manager'), q => q.get('scope') === 'manager');
    await queryAfter(() => page.fill('#rank-search', 'controller'), q => q.get('search') === 'controller');
    const children = await queryAfter(() => page.locator('#rank-body tr').first().click(), q => q.get('scope') === 'object' && q.get('group_scope') === 'manager' && !q.get('search'));
    assert.ok(children.items.length);
    assert.equal(await page.inputValue('#rank-search'), '');
    await page.locator('#rank-body tr').first().click();
    await page.waitForFunction(() => document.querySelector('#object-metadata').textContent.includes('decoded-json'));
    assert.match(await page.locator('#detail-content').textContent(), /Full object history for this interval/);
    await page.screenshot({ path: path.join(out, 'object-desktop.png') });
    await page.setViewportSize({ width: 502, height: 900 });
    await page.screenshot({ path: path.join(out, 'object-mobile.png') });
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'mobile document overflows');
    await page.click('#close-drawer');
    await page.screenshot({ path: path.join(out, 'mobile.png'), fullPage: true });
    assert.deepEqual(errors, []);
    fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify({ status: 'passed', browser: await browser.version(), viewports: [1440, 502], filtered_bytes: filtered.denominator_bytes, unfiltered_bytes: all.denominator_bytes, group_drill_objects: children.total, page_errors: errors }, null, 2) + '\n');
    console.log('PASS: real browser label toggle, searched manager drill-down, object metadata, desktop/mobile rendering');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
