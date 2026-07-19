import assert from 'node:assert/strict';
import test from 'node:test';
import { readinessData, renderReadinessSvg } from './canary_chart.mjs';

const summary = {
  rungs: [
    {
      sourceTaskRows: 250202,
      official: { protocolReadinessMs: 20464.624 },
      rust: { protocolReadinessMs: 1743.175 }
    },
    {
      sourceTaskRows: 5001002,
      official: { protocolReadinessMs: 356867.0 },
      rust: { protocolReadinessMs: 32180.0 }
    }
  ]
};

test('readinessData extracts rows and per-target readiness', () => {
  assert.deepEqual(readinessData(summary), [
    { rows: 250202, officialMs: 20464.624, rustMs: 1743.175 },
    { rows: 5001002, officialMs: 356867.0, rustMs: 32180.0 }
  ]);
});

test('readiness figure labels rungs, values, ratios, and both series', () => {
  for (const theme of ['light', 'dark']) {
    const svg = renderReadinessSvg(readinessData(summary), theme);
    assert.equal(svg, renderReadinessSvg(readinessData(summary), theme));
    assert.match(svg, /250,202 rows/);
    assert.match(svg, /5,001,002 rows/);
    assert.match(svg, /356\.9 s/);
    assert.match(svg, /32\.2 s/);
    assert.match(svg, /11\.7x/);
    assert.match(svg, /11\.1x/);
    assert.match(svg, /official PowerSync 1\.23\.3/);
    assert.match(svg, /Rust\/MDBX/);
  }
  assert.notEqual(
    renderReadinessSvg(readinessData(summary), 'light'),
    renderReadinessSvg(readinessData(summary), 'dark')
  );
});
