import assert from 'node:assert/strict';
import test from 'node:test';
import { CATEGORIES } from './profile_rollup.mjs';
import { OWNERS, RUNTIME_LEAVES, contextTotals, decomposeProfiles, renderMarkdown } from './profile_decompose.mjs';

const bsonUrl = 'file:///app/node_modules/.pnpm/bson@6.10.4/node_modules/bson/lib/bson.cjs';
const serviceUrl = 'file:///app/service/dist/replicator.js';

const profile = {
  nodes: [
    { id: 1, callFrame: { functionName: '(root)', url: '' }, children: [2, 3, 5, 7] },
    { id: 2, callFrame: { functionName: '(idle)', url: '' }, children: [] },
    { id: 3, callFrame: { functionName: 'serializeInto', url: bsonUrl }, children: [4] },
    { id: 4, callFrame: { functionName: 'utf8ByteLength', url: '' }, children: [] },
    { id: 5, callFrame: { functionName: 'hashData', url: serviceUrl }, children: [6] },
    { id: 6, callFrame: { functionName: 'update', url: 'node:internal/crypto/hash' }, children: [] },
    { id: 7, callFrame: { functionName: '(garbage collector)', url: '' }, children: [] }
  ],
  // one 3 ms idle run after bson work, one 20 ms idle run after crypto work
  samples: [3, 4, 2, 3, 6, 2, 2, 7],
  timeDeltas: [0, 1e3, 1e3, 3e3, 1e3, 1e3, 1e4, 1e4, 1e3]
};

test('owner map covers every non-idle rollup category', () => {
  const categoryNames = [...CATEGORIES.map((category) => category.name), 'gc', 'program', 'idle', 'native builtins', 'node core', 'other'];
  for (const name of categoryNames) {
    if (name === 'idle') continue;
    assert.ok(OWNERS.has(name) || RUNTIME_LEAVES.has(name) || name === 'gc', `uncovered category ${name}`);
  }
});

test('runtime leaves are re-attributed to their categorized callers', () => {
  const state = decomposeProfiles([profile]);
  assert.equal(state.activeMicros, 5000);
  assert.equal(state.idleMicros, 23000);
  assert.deepEqual([...state.ownerSplit.get('native builtins')], [['marshalling', 1000]]);
  assert.deepEqual([...state.ownerSplit.get('node core')], [['row processing', 1000]]);
  assert.deepEqual([...state.cryptoCallers], [['powersync service: hashData', 1000]]);
  assert.deepEqual([...state.nativeMarshallingParents], [['serializeInto', 1000]]);
  const totals = contextTotals(state);
  assert.equal(totals.get('marshalling'), 3000);
  assert.equal(totals.get('row processing'), 1000);
  assert.equal(totals.get('gc'), 1000);
});

test('idle runs are bucketed by length and keyed by the preceding context', () => {
  const state = decomposeProfiles([profile]);
  assert.equal(state.idleRuns.get('2-10ms').count, 1);
  assert.equal(state.idleRuns.get('10-100ms').count, 1);
  assert.equal(state.idleAfter.get('marshalling'), 3000);
  assert.equal(state.idleAfter.get('row processing'), 20000);
});

test('markdown output is deterministic and carries the headline splits', () => {
  const state = decomposeProfiles([profile]);
  const markdown = renderMarkdown(state);
  assert.equal(markdown, renderMarkdown(decomposeProfiles([profile])));
  assert.match(markdown, /\| marshalling \| 3\.0 \| 60\.0% \|/);
  assert.match(markdown, /powersync service: hashData — 20\.0%/);
  assert.match(markdown, /serializeInto — 20\.0%/);
  assert.match(markdown, /after row processing — 87\.0%/);
  assert.match(markdown, /after marshalling — 13\.0%/);
});
