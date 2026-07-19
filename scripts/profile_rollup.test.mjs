import assert from 'node:assert/strict';
import test from 'node:test';

import {
  CATEGORIES,
  aggregateProfiles,
  categorizeProfile,
  categoryForFrame,
  renderRollupMarkdown
} from './profile_rollup.mjs';

const fixture = {
  startTime: 0,
  endTime: 675,
  nodes: [
    { id: 1, callFrame: { functionName: '(root)', url: '' } },
    { id: 2, callFrame: { functionName: 'serialize', url: 'file:///app/node_modules/bson/lib/bson.js' } },
    { id: 3, callFrame: { functionName: 'find', url: 'file:///app/node_modules/mongodb/lib/collection.js' } },
    { id: 4, callFrame: { functionName: '(garbage collector)', url: '' } },
    { id: 5, callFrame: { functionName: 'mystery', url: 'file:///app/lib/mystery.js' } }
  ],
  samples: [2, 2, 3, 4, 5, 2],
  timeDeltas: [100, 100, 300, 50, 25, 100]
};

test('categoryForFrame maps special V8 frames and node core before regexes', () => {
  assert.equal(categoryForFrame({ functionName: '(garbage collector)', url: '' }, CATEGORIES), 'gc');
  assert.equal(categoryForFrame({ functionName: '(program)', url: '' }, CATEGORIES), 'program');
  assert.equal(categoryForFrame({ functionName: '(idle)', url: '' }, CATEGORIES), 'idle');
  assert.equal(categoryForFrame({ functionName: 'f', url: '' }, CATEGORIES), 'native builtins');
  assert.equal(categoryForFrame({ functionName: 'f', url: 'node:internal/streams' }, CATEGORIES), 'node core');
  assert.equal(categoryForFrame({ functionName: 'f', url: 'file:///x/unknown.js' }, CATEGORIES), 'other');
});

test('categorizeProfile attributes self time per category in microseconds', () => {
  const result = categorizeProfile(fixture);
  assert.equal(result.totalMicros, 575);
  assert.equal(result.buckets.get('bson'), 400);
  assert.equal(result.buckets.get('mongodb driver'), 50);
  assert.equal(result.buckets.get('gc'), 25);
  assert.equal(result.buckets.get('other'), 100);
  assert.equal(result.otherFrames.get('mystery file:///app/lib/mystery.js'), 100);
});

test('categorizeProfile tolerates profiles without nodes', () => {
  assert.equal(categorizeProfile({ samples: [], timeDeltas: [] }).totalMicros, 0);
  assert.equal(categorizeProfile({}).totalMicros, 0);
});

test('aggregateProfiles sums totals, buckets, and other frames', () => {
  const one = categorizeProfile(fixture);
  const agg = aggregateProfiles([one, one]);
  assert.equal(agg.totalMicros, 1150);
  assert.equal(agg.buckets.get('bson'), 800);
  assert.equal(agg.otherFrames.get('mystery file:///app/lib/mystery.js'), 200);
});

test('renderRollupMarkdown emits a share table and top other frames', () => {
  const md = renderRollupMarkdown('smoke', categorizeProfile(fixture));
  assert.match(md, /## smoke/);
  assert.match(md, /\| bson \| 0\.4 \| 69\.6% \|/);
  assert.match(md, /mystery file:\/\/\/app\/lib\/mystery\.js/);
});
