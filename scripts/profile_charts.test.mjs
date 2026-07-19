import assert from 'node:assert/strict';
import test from 'node:test';
import { aggregateProfiles, categorizeProfile } from './profile_rollup.mjs';
import {
  GROUPS,
  attributionData,
  groupForCategory,
  mergeStacks,
  renderAttributionSvg,
  renderFlamegraphSvg
} from './profile_charts.mjs';

const bsonUrl = 'file:///app/node_modules/.pnpm/bson@6.10.4/node_modules/bson/lib/bson.cjs';

const profile = {
  nodes: [
    { id: 1, callFrame: { functionName: '(root)', url: '' }, children: [2, 3, 4] },
    { id: 2, callFrame: { functionName: '(idle)', url: '' }, children: [] },
    { id: 3, callFrame: { functionName: 'serializeInto', url: bsonUrl }, children: [5] },
    { id: 4, callFrame: { functionName: '(garbage collector)', url: '' }, children: [] },
    { id: 5, callFrame: { functionName: 'serializeString', url: bsonUrl }, children: [] }
  ],
  samples: [2, 3, 5, 4, 2],
  timeDeltas: [0, 1e6, 1e6, 1e6, 1e6, 1e6]
};

test('every rollup category maps to a figure group and idle stays separate', () => {
  const covered = GROUPS.flatMap((group) => group.categories);
  for (const category of covered) assert.notEqual(groupForCategory(category), null);
  assert.equal(new Set(covered).size, 14 - 1);
  assert.equal(groupForCategory('idle'), 'idle');
});

test('mergeStacks excludes idle, keeps ancestry, and merges across profiles', () => {
  const merged = mergeStacks([profile, profile]);
  assert.equal(merged.total, 6e6);
  const children = [...merged.children.values()];
  assert.deepEqual(
    children.map((child) => [child.name, child.total, child.category]).sort(),
    [
      ['(garbage collector)', 2e6, 'gc'],
      ['serializeInto', 4e6, 'bson']
    ]
  );
  const serialize = children.find((child) => child.name === 'serializeInto');
  assert.equal([...serialize.children.values()][0].total, 2e6);
});

test('attribution data reproduces the rollup group arithmetic', () => {
  const aggregate = aggregateProfiles([categorizeProfile(profile)]);
  const data = attributionData(aggregate);
  assert.equal(data.totalMicros, 5e6);
  assert.equal(data.idleMicros, 2e6);
  assert.equal(data.activeMicros, 3e6);
  assert.equal(data.groups.find((group) => group.name === 'BSON + MongoDB driver').micros, 2e6);
  assert.equal(data.groups.find((group) => group.name === 'GC').micros, 1e6);
});

test('figures are deterministic, themed, and never draw idle frames', () => {
  const aggregate = aggregateProfiles([categorizeProfile(profile)]);
  const merged = mergeStacks([profile]);
  for (const theme of ['light', 'dark']) {
    const attribution = renderAttributionSvg(aggregate, theme);
    const flamegraph = renderFlamegraphSvg(merged, theme);
    assert.equal(attribution, renderAttributionSvg(aggregate, theme));
    assert.equal(flamegraph, renderFlamegraphSvg(merged, theme));
    assert.match(attribution, /idle 40\.0%/);
    assert.match(attribution, /66\.7%/);
    assert.match(flamegraph, /serializeInto/);
    assert.doesNotMatch(flamegraph, /\(idle\)/);
    assert.match(flamegraph, /3\.0 s of active self time/);
  }
  assert.notEqual(renderAttributionSvg(aggregate, 'light'), renderAttributionSvg(aggregate, 'dark'));
});
