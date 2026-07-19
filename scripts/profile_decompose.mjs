import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';
import { CATEGORIES, categoryForFrame } from './profile_rollup.mjs';

// Decomposes a directory of V8 .cpuprofile files beyond the self-time rollup:
// runtime-bucket self time is re-attributed to the nearest categorized caller
// on the sampled stack, named hotspots inside those buckets are listed, and
// the idle time is broken into run lengths, steady-state versus drain, and
// the categorized frames that bracket each idle run. Attribution to a caller
// is a stack heuristic, not a measurement of causation; idle runs shorter
// than the sampling interval are under-counted.
const RUNTIME_LEAVES = new Set(['native builtins', 'node core', 'program', 'other']);
const OWNERS = new Map([
  ['bson', 'marshalling'],
  ['mongodb driver', 'marshalling'],
  ['sync-rules', 'row processing'],
  ['jsonbig', 'row processing'],
  ['mongo storage', 'row processing'],
  ['postgres replication', 'row processing'],
  ['other service code', 'row processing'],
  ['logging', 'logging']
]);
const IDLE_RUN_BUCKETS = [
  ['<2ms', 2],
  ['2-10ms', 10],
  ['10-100ms', 100],
  ['100ms-1s', 1000],
  ['>=1s', Infinity]
];
const STEADY_SHARE = 0.9;

function bump(map, key, micros) {
  map.set(key, (map.get(key) ?? 0) + micros);
}

function decomposeProfiles(profiles) {
  const state = {
    activeMicros: 0,
    idleMicros: 0,
    gcMicros: 0,
    selfByOwner: new Map(),
    ownerSplit: new Map(),
    cryptoCallers: new Map(),
    nativeMarshallingParents: new Map(),
    nodeCoreModules: new Map(),
    otherFunctions: new Map(),
    idleRuns: new Map(IDLE_RUN_BUCKETS.map(([label]) => [label, { count: 0, micros: 0 }])),
    idleAfter: new Map(),
    steady: { idleMicros: 0, totalMicros: 0, idleRunCount: 0 },
    drain: { idleMicros: 0, totalMicros: 0 }
  };
  for (const profile of profiles) {
    const nodesById = new Map((profile.nodes ?? []).map((node) => [node.id, node]));
    const parentById = new Map();
    for (const node of profile.nodes ?? []) {
      for (const childId of node.children ?? []) parentById.set(childId, node.id);
    }
    const samples = profile.samples ?? [];
    const timeDeltas = profile.timeDeltas ?? [];
    const categoryOf = (id) => categoryForFrame(nodesById.get(id)?.callFrame ?? {}, CATEGORIES);
    const ownerOf = (leafId) => {
      for (let id = leafId; id != null; id = parentById.get(id)) {
        const frame = nodesById.get(id)?.callFrame ?? {};
        const owner = OWNERS.get(categoryForFrame(frame, CATEGORIES));
        if (owner) return { owner, frame };
        if (frame.functionName === '(root)') break;
      }
      return { owner: '(none)', frame: null };
    };
    const totalMicros = timeDeltas.reduce((sum, delta, i) => (i > 0 && delta > 0 ? sum + delta : sum), 0);
    let elapsed = 0;
    let runMicros = 0;
    let runStartedSteady = false;
    let inRun = false;
    const closeRun = () => {
      if (!inRun) return;
      const ms = runMicros / 1000;
      const bucket = IDLE_RUN_BUCKETS.find(([, limit]) => ms < limit)[0];
      const entry = state.idleRuns.get(bucket);
      entry.count += 1;
      entry.micros += runMicros;
      if (runStartedSteady) state.steady.idleRunCount += 1;
      bump(state.idleAfter, inRun.prevContext, runMicros);
      inRun = false;
      runMicros = 0;
    };
    let prevContext = '(start)';
    for (let i = 0; i < samples.length; i++) {
      const micros = timeDeltas[i + 1] ?? 0;
      if (micros <= 0) continue;
      const category = categoryOf(samples[i]);
      const steadyNow = elapsed < totalMicros * STEADY_SHARE;
      const phase = steadyNow ? state.steady : state.drain;
      phase.totalMicros += micros;
      elapsed += micros;
      if (category === 'idle') {
        state.idleMicros += micros;
        phase.idleMicros += micros;
        if (!inRun) inRun = { prevContext };
        if (runMicros === 0) runStartedSteady = steadyNow;
        runMicros += micros;
        continue;
      }
      closeRun();
      prevContext = category === 'gc' ? 'gc' : (OWNERS.get(category) ?? ownerOf(samples[i]).owner);
      state.activeMicros += micros;
      if (category === 'gc') state.gcMicros += micros;
      const contextKey = OWNERS.get(category) ?? null;
      if (contextKey != null) {
        bump(state.selfByOwner, contextKey, micros);
      } else if (RUNTIME_LEAVES.has(category)) {
        const { owner } = ownerOf(samples[i]);
        if (!state.ownerSplit.has(category)) state.ownerSplit.set(category, new Map());
        bump(state.ownerSplit.get(category), owner, micros);
        const frame = nodesById.get(samples[i])?.callFrame ?? {};
        if (category === 'node core') {
          bump(state.nodeCoreModules, (frame.url ?? '').replace(/^node:/, '').split('/').slice(0, 2).join('/'), micros);
          if ((frame.url ?? '').startsWith('node:internal/crypto')) {
            for (let id = parentById.get(samples[i]); id != null; id = parentById.get(id)) {
              const callerFrame = nodesById.get(id)?.callFrame ?? {};
              const callerCategory = categoryForFrame(callerFrame, CATEGORIES);
              if (OWNERS.has(callerCategory)) {
                bump(state.cryptoCallers, `${callerCategory}: ${callerFrame.functionName || '(anonymous)'}`, micros);
                break;
              }
              if (callerFrame.functionName === '(root)') break;
            }
          }
        }
        if (category === 'native builtins' && owner === 'marshalling') {
          for (let id = parentById.get(samples[i]); id != null; id = parentById.get(id)) {
            const callerFrame = nodesById.get(id)?.callFrame ?? {};
            const callerCategory = categoryForFrame(callerFrame, CATEGORIES);
            if (callerCategory === 'bson' || callerCategory === 'mongodb driver') {
              bump(state.nativeMarshallingParents, callerFrame.functionName || '(anonymous)', micros);
              break;
            }
          }
        }
        if (category === 'other') bump(state.otherFunctions, frame.functionName || '(anonymous)', micros);
      }
    }
    closeRun();
  }
  return state;
}

function contextTotals(state) {
  const totals = new Map([...state.selfByOwner]);
  totals.set('gc', state.gcMicros);
  let unattributed = 0;
  for (const owners of state.ownerSplit.values()) {
    for (const [owner, micros] of owners) {
      if (owner === '(none)') unattributed += micros;
      else bump(totals, owner, micros);
    }
  }
  totals.set('(unattributed runtime)', unattributed);
  return totals;
}

function renderMarkdown(state) {
  const active = state.activeMicros;
  const pct = (micros) => `${((100 * micros) / active).toFixed(1)}%`;
  const ms = (micros) => (micros / 1000).toFixed(1);
  const lines = [];
  lines.push('## Context totals, share of active CPU', '');
  lines.push('Self time plus runtime-bucket time re-attributed to the nearest categorized caller on the stack.', '');
  lines.push('| Context | Self time (ms) | Share of active |', '| --- | ---: | ---: |');
  for (const [context, micros] of [...contextTotals(state).entries()].sort((a, b) => b[1] - a[1])) {
    lines.push(`| ${context} | ${ms(micros)} | ${pct(micros)} |`);
  }
  lines.push('', '## Runtime buckets by nearest categorized caller', '');
  lines.push('| Leaf bucket | Share of active | Split |', '| --- | ---: | --- |');
  const sumOf = (map) => [...map.values()].reduce((sum, value) => sum + value, 0);
  for (const [category, owners] of [...state.ownerSplit.entries()].sort((a, b) => sumOf(b[1]) - sumOf(a[1]))) {
    const split = [...owners.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([owner, micros]) => `${owner} ${pct(micros)}`)
      .join(', ');
    lines.push(`| ${category} | ${pct(sumOf(owners))} | ${split} |`);
  }
  const top = (map, count) => [...map.entries()].sort((a, b) => b[1] - a[1]).slice(0, count);
  lines.push('', '## Named hotspots inside the runtime buckets', '');
  lines.push(`node:internal/crypto totals ${pct(sumOf(state.cryptoCallers))} of active CPU; nearest categorized callers:`, '');
  for (const [caller, micros] of top(state.cryptoCallers, 6)) lines.push(`- ${caller} — ${pct(micros)}`);
  lines.push('', 'Native-builtin time under marshalling, by nearest bson/driver caller:', '');
  for (const [caller, micros] of top(state.nativeMarshallingParents, 6)) lines.push(`- ${caller} — ${pct(micros)}`);
  lines.push('', 'Node core by module:', '');
  for (const [module, micros] of top(state.nodeCoreModules, 6)) lines.push(`- ${module || '(no url)'} — ${pct(micros)}`);
  lines.push('', 'Uncategorized dependency functions:', '');
  for (const [name, micros] of top(state.otherFunctions, 6)) lines.push(`- ${name} — ${pct(micros)}`);
  const idle = state.idleMicros;
  const idlePct = (micros) => `${((100 * micros) / idle).toFixed(1)}%`;
  lines.push('', '## Idle structure', '');
  lines.push('| Idle run length | Runs | Idle time (ms) | Share of idle |', '| --- | ---: | ---: | ---: |');
  for (const [label] of IDLE_RUN_BUCKETS) {
    const { count, micros } = state.idleRuns.get(label);
    if (count === 0) continue;
    lines.push(`| ${label} | ${count} | ${ms(micros)} | ${idlePct(micros)} |`);
  }
  lines.push('', 'Idle time by the context of the last active frame before the run:', '');
  for (const [context, micros] of [...state.idleAfter.entries()].sort((a, b) => b[1] - a[1])) {
    lines.push(`- after ${context} — ${idlePct(micros)}`);
  }
  const steadyPct = ((100 * state.steady.idleMicros) / state.steady.totalMicros).toFixed(1);
  const drainPct = ((100 * state.drain.idleMicros) / state.drain.totalMicros).toFixed(1);
  const overallPct = ((100 * idle) / (idle + active)).toFixed(1);
  const runRate = (state.steady.idleRunCount / (state.steady.totalMicros / 1e6)).toFixed(1);
  lines.push(
    '',
    `First ${Math.round(STEADY_SHARE * 100)}% of wall clock: ${steadyPct}% idle across ${state.steady.idleRunCount} idle runs (${runRate} runs per second). Last ${Math.round((1 - STEADY_SHARE) * 100)}%: ${drainPct}% idle. Overall: ${overallPct}% idle.`
  );
  return `${lines.join('\n')}\n`;
}

function main() {
  const dir = process.argv[2];
  if (!dir) {
    console.error('usage: node scripts/profile_decompose.mjs <dir-with-cpuprofiles>');
    process.exit(2);
  }
  const files = fs
    .readdirSync(dir)
    .filter((name) => name.endsWith('.cpuprofile'))
    .sort();
  if (files.length === 0) {
    console.error(`no .cpuprofile files in ${dir}`);
    process.exit(1);
  }
  const profiles = files.map((name) => JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8')));
  process.stdout.write(renderMarkdown(decomposeProfiles(profiles)));
}

export { OWNERS, RUNTIME_LEAVES, contextTotals, decomposeProfiles, renderMarkdown };

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
