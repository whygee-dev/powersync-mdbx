import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

// Finalized against real 100k official-service profiles. The service is a pnpm
// monorepo: its own code compiles under /app/{packages,modules,libs,service}/dist,
// and dependencies resolve through /app/node_modules/.pnpm/<pkg>/node_modules/<pkg>.
// Order matters: specific package paths precede the broad `powersync service`
// catch-all. Keep the residual `other` bucket under 5% of aggregate self time.
const CATEGORIES = [
  { name: 'sync-rules', test: /\/app\/packages\/sync-rules\// },
  { name: 'jsonbig', test: /\/app\/packages\/jsonbig\// },
  { name: 'mongo storage', test: /\/app\/(modules\/module-mongodb-storage|libs\/lib-mongodb)\// },
  {
    name: 'postgres replication',
    test: /\/app\/(modules\/module-postgres|libs\/lib-postgres|packages\/jpgwire)\/|\/node_modules\/pgwire\//
  },
  { name: 'powersync service', test: /\/app\/(packages|modules|libs|service)\// },
  { name: 'mongodb driver', test: /\/node_modules\/mongodb\// },
  { name: 'bson', test: /\/node_modules\/bson\// },
  { name: 'logging', test: /\/node_modules\/(winston|winston-transport|logform|triple-beam|safe-stable-stringify|@colors)\// }
];

function categoryForFrame(frame, categories) {
  const name = frame.functionName ?? '';
  if (name === '(garbage collector)') return 'gc';
  if (name === '(program)') return 'program';
  if (name === '(idle)') return 'idle';
  const url = frame.url ?? '';
  if (url === '') return 'native builtins';
  for (const category of categories) {
    if (category.test.test(url)) return category.name;
  }
  if (url.startsWith('node:')) return 'node core';
  return 'other';
}

function categorizeProfile(profile, categories = CATEGORIES) {
  const nodesById = new Map((profile.nodes ?? []).map((node) => [node.id, node]));
  const buckets = new Map();
  const otherFrames = new Map();
  let totalMicros = 0;
  const samples = profile.samples ?? [];
  const timeDeltas = profile.timeDeltas ?? [];
  for (let i = 0; i < samples.length; i++) {
    const micros = timeDeltas[i + 1] ?? 0;
    if (micros <= 0) continue;
    const frame = nodesById.get(samples[i])?.callFrame ?? {};
    const category = categoryForFrame(frame, categories);
    totalMicros += micros;
    buckets.set(category, (buckets.get(category) ?? 0) + micros);
    if (category === 'other') {
      const label = `${frame.functionName || '(anonymous)'} ${frame.url ?? ''}`.trim();
      otherFrames.set(label, (otherFrames.get(label) ?? 0) + micros);
    }
  }
  return { totalMicros, buckets, otherFrames };
}

function aggregateProfiles(results) {
  const aggregate = { totalMicros: 0, buckets: new Map(), otherFrames: new Map() };
  for (const result of results) {
    aggregate.totalMicros += result.totalMicros;
    for (const [category, micros] of result.buckets) {
      aggregate.buckets.set(category, (aggregate.buckets.get(category) ?? 0) + micros);
    }
    for (const [label, micros] of result.otherFrames) {
      aggregate.otherFrames.set(label, (aggregate.otherFrames.get(label) ?? 0) + micros);
    }
  }
  return aggregate;
}

function renderRollupMarkdown(title, result, { topOther = 10 } = {}) {
  const lines = [`## ${title}`, '', '| Category | Self time (ms) | Share |', '| --- | ---: | ---: |'];
  const sorted = [...result.buckets.entries()].sort((a, b) => b[1] - a[1]);
  for (const [category, micros] of sorted) {
    const ms = (micros / 1000).toFixed(1);
    const share = result.totalMicros > 0 ? ((100 * micros) / result.totalMicros).toFixed(1) : '0.0';
    lines.push(`| ${category} | ${ms} | ${share}% |`);
  }
  const other = [...result.otherFrames.entries()].sort((a, b) => b[1] - a[1]).slice(0, topOther);
  if (other.length > 0) {
    lines.push('', `Top \`other\` frames:`, '');
    for (const [label, micros] of other) {
      lines.push(`- ${label} — ${(micros / 1000).toFixed(1)} ms`);
    }
  }
  return `${lines.join('\n')}\n`;
}

function main() {
  const dir = process.argv[2];
  if (!dir) {
    console.error('usage: node scripts/profile_rollup.mjs <dir-with-cpuprofiles>');
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
  const results = [];
  let skipped = 0;
  for (const name of files) {
    let result;
    try {
      result = categorizeProfile(JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8')));
    } catch (error) {
      console.error(`skipping ${name}: ${error.message}`);
      skipped += 1;
      continue;
    }
    results.push(result);
    process.stdout.write(renderRollupMarkdown(name, result));
    process.stdout.write('\n');
  }
  if (results.length === 0) {
    console.error(`no readable .cpuprofile files in ${dir}`);
    process.exit(1);
  }
  if (skipped > 0) {
    console.error(`skipped ${skipped} unparsable profile(s)`);
  }
  process.stdout.write(renderRollupMarkdown(`aggregate (${results.length} profiles)`, aggregateProfiles(results)));
}

export { CATEGORIES, aggregateProfiles, categorizeProfile, categoryForFrame, renderRollupMarkdown };

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
