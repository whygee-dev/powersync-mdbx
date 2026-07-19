import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

// Renders the protocol-readiness figure for the symmetric scale canary from
// its checked-in canary-summary.json, so the figure and the artifact cannot
// disagree. The two hues are the first two slots of the validated palette for
// the light and dark surfaces.
const THEMES = {
  light: {
    surface: '#fcfcfb',
    ink: '#0b0b0b',
    secondary: '#52514e',
    muted: '#898781',
    grid: '#e1e0d9',
    official: '#2a78d6',
    rust: '#008300'
  },
  dark: {
    surface: '#1a1a19',
    ink: '#ffffff',
    secondary: '#c3c2b7',
    muted: '#898781',
    grid: '#2c2c2a',
    official: '#3987e5',
    rust: '#008300'
  }
};

const FONT = "system-ui, -apple-system, 'Segoe UI', sans-serif";

function esc(text) {
  return String(text).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

function readinessData(summary) {
  return (summary.rungs ?? []).map((rung) => ({
    rows: rung.sourceTaskRows,
    officialMs: rung.official.protocolReadinessMs,
    rustMs: rung.rust.protocolReadinessMs
  }));
}

function barPath(x, y, width, height, radius = 4) {
  const r = Math.min(radius, width / 2, height / 2);
  return (
    `M${x},${y} h${(width - r).toFixed(2)} a${r},${r} 0 0 1 ${r},${r} v${height - 2 * r} ` +
    `a${r},${r} 0 0 1 -${r},${r} h${(-(width - r)).toFixed(2)} z`
  );
}

function renderReadinessSvg(rungs, themeName, { width = 920 } = {}) {
  const theme = THEMES[themeName];
  const labelWidth = 108;
  const ratioWidth = 64;
  const x0 = 16 + labelWidth;
  const plotWidth = width - x0 - ratioWidth - 72;
  const barHeight = 16;
  const pairGap = 2;
  const groupGap = 16;
  const top = 96;
  const maxSeconds = Math.max(...rungs.map((rung) => rung.officialMs)) / 1000;
  const tickStep = maxSeconds > 150 ? 100 : maxSeconds > 40 ? 25 : 5;
  const scaleMax = Math.ceil((maxSeconds * 1.02) / tickStep) * tickStep;
  const xFor = (seconds) => x0 + (plotWidth * seconds) / scaleMax;
  const groupHeight = barHeight * 2 + pairGap;
  const plotBottom = top + rungs.length * (groupHeight + groupGap) - groupGap;
  const parts = [
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${plotBottom + 40}" viewBox="0 0 ${width} ${plotBottom + 40}" role="img">`,
    `<rect width="${width}" height="${plotBottom + 40}" fill="${theme.surface}"/>`,
    `<text x="16" y="26" font-family="${FONT}" font-size="13" font-weight="600" fill="${theme.ink}">Initial replication — protocol readiness by rung</text>`,
    `<text x="16" y="44" font-family="${FONT}" font-size="11.5" fill="${theme.secondary}">first proven /sync/stream checkpoint for one routed subscription; one measured run per target and rung</text>`,
    `<rect x="16" y="61" width="10" height="10" rx="2" fill="${theme.official}"/>`,
    `<text x="31" y="70" font-family="${FONT}" font-size="12" fill="${theme.secondary}">official PowerSync 1.23.3</text>`,
    `<rect x="192" y="61" width="10" height="10" rx="2" fill="${theme.rust}"/>`,
    `<text x="207" y="70" font-family="${FONT}" font-size="12" fill="${theme.secondary}">Rust/MDBX</text>`,
    `<text x="${width - 16}" y="${top - 14}" text-anchor="end" font-family="${FONT}" font-size="10.5" fill="${theme.muted}">official / Rust</text>`
  ];
  for (let tick = 0; tick <= scaleMax; tick += tickStep) {
    const x = xFor(tick).toFixed(2);
    parts.push(`<line x1="${x}" y1="${top - 8}" x2="${x}" y2="${plotBottom + 6}" stroke="${theme.grid}" stroke-width="1"/>`);
    parts.push(
      `<text x="${x}" y="${plotBottom + 22}" text-anchor="middle" font-family="${FONT}" font-size="10.5" fill="${theme.muted}">${tick}${tick === scaleMax ? ' s' : ''}</text>`
    );
  }
  rungs.forEach((rung, index) => {
    const y = top + index * (groupHeight + groupGap);
    const officialSeconds = rung.officialMs / 1000;
    const rustSeconds = rung.rustMs / 1000;
    parts.push(
      `<text x="${x0 - 12}" y="${y + barHeight + 1}" text-anchor="end" font-family="${FONT}" font-size="12" fill="${theme.secondary}">${esc(rung.rows.toLocaleString('en-US'))} rows</text>`
    );
    for (const [seconds, fill, offset] of [
      [officialSeconds, theme.official, 0],
      [rustSeconds, theme.rust, barHeight + pairGap]
    ]) {
      const barWidth = Math.max(xFor(seconds) - x0, 1.5);
      parts.push(`<path d="${barPath(x0, y + offset, barWidth, barHeight)}" fill="${fill}"/>`);
      parts.push(
        `<text x="${(x0 + barWidth + 7).toFixed(2)}" y="${y + offset + 12}" font-family="${FONT}" font-size="11" fill="${theme.ink}">${seconds.toFixed(1)} s</text>`
      );
    }
    parts.push(
      `<text x="${width - 16}" y="${y + barHeight + 1}" text-anchor="end" font-family="${FONT}" font-size="12" fill="${theme.muted}">${(rung.officialMs / rung.rustMs).toFixed(1)}x</text>`
    );
  });
  parts.push('</svg>');
  return parts.join('\n');
}

function main() {
  const [summaryPath, outDir] = process.argv.slice(2);
  if (!summaryPath || !outDir) {
    console.error('usage: node scripts/canary_chart.mjs <canary-summary.json> <output-dir>');
    process.exit(2);
  }
  const rungs = readinessData(JSON.parse(fs.readFileSync(summaryPath, 'utf8')));
  if (rungs.length === 0) {
    console.error(`no rungs in ${summaryPath}`);
    process.exit(1);
  }
  fs.mkdirSync(outDir, { recursive: true });
  for (const [themeName, suffix] of [
    ['light', ''],
    ['dark', '-dark']
  ]) {
    fs.writeFileSync(path.join(outDir, `readiness${suffix}.svg`), renderReadinessSvg(rungs, themeName));
  }
  console.log(`wrote readiness figures for ${rungs.length} rung(s) to ${outDir}`);
}

export { THEMES, readinessData, renderReadinessSvg };

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
