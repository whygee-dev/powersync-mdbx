import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

export function captureResourceSnapshot({ components, storagePaths = [], walLsn = null }) {
  return {
    capturedAt: new Date().toISOString(),
    walLsn,
    components: Object.fromEntries(
      components.map(({ label, container }) => [label, captureContainerResources(container)])
    ),
    storage: measureStorage(storagePaths)
  };
}

export function captureContainerResources(container, { run = spawnSync, readFile = fs.readFileSync } = {}) {
  const inspected = run('docker', ['inspect', '--format', '{{json .Id}}\t{{json .State.StartedAt}}\t{{.State.Pid}}', container], {
    encoding: 'utf8'
  });
  if (inspected.status !== 0) {
    return { status: 'not-running', container };
  }

  const [containerIdJson, startedAtJson, pidRaw] = String(inspected.stdout ?? '').trim().split('\t');
  const containerId = parseJsonString(containerIdJson);
  const startedAt = parseJsonString(startedAtJson);
  const pid = Number.parseInt(pidRaw, 10);
  if (containerId != null && Number.isSafeInteger(pid) && pid > 0) {
    const native = captureNativeLinuxContainerResources(pid, { readFile });
    if (native != null && cgroupBelongsToContainer(native.cgroupPath, containerId)) {
      return {
        status: 'captured',
        container,
        containerId,
        startedAt,
        source: 'linux-cgroup-v2',
        access: 'host-proc',
        ...native
      };
    }
  }

  const containerNative = captureContainerLinuxResources(container, { run });
  if (containerNative != null) {
    return {
      status: 'captured',
      container,
      containerId,
      startedAt,
      source: 'linux-cgroup-v2',
      access: 'docker-exec',
      ...containerNative
    };
  }

  const fallback = captureDockerStats(container, { run });
  return fallback == null
    ? { status: 'unavailable', container, source: 'none' }
    : { status: 'captured', container, containerId, startedAt, source: 'docker-stats-fallback', ...fallback };
}

export function cgroupBelongsToContainer(cgroupPath, containerId) {
  if (typeof cgroupPath !== 'string' || typeof containerId !== 'string') return false;
  const normalizedId = containerId.trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(normalizedId)) return false;
  return cgroupPath.toLowerCase().includes(normalizedId);
}

export function captureContainerLinuxResources(container, { run = spawnSync } = {}) {
  const read = (filePath, optional = false) => {
    const result = run('docker', ['exec', container, 'cat', filePath], { encoding: 'utf8' });
    if (result.status !== 0) {
      if (optional) return null;
      throw new Error(`cannot read ${filePath} in ${container}`);
    }
    return String(result.stdout ?? '');
  };

  try {
    const cgroup = parseCgroupPath(read('/proc/1/cgroup'));
    if (cgroup == null) return null;
    const cgroupRoot = resolveCgroupRoot(cgroup);
    if (cgroupRoot == null) return null;
    const cpu = parseKeyValueCounters(read(path.posix.join(cgroupRoot, 'cpu.stat')));
    const memoryCurrentBytes = parseInteger(read(path.posix.join(cgroupRoot, 'memory.current')));
    const memoryPeakRaw = read(path.posix.join(cgroupRoot, 'memory.peak'), true);
    const memoryPeakBytes = memoryPeakRaw == null ? null : parseInteger(memoryPeakRaw);
    const io = parseCgroupIoStat(read(path.posix.join(cgroupRoot, 'io.stat')));
    const process = parseProcStatus(read('/proc/1/status'));
    const network = parseNetDev(read('/proc/1/net/dev'));
    if (cpu.usage_usec == null || memoryCurrentBytes == null) return null;
    return {
      cpuUsageUsec: cpu.usage_usec,
      memoryCurrentBytes,
      memoryPeakBytes,
      processCurrentRssBytes: process.vmRssBytes,
      processPeakRssBytes: process.vmHwmBytes,
      ioReadBytes: io.readBytes,
      ioWriteBytes: io.writeBytes,
      networkRxBytes: network.rxBytes,
      networkTxBytes: network.txBytes,
      cgroupPath: cgroup
    };
  } catch {
    return null;
  }
}

function resolveCgroupRoot(cgroupPath) {
  if (typeof cgroupPath !== 'string' || !cgroupPath.startsWith('/')) return null;
  const root = '/sys/fs/cgroup';
  const resolved = path.posix.resolve(root, cgroupPath.slice(1));
  return resolved === root || resolved.startsWith(`${root}/`) ? resolved : null;
}

export function captureNativeLinuxContainerResources(pid, { readFile = fs.readFileSync } = {}) {
  try {
    const cgroup = parseCgroupPath(readText(readFile, `/proc/${pid}/cgroup`));
    if (cgroup == null) return null;
    const cgroupRoot = path.join('/sys/fs/cgroup', cgroup.replace(/^\/+/, ''));
    const cpu = parseKeyValueCounters(readText(readFile, path.join(cgroupRoot, 'cpu.stat')));
    const memoryCurrentBytes = parseInteger(readText(readFile, path.join(cgroupRoot, 'memory.current')));
    const memoryPeakBytes = readOptionalInteger(readFile, path.join(cgroupRoot, 'memory.peak'));
    const io = parseCgroupIoStat(readText(readFile, path.join(cgroupRoot, 'io.stat')));
    const process = parseProcStatus(readText(readFile, `/proc/${pid}/status`));
    const network = parseNetDev(readText(readFile, `/proc/${pid}/net/dev`));
    return {
      cpuUsageUsec: cpu.usage_usec ?? null,
      memoryCurrentBytes,
      memoryPeakBytes,
      processCurrentRssBytes: process.vmRssBytes,
      processPeakRssBytes: process.vmHwmBytes,
      ioReadBytes: io.readBytes,
      ioWriteBytes: io.writeBytes,
      networkRxBytes: network.rxBytes,
      networkTxBytes: network.txBytes,
      cgroupPath: cgroup
    };
  } catch {
    return null;
  }
}

export function captureDockerStats(container, { run = spawnSync } = {}) {
  const result = run(
    'docker',
    ['stats', '--no-stream', '--format', '{{json .}}', container],
    { encoding: 'utf8' }
  );
  if (result.status !== 0) return null;
  const line = String(result.stdout ?? '').trim().split('\n').find(Boolean);
  if (!line) return null;
  try {
    const stats = JSON.parse(line);
    const [memoryCurrentBytes] = parseBytePair(stats.MemUsage);
    const [networkRxBytes, networkTxBytes] = parseBytePair(stats.NetIO);
    const [ioReadBytes, ioWriteBytes] = parseBytePair(stats.BlockIO);
    return {
      cpuPercent: parsePercent(stats.CPUPerc),
      memoryCurrentBytes,
      memoryPeakBytes: null,
      processCurrentRssBytes: null,
      processPeakRssBytes: null,
      ioReadBytes,
      ioWriteBytes,
      networkRxBytes,
      networkTxBytes
    };
  } catch {
    return null;
  }
}

export function diffResourceSnapshots(before, after) {
  const labels = new Set([
    ...Object.keys(before?.components ?? {}),
    ...Object.keys(after?.components ?? {})
  ]);
  const components = Object.fromEntries(
    [...labels].map((label) => {
      const start = before?.components?.[label] ?? { status: 'not-running' };
      const end = after?.components?.[label] ?? { status: 'not-running' };
      return [label, diffComponent(start, end)];
    })
  );
  return {
    startedAt: before?.capturedAt ?? null,
    finishedAt: after?.capturedAt ?? null,
    wal: {
      startLsn: before?.walLsn ?? null,
      endLsn: after?.walLsn ?? null,
      insertedBytes: lsnDistanceBytes(before?.walLsn, after?.walLsn)
    },
    components,
    storage: diffStorage(before?.storage, after?.storage)
  };
}

function diffComponent(before, after) {
  const beforeIsAbsent = before?.status === 'not-running';
  const identityMatches =
    beforeIsAbsent ||
    (before?.containerId != null &&
      before.containerId === after?.containerId &&
      before.startedAt === after?.startedAt &&
      (before.source !== 'linux-cgroup-v2' || before.cgroupPath === after?.cgroupPath));
  const countersAvailable = after?.status === 'captured' && identityMatches;
  return {
    status: countersAvailable ? 'captured' : identityMatches ? after?.status ?? 'unavailable' : 'identity-mismatch',
    source: after?.source ?? null,
    access: after?.access ?? null,
    cpuSeconds: countersAvailable ? divide(counterDelta(before?.cpuUsageUsec, after?.cpuUsageUsec, beforeIsAbsent), 1_000_000) : null,
    sampledCpuPercent: after?.cpuPercent ?? null,
    currentMemoryBytes: after?.memoryCurrentBytes ?? null,
    cgroupLifetimePeakMemoryBytes: after?.memoryPeakBytes ?? null,
    mainProcessCurrentRssBytes: after?.processCurrentRssBytes ?? null,
    mainProcessLifetimePeakRssBytes: after?.processPeakRssBytes ?? null,
    blockReadBytes: countersAvailable ? counterDelta(before?.ioReadBytes, after?.ioReadBytes, beforeIsAbsent) : null,
    blockWriteBytes: countersAvailable ? counterDelta(before?.ioWriteBytes, after?.ioWriteBytes, beforeIsAbsent) : null,
    networkRxBytes: countersAvailable ? counterDelta(before?.networkRxBytes, after?.networkRxBytes, beforeIsAbsent) : null,
    networkTxBytes: countersAvailable ? counterDelta(before?.networkTxBytes, after?.networkTxBytes, beforeIsAbsent) : null
  };
}

function diffStorage(before = {}, after = {}) {
  const labels = new Set([...Object.keys(before), ...Object.keys(after)]);
  return Object.fromEntries(
    [...labels].map((label) => [
      label,
      {
        logicalBytes: signedDelta(before[label]?.logicalBytes, after[label]?.logicalBytes, before[label] == null),
        allocatedBytes: signedDelta(before[label]?.allocatedBytes, after[label]?.allocatedBytes, before[label] == null),
        files: signedDelta(before[label]?.files, after[label]?.files, before[label] == null)
      }
    ])
  );
}

export function measureStorage(entries) {
  return Object.fromEntries(
    entries.map(({ label, filePath }) => [label, measurePath(filePath)])
  );
}

export function measurePath(filePath) {
  const totals = { logicalBytes: 0, allocatedBytes: 0, files: 0 };
  if (!fs.existsSync(filePath)) return totals;
  const pending = [filePath];
  while (pending.length > 0) {
    const current = pending.pop();
    // Live data directories drop entries between listing and stat (journal
    // rotation, checkpoint temp files); skip vanished paths instead of failing.
    let stat;
    try {
      stat = fs.lstatSync(current, { bigint: true });
    } catch (error) {
      if (error?.code === 'ENOENT') continue;
      throw error;
    }
    if (stat.isDirectory()) {
      let entries;
      try {
        entries = fs.readdirSync(current);
      } catch (error) {
        if (error?.code === 'ENOENT') continue;
        throw error;
      }
      for (const entry of entries) pending.push(path.join(current, entry));
      continue;
    }
    totals.files += 1;
    totals.logicalBytes += safeNumber(stat.size);
    totals.allocatedBytes += safeNumber(stat.blocks * 512n);
  }
  return totals;
}

export function parseCgroupPath(input) {
  for (const line of String(input).split('\n')) {
    const match = /^0::(\/.*)$/.exec(line.trim());
    if (match) return match[1];
  }
  return null;
}

export function parseKeyValueCounters(input) {
  const result = {};
  for (const line of String(input).trim().split('\n')) {
    const [key, value] = line.trim().split(/\s+/, 2);
    const parsed = parseInteger(value);
    if (key && parsed != null) result[key] = parsed;
  }
  return result;
}

export function parseCgroupIoStat(input) {
  let readBytes = 0;
  let writeBytes = 0;
  let fields = 0;
  for (const line of String(input).trim().split('\n')) {
    for (const field of line.trim().split(/\s+/).slice(1)) {
      const [key, raw] = field.split('=', 2);
      const value = parseInteger(raw);
      if (key === 'rbytes' && value != null) {
        readBytes += value;
        fields += 1;
      }
      if (key === 'wbytes' && value != null) {
        writeBytes += value;
        fields += 1;
      }
    }
  }
  return fields > 0 ? { readBytes, writeBytes } : { readBytes: null, writeBytes: null };
}

export function parseProcStatus(input) {
  const value = (name) => {
    const match = new RegExp(`^${name}:\\s+(\\d+)\\s+kB$`, 'm').exec(String(input));
    return match ? Number(match[1]) * 1024 : null;
  };
  return { vmRssBytes: value('VmRSS'), vmHwmBytes: value('VmHWM') };
}

export function parseNetDev(input) {
  let rxBytes = 0;
  let txBytes = 0;
  let interfaces = 0;
  for (const line of String(input).split('\n')) {
    const match = /^\s*([^:]+):\s*(.*)$/.exec(line);
    if (!match || match[1].trim() === 'lo') continue;
    const fields = match[2].trim().split(/\s+/);
    const rx = parseInteger(fields[0]);
    const tx = parseInteger(fields[8]);
    if (rx == null || tx == null) continue;
    rxBytes += rx;
    txBytes += tx;
    interfaces += 1;
  }
  return interfaces > 0 ? { rxBytes, txBytes } : { rxBytes: null, txBytes: null };
}

function parseJsonString(value) {
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === 'string' ? parsed : null;
  } catch {
    return null;
  }
}

export function parseByteSize(input) {
  const match = /^\s*([0-9]+(?:\.[0-9]+)?)\s*([kmgtpe]?i?b)\s*$/i.exec(String(input ?? ''));
  if (!match) return null;
  const value = Number(match[1]);
  const unit = match[2].toLowerCase();
  const decimal = { b: 1, kb: 1e3, mb: 1e6, gb: 1e9, tb: 1e12, pb: 1e15, eb: 1e18 };
  const binary = { kib: 2 ** 10, mib: 2 ** 20, gib: 2 ** 30, tib: 2 ** 40, pib: 2 ** 50, eib: 2 ** 60 };
  const multiplier = decimal[unit] ?? binary[unit];
  return multiplier == null ? null : Math.round(value * multiplier);
}

export function lsnDistanceBytes(start, end) {
  const startValue = parseLsn(start);
  const endValue = parseLsn(end);
  if (startValue == null || endValue == null || endValue < startValue) return null;
  const difference = endValue - startValue;
  return difference <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(difference) : difference.toString();
}

function parseLsn(value) {
  if (typeof value !== 'string' || !/^[0-9A-F]+\/[0-9A-F]+$/i.test(value)) return null;
  const [upper, lower] = value.split('/');
  return (BigInt(`0x${upper}`) << 32n) + BigInt(`0x${lower}`);
}

function parseBytePair(input) {
  const [left, right] = String(input ?? '').split(/\s*\/\s*/, 2);
  return [parseByteSize(left), parseByteSize(right)];
}

function parsePercent(input) {
  const value = Number.parseFloat(String(input ?? '').replace('%', ''));
  return Number.isFinite(value) ? value : null;
}

function parseInteger(input) {
  if (!/^\d+$/.test(String(input ?? '').trim())) return null;
  const value = Number(input);
  return Number.isSafeInteger(value) ? value : null;
}

function readOptionalInteger(readFile, filePath) {
  try {
    return parseInteger(readText(readFile, filePath));
  } catch {
    return null;
  }
}

function readText(readFile, filePath) {
  return String(readFile(filePath, 'utf8'));
}

function counterDelta(before, after, missingBeforeIsZero = false) {
  const start = before == null && missingBeforeIsZero ? 0 : Number(before);
  const finish = Number(after);
  if (!Number.isFinite(start) || !Number.isFinite(finish) || finish < start) return null;
  return finish - start;
}

// Storage sizes are not monotonic counters: allocated bytes legitimately
// shrink when a store releases preallocated extents or drops temp files.
function signedDelta(before, after, missingBeforeIsZero = false) {
  const start = before == null && missingBeforeIsZero ? 0 : Number(before);
  const finish = Number(after);
  if (!Number.isFinite(start) || !Number.isFinite(finish)) return null;
  return finish - start;
}

function divide(value, divisor) {
  return value == null ? null : value / divisor;
}

function safeNumber(value) {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`resource counter exceeds JavaScript safe integer: ${value}`);
  }
  return Number(value);
}
