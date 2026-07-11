import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  cgroupBelongsToContainer,
  captureContainerResources,
  diffResourceSnapshots,
  lsnDistanceBytes,
  measurePath,
  parseByteSize,
  parseCgroupIoStat,
  parseCgroupPath,
  parseKeyValueCounters,
  parseNetDev,
  parseProcStatus
} from './resource_evidence.mjs';

test('host cgroup identity must contain the inspected container id', () => {
  const id = 'a'.repeat(64);
  assert.equal(cgroupBelongsToContainer(`/system.slice/docker-${id}.scope`, id), true);
  assert.equal(cgroupBelongsToContainer('/system.slice/docker-unrelated.scope', id), false);
  assert.equal(cgroupBelongsToContainer('/', id), false);
});

test('reads cgroup and proc counters from inside a Linux container when host proc is unavailable', () => {
  const files = new Map([
    ['/proc/1/cgroup', '0::/\n'],
    ['/sys/fs/cgroup/cpu.stat', 'usage_usec 123456\nuser_usec 100000\n'],
    ['/sys/fs/cgroup/memory.current', '4096\n'],
    ['/sys/fs/cgroup/memory.peak', '8192\n'],
    ['/sys/fs/cgroup/io.stat', '8:0 rbytes=10 wbytes=20\n'],
    ['/proc/1/status', 'VmHWM:\t7 kB\nVmRSS:\t5 kB\n'],
    ['/proc/1/net/dev', 'eth0: 30 0 0 0 0 0 0 0 40 0\n']
  ]);
  const run = (_command, args) => {
    if (args[0] === 'inspect') {
      return { status: 0, stdout: '"container-id"\t"2026-01-01T00:00:00Z"\t42\n' };
    }
    const value = files.get(args[3]);
    return value == null ? { status: 1, stdout: '' } : { status: 0, stdout: value };
  };
  const captured = captureContainerResources('service', {
    run,
    readFile: () => {
      throw new Error('host proc unavailable');
    }
  });
  assert.deepEqual(captured, {
    status: 'captured',
    container: 'service',
    containerId: 'container-id',
    startedAt: '2026-01-01T00:00:00Z',
    source: 'linux-cgroup-v2',
    access: 'docker-exec',
    cpuUsageUsec: 123456,
    memoryCurrentBytes: 4096,
    memoryPeakBytes: 8192,
    processCurrentRssBytes: 5120,
    processPeakRssBytes: 7168,
    ioReadBytes: 10,
    ioWriteBytes: 20,
    networkRxBytes: 30,
    networkTxBytes: 40,
    cgroupPath: '/'
  });
});

test('docker-exec reads counters below a non-root container cgroup path', () => {
  const cgroup = '/system.slice/docker-service.scope';
  const cgroupRoot = `/sys/fs/cgroup${cgroup}`;
  const files = new Map([
    ['/proc/1/cgroup', `0::${cgroup}\n`],
    [`${cgroupRoot}/cpu.stat`, 'usage_usec 321\n'],
    [`${cgroupRoot}/memory.current`, '1024\n'],
    [`${cgroupRoot}/memory.peak`, '2048\n'],
    [`${cgroupRoot}/io.stat`, '8:0 rbytes=11 wbytes=22\n'],
    ['/proc/1/status', 'VmHWM:\t4 kB\nVmRSS:\t3 kB\n'],
    ['/proc/1/net/dev', 'eth0: 33 0 0 0 0 0 0 0 44 0\n']
  ]);
  const requested = [];
  const run = (_command, args) => {
    if (args[0] === 'inspect') {
      return { status: 0, stdout: '"container-id"\t"2026-01-01T00:00:00Z"\t42\n' };
    }
    requested.push(args[3]);
    const value = files.get(args[3]);
    return value == null ? { status: 1, stdout: '' } : { status: 0, stdout: value };
  };
  const captured = captureContainerResources('service', {
    run,
    readFile: () => {
      throw new Error('host proc unavailable');
    }
  });
  assert.equal(captured.source, 'linux-cgroup-v2');
  assert.equal(captured.access, 'docker-exec');
  assert.equal(captured.cgroupPath, cgroup);
  assert.equal(captured.cpuUsageUsec, 321);
  assert.equal(captured.memoryPeakBytes, 2048);
  assert.ok(requested.includes(`${cgroupRoot}/cpu.stat`));
  assert.ok(!requested.includes('/sys/fs/cgroup/cpu.stat'));
});

test('parses cgroup v2 counters and sums block devices', () => {
  assert.equal(parseCgroupPath('0::/system.slice/docker-abc.scope\n'), '/system.slice/docker-abc.scope');
  assert.deepEqual(parseKeyValueCounters('usage_usec 1234\nuser_usec 1000\n'), {
    usage_usec: 1234,
    user_usec: 1000
  });
  assert.deepEqual(
    parseCgroupIoStat('8:0 rbytes=10 wbytes=20 rios=1 wios=2\n8:16 rbytes=30 wbytes=40'),
    { readBytes: 40, writeBytes: 60 }
  );
  assert.deepEqual(parseCgroupIoStat(''), { readBytes: null, writeBytes: null });
});

test('parses process RSS and namespace network counters', () => {
  assert.deepEqual(parseProcStatus('Name:\ttest\nVmHWM:\t200 kB\nVmRSS:\t150 kB\n'), {
    vmRssBytes: 153600,
    vmHwmBytes: 204800
  });
  assert.deepEqual(
    parseNetDev('Inter-| Receive | Transmit\n lo: 5 0 0 0 0 0 0 0 6 0\n eth0: 100 0 0 0 0 0 0 0 200 0'),
    { rxBytes: 100, txBytes: 200 }
  );
  assert.deepEqual(parseNetDev('Inter-| Receive | Transmit\n lo: 5 0 0 0 0 0 0 0 6 0'), {
    rxBytes: null,
    txBytes: null
  });
});

test('parses Docker decimal and binary byte units', () => {
  assert.equal(parseByteSize('1.5kB'), 1500);
  assert.equal(parseByteSize('1.5MiB'), 1572864);
  assert.equal(parseByteSize('8GiB'), 8589934592);
  assert.equal(parseByteSize('n/a'), null);
});

test('computes LSN distance across a segment boundary', () => {
  assert.equal(lsnDistanceBytes('0/FFFFFFF0', '1/00000010'), 32);
  assert.equal(lsnDistanceBytes('1/10', '1/0F'), null);
});

test('resource deltas reject reset counters and preserve peak evidence', () => {
  const before = {
    capturedAt: '2026-01-01T00:00:00Z',
    walLsn: '0/10',
    components: {
      service: {
        status: 'captured',
        containerId: 'same',
        startedAt: '2026-01-01T00:00:00Z',
        cpuUsageUsec: 100,
        ioReadBytes: 10,
        ioWriteBytes: 20
      }
    },
    storage: { data: { logicalBytes: 100, allocatedBytes: 80, files: 1 } }
  };
  const after = {
    capturedAt: '2026-01-01T00:00:01Z',
    walLsn: '0/30',
    components: {
      service: {
        status: 'captured',
        source: 'linux-cgroup-v2',
        containerId: 'same',
        startedAt: '2026-01-01T00:00:00Z',
        cpuUsageUsec: 1100,
        memoryPeakBytes: 4096,
        processPeakRssBytes: 2048,
        ioReadBytes: 5,
        ioWriteBytes: 120
      }
    },
    storage: { data: { logicalBytes: 250, allocatedBytes: 200, files: 2 } }
  };
  const delta = diffResourceSnapshots(before, after);
  assert.equal(delta.wal.insertedBytes, 32);
  assert.equal(delta.components.service.cpuSeconds, 0.001);
  assert.equal(delta.components.service.blockReadBytes, null);
  assert.equal(delta.components.service.blockWriteBytes, 100);
  assert.equal(delta.components.service.cgroupLifetimePeakMemoryBytes, 4096);
  assert.equal(delta.components.service.mainProcessLifetimePeakRssBytes, 2048);
  assert.deepEqual(delta.storage.data, { logicalBytes: 150, allocatedBytes: 120, files: 1 });

  const replaced = diffResourceSnapshots(before, {
    ...after,
    components: {
      service: { ...after.components.service, containerId: 'replacement' }
    }
  });
  assert.equal(replaced.components.service.status, 'identity-mismatch');
  assert.equal(replaced.components.service.cpuSeconds, null);
});

test('storage accounting distinguishes sparse logical and allocated bytes', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'resource-evidence-'));
  try {
    const file = path.join(directory, 'sparse');
    const descriptor = fs.openSync(file, 'w');
    fs.ftruncateSync(descriptor, 8 * 1024 * 1024);
    fs.closeSync(descriptor);
    const measured = measurePath(directory);
    assert.equal(measured.files, 1);
    assert.equal(measured.logicalBytes, 8 * 1024 * 1024);
    assert.ok(measured.allocatedBytes < measured.logicalBytes);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
