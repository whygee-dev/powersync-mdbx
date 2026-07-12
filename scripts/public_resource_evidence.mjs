export function summarizePublicResourceEvidence(resources) {
  const summarizeWindow = (window) => {
    if (window == null) return null;
    return {
      durationMs:
        Number.isFinite(Date.parse(window.finishedAt)) && Number.isFinite(Date.parse(window.startedAt))
          ? Date.parse(window.finishedAt) - Date.parse(window.startedAt)
          : null,
      walInsertedBytes: window.wal?.insertedBytes ?? null,
      components: Object.fromEntries(
        Object.entries(window.components ?? {}).map(([name, component]) => [
          name,
          {
            status: component.status ?? null,
            source: component.source ?? null,
            access: component.access ?? null,
            cpuSeconds: component.cpuSeconds ?? null,
            cgroupLifetimePeakMemoryBytes: component.cgroupLifetimePeakMemoryBytes ?? null,
            mainProcessLifetimePeakRssBytes: component.mainProcessLifetimePeakRssBytes ?? null,
            blockReadBytes: component.blockReadBytes ?? null,
            blockWriteBytes: component.blockWriteBytes ?? null,
            networkRxBytes: component.networkRxBytes ?? null,
            networkTxBytes: component.networkTxBytes ?? null
          }
        ])
      ),
      storageGrowth: Object.fromEntries(
        Object.entries(window.storage ?? {}).map(([name, storage]) => [
          name,
          {
            logicalBytes: storage.logicalBytes ?? null,
            allocatedBytes: storage.allocatedBytes ?? null,
            files: storage.files ?? null
          }
        ])
      )
    };
  };

  return {
    status: resources?.status ?? 'not-collected',
    initial: summarizeWindow(resources?.initial),
    total: summarizeWindow(resources?.total)
  };
}
