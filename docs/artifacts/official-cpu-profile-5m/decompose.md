## Context totals, share of active CPU

Self time plus runtime-bucket time re-attributed to the nearest categorized caller on the stack.

| Context | Self time (ms) | Share of active |
| --- | ---: | ---: |
| marshalling | 356660.4 | 47.6% |
| row processing | 276210.4 | 36.9% |
| gc | 54294.0 | 7.2% |
| (unattributed runtime) | 41595.5 | 5.6% |
| logging | 20681.1 | 2.8% |

## Runtime buckets by nearest categorized caller

| Leaf bucket | Share of active | Split |
| --- | ---: | --- |
| node core | 16.2% | row processing 10.2%, marshalling 4.3%, (none) 1.6%, logging 0.0% |
| native builtins | 13.0% | marshalling 10.4%, row processing 1.4%, (none) 0.9%, logging 0.3% |
| other | 3.4% | row processing 2.9%, logging 0.4%, (none) 0.1%, marshalling 0.0% |
| program | 2.9% | (none) 2.9% |

## Named hotspots inside the runtime buckets

node:internal/crypto totals 8.6% of active CPU; nearest categorized callers:

- other service code: hashData — 5.3%
- other service code: uuidForRowBson — 1.6%
- other service code: hashDelete — 1.5%
- postgres replication: snapshotTable — 0.1%
- mongo storage: saveBucketData — 0.0%
- postgres replication: _hash — 0.0%

Native-builtin time under marshalling, by nearest bson/driver caller:

- utf8ByteLength — 5.5%
- writeCommand — 2.0%
- toHex — 1.8%
- encodeUTF8Into — 1.1%
- allocateUnsafe — 0.0%
- now — 0.0%

Node core by module:

- internal/crypto — 8.6%
- buffer — 2.1%
- internal/buffer — 1.5%
- internal/streams — 0.7%
- internal/bootstrap — 0.5%
- internal/encoding — 0.4%

Uncategorized dependency functions:

- parse — 0.6%
- parseYear — 0.5%
- splitDateString — 0.3%
- parseISO — 0.3%
- parseDate — 0.2%
- v35 — 0.2%

## Idle structure

| Idle run length | Runs | Idle time (ms) | Share of idle |
| --- | ---: | ---: | ---: |
| <2ms | 8782 | 15406.0 | 2.5% |
| 2-10ms | 52516 | 286517.1 | 46.6% |
| 10-100ms | 16546 | 275950.2 | 44.9% |
| 100ms-1s | 313 | 36574.3 | 6.0% |

Idle time by the context of the last active frame before the run:

- after marshalling — 68.0%
- after row processing — 13.7%
- after (none) — 11.2%
- after gc — 5.4%
- after logging — 1.6%

First 90% of wall clock: 41.6% idle across 73376 idle runs (59.8 runs per second). Last 10%: 76.0% idle. Overall: 45.1% idle.
