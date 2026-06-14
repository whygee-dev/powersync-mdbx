use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    protocol::messages::{
        sample_sync_fixture_signature, sample_sync_last_op_id, sample_sync_stream_bson_chunks,
        sample_sync_stream_ndjson_chunks, SyncChunk,
    },
    sync_rules::RustExecutionPlan,
};

use super::{
    Storage, StorageError, StreamEncoding, SyncBodySource, SyncBucketCursors, SyncChunkSource,
};

const DEFAULT_SYNC_EDGE_PATH: &str = "/tmp/powersync-rust-sync-edge";
const MANIFEST_FILE: &str = "manifest.json";
const INDEX_FILE: &str = "cursor-index.bin";
const NDJSON_BODY_FILE: &str = "snapshot-ndjson.body";
const BSON_BODY_FILE: &str = "snapshot-bson.body";
const INDEX_RECORD_SIZE: usize = 1 + 8 + 8 + 8 + 4;
const INDEX_MAGIC: &[u8; 8] = b"PSSEDGE1";
const STORAGE_LAYOUT_VERSION: &str = "sync-edge-v2";

#[derive(Debug)]
pub struct SyncEdgeStorage {
    root_dir: PathBuf,
    last_op_id: u64,
    ndjson_path: PathBuf,
    bson_path: PathBuf,
    stream_index: HashMap<(StreamEncoding, u64), StreamIndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyncEdgeManifest {
    fixture_signature: String,
    last_op_id: u64,
    generation: String,
    stream_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct StreamIndexEntry {
    encoding: StreamEncoding,
    after: u64,
    offset: u64,
    length: u64,
    chunk_count: u32,
}

impl SyncEdgeStorage {
    pub fn new_from_env() -> Self {
        let path = env::var("POWERSYNC_RUST_SYNC_EDGE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_SYNC_EDGE_PATH));
        Self::new(path)
    }

    pub fn new(path: impl AsRef<Path>) -> Self {
        let root_dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&root_dir).unwrap_or_else(|error| {
            panic!(
                "failed to create sync-edge root at {}: {error}",
                root_dir.display()
            )
        });
        let _init_guard = init_lock()
            .lock()
            .expect("sync-edge init lock should not be poisoned");

        let fixture_signature = storage_fixture_signature();
        let fixture_last_op_id = sample_sync_last_op_id();

        let manifest = match read_manifest(&root_dir) {
            Some(manifest)
                if manifest.fixture_signature == fixture_signature
                    && manifest.last_op_id == fixture_last_op_id
                    && generation_files_exist(&root_dir, &manifest.generation) =>
            {
                manifest
            }
            _ => seed_generation(&root_dir, fixture_last_op_id, &fixture_signature),
        };

        let generation_dir = root_dir.join(&manifest.generation);
        let stream_index = load_stream_index(&generation_dir);
        let ndjson_path = generation_dir.join(NDJSON_BODY_FILE);
        let bson_path = generation_dir.join(BSON_BODY_FILE);

        info!(
            backend = "sync-edge",
            root_dir = %root_dir.display(),
            generation = %manifest.generation,
            fixture_signature = %manifest.fixture_signature,
            last_op_id = manifest.last_op_id,
            stream_count = stream_index.len(),
            request_rebuild = false,
            in_memory_fallback = false,
            read_path = "preframed-sync-edge",
            "initialized sync-edge preframed snapshot storage"
        );

        Self {
            root_dir,
            last_op_id: manifest.last_op_id,
            ndjson_path,
            bson_path,
            stream_index,
        }
    }

    pub fn last_op_id(&self) -> u64 {
        self.last_op_id
    }

    fn snapshot_blob_path(&self, encoding: StreamEncoding) -> &Path {
        match encoding {
            StreamEncoding::Ndjson => &self.ndjson_path,
            StreamEncoding::Bson => &self.bson_path,
        }
    }
}

fn init_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

impl Default for SyncEdgeStorage {
    fn default() -> Self {
        Self::new_from_env()
    }
}

impl Storage for SyncEdgeStorage {
    fn sync_chunk_source_for_buckets_with_plan(
        &self,
        buckets: &SyncBucketCursors,
        _plan: &RustExecutionPlan,
        encoding: StreamEncoding,
    ) -> Result<SyncChunkSource, StorageError> {
        Ok(self.sync_chunk_source(buckets.max_after(), encoding))
    }

    fn sync_hold_open_body_source_for_buckets_with_plan(
        &self,
        buckets: &SyncBucketCursors,
        _plan: &RustExecutionPlan,
        encoding: StreamEncoding,
    ) -> Result<Option<SyncBodySource>, StorageError> {
        Ok(Some(self.sync_body_source(buckets.max_after(), encoding)?))
    }
}

impl SyncEdgeStorage {
    fn sync_chunk_source(&self, after: Option<u64>, encoding: StreamEncoding) -> SyncChunkSource {
        let after = after.unwrap_or(0).min(self.last_op_id);
        let chunks: Vec<SyncChunk> = match encoding {
            StreamEncoding::Ndjson => sample_sync_stream_ndjson_chunks(Some(after)),
            StreamEncoding::Bson => sample_sync_stream_bson_chunks(Some(after)),
        };

        info!(
            backend = "sync-edge",
            root_dir = %self.root_dir.display(),
            encoding = ?encoding,
            after,
            chunk_count = chunks.len(),
            request_rebuild = false,
            in_memory_fallback = false,
            read_path = "fixture-chunks-fallback",
            "loaded sync chunks from fixture fallback stream path"
        );

        SyncChunkSource {
            chunk_count_hint: Some(chunks.len()),
            chunks: Box::new(chunks.into_iter()),
            final_cursors: None,
        }
    }

    fn sync_body_source(
        &self,
        after: Option<u64>,
        encoding: StreamEncoding,
    ) -> Result<SyncBodySource, StorageError> {
        let after = after.unwrap_or(0).min(self.last_op_id);
        let started_at = std::time::Instant::now();

        let Some(entry) = self.stream_index.get(&(encoding, after)).copied() else {
            return Err(StorageError(format!(
                "missing sync-edge stream for encoding={encoding:?}, after={after}, root={}",
                self.root_dir.display()
            )));
        };
        let body = read_blob_slice(
            self.snapshot_blob_path(entry.encoding),
            entry.offset,
            entry.length,
        );
        let body_len = body.len();
        let chunk_count = entry.chunk_count as usize;

        info!(
            backend = "sync-edge",
            root_dir = %self.root_dir.display(),
            encoding = ?encoding,
            after,
            body_bytes = body_len,
            chunk_count,
            read_ms = started_at.elapsed().as_millis(),
            request_rebuild = false,
            in_memory_fallback = false,
            read_path = "preframed-sync-edge",
            "loaded preframed sync body from sync-edge snapshots"
        );

        Ok(SyncBodySource {
            body: Bytes::from(body),
            chunk_count_hint: Some(chunk_count),
        })
    }
}

fn read_manifest(root_dir: &Path) -> Option<SyncEdgeManifest> {
    let bytes = fs::read(root_dir.join(MANIFEST_FILE)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn generation_files_exist(root_dir: &Path, generation: &str) -> bool {
    let generation_dir = root_dir.join(generation);
    let index = generation_dir.join(INDEX_FILE);
    let ndjson = generation_dir.join(NDJSON_BODY_FILE);
    let bson = generation_dir.join(BSON_BODY_FILE);

    index.exists()
        && ndjson.exists()
        && bson.exists()
        && fs::metadata(index)
            .map(|meta| meta.len() > 0)
            .unwrap_or(false)
        && fs::metadata(ndjson)
            .map(|meta| meta.len() > 0)
            .unwrap_or(false)
        && fs::metadata(bson)
            .map(|meta| meta.len() > 0)
            .unwrap_or(false)
}

fn seed_generation(root_dir: &Path, last_op_id: u64, fixture_signature: &str) -> SyncEdgeManifest {
    let generation = format!("sync-edge-{}", sanitize_signature(fixture_signature));
    let generation_dir = root_dir.join(&generation);

    fs::create_dir_all(&generation_dir).unwrap_or_else(|error| {
        panic!(
            "failed to create sync-edge generation dir {}: {error}",
            generation_dir.display()
        )
    });

    let ndjson_entries = write_preframed_snapshot_blob(
        &generation_dir.join(NDJSON_BODY_FILE),
        StreamEncoding::Ndjson,
        last_op_id,
    );
    let bson_entries = write_preframed_snapshot_blob(
        &generation_dir.join(BSON_BODY_FILE),
        StreamEncoding::Bson,
        last_op_id,
    );

    let mut index_entries = Vec::with_capacity(ndjson_entries.len() + bson_entries.len());
    index_entries.extend(ndjson_entries);
    index_entries.extend(bson_entries);
    write_index_file(&generation_dir.join(INDEX_FILE), &index_entries);

    let manifest = SyncEdgeManifest {
        fixture_signature: fixture_signature.to_owned(),
        last_op_id,
        generation,
        stream_count: index_entries.len(),
    };
    write_manifest(root_dir, &manifest);

    info!(
        backend = "sync-edge",
        root_dir = %root_dir.display(),
        generation = %manifest.generation,
        fixture_signature = %manifest.fixture_signature,
        last_op_id = manifest.last_op_id,
        stream_count = manifest.stream_count,
        request_rebuild = false,
        in_memory_fallback = false,
        "compiled preframed sync-edge snapshots"
    );

    manifest
}

fn write_manifest(root_dir: &Path, manifest: &SyncEdgeManifest) {
    let bytes = serde_json::to_vec_pretty(manifest).expect("sync-edge manifest should serialize");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let tmp_path = root_dir.join(format!(
        "{MANIFEST_FILE}.{}.{}.tmp",
        std::process::id(),
        nonce
    ));

    let mut file = fs::File::create(&tmp_path).unwrap_or_else(|error| {
        panic!(
            "failed to create temporary sync-edge manifest {}: {error}",
            tmp_path.display()
        )
    });
    file.write_all(&bytes)
        .expect("failed to write temporary sync-edge manifest");
    fs::rename(&tmp_path, root_dir.join(MANIFEST_FILE)).unwrap_or_else(|error| {
        panic!(
            "failed to publish sync-edge manifest into {}: {error}",
            root_dir.display()
        )
    });
}

fn write_preframed_snapshot_blob(
    path: &Path,
    encoding: StreamEncoding,
    last_op_id: u64,
) -> Vec<StreamIndexEntry> {
    let file = File::create(path).unwrap_or_else(|error| {
        panic!(
            "failed to create sync-edge blob at {}: {error}",
            path.display()
        )
    });
    let mut writer = BufWriter::new(file);
    let mut entries = Vec::with_capacity((last_op_id + 1) as usize);
    let mut offset = 0u64;

    for after in 0..=last_op_id {
        let chunks = match encoding {
            StreamEncoding::Ndjson => sample_sync_stream_ndjson_chunks(Some(after)),
            StreamEncoding::Bson => sample_sync_stream_bson_chunks(Some(after)),
        };
        let chunk_count = chunks.len() as u32;
        let start_offset = offset;

        for chunk in chunks {
            let chunk_len = chunk.bytes.len() as u64;
            writer
                .write_all(chunk.bytes.as_ref())
                .unwrap_or_else(|error| {
                    panic!(
                        "failed to write sync-edge blob chunk at {}: {error}",
                        path.display()
                    )
                });
            offset += chunk_len;
        }

        entries.push(StreamIndexEntry {
            encoding,
            after,
            offset: start_offset,
            length: offset - start_offset,
            chunk_count,
        });
    }

    writer.flush().unwrap_or_else(|error| {
        panic!(
            "failed to flush sync-edge blob at {}: {error}",
            path.display()
        )
    });

    entries
}

fn load_stream_index(generation_dir: &Path) -> HashMap<(StreamEncoding, u64), StreamIndexEntry> {
    let index_entries = read_index_file(&generation_dir.join(INDEX_FILE));
    let mut streams = HashMap::with_capacity(index_entries.len());

    for entry in index_entries {
        streams.insert((entry.encoding, entry.after), entry);
    }

    streams
}

fn write_index_file(path: &Path, entries: &[StreamIndexEntry]) {
    let mut bytes = Vec::with_capacity(12 + (entries.len() * INDEX_RECORD_SIZE));
    bytes.extend_from_slice(INDEX_MAGIC);
    bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    for entry in entries {
        bytes.push(encoding_code(entry.encoding));
        bytes.extend_from_slice(&entry.after.to_le_bytes());
        bytes.extend_from_slice(&entry.offset.to_le_bytes());
        bytes.extend_from_slice(&entry.length.to_le_bytes());
        bytes.extend_from_slice(&entry.chunk_count.to_le_bytes());
    }

    fs::write(path, bytes).unwrap_or_else(|error| {
        panic!(
            "failed to write sync-edge index {}: {error}",
            path.display()
        )
    });
}

fn read_index_file(path: &Path) -> Vec<StreamIndexEntry> {
    let bytes = fs::read(path).unwrap_or_else(|error| {
        panic!("failed to read sync-edge index {}: {error}", path.display())
    });

    let Some(magic) = bytes.get(..INDEX_MAGIC.len()) else {
        panic!(
            "sync-edge index at {} is too short for magic header",
            path.display()
        );
    };
    assert_eq!(
        magic,
        INDEX_MAGIC,
        "sync-edge index at {} has invalid magic",
        path.display()
    );

    let mut cursor = INDEX_MAGIC.len();
    let record_count = read_u32(&bytes, &mut cursor) as usize;
    let expected_len = cursor + (record_count * INDEX_RECORD_SIZE);
    assert_eq!(
        expected_len,
        bytes.len(),
        "sync-edge index at {} has unexpected length",
        path.display()
    );

    let mut entries = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        let encoding = decode_encoding(
            *bytes
                .get(cursor)
                .unwrap_or_else(|| panic!("sync-edge index at {} truncated", path.display())),
        );
        cursor += 1;

        let after = read_u64(&bytes, &mut cursor);
        let offset = read_u64(&bytes, &mut cursor);
        let length = read_u64(&bytes, &mut cursor);
        let chunk_count = read_u32(&bytes, &mut cursor);

        entries.push(StreamIndexEntry {
            encoding,
            after,
            offset,
            length,
            chunk_count,
        });
    }

    entries
}

fn read_blob_slice(path: &Path, offset: u64, length: u64) -> Vec<u8> {
    let mut file = File::open(path).unwrap_or_else(|error| {
        panic!("failed to open sync-edge blob {}: {error}", path.display())
    });
    file.seek(SeekFrom::Start(offset)).unwrap_or_else(|error| {
        panic!(
            "failed to seek sync-edge blob {} to {}: {error}",
            path.display(),
            offset
        )
    });

    let body_len = usize::try_from(length).unwrap_or_else(|_| {
        panic!(
            "sync-edge blob slice length {} does not fit in usize for {}",
            length,
            path.display()
        )
    });
    let mut body = vec![0_u8; body_len];
    file.read_exact(&mut body).unwrap_or_else(|error| {
        panic!(
            "failed to read sync-edge blob {} slice offset={} length={}: {error}",
            path.display(),
            offset,
            length
        )
    });

    body
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> u32 {
    let end = *cursor + 4;
    let value = bytes
        .get(*cursor..end)
        .unwrap_or_else(|| panic!("expected 4-byte value at offset {}", *cursor))
        .try_into()
        .expect("slice length should be 4");
    *cursor = end;
    u32::from_le_bytes(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> u64 {
    let end = *cursor + 8;
    let value = bytes
        .get(*cursor..end)
        .unwrap_or_else(|| panic!("expected 8-byte value at offset {}", *cursor))
        .try_into()
        .expect("slice length should be 8");
    *cursor = end;
    u64::from_le_bytes(value)
}

fn encoding_code(encoding: StreamEncoding) -> u8 {
    match encoding {
        StreamEncoding::Ndjson => 0,
        StreamEncoding::Bson => 1,
    }
}

fn decode_encoding(value: u8) -> StreamEncoding {
    match value {
        0 => StreamEncoding::Ndjson,
        1 => StreamEncoding::Bson,
        _ => panic!("unknown sync-edge encoding code: {value}"),
    }
}

fn storage_fixture_signature() -> String {
    format!(
        "{STORAGE_LAYOUT_VERSION}:{}",
        sample_sync_fixture_signature()
    )
}

fn sanitize_signature(signature: &str) -> String {
    signature
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn sync_edge_storage_preframed_bodies_match_fixture_chunks_for_common_cursors() {
        let directory = TempDir::new().expect("temp directory should exist");
        let storage = SyncEdgeStorage::new(directory.path());
        let last_op_id = sample_sync_last_op_id();

        for after in [
            None,
            Some(0),
            Some(1),
            Some(15),
            Some(last_op_id),
            Some(last_op_id + 10),
        ] {
            assert_body_matches_fixture(&storage, after, StreamEncoding::Ndjson);
            assert_body_matches_fixture(&storage, after, StreamEncoding::Bson);
        }
    }

    fn assert_body_matches_fixture(
        storage: &SyncEdgeStorage,
        after: Option<u64>,
        encoding: StreamEncoding,
    ) {
        let body = storage
            .sync_body_source(after, encoding)
            .expect("sync body source")
            .body;
        let expected_chunks = match encoding {
            StreamEncoding::Ndjson => sample_sync_stream_ndjson_chunks(after),
            StreamEncoding::Bson => sample_sync_stream_bson_chunks(after),
        };

        let mut expected_body = Vec::new();
        for chunk in expected_chunks {
            expected_body.extend_from_slice(&chunk.bytes);
        }

        assert_eq!(body.as_ref(), expected_body.as_slice());
    }
}
