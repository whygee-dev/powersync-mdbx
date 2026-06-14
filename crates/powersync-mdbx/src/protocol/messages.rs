use std::{env, iter::Peekable, sync::OnceLock};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::sync_rules::{DEFAULT_TASKS_BUCKET_NAME, DEFAULT_TASKS_STREAM_NAME};

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum StreamingSyncLine {
    Checkpoint(StreamingSyncCheckpoint),
    Data(StreamingSyncData),
    CheckpointComplete(StreamingSyncCheckpointComplete),
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamingSyncCheckpoint {
    pub checkpoint: Checkpoint,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamingSyncData {
    pub data: SyncBucketData,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamingSyncCheckpointComplete {
    pub checkpoint_complete: CheckpointComplete,
}

#[derive(Debug, Clone, Serialize)]
pub struct Checkpoint {
    pub last_op_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_checkpoint: Option<String>,
    pub buckets: Vec<CheckpointBucket>,
    pub streams: Vec<StreamDescription>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckpointComplete {
    pub last_op_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckpointBucket {
    pub bucket: String,
    pub priority: u8,
    pub subscriptions: Vec<BucketSubscriptionReason>,
    pub checksum: i32,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum BucketSubscriptionReason {
    Default { default: u32 },
    Explicit { sub: u32 },
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamDescription {
    pub name: String,
    pub is_default: bool,
    pub errors: Vec<StreamSubscriptionError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamSubscriptionError {
    pub subscription: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncBucketData {
    pub bucket: String,
    pub data: Vec<OplogEntry>,
    pub has_more: bool,
    pub after: String,
    pub next_after: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OplogEntry {
    pub op_id: String,
    pub op: OplogOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    pub checksum: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subkey: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub enum OplogOperation {
    #[serde(rename = "CLEAR")]
    Clear,
    #[serde(rename = "PUT")]
    Put,
    #[serde(rename = "REMOVE")]
    Remove,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SyncChunkKind {
    Checkpoint,
    Data,
    CheckpointComplete,
}

#[derive(Debug, Clone)]
pub struct SyncChunk {
    pub kind: SyncChunkKind,
    pub bytes: Bytes,
}

#[derive(Debug, Clone)]
pub struct BucketSyncView {
    pub bucket_name: String,
    pub stream_name: String,
    pub is_default: bool,
    pub current_entries: Vec<OplogEntry>,
    pub tail_entries: Vec<OplogEntry>,
    pub last_op_id: u64,
    pub snapshot_floor_op_id: u64,
    pub snapshot_clear_checksum: Option<u32>,
    pub force_snapshot_clear: bool,
    pub checkpoint_checksum: Option<i32>,
    pub checkpoint_count: Option<u64>,
    pub after: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TaskBucketSyncView {
    pub bucket_name: String,
    pub stream_name: String,
    pub is_default: bool,
    pub current_rows: Vec<TaskRow>,
    pub tail_entries: Vec<OplogEntry>,
    pub last_op_id: u64,
    pub snapshot_floor_op_id: u64,
    pub after: Option<u64>,
}

pub fn put_checksum(object_type: &str, object_id: &str, data: &str) -> u32 {
    let mut hash = Sha256::new();
    hash.update(b"put.");
    hash.update(object_type.as_bytes());
    hash.update(b".");
    hash.update(object_id.as_bytes());
    hash.update(b".");
    hash.update(data.as_bytes());
    checksum_from_digest(hash.finalize().as_slice())
}

pub fn remove_checksum(source_key: &str) -> u32 {
    let mut hash = Sha256::new();
    hash.update(b"delete.");
    hash.update(source_key.as_bytes());
    checksum_from_digest(hash.finalize().as_slice())
}

pub fn source_subkey_for_object(object_type: &str, object_id: &str) -> String {
    format!("{object_type}/{object_id}")
}

pub fn bucket_checksum_from_entries(entries: &[OplogEntry]) -> i32 {
    protocol_checksum_i32(entries.iter().fold(0_u32, |checksum, entry| {
        checksum.wrapping_add(entry.checksum)
    }))
}

pub fn protocol_checksum_i32(checksum: u32) -> i32 {
    checksum as i32
}

fn checksum_from_digest(digest: &[u8]) -> u32 {
    u32::from_le_bytes(
        digest[..4]
            .try_into()
            .expect("sha256 digest should contain at least 4 bytes"),
    )
}

pub fn sample_sync_stream(after: Option<u64>) -> Vec<StreamingSyncLine> {
    let fixture = sync_fixture();
    let after = after.unwrap_or(0);
    stream_lines_for_after(fixture, after)
}

pub(crate) fn sample_sync_stream_ndjson_chunks(after: Option<u64>) -> Vec<SyncChunk> {
    let fixture = sync_fixture();
    let after = after.unwrap_or(0);
    stream_chunks_for_after(fixture, after, WireFormat::Ndjson)
}

pub(crate) fn sample_sync_stream_bson_chunks(after: Option<u64>) -> Vec<SyncChunk> {
    let fixture = sync_fixture();
    let after = after.unwrap_or(0);
    stream_chunks_for_after(fixture, after, WireFormat::Bson)
}

pub(crate) fn sample_sync_last_op_id() -> u64 {
    sync_fixture().last_op_id
}

pub(crate) fn sample_sync_fixture_signature() -> String {
    let fixture = sync_fixture();
    format!(
        "v1:last_op_id={}:page_size={}",
        fixture.last_op_id, fixture.page_size
    )
}

pub fn task_sync_stream_ndjson_chunks(
    current_rows: Vec<TaskRow>,
    tail_entries: Vec<OplogEntry>,
    snapshot_floor_op_id: u64,
    last_op_id: u64,
    after: Option<u64>,
) -> Vec<SyncChunk> {
    task_bucket_sync_stream_ndjson_chunks(
        vec![TaskBucketSyncView {
            bucket_name: DEFAULT_TASKS_BUCKET_NAME.to_owned(),
            stream_name: DEFAULT_TASKS_STREAM_NAME.to_owned(),
            is_default: true,
            current_rows,
            tail_entries,
            last_op_id,
            snapshot_floor_op_id,
            after,
        }],
        last_op_id,
    )
}

pub fn task_sync_stream_bson_chunks(
    current_rows: Vec<TaskRow>,
    tail_entries: Vec<OplogEntry>,
    snapshot_floor_op_id: u64,
    last_op_id: u64,
    after: Option<u64>,
) -> Vec<SyncChunk> {
    task_bucket_sync_stream_bson_chunks(
        vec![TaskBucketSyncView {
            bucket_name: DEFAULT_TASKS_BUCKET_NAME.to_owned(),
            stream_name: DEFAULT_TASKS_STREAM_NAME.to_owned(),
            is_default: true,
            current_rows,
            tail_entries,
            last_op_id,
            snapshot_floor_op_id,
            after,
        }],
        last_op_id,
    )
}

pub fn task_bucket_sync_stream_ndjson_chunks(
    bucket_views: Vec<TaskBucketSyncView>,
    last_op_id: u64,
) -> Vec<SyncChunk> {
    bucket_sync_stream_chunks(
        bucket_views
            .into_iter()
            .map(task_bucket_view_into_bucket_view)
            .collect(),
        last_op_id,
        WireFormat::Ndjson,
    )
}

pub fn task_bucket_sync_stream_bson_chunks(
    bucket_views: Vec<TaskBucketSyncView>,
    last_op_id: u64,
) -> Vec<SyncChunk> {
    bucket_sync_stream_chunks(
        bucket_views
            .into_iter()
            .map(task_bucket_view_into_bucket_view)
            .collect(),
        last_op_id,
        WireFormat::Bson,
    )
}

pub fn bucket_sync_stream_ndjson_chunks(
    bucket_views: Vec<BucketSyncView>,
    last_op_id: u64,
) -> Vec<SyncChunk> {
    bucket_sync_stream_ndjson_chunk_iter(bucket_views, last_op_id).collect()
}

pub fn bucket_sync_stream_bson_chunks(
    bucket_views: Vec<BucketSyncView>,
    last_op_id: u64,
) -> Vec<SyncChunk> {
    bucket_sync_stream_bson_chunk_iter(bucket_views, last_op_id).collect()
}

pub fn bucket_sync_stream_ndjson_chunk_iter(
    bucket_views: Vec<BucketSyncView>,
    last_op_id: u64,
) -> impl Iterator<Item = SyncChunk> + Send {
    BucketSyncChunkIterator::new(
        bucket_views,
        last_op_id,
        StreamPageSizes::from_env(),
        WireFormat::Ndjson,
    )
}

pub fn bucket_sync_stream_bson_chunk_iter(
    bucket_views: Vec<BucketSyncView>,
    last_op_id: u64,
) -> impl Iterator<Item = SyncChunk> + Send {
    BucketSyncChunkIterator::new(
        bucket_views,
        last_op_id,
        StreamPageSizes::from_env(),
        WireFormat::Bson,
    )
}

pub(crate) fn benchmark_task_rows() -> Vec<TaskRow> {
    benchmark_profile_task_rows(benchmark_profile_from_env())
}

fn stream_lines_for_after(fixture: &SyncFixture, after: u64) -> Vec<StreamingSyncLine> {
    let mut lines = vec![fixture.checkpoint.clone()];

    lines.extend(data_lines_for_after(fixture, after));

    lines.push(fixture.complete.clone());
    lines
}

#[derive(Debug, Clone, Copy)]
enum WireFormat {
    Ndjson,
    Bson,
}

#[derive(Debug, Clone)]
struct SyncFixture {
    page_size: usize,
    last_op_id: u64,
    full_with_clear: Vec<OplogEntry>,
    puts: Vec<OplogEntry>,
    put_pages: Vec<SyncDataPage>,
    checkpoint: StreamingSyncLine,
    complete: StreamingSyncLine,
    cached_ndjson_full: Vec<SyncChunk>,
    cached_ndjson_no_data: Vec<SyncChunk>,
    cached_bson_full: Vec<SyncChunk>,
    cached_bson_no_data: Vec<SyncChunk>,
}

#[derive(Debug, Clone)]
struct SyncDataPage {
    start_after: u64,
    next_after: u64,
    has_more: bool,
    entries: Vec<OplogEntry>,
    ndjson_chunk: SyncChunk,
    bson_chunk: SyncChunk,
}

fn sync_fixture() -> &'static SyncFixture {
    static FIXTURE: OnceLock<SyncFixture> = OnceLock::new();
    FIXTURE.get_or_init(build_sync_fixture)
}

fn build_sync_fixture() -> SyncFixture {
    let task_rows = benchmark_task_rows();
    let page_size = benchmark_page_size_from_env();
    let include_clear = benchmark_include_clear_from_env();

    let mut puts = Vec::with_capacity(task_rows.len());
    for (index, row) in task_rows.into_iter().enumerate() {
        let data = serde_json::to_string(&row)
            .expect("task row should serialize for streaming sync payload");
        puts.push(OplogEntry {
            op_id: (index + 1).to_string(),
            op: OplogOperation::Put,
            object_type: Some("tasks".to_owned()),
            object_id: Some(row.id.clone()),
            checksum: put_checksum("tasks", &row.id, &data),
            data: Some(data),
            subkey: None,
        });
    }

    let mut full_with_clear = Vec::with_capacity(puts.len() + usize::from(include_clear));
    if include_clear {
        full_with_clear.push(OplogEntry {
            op_id: "0".to_owned(),
            op: OplogOperation::Clear,
            object_type: None,
            object_id: None,
            data: None,
            checksum: 0,
            subkey: None,
        });
    }
    full_with_clear.extend(puts.iter().cloned());

    let last_op_id = puts.len() as u64;
    let checkpoint = checkpoint_line(
        &[BucketSyncView {
            bucket_name: DEFAULT_TASKS_BUCKET_NAME.to_owned(),
            stream_name: DEFAULT_TASKS_STREAM_NAME.to_owned(),
            is_default: true,
            current_entries: puts
                .iter()
                .map(|entry| OplogEntry {
                    op_id: "0".to_owned(),
                    op: entry.op.clone(),
                    object_type: entry.object_type.clone(),
                    object_id: entry.object_id.clone(),
                    data: entry.data.clone(),
                    checksum: entry.checksum,
                    subkey: entry.subkey.clone(),
                })
                .collect(),
            tail_entries: Vec::new(),
            last_op_id,
            snapshot_floor_op_id: last_op_id,
            snapshot_clear_checksum: None,
            force_snapshot_clear: false,
            checkpoint_checksum: None,
            checkpoint_count: None,
            after: Some(0),
        }],
        last_op_id,
    );
    let complete = checkpoint_complete_line(last_op_id);
    let full_pages = build_data_pages(full_with_clear.clone(), 0, page_size, last_op_id);
    let put_pages = build_data_pages(puts.clone(), 0, page_size, last_op_id);

    let full_data_lines = full_pages
        .iter()
        .map(|page| {
            data_line_from_entries(
                DEFAULT_TASKS_BUCKET_NAME,
                page.entries.clone(),
                page.start_after,
                page.next_after,
                page.has_more,
            )
        })
        .collect::<Vec<_>>();

    let mut full_sync_lines = vec![checkpoint.clone()];
    full_sync_lines.extend(full_data_lines.clone());
    full_sync_lines.push(complete.clone());

    let cached_ndjson_full = encode_lines(WireFormat::Ndjson, &full_sync_lines);
    let cached_ndjson_no_data =
        encode_lines(WireFormat::Ndjson, &[checkpoint.clone(), complete.clone()]);
    let cached_bson_full = encode_lines(WireFormat::Bson, &full_sync_lines);
    let cached_bson_no_data =
        encode_lines(WireFormat::Bson, &[checkpoint.clone(), complete.clone()]);

    SyncFixture {
        page_size,
        last_op_id,
        full_with_clear,
        puts,
        put_pages,
        checkpoint,
        complete,
        cached_ndjson_full,
        cached_ndjson_no_data,
        cached_bson_full,
        cached_bson_no_data,
    }
}

fn data_lines_for_after(fixture: &SyncFixture, after: u64) -> Vec<StreamingSyncLine> {
    let data_entries = if after == 0 {
        fixture.full_with_clear.clone()
    } else if after >= fixture.last_op_id {
        Vec::new()
    } else {
        let start = usize::try_from(after).unwrap_or(usize::MAX);
        fixture.puts.get(start..).unwrap_or_default().to_vec()
    };

    paginated_data_lines(data_entries, after, fixture.last_op_id, fixture.page_size)
}

fn stream_chunks_for_after(
    fixture: &SyncFixture,
    after: u64,
    format: WireFormat,
) -> Vec<SyncChunk> {
    match format {
        WireFormat::Ndjson => {
            if after == 0 {
                return fixture.cached_ndjson_full.clone();
            }
            if after >= fixture.last_op_id {
                return fixture.cached_ndjson_no_data.clone();
            }
        }
        WireFormat::Bson => {
            if after == 0 {
                return fixture.cached_bson_full.clone();
            }
            if after >= fixture.last_op_id {
                return fixture.cached_bson_no_data.clone();
            }
        }
    }

    let (checkpoint, complete) = match format {
        WireFormat::Ndjson => (
            fixture.cached_ndjson_no_data[0].clone(),
            fixture.cached_ndjson_no_data[1].clone(),
        ),
        WireFormat::Bson => (
            fixture.cached_bson_no_data[0].clone(),
            fixture.cached_bson_no_data[1].clone(),
        ),
    };

    let mut chunks = Vec::with_capacity(fixture.put_pages.len() + 2);
    chunks.push(checkpoint);
    chunks.extend(select_put_page_chunks(fixture, after, format));
    chunks.push(complete);
    chunks
}

fn select_put_page_chunks(fixture: &SyncFixture, after: u64, format: WireFormat) -> Vec<SyncChunk> {
    let mut selected = Vec::new();

    for page in &fixture.put_pages {
        if page.next_after <= after {
            continue;
        }

        if after > page.start_after && after < page.next_after {
            let trimmed_entries = page
                .entries
                .iter()
                .filter(|entry| oplog_entry_id(entry) > after)
                .cloned()
                .collect::<Vec<_>>();

            if trimmed_entries.is_empty() {
                continue;
            }

            let next_after = trimmed_entries
                .last()
                .map(oplog_entry_id)
                .expect("trimmed entries are non-empty");

            selected.push(encode_data_chunk(
                trimmed_entries,
                after,
                next_after,
                next_after < fixture.last_op_id,
                format,
            ));
            continue;
        }

        selected.push(page.chunk_for(format));
    }

    selected
}

fn build_data_pages(
    data_entries: Vec<OplogEntry>,
    start_after: u64,
    page_size: usize,
    last_op_id: u64,
) -> Vec<SyncDataPage> {
    if data_entries.is_empty() {
        return Vec::new();
    }

    let page_size = page_size.max(1);
    let total_pages = data_entries.len().div_ceil(page_size);
    let mut pages = Vec::with_capacity(total_pages);
    let mut current_after = start_after;

    for (index, chunk) in data_entries.chunks(page_size).enumerate() {
        let entries = chunk.to_vec();
        let next_after = entries
            .last()
            .map(oplog_entry_id)
            .expect("page chunk should contain at least one oplog entry");
        let has_more = index + 1 < total_pages && next_after < last_op_id;

        let line = data_line_from_entries(
            DEFAULT_TASKS_BUCKET_NAME,
            entries.clone(),
            current_after,
            next_after,
            has_more,
        );
        let ndjson_chunk = encode_line_chunk(WireFormat::Ndjson, &line);
        let bson_chunk = encode_line_chunk(WireFormat::Bson, &line);

        pages.push(SyncDataPage {
            start_after: current_after,
            next_after,
            has_more,
            entries,
            ndjson_chunk,
            bson_chunk,
        });

        current_after = next_after;
    }

    pages
}

fn encode_data_chunk(
    entries: Vec<OplogEntry>,
    after: u64,
    next_after: u64,
    has_more: bool,
    format: WireFormat,
) -> SyncChunk {
    let line = data_line_from_entries(
        DEFAULT_TASKS_BUCKET_NAME,
        entries,
        after,
        next_after,
        has_more,
    );
    encode_line_chunk(format, &line)
}

fn encode_line_chunk(format: WireFormat, line: &StreamingSyncLine) -> SyncChunk {
    let kind = chunk_kind(line);
    let bytes = match format {
        WireFormat::Ndjson => {
            let mut json = serde_json::to_vec(line).expect("stream line should serialize");
            json.push(b'\n');
            Bytes::from(json)
        }
        WireFormat::Bson => {
            Bytes::from(bson::to_vec(line).expect("stream line should serialize to bson bytes"))
        }
    };

    SyncChunk { kind, bytes }
}

impl SyncDataPage {
    fn chunk_for(&self, format: WireFormat) -> SyncChunk {
        match format {
            WireFormat::Ndjson => self.ndjson_chunk.clone(),
            WireFormat::Bson => self.bson_chunk.clone(),
        }
    }
}

fn oplog_entry_id(entry: &OplogEntry) -> u64 {
    entry
        .op_id
        .parse::<u64>()
        .expect("oplog entry op_id should parse as u64")
}

fn paginated_data_lines(
    data_entries: Vec<OplogEntry>,
    after: u64,
    last_op_id: u64,
    page_size: usize,
) -> Vec<StreamingSyncLine> {
    if data_entries.is_empty() {
        return Vec::new();
    }

    let page_size = page_size.max(1);
    let mut lines = Vec::new();
    let mut current_after = after;

    for chunk in data_entries.chunks(page_size) {
        let next_after = chunk
            .last()
            .and_then(|entry| entry.op_id.parse::<u64>().ok())
            .unwrap_or(last_op_id);
        let is_last_page = next_after >= last_op_id;

        lines.push(data_line_from_entries(
            DEFAULT_TASKS_BUCKET_NAME,
            chunk.to_vec(),
            current_after,
            next_after,
            !is_last_page,
        ));

        current_after = next_after;
    }

    lines
}

#[cfg(test)]
fn task_sync_stream_chunks_with_page_sizes(
    current_rows: Vec<TaskRow>,
    tail_entries: Vec<OplogEntry>,
    snapshot_floor_op_id: u64,
    last_op_id: u64,
    after: Option<u64>,
    page_sizes: StreamPageSizes,
    format: WireFormat,
) -> Vec<SyncChunk> {
    bucket_sync_stream_chunks_with_page_sizes(
        vec![task_bucket_view_into_bucket_view(TaskBucketSyncView {
            bucket_name: DEFAULT_TASKS_BUCKET_NAME.to_owned(),
            stream_name: DEFAULT_TASKS_STREAM_NAME.to_owned(),
            is_default: true,
            current_rows,
            tail_entries,
            last_op_id,
            snapshot_floor_op_id,
            after,
        })],
        last_op_id,
        page_sizes,
        format,
    )
}

fn data_line_from_entries(
    bucket_name: &str,
    entries: Vec<OplogEntry>,
    after: u64,
    next_after: u64,
    has_more: bool,
) -> StreamingSyncLine {
    StreamingSyncLine::Data(StreamingSyncData {
        data: SyncBucketData {
            bucket: bucket_name.to_owned(),
            data: entries,
            has_more,
            after: after.to_string(),
            next_after: next_after.to_string(),
        },
    })
}

fn checkpoint_line(bucket_views: &[BucketSyncView], last_op_id: u64) -> StreamingSyncLine {
    StreamingSyncLine::Checkpoint(StreamingSyncCheckpoint {
        checkpoint: Checkpoint {
            last_op_id: last_op_id.to_string(),
            write_checkpoint: None,
            buckets: bucket_views
                .iter()
                // Priority-based partial sync and explicit per-subscription
                // correlation are out of scope (see docs/scope.md): every bucket
                // is reported at a fixed priority as a default subscription. The
                // benchmarked streams use default/auto subscriptions, for which
                // this is the correct shape.
                .map(|view| CheckpointBucket {
                    bucket: view.bucket_name.clone(),
                    priority: 3,
                    subscriptions: vec![BucketSubscriptionReason::Default { default: 0 }],
                    checksum: view
                        .checkpoint_checksum
                        .unwrap_or_else(|| bucket_checksum_from_entries(&view.current_entries)),
                    count: view
                        .checkpoint_count
                        .unwrap_or(view.current_entries.len() as u64),
                })
                .collect(),
            streams: dedup_stream_descriptions(bucket_views),
        },
    })
}

fn task_bucket_view_into_bucket_view(view: TaskBucketSyncView) -> BucketSyncView {
    BucketSyncView {
        bucket_name: view.bucket_name,
        stream_name: view.stream_name,
        is_default: view.is_default,
        current_entries: view
            .current_rows
            .into_iter()
            .map(task_row_to_current_entry)
            .collect(),
        tail_entries: view.tail_entries,
        last_op_id: view.last_op_id,
        snapshot_floor_op_id: view.snapshot_floor_op_id,
        snapshot_clear_checksum: None,
        force_snapshot_clear: false,
        checkpoint_checksum: None,
        checkpoint_count: None,
        after: view.after,
    }
}

fn checkpoint_complete_line(last_op_id: u64) -> StreamingSyncLine {
    StreamingSyncLine::CheckpointComplete(StreamingSyncCheckpointComplete {
        checkpoint_complete: CheckpointComplete {
            last_op_id: last_op_id.to_string(),
        },
    })
}

fn encode_lines(format: WireFormat, lines: &[StreamingSyncLine]) -> Vec<SyncChunk> {
    lines.iter().map(|line| encode_line(format, line)).collect()
}

fn encode_line(format: WireFormat, line: &StreamingSyncLine) -> SyncChunk {
    let kind = chunk_kind(line);
    let bytes = match format {
        WireFormat::Ndjson => {
            let mut json = serde_json::to_vec(line).expect("stream line should serialize");
            json.push(b'\n');
            Bytes::from(json)
        }
        WireFormat::Bson => {
            Bytes::from(bson::to_vec(line).expect("stream line should serialize to bson bytes"))
        }
    };

    SyncChunk { kind, bytes }
}

fn chunk_kind(line: &StreamingSyncLine) -> SyncChunkKind {
    match line {
        StreamingSyncLine::Checkpoint(_) => SyncChunkKind::Checkpoint,
        StreamingSyncLine::Data(_) => SyncChunkKind::Data,
        StreamingSyncLine::CheckpointComplete(_) => SyncChunkKind::CheckpointComplete,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRow {
    pub id: String,
    pub org_id: String,
    pub project_id: String,
    pub title: String,
    pub status: String,
    pub priority: u32,
    pub assignee_id: String,
    pub story_points: u32,
    pub updated_at: String,
    pub summary: String,
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkProfile {
    org_count: u32,
    users_per_org: u32,
    projects_per_org: u32,
    tasks_per_project: u32,
    batch_delete_count: u32,
}

fn benchmark_profile_task_rows(profile: BenchmarkProfile) -> Vec<TaskRow> {
    let mut rows = Vec::new();

    for org_number in 1..=profile.org_count {
        for project_index in 1..=profile.projects_per_org {
            for task_index in 1..=profile.tasks_per_project {
                let assignee_index = ((task_index - 1) % profile.users_per_org) + 1;
                rows.push(TaskRow {
                    id: task_id(org_number, project_index, task_index),
                    org_id: org_id(org_number),
                    project_id: project_id(org_number, project_index),
                    title: format!("Task {}.{}", project_index, task_index),
                    status: "todo".to_owned(),
                    priority: ((task_index - 1) % 5) + 1,
                    assignee_id: user_id(org_number, assignee_index),
                    story_points: ((task_index - 1) % 8) + 1,
                    updated_at: "2026-01-01T00:00:00Z".to_owned(),
                    summary: format!("base:{org_number}:{project_index}:{task_index}"),
                });
            }
        }
    }

    let target_org_id = org_id(1);
    rows.push(make_sentinel_task_row(
        SentinelKind::Update,
        1,
        &target_org_id,
        profile,
    ));
    rows.push(make_sentinel_task_row(
        SentinelKind::Delete,
        1,
        &target_org_id,
        profile,
    ));

    for index in 1..=profile.batch_delete_count {
        rows.push(make_sentinel_task_row(
            SentinelKind::BatchDelete,
            index,
            &target_org_id,
            profile,
        ));
    }

    rows
}

#[derive(Debug, Clone, Copy)]
struct StreamPageSizes {
    snapshot: usize,
    delta: usize,
}

impl StreamPageSizes {
    fn from_env() -> Self {
        let snapshot = benchmark_page_size_from_env();
        let delta = benchmark_delta_page_size_from_env(snapshot);
        Self { snapshot, delta }
    }
}

fn bucket_sync_stream_chunks(
    bucket_views: Vec<BucketSyncView>,
    last_op_id: u64,
    format: WireFormat,
) -> Vec<SyncChunk> {
    bucket_sync_stream_chunks_with_page_sizes(
        bucket_views,
        last_op_id,
        StreamPageSizes::from_env(),
        format,
    )
}

fn bucket_sync_stream_chunks_with_page_sizes(
    bucket_views: Vec<BucketSyncView>,
    last_op_id: u64,
    page_sizes: StreamPageSizes,
    format: WireFormat,
) -> Vec<SyncChunk> {
    BucketSyncChunkIterator::new(bucket_views, last_op_id, page_sizes, format).collect()
}

struct BucketSyncChunkIterator {
    format: WireFormat,
    last_op_id: u64,
    checkpoint: Option<StreamingSyncLine>,
    views: std::vec::IntoIter<BucketPageIterator>,
    page: Option<BucketPageIterator>,
    complete_pending: bool,
}

struct BucketPageIterator {
    bucket_name: String,
    entries: Peekable<Box<dyn Iterator<Item = OplogEntry> + Send>>,
    current_after: u64,
    page_size: usize,
}

impl BucketSyncChunkIterator {
    fn new(
        bucket_views: Vec<BucketSyncView>,
        last_op_id: u64,
        page_sizes: StreamPageSizes,
        format: WireFormat,
    ) -> Self {
        let checkpoint = checkpoint_line(&bucket_views, last_op_id);
        let views = bucket_views
            .into_iter()
            .map(move |mut view| {
                let view_last_op_id = view.last_op_id.min(last_op_id);
                let bounded_after = view.after.unwrap_or(0).min(view_last_op_id);
                let snapshot_floor_op_id = view.snapshot_floor_op_id.min(view_last_op_id);
                let full_snapshot = bounded_after < snapshot_floor_op_id;
                let page_size = if full_snapshot {
                    page_sizes.snapshot
                } else {
                    page_sizes.delta
                }
                .max(1);

                let entries: Box<dyn Iterator<Item = OplogEntry> + Send> = if full_snapshot {
                    let include_clear = benchmark_include_clear_from_env()
                        || view.force_snapshot_clear
                        || view
                            .snapshot_clear_checksum
                            .is_some_and(|checksum| checksum != 0);
                    let entry_count = view.current_entries.len() as u64 + u64::from(include_clear);
                    debug_assert!(
                        view_last_op_id.saturating_add(1) >= entry_count,
                        "synthetic full snapshot requires enough op_id range for current entries"
                    );
                    let snapshot_start_op_id =
                        view_last_op_id.saturating_sub(entry_count.saturating_sub(1));
                    let row_start_op_id = snapshot_start_op_id + u64::from(include_clear);
                    let clear = include_clear
                        .then(|| {
                            snapshot_clear_entry(
                                snapshot_start_op_id,
                                view.snapshot_clear_checksum.unwrap_or(0),
                            )
                        })
                        .into_iter();
                    let rows = std::mem::take(&mut view.current_entries)
                        .into_iter()
                        .enumerate()
                        .map(move |(index, entry)| {
                            snapshot_current_entry(entry, row_start_op_id + index as u64)
                        });
                    Box::new(clear.chain(rows))
                } else if bounded_after >= view_last_op_id {
                    Box::new(std::iter::empty())
                } else {
                    Box::new(
                        std::mem::take(&mut view.tail_entries)
                            .into_iter()
                            .filter(move |entry| oplog_entry_id(entry) > bounded_after),
                    )
                };

                BucketPageIterator {
                    bucket_name: view.bucket_name,
                    entries: entries.peekable(),
                    current_after: if full_snapshot { 0 } else { bounded_after },
                    page_size,
                }
            })
            .collect::<Vec<_>>()
            .into_iter();

        Self {
            format,
            last_op_id,
            checkpoint: Some(checkpoint),
            views,
            page: None,
            complete_pending: true,
        }
    }
}

impl Iterator for BucketSyncChunkIterator {
    type Item = SyncChunk;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(checkpoint) = self.checkpoint.take() {
            return Some(encode_line(self.format, &checkpoint));
        }

        loop {
            if let Some(page) = &mut self.page {
                let entries = page
                    .entries
                    .by_ref()
                    .take(page.page_size)
                    .collect::<Vec<_>>();
                if !entries.is_empty() {
                    let next_after = entries
                        .last()
                        .map(oplog_entry_id)
                        .unwrap_or(page.current_after);
                    let has_more = page.entries.peek().is_some();
                    let line = data_line_from_entries(
                        &page.bucket_name,
                        entries,
                        page.current_after,
                        next_after,
                        has_more,
                    );
                    page.current_after = next_after;
                    return Some(encode_line(self.format, &line));
                }
                self.page = None;
            }

            if let Some(page) = self.views.next() {
                self.page = Some(page);
                continue;
            }

            if self.complete_pending {
                self.complete_pending = false;
                return Some(encode_line(
                    self.format,
                    &checkpoint_complete_line(self.last_op_id),
                ));
            }
            return None;
        }
    }
}

fn dedup_stream_descriptions(bucket_views: &[BucketSyncView]) -> Vec<StreamDescription> {
    let mut streams = Vec::new();

    for view in bucket_views {
        if streams
            .iter()
            .any(|stream: &StreamDescription| stream.name == view.stream_name)
        {
            continue;
        }

        streams.push(StreamDescription {
            name: view.stream_name.clone(),
            is_default: view.is_default,
            errors: Vec::new(),
        });
    }

    streams
}

#[cfg(test)]
fn snapshot_task_entries(current_rows: Vec<TaskRow>, last_op_id: u64) -> Vec<OplogEntry> {
    snapshot_task_entries_with_optional_clear(current_rows, last_op_id, true)
}

#[cfg(test)]
fn snapshot_current_entries_with_optional_clear(
    current_entries: Vec<OplogEntry>,
    last_op_id: u64,
    include_clear: bool,
    clear_checksum: u32,
) -> Vec<OplogEntry> {
    let entry_count = current_entries.len() as u64 + u64::from(include_clear);
    debug_assert!(
        last_op_id.saturating_add(1) >= entry_count,
        "synthetic full snapshot requires enough op_id range for current entries"
    );
    let snapshot_start_op_id = last_op_id.saturating_sub(entry_count.saturating_sub(1));
    let mut entries = Vec::with_capacity(current_entries.len() + usize::from(include_clear));
    let row_start_op_id = if include_clear {
        entries.push(snapshot_clear_entry(snapshot_start_op_id, clear_checksum));
        snapshot_start_op_id + 1
    } else {
        snapshot_start_op_id
    };
    entries.extend(
        current_entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| snapshot_current_entry(entry, row_start_op_id + index as u64)),
    );
    entries
}

#[cfg(test)]
fn snapshot_task_entries_with_optional_clear(
    current_rows: Vec<TaskRow>,
    last_op_id: u64,
    include_clear: bool,
) -> Vec<OplogEntry> {
    snapshot_current_entries_with_optional_clear(
        current_rows
            .into_iter()
            .map(task_row_to_current_entry)
            .collect(),
        last_op_id,
        include_clear,
        0,
    )
}

fn snapshot_clear_entry(op_id: u64, checksum: u32) -> OplogEntry {
    OplogEntry {
        op_id: op_id.to_string(),
        op: OplogOperation::Clear,
        object_type: None,
        object_id: None,
        data: None,
        checksum,
        subkey: None,
    }
}

fn snapshot_current_entry(mut entry: OplogEntry, op_id: u64) -> OplogEntry {
    entry.op_id = op_id.to_string();
    entry
}

fn task_row_to_current_entry(row: TaskRow) -> OplogEntry {
    let data =
        serde_json::to_string(&row).expect("task row should serialize for runtime sync payload");
    OplogEntry {
        op_id: "0".to_owned(),
        op: OplogOperation::Put,
        object_type: Some("tasks".to_owned()),
        object_id: Some(row.id.clone()),
        checksum: put_checksum("tasks", &row.id, &data),
        data: Some(data),
        subkey: None,
    }
}

fn benchmark_profile_from_env() -> BenchmarkProfile {
    match env::var("POWERSYNC_BENCHMARK_PROFILE")
        .unwrap_or_else(|_| "smoke".to_owned())
        .as_str()
    {
        "medium" => BenchmarkProfile {
            org_count: 2,
            users_per_org: 36,
            projects_per_org: 48,
            tasks_per_project: 18,
            batch_delete_count: 12,
        },
        "large" => BenchmarkProfile {
            org_count: 3,
            users_per_org: 72,
            projects_per_org: 96,
            tasks_per_project: 28,
            batch_delete_count: 32,
        },
        _ => BenchmarkProfile {
            org_count: 1,
            users_per_org: 8,
            projects_per_org: 6,
            tasks_per_project: 6,
            batch_delete_count: 4,
        },
    }
}

fn benchmark_page_size_from_env() -> usize {
    env::var("POWERSYNC_RUST_STREAM_PAGE_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(256)
}

fn benchmark_delta_page_size_from_env(snapshot_page_size: usize) -> usize {
    env::var("POWERSYNC_RUST_DELTA_STREAM_PAGE_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .or_else(|| {
            env::var("POWERSYNC_RUST_STREAM_PAGE_SIZE")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
        })
        .unwrap_or(snapshot_page_size.max(64))
}

fn benchmark_include_clear_from_env() -> bool {
    env::var("POWERSYNC_RUST_STREAM_INCLUDE_CLEAR")
        .ok()
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[derive(Debug, Clone, Copy)]
enum SentinelKind {
    Update,
    Delete,
    BatchDelete,
}

fn make_sentinel_task_row(
    kind: SentinelKind,
    index: u32,
    target_org_id: &str,
    profile: BenchmarkProfile,
) -> TaskRow {
    let project_index = ((index - 1) % profile.projects_per_org) + 1;
    let org_number = target_org_id
        .split('-')
        .nth(1)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let assignee_index = ((index - 1) % profile.users_per_org) + 1;
    let base_title = match kind {
        SentinelKind::Update => "Update sentinel".to_owned(),
        SentinelKind::Delete => "Delete sentinel".to_owned(),
        SentinelKind::BatchDelete => format!("Batch delete {index}"),
    };

    TaskRow {
        id: sentinel_task_id(kind, index),
        org_id: target_org_id.to_owned(),
        project_id: project_id(org_number, project_index),
        title: format!("{base_title} benchmark row"),
        status: match kind {
            SentinelKind::Update => "todo".to_owned(),
            SentinelKind::Delete | SentinelKind::BatchDelete => "backlog".to_owned(),
        },
        priority: (index % 5) + 1,
        assignee_id: user_id(org_number, assignee_index),
        story_points: (index % 8) + 1,
        updated_at: "2026-01-01T00:00:00Z".to_owned(),
        summary: format!("sentinel:{}:{index}", sentinel_kind_label(kind)),
    }
}

fn sentinel_kind_label(kind: SentinelKind) -> &'static str {
    match kind {
        SentinelKind::Update => "update",
        SentinelKind::Delete => "delete",
        SentinelKind::BatchDelete => "batch-delete",
    }
}

fn sentinel_task_id(kind: SentinelKind, index: u32) -> String {
    format!(
        "task-sentinel-{}-{}",
        sentinel_kind_label(kind),
        pad(index, 4)
    )
}

fn org_id(index: u32) -> String {
    format!("org-{}", pad(index, 3))
}

fn user_id(org_number: u32, index: u32) -> String {
    format!("user-{}-{}", org_id(org_number), pad(index, 4))
}

fn project_id(org_number: u32, index: u32) -> String {
    format!("project-{}-{}", org_id(org_number), pad(index, 4))
}

fn task_id(org_number: u32, project_index: u32, task_index: u32) -> String {
    format!(
        "task-{}-{}-{}",
        org_id(org_number),
        pad(project_index, 4),
        pad(task_index, 4)
    )
}

fn pad(value: u32, size: usize) -> String {
    format!("{value:0size$}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_rules::project_tasks_bucket_name;
    use bson::Document;
    use serde_json::Value;

    #[test]
    fn powersync_hash_helpers_match_service_core_vectors() {
        assert_eq!(
            put_checksum("tasks", "task-1", r#"{"id":"task-1"}"#),
            2230509878
        );
        assert_eq!(
            put_checksum(
                "tickets",
                "ticket-1",
                r#"{"ticket_id":"ticket-1","queue_id":"queue-a","title":"Fix bug"}"#
            ),
            1618285734
        );
        assert_eq!(remove_checksum("tasks/task-1"), 1514895139);
    }

    #[test]
    fn bucket_checksum_uses_wrapping_signed_protocol_value() {
        let entries = vec![
            OplogEntry {
                op_id: "1".to_owned(),
                op: OplogOperation::Put,
                object_type: Some("items".to_owned()),
                object_id: Some("a".to_owned()),
                data: Some("{}".to_owned()),
                checksum: 3_000_000_000,
                subkey: None,
            },
            OplogEntry {
                op_id: "2".to_owned(),
                op: OplogOperation::Put,
                object_type: Some("items".to_owned()),
                object_id: Some("b".to_owned()),
                data: Some("{}".to_owned()),
                checksum: 3_000_000_000,
                subkey: None,
            },
        ];

        assert_eq!(bucket_checksum_from_entries(&entries), 1_705_032_704_i32);
        assert_eq!(protocol_checksum_i32(2_230_509_878), -2_064_457_418);
    }

    #[test]
    fn full_sync_emits_checkpoint_data_and_complete() {
        let lines = sample_sync_stream(None);
        assert!(lines.len() >= 3);
        assert!(matches!(lines[0], StreamingSyncLine::Checkpoint(_)));
        assert!(lines[1..lines.len() - 1]
            .iter()
            .all(|line| matches!(line, StreamingSyncLine::Data(_))));
        assert!(matches!(
            lines.last(),
            Some(StreamingSyncLine::CheckpointComplete(_))
        ));
    }

    #[test]
    fn incremental_sync_after_cursor_omits_clear() {
        let lines = sample_sync_stream(Some(1));
        let StreamingSyncLine::Data(data_line) = &lines[1] else {
            panic!("expected data line");
        };

        assert!(!data_line.data.data.is_empty());
        assert!(matches!(data_line.data.data[0].op, OplogOperation::Put));
        assert_eq!(data_line.data.data[0].op_id, "2");
        assert_eq!(data_line.data.after, "1");
    }

    #[test]
    fn full_sync_data_pages_progress_cursor() {
        let entries = snapshot_task_entries(benchmark_task_rows(), sample_sync_last_op_id());
        let lines = paginated_data_lines(entries, 0, sample_sync_last_op_id(), 16);
        let data_lines = lines
            .iter()
            .filter_map(|line| match line {
                StreamingSyncLine::Data(line) => Some(line),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(data_lines.len() > 1, "expected paged full-sync data lines");
        assert_eq!(data_lines[0].data.after, "0");

        for (current, next) in data_lines.iter().zip(data_lines.iter().skip(1)) {
            assert_eq!(current.data.next_after, next.data.after);
            assert!(current.data.has_more);
        }

        assert!(
            !data_lines
                .last()
                .expect("last page should exist")
                .data
                .has_more
        );
    }

    #[test]
    fn up_to_date_cursor_skips_data_line() {
        let initial = sample_sync_stream(None);
        let StreamingSyncLine::Checkpoint(checkpoint) = &initial[0] else {
            panic!("expected checkpoint");
        };
        let last_op_id = checkpoint
            .checkpoint
            .last_op_id
            .parse::<u64>()
            .expect("last_op_id should parse");

        let lines = sample_sync_stream(Some(last_op_id));
        assert_eq!(lines.len(), 2);
        assert!(matches!(lines[0], StreamingSyncLine::Checkpoint(_)));
        assert!(matches!(lines[1], StreamingSyncLine::CheckpointComplete(_)));
    }

    #[test]
    fn ndjson_chunks_skip_data_for_up_to_date_cursor() {
        let initial = sample_sync_stream(None);
        let StreamingSyncLine::Checkpoint(checkpoint) = &initial[0] else {
            panic!("expected checkpoint");
        };
        let last_op_id = checkpoint
            .checkpoint
            .last_op_id
            .parse::<u64>()
            .expect("last_op_id should parse");

        let chunks = sample_sync_stream_ndjson_chunks(Some(last_op_id));
        assert_eq!(chunks.len(), 2);

        let docs = chunks
            .iter()
            .map(|chunk| {
                assert!(
                    chunk.bytes.as_ref().ends_with(b"\n"),
                    "ndjson chunk should end with newline"
                );
                serde_json::from_slice::<Value>(&chunk.bytes[..chunk.bytes.len() - 1])
                    .expect("ndjson chunk should parse")
            })
            .collect::<Vec<_>>();

        assert!(docs[0].get("checkpoint").is_some());
        assert!(docs[1].get("checkpoint_complete").is_some());
        assert_eq!(chunks[0].kind, SyncChunkKind::Checkpoint);
        assert_eq!(chunks[1].kind, SyncChunkKind::CheckpointComplete);
    }

    #[test]
    fn bson_chunks_encode_checkpoint_data_complete() {
        let chunks = sample_sync_stream_bson_chunks(None);
        assert!(chunks.len() >= 3);

        let docs = chunks
            .iter()
            .map(|chunk| bson::from_slice::<Document>(&chunk.bytes).expect("valid bson chunk"))
            .collect::<Vec<_>>();

        assert!(docs[0].contains_key("checkpoint"));
        assert!(docs[1..docs.len() - 1]
            .iter()
            .all(|doc| doc.contains_key("data")));
        assert!(docs[docs.len() - 1].contains_key("checkpoint_complete"));
    }

    #[test]
    fn task_snapshot_entries_assign_monotonic_op_ids_ending_at_last_op_id() {
        let rows = vec![
            TaskRow {
                id: "task-a".to_owned(),
                org_id: "org-001".to_owned(),
                project_id: "project-001".to_owned(),
                title: "Task A".to_owned(),
                status: "todo".to_owned(),
                priority: 1,
                assignee_id: "user-001".to_owned(),
                story_points: 2,
                updated_at: "2026-04-11T00:00:00Z".to_owned(),
                summary: "runtime:a".to_owned(),
            },
            TaskRow {
                id: "task-b".to_owned(),
                org_id: "org-001".to_owned(),
                project_id: "project-001".to_owned(),
                title: "Task B".to_owned(),
                status: "todo".to_owned(),
                priority: 2,
                assignee_id: "user-002".to_owned(),
                story_points: 3,
                updated_at: "2026-04-11T00:00:01Z".to_owned(),
                summary: "runtime:b".to_owned(),
            },
        ];

        let entries = snapshot_task_entries(rows, 42);
        let op_ids = entries.iter().map(oplog_entry_id).collect::<Vec<_>>();

        assert_eq!(op_ids, vec![40, 41, 42]);
        assert!(matches!(entries[0].op, OplogOperation::Clear));
        assert!(matches!(entries[1].op, OplogOperation::Put));
        assert!(matches!(entries[2].op, OplogOperation::Put));
    }

    #[test]
    fn task_snapshot_entries_can_omit_clear_and_still_end_at_last_op_id() {
        let rows = vec![
            TaskRow {
                id: "task-a".to_owned(),
                org_id: "org-001".to_owned(),
                project_id: "project-001".to_owned(),
                title: "Task A".to_owned(),
                status: "todo".to_owned(),
                priority: 1,
                assignee_id: "user-001".to_owned(),
                story_points: 2,
                updated_at: "2026-04-11T00:00:00Z".to_owned(),
                summary: "runtime:a".to_owned(),
            },
            TaskRow {
                id: "task-b".to_owned(),
                org_id: "org-001".to_owned(),
                project_id: "project-001".to_owned(),
                title: "Task B".to_owned(),
                status: "todo".to_owned(),
                priority: 2,
                assignee_id: "user-002".to_owned(),
                story_points: 3,
                updated_at: "2026-04-11T00:00:01Z".to_owned(),
                summary: "runtime:b".to_owned(),
            },
        ];

        let entries = snapshot_task_entries_with_optional_clear(rows, 42, false);
        let op_ids = entries.iter().map(oplog_entry_id).collect::<Vec<_>>();

        assert_eq!(op_ids, vec![41, 42]);
        assert!(entries
            .iter()
            .all(|entry| matches!(entry.op, OplogOperation::Put)));
    }

    #[test]
    fn task_snapshot_pagination_advances_cursor_for_full_snapshot_pages() {
        let entries = snapshot_task_entries(
            vec![
                TaskRow {
                    id: "task-a".to_owned(),
                    org_id: "org-001".to_owned(),
                    project_id: "project-001".to_owned(),
                    title: "Task A".to_owned(),
                    status: "todo".to_owned(),
                    priority: 1,
                    assignee_id: "user-001".to_owned(),
                    story_points: 2,
                    updated_at: "2026-04-11T00:00:00Z".to_owned(),
                    summary: "runtime:a".to_owned(),
                },
                TaskRow {
                    id: "task-b".to_owned(),
                    org_id: "org-001".to_owned(),
                    project_id: "project-001".to_owned(),
                    title: "Task B".to_owned(),
                    status: "todo".to_owned(),
                    priority: 2,
                    assignee_id: "user-002".to_owned(),
                    story_points: 3,
                    updated_at: "2026-04-11T00:00:01Z".to_owned(),
                    summary: "runtime:b".to_owned(),
                },
                TaskRow {
                    id: "task-c".to_owned(),
                    org_id: "org-001".to_owned(),
                    project_id: "project-001".to_owned(),
                    title: "Task C".to_owned(),
                    status: "todo".to_owned(),
                    priority: 3,
                    assignee_id: "user-003".to_owned(),
                    story_points: 5,
                    updated_at: "2026-04-11T00:00:02Z".to_owned(),
                    summary: "runtime:c".to_owned(),
                },
            ],
            44,
        );
        let lines = paginated_data_lines(entries, 0, 44, 2);
        let data_lines = lines
            .iter()
            .map(|line| match line {
                StreamingSyncLine::Data(line) => line,
                _ => panic!("expected only data lines"),
            })
            .collect::<Vec<_>>();

        assert_eq!(data_lines.len(), 2);
        assert_eq!(data_lines[0].data.after, "0");
        assert_eq!(data_lines[0].data.next_after, "42");
        assert!(data_lines[0].data.has_more);
        assert_eq!(data_lines[1].data.after, "42");
        assert_eq!(data_lines[1].data.next_after, "44");
        assert!(!data_lines[1].data.has_more);
    }

    #[test]
    fn sparse_bucket_delta_does_not_claim_an_unavailable_next_page() {
        let chunks = bucket_sync_stream_chunks_with_page_sizes(
            vec![BucketSyncView {
                bucket_name: "project[project-a]".to_owned(),
                stream_name: "tasks_by_project".to_owned(),
                is_default: false,
                current_entries: Vec::new(),
                tail_entries: vec![OplogEntry {
                    op_id: "2".to_owned(),
                    op: OplogOperation::Put,
                    object_type: Some("tasks".to_owned()),
                    object_id: Some("task-a".to_owned()),
                    data: Some(r#"{"id":"task-a"}"#.to_owned()),
                    checksum: 7,
                    subkey: None,
                }],
                last_op_id: 100,
                snapshot_floor_op_id: 0,
                snapshot_clear_checksum: None,
                force_snapshot_clear: false,
                checkpoint_checksum: Some(7),
                checkpoint_count: Some(1),
                after: Some(0),
            }],
            100,
            StreamPageSizes {
                snapshot: 1,
                delta: 1,
            },
            WireFormat::Ndjson,
        );
        let data = chunks
            .iter()
            .find(|chunk| chunk.kind == SyncChunkKind::Data)
            .expect("data chunk");
        let document = serde_json::from_slice::<Value>(&data.bytes[..data.bytes.len() - 1])
            .expect("NDJSON data chunk");

        assert_eq!(document["data"]["next_after"], "2");
        assert_eq!(document["data"]["has_more"], false);
    }

    #[test]
    fn bucketed_task_chunks_emit_checkpoint_and_data_for_multiple_buckets() {
        let project_bucket = project_tasks_bucket_name("project-002");
        let chunks = bucket_sync_stream_chunks_with_page_sizes(
            vec![
                task_bucket_view_into_bucket_view(TaskBucketSyncView {
                    bucket_name: DEFAULT_TASKS_BUCKET_NAME.to_owned(),
                    stream_name: DEFAULT_TASKS_STREAM_NAME.to_owned(),
                    is_default: true,
                    current_rows: vec![
                        TaskRow {
                            id: "task-a".to_owned(),
                            org_id: "org-001".to_owned(),
                            project_id: "project-001".to_owned(),
                            title: "Task A".to_owned(),
                            status: "todo".to_owned(),
                            priority: 1,
                            assignee_id: "user-001".to_owned(),
                            story_points: 2,
                            updated_at: "2026-04-11T00:00:00Z".to_owned(),
                            summary: "runtime:a".to_owned(),
                        },
                        TaskRow {
                            id: "task-b".to_owned(),
                            org_id: "org-001".to_owned(),
                            project_id: "project-002".to_owned(),
                            title: "Task B".to_owned(),
                            status: "todo".to_owned(),
                            priority: 2,
                            assignee_id: "user-002".to_owned(),
                            story_points: 3,
                            updated_at: "2026-04-11T00:00:01Z".to_owned(),
                            summary: "runtime:b".to_owned(),
                        },
                    ],
                    tail_entries: Vec::new(),
                    last_op_id: 44,
                    snapshot_floor_op_id: 44,
                    after: Some(0),
                }),
                task_bucket_view_into_bucket_view(TaskBucketSyncView {
                    bucket_name: project_bucket.clone(),
                    stream_name: "tasks_by_project".to_owned(),
                    is_default: false,
                    current_rows: vec![TaskRow {
                        id: "task-b".to_owned(),
                        org_id: "org-001".to_owned(),
                        project_id: "project-002".to_owned(),
                        title: "Task B".to_owned(),
                        status: "todo".to_owned(),
                        priority: 2,
                        assignee_id: "user-002".to_owned(),
                        story_points: 3,
                        updated_at: "2026-04-11T00:00:01Z".to_owned(),
                        summary: "runtime:b".to_owned(),
                    }],
                    tail_entries: Vec::new(),
                    last_op_id: 44,
                    snapshot_floor_op_id: 44,
                    after: Some(0),
                }),
            ],
            44,
            StreamPageSizes {
                snapshot: 16,
                delta: 16,
            },
            WireFormat::Ndjson,
        );

        let docs: Vec<Value> = chunks
            .iter()
            .map(|chunk| {
                serde_json::from_slice::<Value>(&chunk.bytes[..chunk.bytes.len() - 1])
                    .expect("ndjson chunk should parse")
            })
            .collect::<Vec<_>>();

        let checkpoint = docs[0]
            .get("checkpoint")
            .expect("checkpoint payload should exist");
        let buckets = checkpoint
            .get("buckets")
            .and_then(Value::as_array)
            .expect("checkpoint buckets");
        assert_eq!(buckets.len(), 2);
        assert!(buckets.iter().any(|bucket| {
            bucket.get("bucket").and_then(Value::as_str) == Some(DEFAULT_TASKS_BUCKET_NAME)
        }));
        assert!(buckets
            .iter()
            .any(|bucket| bucket.get("bucket").and_then(Value::as_str)
                == Some(project_bucket.as_str())));

        let data_buckets = docs
            .iter()
            .filter_map(|doc| doc.get("data"))
            .map(|data| {
                data.get("bucket")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert!(data_buckets.contains(&DEFAULT_TASKS_BUCKET_NAME.to_owned()));
        assert!(data_buckets.contains(&project_bucket));
    }

    #[test]
    fn ndjson_incremental_chunks_trim_mid_page_cursor() {
        let chunks = sample_sync_stream_ndjson_chunks(Some(15));
        assert!(chunks.len() >= 3);
        assert_eq!(chunks[0].kind, SyncChunkKind::Checkpoint);
        assert_eq!(
            chunks[chunks.len() - 1].kind,
            SyncChunkKind::CheckpointComplete
        );

        let docs = chunks
            .iter()
            .map(|chunk| {
                serde_json::from_slice::<Value>(&chunk.bytes[..chunk.bytes.len() - 1])
                    .expect("ndjson chunk should parse")
            })
            .collect::<Vec<_>>();

        let first_data = docs
            .iter()
            .find_map(|doc| doc.get("data"))
            .expect("incremental stream should include a data chunk");
        assert_eq!(first_data.get("after").and_then(Value::as_str), Some("15"));

        let first_op_id = first_data
            .get("data")
            .and_then(Value::as_array)
            .and_then(|entries| entries.first())
            .and_then(|entry| entry.get("op_id"))
            .and_then(Value::as_str);
        assert_eq!(first_op_id, Some("16"));
    }

    #[test]
    fn full_snapshot_can_coalesce_large_runtime_state_into_few_pages() {
        let current_rows = (1..=1742)
            .map(|index| TaskRow {
                id: format!("task-{index:04}"),
                org_id: "org-001".to_owned(),
                project_id: "project-001".to_owned(),
                title: format!("Task {index}"),
                status: "todo".to_owned(),
                priority: 1,
                assignee_id: "user-001".to_owned(),
                story_points: 1,
                updated_at: "2026-04-11T00:00:00Z".to_owned(),
                summary: format!("runtime:{index}"),
            })
            .collect::<Vec<_>>();

        let chunks = task_sync_stream_chunks_with_page_sizes(
            current_rows,
            Vec::new(),
            1742,
            1742,
            Some(0),
            StreamPageSizes {
                snapshot: 256,
                delta: 64,
            },
            WireFormat::Ndjson,
        );

        let data_chunks = chunks
            .iter()
            .filter(|chunk| chunk.kind == SyncChunkKind::Data)
            .collect::<Vec<_>>();

        assert_eq!(
            data_chunks.len(),
            7,
            "1742-row full snapshot should fit into seven 256-row data pages"
        );
    }

    #[test]
    fn incremental_task_tail_prefers_larger_delta_page_size_than_snapshot_page_size() {
        let current_rows = (1..=46)
            .map(|index| TaskRow {
                id: format!("task-{index:04}"),
                org_id: "org-001".to_owned(),
                project_id: "project-001".to_owned(),
                title: format!("Task {index}"),
                status: "todo".to_owned(),
                priority: 1,
                assignee_id: "user-001".to_owned(),
                story_points: 1,
                updated_at: "2026-04-11T00:00:00Z".to_owned(),
                summary: format!("runtime:{index}"),
            })
            .collect::<Vec<_>>();
        let tail_entries = (43..=62)
            .map(|op_id| OplogEntry {
                op_id: op_id.to_string(),
                op: OplogOperation::Put,
                object_type: Some("tasks".to_owned()),
                object_id: Some(format!("task-runtime-{op_id}")),
                data: Some(
                    serde_json::to_string(&TaskRow {
                        id: format!("task-runtime-{op_id}"),
                        org_id: "org-001".to_owned(),
                        project_id: "project-001".to_owned(),
                        title: format!("Runtime {op_id}"),
                        status: "todo".to_owned(),
                        priority: 1,
                        assignee_id: "user-001".to_owned(),
                        story_points: 1,
                        updated_at: "2026-04-11T00:00:00Z".to_owned(),
                        summary: format!("runtime:{op_id}"),
                    })
                    .expect("task row should serialize"),
                ),
                checksum: 0,
                subkey: None,
            })
            .collect::<Vec<_>>();

        let chunks = task_sync_stream_chunks_with_page_sizes(
            current_rows,
            tail_entries,
            0,
            62,
            Some(42),
            StreamPageSizes {
                snapshot: 16,
                delta: 64,
            },
            WireFormat::Ndjson,
        );

        let data_chunks = chunks
            .iter()
            .filter(|chunk| chunk.kind == SyncChunkKind::Data)
            .collect::<Vec<_>>();

        assert_eq!(
            data_chunks.len(),
            1,
            "incremental 20-op batch should stay in one data page"
        );
    }
}
