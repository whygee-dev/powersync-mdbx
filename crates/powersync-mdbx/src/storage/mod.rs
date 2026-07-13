mod sync_edge;
mod wire_mdbx;

use std::{collections::BTreeMap, env, future::Future, pin::Pin, sync::Arc, time::Duration};

use bytes::Bytes;

use crate::{
    protocol::messages::SyncChunk,
    sync_rules::{default_bucket_requests, RustExecutionPlan},
};

pub use sync_edge::SyncEdgeStorage;
pub use wire_mdbx::WireMdbxStorage;

/// Error surfaced by storage read paths. The message carries internal detail
/// (paths, MDBX errors) and must never be sent to clients verbatim.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct StorageError(pub String);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum StreamEncoding {
    Ndjson,
    Bson,
}

pub type SyncChunkIterator = Box<dyn Iterator<Item = SyncChunk> + Send>;

pub struct SyncChunkSource {
    pub chunks: SyncChunkIterator,
    pub chunk_count_hint: Option<usize>,
    /// Cursors actually represented by this source, captured from the same
    /// storage snapshot as its chunks.
    pub final_cursors: Option<SyncBucketCursors>,
}

pub struct SyncBodySource {
    pub body: Bytes,
    pub chunk_count_hint: Option<usize>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SyncBucketCursor {
    pub name: String,
    pub after: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SyncBucketCursors {
    pub buckets: Vec<SyncBucketCursor>,
    pub default_when_empty: bool,
}

impl Default for SyncBucketCursors {
    fn default() -> Self {
        Self {
            buckets: Vec::new(),
            default_when_empty: true,
        }
    }
}

impl SyncBucketCursors {
    pub fn from_pairs(pairs: impl IntoIterator<Item = (impl Into<String>, u64)>) -> Self {
        Self {
            buckets: pairs
                .into_iter()
                .map(|(name, after)| SyncBucketCursor {
                    name: name.into(),
                    after,
                })
                .collect(),
            default_when_empty: false,
        }
    }

    pub fn after_for(&self, bucket_name: &str) -> Option<u64> {
        self.buckets
            .iter()
            .find(|bucket| bucket.name == bucket_name)
            .map(|bucket| bucket.after)
    }

    pub fn max_after(&self) -> Option<u64> {
        self.buckets.iter().map(|bucket| bucket.after).max()
    }

    pub fn with_updated_after(&self, bucket_name: &str, after: u64) -> Self {
        let mut buckets = self.buckets.clone();
        if let Some(bucket) = buckets.iter_mut().find(|bucket| bucket.name == bucket_name) {
            bucket.after = after;
        } else {
            buckets.push(SyncBucketCursor {
                name: bucket_name.to_owned(),
                after,
            });
        }
        Self {
            buckets,
            default_when_empty: self.default_when_empty,
        }
    }

    pub fn with_global_after(&self, after: u64) -> Self {
        if self.buckets.is_empty() {
            if !self.default_when_empty {
                return self.clone();
            }

            return Self::from_pairs(
                default_bucket_requests()
                    .into_iter()
                    .map(|bucket| (bucket.bucket_name().to_owned(), after)),
            );
        }

        Self {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| SyncBucketCursor {
                    name: bucket.name.clone(),
                    after,
                })
                .collect(),
            default_when_empty: self.default_when_empty,
        }
    }
}

pub trait Storage: Send + Sync {
    fn is_ready(&self) -> Result<bool, StorageError> {
        Ok(true)
    }

    fn sync_chunk_source_for_buckets_with_plan(
        &self,
        buckets: &SyncBucketCursors,
        plan: &RustExecutionPlan,
        encoding: StreamEncoding,
    ) -> Result<SyncChunkSource, StorageError>;

    fn read_parameter_lookup_rows(
        &self,
        _lookup_id: &str,
        _key_values: &[String],
        _max_entries: usize,
    ) -> Result<Vec<BTreeMap<String, String>>, StorageError> {
        Err(StorageError(
            "parameter lookup reads are not supported by this storage backend".to_owned(),
        ))
    }

    fn sync_body_source_for_buckets_with_plan(
        &self,
        buckets: &SyncBucketCursors,
        plan: &RustExecutionPlan,
        encoding: StreamEncoding,
    ) -> Result<SyncBodySource, StorageError> {
        let SyncChunkSource {
            mut chunks,
            chunk_count_hint,
            ..
        } = self.sync_chunk_source_for_buckets_with_plan(buckets, plan, encoding)?;
        let mut body = Vec::new();

        for chunk in chunks.by_ref() {
            body.extend_from_slice(&chunk.bytes);
        }

        Ok(SyncBodySource {
            body: Bytes::from(body),
            chunk_count_hint,
        })
    }

    fn sync_hold_open_body_source_for_buckets_with_plan(
        &self,
        _buckets: &SyncBucketCursors,
        _plan: &RustExecutionPlan,
        _encoding: StreamEncoding,
    ) -> Result<Option<SyncBodySource>, StorageError> {
        Ok(None)
    }

    fn latest_sync_bucket_cursors_with_plan(
        &self,
        _buckets: &SyncBucketCursors,
        _plan: &RustExecutionPlan,
    ) -> Result<Option<SyncBucketCursors>, StorageError> {
        Ok(None)
    }

    fn wait_for_new_sync_bucket_cursors_with_plan<'a>(
        &'a self,
        _buckets: &'a SyncBucketCursors,
        _plan: &'a RustExecutionPlan,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Option<SyncBucketCursors>, StorageError>> + Send + 'a>>
    {
        Box::pin(async move {
            tokio::time::sleep(timeout).await;
            Ok(None)
        })
    }

    fn diagnostics_json(&self) -> Option<serde_json::Value> {
        None
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StorageBackend {
    /// Static fixture backend used by HTTP contract tests.
    SyncEdge,
    /// Production backend: replication-backed MDBX state.
    WireMdbx,
}

impl StorageBackend {
    pub fn from_env() -> Self {
        let raw = env::var("POWERSYNC_RUST_STORAGE_BACKEND").ok();
        Self::from_optional_str(raw.as_deref())
    }

    fn from_optional_str(raw: Option<&str>) -> Self {
        match raw.unwrap_or("wire-mdbx").to_lowercase().as_str() {
            "sync-edge" | "sync_edge" => Self::SyncEdge,
            "wire-mdbx" | "wire_mdbx" => Self::WireMdbx,
            _ => Self::WireMdbx,
        }
    }
}

pub fn build_storage(backend: StorageBackend) -> Arc<dyn Storage> {
    match backend {
        StorageBackend::SyncEdge => Arc::new(SyncEdgeStorage::new_from_env()),
        StorageBackend::WireMdbx => Arc::new(WireMdbxStorage::new_from_env()),
    }
}

#[cfg(test)]
mod tests {
    use super::{StorageBackend, SyncBucketCursors};
    use crate::sync_rules::{default_bucket_requests, DEFAULT_TASKS_BUCKET_NAME};

    #[test]
    fn storage_backend_defaults_to_wire_mdbx_for_missing_value_and_falls_back_for_unknown() {
        assert_eq!(
            StorageBackend::from_optional_str(Some("unknown")),
            StorageBackend::WireMdbx
        );
        assert_eq!(
            StorageBackend::from_optional_str(None),
            StorageBackend::WireMdbx
        );
    }

    #[test]
    fn storage_backend_parses_sync_edge_aliases() {
        assert_eq!(
            StorageBackend::from_optional_str(Some("sync-edge")),
            StorageBackend::SyncEdge
        );
        assert_eq!(
            StorageBackend::from_optional_str(Some("sync_edge")),
            StorageBackend::SyncEdge
        );
    }

    #[test]
    fn storage_backend_parses_wire_mdbx_aliases() {
        assert_eq!(
            StorageBackend::from_optional_str(Some("wire-mdbx")),
            StorageBackend::WireMdbx
        );
        assert_eq!(
            StorageBackend::from_optional_str(Some("wire_mdbx")),
            StorageBackend::WireMdbx
        );
    }

    #[test]
    fn sync_bucket_cursors_update_global_after_without_default_bucket_special_casing() {
        let cursors =
            SyncBucketCursors::from_pairs([("ignored", 7), (DEFAULT_TASKS_BUCKET_NAME, 42)]);

        assert_eq!(cursors.max_after(), Some(42));
        assert_eq!(
            cursors.with_updated_after(DEFAULT_TASKS_BUCKET_NAME, 99),
            SyncBucketCursors::from_pairs([("ignored", 7), (DEFAULT_TASKS_BUCKET_NAME, 99)])
        );
        assert_eq!(
            cursors.with_updated_after("other", 5),
            SyncBucketCursors::from_pairs([
                ("ignored", 7),
                (DEFAULT_TASKS_BUCKET_NAME, 42),
                ("other", 5),
            ])
        );
        assert_eq!(
            cursors.with_global_after(99),
            SyncBucketCursors::from_pairs([("ignored", 99), (DEFAULT_TASKS_BUCKET_NAME, 99)])
        );
        assert_eq!(
            SyncBucketCursors::default().with_global_after(11),
            SyncBucketCursors::from_pairs(
                default_bucket_requests()
                    .into_iter()
                    .map(|bucket| (bucket.bucket_name().to_owned(), 11))
            )
        );
        assert_eq!(
            SyncBucketCursors::from_pairs(std::iter::empty::<(&str, u64)>()).with_global_after(11),
            SyncBucketCursors::from_pairs(std::iter::empty::<(&str, u64)>())
        );
    }
}
