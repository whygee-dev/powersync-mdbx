use std::{
    fmt::{self, Display},
    str::FromStr,
};

use pgwire_replication::lsn::Lsn;

#[cfg(test)]
const POSTGRES_EPOCH_MICROS: u64 = 946_684_800_u64 * 1_000_000;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresLsn(pub u64);

impl PostgresLsn {
    pub fn to_u64(self) -> u64 {
        self.0
    }
}

impl From<PostgresLsn> for Lsn {
    fn from(value: PostgresLsn) -> Self {
        Self::from_u64(value.to_u64())
    }
}

impl From<Lsn> for PostgresLsn {
    fn from(value: Lsn) -> Self {
        Self(value.as_u64())
    }
}

impl Display for PostgresLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let high = self.0 >> 32;
        let low = self.0 & 0xFFFF_FFFF;
        write!(f, "{high:X}/{low:X}")
    }
}

impl FromStr for PostgresLsn {
    type Err = PostgresLsnParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (left, right) = value
            .split_once('/')
            .ok_or_else(|| PostgresLsnParseError::InvalidFormat(value.to_owned()))?;
        Ok(Self((parse_lsn_half(left)? << 32) | parse_lsn_half(right)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PostgresLsnParseError {
    #[error("invalid PostgreSQL LSN {0:?}")]
    InvalidFormat(String),
    #[error("invalid PostgreSQL LSN half {0:?}")]
    InvalidHalf(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub enum ReplicationSlotMode {
    Temporary,
    Persistent,
}

#[cfg(test)]
pub fn create_publication_query(publication_name: &str) -> String {
    format!(
        "CREATE PUBLICATION {} FOR ALL TABLES",
        quote_identifier(publication_name)
    )
}

#[cfg(test)]
pub fn create_replication_slot_query(slot_name: &str, mode: ReplicationSlotMode) -> String {
    match mode {
        ReplicationSlotMode::Temporary => {
            format!(
                "CREATE_REPLICATION_SLOT {slot_name} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"
            )
        }
        ReplicationSlotMode::Persistent => {
            format!("CREATE_REPLICATION_SLOT {slot_name} LOGICAL pgoutput NOEXPORT_SNAPSHOT")
        }
    }
}

#[cfg(test)]
pub fn start_replication_query(
    slot_name: &str,
    publication_names: &[String],
    start_lsn: PostgresLsn,
) -> Result<String, ReplicationQueryError> {
    let publications = normalize_publication_names(publication_names)?;
    Ok(format!(
        "START_REPLICATION SLOT {slot_name} LOGICAL {start_lsn} (proto_version '1', publication_names '{publications}')"
    ))
}

#[cfg(test)]
pub fn standby_status_update_frame(lsn: PostgresLsn, now_unix_micros: u64) -> [u8; 34] {
    let postgres_time = now_unix_micros.saturating_sub(POSTGRES_EPOCH_MICROS);
    let lsn = lsn.to_u64();
    let mut frame = [0u8; 34];
    frame[0] = b'r';
    frame[1..9].copy_from_slice(&lsn.to_be_bytes());
    frame[9..17].copy_from_slice(&lsn.to_be_bytes());
    frame[17..25].copy_from_slice(&lsn.to_be_bytes());
    frame[25..33].copy_from_slice(&postgres_time.to_be_bytes());
    frame[33] = 0;
    frame
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg(test)]
pub enum ReplicationQueryError {
    #[error("expected at least one publication name")]
    MissingPublicationNames,
}

#[cfg(test)]
fn quote_identifier(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn parse_lsn_half(value: &str) -> Result<u64, PostgresLsnParseError> {
    // Each half of an `X/Y` LSN is a 32-bit word; parsing as u32 rejects an
    // oversized half instead of letting it carry into the other word.
    u32::from_str_radix(value, 16)
        .map(u64::from)
        .map_err(|_| PostgresLsnParseError::InvalidHalf(value.to_owned()))
}

#[cfg(test)]
fn normalize_publication_names(
    publication_names: &[String],
) -> Result<String, ReplicationQueryError> {
    let normalized = publication_names
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();

    if normalized.is_empty() {
        return Err(ReplicationQueryError::MissingPublicationNames);
    }

    Ok(normalized.join(","))
}

#[cfg(test)]
mod tests {
    use super::{
        create_publication_query, create_replication_slot_query, standby_status_update_frame,
        start_replication_query, PostgresLsn, PostgresLsnParseError, ReplicationQueryError,
        ReplicationSlotMode,
    };
    use std::str::FromStr;

    #[test]
    fn parses_and_formats_postgres_lsn() {
        let lsn = PostgresLsn::from_str("16/B3730").expect("lsn");
        assert_eq!(lsn.to_u64(), (0x16_u64 << 32) + 0xB3730);
        assert_eq!(lsn.to_string(), "16/B3730");
    }

    #[test]
    fn invalid_lsn_format_is_rejected() {
        let error = PostgresLsn::from_str("bogus").expect_err("invalid");
        assert_eq!(
            error,
            PostgresLsnParseError::InvalidFormat("bogus".to_owned())
        );
    }

    #[test]
    fn invalid_lsn_half_is_rejected() {
        let error = PostgresLsn::from_str("XYZ/1").expect_err("invalid");
        assert_eq!(error, PostgresLsnParseError::InvalidHalf("XYZ".to_owned()));
    }

    #[test]
    fn lsn_half_exceeding_32_bits_is_rejected() {
        // Each LSN half is a 32-bit value; an oversized half must be rejected,
        // not silently carried into the high word.
        assert_eq!(
            PostgresLsn::from_str("0/1FFFFFFFF").expect_err("oversized low half"),
            PostgresLsnParseError::InvalidHalf("1FFFFFFFF".to_owned())
        );
        assert_eq!(
            PostgresLsn::from_str("1FFFFFFFF/0").expect_err("oversized high half"),
            PostgresLsnParseError::InvalidHalf("1FFFFFFFF".to_owned())
        );
        // The maximum valid LSN still parses.
        assert_eq!(
            PostgresLsn::from_str("FFFFFFFF/FFFFFFFF")
                .expect("max lsn")
                .to_u64(),
            u64::MAX
        );
    }

    #[test]
    fn builds_persistent_slot_query() {
        assert_eq!(
            create_replication_slot_query("slot_rust", ReplicationSlotMode::Persistent),
            "CREATE_REPLICATION_SLOT slot_rust LOGICAL pgoutput NOEXPORT_SNAPSHOT"
        );
    }

    #[test]
    fn builds_temporary_slot_query() {
        assert_eq!(
            create_replication_slot_query("slot_rust", ReplicationSlotMode::Temporary),
            "CREATE_REPLICATION_SLOT slot_rust TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"
        );
    }

    #[test]
    fn builds_publication_query_with_identifier_quoting() {
        assert_eq!(
            create_publication_query("pub\"rust"),
            "CREATE PUBLICATION \"pub\"\"rust\" FOR ALL TABLES"
        );
    }

    #[test]
    fn builds_start_replication_query() {
        let query = start_replication_query(
            "slot_rust",
            &[String::from("pub_a"), String::from("pub_b")],
            PostgresLsn::from_str("0/16B6AF0").expect("lsn"),
        )
        .expect("query");

        assert_eq!(
            query,
            "START_REPLICATION SLOT slot_rust LOGICAL 0/16B6AF0 (proto_version '1', publication_names 'pub_a,pub_b')"
        );
    }

    #[test]
    fn start_replication_query_requires_publications() {
        let error = start_replication_query("slot_rust", &[], PostgresLsn(0)).expect_err("missing");
        assert_eq!(error, ReplicationQueryError::MissingPublicationNames);
    }

    #[test]
    fn standby_status_update_frame_matches_postgres_wire_shape() {
        let frame = standby_status_update_frame(PostgresLsn(42), 946_684_800_000_123);
        assert_eq!(frame.len(), 34);
        assert_eq!(frame[0], b'r');
        assert_eq!(u64::from_be_bytes(frame[1..9].try_into().unwrap()), 42);
        assert_eq!(u64::from_be_bytes(frame[9..17].try_into().unwrap()), 42);
        assert_eq!(u64::from_be_bytes(frame[17..25].try_into().unwrap()), 42);
        assert_eq!(u64::from_be_bytes(frame[25..33].try_into().unwrap()), 123);
        assert_eq!(frame[33], 0);
    }
}
