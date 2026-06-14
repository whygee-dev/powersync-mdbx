use std::{
    env, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PowerSyncConfig {
    #[serde(default, deserialize_with = "deserialize_optional_u16")]
    pub port: Option<u16>,
    pub replication: Option<ReplicationConfig>,
    pub sync_rules: Option<SyncRulesConfig>,
    pub client_auth: Option<ClientAuthConfig>,
    pub api: Option<ApiConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReplicationConfig {
    #[serde(default)]
    pub connections: Vec<ReplicationConnection>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReplicationConnection {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub tag: Option<String>,
    #[serde(rename = "type", default)]
    pub connection_type: Option<String>,
    pub uri: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SyncRulesConfig {
    pub path: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClientAuthConfig {
    #[serde(default, alias = "jwks_url")]
    pub jwks_uri: Option<String>,
    #[serde(default)]
    pub audience: Vec<String>,
    #[serde(default, alias = "issuers")]
    pub issuer: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ApiConfig {
    #[serde(default)]
    pub tokens: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LoadedPowerSyncConfig {
    path: PathBuf,
    config: PowerSyncConfig,
}

impl LoadedPowerSyncConfig {
    pub fn config(&self) -> &PowerSyncConfig {
        &self.config
    }

    pub fn resolve_path(&self, path: &str) -> PathBuf {
        let path = Path::new(path);
        if path.is_absolute() {
            return path.to_path_buf();
        }
        self.path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(path)
    }
}

pub fn load_config_from_env() -> Result<Option<LoadedPowerSyncConfig>, String> {
    let Some(path) = env::var("POWERSYNC_CONFIG_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    load_config(path)
}

pub fn load_config(path: impl Into<PathBuf>) -> Result<Option<LoadedPowerSyncConfig>, String> {
    let path = path.into();
    let raw = fs::read_to_string(&path).map_err(|error| {
        format!(
            "failed to read PowerSync config {}: {error}",
            path.display()
        )
    })?;
    let expanded = expand_env_tags(&raw);
    let config = serde_yaml::from_str::<PowerSyncConfig>(&expanded).map_err(|error| {
        format!(
            "failed to parse PowerSync config {}: {error}",
            path.display()
        )
    })?;
    Ok(Some(LoadedPowerSyncConfig { path, config }))
}

pub fn env_or_config_port(config: Option<&LoadedPowerSyncConfig>) -> u16 {
    env::var("POWERSYNC_RUST_PORT")
        .or_else(|_| env::var("PS_PORT"))
        .or_else(|_| env::var("PORT"))
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .or_else(|| config.and_then(|loaded| loaded.config().port))
        .unwrap_or(8080)
}

fn expand_env_tags(raw: &str) -> String {
    raw.lines()
        .map(expand_env_tag_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn expand_env_tag_line(line: &str) -> String {
    let Some(index) = line.find("!env") else {
        return line.to_owned();
    };
    let (prefix, rest) = line.split_at(index);
    let var_name = rest.trim_start_matches("!env").trim();
    if var_name.is_empty() {
        return line.to_owned();
    }
    let value = env::var(var_name).unwrap_or_default();
    format!(
        "{prefix}{}",
        serde_json::to_string(&value).expect("string should encode")
    )
}

fn deserialize_optional_u16<'de, D>(deserializer: D) -> Result<Option<u16>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_yaml::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(serde_yaml::Value::Number(number)) => number
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
            .ok_or_else(|| serde::de::Error::custom("invalid u16 port"))
            .map(Some),
        Some(serde_yaml::Value::String(value)) => value
            .parse::<u16>()
            .map(Some)
            .map_err(|error| serde::de::Error::custom(format!("invalid u16 port: {error}"))),
        Some(_) => Err(serde::de::Error::custom("invalid port type")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn loads_powersync_config_with_env_tags_and_relative_sync_rules_path() {
        unsafe {
            env::set_var("PS_DATA_SOURCE_URI", "postgres://user:pass@localhost/db");
            env::set_var("PS_PORT", "2718");
        }
        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
replication:
  connections:
    - type: postgresql
      uri: !env PS_DATA_SOURCE_URI
port: !env PS_PORT
sync_rules:
  path: sync_rules.yaml
client_auth:
  jwks_uri: https://example.test/jwks
  audience: ["dev-fixture-app"]
  issuer: ["https://issuer.example"]
api:
  tokens:
    - admin-token
"#
        )
        .unwrap();

        let loaded = load_config(file.path()).unwrap().unwrap();
        assert_eq!(loaded.config().port, Some(2718));
        assert_eq!(
            loaded.config().replication.as_ref().unwrap().connections[0].uri,
            "postgres://user:pass@localhost/db"
        );
        assert!(loaded
            .resolve_path("sync_rules.yaml")
            .ends_with("sync_rules.yaml"));

        unsafe {
            env::remove_var("PS_DATA_SOURCE_URI");
            env::remove_var("PS_PORT");
        }
    }
}
