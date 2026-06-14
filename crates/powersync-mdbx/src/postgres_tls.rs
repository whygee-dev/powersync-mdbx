use std::{path::PathBuf, str::FromStr};

use pgwire_replication::config::{SslMode as ReplicationSslMode, TlsConfig};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use tokio_postgres::config::Host;
use tokio_postgres_rustls::MakeRustlsConnect;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresTlsPolicy {
    Disabled,
    VerifyFull {
        ca_path: Option<PathBuf>,
        client_cert_path: Option<PathBuf>,
        client_key_path: Option<PathBuf>,
    },
}

#[derive(Debug, Clone)]
pub struct ParsedPostgresConnection {
    pub config: tokio_postgres::Config,
    pub tls: PostgresTlsPolicy,
}

impl ParsedPostgresConnection {
    pub fn parse(uri: &str) -> Result<Self, String> {
        let mut url =
            Url::parse(uri).map_err(|error| format!("invalid PostgreSQL URI: {error}"))?;
        let mut sslmode = None;
        let mut ca_path = None;
        let mut client_cert_path = None;
        let mut client_key_path = None;
        let mut retained = Vec::new();
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "sslmode" => sslmode = Some(value.into_owned()),
                "sslrootcert" => ca_path = nonempty_path(&value),
                "sslcert" => client_cert_path = nonempty_path(&value),
                "sslkey" => client_key_path = nonempty_path(&value),
                "sslsni" => {
                    return Err(
                        "PostgreSQL sslsni overrides are not supported; use a URI hostname matching the server certificate"
                            .to_owned(),
                    )
                }
                _ => retained.push((key.into_owned(), value.into_owned())),
            }
        }
        url.set_query(None);
        if !retained.is_empty() {
            url.query_pairs_mut().extend_pairs(retained);
        }
        let sanitized_uri = url.to_string();
        let config = tokio_postgres::Config::from_str(&sanitized_uri)
            .map_err(|error| format!("invalid PostgreSQL connection settings: {error}"))?;
        let unix_socket = config
            .get_hosts()
            .iter()
            .all(|host| matches!(host, Host::Unix(_)));
        let tls = match sslmode.as_deref() {
            Some("disable") if ca_path.is_none() && client_cert_path.is_none() && client_key_path.is_none() => {
                PostgresTlsPolicy::Disabled
            }
            Some("verify-full") => {
                if client_cert_path.is_some() != client_key_path.is_some() {
                    return Err("PostgreSQL sslcert and sslkey must be configured together".to_owned());
                }
                PostgresTlsPolicy::VerifyFull {
                    ca_path,
                    client_cert_path,
                    client_key_path,
                }
            }
            None if unix_socket => PostgresTlsPolicy::Disabled,
            None => {
                return Err(
                    "TCP PostgreSQL connections require an explicit sslmode=verify-full or sslmode=disable"
                        .to_owned(),
                )
            }
            Some(other) => {
                return Err(format!(
                    "unsupported PostgreSQL sslmode={other}; use verify-full, or explicitly use disable only on a trusted private transport"
                ))
            }
        };
        Ok(Self { config, tls })
    }

    pub fn replication_tls(&self) -> TlsConfig {
        match &self.tls {
            PostgresTlsPolicy::Disabled => TlsConfig::disabled(),
            PostgresTlsPolicy::VerifyFull {
                ca_path,
                client_cert_path,
                client_key_path,
            } => TlsConfig {
                mode: ReplicationSslMode::VerifyFull,
                ca_pem_path: ca_path.clone(),
                sni_hostname: None,
                client_cert_pem_path: client_cert_path.clone(),
                client_key_pem_path: client_key_path.clone(),
            },
        }
    }

    pub fn rustls_connector(&self) -> Result<MakeRustlsConnect, String> {
        let PostgresTlsPolicy::VerifyFull {
            ca_path,
            client_cert_path,
            client_key_path,
        } = &self.tls
        else {
            return Err("TLS connector requested for a plaintext PostgreSQL policy".to_owned());
        };

        let mut roots = RootCertStore::empty();
        if let Some(path) = ca_path {
            let mut certificate_count = 0usize;
            let certificates = CertificateDer::pem_file_iter(path).map_err(|error| {
                format!("open PostgreSQL CA certificate {}: {error}", path.display())
            })?;
            for certificate in certificates {
                roots
                    .add(certificate.map_err(|error| {
                        format!("read PostgreSQL CA certificate {}: {error}", path.display())
                    })?)
                    .map_err(|error| {
                        format!("add PostgreSQL CA certificate {}: {error}", path.display())
                    })?;
                certificate_count += 1;
            }
            if certificate_count == 0 {
                return Err(format!(
                    "PostgreSQL CA certificate file {} contains no certificates",
                    path.display()
                ));
            }
        } else {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        let builder = ClientConfig::builder().with_root_certificates(roots);
        let config = match (client_cert_path, client_key_path) {
            (Some(cert_path), Some(key_path)) => {
                let certificates = CertificateDer::pem_file_iter(cert_path)
                    .map_err(|error| {
                        format!(
                            "open PostgreSQL client certificate {}: {error}",
                            cert_path.display()
                        )
                    })?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        format!(
                            "read PostgreSQL client certificate {}: {error}",
                            cert_path.display()
                        )
                    })?;
                let key = PrivateKeyDer::from_pem_file(key_path).map_err(|error| {
                    format!("read PostgreSQL client key {}: {error}", key_path.display())
                })?;
                builder
                    .with_client_auth_cert(certificates, key)
                    .map_err(|error| format!("configure PostgreSQL client certificate: {error}"))?
            }
            (None, None) => builder.with_no_client_auth(),
            _ => unreachable!("certificate/key pairing validated during parsing"),
        };
        Ok(MakeRustlsConnect::new(config))
    }
}

fn nonempty_path(value: &str) -> Option<PathBuf> {
    (!value.trim().is_empty()).then(|| PathBuf::from(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_policy_is_explicit_and_fail_closed() {
        assert!(ParsedPostgresConnection::parse("postgres://u:p@db.example/app").is_err());
        assert!(
            ParsedPostgresConnection::parse("postgres://u:p@db.example/app?sslmode=require")
                .is_err()
        );
        assert!(matches!(
            ParsedPostgresConnection::parse(
                "postgres://u:p@db.example/app?application_name=sync&sslmode=verify-full"
            )
            .expect("verify-full policy")
            .tls,
            PostgresTlsPolicy::VerifyFull { .. }
        ));
        assert!(matches!(
            ParsedPostgresConnection::parse("postgres://u:p@localhost/app?sslmode=disable")
                .expect("explicit plaintext policy")
                .tls,
            PostgresTlsPolicy::Disabled
        ));
    }

    #[test]
    fn client_certificate_requires_key_pair() {
        let error = ParsedPostgresConnection::parse(
            "postgres://u:p@db.example/app?sslmode=verify-full&sslcert=client.pem",
        )
        .expect_err("incomplete mTLS policy must fail");
        assert!(error.contains("sslcert and sslkey"));
    }

    #[test]
    fn custom_ca_replaces_system_roots_and_must_contain_a_certificate() {
        let empty_ca = tempfile::NamedTempFile::new().expect("temporary CA file");
        let connection = ParsedPostgresConnection {
            config: tokio_postgres::Config::new(),
            tls: PostgresTlsPolicy::VerifyFull {
                ca_path: Some(empty_ca.path().to_owned()),
                client_cert_path: None,
                client_key_path: None,
            },
        };

        let error = match connection.rustls_connector() {
            Ok(_) => panic!("an empty custom CA file must fail closed"),
            Err(error) => error,
        };
        assert!(error.contains("contains no certificates"));
    }
}
