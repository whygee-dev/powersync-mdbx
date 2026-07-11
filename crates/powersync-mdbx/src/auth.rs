use std::{
    collections::BTreeMap,
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::http::{HeaderMap, StatusCode};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use url::{Host, Url};

const AUTH_REQUIRED_CODE: &str = "PSYNC_S2106";

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct AuthFailure {
    pub status: StatusCode,
    pub body: Value,
}

impl AuthFailure {
    fn required() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            body: json!({"error": "Authentication required", "code": AUTH_REQUIRED_CODE}),
        }
    }

    fn failed() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            body: json!({"error": "Authentication failed", "code": AUTH_REQUIRED_CODE}),
        }
    }

    pub fn disabled() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            body: json!({"error": "Authentication disabled", "code": AUTH_REQUIRED_CODE}),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TokenPayload {
    claims: Value,
    exp: Option<u64>,
    user_id: Option<String>,
}

impl TokenPayload {
    #[doc(hidden)] // test-support: public only so integration tests can build payloads
    pub fn new_for_tests(claims: Value, user_id: Option<String>) -> Self {
        let exp = claims
            .as_object()
            .and_then(|claims| claims.get("exp"))
            .and_then(value_as_u64);
        Self {
            claims,
            exp,
            user_id,
        }
    }

    pub fn claims(&self) -> &Value {
        &self.claims
    }

    pub fn exp(&self) -> Option<u64> {
        self.exp
    }

    pub fn remaining_lifetime(&self) -> Option<Duration> {
        let expires_at = UNIX_EPOCH.checked_add(Duration::from_secs(self.exp?))?;
        Some(
            expires_at
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO),
        )
    }

    pub fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }

    pub fn flattened_claim_strings(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        flatten_claim_value("", &self.claims, &mut out);
        out
    }
}

fn flatten_claim_value(prefix: &str, value: &Value, out: &mut BTreeMap<String, String>) {
    match value {
        Value::Object(map) => {
            for (key, nested) in map {
                let next = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_claim_value(&next, nested, out);
            }
        }
        Value::Null => {}
        Value::String(string) => {
            if !prefix.is_empty() {
                out.insert(prefix.to_owned(), string.clone());
            }
        }
        Value::Number(number) => {
            if !prefix.is_empty() {
                out.insert(prefix.to_owned(), number.to_string());
            }
        }
        Value::Bool(boolean) => {
            if !prefix.is_empty() {
                out.insert(prefix.to_owned(), boolean.to_string());
            }
        }
        Value::Array(array) => {
            if !prefix.is_empty() {
                out.insert(prefix.to_owned(), Value::Array(array.clone()).to_string());
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct UserAuthConfig {
    keys: Vec<AuthKey>,
    audiences: Vec<String>,
    issuers: Vec<String>,
}

impl UserAuthConfig {
    pub fn from_hs256_secrets(
        secrets: Vec<(Option<String>, Vec<u8>)>,
        audiences: Vec<String>,
        issuers: Vec<String>,
    ) -> Result<Self, String> {
        validate_claim_policy(&audiences, &issuers)?;
        Ok(Self {
            keys: secrets
                .into_iter()
                .map(|(kid, secret)| AuthKey::Hmac(HmacKey { kid, secret }))
                .collect(),
            audiences,
            issuers,
        })
    }

    pub fn from_env() -> Result<Option<Self>, String> {
        Self::from_env_with_config(None)
    }

    pub fn from_env_with_config(
        config: Option<&crate::config::PowerSyncConfig>,
    ) -> Result<Option<Self>, String> {
        let supabase_secret = env::var("POWERSYNC_RUST_SUPABASE_JWT_SECRET")
            .ok()
            .filter(|secret| !secret.is_empty());
        let raw_jwks = env::var("POWERSYNC_RUST_JWKS_JSON")
            .ok()
            .filter(|raw_jwks| !raw_jwks.trim().is_empty());
        let jwks_url = env::var("POWERSYNC_RUST_JWKS_URL")
            .or_else(|_| env::var("PS_JWKS_URL"))
            .or_else(|_| env::var("PS_JWKS_URI"))
            .ok()
            .filter(|url| !url.trim().is_empty())
            .or_else(|| {
                config
                    .and_then(|config| config.client_auth.as_ref())
                    .and_then(|client_auth| client_auth.jwks_uri.clone())
            });
        let audiences = env::var("POWERSYNC_RUST_JWT_AUDIENCES")
            .or_else(|_| env::var("PS_JWT_AUDIENCES"))
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .or_else(|| {
                config
                    .and_then(|config| config.client_auth.as_ref())
                    .map(|client_auth| client_auth.audience.clone())
            })
            .unwrap_or_default();
        let issuers = env::var("POWERSYNC_RUST_JWT_ISSUERS")
            .or_else(|_| env::var("PS_JWT_ISSUERS"))
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .or_else(|| {
                config
                    .and_then(|config| config.client_auth.as_ref())
                    .map(|client_auth| client_auth.issuer.clone())
            })
            .unwrap_or_default();

        Self::from_sources(supabase_secret, raw_jwks, jwks_url, audiences, issuers)
    }

    fn from_sources(
        supabase_secret: Option<String>,
        raw_jwks: Option<String>,
        jwks_url: Option<String>,
        audiences: Vec<String>,
        issuers: Vec<String>,
    ) -> Result<Option<Self>, String> {
        let mut keys = Vec::new();
        if let Some(secret) = supabase_secret {
            keys.push(AuthKey::Hmac(HmacKey {
                kid: None,
                secret: secret.into_bytes(),
            }));
        }
        if let Some(raw_jwks) = raw_jwks {
            let jwks_keys = parse_jwks(&raw_jwks)?;
            if jwks_keys.is_empty() {
                return Err(
                    "POWERSYNC_RUST_JWKS_JSON did not contain any supported HS256 or RS256 keys"
                        .to_owned(),
                );
            }
            keys.extend(jwks_keys);
        }
        if let Some(jwks_url) = jwks_url {
            let raw = fetch_jwks(&jwks_url)?;
            let jwks_keys = parse_jwks(&raw)?;
            if jwks_keys.is_empty() {
                return Err(format!(
                    "JWKS URL {jwks_url} did not contain any supported HS256 or RS256 keys"
                ));
            }
            keys.extend(jwks_keys);
        }
        if keys.is_empty() {
            return Ok(None);
        }

        validate_claim_policy(&audiences, &issuers)?;

        Ok(Some(Self {
            keys,
            audiences,
            issuers,
        }))
    }

    pub fn authorize_headers(&self, headers: &HeaderMap) -> Result<TokenPayload, AuthFailure> {
        let token = extract_token(headers)?;
        self.verify_token(&token)
    }

    pub fn verify_token(&self, token: &str) -> Result<TokenPayload, AuthFailure> {
        let (header_segment, payload_segment, signature_segment) = split_jwt(token)?;
        let header: JwtHeader =
            decode_segment(header_segment).map_err(|_| AuthFailure::failed())?;
        let payload: Value = decode_segment(payload_segment).map_err(|_| AuthFailure::failed())?;
        if header.alg != "HS256" {
            return self.verify_rsa_token(token, header, payload);
        }

        let key =
            select_hmac_key(&self.keys, header.kid.as_deref()).ok_or_else(AuthFailure::failed)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature_segment)
            .map_err(|_| AuthFailure::failed())?;

        let mut mac = HmacSha256::new_from_slice(&key.secret).map_err(|_| AuthFailure::failed())?;
        mac.update(format!("{header_segment}.{payload_segment}").as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| AuthFailure::failed())?;

        self.validate_payload(payload)
    }

    fn verify_rsa_token(
        &self,
        token: &str,
        header: JwtHeader,
        payload: Value,
    ) -> Result<TokenPayload, AuthFailure> {
        if header.alg != "RS256" {
            return Err(AuthFailure::failed());
        }
        let key =
            select_rsa_key(&self.keys, header.kid.as_deref()).ok_or_else(AuthFailure::failed)?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.required_spec_claims.clear();
        let decoding_key =
            DecodingKey::from_rsa_components(&key.n, &key.e).map_err(|_| AuthFailure::failed())?;
        decode::<Value>(token, &decoding_key, &validation).map_err(|_| AuthFailure::failed())?;
        self.validate_payload(payload)
    }

    fn validate_payload(&self, payload: Value) -> Result<TokenPayload, AuthFailure> {
        const NBF_LEEWAY_SECS: u64 = 60;

        let Some(claims) = payload.as_object() else {
            return Err(AuthFailure::failed());
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // `exp` is required, like the official service: a token without a
        // parseable expiry would otherwise be valid forever.
        let Some(exp) = claims.get("exp").and_then(value_as_u64) else {
            return Err(AuthFailure::failed());
        };
        if exp <= now {
            return Err(AuthFailure::failed());
        }
        if let Some(nbf_claim) = claims.get("nbf") {
            let Some(nbf) = value_as_u64(nbf_claim) else {
                return Err(AuthFailure::failed());
            };
            if nbf > now + NBF_LEEWAY_SECS {
                return Err(AuthFailure::failed());
            }
        }

        if !self.audiences.is_empty() && !audience_matches(claims.get("aud"), &self.audiences) {
            return Err(AuthFailure::failed());
        }
        if !self.issuers.is_empty()
            && !claims
                .get("iss")
                .and_then(Value::as_str)
                .is_some_and(|issuer| self.issuers.iter().any(|expected| expected == issuer))
        {
            return Err(AuthFailure::failed());
        }

        let user_id = claims
            .get("sub")
            .and_then(Value::as_str)
            .or_else(|| claims.get("user_id").and_then(Value::as_str))
            .map(str::to_owned);

        Ok(TokenPayload {
            claims: payload,
            exp: Some(exp),
            user_id,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ApiAuthConfig {
    tokens: Vec<String>,
}

impl ApiAuthConfig {
    pub fn new(tokens: Vec<String>) -> Self {
        Self { tokens }
    }

    pub fn from_env() -> Self {
        Self::from_env_with_config(None)
    }

    pub fn from_env_with_config(config: Option<&crate::config::PowerSyncConfig>) -> Self {
        let tokens = env::var("POWERSYNC_RUST_API_TOKENS")
            .or_else(|_| env::var("PS_API_TOKENS"))
            .or_else(|_| env::var("PS_API_TOKEN"))
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .or_else(|| {
                config
                    .and_then(|config| config.api.as_ref())
                    .map(|api| api.tokens.clone())
            })
            .unwrap_or_default();
        Self { tokens }
    }

    pub fn authorize_headers(&self, headers: &HeaderMap) -> Result<(), AuthFailure> {
        if self.tokens.is_empty() {
            return Err(AuthFailure::disabled());
        }
        let token = extract_token(headers)?;
        if self
            .tokens
            .iter()
            .any(|expected| constant_time_token_eq(expected, &token))
        {
            Ok(())
        } else {
            Err(AuthFailure::failed())
        }
    }

    pub fn is_configured(&self) -> bool {
        !self.tokens.is_empty()
    }
}

/// Compares tokens without short-circuiting on the first mismatched byte.
/// Hashing both sides first also keeps token length out of the timing signal.
fn constant_time_token_eq(expected: &str, candidate: &str) -> bool {
    use sha2::Digest;
    let expected = Sha256::digest(expected.as_bytes());
    let candidate = Sha256::digest(candidate.as_bytes());
    expected
        .iter()
        .zip(candidate.iter())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

#[derive(Debug, Clone)]
struct HmacKey {
    kid: Option<String>,
    secret: Vec<u8>,
}

#[derive(Debug, Clone)]
struct RsaKey {
    kid: Option<String>,
    n: String,
    e: String,
}

#[derive(Debug, Clone)]
enum AuthKey {
    Hmac(HmacKey),
    Rsa(RsaKey),
}

#[derive(Debug, Deserialize)]
struct JwtHeader {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    kty: String,
    #[serde(default)]
    alg: Option<String>,
    #[serde(default)]
    kid: Option<String>,
    #[serde(default)]
    k: String,
    #[serde(default)]
    n: String,
    #[serde(default)]
    e: String,
}

fn extract_token(headers: &HeaderMap) -> Result<String, AuthFailure> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let mut parts = raw.split_whitespace();
    let Some(scheme) = parts.next() else {
        return Err(AuthFailure::required());
    };
    let Some(token) = parts.next() else {
        return Err(AuthFailure::required());
    };
    if parts.next().is_some() {
        return Err(AuthFailure::required());
    }
    if scheme.eq_ignore_ascii_case("bearer") || scheme.eq_ignore_ascii_case("token") {
        Ok(token.to_owned())
    } else {
        Err(AuthFailure::required())
    }
}

fn split_jwt(token: &str) -> Result<(&str, &str, &str), AuthFailure> {
    let mut parts = token.split('.');
    let header = parts.next().ok_or_else(AuthFailure::failed)?;
    let payload = parts.next().ok_or_else(AuthFailure::failed)?;
    let signature = parts.next().ok_or_else(AuthFailure::failed)?;
    if parts.next().is_some() {
        return Err(AuthFailure::failed());
    }
    Ok((header, payload, signature))
}

fn decode_segment<T: for<'de> Deserialize<'de>>(segment: &str) -> Result<T, String> {
    let decoded = URL_SAFE_NO_PAD
        .decode(segment)
        .map_err(|error| format!("base64 decode failed: {error}"))?;
    serde_json::from_slice(&decoded).map_err(|error| format!("json decode failed: {error}"))
}

#[cfg(test)]
fn parse_hmac_jwks(raw: &str) -> Result<Vec<HmacKey>, String> {
    parse_jwks(raw).map(|keys| {
        keys.into_iter()
            .filter_map(|key| match key {
                AuthKey::Hmac(key) => Some(key),
                AuthKey::Rsa(_) => None,
            })
            .collect()
    })
}

fn parse_jwks(raw: &str) -> Result<Vec<AuthKey>, String> {
    let jwks = if raw.trim_start().starts_with('[') {
        Jwks {
            keys: serde_json::from_str(raw)
                .map_err(|error| format!("invalid JWKS JSON array: {error}"))?,
        }
    } else {
        serde_json::from_str::<Jwks>(raw).map_err(|error| format!("invalid JWKS JSON: {error}"))?
    };

    jwks.keys
        .into_iter()
        .filter(|key| key.kty.eq_ignore_ascii_case("oct") || key.kty.eq_ignore_ascii_case("RSA"))
        .map(|key| match key.kty.to_ascii_uppercase().as_str() {
            "OCT" => {
                if let Some(alg) = &key.alg {
                    if !alg.eq_ignore_ascii_case("HS256") {
                        return Err(format!("unsupported jwk alg {alg}; expected HS256"));
                    }
                }
                let secret = URL_SAFE_NO_PAD
                    .decode(key.k)
                    .map_err(|error| format!("invalid jwk secret: {error}"))?;
                Ok(AuthKey::Hmac(HmacKey {
                    kid: key.kid,
                    secret,
                }))
            }
            "RSA" => {
                if let Some(alg) = &key.alg {
                    if !alg.eq_ignore_ascii_case("RS256") {
                        return Err(format!("unsupported jwk alg {alg}; expected RS256"));
                    }
                }
                if key.n.is_empty() || key.e.is_empty() {
                    return Err("RSA jwk missing modulus or exponent".to_owned());
                }
                Ok(AuthKey::Rsa(RsaKey {
                    kid: key.kid,
                    n: key.n,
                    e: key.e,
                }))
            }
            _ => unreachable!("unsupported keys were filtered"),
        })
        .collect::<Result<Vec<_>, _>>()
}

fn fetch_jwks(url: &str) -> Result<String, String> {
    let url = validate_jwks_url(url)?;
    let allow_loopback_http_redirects = jwks_url_is_loopback_http(&url);
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= 10 {
                return attempt.error("too many JWKS redirects");
            }
            if jwks_redirect_transport_allowed(
                attempt.url(),
                allow_loopback_http_redirects,
            ) {
                attempt.follow()
            } else {
                attempt.error(
                    "JWKS redirects must use HTTPS unless the configured URL and redirect destination are both explicit loopback HTTP development URLs",
                )
            }
        }))
        .build()
        .map_err(|error| format!("failed to build JWKS HTTP client: {error}"))?
        .get(url.clone())
        .send()
        .map_err(|error| format!("failed to fetch JWKS URL {url}: {error}"))?
        .error_for_status()
        .map_err(|error| format!("JWKS URL {url} returned an error: {error}"))?
        .text()
        .map_err(|error| format!("failed to read JWKS URL {url}: {error}"))
}

fn validate_jwks_url(raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|error| format!("invalid JWKS URL {raw}: {error}"))?;
    if url.host().is_none() {
        return Err(format!("JWKS URL {raw} must include a host"));
    }
    if !jwks_url_transport_allowed(&url) {
        return Err(format!(
            "JWKS URL {raw} must use HTTPS; HTTP is permitted only for explicit loopback development URLs"
        ));
    }
    Ok(url)
}

fn jwks_url_transport_allowed(url: &Url) -> bool {
    url.scheme() == "https" || jwks_url_is_loopback_http(url)
}

fn jwks_url_is_loopback_http(url: &Url) -> bool {
    if url.scheme() != "http" {
        return false;
    }
    match url.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

fn jwks_redirect_transport_allowed(url: &Url, allow_loopback_http: bool) -> bool {
    url.scheme() == "https" || (allow_loopback_http && jwks_url_is_loopback_http(url))
}

fn select_hmac_key<'a>(keys: &'a [AuthKey], kid: Option<&str>) -> Option<&'a HmacKey> {
    select_auth_key(keys, kid, |key| match key {
        AuthKey::Hmac(key) => Some(key),
        AuthKey::Rsa(_) => None,
    })
}

fn select_rsa_key<'a>(keys: &'a [AuthKey], kid: Option<&str>) -> Option<&'a RsaKey> {
    select_auth_key(keys, kid, |key| match key {
        AuthKey::Hmac(_) => None,
        AuthKey::Rsa(key) => Some(key),
    })
}

fn select_auth_key<'a, T>(
    keys: &'a [AuthKey],
    kid: Option<&str>,
    as_key: impl Fn(&'a AuthKey) -> Option<&'a T>,
) -> Option<&'a T> {
    match kid {
        Some(expected) => keys.iter().find_map(|key| {
            let typed = as_key(key)?;
            if auth_key_kid(key) == Some(expected) {
                Some(typed)
            } else {
                None
            }
        }),
        None => keys.iter().find_map(as_key),
    }
}

fn auth_key_kid(key: &AuthKey) -> Option<&str> {
    match key {
        AuthKey::Hmac(key) => key.kid.as_deref(),
        AuthKey::Rsa(key) => key.kid.as_deref(),
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        // RFC 7519 NumericDate values may carry fractional seconds; truncate
        // instead of treating a float claim as absent.
        Value::Number(number) => number.as_u64().or_else(|| {
            number
                .as_f64()
                .filter(|value| value.is_finite() && *value >= 0.0)
                .map(|value| value as u64)
        }),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn audience_matches(value: Option<&Value>, audiences: &[String]) -> bool {
    let Some(value) = value else {
        return false;
    };
    match value {
        Value::String(text) => audiences.iter().any(|audience| audience == text),
        Value::Array(array) => array
            .iter()
            .filter_map(Value::as_str)
            .any(|candidate| audiences.iter().any(|audience| audience == candidate)),
        _ => false,
    }
}

fn validate_claim_policy(audiences: &[String], issuers: &[String]) -> Result<(), String> {
    if audiences.is_empty() {
        return Err(
            "JWT keys are configured but no accepted audience is set; configure POWERSYNC_RUST_JWT_AUDIENCES"
                .to_owned(),
        );
    }
    if issuers.is_empty() {
        return Err(
            "JWT keys are configured but no accepted issuer is set; configure POWERSYNC_RUST_JWT_ISSUERS"
                .to_owned(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use serde_json::json;

    fn signed_token(secret: &[u8], kid: Option<&str>, payload: Value) -> String {
        let header = if let Some(kid) = kid {
            json!({"alg": "HS256", "typ": "JWT", "kid": kid})
        } else {
            json!({"alg": "HS256", "typ": "JWT"})
        };
        let header_segment = URL_SAFE_NO_PAD.encode(header.to_string());
        let payload_segment = URL_SAFE_NO_PAD.encode(payload.to_string());
        let mut mac = HmacSha256::new_from_slice(secret).expect("secret should be valid");
        mac.update(format!("{header_segment}.{payload_segment}").as_bytes());
        let signature_segment = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{header_segment}.{payload_segment}.{signature_segment}")
    }

    #[test]
    fn api_auth_uses_bearer_and_token_prefixes() {
        let config = ApiAuthConfig {
            tokens: vec!["admin-token".to_owned()],
        };
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer admin-token".parse().unwrap());
        assert!(config.authorize_headers(&headers).is_ok());
        headers.insert("authorization", "Token admin-token".parse().unwrap());
        assert!(config.authorize_headers(&headers).is_ok());
    }

    #[test]
    fn user_auth_accepts_valid_hs256_token() {
        let config = UserAuthConfig {
            keys: vec![AuthKey::Hmac(HmacKey {
                kid: Some("kid-1".to_owned()),
                secret: b"super-secret".to_vec(),
            })],
            audiences: vec!["powersync".to_owned()],
            issuers: vec!["https://issuer.example".to_owned()],
        };
        let token = signed_token(
            b"super-secret",
            Some("kid-1"),
            json!({
                "sub": "user-1",
                "org_id": "org-1",
                "aud": "powersync",
                "iss": "https://issuer.example",
                "exp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 300,
            }),
        );

        let payload = config.verify_token(&token).expect("token should verify");
        assert_eq!(payload.user_id(), Some("user-1"));
        assert_eq!(
            payload.flattened_claim_strings().get("org_id"),
            Some(&"org-1".to_owned())
        );
    }

    #[test]
    fn user_auth_rejects_token_signed_with_wrong_secret() {
        let config = UserAuthConfig {
            keys: vec![AuthKey::Hmac(HmacKey {
                kid: None,
                secret: b"super-secret".to_vec(),
            })],
            audiences: Vec::new(),
            issuers: Vec::new(),
        };
        // Validly structured token, but signed with a secret the verifier does
        // not hold: the HMAC signature must fail closed.
        let token = signed_token(
            b"attacker-secret",
            None,
            json!({
                "sub": "user-1",
                "exp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 300,
            }),
        );
        assert_eq!(
            config.verify_token(&token).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn user_auth_rejects_alg_none_token() {
        let config = UserAuthConfig {
            keys: vec![AuthKey::Hmac(HmacKey {
                kid: None,
                secret: b"super-secret".to_vec(),
            })],
            audiences: Vec::new(),
            issuers: Vec::new(),
        };
        let header = URL_SAFE_NO_PAD.encode(json!({"alg": "none", "typ": "JWT"}).to_string());
        let payload = URL_SAFE_NO_PAD.encode(
            json!({
                "sub": "user-1",
                "exp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 300,
            })
            .to_string(),
        );
        // `alg: none` carries no signature and must never be accepted.
        let token = format!("{header}.{payload}.");
        assert_eq!(
            config.verify_token(&token).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn user_auth_rejects_wrong_audience_and_accepts_configured_audience() {
        let config = UserAuthConfig {
            keys: vec![AuthKey::Hmac(HmacKey {
                kid: None,
                secret: b"super-secret".to_vec(),
            })],
            audiences: vec!["powersync".to_owned()],
            issuers: Vec::new(),
        };
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;

        let wrong_aud = signed_token(
            b"super-secret",
            None,
            json!({"sub": "user-1", "aud": "someone-else", "exp": exp}),
        );
        assert_eq!(
            config.verify_token(&wrong_aud).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );

        let right_aud = signed_token(
            b"super-secret",
            None,
            json!({"sub": "user-1", "aud": "powersync", "exp": exp}),
        );
        assert!(config.verify_token(&right_aud).is_ok());
    }

    #[test]
    fn user_auth_requires_scoped_policy_and_exact_issuer() {
        assert!(UserAuthConfig::from_hs256_secrets(
            vec![(None, b"super-secret".to_vec())],
            Vec::new(),
            vec!["https://issuer.example".to_owned()],
        )
        .is_err());
        assert!(UserAuthConfig::from_hs256_secrets(
            vec![(None, b"super-secret".to_vec())],
            vec!["powersync".to_owned()],
            Vec::new(),
        )
        .is_err());

        let config = UserAuthConfig::from_hs256_secrets(
            vec![(None, b"super-secret".to_vec())],
            vec!["powersync".to_owned()],
            vec!["https://issuer.example".to_owned()],
        )
        .expect("scoped policy");
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        for claims in [
            json!({"sub": "user-1", "aud": "powersync", "exp": exp}),
            json!({"sub": "user-1", "aud": "powersync", "iss": 1, "exp": exp}),
            json!({"sub": "user-1", "aud": "powersync", "iss": "https://other.example", "exp": exp}),
        ] {
            let token = signed_token(b"super-secret", None, claims);
            assert_eq!(
                config.verify_token(&token).unwrap_err().status,
                StatusCode::UNAUTHORIZED
            );
        }
        let token = signed_token(
            b"super-secret",
            None,
            json!({
                "sub": "user-1",
                "aud": ["other", "powersync"],
                "iss": "https://issuer.example",
                "exp": exp
            }),
        );
        assert!(config.verify_token(&token).is_ok());
    }

    #[test]
    fn user_auth_rejects_expired_token() {
        let config = UserAuthConfig {
            keys: vec![AuthKey::Hmac(HmacKey {
                kid: None,
                secret: b"super-secret".to_vec(),
            })],
            audiences: Vec::new(),
            issuers: Vec::new(),
        };
        let token = signed_token(
            b"super-secret",
            None,
            json!({
                "sub": "user-1",
                "exp": 1,
            }),
        );

        assert_eq!(
            config.verify_token(&token).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn user_auth_rejects_token_without_exp() {
        let config = UserAuthConfig {
            keys: vec![AuthKey::Hmac(HmacKey {
                kid: None,
                secret: b"super-secret".to_vec(),
            })],
            audiences: Vec::new(),
            issuers: Vec::new(),
        };
        let token = signed_token(b"super-secret", None, json!({"sub": "user-1"}));

        assert_eq!(
            config.verify_token(&token).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn user_auth_honors_fractional_exp_instead_of_disabling_expiry() {
        let config = UserAuthConfig {
            keys: vec![AuthKey::Hmac(HmacKey {
                kid: None,
                secret: b"super-secret".to_vec(),
            })],
            audiences: Vec::new(),
            issuers: Vec::new(),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let expired = signed_token(
            b"super-secret",
            None,
            json!({"sub": "user-1", "exp": 1700000000.5}),
        );
        assert_eq!(
            config.verify_token(&expired).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );

        let valid = signed_token(
            b"super-secret",
            None,
            json!({"sub": "user-1", "exp": (now + 300) as f64 + 0.5}),
        );
        assert!(config.verify_token(&valid).is_ok());
    }

    #[test]
    fn user_auth_validates_nbf_with_leeway() {
        let config = UserAuthConfig {
            keys: vec![AuthKey::Hmac(HmacKey {
                kid: None,
                secret: b"super-secret".to_vec(),
            })],
            audiences: Vec::new(),
            issuers: Vec::new(),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let not_yet_valid = signed_token(
            b"super-secret",
            None,
            json!({"sub": "user-1", "exp": now + 600, "nbf": now + 300}),
        );
        assert_eq!(
            config.verify_token(&not_yet_valid).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );

        let within_leeway = signed_token(
            b"super-secret",
            None,
            json!({"sub": "user-1", "exp": now + 600, "nbf": now + 30}),
        );
        assert!(config.verify_token(&within_leeway).is_ok());

        let already_valid = signed_token(
            b"super-secret",
            None,
            json!({"sub": "user-1", "exp": now + 600, "nbf": now - 300}),
        );
        assert!(config.verify_token(&already_valid).is_ok());
    }

    #[test]
    fn api_auth_rejects_wrong_token_with_constant_time_compare() {
        let config = ApiAuthConfig {
            tokens: vec!["admin-token".to_owned()],
        };
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer admin-tokem".parse().unwrap());
        assert_eq!(
            config.authorize_headers(&headers).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );
        assert!(constant_time_token_eq("admin-token", "admin-token"));
        assert!(!constant_time_token_eq("admin-token", "admin-toke"));
    }

    #[test]
    fn parse_hmac_jwks_supports_object_wrapped_keys() {
        let jwks = json!({
            "keys": [
                {"kty": "oct", "alg": "HS256", "kid": "kid-1", "k": URL_SAFE_NO_PAD.encode("super-secret")}
            ]
        });
        let keys = parse_hmac_jwks(&jwks.to_string()).expect("jwks should parse");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].kid.as_deref(), Some("kid-1"));
        assert_eq!(keys[0].secret, b"super-secret".to_vec());
    }

    #[test]
    fn user_auth_accepts_valid_rs256_token_from_jwks() {
        const RS256_FIXTURE_TOKEN: &str = concat!(
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InJzYS0xIn0.",
            "eyJzdWIiOiJ1c2VyLTEiLCJhdWQiOiJkZXYtZml4dHVyZS1hcHAiLCJleHAiOjQxMDI0NDQ4MDB9.",
            "PF2i1VX6jE1FsyjEsrqM-eFc49pkOwNq1Xb5EDJIBkecUxayjA_ozhJ3-L3y7FCOvKezcApJQ3kzxBEckmbRBXLrbE-EiEjcZP9kqcCBdWtg3rgqD3ZkdflLm4umK1CaFUpA-JgO15eESq59KMP81ZJe3kpkKAS37-kM_bu0E_thQ_BYS9NlF1dc-_s7GGhc5UbNcGlq6yLYJBClikFfww31zCk6C4iYZCxrEtCxxEHpbPeahc2Y1VfeZ9MwtMwCg3J_lGaLG8YY3jVSlMZBerVKATsHq78HwO0LNzKdSQGUH1Ff9Gp5UcA09O0ULoPXnU7Y-uVE3_Pd97TgioZr_Q"
        );
        let jwks = json!({
            "keys": [{
                "kty": "RSA",
                "alg": "RS256",
                "kid": "rsa-1",
                "n": "x8BheuwzpATH42l0EIH68PQJUreVQR1w2TGEGHHZ-rOyBdZ5QTX2k8LwJZXPSnJxrsb34Ouphy6qNtXHXGQ-18lWU5vZa5HY7wJ7yR342IiRPqJwt-5SwbHsM2nBzCNk5ZjsgNSNWJppQhCbpCbmnowVTUf8Jpb4YdXQoScbyKfgle053QsSUI-iMzyWW7SWHf3V4LxjoGCqotJBTSG4R9s3U6Hbx8jVeW0VuTUOc0mrnttXM3OH6vPMXCB8tY5Qd82YTx_c3jUWW88I5S_dlY7nlLJMFGRYPjfEIYhK8O-r0htfK0MDscEp0EMmPsmmRfd7mXfdKI-osDuF2pgELw",
                "e": "AQAB"
            }]
        });
        let config = UserAuthConfig {
            keys: parse_jwks(&jwks.to_string()).expect("jwks should parse"),
            audiences: vec!["dev-fixture-app".to_owned()],
            issuers: Vec::new(),
        };

        let payload = config
            .verify_token(RS256_FIXTURE_TOKEN)
            .expect("token should verify");
        assert_eq!(payload.user_id(), Some("user-1"));
    }

    #[test]
    fn user_auth_from_sources_rejects_malformed_jwks_without_silently_disabling_auth() {
        let error = UserAuthConfig::from_sources(
            None,
            Some(
                json!({
                    "keys": [
                        {"kty": "RSA", "alg": "RS256", "kid": "rsa-1", "k": URL_SAFE_NO_PAD.encode("unused")}
                    ]
                })
                .to_string(),
            ),
            None,
            Vec::new(),
            Vec::new(),
        )
        .expect_err("unsupported JWKS should fail closed");

        assert!(error.contains("RSA jwk missing"));
    }

    #[test]
    fn remote_jwks_requires_https_outside_loopback() {
        for url in [
            "http://example.com/jwks.json",
            "http://10.0.0.1/jwks.json",
            "http://localhost.example/jwks.json",
            "ftp://example.com/jwks.json",
        ] {
            let error = validate_jwks_url(url).expect_err("insecure remote URL should fail");
            assert!(
                error.contains("must use HTTPS"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn user_auth_rejects_insecure_remote_jwks_before_fetching() {
        let error = UserAuthConfig::from_sources(
            None,
            None,
            Some("http://example.com/jwks.json".to_owned()),
            Vec::new(),
            Vec::new(),
        )
        .expect_err("insecure remote JWKS configuration should fail at startup");

        assert!(error.contains("must use HTTPS"));
    }

    #[test]
    fn remote_jwks_accepts_https_and_explicit_loopback_http() {
        for url in [
            "https://example.com/jwks.json",
            "http://localhost:8080/jwks.json",
            "http://127.0.0.1:8080/jwks.json",
            "http://127.255.255.254/jwks.json",
            "http://[::1]:8080/jwks.json",
        ] {
            validate_jwks_url(url).expect("allowed JWKS URL should validate");
        }
    }

    #[test]
    fn remote_jwks_redirects_cannot_downgrade_remote_https_to_loopback_http() {
        let remote_https = validate_jwks_url("https://example.com/jwks.json").unwrap();
        let loopback_http = validate_jwks_url("http://127.0.0.1:8080/jwks.json").unwrap();
        let other_loopback_http = validate_jwks_url("http://localhost:8081/jwks.json").unwrap();
        let redirect_https = validate_jwks_url("https://keys.example.com/jwks.json").unwrap();

        assert!(!jwks_redirect_transport_allowed(
            &loopback_http,
            jwks_url_is_loopback_http(&remote_https)
        ));
        assert!(jwks_redirect_transport_allowed(
            &other_loopback_http,
            jwks_url_is_loopback_http(&loopback_http)
        ));
        assert!(jwks_redirect_transport_allowed(
            &redirect_https,
            jwks_url_is_loopback_http(&remote_https)
        ));
    }

    #[test]
    fn remote_jwks_rejects_malformed_or_hostless_urls() {
        for url in ["not a URL", "https://", "file:///tmp/jwks.json"] {
            assert!(validate_jwks_url(url).is_err(), "{url} should fail");
        }
    }
}
