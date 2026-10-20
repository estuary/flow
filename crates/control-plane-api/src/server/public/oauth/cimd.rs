//! Client ID Metadata Documents identify clients without a registration database.
//! The client's `client_id` is an HTTPS URL serving its OAuth metadata.
//!
//! Fetching a caller-chosen URL is an SSRF primitive. `assert_safe_target` rejects
//! non-public addresses, and the fetch pins those addresses and refuses redirects.
//! Client names are untrusted text: the consent UI must also show the hostname
//! of the client ID, whose ownership is established by DNS and TLS.

use super::Error;

// Bound streaming responses so a metadata endpoint cannot exhaust memory.
const MAX_DOCUMENT_BYTES: usize = 5 * 1024;
const FETCH_TIMEOUT_SECONDS: u64 = 10;

#[derive(serde::Deserialize)]
pub(super) struct ClientMetadata {
    pub client_id: String,
    pub client_name: String,
    pub redirect_uris: Vec<String>,
}

// Reject embedded credentials that could make an attacker-controlled hostname
// look like a trusted client, and require a document path without query state.
fn validate_client_id_url(client_id: &str) -> Result<url::Url, Error> {
    let url = url::Url::parse(client_id)
        .map_err(|_| Error("invalid_client", "client_id must be a valid URL"))?;
    // URL parsing normalizes dot segments, so check the original path as well.
    let path = client_id
        .split(['?', '#'])
        .next()
        .unwrap()
        .split_once("://")
        .and_then(|(_, rest)| rest.split_once('/'))
        .map(|(_, path)| path)
        .unwrap_or("");
    if url.scheme() != "https"
        || url.host_str().is_none()
        || path.is_empty()
        || path.split('/').any(|segment| matches!(segment, "." | ".."))
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error(
            "invalid_client",
            "client_id must be an HTTPS document URL without credentials, query, fragment, or dot segments",
        ));
    }
    Ok(url)
}

/// Network IO is bounded here; document parsing is handled by `parse_client_metadata`.
pub(super) async fn fetch_client_metadata(client_id: &str) -> Result<ClientMetadata, Error> {
    tokio::time::timeout(
        std::time::Duration::from_secs(FETCH_TIMEOUT_SECONDS),
        fetch_client_metadata_inner(client_id),
    )
    .await
    .map_err(|_| Error("invalid_client", "client metadata fetch timed out"))?
}

async fn fetch_client_metadata_inner(client_id: &str) -> Result<ClientMetadata, Error> {
    let url = validate_client_id_url(client_id)?;
    let host = url.host_str().unwrap();
    let addresses = assert_safe_target(&url).await?;
    // Pin the validated DNS answer to prevent rebinding. Redirects and ambient
    // proxies must not introduce a second, unvalidated destination.
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(super::internal)?;
    let mut response = http
        .get(url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|_| Error("invalid_client", "could not fetch client metadata"))?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(Error(
            "invalid_client",
            "client metadata endpoint must return HTTP 200",
        ));
    }
    // Enforce the cap during streaming, including responses without Content-Length.
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error("invalid_client", "could not read client metadata"))?
    {
        if bytes.len() + chunk.len() > MAX_DOCUMENT_BYTES {
            return Err(Error(
                "invalid_client",
                "client metadata exceeds the 5 KiB limit",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    parse_client_metadata(client_id, &bytes)
}

fn parse_client_metadata(client_id: &str, body: &[u8]) -> Result<ClientMetadata, Error> {
    let document: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| Error("invalid_client", "client metadata must be valid JSON"))?;
    // Public metadata must not publish a client secret, even as a null value.
    if document.get("client_secret").is_some() {
        return Err(Error(
            "invalid_client",
            "client metadata must not contain client_secret",
        ));
    }
    let mut client: ClientMetadata =
        serde_json::from_value(document).map_err(|_| Error("invalid_client", "client metadata must contain string client_id and client_name fields and a redirect_uris string list"))?;
    client.client_name = client.client_name.trim().to_owned();
    // Exact self-reference binds the document to the URL that served it.
    if client.client_id != client_id
        || client.client_name.is_empty()
        || client.redirect_uris.is_empty()
    {
        return Err(Error(
            "invalid_client",
            "client metadata must name the requested client_id and have a nonblank name and nonempty redirect_uris",
        ));
    }
    Ok(client)
}

/// Match registered callbacks exactly, except for loopback hosts and ports.
/// Native clients choose a free port when starting authorization, so their
/// callback port cannot be fixed in the metadata document.
pub(super) fn assert_redirect_uri_allowed(
    requested: &str,
    registered: &[String],
) -> Result<(), Error> {
    let url = validate_redirect_uri(requested)?;
    if registered
        .iter()
        .any(|candidate| requested == candidate || loopback_match(&url, candidate))
    {
        return Ok(());
    }
    Err(Error(
        "invalid_request",
        "redirect_uri does not match the client metadata",
    ))
}

// Metadata is client-controlled, so membership alone cannot authorize browser
// navigation. In particular, executable schemes can run in the dashboard origin.
pub(super) fn validate_redirect_uri(uri: &str) -> Result<url::Url, Error> {
    let url = url::Url::parse(uri)
        .map_err(|_| Error("invalid_request", "redirect_uri must be a valid URL"))?;
    if !(url.scheme() == "https" || (url.scheme() == "http" && is_loopback_host(url.host())))
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error(
            "invalid_request",
            "redirect_uri must use HTTPS (or HTTP for loopback) without credentials or a fragment",
        ));
    }
    Ok(url)
}

fn loopback_match(requested: &url::Url, candidate: &str) -> bool {
    let Ok(candidate) = url::Url::parse(candidate) else {
        return false;
    };
    is_loopback_host(requested.host())
        && is_loopback_host(candidate.host())
        && requested.scheme() == candidate.scheme()
        && requested.path() == candidate.path()
        && requested.query() == candidate.query()
}

pub(super) fn is_loopback_host(host: Option<url::Host<&str>>) -> bool {
    match host {
        Some(url::Host::Domain("localhost")) => true,
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    }
}

/// Anyone can choose a client ID, so refuse destinations inside the network perimeter.
/// Return the checked addresses for pinning: a second DNS lookup could yield a
/// different destination and bypass this check.
async fn assert_safe_target(url: &url::Url) -> Result<Vec<std::net::SocketAddr>, Error> {
    let host = url.host_str().unwrap();
    let addresses = tokio::net::lookup_host((host, url.port_or_known_default().unwrap()))
        .await
        .map_err(|_| Error("invalid_client", "client_id hostname could not be resolved"))?
        .collect::<Vec<_>>();
    if addresses.is_empty() || addresses.iter().any(|a| !public_ip(a.ip())) {
        return Err(Error(
            "invalid_client",
            "client_id must resolve only to public addresses",
        ));
    }
    Ok(addresses)
}

/// Only globally routable unicast destinations are eligible for metadata fetches.
/// IPv4-mapped IPv6, transition ranges, and special-purpose ranges fail closed.
fn public_ip(ip: std::net::IpAddr) -> bool {
    if ip.is_multicast() {
        return false;
    }
    match ip {
        std::net::IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 240
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && (b == 0 || b == 168 || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        std::net::IpAddr::V6(ip) => {
            let s = ip.segments();
            (s[0] & 0xe000) == 0x2000
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}
