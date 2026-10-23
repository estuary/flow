//! Client ID Metadata Documents (CIMD) identify clients without a registration database.
//! The client's `client_id` is an HTTPS URL serving its OAuth metadata.

use super::Error;

const MAX_DOCUMENT_BYTES: usize = 5 * 1024;
const FETCH_TIMEOUT_SECONDS: u64 = 10;

// Copied from crates/egress on daveg/libkrun-sandbox.
// TODO: Use egress::baseline(&[]) when that crate lands.
const BASELINE: &[&str] = &[
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.0.0.0/24",
    "192.0.2.0/24",
    "192.88.99.0/24",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "198.51.100.0/24",
    "203.0.113.0/24",
    "224.0.0.0/4",
    "240.0.0.0/4",
];

fn baseline() -> Vec<ipnetwork::Ipv4Network> {
    BASELINE
        .iter()
        .map(|raw| raw.parse().expect("BASELINE entries are constants"))
        .collect()
}

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
    // Require the supplied identity to match the parser's serialized URL so
    // displayed client IDs and fetched URLs cannot differ through normalization.
    if url.scheme() != "https"
        || url.host_str().is_none()
        || matches!(url.host(), Some(url::Host::Ipv6(_)))
        || url.path() == "/"
        || url.as_str() != client_id
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error(
            "invalid_client",
            "client_id must be a canonical HTTPS URL with a non-root path and without credentials, query, fragment, or an IPv6 host",
        ));
    }
    Ok(url)
}

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
    let http = reqwest::Client::builder()
        // Redirects and proxies are rejected to protect against unvalidated destinations.
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        // Pin the validated DNS answer to prevent rebinding.
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
    // Limit bytes read even when Content-Length is absent.
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
    let mut client: ClientMetadata =
        serde_json::from_slice(body).map_err(|_| Error("invalid_client", "client metadata must contain string client_id and client_name fields and a redirect_uris string list"))?;
    client.client_name = client.client_name.trim().to_owned();
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

/// Match registered callbacks, allowing loopback hosts and ports to vary.
/// Native clients may choose an available callback port at runtime.
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

// Reject unsafe callbacks, e.g. client-controlled metadata could supply a javascript: URL.
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

/// Return validated addresses that the caller must pin to the hostname to avoid unchecked DNS results.
async fn assert_safe_target(url: &url::Url) -> Result<Vec<std::net::SocketAddr>, Error> {
    let host = url.host_str().unwrap();
    let addresses = tokio::net::lookup_host((host, url.port_or_known_default().unwrap()))
        .await
        .map_err(|_| Error("invalid_client", "client_id hostname could not be resolved"))?;
    ipv4_targets(addresses)
}

fn ipv4_targets(
    addresses: impl IntoIterator<Item = std::net::SocketAddr>,
) -> Result<Vec<std::net::SocketAddr>, Error> {
    // Ignore IPv6 answers so hosts with both IPv4 and IPv6 remain usable.
    let addresses = addresses
        .into_iter()
        .filter_map(|address| match address {
            std::net::SocketAddr::V4(address) => Some(address),
            std::net::SocketAddr::V6(_) => None,
        })
        .collect::<Vec<_>>();
    let baseline = baseline();
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|a| baseline.iter().any(|network| network.contains(*a.ip())))
    {
        return Err(Error(
            "invalid_client",
            "client_id must resolve to IPv4 addresses, all of which must be public",
        ));
    }
    Ok(addresses
        .into_iter()
        .map(std::net::SocketAddr::V4)
        .collect())
}

#[cfg(test)]
mod tests {
    #[test]
    fn client_urls_and_addresses() {
        assert!(super::validate_client_id_url("https://client.example.test/metadata.json").is_ok());
        for url in [
            "http://client.example.test/metadata.json",
            "https://client.example.test/",
            "https://user@client.example.test/metadata.json",
            "https://client.example.test/metadata.json?query",
            "https://client.example.test/metadata.json#fragment",
            "https://CLIENT.example.test/metadata.json",
            "https://[::1]/metadata.json",
        ] {
            assert!(super::validate_client_id_url(url).is_err(), "{url}");
        }
        for (addresses, allowed) in [
            (vec!["8.8.8.8:443"], true),
            (vec!["8.8.8.8:443", "[2606:4700:4700::1111]:443"], true),
            (vec!["[2606:4700:4700::1111]:443"], false),
            (vec!["8.8.8.8:443", "127.0.0.1:443"], false),
            (vec!["10.0.0.1:443"], false),
            (vec!["169.254.169.254:443"], false),
        ] {
            assert_eq!(
                super::ipv4_targets(addresses.iter().map(|a| a.parse().unwrap())).is_ok(),
                allowed,
                "{addresses:?}",
            );
        }
    }

    #[test]
    fn callbacks() {
        let registered = vec![
            "https://client.example.test/callback?fixed=1".into(),
            "http://localhost:8000/callback?fixed=1".into(),
            "javascript:alert(1)".into(),
        ];
        for (uri, allowed) in [
            ("https://client.example.test/callback?fixed=1", true),
            ("https://other.example.test/callback?fixed=1", false),
            ("https://client.example.test/callback?fixed=2", false),
            ("http://127.0.0.1:9000/callback?fixed=1", true),
            ("http://[::1]:9000/callback?fixed=1", true),
            ("http://localhost:9000/other?fixed=1", false),
            ("http://localhost:9000/callback?fixed=2", false),
            ("http://other.example.test:9000/callback?fixed=1", false),
            ("javascript:alert(1)", false),
            ("https://user@client.example.test/callback?fixed=1", false),
            (
                "https://client.example.test/callback?fixed=1#fragment",
                false,
            ),
        ] {
            assert_eq!(
                super::assert_redirect_uri_allowed(uri, &registered).is_ok(),
                allowed,
                "{uri}"
            );
        }
    }
}
