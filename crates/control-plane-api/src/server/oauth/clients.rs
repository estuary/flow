//! Public OAuth clients identify themselves with HTTPS metadata documents.
use super::{Error, Result};

#[derive(Clone, serde::Deserialize)]
pub struct Client {
    pub client_id: String,
    pub client_name: String,
    pub redirect_uris: Vec<String>,
    #[serde(default = "public_auth")]
    pub token_endpoint_auth_method: String,
}

fn public_auth() -> String {
    "none".to_string()
}

pub async fn resolve(client_id: &str, configured: &[Client]) -> Result<Client> {
    if let Some(client) = configured.iter().find(|c| c.client_id == client_id) {
        return Ok(client.clone());
    }
    let url = url::Url::parse(client_id).map_err(|_| Error::invalid("invalid client_id"))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() == "/"
        || url.port_or_known_default() != Some(443)
    {
        return Err(Error::invalid("client_id must be an HTTPS metadata URL"));
    }
    let host = url.host_str().unwrap();
    let addresses: Vec<_> = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::lookup_host((host, 443)),
    )
    .await
    .map_err(|_| Error::invalid("client metadata DNS timeout"))?
    .map_err(|_| Error::invalid("client metadata DNS failed"))?
    .collect();
    if addresses.is_empty() || addresses.iter().any(|a| !public_ip(a.ip())) {
        return Err(Error::invalid(
            "client metadata must resolve to public addresses",
        ));
    }
    // Pin the addresses we checked. Disable proxies and redirects so neither
    // can bypass the SSRF boundary or move the request to a different host.
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(10))
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(|_| Error::internal())?;
    let mut response = http
        .get(url)
        .send()
        .await
        .map_err(|_| Error::invalid("client metadata fetch failed"))?;
    if !response.status().is_success() {
        return Err(Error::invalid("client metadata fetch failed"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::invalid("client metadata read failed"))?
    {
        if bytes.len() + chunk.len() > 16 * 1024 {
            return Err(Error::invalid("client metadata exceeds 16 KiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    let client: Client =
        serde_json::from_slice(&bytes).map_err(|_| Error::invalid("invalid client metadata"))?;
    if client.client_id != client_id {
        return Err(Error::invalid("metadata client_id mismatch"));
    }
    validate(&client)?;
    Ok(client)
}

pub fn validate(client: &Client) -> Result<()> {
    if client.client_id.is_empty()
        || client.client_id.len() > 2048
        || client.client_name.is_empty()
        || client.client_name.len() > 256
        || client.token_endpoint_auth_method != "none"
        || client.redirect_uris.is_empty()
        || client.redirect_uris.len() > 32
        || client
            .redirect_uris
            .iter()
            .any(|uri| redirect_url(uri).is_none())
    {
        return Err(Error::invalid("invalid public client metadata"));
    }
    Ok(())
}

fn redirect_url(uri: &str) -> Option<url::Url> {
    if uri.len() > 2048 {
        return None;
    }
    let url = url::Url::parse(uri).ok()?;
    if url.host_str().is_none()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || !(url.scheme() == "https" || (url.scheme() == "http" && loopback(&url)))
    {
        return None;
    }
    // These names are reserved for the authorization response.
    if url
        .query_pairs()
        .any(|(key, _)| matches!(key.as_ref(), "code" | "state" | "error" | "iss"))
    {
        return None;
    }
    Some(url)
}

fn loopback(url: &url::Url) -> bool {
    matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
}

pub fn redirect_allowed(requested: &str, client: &Client) -> bool {
    if redirect_url(requested).is_none() {
        return false;
    }
    client.redirect_uris.iter().any(|registered| {
        requested == registered
            || portless_loopback(requested)
                .zip(portless_loopback(registered))
                .is_some_and(|(a, b)| a == b)
    })
}

// Compare original bytes except the loopback port. URL parsing normalizes
// dot-segments and escapes, which must not broaden registered redirect paths.
fn portless_loopback(uri: &str) -> Option<String> {
    let url = redirect_url(uri)?;
    if url.scheme() != "http" || !loopback(&url) {
        return None;
    }
    let rest = uri.strip_prefix("http://")?;
    let boundary = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, suffix) = rest.split_at(boundary);
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => host,
        _ => authority,
    };
    Some(format!("http://{host}{suffix}"))
}

fn public_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && (b == 0 || b == 168 || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        std::net::IpAddr::V6(ip) => {
            let s = ip.segments();
            // Only global unicast, excluding transition and special-purpose ranges.
            (s[0] & 0xe000) == 0x2000
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn metadata_and_redirect_boundaries() {
        let client = super::Client {
            client_id: "test".into(),
            client_name: "Test".into(),
            redirect_uris: vec![
                "http://127.0.0.1/callback".into(),
                "https://example.test/cb".into(),
            ],
            token_endpoint_auth_method: "none".into(),
        };
        assert!(super::validate(&client).is_ok());
        for uri in ["http://127.0.0.1:1234/callback", "https://example.test/cb"] {
            assert!(super::redirect_allowed(uri, &client));
        }
        for uri in [
            "http://localhost:1234/callback",
            "http://127.0.0.1:1234/other",
            "http://127.0.0.1:1234/other/../callback",
            "https://example.test:444/cb",
            "https://example.test/cb#token",
            "https://evil.test/cb",
        ] {
            assert!(!super::redirect_allowed(uri, &client));
        }
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "fc00::1",
            "2001:db8::1",
            "2002:7f00:1::",
        ] {
            assert!(!super::public_ip(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["8.8.8.8", "2606:4700:4700::1111"] {
            assert!(super::public_ip(ip.parse().unwrap()));
        }
    }
}
