//! Public-client registration and bounded, DNS-pinned metadata retrieval.

#[derive(Clone, serde::Deserialize)]
pub struct Config {
    /// A canonical HTTPS origin, without an issuer path.
    pub issuer: url::Url,
    pub dashboard: url::Url,
    #[serde(default)]
    pub clients: std::collections::BTreeMap<String, Metadata>,
}

#[derive(Clone, serde::Deserialize)]
pub struct Metadata {
    pub client_id: String,
    pub client_name: String,
    pub redirect_uris: Vec<String>,
    pub token_endpoint_auth_method: String,
    pub grant_types: Vec<String>,
    pub response_types: Vec<String>,
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            secure_url(&self.issuer) && self.issuer.path() == "/" && self.issuer.query().is_none(),
            "OAuth issuer must be an HTTPS origin"
        );
        anyhow::ensure!(
            secure_url(&self.dashboard) && self.dashboard.query().is_none(),
            "OAuth dashboard must be an HTTPS URL without query or fragment"
        );
        for (id, metadata) in &self.clients {
            anyhow::ensure!(metadata.valid(id), "invalid OAuth client metadata for {id}");
        }
        Ok(())
    }

    pub fn resource(&self) -> &str {
        self.issuer.as_str().trim_end_matches('/')
    }
}

fn secure_url(url: &url::Url) -> bool {
    url.scheme() == "https"
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}

impl Metadata {
    fn valid(&self, id: &str) -> bool {
        self.client_id == id
            && !id.is_empty()
            && id.len() <= 2048
            && !self.client_name.is_empty()
            && self.client_name.len() <= 256
            && self.token_endpoint_auth_method == "none"
            && self.grant_types.iter().any(|g| g == "authorization_code")
            && self
                .grant_types
                .iter()
                .all(|g| matches!(g.as_str(), "authorization_code" | "refresh_token"))
            && self.response_types == ["code"]
            && !self.redirect_uris.is_empty()
            && self.redirect_uris.len() <= 20
            && self.redirect_uris.iter().all(|r| valid_redirect(r))
    }
}

fn valid_redirect(raw: &str) -> bool {
    let Ok(url) = url::Url::parse(raw) else {
        return false;
    };
    if raw.len() > 2048
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return false;
    }
    // Exact registered loopback URIs are allowed for native public clients.
    // Wildcards, dynamic ports, custom schemes and localhost DNS are not supported.
    secure_url(&url)
        || (url.scheme() == "http"
            && matches!(url.host(),
        Some(url::Host::Ipv4(ip)) if ip.is_loopback())
            || url.scheme() == "http"
                && matches!(url.host(), Some(url::Host::Ipv6(ip)) if ip.is_loopback()))
}

pub(super) async fn resolve(config: &Config, id: &str) -> Result<Metadata, super::Error> {
    if let Some(metadata) = config.clients.get(id) {
        return Ok(metadata.clone());
    }
    let url = url::Url::parse(id).map_err(|_| super::Error::invalid_client())?;
    if id.len() > 2048
        || !secure_url(&url)
        || url.path() == "/"
        || url.query().is_some()
        || url.port_or_known_default() != Some(443)
    {
        return Err(super::Error::invalid_client());
    }
    static FETCHES: std::sync::LazyLock<tokio::sync::Semaphore> =
        std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(16));
    let _permit = FETCHES
        .try_acquire()
        .map_err(|_| super::Error::invalid_client())?;
    tokio::time::timeout(std::time::Duration::from_secs(5), fetch(url, id))
        .await
        .map_err(|_| super::Error::invalid_client())?
}

async fn fetch(url: url::Url, id: &str) -> Result<Metadata, super::Error> {
    let host = url.host_str().ok_or_else(super::Error::invalid_client)?;
    let addrs: Vec<_> = tokio::net::lookup_host((host, 443))
        .await
        .map_err(|_| super::Error::invalid_client())?
        .collect();
    if addrs.is_empty() || addrs.iter().any(|a| !public_ip(a.ip())) {
        return Err(super::Error::invalid_client());
    }
    // Pin vetted addresses so a second DNS lookup cannot rebind to a private
    // service. Disable proxies and redirects, both of which could bypass this.
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(host, &addrs)
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|_| super::Error::invalid_client())?;
    let mut response = client
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|_| super::Error::invalid_client())?;
    if response.status() != reqwest::StatusCode::OK
        || response.content_length().is_some_and(|n| n > 65536)
        || !response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .is_some_and(|h| h.split(';').next().unwrap().trim() == "application/json")
    {
        return Err(super::Error::invalid_client());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| super::Error::invalid_client())?
    {
        if bytes.len() + chunk.len() > 65536 {
            return Err(super::Error::invalid_client());
        }
        bytes.extend_from_slice(&chunk);
    }
    let metadata: Metadata =
        serde_json::from_slice(&bytes).map_err(|_| super::Error::invalid_client())?;
    if !metadata.valid(id) {
        return Err(super::Error::invalid_client());
    }
    Ok(metadata)
}

fn public_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(matches!(a, 0 | 10 | 127 | 224..=255)
                || a == 100 && (64..=127).contains(&b)
                || a == 169 && b == 254
                || a == 172 && (16..=31).contains(&b)
                || a == 192 && (b == 0 || b == 168 || b == 88 && c == 99)
                || a == 198 && (b == 18 || b == 19 || b == 51 && c == 100)
                || a == 203 && b == 0 && c == 113)
        }
        std::net::IpAddr::V6(ip) => {
            let s = ip.segments();
            // Only global unicast; exclude transition, documentation and special
            // assignments (including Teredo, mapped IPv4 and NAT64).
            s[0] & 0xe000 == 0x2000
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn metadata_security_boundaries() {
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "192.0.0.1",
            "198.18.0.1",
            "::1",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::1",
            "2001:db8::1",
        ] {
            assert!(!super::public_ip(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["8.8.8.8", "2606:4700:4700::1111"] {
            assert!(super::public_ip(ip.parse().unwrap()));
        }
        for uri in [
            "https://client.example/callback",
            "http://127.0.0.1:8080/callback",
            "http://[::1]:8080/",
        ] {
            assert!(super::valid_redirect(uri));
        }
        for uri in [
            "https://client.example/#fragment",
            "https://user@client.example/cb",
            "http://localhost/cb",
            "javascript:alert(1)",
        ] {
            assert!(!super::valid_redirect(uri));
        }
    }
}
