use crate::Error;
use broker::process_spec::Id as MemberId;
use proto_gazette::broker;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

// DialState represents a Channel which may be:
// - Ready (if Some)
// - Currently being dialed (if locked)
// - Neither (None and not locked).
// Ready channels also track their number of uses since the last sweep.
type DialState = Arc<futures::lock::Mutex<Option<(Channel, usize)>>>;

pub struct Router {
    states: std::sync::Mutex<HashMap<MemberId, DialState>>,
    service_endpoint: String,
    zone: String,
}

impl Router {
    pub fn new(endpoint: &str, zone: &str) -> Result<Self, Error> {
        let (endpoint, zone) = (endpoint.to_string(), zone.to_string());

        let _endpoint = tonic::transport::Endpoint::from_shared(endpoint.clone())
            .map_err(|_err| Error::InvalidEndpoint(endpoint.clone()))?;

        Ok(Self {
            states: Default::default(),
            service_endpoint: endpoint,
            zone,
        })
    }

    pub async fn route(
        &self,
        route: Option<&broker::Route>,
        primary: bool,
    ) -> Result<Channel, Error> {
        let (index, state) = self.pick(route, primary);

        // Acquire `id`-specific, async-aware lock.
        let mut state = state.lock().await;

        // Fast path: client is dialed and ready.
        if let Some((ref client, uses)) = &mut *state {
            *uses += 1;
            return Ok(client.clone());
        }

        // Slow path: start dialing the endpoint.
        let endpoint_str = match index {
            Some(index) => &route.unwrap().endpoints[index],
            None => &self.service_endpoint,
        };
        let endpoint = tonic::transport::Endpoint::from_shared(endpoint_str.clone())
            .map_err(|_err| Error::InvalidEndpoint(endpoint_str.clone()))?
            .connect_timeout(std::time::Duration::from_secs(5));

        let channel = match endpoint.uri().scheme_str() {
            Some("unix") => {
                endpoint
                    .connect_with_connector(tower::util::service_fn(
                        |uri: tonic::transport::Uri| connect_unix(uri),
                    ))
                    .await?
            }
            Some("https") => {
                endpoint
                    .tls_config(
                        tonic::transport::ClientTlsConfig::new()
                            .with_native_roots()
                            .assume_http2(true),
                    )?
                    .connect()
                    .await?
            }
            Some("http") => endpoint.connect().await?,

            _ => return Err(Error::InvalidEndpoint(endpoint_str.to_owned())),
        };

        *state = Some((channel.clone(), 1));

        Ok(channel)
    }

    fn pick(&self, route: Option<&broker::Route>, primary: bool) -> (Option<usize>, DialState) {
        // Acquire non-async lock which *cannot* be held across an await point.
        let mut states = self.states.lock().unwrap();
        let index = pick(route, primary, &self.zone, &states);

        let default_id = MemberId::default();

        let id = match index {
            Some(index) => &route.unwrap().members[index],
            None => &default_id,
        };

        let state = match states.get(id) {
            Some(value) => value.clone(),
            None => states.entry(id.clone()).or_default().clone(),
        };

        (index, state)
    }

    pub fn sweep(&self) {
        let mut states = self.states.lock().unwrap();

        states.retain(|id, state| {
            // Retain entries which are currently connecting.
            let Some(mut state) = state.try_lock() else {
                return true;
            };
            // Drop entries which are not connected.
            let Some((_client, uses)) = &mut *state else {
                return false;
            };
            // Drop entries which have not been used since the last sweep.
            if *uses == 0 {
                tracing::debug!(?id, "dropping idle member connection");
                return false;
            }
            *uses = 0; // Mark for next sweep.
            true
        });
    }
}

pub(crate) async fn connect_unix(
    uri: tonic::transport::Uri,
) -> std::io::Result<hyper_util::rt::TokioIo<tokio::net::UnixStream>> {
    let path = uri.path();
    // Wait until the filesystem path exists, because it's hard to tell from
    // the error so that we can re-try. This is expected to be cut short by the
    // connection timeout if the path never appears.
    for i in 1.. {
        if let Ok(meta) = tokio::fs::metadata(path).await {
            tracing::debug!(?path, ?meta, "UDS path now exists");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20 * i)).await;
    }
    Ok(hyper_util::rt::TokioIo::new(
        tokio::net::UnixStream::connect(path).await?,
    ))
}

fn pick(
    route: Option<&broker::Route>,
    primary: bool,
    zone: &str,
    states: &HashMap<MemberId, DialState>,
) -> Option<usize> {
    let default_route = broker::Route::default();
    let route = route.unwrap_or(&default_route);

    route
        .members
        .iter()
        .zip(route.endpoints.iter())
        .enumerate()
        .max_by_key(|(index, (id, _endpoint))| {
            let connected = if let Some(state) = states.get(id) {
                if let Some(state) = state.try_lock() {
                    if let Some(_conn) = state.as_ref() {
                        true // Transport is ready.
                    } else {
                        false // Transport is not ready and no task is starting it.
                    }
                } else {
                    true // Another task has started this transport.
                }
            } else {
                false // Transport has not been started.
            };

            // Member selection criteria:
            (
                // If we want the primary, then prefer the primary.
                primary && *index as i32 == route.primary,
                // Prefer members in our same zone.
                zone == id.zone,
                // Prefer members which are already connected.
                connected,
            )
        })
        .map(|(index, _)| index)
}
