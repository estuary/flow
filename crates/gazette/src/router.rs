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

/// Router facilitates dispatching requests to designated members of
/// a dynamic serving topology, by maintaining ready Channels to
/// member endpoints which may be dynamically discovered over time.
#[derive(Clone)]
pub struct Router {
    inner: Arc<Inner>,
}
struct Inner {
    states: std::sync::Mutex<HashMap<MemberId, DialState>>,
    zone: String,
}

impl Router {
    /// Create a new Router with the given default service endpoint,
    /// which prefers to route to members in `zone` where possible.
    pub fn new(zone: &str) -> Self {
        let zone = zone.to_string();

        Self {
            inner: Arc::new(Inner {
                states: Default::default(),
                zone,
            }),
        }
    }

    /// Map an optional broker::Route and indication of whether the "primary"
    /// member is required into a ready Channel for use in the dispatch of an RPC.
    ///
    /// route() will prefer to send requests to a ready member Channel if possible,
    /// or will dial new Channels if required by the `route` and `primary` requirement.
    pub async fn route(
        &self,
        route: Option<&broker::Route>,
        primary: bool,
        default: &MemberId,
    ) -> Result<Channel, Error> {
        let (index, state) = self.pick(route, primary, &default);

        // Acquire MemberId-specific, async-aware lock.
        let mut state = state.lock().await;

        // Fast path: client is dialed and ready.
        if let Some((ref client, uses)) = &mut *state {
            *uses += 1;
            return Ok(client.clone());
        }

        // Slow path: start dialing the endpoint.
        let channel = super::dial_channel(match index {
            Some(index) => &route.unwrap().endpoints[index],
            None => &default.suffix,
        })
        .await?;

        *state = Some((channel.clone(), 1));

        Ok(channel)
    }

    fn pick(
        &self,
        route: Option<&broker::Route>,
        primary: bool,
        default: &MemberId,
    ) -> (Option<usize>, DialState) {
        // Acquire non-async lock which *cannot* be held across an await point.
        let mut states = self.inner.states.lock().unwrap();
        let index = pick(route, primary, &self.inner.zone, &states);

        let id = match index {
            Some(index) => &route.unwrap().members[index],
            None => default,
        };

        let state = match states.get(id) {
            Some(value) => value.clone(),
            None => states.entry(id.clone()).or_default().clone(),
        };

        (index, state)
    }

    // Identify Channels which have not been used since the preceding sweep, and close them.
    // As members come and go, Channels may no longer needed.
    // Call sweep() periodically to clear them out.
    pub fn sweep(&self) {
        let mut states = self.inner.states.lock().unwrap();

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
