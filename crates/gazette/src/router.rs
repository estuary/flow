use crate::Error;
use broker::process_spec::Id as MemberId;
use proto_gazette::broker;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

/// Router facilitates dispatching requests to designated members of
/// a dynamic serving topology, by maintaining ready Channels to
/// member endpoints which may be dynamically discovered over time.
#[derive(Clone)]
pub struct Router {
    inner: Arc<Inner>,
}
struct Inner {
    states: std::sync::Mutex<HashMap<MemberId, (Channel, bool)>>,
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
    /// member is required into a ready Channel for use in the dispatch of an RPC,
    /// and a boolean which is set if and only if the Channel is in our local zone.
    ///
    /// route() dial new Channels as required by the `route` and `primary` requirement.
    /// Use sweep() to periodically clean up Channels which are no longer in use.
    pub fn route(
        &self,
        route: Option<&broker::Route>,
        primary: bool,
        default: &MemberId,
    ) -> Result<(Channel, bool), Error> {
        let index = pick(route, primary, &self.inner.zone);

        let id = match index {
            Some(index) => &route.unwrap().members[index],
            None => default,
        };
        let mut states = self.inner.states.lock().unwrap();

        // Is the channel already started?
        if let Some((channel, mark)) = states.get_mut(id) {
            *mark = true;
            return Ok((channel.clone(), id.zone == self.inner.zone));
        }

        // Start dialing the endpoint.
        let channel = super::dial_channel(match index {
            Some(index) => &route.unwrap().endpoints[index],
            None => &default.suffix,
        })?;
        states.insert(id.clone(), (channel.clone(), true));

        Ok((channel, id.zone == self.inner.zone))
    }

    // Identify Channels which have not been used since the preceding sweep, and close them.
    // As members come and go, Channels may no longer needed.
    // Call sweep() periodically to clear them out.
    pub fn sweep(&self) {
        let mut states = self.inner.states.lock().unwrap();

        states.retain(|id, (_channel, mark)| {
            // Drop entries which have not been used since the last sweep.
            if !*mark {
                tracing::debug!(?id, "dropping idle member connection");
                return false;
            }
            *mark = false; // Mark for next sweep.
            true
        });
    }
}

fn pick(route: Option<&broker::Route>, primary: bool, zone: &str) -> Option<usize> {
    let default_route = broker::Route::default();
    let route = route.unwrap_or(&default_route);

    route
        .members
        .iter()
        .zip(route.endpoints.iter())
        .enumerate()
        .max_by_key(|(index, (id, _endpoint))| {
            // Member selection criteria:
            (
                // If we want the primary, then prefer the primary.
                primary && *index as i32 == route.primary,
                // Prefer members in our same zone.
                zone == id.zone,
                // Randomize over members to balance load.
                rand::random::<u8>(),
            )
        })
        .map(|(index, _)| index)
}
