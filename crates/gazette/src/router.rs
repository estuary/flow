use crate::Error;
use broker::process_spec::Id as MemberId;
use proto_gazette::broker;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

/// Mode controls how Router maps a current request to an member Channel.
pub enum Mode {
    /// Prefer the primary of the current topology.
    Primary,
    /// Prefer the closest replica of the current topology.
    Replica,
    /// Use the default service address, ignoring the current topology.
    /// This is appropriate for un-routed RPCs or when running
    /// behind a proxy.
    Default,
}

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

    /// Map an Option<&mut Header>, Mode, and `default` service address into a
    /// Channel for use in the dispatch of an RPC, and a boolean which is set
    /// if and only if the Channel is in our local zone.
    ///
    /// `default.suffix` must be the dial-able endpoint of the service,
    /// while `default.zone` should be its zone (if known).
    ///
    /// route() dials Channels as required, and users MUST call sweep()
    /// to periodically clean up Channels which are no longer in use.
    ///
    /// route() mutates `header` by clearing its `process_id` if set.
    /// This facilitates passing forward the Header of an RPC response into
    /// a next RPC request, in order to leverage route topology and Etcd
    /// metadata of that response. `process_id` must be cleared because it
    /// represents the handling server in a response context, but in a
    /// request context it denotes the server to which the request is directed,
    /// which is not our intention here. Rather, we wish to use a prior response
    /// Header to pick a *better* member to which we'll route the next request.
    pub fn route(
        &self,
        header: Option<&mut broker::Header>,
        mode: Mode,
        default: &MemberId,
    ) -> Result<(Channel, bool), Error> {
        let (route, primary) = match header {
            Some(header) => {
                header.process_id = None;

                match mode {
                    Mode::Primary => (header.route.as_ref(), true),
                    Mode::Replica => (header.route.as_ref(), false),
                    Mode::Default => (None, false),
                }
            }
            None => (None, false),
        };
        let index = pick(route, primary, &self.inner.zone);

        let id = match index {
            Some(index) => &route.unwrap().members[index],
            None => default,
        };
        let local = id.zone == self.inner.zone;
        tracing::debug!(?id, %local, "picked member");

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

        Ok((channel, local))
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
