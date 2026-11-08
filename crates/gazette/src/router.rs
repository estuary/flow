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
    /// This is appropriate for un-routed RPCs.
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
    // Dialed Channels, and the Instant at which they were last all dropped.
    channels: std::sync::Mutex<(HashMap<MemberId, Channel>, std::time::Instant)>,
    zone: String,
}

impl Router {
    /// Create a new Router with the given default service endpoint,
    /// which prefers to route to members in `zone` where possible.
    pub fn new(zone: &str) -> Self {
        let zone = zone.to_string();

        Self {
            inner: Arc::new(Inner {
                channels: std::sync::Mutex::new((HashMap::new(), std::time::Instant::now())),
                zone,
            }),
        }
    }

    /// Map a Header, Mode, and `default` service address into a Channel for
    /// use in the dispatch of an RPC, and a boolean which is set if and only
    /// if the Channel is in our local zone.
    ///
    /// `default.suffix` must be the dial-able endpoint of the service,
    /// while `default.zone` should be its zone (if known).
    ///
    /// route() dials Channels as required, and drops all of them every
    /// `proto_grpc::CHANNEL_CACHE_MAX_AGE` to be re-dialed on next use.
    /// This rotates long-lived connections, and releases Channels of
    /// members which have left the topology.
    ///
    /// `header` is the field of a Request message type, where applicable.
    /// In some request contexts it's copied from a prior RPC Response
    /// to facilitate route discovery. route() uses the header client-side
    /// to inform member selection and then clears it to `None`, as
    /// Request headers are a server-to-server proxy mechanism and are
    /// not intended for client-to-server requests.
    pub fn route(
        &self,
        header: &mut Option<broker::Header>,
        mode: Mode,
        default: &MemberId,
    ) -> Result<(Channel, bool), Error> {
        let (route, primary) = match header.as_ref() {
            Some(header) => match mode {
                Mode::Primary => (header.route.as_ref(), true),
                Mode::Replica => (header.route.as_ref(), false),
                Mode::Default => (None, false),
            },
            None => (None, false),
        };
        let index = pick(route, primary, &self.inner.zone);

        let id = match index {
            Some(index) => &route.unwrap().members[index],
            None => default,
        };
        let local = id.zone == self.inner.zone;
        tracing::debug!(?id, %local, "picked member");

        let mut guard = self.inner.channels.lock().unwrap();
        let (channels, cleared_at) = &mut *guard;

        if cleared_at.elapsed() >= proto_grpc::CHANNEL_CACHE_MAX_AGE {
            tracing::debug!(n = channels.len(), "dropping all member Channels");
            channels.clear();
            *cleared_at = std::time::Instant::now();
        }

        let channel = match channels.get(id) {
            Some(ch) => ch.clone(),
            None => {
                let ch = proto_grpc::dial_channel(match index {
                    Some(index) => &route.unwrap().endpoints[index],
                    None => &default.suffix,
                })
                .map_err(super::Error::Transport)?;
                channels.insert(id.clone(), ch.clone());
                ch
            }
        };

        // Clear the header after routing so it is not sent on the wire.
        *header = None;

        Ok((channel, local))
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
