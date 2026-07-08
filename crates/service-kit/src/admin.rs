//! The admin surface: a loopback HTTP endpoint presenting a service's
//! [`Registry`] as an auto-refreshing HTML dashboard, a per-handler drill-down
//! page (identity, phase, and recent [`crate::event`] tracks), plus JSON views.
//! The Prometheus scrape endpoint from [`crate::metrics`] is folded in too, so
//! `/metrics` is reachable on the same loopback port.

use crate::{HandlerDetail, Registry, Snapshot};
use std::fmt::Write;
use std::net::SocketAddr;

#[derive(Clone)]
struct AdminState {
    registry: Registry,
    // Service name shown in the page title; the dashboard serves one service.
    title: std::sync::Arc<str>,
}

/// Build the admin router for `service_name`.
/// Must be called from within a Tokio runtime.
pub fn build_router(service_name: impl Into<String>, registry: Registry) -> axum::Router<()> {
    use axum::routing::{get, post};

    let state = AdminState {
        registry,
        title: service_name.into().into(),
    };
    axum::Router::new()
        .route("/", get(index))
        .route("/debug/handlers.json", get(handlers_json))
        .route("/debug/handlers/{id}", get(handler))
        .route("/debug/handlers/{id}/detail.json", get(handler_detail_json))
        // POST (not GET): this mutates process state, so it shouldn't be
        // reachable by a link prefetch or an open-all-tabs.
        .route("/debug/handlers/{id}/level/{level}", post(set_level))
        .with_state(state)
        // `merge` (not `nest`): keep `/metrics` at the root of the admin port,
        // matching what Prometheus scrapers expect by default.
        .merge(crate::metrics::install_recorder())
        .layer(tower_http::trace::TraceLayer::new_for_http())
}

/// Bind `addr` and serve the admin surface until `shutdown` resolves. `addr`
/// should be loopback-only — there is no authentication on this surface.
pub async fn serve(
    service_name: impl Into<String>,
    registry: Registry,
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let service_name = service_name.into();
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|err| anyhow::anyhow!("binding {service_name} admin surface on {addr}: {err}"))?;
    tracing::info!(%addr, service = %service_name, "service-kit admin surface listening");

    axum::serve(listener, build_router(service_name.clone(), registry))
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|err| anyhow::anyhow!("serving {service_name} admin surface: {err}"))
}

async fn handlers_json(
    axum::extract::State(state): axum::extract::State<AdminState>,
) -> axum::Json<Snapshot> {
    axum::Json(state.registry.snapshot())
}

async fn handler(
    axum::extract::State(state): axum::extract::State<AdminState>,
    axum::extract::Path(id): axum::extract::Path<u64>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Result<axum::response::Html<String>> {
    let detail = state
        .registry
        .handler_detail(id)
        .ok_or((axum::http::StatusCode::NOT_FOUND, "no such handler"))?;
    // Drill-down time display: `?t=zulu` to flip from the default relative ages
    // to absolute RFC-3339 timestamps; preserved across the page's 2s refresh.
    let zulu = params.get("t").is_some_and(|v| v == "zulu");
    Ok(axum::response::Html(render_handler(
        &state.title,
        &detail,
        zulu,
    )))
}

async fn handler_detail_json(
    axum::extract::State(state): axum::extract::State<AdminState>,
    axum::extract::Path(id): axum::extract::Path<u64>,
) -> axum::response::Result<axum::Json<HandlerDetail>> {
    let detail = state
        .registry
        .handler_detail(id)
        .ok_or((axum::http::StatusCode::NOT_FOUND, "no such handler"))?;
    Ok(axum::Json(detail))
}

/// `POST` target of the dashboard's trace-level buttons; replies with a
/// see-other redirect back to the index (post/redirect/get).
async fn set_level(
    axum::extract::State(state): axum::extract::State<AdminState>,
    axum::extract::Path((id, level)): axum::extract::Path<(u64, String)>,
) -> axum::response::Result<axum::response::Redirect> {
    let level =
        parse_level(&level).ok_or((axum::http::StatusCode::BAD_REQUEST, "unknown trace level"))?;
    // A miss (handler already finished) is unremarkable — fall through to the
    // refreshed index, which will no longer list it.
    let _ = state.registry.set_trace_override(id, level);
    Ok(axum::response::Redirect::to("/"))
}

/// Parse a path segment into a trace-override level; `Some(None)` clears it.
fn parse_level(s: &str) -> Option<Option<tracing::Level>> {
    Some(match s {
        "off" | "clear" | "none" => None,
        "error" => Some(tracing::Level::ERROR),
        "warn" => Some(tracing::Level::WARN),
        "info" => Some(tracing::Level::INFO),
        "debug" => Some(tracing::Level::DEBUG),
        "trace" => Some(tracing::Level::TRACE),
        _ => return None,
    })
}

async fn index(
    axum::extract::State(state): axum::extract::State<AdminState>,
) -> axum::response::Html<String> {
    axum::response::Html(render_index(&state.title, &state.registry.snapshot()))
}

/// Inline page styling shared by [`render_index`] and [`render_handler`].
const PAGE_STYLE: &str = "body{font:13px/1.4 ui-monospace,Menlo,Consolas,monospace;margin:1.5rem;color:#222}\
h1,h2,h3{font-size:1rem;margin:1.2rem 0 .4rem}\
table{border-collapse:collapse;width:100%;margin-bottom:.8rem}\
th,td{text-align:left;padding:.25rem .6rem;border-bottom:1px solid #ddd;vertical-align:top}\
th{background:#f3f3f3}\
a{color:#06c;text-decoration:none}a:hover{text-decoration:underline}\
form{display:inline}\
button{font:inherit;color:#06c;background:none;border:0;padding:0;cursor:pointer}button:hover{text-decoration:underline}\
.kind{color:#06c}.phase{font-weight:bold}.muted{color:#888}.on{font-weight:bold;color:#c30}";

/// Open an auto-refreshing HTML page with the shared style; `head_title` is the
/// already-escaped `<title>` text.
fn page_open(out: &mut String, head_title: &str) {
    let _ = write!(
        out,
        "<!doctype html><html><head><meta charset=\"utf-8\">\
         <meta http-equiv=\"refresh\" content=\"2\">\
         <title>{head_title}</title><style>{PAGE_STYLE}</style></head><body>",
    );
}

/// A link to a handler's drill-down page, wrapping the given (pre-escaped) text.
fn handler_link(id: u64, text: &str) -> String {
    format!("<a href=\"/debug/handlers/{id}\">{text}</a>")
}

fn render_index(title: &str, snapshot: &Snapshot) -> String {
    let title = esc(title);
    let mut out = String::new();
    page_open(&mut out, &format!("{title} handlers"));

    let _ = write!(
        out,
        "<h1>{title} handlers</h1><p class=\"muted\">{} live, {} recently finished — auto-refreshes every 2s · <a href=\"/debug/handlers.json\">json</a></p>",
        snapshot.live.len(),
        snapshot.recent.len(),
    );

    out.push_str("<h2>Live</h2>");
    if snapshot.live.is_empty() {
        out.push_str("<p class=\"muted\">(none)</p>");
    } else {
        out.push_str(
            "<table><tr><th>id</th><th>kind</th><th>label</th><th>age</th><th>phase</th><th>trace</th><th>fields</th></tr>",
        );
        for h in &snapshot.live {
            let _ = write!(
                out,
                "<tr><td>{}</td><td class=\"kind\">{}</td><td>{}</td><td>{}</td><td class=\"phase\">{} <span class=\"muted\">({})</span></td><td>{}</td><td>{}</td></tr>",
                handler_link(h.id, &h.id.to_string()),
                handler_link(h.id, &esc(h.kind)),
                esc(&h.label),
                fmt_age(h.age_seconds),
                esc(&h.phase),
                fmt_age(h.phase_age_seconds),
                fmt_trace_controls(h.id, h.trace_override),
                fmt_fields(&h.fields),
            );
        }
        out.push_str("</table>");
    }

    out.push_str("<h2>Recently finished</h2>");
    if snapshot.recent.is_empty() {
        out.push_str("<p class=\"muted\">(none)</p>");
    } else {
        out.push_str(
            "<table><tr><th>id</th><th>kind</th><th>label</th><th>ran for</th><th>final phase</th></tr>",
        );
        // Newest first.
        for h in snapshot.recent.iter().rev() {
            let _ = write!(
                out,
                "<tr><td>{}</td><td class=\"kind\">{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                handler_link(h.id, &h.id.to_string()),
                handler_link(h.id, &esc(h.kind)),
                esc(&h.label),
                fmt_age(h.age_seconds),
                esc(&h.final_phase),
            );
        }
        out.push_str("</table>");
    }

    out.push_str("</body></html>");
    out
}

fn render_handler(title: &str, h: &HandlerDetail, zulu: bool) -> String {
    let title = esc(title);
    let mut out = String::new();
    page_open(&mut out, &format!("{title} handler #{}", h.id));

    let status = if h.finished { "finished" } else { "live" };
    // Toggle: the currently-active mode is bold, the alternative is a link
    // back to the same handler with the opposite `t=` query.
    let toggle = if zulu {
        format!("<a href=\"/debug/handlers/{}\">age</a> · <b>zulu</b>", h.id)
    } else {
        format!(
            "<b>age</b> · <a href=\"/debug/handlers/{}?t=zulu\">zulu</a>",
            h.id,
        )
    };
    let _ = write!(
        out,
        "<p class=\"muted\"><a href=\"/\">← {title} handlers</a> · <a href=\"/debug/handlers/{}/detail.json\">json</a> · times: {toggle}</p>\
         <h1>handler #{} <span class=\"muted\">({status})</span></h1>",
        h.id, h.id,
    );

    out.push_str("<table>");
    let _ = write!(
        out,
        "<tr><th>kind</th><td class=\"kind\">{}</td></tr>",
        esc(h.kind)
    );
    let _ = write!(out, "<tr><th>label</th><td>{}</td></tr>", esc(&h.label));

    // Phase parenthetical: phase age (default) or absolute phase-change time
    // (zulu). `phase_since_rfc3339` and `phase_age_seconds` are both `None`
    // on a finished handler, in which case there is no parenthetical.
    let phase_paren = match (zulu, &h.phase_since_rfc3339, h.phase_age_seconds) {
        (true, Some(at), _) => {
            format!(" <span class=\"muted\">(since {})</span>", esc(at))
        }
        (false, _, Some(s)) => format!(" <span class=\"muted\">({})</span>", fmt_age(s)),
        _ => String::new(),
    };
    let _ = write!(
        out,
        "<tr><th>phase</th><td class=\"phase\">{}{phase_paren}</td></tr>",
        esc(&h.phase),
    );

    // Age row. For a finished handler, `age_seconds` is the total runtime —
    // a duration with no zulu equivalent, so it renders the same in both
    // modes. For a live handler in zulu mode, show the absolute start time.
    let (age_label, age_value) = if h.finished {
        ("ran for", fmt_age(h.age_seconds))
    } else if zulu {
        match &h.started_at_rfc3339 {
            Some(at) => ("started", esc(at)),
            None => ("age", fmt_age(h.age_seconds)),
        }
    } else {
        ("age", fmt_age(h.age_seconds))
    };
    let _ = write!(out, "<tr><th>{age_label}</th><td>{age_value}</td></tr>");

    if !h.finished {
        let _ = write!(
            out,
            "<tr><th>trace</th><td>{}</td></tr>",
            fmt_trace_controls(h.id, h.trace_override),
        );
    }
    if !h.fields.is_empty() {
        let _ = write!(
            out,
            "<tr><th>fields</th><td>{}</td></tr>",
            fmt_fields(&h.fields)
        );
    }
    out.push_str("</table>");

    out.push_str("<h2>Tracks</h2>");
    if h.tracks.is_empty() {
        out.push_str("<p class=\"muted\">(no events captured)</p>");
    } else {
        let time_col = if zulu { "at" } else { "age" };
        for (name, events) in &h.tracks {
            let _ = write!(out, "<h3>{}</h3>", esc(name));
            let _ = write!(
                out,
                "<table><tr><th>{time_col}</th><th>level</th><th>message</th><th>fields</th></tr>",
            );
            // Oldest first: the operator reads top-to-bottom following the
            // sequence of events that led to the handler's current state.
            for e in events {
                let when = if zulu {
                    esc(&e.at_rfc3339)
                } else {
                    fmt_age(e.age_seconds)
                };
                let _ = write!(
                    out,
                    "<tr><td>{when}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    e.level,
                    esc(&e.message),
                    fmt_fields(&e.fields),
                );
            }
            out.push_str("</table>");
        }
    }

    out.push_str("</body></html>");
    out
}

/// The current override (highlighted if set) plus the buttons to change it —
/// tiny `POST` forms rather than links, since the target mutates process state.
fn fmt_trace_controls(id: u64, current: Option<&'static str>) -> String {
    let mut out = String::new();
    match current {
        Some(level) => {
            let _ = write!(out, "<span class=\"on\">{level}</span> ");
        }
        None => out.push_str("<span class=\"muted\">base</span> "),
    }
    for label in ["trace", "debug", "off"] {
        let _ = write!(
            out,
            "<form method=\"post\" action=\"/debug/handlers/{id}/level/{label}\"><button>[{label}]</button></form> ",
        );
    }
    out
}

fn fmt_fields(fields: &[(&'static str, String)]) -> String {
    fields
        .iter()
        .map(|(k, v)| format!("{}=<b>{}</b>", esc(k), esc(v)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn fmt_age(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Minimal HTML-escaping for text interpolated into the dashboard.
fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventView;
    use crate::{FinishedView, HandlerDetail, HandlerView};

    #[test]
    fn render_index_includes_handlers_and_escapes() {
        let snapshot = Snapshot {
            live: vec![HandlerView {
                id: 7,
                kind: "leader.materialize",
                label: "acmeCo/<svc>".to_string(),
                phase: "running".to_string(),
                age_seconds: 65,
                phase_age_seconds: 3,
                fields: vec![("shards", "2".to_string())],
                trace_override: Some("TRACE"),
            }],
            recent: vec![FinishedView {
                id: 6,
                kind: "shuffle.log",
                label: "dir/0".to_string(),
                final_phase: "done".to_string(),
                age_seconds: 4000,
                tracks: Default::default(),
            }],
        };

        let html = render_index("my-service", &snapshot);
        assert!(html.contains("<title>my-service handlers</title>"));
        assert!(html.contains("leader.materialize"));
        assert!(html.contains("acmeCo/&lt;svc&gt;"));
        assert!(!html.contains("acmeCo/<svc>"));
        assert!(html.contains("shards=<b>2</b>"));
        assert!(html.contains("1m5s"));
        assert!(html.contains("1h6m"));
        assert!(html.contains("shuffle.log"));
        assert!(html.contains("<a href=\"/debug/handlers/7\">7</a>"));
        assert!(html.contains("<a href=\"/debug/handlers/7\">leader.materialize</a>"));
        assert!(html.contains("<a href=\"/debug/handlers/6\">6</a>"));
        assert!(html.contains("<a href=\"/debug/handlers/6\">shuffle.log</a>"));
        assert!(html.contains("running <span class=\"muted\">(3s)</span>"));
        // Trace controls for the live handler — `POST` forms, not links.
        assert!(html.contains(">TRACE</span>"));
        assert!(
            html.contains(
                "<form method=\"post\" action=\"/debug/handlers/7/level/trace\"><button>"
            )
        );
        assert!(html.contains("/debug/handlers/7/level/off"));
    }

    fn sample_live_detail() -> HandlerDetail {
        HandlerDetail {
            id: 7,
            kind: "leader.materialize",
            label: "acmeCo/<svc>".to_string(),
            phase: "running".to_string(),
            finished: false,
            age_seconds: 65,
            started_at_rfc3339: Some("2026-05-14T18:00:00Z".to_string()),
            phase_age_seconds: Some(3),
            phase_since_rfc3339: Some("2026-05-14T18:01:02Z".to_string()),
            fields: vec![("shards", "2".to_string())],
            trace_override: Some("DEBUG"),
            tracks: std::collections::BTreeMap::from([(
                "io".to_string(),
                vec![
                    EventView {
                        age_seconds: 9,
                        at_rfc3339: "2026-05-14T18:00:56Z".to_string(),
                        level: "DEBUG",
                        message: "read <chunk>".to_string(),
                        fields: vec![("bytes", "3".to_string())],
                    },
                    EventView {
                        age_seconds: 1,
                        at_rfc3339: "2026-05-14T18:01:04Z".to_string(),
                        level: "INFO",
                        message: "done".to_string(),
                        fields: vec![],
                    },
                ],
            )]),
        }
    }

    #[test]
    fn render_handler_includes_tracks_and_escapes() {
        let detail = sample_live_detail();
        let html = render_handler("my-service", &detail, false);
        assert!(html.contains("<title>my-service handler #7</title>"));
        assert!(html.contains("handler #7 <span class=\"muted\">(live)</span>"));
        assert!(html.contains("<a href=\"/\">"));
        assert!(html.contains("/debug/handlers/7/detail.json"));
        assert!(html.contains("/debug/handlers/7/level/trace"));
        // Time-mode toggle: age is active (bold), zulu is a link.
        assert!(html.contains("<b>age</b>"));
        assert!(html.contains("/debug/handlers/7?t=zulu\">zulu</a>"));
        assert!(html.contains("<h3>io</h3>"));
        // The track table header is `age`, not `at`, in age mode.
        assert!(html.contains("<th>age</th><th>level</th>"));
        // Oldest event first; HTML in the message is escaped.
        let done_at = html.find("done").unwrap();
        let read_at = html.find("read &lt;chunk&gt;").unwrap();
        assert!(read_at < done_at);
        assert!(!html.contains("read <chunk>"));
        assert!(html.contains("bytes=<b>3</b>"));
        assert!(html.contains("shards=<b>2</b>"));
        // RFC-3339 strings stay out of the page in age mode.
        assert!(!html.contains("2026-05-14T"));
    }

    #[test]
    fn render_handler_zulu_mode_shows_absolute_times() {
        let detail = sample_live_detail();
        let html = render_handler("my-service", &detail, true);
        // Toggle is flipped: zulu is active, age links back to bare path.
        assert!(html.contains("<b>zulu</b>"));
        assert!(html.contains("/debug/handlers/7\">age</a>"));
        // The age row is relabeled `started` and shows the absolute start time.
        assert!(html.contains("<th>started</th><td>2026-05-14T18:00:00Z</td>"));
        // Phase parenthetical is `(since <zulu>)` instead of `(3s)`.
        assert!(html.contains("(since 2026-05-14T18:01:02Z)"));
        assert!(!html.contains(">(3s)</span>"));
        // Track time column is `at`, with absolute event times.
        assert!(html.contains("<th>at</th><th>level</th>"));
        assert!(html.contains("<td>2026-05-14T18:00:56Z</td>"));
        assert!(html.contains("<td>2026-05-14T18:01:04Z</td>"));
    }

    #[test]
    fn render_handler_finished_has_no_trace_controls() {
        let detail = HandlerDetail {
            id: 3,
            kind: "shuffle.log",
            label: "dir/0".to_string(),
            phase: "done".to_string(),
            finished: true,
            age_seconds: 12,
            started_at_rfc3339: None,
            phase_age_seconds: None,
            phase_since_rfc3339: None,
            fields: vec![],
            trace_override: None,
            tracks: Default::default(),
        };
        let html = render_handler("svc", &detail, false);
        assert!(html.contains("(finished)"));
        assert!(!html.contains("/level/trace"));
        assert!(html.contains("(no events captured)"));
        // `ran for` shows the duration; phase has no parenthetical.
        assert!(html.contains("<th>ran for</th><td>12s</td>"));
        assert!(!html.contains("(since"));
        // The zulu form still works on a finished handler — duration stays a
        // duration; the toggle is still present.
        let html_zulu = render_handler("svc", &detail, true);
        assert!(html_zulu.contains("<b>zulu</b>"));
        assert!(html_zulu.contains("<th>ran for</th><td>12s</td>"));
    }

    #[test]
    fn render_index_handles_empty() {
        let html = render_index(
            "svc",
            &Snapshot {
                live: vec![],
                recent: vec![],
            },
        );
        assert!(html.contains("0 live, 0 recently finished"));
        assert!(html.contains("(none)"));
    }

    #[test]
    fn parse_level_round_trip() {
        assert_eq!(parse_level("trace"), Some(Some(tracing::Level::TRACE)));
        assert_eq!(parse_level("info"), Some(Some(tracing::Level::INFO)));
        assert_eq!(parse_level("off"), Some(None));
        assert_eq!(parse_level("clear"), Some(None));
        assert_eq!(parse_level("bogus"), None);
    }
}
