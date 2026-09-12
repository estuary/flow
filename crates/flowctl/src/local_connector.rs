pub(crate) async fn unary(
    router: &dyn proto_grpc::connector::Router,
    log_level: Option<&str>,
    kind: proto_flow::connector::request::Kind,
) -> anyhow::Result<proto_flow::connector::response::Kind> {
    let (_started, response) = proto_grpc::connector::unary(
        router,
        &ops::tracing_log_handler,
        proto_flow::connector::Request {
            start: Some(proto_flow::connector::request::Start {
                log_level: log_level
                    .and_then(ops::LogLevel::from_str_name)
                    .unwrap_or_default() as i32,
                ..Default::default()
            }),
            kind: Some(kind),
        },
        std::time::Duration::MAX,
        std::time::Duration::MAX,
    )
    .await?;
    Ok(response)
}

pub(crate) async fn spec_capture(
    router: &dyn proto_grpc::connector::Router,
    log_level: Option<&str>,
    spec: proto_flow::capture::request::Spec,
) -> anyhow::Result<proto_flow::capture::response::Spec> {
    let response = unary(
        router,
        log_level,
        proto_flow::connector::request::Kind::Capture(proto_flow::capture::Request {
            kind: Some(proto_flow::capture::request::Kind::Spec(spec)),
            ..Default::default()
        }),
    )
    .await?;
    let Some(proto_flow::capture::Response {
        kind: Some(proto_flow::capture::response::Kind::Spec(spec)),
        ..
    }) = proto_grpc::connector::unwrap_capture(response)
    else {
        anyhow::bail!("connector didn't send expected capture Spec response");
    };
    Ok(*spec)
}

pub(crate) async fn spec_derive(
    router: &dyn proto_grpc::connector::Router,
    log_level: Option<&str>,
    spec: proto_flow::derive::request::Spec,
) -> anyhow::Result<proto_flow::derive::response::Spec> {
    let response = unary(
        router,
        log_level,
        proto_flow::connector::request::Kind::Derive(proto_flow::derive::Request {
            kind: Some(proto_flow::derive::request::Kind::Spec(spec)),
            ..Default::default()
        }),
    )
    .await?;
    let Some(proto_flow::derive::Response {
        kind: Some(proto_flow::derive::response::Kind::Spec(spec)),
        ..
    }) = proto_grpc::connector::unwrap_derive(response)
    else {
        anyhow::bail!("connector didn't send expected derive Spec response");
    };
    Ok(*spec)
}

pub(crate) async fn spec_materialize(
    router: &dyn proto_grpc::connector::Router,
    log_level: Option<&str>,
    spec: proto_flow::materialize::request::Spec,
) -> anyhow::Result<proto_flow::materialize::response::Spec> {
    let response = unary(
        router,
        log_level,
        proto_flow::connector::request::Kind::Materialize(proto_flow::materialize::Request {
            kind: Some(proto_flow::materialize::request::Kind::Spec(spec)),
            ..Default::default()
        }),
    )
    .await?;
    let Some(proto_flow::materialize::Response {
        kind: Some(proto_flow::materialize::response::Kind::Spec(spec)),
        ..
    }) = proto_grpc::connector::unwrap_materialize(response)
    else {
        anyhow::bail!("connector didn't send expected materialize Spec response");
    };
    Ok(*spec)
}
