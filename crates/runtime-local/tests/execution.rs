//! Verify local capture Apply carries its built spec's VMM execution and
//! egress. The connector service refuses an Apply whose Start differs from
//! its built spec, so the refusal it gives instead proves both arrived.

const CATALOG: &str = r#"
captures:
  acmeCo/source:
    endpoint:
      local:
        command: ["false"]
        config: {}
    vmm: true
    egress:
      hosts: [api.acmeco.example, "*.svc.acmeco.example"]
    bindings:
      - resource: { name: docs }
        target: acmeCo/docs

collections:
  acmeCo/docs:
    schema:
      type: object
      properties:
        id: { type: string }
      required: [id]
    key: [/id]
"#;

#[tokio::test]
async fn vmm_execution_is_carried_to_capture_apply() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.flow.yaml");
    std::fs::write(&path, CATALOG).unwrap();
    let url = build::arg_source_to_url(path.to_str().unwrap(), false).unwrap();

    let output = build::for_local_test(&url, true)
        .await
        .into_result()
        .expect("catalog build should succeed");
    let spec = output
        .built
        .built_captures
        .iter()
        .find_map(|row| row.spec.clone())
        .expect("the catalog builds one capture");
    assert_eq!(
        spec.execution,
        Some(proto_flow::flow::ConnectorExecution {
            vmm: true,
            egress: Some(proto_flow::flow::connector_execution::Egress {
                hosts: vec![
                    "api.acmeco.example".to_string(),
                    "*.svc.acmeco.example".to_string(),
                ],
            }),
        })
    );

    let registry = service_kit::Registry::new();
    let run = runtime_local::services::Run::start_capture(
        runtime_local::local_router(
            String::new(),
            None,
            registry.clone(),
            std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
        ),
        1,
        None,
        registry,
    )
    .await
    .unwrap();

    let controls = runtime_local::Controls {
        initial_state_json: bytes::Bytes::new(),
        report_final_state: false,
        publisher_factory: runtime_next::RecordingPublisherFactory,
        logger_factory: runtime_next::TracingLoggerFactory,
    };
    let err = runtime_local::capture_driver::run_sessions(
        &run,
        &spec,
        vec![u32::MAX],
        controls,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect_err("Apply of a VMM local capture fails");

    let err = format!("{err:#}");
    assert!(
        err.contains("VMM execution requires an image connector"),
        "{err}"
    );
}
