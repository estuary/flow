mod common;

#[test]
fn task_vmm_selects_connector_execution() {
    let outcome = common::run(include_str!("execution.yaml"), "{}");

    let mut rows = Vec::new();
    for built in outcome.built_captures.iter() {
        let execution = built.spec.as_ref().and_then(|spec| spec.execution);
        rows.push(format!("capture {}: {execution:?}", built.capture));
    }
    for built in outcome.built_collections.iter() {
        let Some(spec) = &built.spec else { continue };
        let Some(derivation) = &spec.derivation else {
            continue;
        };
        rows.push(format!(
            "derivation {}: {:?}",
            built.collection, derivation.execution
        ));
    }
    for built in outcome.built_materializations.iter() {
        let execution = built.spec.as_ref().and_then(|spec| spec.execution);
        rows.push(format!(
            "materialization {}: {execution:?}",
            built.materialization
        ));
    }
    for error in outcome.errors.iter() {
        rows.push(format!("error {}: {:#}", error.scope, error.error));
    }

    insta::assert_snapshot!(rows.join("\n"));
}
