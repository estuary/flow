use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Egress a task declares: the host names and address ranges its connector
/// may reach.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Egress {
    /// # Host names the connector may reach.
    /// These add to the hosts the connector and its image already declare,
    /// and an empty or missing list adds none. `api.acmeco.example` matches
    /// only itself. `*.acmeco.example` matches every name beneath
    /// `acmeco.example`, at any depth, but not `acmeco.example` itself: list
    /// both to reach both. A wildcard may not cover a public suffix, such as
    /// `*.com`. Names are case-insensitive, have no trailing dot, and are
    /// written in punycode (`xn--`) where they are not ASCII.
    ///
    /// A permitted name is reachable on any port, but only at public
    /// addresses, or at addresses within `cidrs`. Loopback, link-local and
    /// other addresses which are not destinations, and TCP port 25, are never
    /// reachable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
    /// # IPv4 address ranges the connector may reach.
    /// Each is an address and a prefix length, such as `10.0.0.0/8` or
    /// `10.1.2.3/32`, with no bits set below the prefix. Every address within
    /// a range is reachable on any TCP or UDP port, private addresses
    /// included, both directly and by any name which resolves only into
    /// declared ranges. Ranges may overlap, and `0.0.0.0/0` covers every
    /// address. Loopback, link-local and other addresses which are not
    /// destinations, the machine running the connector, and TCP port 25,
    /// remain unreachable.
    ///
    /// Only private and local data planes accept address ranges.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cidrs: Vec<String>,
}

#[cfg(test)]
mod test {
    use serde_json::json;

    #[test]
    fn egress_round_trips_across_task_models() {
        fn round_trip<M: serde::Serialize + serde::de::DeserializeOwned>(
            mut fixture: serde_json::Value,
            egress: Option<serde_json::Value>,
            declared: impl Fn(&M) -> &Option<super::Egress>,
        ) -> String {
            if let Some(egress) = egress {
                fixture["egress"] = egress;
            }
            let model: M = match serde_json::from_value(fixture) {
                Ok(model) => model,
                Err(err) => return format!("refused: {err}"),
            };
            let serialized = serde_json::to_value(&model).unwrap();
            format!(
                "{:?} serialized={}",
                declared(&model),
                serialized
                    .get("egress")
                    .map_or("<omitted>".to_string(), ToString::to_string)
            )
        }

        let capture = json!({
            "endpoint": {"connector": {"image": "an/image", "config": {}}},
            "bindings": [],
        });
        let derivation = json!({
            "using": {"connector": {"image": "an/image", "config": {}}},
            "transforms": [],
        });
        let materialization = json!({
            "endpoint": {"connector": {"image": "an/image", "config": {}}},
            "bindings": [],
        });

        let mut rows = Vec::new();
        for egress in [
            None,
            Some(json!(null)),
            Some(json!({"hosts": []})),
            Some(json!({"hosts": ["api.acmeco.example", "*.svc.acmeco.example"]})),
            Some(json!({"cidrs": ["10.0.0.0/8", "0.0.0.0/0"]})),
            Some(json!({"hosts": ["api.acmeco.example"], "cidrs": ["10.1.2.3/32"]})),
            Some(json!({"hosts": [], "cidrs": []})),
            Some(json!({})),
            Some(json!({"hosts": [], "ports": [443]})),
            Some(json!({"hosts": "api.acmeco.example"})),
            Some(json!({"cidrs": "10.0.0.0/8"})),
        ] {
            let label = egress
                .as_ref()
                .map_or("<omitted>".to_string(), ToString::to_string);
            rows.push(format!(
                "capture {label} => {}",
                round_trip::<crate::CaptureDef>(capture.clone(), egress.clone(), |m| &m.egress)
            ));
            rows.push(format!(
                "derivation {label} => {}",
                round_trip::<crate::Derivation>(derivation.clone(), egress.clone(), |m| &m.egress)
            ));
            rows.push(format!(
                "materialization {label} => {}",
                round_trip::<crate::MaterializationDef>(materialization.clone(), egress, |m| {
                    &m.egress
                })
            ));
        }

        insta::assert_snapshot!(rows.join("\n"), @r#"
        capture <omitted> => None serialized=<omitted>
        derivation <omitted> => None serialized=<omitted>
        materialization <omitted> => None serialized=<omitted>
        capture null => None serialized=<omitted>
        derivation null => None serialized=<omitted>
        materialization null => None serialized=<omitted>
        capture {"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        derivation {"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        materialization {"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        capture {"hosts":["api.acmeco.example","*.svc.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example", "*.svc.acmeco.example"], cidrs: [] }) serialized={"hosts":["api.acmeco.example","*.svc.acmeco.example"]}
        derivation {"hosts":["api.acmeco.example","*.svc.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example", "*.svc.acmeco.example"], cidrs: [] }) serialized={"hosts":["api.acmeco.example","*.svc.acmeco.example"]}
        materialization {"hosts":["api.acmeco.example","*.svc.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example", "*.svc.acmeco.example"], cidrs: [] }) serialized={"hosts":["api.acmeco.example","*.svc.acmeco.example"]}
        capture {"cidrs":["10.0.0.0/8","0.0.0.0/0"]} => Some(Egress { hosts: [], cidrs: ["10.0.0.0/8", "0.0.0.0/0"] }) serialized={"cidrs":["10.0.0.0/8","0.0.0.0/0"]}
        derivation {"cidrs":["10.0.0.0/8","0.0.0.0/0"]} => Some(Egress { hosts: [], cidrs: ["10.0.0.0/8", "0.0.0.0/0"] }) serialized={"cidrs":["10.0.0.0/8","0.0.0.0/0"]}
        materialization {"cidrs":["10.0.0.0/8","0.0.0.0/0"]} => Some(Egress { hosts: [], cidrs: ["10.0.0.0/8", "0.0.0.0/0"] }) serialized={"cidrs":["10.0.0.0/8","0.0.0.0/0"]}
        capture {"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example"], cidrs: ["10.1.2.3/32"] }) serialized={"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]}
        derivation {"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example"], cidrs: ["10.1.2.3/32"] }) serialized={"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]}
        materialization {"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example"], cidrs: ["10.1.2.3/32"] }) serialized={"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]}
        capture {"cidrs":[],"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        derivation {"cidrs":[],"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        materialization {"cidrs":[],"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        capture {} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        derivation {} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        materialization {} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        capture {"hosts":[],"ports":[443]} => refused: unknown field `ports`, expected `hosts` or `cidrs`
        derivation {"hosts":[],"ports":[443]} => refused: unknown field `ports`, expected `hosts` or `cidrs`
        materialization {"hosts":[],"ports":[443]} => refused: unknown field `ports`, expected `hosts` or `cidrs`
        capture {"hosts":"api.acmeco.example"} => refused: invalid type: string "api.acmeco.example", expected a sequence
        derivation {"hosts":"api.acmeco.example"} => refused: invalid type: string "api.acmeco.example", expected a sequence
        materialization {"hosts":"api.acmeco.example"} => refused: invalid type: string "api.acmeco.example", expected a sequence
        capture {"cidrs":"10.0.0.0/8"} => refused: invalid type: string "10.0.0.0/8", expected a sequence
        derivation {"cidrs":"10.0.0.0/8"} => refused: invalid type: string "10.0.0.0/8", expected a sequence
        materialization {"cidrs":"10.0.0.0/8"} => refused: invalid type: string "10.0.0.0/8", expected a sequence
        "#);
    }
}
