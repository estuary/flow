use std::{collections::HashMap, fs, path};

use anyhow::Context;
use handlebars::{handlebars_helper, Handlebars};
use rust_embed::RustEmbed;
use serde::Serialize;

use crate::TenantInfo;

#[derive(Serialize, Debug, PartialEq)]
struct TemplateData {
    level_1_derivations: HashMap<i32, Vec<String>>,
    local: bool,
    // The templated tests should normally not be rendered since every publication will include the
    // ops tenant collections and this causes the tests to fail. The only time tests will be
    // rendered is when running with Generate for local testing.
    tests: bool,
}

fn tenants_to_data(tenants: Vec<TenantInfo>, local: bool, tests: bool) -> TemplateData {
    let mut level_1_derivations: HashMap<i32, Vec<String>> = HashMap::new();

    for tenant in tenants.iter() {
        level_1_derivations
            .entry(tenant.l1_stat_rollup)
            .or_default()
            .push(
                tenant
                    .tenant
                    .strip_suffix('/')
                    .expect("tenant name did not end in /")
                    .to_string(),
            );
    }

    TemplateData {
        level_1_derivations,
        local,
        tests,
    }
}

// Tenant names in the template outputs are represented as hex-encoded hashes of the actual tenant
// name. This is done to ensure compatibility with Typescript function name requirements for the
// transform lambdas regarding allowable characters and length of names.
handlebars_helper!(hashed_tenant: |tenant: String| format!("{:x}", md5::compute(tenant) ));

#[derive(RustEmbed)]
#[folder = "assets/"]
struct Assets;

pub struct Renderer<'a> {
    reg: handlebars::Handlebars<'a>,
    local: bool,
    tests: bool,
}

// These tuples are in the form of template_name:file_name. They are coupled with the actual file
// names in the assets/ directory that will be embedded in the Assets struct, as well as
// Renderer::render.
const TEMPLATES: [(&str, &str); 3] = [
    ("flow", "flow.yaml.hbs"),
    ("catalog-stats", "catalog-stats.ts.hbs"),
    ("catalog-stats-rollup", "catalog-stats-rollup.ts.hbs"),
];

impl Renderer<'_> {
    pub fn new(local: bool, tests: bool) -> anyhow::Result<Self> {
        let mut reg = Handlebars::new();
        reg.register_escape_fn(handlebars::no_escape);
        reg.register_helper("hashed_tenant", Box::new(hashed_tenant));

        for (name, tmpl_path) in TEMPLATES.iter() {
            reg.register_template_string(
                name,
                String::from_utf8(
                    Assets::get(tmpl_path)
                        .context("getting embedded assets file")?
                        .data
                        .into(),
                )
                .context("converting template string to utf8")?,
            )
            .context("registering template string")?;
        }

        Ok(Self { reg, local, tests })
    }

    pub fn render(&self, tenants: Vec<TenantInfo>, working_dir: &path::Path) -> anyhow::Result<()> {
        let data = tenants_to_data(tenants, self.local, self.tests);

        if working_dir.is_dir() {
            fs::remove_dir_all(working_dir).context("clearing working dir")?;
        }
        fs::create_dir_all(working_dir).context("re-creating working dir")?;

        // Don't copy the templates into the working dir since we have already loaded those into the
        // registry.
        for file in Assets::iter().filter(|f| !f.as_ref().ends_with(".hbs")) {
            let data = Assets::get(file.as_ref())
                .context("getting embedded assets file")?
                .data;

            fs::write(working_dir.join(file.as_ref()), data).context("copying assets to tmp")?;
        }

        fs::write(
            working_dir.join("flow.yaml"),
            self.reg.render("flow", &data)?,
        )?;

        for derivation in data.level_1_derivations.iter() {
            fs::write(
                working_dir.join(format!("catalog-stats-{}.ts", derivation.0)),
                self.reg.render("catalog-stats", &derivation)?,
            )?;
        }

        fs::write(
            working_dir.join("catalog-stats-rollup.ts"),
            self.reg
                .render("catalog-stats-rollup", &data.level_1_derivations)?,
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_output() {
        let tenants = vec![
            TenantInfo {
                tenant: "_..-weird-name1/".to_string(),
                l1_stat_rollup: 0,
            },
            TenantInfo {
                tenant: "weird.name_2/".to_string(),
                l1_stat_rollup: 0,
            },
            TenantInfo {
                tenant: "regularName/".to_string(),
                l1_stat_rollup: 1,
            },
        ];

        let tmp_dir = tempfile::TempDir::new().unwrap();
        let r = Renderer::new(true, true).unwrap();
        r.render(tenants, tmp_dir.path()).unwrap();

        for entry in fs::read_dir(&tmp_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                let rendered = fs::read_to_string(entry.path()).unwrap();
                insta::assert_snapshot!(entry.file_name().to_str(), rendered);
            }
        }
    }
}
