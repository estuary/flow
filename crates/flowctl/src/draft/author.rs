use crate::{api_exec, config};
use serde::{Deserialize, Serialize};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Author {
    /// Path or URL to a Flow catalog file to author.
    #[clap(long)]
    source: String,
}

pub async fn do_author(cfg: &config::Config, Author { source }: &Author) -> anyhow::Result<()> {
    let cur_draft = cfg.cur_draft()?;

    let models::Catalog {
        collections,
        captures,
        materializations,
        tests,
        ..
    } = crate::source::bundle(source).await?;

    // Build up the array of `draft_specs` to upsert.
    #[derive(Serialize, Debug)]
    struct DraftSpec<'a, P: serde::Serialize> {
        draft_id: &'a str,
        catalog_name: String,
        spec_type: &'static str,
        spec: P,
    }

    let mut body: Vec<u8> = Vec::new();
    body.push('[' as u8);

    for (name, spec) in collections {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id: &cur_draft,
                catalog_name: name.into(),
                spec_type: "collection",
                spec,
            },
        )
        .unwrap();
    }
    for (name, spec) in captures {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id: &cur_draft,
                catalog_name: name.into(),
                spec_type: "capture",
                spec,
            },
        )
        .unwrap();
    }
    for (name, spec) in materializations {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id: &cur_draft,
                catalog_name: name.into(),
                spec_type: "materialization",
                spec,
            },
        )
        .unwrap();
    }
    for (name, steps) in tests {
        if body.len() != 1 {
            body.push(',' as u8);
        }
        serde_json::to_writer(
            &mut body,
            &DraftSpec {
                draft_id: &cur_draft,
                catalog_name: name.into(),
                spec_type: "test",
                spec: steps,
            },
        )
        .unwrap();
    }
    body.push(']' as u8);

    #[derive(Deserialize)]
    struct Row {
        catalog_name: String,
        spec_type: String,
    }
    let rows: Vec<Row> = api_exec(
        cfg.client()?
            .from("draft_specs")
            .select("catalog_name,spec_type")
            .upsert(String::from_utf8(body).expect("serialized JSON is always UTF-8"))
            .on_conflict("draft_id,catalog_name"),
    )
    .await?;

    let mut table = crate::new_table(vec!["Name", "Type"]);
    for row in rows {
        table.add_row(vec![row.catalog_name, row.spec_type]);
    }
    println!("{table}");

    Ok(())
}
