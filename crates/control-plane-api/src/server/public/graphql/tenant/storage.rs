//! Storage selection for GraphQL tenant creation; legacy directives retain their own defaults.

fn trial_bucket_name(plane: &str) -> Option<(String, String)> {
    use sha2::Digest;

    let (super::super::data_planes::DataPlaneCloudProvider::Aws, region, _, true) =
        super::super::data_planes::parse_data_plane_name(plane)?
    else {
        return None;
    };
    let digest = hex::encode(sha2::Sha256::digest(plane.as_bytes()));
    Some((format!("estuary-trial-{region}-{}", &digest[..8]), region))
}

/// `planes` comes from the open public planes query, ordered by descending ID.
pub(super) fn specs(
    mut planes: Vec<String>,
    data_plane: &str,
) -> async_graphql::Result<(serde_json::Value, serde_json::Value)> {
    if planes.is_empty() {
        return Err(async_graphql::Error::new(
            "there are no open public data-planes to place a new tenant on",
        ));
    }
    if !planes.iter().any(|plane| plane == data_plane) {
        return Err(async_graphql::Error::new(format!(
            "{data_plane} is not a selectable public data-plane"
        )));
    }

    // Preserve the remaining planes' newest-first order after the selected plane.
    planes.sort_by_key(|plane| plane != data_plane);

    let mut store = match trial_bucket_name(data_plane) {
        Some((bucket, region)) => {
            serde_json::json!({"provider": "S3", "bucket": bucket, "region": region})
        }
        None => serde_json::json!({"provider": "GCS", "bucket": "estuary-trial"}),
    };
    let recovery = serde_json::json!({"stores": [store.clone()]});
    store["prefix"] = serde_json::json!("collection-data/");
    let tenant = serde_json::json!({"stores": [store], "data_planes": planes});
    Ok((tenant, recovery))
}

#[cfg(test)]
mod tests {
    #[test]
    fn trial_bucket_golden_vector() {
        // This vector is shared with est-dry-dock's Python bucket derivation.
        assert_eq!(
            super::trial_bucket_name("ops/dp/public/aws-us-east-1-c1"),
            Some((
                "estuary-trial-us-east-1-ccc98e22".to_string(),
                "us-east-1".to_string()
            ))
        );
        for plane in [
            "ops/dp/private/acmeCo/aws-us-east-1-c1",
            "ops/dp/public/gcp-europe-west1-c1",
            "ops/dp/public/azure-eastus2-c1",
            "ops/dp/public/test",
            "ops/dp/public/local-flow-cluster",
        ] {
            assert_eq!(super::trial_bucket_name(plane), None);
        }
    }

    #[test]
    fn selection_and_storage() {
        let planes: Vec<String> = [
            "ops/dp/public/gcp-europe-west1-c1",
            "ops/dp/public/aws-us-east-1-c2",
            "ops/dp/public/aws-us-east-1-c1",
        ]
        .map(String::from)
        .into();

        let (tenant, recovery) =
            super::specs(planes.clone(), "ops/dp/public/gcp-europe-west1-c1").unwrap();
        insta::assert_json_snapshot!((tenant, recovery), @r#"
        [
          {
            "data_planes": [
              "ops/dp/public/gcp-europe-west1-c1",
              "ops/dp/public/aws-us-east-1-c2",
              "ops/dp/public/aws-us-east-1-c1"
            ],
            "stores": [
              {
                "bucket": "estuary-trial",
                "prefix": "collection-data/",
                "provider": "GCS"
              }
            ]
          },
          {
            "stores": [
              {
                "bucket": "estuary-trial",
                "provider": "GCS"
              }
            ]
          }
        ]
        "#);
        let (tenant, recovery) =
            super::specs(planes.clone(), "ops/dp/public/aws-us-east-1-c1").unwrap();
        insta::assert_json_snapshot!((tenant, recovery), @r#"
        [
          {
            "data_planes": [
              "ops/dp/public/aws-us-east-1-c1",
              "ops/dp/public/gcp-europe-west1-c1",
              "ops/dp/public/aws-us-east-1-c2"
            ],
            "stores": [
              {
                "bucket": "estuary-trial-us-east-1-ccc98e22",
                "prefix": "collection-data/",
                "provider": "S3",
                "region": "us-east-1"
              }
            ]
          },
          {
            "stores": [
              {
                "bucket": "estuary-trial-us-east-1-ccc98e22",
                "provider": "S3",
                "region": "us-east-1"
              }
            ]
          }
        ]
        "#);
        for plane in [
            "ops/dp/public/gcp-europe-west1-c1",
            "ops/dp/public/azure-eastus2-c1",
            "ops/dp/public/test",
            "ops/dp/public/local-flow-cluster",
        ] {
            let (tenant, _) = super::specs(vec![plane.to_string()], plane).unwrap();
            assert_eq!(tenant["stores"][0]["provider"], "GCS");
            assert_eq!(tenant["data_planes"], serde_json::json!([plane]));
        }
        assert!(super::specs(vec![], "ops/dp/public/aws-us-east-1-c1").is_err());
        assert!(super::specs(planes, "ops/dp/public/missing").is_err());
    }
}
