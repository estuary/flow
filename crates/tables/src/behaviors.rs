use superslice::Ext;
use url::Url;

impl super::Resource {
    pub fn fetch_content_dom<'s>(
        resources: &'s [Self],
        url: &Url,
    ) -> Option<&'s serde_json::value::RawValue> {
        let range = resources.equal_range_by_key(&url, |resource| &resource.resource);
        resources[range]
            .iter()
            .map(|resource| resource.content_dom.as_ref())
            .next()
    }

    pub fn compile_all_json_schemas(
        slice: &[Self],
    ) -> Result<Vec<(url::Url, doc::Schema)>, json::schema::build::Error> {
        slice
            .iter()
            .filter_map(|resource| {
                if resource.content_type == proto_flow::flow::ContentType::JsonSchema {
                    let v = serde_json::from_str::<serde_json::Value>(resource.content_dom.get())
                        .unwrap();
                    let schema = json::schema::build::build_schema(resource.resource.clone(), &v);

                    match schema {
                        Ok(schema) => Some(Ok((resource.resource.clone(), schema))),
                        Err(err) => Some(Err(err)),
                    }
                } else {
                    None
                }
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl super::Import {
    // path_exists determines whether a forward or backwards import path exists between
    // |src_scope| and |tgt_scope|.
    pub fn path_exists(imports: &[Self], src_scope: &Url, tgt_scope: &Url) -> bool {
        let edges = |from: &Url| {
            let range = imports.equal_range_by_key(&from, |import| &import.from_resource);
            imports[range].iter().map(|import| &import.to_resource)
        };

        // Trim any fragment suffix of each scope to obtain the base resource.
        let (mut src, mut tgt) = (src_scope.clone(), tgt_scope.clone());
        src.set_fragment(None);
        tgt.set_fragment(None);

        // Search forward paths.
        if let Some(_) = pathfinding::directed::bfs::bfs(&&src, |f| edges(f), |s| s == &&tgt) {
            true
        } else if let Some(_) =
            // Search backward paths.
            pathfinding::directed::bfs::bfs(&&tgt, |f| edges(f), |s| s == &&src)
        {
            true
        } else {
            false
        }
    }

    // transitive_imports returns an iterator over the resources that |src|
    // directly or indirectly imports, where |src| is included as the first item.
    // |src| must not have a fragment or transitive_imports will panic.
    pub fn transitive_imports<'a>(
        imports: &'a [Self],
        src: &'a Url,
    ) -> impl Iterator<Item = &'a Url> + 'a {
        assert!(!src.fragment().is_some());

        let edges = move |from: &Url| {
            let range = imports.equal_range_by_key(&from, |import| &import.from_resource);
            imports[range].iter().map(|import| &import.to_resource)
        };
        pathfinding::directed::bfs::bfs_reach(src, move |f| edges(f))
    }
}
