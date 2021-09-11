use doc::{Schema as CompiledSchema, SchemaIndex};
use superslice::Ext;
use url::Url;

impl super::Collection {
    /// UUID pointer of this collection.
    pub fn uuid_ptr(&self) -> String {
        return "/_meta/uuid".to_string();
    }
}

impl super::Transform {
    /// Group name of this transform, used to group shards & shuffled reads
    /// which collectively process the transformation.
    pub fn group_name(&self) -> String {
        format!(
            "derive/{}/{}",
            self.derivation.as_str(),
            self.transform.as_str()
        )
    }
}

impl super::SchemaDoc {
    pub fn compile(&self) -> Result<CompiledSchema, json::schema::build::Error> {
        json::schema::build::build_schema(self.schema.clone(), &self.dom)
    }

    pub fn compile_all(slice: &[Self]) -> Result<Vec<CompiledSchema>, json::schema::build::Error> {
        slice
            .iter()
            .map(|d| d.compile())
            .collect::<Result<Vec<_>, _>>()
    }

    /// Compile and index all schemas, and leak a 'static index over all built schemas.
    pub fn leak_index(slice: &[Self]) -> Result<&'static doc::SchemaIndex<'static>, anyhow::Error> {
        // Compile the bundle of catalog schemas. Then, deliberately "leak" the
        // immutable Schema bundle for the remainder of program in order to achieve
        // a 'static lifetime, which is required for use in spawned tokio Tasks (and
        // therefore in TxnCtx).
        let schemas = Self::compile_all(&slice)?;
        let schemas = Box::leak(Box::new(schemas));

        let mut schema_index = SchemaIndex::<'static>::new();
        for schema in schemas.iter() {
            schema_index.add(schema)?;
        }
        schema_index.verify_references()?;

        // Also leak a &'static SchemaIndex.
        Ok(Box::leak(Box::new(schema_index)))
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
