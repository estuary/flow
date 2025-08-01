use oxigraph::model::Term;
use spareval::QueryableDataset;

pub struct PgSparql {
    pool: sqlx::PgPool,
    base_uri: String,
}

enum EntityType {
    LiveSpecs,
}

struct ParsedUri {
    entity_type: EntityType,
}

enum PgTerm {
    Id(models::Id),
    Text(String),
    Integer(i64),
    Boolean(bool),
}

fn error(err: anyhow::Error) -> Box<dyn Iterator<Item = Result<spareval::InternalQuad<Self>, Self::Error>>>

impl QueryableDataset for PgSparql {
    type InternalTerm = Term;

    type Error = anyhow::Error;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&Self::InternalTerm>,
        predicate: Option<&Self::InternalTerm>,
        object: Option<&Self::InternalTerm>,
        graph_name: Option<Option<&Self::InternalTerm>>,
    ) -> Box<dyn Iterator<Item = Result<spareval::InternalQuad<Self>, Self::Error>>> {

        // Determine table from subject and predicate


    }

    fn internalize_term(
        &self,
        term: oxigraph::model::Term,
    ) -> Result<Self::InternalTerm, Self::Error> {
        todo!()
    }

    fn externalize_term(
        &self,
        term: Self::InternalTerm,
    ) -> Result<oxigraph::model::Term, Self::Error> {
        todo!()
    }
}
