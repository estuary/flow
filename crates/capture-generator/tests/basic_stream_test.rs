use capture_generator::{
    interface::SourceDefinition,
    run::run_stream,
    std::{BasicPagination, BasicStream, BearerTokenAuth},
};
use schemars::schema_for;
use serde_json::json;

#[tokio::test]
async fn test_basic_stream() {
    let stream = BasicStream {
        key: "test_stream".to_string(),
        description: "test stream!".to_string(),
        endpoint: "https://reqres.in/api/users".to_string(),
        method: http::Method::GET,
        spec: schema_for!(()),
    };

    let def = SourceDefinition {
        auth: BearerTokenAuth::default(),
        pagination: BasicPagination {
            page_query_field: "page".to_string(),
            current_page_field: "page".to_string(),
            max_pages_field: "total_pages".to_string(),
        },
    };

    run_stream(
        json!({
            "authentication": {
                "token": "test"
            }
        }),
        def,
        stream,
    )
    .await
    .unwrap();
}
