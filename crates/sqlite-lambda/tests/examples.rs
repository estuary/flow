use serde_json::json;
use sqlite_lambda::{Lambda, Projection};

#[test]
fn test_type_conversions() {
    let mut lambda = Lambda::new(
        "select \"case\", input as output from source;",
        &[
            Projection::new("case", "/case", false, false, false),
            Projection::new("input", "/in", false, false, false),
        ],
        &[],
    )
    .unwrap();

    let fixtures = json!([
      {"case": "true", "in": true},
      {"case": "false", "in": false},
      {"case": "string", "in": "hello"},
      {"case": "pos-int", "in": 123},
      {"case": "neg-int", "in": -456},
      {"case": "array", "in": "[1,2,\"three\"]"},
      {"case": "obj", "in": "{\"four\": 4}"},
      {"case": "invalid-array", "in": "[1 2 \"three\"]"},
      {"case": "invalid-obj", "in": "{four 4}"},
    ]);

    let mut output = fixtures
        .as_array()
        .unwrap()
        .iter()
        .map(|fixture| lambda.invoke(fixture, None, None).unwrap())
        .collect::<Vec<_>>();

    let mut lambda = Lambda::new(
        r#"
        select
          'string-types' as "case",
          str_int * 10,
          str_num * 2.5,
          str_base64,
          cast(str_base64 as text) as str_base64_text,
          cast('a raw string' as blob) as blob_fixture
        from source;"#,
        &[
            Projection::new("str_int", "/str/int", true, false, false),
            Projection::new("str_num", "/str/num", false, true, false),
            Projection::new("str_base64", "/str/b64", false, false, true),
        ],
        &[],
    )
    .unwrap();

    let fixture = json!({"str": {"int": "12", "num": "7.5", "b64": "VGhpcyBpcyBiYXNlNjQ="}});
    output.push(lambda.invoke(&fixture, None, None).unwrap());

    insta::assert_json_snapshot!(output);
}

#[test]
fn test_register_join() {
    let mut lambda = Lambda::new(
        r#"
        select
          n.key,
          s.field as field,
          json_object("cur", r.value, "prev", p.value) as value
        from
          source s,
          register r,
          previous_register p,
          json_each(the_keys) n
        ;"#,
        &[
            Projection::new("the_keys", "/keys", false, false, false),
            Projection::new("field", "/field", false, false, false),
        ],
        &[Projection::new("value", "/value", false, false, false)],
    )
    .unwrap();

    let output = lambda
        .invoke(
            &json!({"keys": {"bar": false, "foo": 1}, "field": "some-field"}),
            Some(&json!({"value": "updated-value"})),
            Some(&json!({"value": "previous-value"})),
        )
        .unwrap();

    insta::assert_json_snapshot!(output, @r###"
    [
      {
        "field": "some-field",
        "key": "bar",
        "value": {
          "cur": "updated-value",
          "prev": "previous-value"
        }
      },
      {
        "field": "some-field",
        "key": "foo",
        "value": {
          "cur": "updated-value",
          "prev": "previous-value"
        }
      }
    ]
    "###);

    // Again. This time keys is empty so no result rows are produced.
    let output = lambda
        .invoke(
            &json!({"keys": {}, "field": "other-field"}),
            Some(&json!({"value": 1})),
            Some(&json!({"value": 2})),
        )
        .unwrap();

    insta::assert_json_snapshot!(output, @"[]");
}

#[test]
fn test_multiple_cursors() {
    let mut lambda = Lambda::new(
        r#"
        select value * 2 as value from source
        union all
        select value * 3 from source
        "#,
        &[Projection::new("value", "/value", false, false, false)],
        &[],
    )
    .unwrap();

    let output = lambda.invoke(&json!({"value": 32}), None, None).unwrap();
    insta::assert_json_snapshot!(output, @r###"
    [
      {
        "value": 64
      },
      {
        "value": 96
      }
    ]
    "###);
}

#[test]
fn test_column_collision_error() {
    let result = Lambda::<serde_json::Value>::new(
        "select One from source;",
        &[
            Projection::new("one", "/one", false, false, false),
            Projection::new("One", "/One", false, false, false),
        ],
        &[],
    );

    let Err(err) = result else { panic!("not an error") };
    insta::assert_display_snapshot!(err, @r###"
    failed to prepare sqlite execution context for table "source" with columns ["one", "One"].
    	Ensure all projected columns have unique, case-insensitive names
    	(Error code 1: SQL error or missing database)
    "###);
}
