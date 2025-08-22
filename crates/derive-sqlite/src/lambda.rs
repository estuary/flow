use super::{Error, Param};
use base64::Engine;

/// Lambda wraps a rusqlite::Statement with $parameters that map into document
/// Projections. It invokes the statement with novel documents, mapping bound
/// document locations into statement parameters.
///
/// Each row returned by its statement is mapped into a JSON document, with each
/// output column becoming a top-level document property. SQLite types are
/// mapped to corresponding JSON types null, integer, float, and string.
///
/// Nested JSON arrays and objects are also supported: As SQLite doesn't have
/// a bespoke JSON value type, this implementation looks for a leading / trailing
/// pair of '{','}' or '[',']' and, if found, will attempt to parse the string
/// as a JSON document. If parsing fails, the raw text is passed through as a
/// regular JSON string.
///
/// As a special case, if the query has a single output column starting
/// with "json_object", as is typically the case with SQLite's JSON functions,
/// then this column is directly mapped into the returned output document.
/// This can be used to implement lambdas with dynamic top-level properties.
pub struct Lambda<'db> {
    stmt: rusqlite::Statement<'db>,
    bindings: Vec<(String, Param)>,
    outputs: Vec<(String, Option<usize>)>,
}

impl<'db> Lambda<'db> {
    /// Create a new Lambda which executes the given `query`, having parameter
    /// bindings that map through the given `params` into extracted
    /// document locations.
    pub fn new(
        db: &'db rusqlite::Connection,
        query: &str,
        params: &[Param],
    ) -> Result<Self, Error> {
        let stmt = db.prepare(query)?;

        // Extract bindings and map each into a Param.
        let mut bindings = Vec::new();
        for index in 0..stmt.parameter_count() {
            let encoding = stmt.parameter_name(index + 1).unwrap();

            bindings.push((
                encoding.to_string(),
                Param::resolve(encoding, params)?.clone(),
            ));
        }

        // Extract output column names, and attempt to pair each with a
        // potential passed-through parameter.
        let mut outputs = Vec::new();

        if stmt.column_count() == 1
            && stmt
                .column_name(0)
                .unwrap()
                .to_ascii_lowercase()
                .starts_with("json")
        {
            // The single column is a top-level JSON document to publish.
            // SQLite's JSON functions all start with json() or json_*(),
            // so use that as an indicator of user intent
            // (they produce long column names like "json_group_object(name expression, value expression)").
        } else {
            // We'll build a top-level JSON document from the output columns.
            for index in 0..stmt.column_count() {
                let name = stmt.column_name(index).unwrap();

                let binding = bindings
                    .iter()
                    .enumerate()
                    .filter_map(
                        |(index, (encoding, _param))| {
                            if name == encoding {
                                Some(index)
                            } else {
                                None
                            }
                        },
                    )
                    .next();

                outputs.push((name.to_string(), binding));
            }
        }

        Ok(Self {
            stmt,
            bindings,
            outputs,
        })
    }

    pub fn invoke<'s, N: doc::AsNode>(
        &'s mut self,
        document: &N,
    ) -> Result<impl Iterator<Item = rusqlite::Result<serde_json::Value>> + 's, Error> {
        let Self {
            stmt,
            bindings,
            outputs,
        } = self;

        for (index, (encoding, param)) in bindings.iter().enumerate() {
            bind_parameter(stmt, index, param, document).map_err(|err| Error::BindingError {
                encoding: encoding.clone(),
                param: param.clone(),
                err,
            })?;
        }

        let result = self
            .stmt
            .raw_query()
            .mapped(|row| Ok(row_to_json(bindings, outputs, row)));

        Ok(result)
    }

    pub fn invoke_vec<'s, N: doc::AsNode>(
        &'s mut self,
        document: &N,
    ) -> Result<Vec<serde_json::Value>, Error> {
        Ok(self.invoke(document)?.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn is_explain(&self) -> bool {
        self.stmt.is_explain() > 0
    }

    pub fn is_readonly(&self) -> bool {
        self.stmt.readonly()
    }
}

fn bind_parameter<N: doc::AsNode>(
    stmt: &mut rusqlite::Statement<'_>,
    index: usize,
    param: &Param,
    document: &N,
) -> rusqlite::Result<()> {
    match param.extractor.query(document) {
        Ok(node) => bind_parameter_node(stmt, index, param, node),
        Err(node) => bind_parameter_node(stmt, index, param, node.as_ref()),
    }
}

fn bind_parameter_node<N: doc::AsNode>(
    stmt: &mut rusqlite::Statement<'_>,
    index: usize,
    Param {
        is_content_encoding_base64,
        is_format_integer,
        is_format_number,
        ..
    }: &Param,
    node: &N,
) -> rusqlite::Result<()> {
    use doc::Node;

    match node.as_node() {
        Node::Null => return stmt.raw_bind_parameter(index + 1, None::<bool>),
        Node::Bool(b) => return stmt.raw_bind_parameter(index + 1, b),

        Node::String(s) => {
            if *is_format_integer {
                if let Ok(i) = s.parse::<i64>() {
                    return stmt.raw_bind_parameter(index + 1, i);
                }
            }
            if *is_format_number {
                if let Ok(f) = s.parse::<f64>() {
                    return stmt.raw_bind_parameter(index + 1, f);
                }
            }
            if *is_content_encoding_base64 {
                if let Ok(b) = base64::engine::general_purpose::STANDARD.decode(s) {
                    return stmt.raw_bind_parameter(index + 1, b);
                }
            }
            stmt.raw_bind_parameter(index + 1, s)
        }
        Node::Bytes(b) => stmt.raw_bind_parameter(index + 1, b),
        Node::Float(f) => stmt.raw_bind_parameter(index + 1, f),
        Node::NegInt(s) => stmt.raw_bind_parameter(index + 1, s),
        Node::PosInt(u) => stmt.raw_bind_parameter(index + 1, u),
        Node::Array(_) => stmt.raw_bind_parameter(
            index + 1,
            &serde_json::to_string(&doc::SerPolicy::noop().on(node)).unwrap(),
        ),
        Node::Object(_) => stmt.raw_bind_parameter(
            index + 1,
            &serde_json::to_string(&doc::SerPolicy::noop().on(node)).unwrap(),
        ),
    }
}

fn row_to_json(
    bindings: &[(String, Param)],
    columns: &[(String, Option<usize>)],
    row: &rusqlite::Row<'_>,
) -> serde_json::Value {
    if columns.is_empty() {
        // SELECT json_object(...) from ...
        convert_value_ref(row.get_ref(0).unwrap())
    } else {
        // SELECT 1 as foo, 'two' as bar from ...
        serde_json::Value::Object(
            columns
                .iter()
                .enumerate()
                .map(|(index, (name, binding))| {
                    (
                        binding
                            .map(|b| bindings[b].1.projection.field.clone())
                            .unwrap_or_else(|| name.clone()),
                        convert_value_ref(row.get_ref(index).unwrap()),
                    )
                })
                .collect(),
        )
    }
}

fn convert_value_ref(value: rusqlite::types::ValueRef<'_>) -> serde_json::Value {
    use rusqlite::types::ValueRef;
    use serde_json::{Number, Value};

    match value {
        ValueRef::Text(s) => {
            if matches!(
                (s.first(), s.last()),
                (Some(b'{'), Some(b'}')) | (Some(b'['), Some(b']'))
            ) {
                if let Ok(v) = serde_json::from_slice(s) {
                    return v;
                }
            }
            serde_json::Value::String(String::from_utf8(s.to_vec()).unwrap())
        }
        ValueRef::Blob(b) => Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
        ValueRef::Integer(i) => Value::Number(Number::from(i)),
        ValueRef::Real(f) => match Number::from_f64(f) {
            Some(n) => Value::Number(n),
            _ => Value::String(format!("{f}")),
        },
        ValueRef::Null => Value::Null,
    }
}

#[cfg(test)]
mod test {
    use super::super::test_param;
    use super::Lambda;
    use serde_json::json;

    #[test]
    fn test_binding_and_output_mapping() {
        let params = &[
            test_param("id", "/id", false, false, false),
            test_param("sender", "/sender", false, false, false),
            test_param("recipient", "/recipient", false, false, false),
            test_param("amount", "/amount", false, false, false),
            test_param("nested/prop", "/nested/prop", false, false, false),
        ];

        let snap = |lambda: &Lambda| {
            let bindings = lambda
                .bindings
                .iter()
                .map(|(encoding, param)| json!({"encoding": encoding, "projection": &param.projection}))
                .collect::<Vec<_>>();

            json!({
                "bindings": bindings,
                "outputs": &lambda.outputs,
            })
        };

        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(
            r#"
            create table current_balances(
                account text primary key not null,
                balance real not null
            );
        "#,
        )
        .unwrap();

        let lambda = Lambda::new(
            &db,
            r#"
            with r as (
              select
                $id,
                $sender,
                $recipient,
                $amount,
                $p::(nested/prop),
                json_object('foo', 'bar')
            )
            select
              r.*,
              case when changes() or $sender = 'deposit'
                then 'approve' else 'deny' end as outcome,
              coalesce(b.balance, 0) as sender_balance
            from r
            left outer join current_balances b on $sender = b.account;
        "#,
            params,
        )
        .unwrap();

        insta::assert_json_snapshot!(snap(&lambda));
        assert!(lambda.is_readonly());

        let lambda = Lambda::new(
            &db,
            r#"
            update current_balances
            set balance = balance - $amount
            where account = $sender and balance >= $amount;
        "#,
            params,
        )
        .unwrap();

        insta::assert_json_snapshot!(snap(&lambda));
        assert!(!lambda.is_readonly());

        let lambda = Lambda::new(
            &db,
            r#"
            insert into current_balances (account, balance)
            values ($recipient, $amount)
            on conflict (account)
            do update set balance = balance + $amount
            returning $id, account, balance as upsert_balance;
            "#,
            params,
        )
        .unwrap();

        insta::assert_json_snapshot!(snap(&lambda));
        assert!(!lambda.is_readonly());

        let lambda = Lambda::new(
            &db,
            r#"
            alter table current_balances add column extra text not null default 'hello';
            "#,
            params,
        )
        .unwrap();

        insta::assert_json_snapshot!(snap(&lambda));
        assert!(!lambda.is_readonly());

        let lambda = Lambda::new(
            &db,
            r#"
            explain query plan select account, balance from current_balances where account = $sender;
            "#,
            params,
        )
        .unwrap();

        insta::assert_json_snapshot!(snap(&lambda));
        assert!(lambda.is_readonly());
        assert!(lambda.is_explain());

        let lambda = Lambda::new(
            &db,
            "\n            /* A ; comment only */\n -- Which is treated as a no-op            ",
            params,
        )
        .unwrap();

        insta::assert_json_snapshot!(snap(&lambda));
        assert!(lambda.is_readonly());
        assert!(!lambda.is_explain());
    }

    #[test]
    fn test_type_conversions() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        let mut lambda = Lambda::new(
            &db,
            "select $case, $input as output;",
            &[
                test_param("case", "/case", false, false, false),
                test_param("input", "/in", false, false, false),
            ],
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
          {"case": "missing"},
        ]);

        let mut output = fixtures
            .as_array()
            .unwrap()
            .iter()
            .map(|fixture| lambda.invoke_vec(fixture).unwrap())
            .collect::<Vec<_>>();

        let mut lambda = Lambda::new(
            &db,
            r#"
        select
          'string-types' as "case",
          $str_int * 10,
          $str_num * 2.5,
          $str_base64,
          cast($str_base64 as text) as str_base64_text,
          cast('a raw string' as blob) as blob_fixture;
        "#,
            &[
                test_param("str_int", "/str/int", true, false, false),
                test_param("str_num", "/str/num", false, true, false),
                test_param("str_base64", "/str/b64", false, false, true),
            ],
        )
        .unwrap();

        let fixture = json!({"str": {"int": "12", "num": "7.5", "b64": "VGhpcyBpcyBiYXNlNjQ="}});
        output.push(lambda.invoke_vec(&fixture).unwrap());

        let mut lambda = Lambda::new(
            &db,
            r#"
        select json_object(
            'case', 'top-level-object',
            'str-int', $str_int
        );
        "#,
            &[test_param("str_int", "/str/int", true, false, false)],
        )
        .unwrap();

        let fixture = json!({"str": {"int": "12"}});
        output.push(lambda.invoke_vec(&fixture).unwrap());

        insta::assert_json_snapshot!(output);
    }
}
