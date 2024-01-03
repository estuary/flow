use futures::TryStreamExt;
use proto_gazette::broker;
use tracing_subscriber::{filter::LevelFilter, EnvFilter};

#[tokio::test]
async fn foobar() -> anyhow::Result<()> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    tracing::info!("hello");

    let router = gazette::journal::Router::new(
        "http://localhost:8080",
        gazette::Interceptor::new(None)?,
        "local",
    )?;
    let client = gazette::journal::Client::new(reqwest::Client::new(), router);

    let rr = broker::ReadRequest {
        begin_mod_time: 0,
        block: false,
        do_not_proxy: false,
        end_offset: 0,
        header: None,
        journal: "estuary/mahdi-test/events/pivot=00".to_string(),
        metadata_only: true,
        offset: 0,
    };

    let stream = client.read(rr);
    tokio::pin!(stream);

    let mut offset = 0;

    while let Some(mut resp) = stream.try_next().await? {
        let content_len = resp.content.len();
        resp.content = Default::default();

        offset = resp.offset + content_len as i64;
        tracing::info!(?resp, offset, "got resp");
    }

    tracing::info!(offset, "goodbye");

    Ok(())
}

#[test]
fn parse_test() -> anyhow::Result<()> {
    let fixture = b"[0,1,[2,3]] \"hi hello world\"\n42\n[1,true] \"\" false\n \"extra ";
    const MAX_DOC: usize = 1 << 20; // 1MB.

    // dom api.
    {
        use simdjson_rust::dom;

        let mut parser = dom::Parser::new(MAX_DOC);

        let mut input = fixture.to_vec();
        input.extend_from_slice(&[0; simdjson_rust::SIMDJSON_PADDING]);
        input.truncate(fixture.len());

        let stream = parser.parse_batch_v2(&input, MAX_DOC)?;
        let mut it = stream.iter();

        let mut alloc = doc::Allocator::new();

        while let Some(elem) = it.next() {
            alloc.reset();

            let node = elem_to_heap_node(&alloc, elem?);
            let out = serde_json::to_string(&doc::SerPolicy::default().on(&node)).unwrap();

            eprintln!("current_index {}: {out}", it.current_index(),);
        }

        eprintln!("truncated: {}", stream.truncated_bytes());
    }

    Ok(())
}

fn elem_to_heap_node<'a>(
    alloc: &'a doc::Allocator,
    elem: simdjson_rust::dom::Element<'_>,
) -> doc::HeapNode<'a> {
    use simdjson_rust::dom::ElementType;

    match elem.get_type() {
        ElementType::Array => {
            let arr = elem.get_array().expect("get_type is array");
            let vec = doc::BumpVec::with_contents(
                alloc,
                arr.iter().map(|elem| elem_to_heap_node(alloc, elem)),
            );
            doc::HeapNode::Array(vec)
        }
        ElementType::Object => {
            let obj = elem.get_object().expect("get_type is object");

            let mut is_sorted = true;
            let mut last_property = "";

            let mut vec = doc::BumpVec::with_contents(
                alloc,
                obj.iter()
                    .map(|(property, elem)| {
                        if property < last_property {
                            is_sorted = false;
                        }
                        last_property = property;

                        doc::HeapField {
                            property: doc::BumpStr::from_str(property, alloc),
                            value: elem_to_heap_node(alloc, elem),
                        }
                    })
                    .collect::<Vec<_>>()
                    .into_iter(),
            );

            if !is_sorted {
                vec.sort_by(|lhs, rhs| lhs.property.as_str().cmp(rhs.property.as_str()));
            }
            doc::HeapNode::Object(vec)
        }
        ElementType::Bool => {
            let b = elem.get_bool().expect("get_type is bool");
            doc::HeapNode::Bool(b)
        }
        ElementType::Double => {
            let d = elem.get_double().expect("get_type is double");
            doc::HeapNode::Float(d)
        }
        ElementType::Int64 => {
            let i = elem.get_int64().expect("get_type is int64");

            if i < 0 {
                doc::HeapNode::NegInt(i)
            } else {
                doc::HeapNode::PosInt(i as u64)
            }
        }
        ElementType::NullValue => doc::HeapNode::Null,
        ElementType::String => {
            let s = elem.get_string().expect("get_type is string");
            doc::HeapNode::String(doc::BumpStr::from_str(s, alloc))
        }
        ElementType::UInt64 => {
            let i = elem.get_uint64().expect("get_type is uint64");
            doc::HeapNode::PosInt(i)
        }
    }
}

/*
pub fn do_stuff(
    stream: impl futures::Stream<Item = Result<broker::ReadResponse, gazette::Error>>,
) -> impl futures::Stream<Item = Result<broker::ReadResponse, gazette::Error>> {
    use simdjson_sys as ffi;

    coroutines::try_coroutine(move |mut co| async move {
        let mut stream = std::pin::pin!(stream);
        let mut chunk = Vec::new();

        while let Some(mut resp) = stream.try_next().await? {
            if resp.fragment.is_some() {
                () = co.yield_(resp).await;
                continue;
            }
            chunk.extend_from_slice(&resp.content);

            if let None = ::memchr::memrchr(b'\n', &chunk) {
                continue;
            }

            // Add required simdjson padding.
            chunk.extend_from_slice(&[0; 8]);




            //() = co.yield_(resp).await;
        }
        Ok(())
    })
}
*/
