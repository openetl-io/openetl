//! An overwrite stream whose read ends without rows: one with declared
//! types takes the schema they shape to, at the barrier that ends the read,
//! so the destination replaces its table; others are left as they were.

use arrow_schema::{Field, Schema};

use super::*;

/// The declared types of the stream `Users`.
fn declared() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("Name", DataType::Utf8, true),
        Field::new("price", DataType::Decimal128(12, 2), true),
    ])
}

/// Stream `Users`, overwritten and declared, read on `lanes` lanes.
fn overwrite(lanes: u32) -> openetl_proto::engine::v1::StartJobRequest {
    let mut req = crate::plan::tests::lanes_request(&["Users"], lanes, 1);
    req.streams[0].arrow_schema = ipc::encode_schema(&Arc::new(declared())).unwrap();
    req.streams[0].overwrite = true;
    req
}

fn ending(n: u64) -> Vec<u8> {
    Barrier {
        epoch: Some(Epoch { generation: 1, n }),
        ends_read: true,
    }
    .encode_to_vec()
}

/// Each ready epoch's schema of `Users`, if it has one.
fn schemas(ready: &[EpochReady]) -> Vec<(u64, Option<SchemaRef>)> {
    ready
        .iter()
        .map(|r| {
            let s = r.schemas.get("Users");
            (
                r.epoch.unwrap().n,
                s.map(|b| ipc::decode_schema(b).unwrap()),
            )
        })
        .collect()
}

fn columns(s: &SchemaRef) -> Vec<(String, DataType)> {
    s.fields()
        .iter()
        .filter(|f| !f.name().starts_with("_openetl_"))
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect()
}

/// Runs a job of one lane that reads `frames` of `Users`, then ends.
async fn run_one(
    req: openetl_proto::engine::v1::StartJobRequest,
    frames: Vec<(FrameKind, Vec<u8>)>,
) -> (Vec<frame::Frame>, Vec<EpochReady>) {
    let mut h = start_lanes(req);
    for (kind, payload) in frames {
        put(&mut h.ins[0], kind, 0, payload).await;
    }
    h.end_all(1).await;
    let (mut out, ready) = h.finish().await;
    (out.remove(0), ready)
}

#[tokio::test]
async fn an_empty_overwrite_takes_its_declared_schema_where_its_read_ends() {
    let (out, ready) = run_one(
        overwrite(1),
        vec![
            (FrameKind::Barrier, barrier(1)),
            (FrameKind::Barrier, ending(2)),
        ],
    )
    .await;
    assert!(out.iter().all(|f| f.kind != FrameKind::Batch));
    let s = schemas(&ready);
    assert_eq!(s.len(), 2);
    assert_eq!(s[0], (1, None), "before its read ends, it has none");
    let (n, Some(schema)) = &s[1] else {
        panic!("no schema where the read ends: {s:?}")
    };
    assert_eq!(*n, 2);
    assert_eq!(
        columns(schema),
        vec![
            ("id".into(), DataType::Int64),
            ("name".into(), DataType::Utf8),
            ("price".into(), DataType::Decimal128(12, 2)),
        ]
    );
    // Named as the stream's rows would be: the Namer's table, and the
    // metadata columns.
    assert_eq!(
        schema.metadata().get(naming::TABLE_KEY).map(String::as_str),
        Some("users")
    );
    let names = naming::source_names(schema).unwrap();
    assert_eq!(names.get("name").map(String::as_str), Some("Name"));
    for m in [
        "_openetl_id",
        "_openetl_op",
        "_openetl_seq",
        "_openetl_synced_at",
    ] {
        assert!(schema.field_with_name(m).is_ok(), "{m} is missing");
    }
}

#[tokio::test]
async fn an_overwrite_with_rows_on_one_lane_keeps_the_schema_its_rows_give() {
    // Lane 1's split had no rows and placed the read's last barrier, so its
    // barrier ends the read; lane 0's rows have no Name or price.
    for flagged in [vec![1], vec![0, 1]] {
        let mut h = start_lanes(overwrite(2));
        put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[r#"{"id": 1}"#])).await;
        for l in 0..2 {
            let b = if flagged.contains(&l) {
                ending(1)
            } else {
                barrier(1)
            };
            put(&mut h.ins[l], FrameKind::Barrier, 0, b).await;
        }
        h.end_all(1).await;
        let (_, ready) = h.finish().await;
        let s = schemas(&ready);
        let (1, Some(schema)) = &s[0] else {
            panic!("{s:?}")
        };
        assert_eq!(columns(schema), vec![("id".into(), DataType::Int64)]);
    }
}

#[tokio::test]
async fn an_empty_overwrite_without_declared_types_has_no_schema() {
    let mut req = overwrite(1);
    req.streams[0].arrow_schema = Vec::new();
    let (_, ready) = run_one(req, vec![(FrameKind::Barrier, ending(1))]).await;
    assert_eq!(schemas(&ready), vec![(1, None)]);
}

#[tokio::test]
async fn an_empty_append_or_merge_has_no_schema() {
    for soft_deletes in [false, true] {
        let mut req = overwrite(1);
        req.streams[0].overwrite = false;
        req.streams[0].soft_deletes = soft_deletes;
        req.streams[0].primary_key = vec!["id".into()];
        let (_, ready) = run_one(req, vec![(FrameKind::Barrier, ending(1))]).await;
        assert_eq!(schemas(&ready), vec![(1, None)]);
    }
}

#[tokio::test]
async fn an_empty_overwrite_proposes_its_declared_schema_to_the_remote_owner() {
    let mut req = request(&["Users"]);
    req.streams[0].arrow_schema = ipc::encode_schema(&Arc::new(declared())).unwrap();
    req.streams[0].overwrite = true;
    req.remote_schemas = true;
    let mut h = start_owned(req);
    put(&mut h.to_engine, FrameKind::Barrier, 0, ending(1)).await;
    put(&mut h.to_engine, FrameKind::EndOfStream, 0, vec![]).await;
    h.to_engine.shutdown().await.unwrap();
    drain(&mut h.from_engine).await;
    h.job.await.unwrap().unwrap();
    let ready = ready(&mut h.events).await;
    let s = schemas(&ready);
    let (1, Some(schema)) = &s[0] else {
        panic!("{s:?}")
    };
    assert_eq!(columns(schema).len(), 3);
}

/// The schema an empty overwrite's read records is the one a later read
/// whose rows carry only declared columns keeps: restored from it, those
/// rows change nothing, on either lane, with a declared cursor or none.
#[tokio::test]
async fn rows_after_an_empty_overwrite_keep_its_schema() {
    let typed = RecordBatch::try_new(
        Arc::new(declared()),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1])),
            Arc::new(arrow_array::StringArray::from(vec!["a"])),
            Arc::new(
                arrow_array::Decimal128Array::from(vec![150])
                    .with_precision_and_scale(12, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    for cursor in ["", "id"] {
        let stream = |restored: &[u8]| {
            let mut req = overwrite(1);
            req.streams[0].cursor_field = cursor.into();
            req.streams[0].restored_schema = restored.to_vec();
            req
        };
        let (_, ready) = run_one(stream(&[]), vec![(FrameKind::Barrier, ending(1))]).await;
        let encoded = ready[0].schemas.get("Users").unwrap().clone();
        let empty = ipc::decode_schema(&encoded).unwrap();
        assert!(empty.field_with_name("_openetl_seq").is_ok());

        let reads = [
            (
                FrameKind::Rows,
                rows(&[r#"{"price": "1.50", "Name": "a", "id": 1}"#]),
            ),
            (FrameKind::Batch, ipc::encode_batch(&typed).unwrap()),
        ];
        for (kind, payload) in reads {
            let (out, ready) = run_one(
                stream(&encoded),
                vec![(kind, payload), (FrameKind::Barrier, ending(1))],
            )
            .await;
            assert!(out.iter().any(|f| f.kind == FrameKind::Batch));
            let s = schemas(&ready);
            assert_eq!(
                s,
                vec![(1, Some(empty.clone()))],
                "{kind:?}, cursor {cursor:?}"
            );
        }
    }
}

/// Without its cursor's declared type, an empty overwrite's schema would
/// lack the _openetl_seq its first rows give, so it takes none.
#[tokio::test]
async fn an_empty_overwrite_whose_cursor_has_no_declared_type_has_no_schema() {
    let mut req = overwrite(1);
    req.streams[0].cursor_field = "updated_at".into();
    let (_, ready) = run_one(req, vec![(FrameKind::Barrier, ending(1))]).await;
    assert_eq!(schemas(&ready), vec![(1, None)]);
}
