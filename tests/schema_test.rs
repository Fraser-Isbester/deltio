#![allow(deprecated)]

mod test_helpers;

use deltio::pubsub_proto::schema::Type as SchemaTypeProto;
use deltio::pubsub_proto::validate_message_request::SchemaSpec;
use deltio::pubsub_proto::*;
use prost::Message;
use prost_reflect::DynamicMessage;
use test_helpers::*;

const AVRO_USER_SCHEMA: &str = r#"{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
    ]
}"#;

const AVRO_USER_SCHEMA_V2: &str = r#"{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "email", "type": "string", "default": "unknown"}
    ]
}"#;

const PROTO_PERSON_SCHEMA: &str = r#"
    syntax = "proto3";
    package myproject;

    message Person {
        string name = 1;
        int32 age = 2;
    }
"#;

#[tokio::test]
async fn test_schema_lifecycle_avro() {
    let mut host = TestHost::start().await.unwrap();

    let project = "projects/test-proj";
    let schema_id = "user-schema";
    let schema_name = format!("{}/schemas/{}", project, schema_id);

    // 1. Create schema
    let create_res = host
        .schema
        .create_schema(CreateSchemaRequest {
            parent: project.to_string(),
            schema_id: schema_id.to_string(),
            schema: Some(Schema {
                name: schema_name.clone(),
                r#type: SchemaTypeProto::Avro as i32,
                definition: AVRO_USER_SCHEMA.to_string(),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(create_res.name, schema_name);
    assert_eq!(create_res.r#type, SchemaTypeProto::Avro as i32);
    assert_eq!(create_res.definition, AVRO_USER_SCHEMA);
    let rev1_id = create_res.revision_id.clone();
    assert!(!rev1_id.is_empty());

    // Duplicate create should fail
    let dup_err = host
        .schema
        .create_schema(CreateSchemaRequest {
            parent: project.to_string(),
            schema_id: schema_id.to_string(),
            schema: Some(Schema {
                name: schema_name.clone(),
                r#type: SchemaTypeProto::Avro as i32,
                definition: AVRO_USER_SCHEMA.to_string(),
                ..Default::default()
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(dup_err.code(), tonic::Code::AlreadyExists);

    // 2. Get schema
    let get_res = host
        .schema
        .get_schema(GetSchemaRequest {
            name: schema_name.clone(),
            view: SchemaView::Full as i32,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(get_res.name, schema_name);
    assert_eq!(get_res.revision_id, rev1_id);
    assert_eq!(get_res.definition, AVRO_USER_SCHEMA);

    // 3. Commit new revision
    let commit_res = host
        .schema
        .commit_schema(CommitSchemaRequest {
            name: schema_name.clone(),
            schema: Some(Schema {
                name: schema_name.clone(),
                r#type: SchemaTypeProto::Avro as i32,
                definition: AVRO_USER_SCHEMA_V2.to_string(),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();

    let rev2_id = commit_res.revision_id.clone();
    assert_ne!(rev1_id, rev2_id);
    assert_eq!(commit_res.definition, AVRO_USER_SCHEMA_V2);

    // 4. List revisions
    let revs_res = host
        .schema
        .list_schema_revisions(ListSchemaRevisionsRequest {
            name: schema_name.clone(),
            page_size: 10,
            page_token: String::new(),
            view: SchemaView::Full as i32,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(revs_res.schemas.len(), 2);

    // 5. Rollback to rev1
    let rollback_res = host
        .schema
        .rollback_schema(RollbackSchemaRequest {
            name: schema_name.clone(),
            revision_id: rev1_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    // Rollback creates a new revision with rev1's definition
    assert_eq!(rollback_res.definition, AVRO_USER_SCHEMA);
    let rev3_id = rollback_res.revision_id.clone();
    assert_ne!(rev3_id, rev1_id);
    assert_ne!(rev3_id, rev2_id);

    // 6. Delete revision 2
    let del_rev_res = host
        .schema
        .delete_schema_revision(DeleteSchemaRevisionRequest {
            name: format!("{}@{}", schema_name, rev2_id),
            revision_id: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(del_rev_res.revision_id, rev2_id);

    // 7. List schemas
    let list_res = host
        .schema
        .list_schemas(ListSchemasRequest {
            parent: project.to_string(),
            page_size: 10,
            page_token: String::new(),
            view: SchemaView::Full as i32,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res.schemas.len(), 1);
    assert_eq!(list_res.schemas[0].name, schema_name);

    // 8. Delete schema
    host.schema
        .delete_schema(DeleteSchemaRequest {
            name: schema_name.clone(),
        })
        .await
        .unwrap();

    // Verify it is deleted
    let not_found = host
        .schema
        .get_schema(GetSchemaRequest {
            name: schema_name.clone(),
            view: SchemaView::Full as i32,
        })
        .await
        .unwrap_err();
    assert_eq!(not_found.code(), tonic::Code::NotFound);

    host.dispose().await;
}

#[tokio::test]
async fn test_validate_schema_and_message_rpc() {
    let mut host = TestHost::start().await.unwrap();
    let project = "projects/val-proj";

    // 1. Validate valid protobuf schema definition
    host.schema
        .validate_schema(ValidateSchemaRequest {
            parent: project.to_string(),
            schema: Some(Schema {
                name: format!("{}/schemas/person", project),
                r#type: SchemaTypeProto::ProtocolBuffer as i32,
                definition: PROTO_PERSON_SCHEMA.to_string(),
                ..Default::default()
            }),
        })
        .await
        .unwrap();

    // 2. Validate invalid schema definition
    let invalid_schema_err = host
        .schema
        .validate_schema(ValidateSchemaRequest {
            parent: project.to_string(),
            schema: Some(Schema {
                name: format!("{}/schemas/bad", project),
                r#type: SchemaTypeProto::ProtocolBuffer as i32,
                definition: "syntax = broken;".to_string(),
                ..Default::default()
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(invalid_schema_err.code(), tonic::Code::InvalidArgument);

    // 3. Create stored proto schema for message validation
    let schema_name = format!("{}/schemas/person", project);
    host.schema
        .create_schema(CreateSchemaRequest {
            parent: project.to_string(),
            schema_id: "person".to_string(),
            schema: Some(Schema {
                name: schema_name.clone(),
                r#type: SchemaTypeProto::ProtocolBuffer as i32,
                definition: PROTO_PERSON_SCHEMA.to_string(),
                ..Default::default()
            }),
        })
        .await
        .unwrap();

    // 4. Validate valid JSON message against stored proto schema
    host.schema
        .validate_message(ValidateMessageRequest {
            parent: project.to_string(),
            encoding: Encoding::Json as i32,
            message: br#"{"name": "Alice", "age": 28}"#.to_vec(),
            schema_spec: Some(SchemaSpec::Name(schema_name.clone())),
        })
        .await
        .unwrap();

    // 5. Validate invalid JSON message against stored proto schema
    let bad_msg_err = host
        .schema
        .validate_message(ValidateMessageRequest {
            parent: project.to_string(),
            encoding: Encoding::Json as i32,
            message: br#"{"name": "Alice", "age": "not-a-number"}"#.to_vec(),
            schema_spec: Some(SchemaSpec::Name(schema_name.clone())),
        })
        .await
        .unwrap_err();
    assert_eq!(bad_msg_err.code(), tonic::Code::InvalidArgument);

    // 6. Validate ad-hoc Avro schema and message
    host.schema
        .validate_message(ValidateMessageRequest {
            parent: project.to_string(),
            encoding: Encoding::Json as i32,
            message: br#"{"name": "Bob", "age": 42}"#.to_vec(),
            schema_spec: Some(SchemaSpec::Schema(Schema {
                name: "".to_string(),
                r#type: SchemaTypeProto::Avro as i32,
                definition: AVRO_USER_SCHEMA.to_string(),
                ..Default::default()
            })),
        })
        .await
        .unwrap();

    host.dispose().await;
}

#[tokio::test]
async fn test_topic_schema_publish_validation_and_attribute_injection_json() {
    let mut host = TestHost::start().await.unwrap();
    let project = "projects/pub-val-proj";

    // 1. Create Avro Schema
    let schema_id = "user-avro";
    let schema_name = format!("{}/schemas/{}", project, schema_id);
    let created_schema = host
        .schema
        .create_schema(CreateSchemaRequest {
            parent: project.to_string(),
            schema_id: schema_id.to_string(),
            schema: Some(Schema {
                name: schema_name.clone(),
                r#type: SchemaTypeProto::Avro as i32,
                definition: AVRO_USER_SCHEMA.to_string(),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();

    let expected_revision_id = created_schema.revision_id;

    // 2. Create Topic with schema settings (JSON encoding)
    let topic_name = format!("{}/topics/user-events", project);
    let created_topic = host
        .publisher
        .create_topic(Topic {
            name: topic_name.clone(),
            schema_settings: Some(SchemaSettings {
                schema: schema_name.clone(),
                encoding: Encoding::Json as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    assert!(created_topic.schema_settings.is_some());
    let settings = created_topic.schema_settings.unwrap();
    assert_eq!(settings.schema, schema_name);
    assert_eq!(settings.encoding, Encoding::Json as i32);

    // 3. Create Subscription to read published messages
    let sub_name = format!("{}/subscriptions/user-sub", project);
    host.subscriber
        .create_subscription(Subscription {
            name: sub_name.clone(),
            topic: topic_name.clone(),
            ack_deadline_seconds: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    // 4. Publish invalid message -> should fail schema validation
    let bad_publish_err = host
        .publisher
        .publish(PublishRequest {
            topic: topic_name.clone(),
            messages: vec![PubsubMessage {
                data: br#"{"name": "Alice", "age": "invalid-age"}"#.to_vec(),
                ..Default::default()
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(bad_publish_err.code(), tonic::Code::InvalidArgument);

    // 5. Publish valid message -> should succeed and inject googclient_* attributes
    let valid_data = br#"{"name": "Alice", "age": 30}"#.to_vec();
    let publish_res = host
        .publisher
        .publish(PublishRequest {
            topic: topic_name.clone(),
            messages: vec![PubsubMessage {
                data: valid_data.clone(),
                ..Default::default()
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(publish_res.message_ids.len(), 1);

    // 6. Pull message and verify injected attributes
    let pull_res = host
        .subscriber
        .pull(PullRequest {
            subscription: sub_name.clone(),
            max_messages: 1,
            return_immediately: true,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(pull_res.received_messages.len(), 1);
    let received_msg = pull_res.received_messages[0]
        .message
        .as_ref()
        .expect("Message should be present");
    assert_eq!(received_msg.data, valid_data);

    // Check googclient_* attributes
    assert_eq!(
        received_msg.attributes.get("googclient_schemaname"),
        Some(&schema_name)
    );
    assert_eq!(
        received_msg.attributes.get("googclient_schemaencoding"),
        Some(&"JSON".to_string())
    );
    assert_eq!(
        received_msg.attributes.get("googclient_schemarevisionid"),
        Some(&expected_revision_id)
    );

    host.dispose().await;
}

#[tokio::test]
async fn test_topic_schema_publish_validation_binary_proto() {
    let mut host = TestHost::start().await.unwrap();
    let project = "projects/proto-proj";

    // 1. Create Protobuf Schema
    let schema_id = "person-proto";
    let schema_name = format!("{}/schemas/{}", project, schema_id);
    let created_schema = host
        .schema
        .create_schema(CreateSchemaRequest {
            parent: project.to_string(),
            schema_id: schema_id.to_string(),
            schema: Some(Schema {
                name: schema_name.clone(),
                r#type: SchemaTypeProto::ProtocolBuffer as i32,
                definition: PROTO_PERSON_SCHEMA.to_string(),
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();

    let expected_revision_id = created_schema.revision_id;

    // 2. Create Topic with schema settings (BINARY encoding)
    let topic_name = format!("{}/topics/proto-events", project);
    host.publisher
        .create_topic(Topic {
            name: topic_name.clone(),
            schema_settings: Some(SchemaSettings {
                schema: schema_name.clone(),
                encoding: Encoding::Binary as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // 3. Create Subscription
    let sub_name = format!("{}/subscriptions/proto-sub", project);
    host.subscriber
        .create_subscription(Subscription {
            name: sub_name.clone(),
            topic: topic_name.clone(),
            ack_deadline_seconds: 10,
            ..Default::default()
        })
        .await
        .unwrap();

    // 4. Encode valid protobuf message
    let file = protox::file::File::from_source("person.proto", PROTO_PERSON_SCHEMA).unwrap();
    let mut pool = prost_reflect::DescriptorPool::new();
    pool.add_file_descriptor_proto(file.file_descriptor_proto().clone())
        .unwrap();
    let desc = pool.all_messages().next().unwrap();
    let mut dyn_msg = DynamicMessage::new(desc);
    dyn_msg
        .try_set_field_by_name("name", prost_reflect::Value::String("Charlie".into()))
        .unwrap();
    dyn_msg
        .try_set_field_by_name("age", prost_reflect::Value::I32(35))
        .unwrap();
    let valid_proto_bytes = dyn_msg.encode_to_vec();

    // 5. Publish invalid binary bytes -> rejected
    let bad_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
    let bad_publish = host
        .publisher
        .publish(PublishRequest {
            topic: topic_name.clone(),
            messages: vec![PubsubMessage {
                data: bad_bytes,
                ..Default::default()
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(bad_publish.code(), tonic::Code::InvalidArgument);

    // 6. Publish valid binary message -> succeeds
    let pub_res = host
        .publisher
        .publish(PublishRequest {
            topic: topic_name.clone(),
            messages: vec![PubsubMessage {
                data: valid_proto_bytes.clone(),
                ..Default::default()
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pub_res.message_ids.len(), 1);

    // 7. Pull and verify attributes
    let pull_res = host
        .subscriber
        .pull(PullRequest {
            subscription: sub_name.clone(),
            max_messages: 1,
            return_immediately: true,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(pull_res.received_messages.len(), 1);
    let msg = pull_res.received_messages[0]
        .message
        .as_ref()
        .expect("Message present");
    assert_eq!(msg.data, valid_proto_bytes);
    assert_eq!(
        msg.attributes.get("googclient_schemaname"),
        Some(&schema_name)
    );
    assert_eq!(
        msg.attributes.get("googclient_schemaencoding"),
        Some(&"BINARY".to_string())
    );
    assert_eq!(
        msg.attributes.get("googclient_schemarevisionid"),
        Some(&expected_revision_id)
    );

    host.dispose().await;
}

#[tokio::test]
async fn test_create_topic_with_nonexistent_schema_fails() {
    let mut host = TestHost::start().await.unwrap();
    let project = "projects/err-proj";

    let err = host
        .publisher
        .create_topic(Topic {
            name: format!("{}/topics/orphan-topic", project),
            schema_settings: Some(SchemaSettings {
                schema: format!("{}/schemas/ghost-schema", project),
                encoding: Encoding::Json as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);

    host.dispose().await;
}
