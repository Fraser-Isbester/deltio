use crate::api::page_token::PageToken;
use crate::api::parser;
use crate::pubsub_proto::publisher_server::Publisher;
use crate::pubsub_proto::*;
use crate::topics::TopicName;
use crate::topics::topic_manager::TopicManager;
use crate::topics::{
    CreateTopicError, DeleteError, GetTopicError, ListSubscriptionsError, ListTopicsError,
    PublishMessagesError,
};
use crate::schemas::schema_manager::{SchemaManager, SchemaManagerError};
use crate::schemas::schema_name::SchemaName;
use crate::tracing::ActivitySpan;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct PublisherService {
    pub topic_manager: Arc<TopicManager>,
    pub schema_manager: Arc<SchemaManager>,
}

impl PublisherService {
    pub fn new(topic_manager: Arc<TopicManager>, schema_manager: Arc<SchemaManager>) -> Self {
        Self {
            topic_manager,
            schema_manager,
        }
    }

    /// Gets the internal topic.
    async fn get_topic_internal(
        &self,
        topic_name: &TopicName,
    ) -> Result<Arc<crate::topics::Topic>, Status> {
        self.topic_manager
            .get_topic(topic_name)
            .map_err(|e| match e {
                GetTopicError::DoesNotExist => topic_not_found(topic_name),
                GetTopicError::Closed => Status::internal("System is shutting down"),
            })
    }
}

#[async_trait::async_trait]
impl Publisher for PublisherService {
    async fn create_topic(&self, request: Request<Topic>) -> Result<Response<Topic>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let topic_name = parser::parse_topic_name(&request.name)?;
        let topic_name_str = topic_name.to_string();

        if let Some(settings) = &request.schema_settings {
            if !settings.schema.is_empty() {
                let parsed_schema_name = SchemaName::try_parse(&settings.schema)
                    .ok_or_else(|| Status::invalid_argument("Invalid schema name in schema_settings"))?;
                self.schema_manager
                    .get_schema(&parsed_schema_name, SchemaView::Basic)
                    .map_err(|e| match e {
                        SchemaManagerError::NotFound => {
                            Status::not_found(format!("Schema not found: {}", settings.schema))
                        }
                        _ => Status::invalid_argument(e.to_string()),
                    })?;
            }
        }

        let topic = self
            .topic_manager
            .create_topic_with_schema(topic_name, request.schema_settings.clone())
            .map_err(|e| match e {
                CreateTopicError::AlreadyExists => Status::already_exists("Topic already exists"),
                CreateTopicError::Closed => conflict(),
            })?;

        let response = Topic {
            name: topic_name_str.clone(),
            kms_key_name: String::default(),
            labels: HashMap::default(),
            message_retention_duration: None,
            satisfies_pzs: false,
            schema_settings: topic.info.schema_settings.clone(),
            message_storage_policy: None,
        };

        log::debug!("{}: creating topic {}", topic_name_str, start);
        Ok(Response::new(response))
    }

    async fn update_topic(
        &self,
        _request: Request<UpdateTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        Err(Status::unimplemented(
            "UpdateTopic is not implemented in Deltio",
        ))
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let start = ActivitySpan::start();
        let request = request.into_inner();
        let topic_name = parser::parse_topic_name(&request.topic)?;

        let topic = self.get_topic_internal(&topic_name).await?;

        let message_count = request.messages.len();
        let mut messages = Vec::with_capacity(message_count);

        if let Some(schema_settings) = &topic.info.schema_settings {
            let (validator, active_revision_id) = self
                .schema_manager
                .resolve_validator(&schema_settings.schema)
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "Schema not found: {}",
                        schema_settings.schema
                    ))
                })?;

            let encoding =
                Encoding::try_from(schema_settings.encoding).unwrap_or(Encoding::Unspecified);
            if encoding == Encoding::Unspecified {
                return Err(Status::invalid_argument("Schema encoding must be set on topic"));
            }

            let encoding_str = match encoding {
                Encoding::Binary => "BINARY",
                Encoding::Json => "JSON",
                Encoding::Unspecified => "UNSPECIFIED",
            };

            let parsed_name = SchemaName::try_parse(&schema_settings.schema)
                .ok_or_else(|| Status::invalid_argument("Invalid schema name in schema settings"))?;
            let canonical_schema_name = parsed_name.name_without_revision();

            for raw_msg in request.messages {
                validator
                    .validate(&raw_msg.data, encoding)
                    .map_err(|e| Status::invalid_argument(format!("Message failed schema validation: {e}")))?;

                let mut topic_msg = parser::parse_topic_message(raw_msg);
                let attrs = topic_msg.attributes.get_or_insert_with(HashMap::new);
                attrs.insert("googclient_schemaname".into(), canonical_schema_name.clone());
                attrs.insert("googclient_schemaencoding".into(), encoding_str.into());
                attrs.insert(
                    "googclient_schemarevisionid".into(),
                    active_revision_id.clone(),
                );

                messages.push(topic_msg);
            }
        } else {
            for raw_msg in request.messages {
                messages.push(parser::parse_topic_message(raw_msg));
            }
        }

        let result = topic
            .publish_messages(messages)
            .await
            .map_err(|e| match e {
                PublishMessagesError::TopicDoesNotExist => topic_not_found(&topic_name),
                PublishMessagesError::Closed => conflict(),
            })?;

        let response = Response::new(PublishResponse {
            message_ids: result.message_ids.iter().map(|m| m.to_string()).collect(),
        });

        log::debug!(
            "{}: publishing {} messages {}",
            &topic_name,
            message_count,
            start
        );

        Ok(response)
    }

    async fn get_topic(
        &self,
        request: Request<GetTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let topic_name = parser::parse_topic_name(&request.topic)?;

        let topic = self.get_topic_internal(&topic_name).await?;

        log::debug!("{}: getting topic {}", &topic_name, start);
        Ok(Response::new(Topic {
            name: topic.name.to_string(),
            labels: Default::default(),
            message_storage_policy: None,
            kms_key_name: "".to_string(),
            schema_settings: topic.info.schema_settings.clone(),
            satisfies_pzs: false,
            message_retention_duration: None,
        }))
    }

    async fn list_topics(
        &self,
        request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let paging = parser::parse_paging(request.page_size, &request.page_token)?;
        let project_id = parser::parse_project_id(&request.project)?;

        let page = self
            .topic_manager
            .list_topics(Box::from(project_id), paging)
            .map_err(|e| match e {
                ListTopicsError::Closed => conflict(),
            })?;

        let topics = page
            .topics
            .into_iter()
            .map(|topic| Topic {
                name: topic.name.to_string(),
                labels: HashMap::default(),
                message_storage_policy: None,
                kms_key_name: "".to_string(),
                schema_settings: topic.info.schema_settings.clone(),
                satisfies_pzs: false,
                message_retention_duration: None,
            })
            .collect();

        let page_token = page.offset.map(|v| PageToken::new(v).encode());
        let response = ListTopicsResponse {
            topics,
            next_page_token: page_token.unwrap_or(String::default()),
        };

        log::debug!(
            "{}: listing {} topics {}",
            &request.project,
            response.topics.len(),
            start
        );
        Ok(Response::new(response))
    }

    async fn list_topic_subscriptions(
        &self,
        request: Request<ListTopicSubscriptionsRequest>,
    ) -> Result<Response<ListTopicSubscriptionsResponse>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let topic_name = parser::parse_topic_name(&request.topic)?;

        let paging = parser::parse_paging(request.page_size, &request.page_token)?;

        let topic = self.get_topic_internal(&topic_name).await?;

        let page = topic
            .list_subscriptions(paging)
            .await
            .map_err(|e| match e {
                ListSubscriptionsError::Closed => conflict(),
            })?;

        log::debug!(
            "{}: listing {} subscriptions {}",
            &topic_name,
            page.subscriptions.len(),
            start
        );
        Ok(Response::new(ListTopicSubscriptionsResponse {
            subscriptions: page
                .subscriptions
                .iter()
                .map(|s| s.name.to_string())
                .collect(),
            next_page_token: page
                .offset
                .map(|o| PageToken::new(o).encode())
                .unwrap_or(String::default()),
        }))
    }

    async fn list_topic_snapshots(
        &self,
        _request: Request<ListTopicSnapshotsRequest>,
    ) -> Result<Response<ListTopicSnapshotsResponse>, Status> {
        Err(Status::unimplemented(
            "ListTopic_snapshots is not implemented in Deltio",
        ))
    }

    async fn delete_topic(
        &self,
        request: Request<DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();

        let topic_name = parser::parse_topic_name(&request.topic)?;
        let topic = self.get_topic_internal(&topic_name).await?;

        topic.delete().await.map_err(|e| match e {
            DeleteError::Closed => conflict(),
        })?;

        log::debug!("{}: deleting topic {}", &topic_name, start);
        Ok(Response::new(()))
    }

    async fn detach_subscription(
        &self,
        _request: Request<DetachSubscriptionRequest>,
    ) -> Result<Response<DetachSubscriptionResponse>, Status> {
        Err(Status::unimplemented(
            "DetachSubscription is not implemented in Deltio",
        ))
    }
}

/// Status for when returned errors indicate that the resource is no longer
/// accepting requests, which usually indicates that it has been deleted, or
/// that the system is currently shutting down. The former is more likely.
#[inline]
fn conflict() -> Status {
    Status::failed_precondition("The operation resulted in a conflict.")
}

/// Returns a status indicating that the resource was not found.
#[inline]
fn topic_not_found(topic_name: &TopicName) -> Status {
    Status::not_found(format!(
        "Resource not found (resource={}).",
        &topic_name.topic_id()
    ))
}
