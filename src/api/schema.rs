use crate::api::page_token::PageToken;
use crate::api::parser;
use crate::pubsub_proto::schema_service_server::SchemaService;
use crate::pubsub_proto::validate_message_request::SchemaSpec;
use crate::pubsub_proto::*;
use crate::schemas::avro::AvroValidator;
use crate::schemas::proto::ProtoValidator;
use crate::schemas::schema_manager::{SchemaManager, SchemaManagerError};
use crate::schemas::schema_name::SchemaName;
use crate::schemas::types::SchemaType;
use crate::schemas::validator::SchemaValidator;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// Implementation of the Google Cloud Pub/Sub SchemaService gRPC API.
pub struct SchemaServiceImpl {
    pub schema_manager: Arc<SchemaManager>,
}

impl SchemaServiceImpl {
    /// Creates a new `SchemaServiceImpl`.
    pub fn new(schema_manager: Arc<SchemaManager>) -> Self {
        Self { schema_manager }
    }
}

#[async_trait::async_trait]
impl SchemaService for SchemaServiceImpl {
    async fn create_schema(
        &self,
        request: Request<CreateSchemaRequest>,
    ) -> Result<Response<Schema>, Status> {
        let req = request.into_inner();
        let project_id = parser::parse_project_id(&req.parent)?;

        let schema_proto = req
            .schema
            .ok_or_else(|| Status::invalid_argument("schema is required"))?;

        let schema_id = if !req.schema_id.trim().is_empty() {
            req.schema_id.trim().to_string()
        } else if let Some(parsed) = SchemaName::try_parse(&schema_proto.name) {
            parsed.schema_id().to_string()
        } else {
            return Err(Status::invalid_argument(
                "schema_id is required in request or schema.name",
            ));
        };

        let schema_type = SchemaType::from_i32(schema_proto.r#type).ok_or_else(|| {
            Status::invalid_argument(format!("Invalid schema type: {}", schema_proto.r#type))
        })?;

        if schema_proto.definition.trim().is_empty() {
            return Err(Status::invalid_argument("schema definition cannot be empty"));
        }

        let name = SchemaName::new(project_id, schema_id);
        let created = self
            .schema_manager
            .create_schema(name, schema_type, schema_proto.definition)
            .map_err(|e| match e {
                SchemaManagerError::AlreadyExists => {
                    Status::already_exists("Schema already exists")
                }
                SchemaManagerError::Validation(val_err) => {
                    Status::invalid_argument(val_err.to_string())
                }
                other => Status::internal(other.to_string()),
            })?;

        Ok(Response::new(created))
    }

    async fn get_schema(&self, request: Request<GetSchemaRequest>) -> Result<Response<Schema>, Status> {
        let req = request.into_inner();
        let name = SchemaName::try_parse(&req.name)
            .ok_or_else(|| Status::invalid_argument("Invalid schema name"))?;

        let view = SchemaView::try_from(req.view).unwrap_or(SchemaView::Full);

        let schema = self
            .schema_manager
            .get_schema(&name, view)
            .map_err(|e| match e {
                SchemaManagerError::NotFound => Status::not_found("Schema not found"),
                SchemaManagerError::RevisionNotFound => {
                    Status::not_found("Schema revision not found")
                }
                other => Status::internal(other.to_string()),
            })?;

        Ok(Response::new(schema))
    }

    async fn list_schemas(
        &self,
        request: Request<ListSchemasRequest>,
    ) -> Result<Response<ListSchemasResponse>, Status> {
        let req = request.into_inner();
        let project_id = parser::parse_project_id(&req.parent)?;
        let paging = parser::parse_paging(req.page_size, &req.page_token)?;
        let view = SchemaView::try_from(req.view).unwrap_or(SchemaView::Basic);

        let (schemas, next_offset) =
            self.schema_manager.list_schemas(&project_id, paging, view);
        let next_page_token = next_offset
            .map(|o| PageToken::new(o).encode())
            .unwrap_or_default();

        Ok(Response::new(ListSchemasResponse {
            schemas,
            next_page_token,
        }))
    }

    async fn list_schema_revisions(
        &self,
        request: Request<ListSchemaRevisionsRequest>,
    ) -> Result<Response<ListSchemaRevisionsResponse>, Status> {
        let req = request.into_inner();
        let name = SchemaName::try_parse(&req.name)
            .ok_or_else(|| Status::invalid_argument("Invalid schema name"))?;
        let paging = parser::parse_paging(req.page_size, &req.page_token)?;
        let view = SchemaView::try_from(req.view).unwrap_or(SchemaView::Basic);

        let (schemas, next_offset) = self
            .schema_manager
            .list_schema_revisions(&name, paging, view)
            .map_err(|e| match e {
                SchemaManagerError::NotFound => Status::not_found("Schema not found"),
                other => Status::internal(other.to_string()),
            })?;
        let next_page_token = next_offset
            .map(|o| PageToken::new(o).encode())
            .unwrap_or_default();

        Ok(Response::new(ListSchemaRevisionsResponse {
            schemas,
            next_page_token,
        }))
    }

    async fn commit_schema(
        &self,
        request: Request<CommitSchemaRequest>,
    ) -> Result<Response<Schema>, Status> {
        let req = request.into_inner();
        let name = SchemaName::try_parse(&req.name)
            .ok_or_else(|| Status::invalid_argument("Invalid schema name"))?;

        let schema_proto = req
            .schema
            .ok_or_else(|| Status::invalid_argument("schema is required"))?;

        let schema_type = SchemaType::from_i32(schema_proto.r#type).ok_or_else(|| {
            Status::invalid_argument(format!("Invalid schema type: {}", schema_proto.r#type))
        })?;

        if schema_proto.definition.trim().is_empty() {
            return Err(Status::invalid_argument("schema definition cannot be empty"));
        }

        let committed = self
            .schema_manager
            .commit_schema(&name, schema_type, schema_proto.definition)
            .map_err(|e| match e {
                SchemaManagerError::NotFound => Status::not_found("Schema not found"),
                SchemaManagerError::Validation(val_err) => {
                    Status::invalid_argument(val_err.to_string())
                }
                other => Status::internal(other.to_string()),
            })?;

        Ok(Response::new(committed))
    }

    async fn rollback_schema(
        &self,
        request: Request<RollbackSchemaRequest>,
    ) -> Result<Response<Schema>, Status> {
        let req = request.into_inner();
        let name = SchemaName::try_parse(&req.name)
            .ok_or_else(|| Status::invalid_argument("Invalid schema name"))?;

        if req.revision_id.trim().is_empty() {
            return Err(Status::invalid_argument("revision_id is required"));
        }

        let rolled_back = self
            .schema_manager
            .rollback_schema(&name, req.revision_id.trim())
            .map_err(|e| match e {
                SchemaManagerError::NotFound => Status::not_found("Schema not found"),
                SchemaManagerError::RevisionNotFound => {
                    Status::not_found("Target revision not found")
                }
                other => Status::internal(other.to_string()),
            })?;

        Ok(Response::new(rolled_back))
    }

    async fn delete_schema_revision(
        &self,
        request: Request<DeleteSchemaRevisionRequest>,
    ) -> Result<Response<Schema>, Status> {
        let req = request.into_inner();
        let parsed_name = SchemaName::try_parse(&req.name)
            .ok_or_else(|| Status::invalid_argument("Invalid schema name"))?;

        #[allow(deprecated)]
        let rev_id = match parsed_name.revision_id() {
            Some(rev) => rev.to_string(),
            None if !req.revision_id.trim().is_empty() => req.revision_id.trim().to_string(),
            _ => {
                return Err(Status::invalid_argument(
                    "Revision ID must be specified in name (@revision) or revision_id",
                ));
            }
        };

        let deleted = self
            .schema_manager
            .delete_schema_revision(&parsed_name, &rev_id)
            .map_err(|e| match e {
                SchemaManagerError::NotFound => Status::not_found("Schema not found"),
                SchemaManagerError::RevisionNotFound => {
                    Status::not_found("Revision not found")
                }
                SchemaManagerError::CannotDeleteLastRevision => Status::failed_precondition(
                    "Cannot delete the only revision of a schema (delete the schema instead)",
                ),
                other => Status::internal(other.to_string()),
            })?;

        Ok(Response::new(deleted))
    }

    async fn delete_schema(
        &self,
        request: Request<DeleteSchemaRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let name = SchemaName::try_parse(&req.name)
            .ok_or_else(|| Status::invalid_argument("Invalid schema name"))?;

        self.schema_manager
            .delete_schema(&name)
            .map_err(|e| match e {
                SchemaManagerError::NotFound => Status::not_found("Schema not found"),
                other => Status::internal(other.to_string()),
            })?;

        Ok(Response::new(()))
    }

    async fn validate_schema(
        &self,
        request: Request<ValidateSchemaRequest>,
    ) -> Result<Response<ValidateSchemaResponse>, Status> {
        let req = request.into_inner();
        let _project_id = parser::parse_project_id(&req.parent)?;

        let schema_proto = req
            .schema
            .ok_or_else(|| Status::invalid_argument("schema is required"))?;

        let schema_type = SchemaType::from_i32(schema_proto.r#type).ok_or_else(|| {
            Status::invalid_argument(format!("Invalid schema type: {}", schema_proto.r#type))
        })?;

        match schema_type {
            SchemaType::ProtocolBuffer => {
                ProtoValidator::new(&schema_proto.definition)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
            }
            SchemaType::Avro => {
                AvroValidator::new(&schema_proto.definition)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
            }
        }

        Ok(Response::new(ValidateSchemaResponse {}))
    }

    async fn validate_message(
        &self,
        request: Request<ValidateMessageRequest>,
    ) -> Result<Response<ValidateMessageResponse>, Status> {
        let req = request.into_inner();
        let encoding = Encoding::try_from(req.encoding).unwrap_or(Encoding::Unspecified);
        if encoding == Encoding::Unspecified {
            return Err(Status::invalid_argument(
                "encoding must be specified (JSON or BINARY)",
            ));
        }

        let schema_spec = req
            .schema_spec
            .ok_or_else(|| Status::invalid_argument("schema_spec is required"))?;

        match schema_spec {
            SchemaSpec::Name(name) => {
                let (validator, _) = self
                    .schema_manager
                    .resolve_validator(&name)
                    .ok_or_else(|| Status::not_found(format!("Schema not found: {name}")))?;

                validator
                    .validate(&req.message, encoding)
                    .map_err(|e| Status::invalid_argument(format!("Message validation failed: {e}")))?;
            }
            SchemaSpec::Schema(schema_proto) => {
                let schema_type = SchemaType::from_i32(schema_proto.r#type).ok_or_else(|| {
                    Status::invalid_argument(format!("Invalid schema type: {}", schema_proto.r#type))
                })?;

                let validator: Box<dyn SchemaValidator> = match schema_type {
                    SchemaType::ProtocolBuffer => Box::new(
                        ProtoValidator::new(&schema_proto.definition)
                            .map_err(|e| Status::invalid_argument(e.to_string()))?,
                    ),
                    SchemaType::Avro => Box::new(
                        AvroValidator::new(&schema_proto.definition)
                            .map_err(|e| Status::invalid_argument(e.to_string()))?,
                    ),
                };

                validator
                    .validate(&req.message, encoding)
                    .map_err(|e| Status::invalid_argument(format!("Message validation failed: {e}")))?;
            }
        }

        Ok(Response::new(ValidateMessageResponse {}))
    }
}
