use crate::pubsub_proto::schema::Type as SchemaTypeProto;
use crate::pubsub_proto::{Schema as SchemaProto, SchemaView};
use crate::schemas::avro::AvroValidator;
use crate::schemas::proto::ProtoValidator;
use crate::schemas::schema_name::SchemaName;
use crate::schemas::validator::{SchemaValidator, SchemaValidationError};
use std::sync::Arc;
use std::time::SystemTime;

/// The schema definition type (Protocol Buffer or Avro).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    ProtocolBuffer,
    Avro,
}

impl SchemaType {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            1 => Some(SchemaType::ProtocolBuffer),
            2 => Some(SchemaType::Avro),
            _ => None,
        }
    }

    pub fn to_i32(self) -> i32 {
        match self {
            SchemaType::ProtocolBuffer => SchemaTypeProto::ProtocolBuffer as i32,
            SchemaType::Avro => SchemaTypeProto::Avro as i32,
        }
    }
}

/// A specific revision of a schema.
pub struct SchemaRevision {
    pub revision_id: String,
    pub revision_create_time: SystemTime,
    pub schema_type: SchemaType,
    pub definition: String,
    pub validator: Arc<dyn SchemaValidator>,
}

impl Clone for SchemaRevision {
    fn clone(&self) -> Self {
        Self {
            revision_id: self.revision_id.clone(),
            revision_create_time: self.revision_create_time,
            schema_type: self.schema_type,
            definition: self.definition.clone(),
            validator: Arc::clone(&self.validator),
        }
    }
}

impl SchemaRevision {
    /// Creates a new schema revision, validating its definition.
    pub fn new(
        revision_id: String,
        schema_type: SchemaType,
        definition: String,
    ) -> Result<Self, SchemaValidationError> {
        let validator: Arc<dyn SchemaValidator> = match schema_type {
            SchemaType::ProtocolBuffer => Arc::new(ProtoValidator::new(&definition)?),
            SchemaType::Avro => Arc::new(AvroValidator::new(&definition)?),
        };

        Ok(Self {
            revision_id,
            revision_create_time: SystemTime::now(),
            schema_type,
            definition,
            validator,
        })
    }
}

/// A schema entity containing one or more revisions.
#[derive(Clone)]
pub struct Schema {
    pub name: SchemaName,
    pub revisions: Vec<SchemaRevision>,
}

impl Schema {
    /// Creates a new schema with its initial revision.
    pub fn new(name: SchemaName, initial_revision: SchemaRevision) -> Self {
        Self {
            name,
            revisions: vec![initial_revision],
        }
    }

    /// Returns the active (latest) revision.
    pub fn latest_revision(&self) -> &SchemaRevision {
        self.revisions
            .last()
            .expect("Schema must always have at least one revision")
    }

    /// Finds a revision by ID.
    pub fn get_revision(&self, revision_id: &str) -> Option<&SchemaRevision> {
        self.revisions.iter().find(|r| r.revision_id == revision_id)
    }

    /// Converts this schema to its protobuf representation.
    pub fn to_proto(&self, revision_opt: Option<&str>, view: SchemaView) -> Option<SchemaProto> {
        let (revision, formatted_name) = match revision_opt {
            Some(rev_id) => {
                let rev = self.get_revision(rev_id)?;
                (rev, self.name.with_revision(rev_id).to_string())
            }
            None => (self.latest_revision(), self.name.to_string()),
        };

        let definition = match view {
            SchemaView::Basic => String::new(),
            _ => revision.definition.clone(),
        };

        Some(SchemaProto {
            name: formatted_name,
            r#type: revision.schema_type.to_i32(),
            definition,
            revision_id: revision.revision_id.clone(),
            revision_create_time: Some(prost_types::Timestamp::from(
                revision.revision_create_time,
            )),
        })
    }
}
