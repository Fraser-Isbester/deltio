use crate::paging::Paging;
use crate::pubsub_proto::{Schema as SchemaProto, SchemaView};
use crate::schemas::schema_name::SchemaName;
use crate::schemas::types::{Schema, SchemaRevision, SchemaType};
use crate::schemas::validator::{SchemaValidator, SchemaValidationError};
use parking_lot::RwLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

/// In-memory manager for Pub/Sub schemas and their revision histories.
pub struct SchemaManager {
    state: Arc<RwLock<State>>,
}

struct State {
    schemas: HashMap<String, Schema>,
}

impl State {
    fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }
}

/// Errors occurring during schema operations.
#[derive(Debug, thiserror::Error)]
pub enum SchemaManagerError {
    #[error("Schema already exists")]
    AlreadyExists,
    #[error("Schema not found")]
    NotFound,
    #[error("Revision not found")]
    RevisionNotFound,
    #[error("Cannot delete the only revision of a schema (delete the schema instead)")]
    CannotDeleteLastRevision,
    #[error("Validation error: {0}")]
    Validation(#[from] SchemaValidationError),
    #[error("Unsupported schema type: {0}")]
    UnsupportedType(i32),
}

impl SchemaManager {
    /// Creates a new `SchemaManager`.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::new())),
        }
    }

    fn generate_revision_id() -> String {
        format!("{:08x}", rand::random::<u32>())
    }

    /// Creates a new schema with its initial revision.
    pub fn create_schema(
        &self,
        name: SchemaName,
        schema_type: SchemaType,
        definition: String,
    ) -> Result<SchemaProto, SchemaManagerError> {
        let revision_id = Self::generate_revision_id();
        let initial_revision = SchemaRevision::new(revision_id, schema_type, definition)?;

        let mut state = self.state.write();
        let canonical_name = name.name_without_revision();

        match state.schemas.entry(canonical_name) {
            Entry::Vacant(e) => {
                let schema = Schema::new(name, initial_revision);
                let proto = schema
                    .to_proto(None, SchemaView::Full)
                    .expect("Initial revision must convert to proto");
                e.insert(schema);
                Ok(proto)
            }
            Entry::Occupied(_) => Err(SchemaManagerError::AlreadyExists),
        }
    }

    /// Gets a schema by name (with optional revision qualifier) and view.
    pub fn get_schema(
        &self,
        name: &SchemaName,
        view: SchemaView,
    ) -> Result<SchemaProto, SchemaManagerError> {
        let state = self.state.read();
        let canonical_name = name.name_without_revision();
        let schema = state
            .schemas
            .get(&canonical_name)
            .ok_or(SchemaManagerError::NotFound)?;

        schema
            .to_proto(name.revision_id(), view)
            .ok_or(SchemaManagerError::RevisionNotFound)
    }

    /// Lists schemas in a project.
    pub fn list_schemas(
        &self,
        project_id: &str,
        paging: Paging,
        view: SchemaView,
    ) -> (Vec<SchemaProto>, Option<usize>) {
        let state = self.state.read();
        let mut matching: Vec<_> = state
            .schemas
            .values()
            .filter(|s| s.name.is_in_project(project_id))
            .collect();

        matching.sort_by(|a, b| a.name.cmp(&b.name));

        let skip = paging.to_skip();
        let paged: Vec<_> = matching
            .into_iter()
            .skip(skip)
            .take(paging.size())
            .filter_map(|s| s.to_proto(None, view))
            .collect();

        let next_page = paging.next_page_from_slice_result(&paged);
        (paged, next_page.offset())
    }

    /// Lists all revisions of a schema.
    pub fn list_schema_revisions(
        &self,
        name: &SchemaName,
        paging: Paging,
        view: SchemaView,
    ) -> Result<(Vec<SchemaProto>, Option<usize>), SchemaManagerError> {
        let state = self.state.read();
        let canonical_name = name.name_without_revision();
        let schema = state
            .schemas
            .get(&canonical_name)
            .ok_or(SchemaManagerError::NotFound)?;

        let skip = paging.to_skip();
        let paged: Vec<_> = schema
            .revisions
            .iter()
            .rev() // Return newest revisions first
            .skip(skip)
            .take(paging.size())
            .filter_map(|r| schema.to_proto(Some(&r.revision_id), view))
            .collect();

        let next_page = paging.next_page_from_slice_result(&paged);
        Ok((paged, next_page.offset()))
    }

    /// Commits a new schema revision.
    pub fn commit_schema(
        &self,
        name: &SchemaName,
        schema_type: SchemaType,
        definition: String,
    ) -> Result<SchemaProto, SchemaManagerError> {
        let new_revision_id = Self::generate_revision_id();
        let new_revision = SchemaRevision::new(new_revision_id, schema_type, definition)?;

        let mut state = self.state.write();
        let canonical_name = name.name_without_revision();
        let schema = state
            .schemas
            .get_mut(&canonical_name)
            .ok_or(SchemaManagerError::NotFound)?;

        schema.revisions.push(new_revision);
        Ok(schema
            .to_proto(None, SchemaView::Full)
            .expect("Committed revision must exist"))
    }

    /// Rolls back a schema to a prior revision by creating a new revision with the target revision's definition.
    pub fn rollback_schema(
        &self,
        name: &SchemaName,
        target_revision_id: &str,
    ) -> Result<SchemaProto, SchemaManagerError> {
        let mut state = self.state.write();
        let canonical_name = name.name_without_revision();
        let schema = state
            .schemas
            .get_mut(&canonical_name)
            .ok_or(SchemaManagerError::NotFound)?;

        let target_revision = schema
            .get_revision(target_revision_id)
            .ok_or(SchemaManagerError::RevisionNotFound)?
            .clone();

        let new_revision_id = Self::generate_revision_id();
        let new_revision = SchemaRevision::new(
            new_revision_id,
            target_revision.schema_type,
            target_revision.definition,
        )?;

        schema.revisions.push(new_revision);
        Ok(schema
            .to_proto(None, SchemaView::Full)
            .expect("Rolled-back revision must exist"))
    }

    /// Deletes a specific schema revision.
    pub fn delete_schema_revision(
        &self,
        name: &SchemaName,
        revision_id: &str,
    ) -> Result<SchemaProto, SchemaManagerError> {
        let mut state = self.state.write();
        let canonical_name = name.name_without_revision();
        let schema = state
            .schemas
            .get_mut(&canonical_name)
            .ok_or(SchemaManagerError::NotFound)?;

        if schema.revisions.len() <= 1 {
            return Err(SchemaManagerError::CannotDeleteLastRevision);
        }

        let index = schema
            .revisions
            .iter()
            .position(|r| r.revision_id == revision_id)
            .ok_or(SchemaManagerError::RevisionNotFound)?;

        let deleted = schema.revisions.remove(index);
        let proto = SchemaProto {
            name: name.with_revision(&deleted.revision_id).to_string(),
            r#type: deleted.schema_type.to_i32(),
            definition: deleted.definition,
            revision_id: deleted.revision_id,
            revision_create_time: Some(prost_types::Timestamp::from(
                deleted.revision_create_time,
            )),
        };

        Ok(proto)
    }

    /// Deletes an entire schema and all its revisions.
    pub fn delete_schema(&self, name: &SchemaName) -> Result<(), SchemaManagerError> {
        let mut state = self.state.write();
        let canonical_name = name.name_without_revision();
        state
            .schemas
            .remove(&canonical_name)
            .ok_or(SchemaManagerError::NotFound)?;
        Ok(())
    }

    /// Resolves a validator and active revision ID for a schema name (with optional @revision).
    pub fn resolve_validator(
        &self,
        schema_raw_or_name: &str,
    ) -> Option<(Arc<dyn SchemaValidator>, String)> {
        let parsed = SchemaName::try_parse(schema_raw_or_name)?;
        let state = self.state.read();
        let schema = state.schemas.get(&parsed.name_without_revision())?;

        let revision = match parsed.revision_id() {
            Some(rev_id) => schema.get_revision(rev_id)?,
            None => schema.latest_revision(),
        };

        Some((Arc::clone(&revision.validator), revision.revision_id.clone()))
    }
}

impl Default for SchemaManager {
    fn default() -> Self {
        Self::new()
    }
}
