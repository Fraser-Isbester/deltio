use std::fmt::{Display, Formatter};

/// Represents a parsed schema name.
///
/// Expected format: `projects/{project}/schemas/{schema}` or
/// `projects/{project}/schemas/{schema}@{revision_id}`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaName {
    project_id: Box<str>,
    schema_id: Box<str>,
    revision_id: Option<Box<str>>,
}

impl SchemaName {
    /// Creates a new `SchemaName`.
    pub fn new(project_id: impl Into<Box<str>>, schema_id: impl Into<Box<str>>) -> Self {
        Self {
            project_id: project_id.into(),
            schema_id: schema_id.into(),
            revision_id: None,
        }
    }

    /// Creates a new `SchemaName` with a specific revision.
    pub fn new_with_revision(
        project_id: impl Into<Box<str>>,
        schema_id: impl Into<Box<str>>,
        revision_id: impl Into<Box<str>>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            schema_id: schema_id.into(),
            revision_id: Some(revision_id.into()),
        }
    }

    /// Attempts to parse a raw string as a `SchemaName`.
    pub fn try_parse(raw: &str) -> Option<Self> {
        const PREFIX: &str = "projects/";
        const SCHEMAS_DELIM: &str = "/schemas/";

        if !raw.starts_with(PREFIX) {
            return None;
        }

        let without_prefix = &raw[PREFIX.len()..];
        let delim_index = without_prefix.find(SCHEMAS_DELIM)?;
        let project_id = &without_prefix[..delim_index];
        if project_id.is_empty() {
            return None;
        }

        let after_delim = &without_prefix[delim_index + SCHEMAS_DELIM.len()..];
        if after_delim.is_empty() {
            return None;
        }

        if let Some(at_idx) = after_delim.find('@') {
            let schema_id = &after_delim[..at_idx];
            let revision_id = &after_delim[at_idx + 1..];
            if schema_id.is_empty() || revision_id.is_empty() {
                return None;
            }
            Some(Self::new_with_revision(project_id, schema_id, revision_id))
        } else {
            Some(Self::new(project_id, after_delim))
        }
    }

    /// Returns the project ID.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Returns the schema ID.
    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }

    /// Returns the revision ID if present.
    pub fn revision_id(&self) -> Option<&str> {
        self.revision_id.as_deref()
    }

    /// Returns the schema canonical name without revision specifier.
    pub fn name_without_revision(&self) -> String {
        format!("projects/{}/schemas/{}", self.project_id, self.schema_id)
    }

    /// Returns a new `SchemaName` with the specified revision.
    pub fn with_revision(&self, revision_id: &str) -> Self {
        Self::new_with_revision(&*self.project_id, &*self.schema_id, revision_id)
    }

    /// Returns a new `SchemaName` without any revision specifier.
    pub fn without_revision(&self) -> Self {
        Self::new(&*self.project_id, &*self.schema_id)
    }

    /// Checks if this schema belongs to the specified project.
    pub fn is_in_project(&self, project_id: &str) -> bool {
        self.project_id.as_ref() == project_id
    }
}

impl Display for SchemaName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.revision_id {
            Some(rev) => write!(
                f,
                "projects/{}/schemas/{}@{}",
                self.project_id, self.schema_id, rev
            ),
            None => write!(f, "projects/{}/schemas/{}", self.project_id, self.schema_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_without_revision() {
        let name = SchemaName::try_parse("projects/my-project/schemas/my-schema").unwrap();
        assert_eq!(name.project_id(), "my-project");
        assert_eq!(name.schema_id(), "my-schema");
        assert_eq!(name.revision_id(), None);
        assert_eq!(name.to_string(), "projects/my-project/schemas/my-schema");
        assert_eq!(
            name.name_without_revision(),
            "projects/my-project/schemas/my-schema"
        );
    }

    #[test]
    fn parse_valid_with_revision() {
        let name = SchemaName::try_parse("projects/my-project/schemas/my-schema@rev123").unwrap();
        assert_eq!(name.project_id(), "my-project");
        assert_eq!(name.schema_id(), "my-schema");
        assert_eq!(name.revision_id(), Some("rev123"));
        assert_eq!(
            name.to_string(),
            "projects/my-project/schemas/my-schema@rev123"
        );
        assert_eq!(
            name.name_without_revision(),
            "projects/my-project/schemas/my-schema"
        );
    }

    #[test]
    fn parse_invalid() {
        assert!(SchemaName::try_parse("").is_none());
        assert!(SchemaName::try_parse("projects//schemas/foo").is_none());
        assert!(SchemaName::try_parse("projects/p/schemas/").is_none());
        assert!(SchemaName::try_parse("projects/p/schemas/foo@").is_none());
        assert!(SchemaName::try_parse("topics/my-topic").is_none());
    }
}
