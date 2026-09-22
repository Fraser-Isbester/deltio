use crate::pubsub_proto::Encoding;

/// Errors that can occur during schema definition parsing or message validation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaValidationError {
    #[error("Failed to parse schema definition: {0}")]
    ParseError(String),
    #[error("Message does not conform to Avro schema (binary): {0}")]
    InvalidAvroBinary(String),
    #[error("Message does not conform to Avro schema (JSON): {0}")]
    InvalidAvroJson(String),
    #[error("Message does not conform to Protocol Buffer schema (binary): {0}")]
    InvalidProtoBinary(String),
    #[error("Message does not conform to Protocol Buffer schema (JSON): {0}")]
    InvalidProtoJson(String),
    #[error("Encoding unspecified; must be BINARY or JSON")]
    UnspecifiedEncoding,
    #[error("Unsupported schema type: {0:?}")]
    UnsupportedType(i32),
}

/// Trait implemented by validators for different schema formats.
pub trait SchemaValidator: Send + Sync {
    /// Validates message payload bytes against the schema under the given encoding.
    fn validate(&self, data: &[u8], encoding: Encoding) -> Result<(), SchemaValidationError>;
}
