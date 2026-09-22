use crate::pubsub_proto::Encoding;
use crate::schemas::validator::{SchemaValidator, SchemaValidationError};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

/// Schema validator for Protocol Buffer (.proto) definitions.
#[derive(Debug, Clone)]
pub struct ProtoValidator {
    descriptor: MessageDescriptor,
}

impl ProtoValidator {
    /// Creates a new `ProtoValidator` by parsing the .proto schema string.
    pub fn new(definition: &str) -> Result<Self, SchemaValidationError> {
        let file = protox::file::File::from_source("schema.proto", definition)
            .map_err(|e| SchemaValidationError::ParseError(e.to_string()))?;

        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file.file_descriptor_proto().clone())
            .map_err(|e| SchemaValidationError::ParseError(e.to_string()))?;

        // Per GCP Pub/Sub: uses the first message defined in the schema file.
        let descriptor = pool.all_messages().next().ok_or_else(|| {
            SchemaValidationError::ParseError(
                "Protobuf schema must contain at least one message definition".into(),
            )
        })?;

        Ok(Self { descriptor })
    }
}

impl SchemaValidator for ProtoValidator {
    fn validate(&self, data: &[u8], encoding: Encoding) -> Result<(), SchemaValidationError> {
        match encoding {
            Encoding::Binary => {
                DynamicMessage::decode(self.descriptor.clone(), data)
                    .map_err(|e| SchemaValidationError::InvalidProtoBinary(e.to_string()))?;
                Ok(())
            }
            Encoding::Json => {
                let mut deserializer = serde_json::Deserializer::from_slice(data);
                DynamicMessage::deserialize(self.descriptor.clone(), &mut deserializer)
                    .map_err(|e| SchemaValidationError::InvalidProtoJson(e.to_string()))?;
                Ok(())
            }
            Encoding::Unspecified => Err(SchemaValidationError::UnspecifiedEncoding),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    const PROTO_SCHEMA: &str = r#"
        syntax = "proto3";
        package myproject;

        message Person {
            string name = 1;
            int32 age = 2;
        }
    "#;

    #[test]
    fn test_valid_json() {
        let validator = ProtoValidator::new(PROTO_SCHEMA).unwrap();
        let valid_json = br#"{"name": "Alice", "age": 30}"#;
        assert!(validator.validate(valid_json, Encoding::Json).is_ok());

        // Invalid JSON type (age is string instead of number)
        let invalid_json = br#"{"name": "Alice", "age": "thirty"}"#;
        assert!(validator.validate(invalid_json, Encoding::Json).is_err());
    }

    #[test]
    fn test_valid_binary() {
        let validator = ProtoValidator::new(PROTO_SCHEMA).unwrap();

        // Dynamically create a valid message and encode to bytes
        let mut msg = DynamicMessage::new(validator.descriptor.clone());
        msg.try_set_field_by_name("name", prost_reflect::Value::String("Alice".into()))
            .unwrap();
        msg.try_set_field_by_name("age", prost_reflect::Value::I32(30))
            .unwrap();
        let bytes = msg.encode_to_vec();

        assert!(validator.validate(&bytes, Encoding::Binary).is_ok());

        // Note: protobuf is very forgiving on binary decode of arbitrary bytes, but invalid wire tags or truncated fields fail
        let corrupt = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        assert!(validator.validate(&corrupt, Encoding::Binary).is_err());
    }

    #[test]
    fn test_invalid_definition() {
        let err = ProtoValidator::new("syntax = 'not a valid proto'").unwrap_err();
        assert!(matches!(err, SchemaValidationError::ParseError(_)));
    }
}
