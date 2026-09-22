use crate::pubsub_proto::Encoding;
use crate::schemas::validator::{SchemaValidator, SchemaValidationError};
use apache_avro::Schema;

/// Schema validator for Apache Avro definitions.
#[derive(Debug, Clone)]
pub struct AvroValidator {
    schema: Schema,
}

impl AvroValidator {
    /// Creates a new `AvroValidator` by parsing the JSON Avro schema definition.
    pub fn new(definition: &str) -> Result<Self, SchemaValidationError> {
        let schema = Schema::parse_str(definition)
            .map_err(|e| SchemaValidationError::ParseError(e.to_string()))?;
        Ok(Self { schema })
    }
}

impl SchemaValidator for AvroValidator {
    fn validate(&self, data: &[u8], encoding: Encoding) -> Result<(), SchemaValidationError> {
        match encoding {
            Encoding::Binary => {
                let mut reader = data;
                #[allow(deprecated)]
                match apache_avro::from_avro_datum(&self.schema, &mut reader, None) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(SchemaValidationError::InvalidAvroBinary(e.to_string())),
                }
            }
            Encoding::Json => {
                let json_val: serde_json::Value = serde_json::from_slice(data).map_err(|e| {
                    SchemaValidationError::InvalidAvroJson(format!("Invalid JSON syntax: {e}"))
                })?;
                let avro_val = apache_avro::types::Value::try_from(json_val).map_err(|e| {
                    SchemaValidationError::InvalidAvroJson(format!(
                        "Cannot convert JSON to Avro value: {e}"
                    ))
                })?;
                match avro_val.resolve(&self.schema) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(SchemaValidationError::InvalidAvroJson(format!(
                        "Avro schema validation failed: {e}"
                    ))),
                }
            }
            Encoding::Unspecified => Err(SchemaValidationError::UnspecifiedEncoding),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const AVRO_SCHEMA: &str = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }"#;

    #[test]
    fn test_valid_json() {
        let validator = AvroValidator::new(AVRO_SCHEMA).unwrap();
        let valid_json = br#"{"name": "Alice", "age": 30}"#;
        assert!(validator.validate(valid_json, Encoding::Json).is_ok());

        // Missing field
        let missing_field = br#"{"name": "Alice"}"#;
        assert!(validator.validate(missing_field, Encoding::Json).is_err());

        // Wrong type
        let wrong_type = br#"{"name": "Alice", "age": "thirty"}"#;
        assert!(validator.validate(wrong_type, Encoding::Json).is_err());
    }

    #[test]
    fn test_valid_binary() {
        let validator = AvroValidator::new(AVRO_SCHEMA).unwrap();

        // Encode valid datum into binary
        let mut datum_map = std::collections::HashMap::new();
        datum_map.insert("name".to_string(), apache_avro::types::Value::String("Bob".into()));
        datum_map.insert("age".to_string(), apache_avro::types::Value::Int(25));
        let record = apache_avro::types::Value::Record(datum_map.into_iter().collect());
        #[allow(deprecated)]
        let binary_bytes = apache_avro::to_avro_datum(&validator.schema, record).unwrap();

        assert!(validator.validate(&binary_bytes, Encoding::Binary).is_ok());

        // Garbage bytes should fail
        assert!(validator.validate(b"corrupt-data", Encoding::Binary).is_err());
    }

    #[test]
    fn test_invalid_schema_definition() {
        let err = AvroValidator::new("not valid json").unwrap_err();
        assert!(matches!(err, SchemaValidationError::ParseError(_)));
    }
}
