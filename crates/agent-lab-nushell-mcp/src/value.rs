use nu_protocol::{Record, ShellError, Span, Value, shell_error::generic::GenericError};
use serde_json::{Map, Number, Value as JsonValue};

pub(crate) fn json_to_nu(value: JsonValue, span: Span) -> Value {
    match value {
        JsonValue::Null => Value::nothing(span),
        JsonValue::Bool(value) => Value::bool(value, span),
        JsonValue::Number(value) => number_to_nu(&value, span),
        JsonValue::String(value) => Value::string(value, span),
        JsonValue::Array(values) => Value::list(
            values
                .into_iter()
                .map(|value| json_to_nu(value, span))
                .collect(),
            span,
        ),
        JsonValue::Object(values) => {
            let record = values
                .into_iter()
                .map(|(name, value)| (name, json_to_nu(value, span)))
                .collect::<Record>();
            Value::record(record, span)
        }
    }
}

pub(crate) fn nu_record_to_json(
    value: Option<Value>,
    span: Span,
) -> Result<Map<String, JsonValue>, ShellError> {
    match value {
        None | Some(Value::Nothing { .. }) => Ok(Map::new()),
        Some(Value::Record { val, .. }) => val
            .into_owned()
            .into_iter()
            .map(|(name, value)| Ok((name, nu_to_json(value)?)))
            .collect(),
        Some(value) => Err(ShellError::Generic(GenericError::new(
            "MCP arguments must be a record",
            format!("received {}", value.get_type()),
            span,
        ))),
    }
}

fn number_to_nu(value: &Number, span: Span) -> Value {
    value.as_i64().map_or_else(
        || Value::float(value.as_f64().expect("JSON numbers are finite"), span),
        |value| Value::int(value, span),
    )
}

fn nu_to_json(value: Value) -> Result<JsonValue, ShellError> {
    let span = value.span();
    match value {
        Value::Nothing { .. } => Ok(JsonValue::Null),
        Value::Bool { val, .. } => Ok(JsonValue::Bool(val)),
        Value::Int { val, .. } => Ok(JsonValue::Number(val.into())),
        Value::Float { val, .. } => Number::from_f64(val).map_or_else(
            || {
                Err(ShellError::Generic(GenericError::new(
                    "MCP arguments require finite numbers",
                    "received a non-finite float",
                    span,
                )))
            },
            |value| Ok(JsonValue::Number(value)),
        ),
        Value::String { val, .. } => Ok(JsonValue::String(val)),
        Value::List { vals, .. } => vals
            .into_iter()
            .map(nu_to_json)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        Value::Record { val, .. } => val
            .into_owned()
            .into_iter()
            .map(|(name, value)| Ok((name, nu_to_json(value)?)))
            .collect::<Result<Map<_, _>, _>>()
            .map(JsonValue::Object),
        value => Err(ShellError::Generic(GenericError::new(
            "Unsupported MCP argument value",
            format!("cannot encode {} as JSON", value.get_type()),
            span,
        ))),
    }
}
