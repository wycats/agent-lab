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

pub(crate) fn json_to_nu_tool_result(value: JsonValue, span: Span) -> Value {
    if let JsonValue::Object(values) = &value
        && values.len() == 1
        && let Some(JsonValue::Array(items)) = values.values().next()
    {
        return json_to_nu(JsonValue::Array(items.clone()), span);
    }
    json_to_nu(value, span)
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
    if let Some(value) = value.as_i64() {
        Value::int(value, span)
    } else if value.as_u64().is_some() {
        // Nushell integers are signed i64 values. Preserve oversized unsigned
        // integers exactly as decimal text rather than silently rounding them
        // through f64.
        Value::string(value.to_string(), span)
    } else {
        Value::float(value.as_f64().expect("JSON numbers are finite"), span)
    }
}

pub(crate) fn nu_to_json(value: Value) -> Result<JsonValue, ShellError> {
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

#[cfg(test)]
mod tests {
    use nu_protocol::Span;
    use serde_json::json;

    use super::{json_to_nu, json_to_nu_tool_result};

    #[test]
    fn oversized_unsigned_json_integer_preserves_its_exact_value() {
        let value = json_to_nu(json!(u64::MAX), Span::unknown());

        assert_eq!(value.as_str(), Ok("18446744073709551615"));
    }

    #[test]
    fn sole_nested_collection_is_projected_as_a_table() {
        let value = json_to_nu_tool_result(
            json!({ "items": [{ "name": "alpha" }, { "name": "gamma" }] }),
            Span::unknown(),
        );

        assert_eq!(value.into_list().unwrap().len(), 2);
    }

    #[test]
    fn result_envelopes_with_other_fields_remain_records() {
        let value = json_to_nu_tool_result(
            json!({ "items": [{ "name": "alpha" }], "nextCursor": null }),
            Span::unknown(),
        );

        assert!(value.into_record().is_ok());
    }
}
