use logtap::parser::parse_line;
use serde_json::Value;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_parse() {
        let line = r#"{"level": "info", "msg": "server started"}"#;

        let result = parse_line(line);

        assert_eq!(
            result.get("level"),
            Some(&Value::String("info".to_string()))
        );
        assert_eq!(
            result.get("msg"),
            Some(&Value::String("server started".to_string()))
        );
    }

    #[test]
    fn parseline_with_text() {
        let line = "isso aqui nao e json";

        let result = parse_line(line);

        assert_eq!(
            result.get("message"),
            Some(&Value::String(line.to_string()))
        );
        assert_eq!(
            result.get("level"),
            Some(&Value::String("UNKNOWN".to_string()))
        );
        assert_eq!(
            result.get("parse_issue"),
            Some(&Value::String("invalid_json".to_string()))
        );
    }

    #[test]
    fn parse_empty_json_line_returns_empty_object() {
        let line = "{}";

        let result = parse_line(line);

        assert!(result.as_object().is_some_and(|obj| obj.is_empty()));
    }
}
