use logtap::parser::parse_line;
use serde_json::Value;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_parse() {
        let linha = r#"{"level": "info", "msg": "server started"}"#;

        let resultado = parse_line(linha);

        assert_eq!(
            resultado.fields.get("level"),
            Some(&Value::String("info".to_string()))
        );
        assert_eq!(
            resultado.fields.get("msg"),
            Some(&Value::String("server started".to_string()))
        );
        assert_eq!(resultado.raw, linha);
    }

    #[test]
    fn parseline_with_text() {
        let linha = "isso aqui nao e json";

        let resultado = parse_line(linha);

        assert_eq!(
            resultado.fields.get("message"),
            Some(&Value::String(linha.to_string()))
        );
        assert_eq!(
            resultado.fields.get("level"),
            Some(&Value::String("UNKNOWN".to_string()))
        );
    }

    #[test]
    fn parse_empty_json_line_returns_empty_object() {
        let linha = "{}";

        let resultado = parse_line(linha);

        assert!(resultado.fields.is_empty());
    }
}