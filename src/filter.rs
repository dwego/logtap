use crate::LogLine;
use regex::Regex;
use serde::Deserialize;
use tokio::sync::mpsc::{Receiver, Sender};

/// Operation used to compare a field value.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RuleOp {
    /// Matches when the field value is exactly.
    Equals,

    /// Matches when the field value contains the configured value.
    Contains,

    /// Matches when the field value satisfies the configured regular expression.
    Regex,
}

/// Action performed when a filter rule matches.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RuleAction {
    /// Remove the entire log entry from the processing pipeline.
    Drop,

    /// Replaces the matching field value with a masked value.
    Mask,
}

/// A single filtering rule applied to a log entry.
///
/// Rules define which field should be checked, how the value should be
/// compared, and what action should be performed when a match occurs.
#[derive(Debug, Deserialize, Clone)]
pub struct FilterRule {
    /// JSON field name to inspect.
    pub field: String,
    /// Comparison operation used to evaluate the field.
    
    pub op: RuleOp,
    /// Value or pattern used for maching.
    pub value: String,

    /// Action executed when the rule matches
    pub action: RuleAction,
}

/// Runs the log filtering pipeline.
///
/// Receives log entries from a channel, applies built-in masking patterns
/// and configured filter rules, then forwards accepted logs to the output channel.
///
/// Logs can either be dropped, masked, or passed through unchanged depending
/// on the configured rules.
///
/// # Arguments
///
/// * `rx` - Input channel receiving log entries.
/// * `tx` - Output channel for filtered log entries.
/// * `rules` - User-defined filtering rules.
/// * `mask_common_patterns` - Enables built-in masking of sensitive patterns
///   such as emails, credit card numbers, and API keys.
pub async fn run_filter(
    mut rx: Receiver<LogLine>,
    tx: Sender<LogLine>,
    rules: Vec<FilterRule>,
    mask_common_patterns: bool,
) {
    let email_re = Regex::new(r"(?i)\b([a-z0-9._%+-])[a-z0-9._%+-]*(@[a-z0-9.-]+\.[a-z]{2,})\b")
        .expect("invalid regex for email");
    let card_re = Regex::new(r"\b(?:\d[ -]?){13,19}\b").expect("invalid regex for card");
    let api_key_re = Regex::new(r"\bsk-[A-Za-z0-9]{16,}\b").expect("invalid regex for API key");

    let builtin_patterns: Vec<(&Regex, &str)> = vec![
        (&email_re, "$1***$2"),
        (&card_re, "[card-masked]"),
        (&api_key_re, "[api-key-masked]"),
    ];

    while let Some(mut log) = rx.recv().await {
        if mask_common_patterns {
            mask_builtin(&mut log, &builtin_patterns);
        }

        let mut dropped = false;

        for rule in &rules {
            let field_val = log.get(&rule.field).and_then(|v| v.as_str()).unwrap_or("");

            let matched = match rule.op {
                RuleOp::Equals => field_val == rule.value,
                RuleOp::Contains => field_val.contains(&rule.value),
                RuleOp::Regex => Regex::new(&rule.value)
                    .map(|re| re.is_match(field_val))
                    .unwrap_or(false),
            };

            if matched {
                match rule.action {
                    RuleAction::Drop => {
                        dropped = true;
                        break;
                    }
                    RuleAction::Mask => {
                        log[&rule.field] = serde_json::json!("***");
                    }
                }
            }
        }

        if !dropped && tx.send(log).await.is_err() {
            break;
        }
    }
}

fn mask_builtin(log: &mut LogLine, patterns: &[(&Regex, &str)]) {
    if let Some(obj) = log.as_object_mut() {
        for (_, v) in obj.iter_mut() {
            if let Some(s) = v.as_str() {
                let mut masked = s.to_string();
                for (re, replacement) in patterns {
                    masked = re.replace_all(&masked, *replacement).to_string();
                }
                if masked != s {
                    *v = serde_json::Value::String(masked);
                }
            }
        }
    }
}
