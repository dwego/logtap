use crate::record::LogLine;
use regex::Regex;
use serde::Deserialize;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, Deserialize, Clone)]
pub enum RuleOp {
    Equals,
    Contains,
    Regex,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RuleAction {
    Drop,
    Mask,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct FilterRule {
    pub field: String,
    pub op: RuleOp,
    pub value: String,
    pub action: RuleAction,
}

pub async fn run_filter(mut rx: Receiver<LogLine>, tx: Sender<LogLine>, rules: Vec<FilterRule>) {
    while let Some(mut log) = rx.recv().await {
        let mut dropped = false;

        for rule in &rules {
            let field_val = log.get(&rule.field).and_then(|v| v.as_str()).unwrap_or("");
            let matched = match rule.op {
                RuleOp::Equals => field_val == rule.value,
                RuleOp::Contains => field_val.contains(&rule.value),
                RuleOp::Regex => Regex::new(&rule.value).unwrap().is_match(field_val),
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
