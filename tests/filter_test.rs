use logtap::filter::{FilterRule, RuleAction, RuleOp, run_filter};

#[tokio::test]
async fn filter_drops_log_when_equals_rule_matches() {
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(10);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(10);

    let rules = vec![FilterRule {
        field: "level".to_string(),
        op: RuleOp::Equals,
        value: "debug".to_string(),
        action: RuleAction::Drop,
    }];

    let filter = tokio::spawn(run_filter(input_rx, output_tx, rules, false));

    input_tx
        .send(serde_json::json!({
            "level": "debug",
            "message": "this should be dropped"
        }))
        .await
        .unwrap();

    drop(input_tx);

    let received = output_rx.recv().await;

    filter.await.unwrap();

    assert!(received.is_none());
}

#[tokio::test]
async fn filter_keeps_log_when_no_rule_matches() {
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(10);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(10);

    let rules = vec![FilterRule {
        field: "level".to_string(),
        op: RuleOp::Equals,
        value: "debug".to_string(),
        action: RuleAction::Drop,
    }];

    let filter = tokio::spawn(run_filter(input_rx, output_tx, rules, false));

    input_tx
        .send(serde_json::json!({
            "level": "info",
            "message": "this should pass"
        }))
        .await
        .unwrap();

    drop(input_tx);

    let received = output_rx.recv().await.unwrap();

    filter.await.unwrap();

    assert_eq!(
        received,
        serde_json::json!({
            "level": "info",
            "message": "this should pass"
        })
    );
}

#[tokio::test]
async fn filter_masks_field_when_contains_rule_matches() {
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(10);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(10);

    let rules = vec![FilterRule {
        field: "email".to_string(),
        op: RuleOp::Contains,
        value: "@".to_string(),
        action: RuleAction::Mask,
    }];

    let filter = tokio::spawn(run_filter(input_rx, output_tx, rules, false));

    input_tx
        .send(serde_json::json!({
            "email": "user@example.com",
            "message": "user logged in"
        }))
        .await
        .unwrap();

    drop(input_tx);

    let received = output_rx.recv().await.unwrap();

    filter.await.unwrap();

    assert_eq!(received["email"], serde_json::json!("***"));
    assert_eq!(received["message"], serde_json::json!("user logged in"));
}

#[tokio::test]
async fn filter_applies_regex_rule() {
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(10);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(10);

    let rules = vec![FilterRule {
        field: "message".to_string(),
        op: RuleOp::Regex,
        value: r"\d{3}-\d{2}-\d{4}".to_string(),
        action: RuleAction::Mask,
    }];

    let filter = tokio::spawn(run_filter(input_rx, output_tx, rules, false));

    input_tx
        .send(serde_json::json!({
            "message": "ssn 123-45-6789"
        }))
        .await
        .unwrap();

    drop(input_tx);

    let received = output_rx.recv().await.unwrap();

    filter.await.unwrap();

    assert_eq!(received["message"], serde_json::json!("***"));
}

#[tokio::test]
async fn filter_masks_builtin_email_pattern_when_enabled() {
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(10);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(10);

    let filter = tokio::spawn(run_filter(input_rx, output_tx, vec![], true));

    input_tx
        .send(serde_json::json!({
            "message": "user login: user@example.com"
        }))
        .await
        .unwrap();

    drop(input_tx);

    let received = output_rx.recv().await.unwrap();

    filter.await.unwrap();

    let message = received["message"].as_str().unwrap();
    assert!(!message.contains("user@example.com"));
    assert!(message.contains("***"));
}

#[tokio::test]
async fn filter_does_not_mask_builtin_patterns_when_disabled() {
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(10);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(10);

    let filter = tokio::spawn(run_filter(input_rx, output_tx, vec![], false));

    input_tx
        .send(serde_json::json!({
            "message": "user login: user@example.com"
        }))
        .await
        .unwrap();

    drop(input_tx);

    let received = output_rx.recv().await.unwrap();

    filter.await.unwrap();

    assert_eq!(
        received["message"],
        serde_json::json!("user login: user@example.com")
    );
}
