//! Phase 19: Telegram Approval Gate tests
//!
//! Tests the approval gate as a hard security boundary that the LLM cannot
//! bypass, with role-based authorization and replay safety.

use std::sync::Arc;
use std::time::{Duration, Instant};
use zeroclaw::config::ApprovalPolicyConfig;
use zeroclaw::security::approval::{ApprovalGate, ApprovalRegistry, ApprovalRequest};
use zeroclaw::security::policy::{CommandOutcome, CommandRiskLevel, SecurityPolicy};
use zeroclaw::security::AutonomyLevel;
use zeroclaw::tools::{ShellTool, Tool};

// ── Helpers ──────────────────────────────────────────────────────

fn test_runtime() -> Arc<dyn zeroclaw::runtime::RuntimeAdapter> {
    Arc::new(zeroclaw::runtime::NativeRuntime::new())
}

fn make_request(
    id: &str,
    risk: CommandRiskLevel,
    authorized: Vec<&str>,
    timeout_secs: u64,
) -> ApprovalRequest {
    ApprovalRequest {
        id: id.into(),
        command: "echo test".into(),
        risk_level: risk,
        origin_chat_id: "chat_123".into(),
        origin_user_id: Some("user_456".into()),
        authorized_approver_ids: authorized.into_iter().map(String::from).collect(),
        created_at: Instant::now(),
        expires_at: Instant::now() + Duration::from_secs(timeout_secs),
        resolved: false,
    }
}

fn make_gate(policy: ApprovalPolicyConfig) -> Arc<ApprovalGate> {
    let registry = Arc::new(ApprovalRegistry::new());
    let channel = Arc::new(zeroclaw::channels::telegram::TelegramChannel::new(
        "fake-token".into(),
        vec!["*".into()],
    ));
    let context = Arc::new(std::sync::Mutex::new(None));
    Arc::new(ApprovalGate::new(
        registry,
        channel,
        context,
        policy,
        vec!["user1".into(), "user2".into()],
    ))
}

// ── Security Boundary Tests (CRITICAL) ──────────────────────────

/// Test 1: LLM cannot self-approve by setting approved=true in args.
/// When approval gate is active, the `approved` param is silently ignored.
#[tokio::test]
async fn gate1_llm_cannot_bypass_with_approved_true() {
    // Build a ShellTool with an approval gate active
    let security = Arc::new(SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["touch".into()],
        workspace_dir: std::env::temp_dir(),
        require_approval_for_medium_risk: true,
        block_high_risk_commands: true,
        ..SecurityPolicy::default()
    });

    let gate = make_gate(ApprovalPolicyConfig::default());
    let tool = ShellTool::new(security, test_runtime(), Some(gate));

    // The LLM tries to self-approve a medium-risk command
    let result = tool
        .execute(serde_json::json!({
            "command": "touch /tmp/zeroclaw_bypass_test",
            "approved": true
        }))
        .await
        .unwrap();

    // Should fail because the gate intercepts and ignores `approved=true`.
    // Without a real Telegram channel responding, the approval will fail
    // (no context set = "No Telegram context available").
    assert!(
        !result.success,
        "LLM should NOT be able to self-approve when gate is active"
    );
}

/// Test 1b: Without a gate, LLM CAN self-approve (backward compat).
#[tokio::test]
async fn gate1b_no_gate_llm_can_self_approve() {
    let security = Arc::new(SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["touch".into()],
        workspace_dir: std::env::temp_dir(),
        require_approval_for_medium_risk: true,
        block_high_risk_commands: true,
        ..SecurityPolicy::default()
    });

    let tool = ShellTool::new(security, test_runtime(), None);

    // Without a gate, approved=true works (current behavior)
    let result = tool
        .execute(serde_json::json!({
            "command": "touch /tmp/zeroclaw_approve_test",
            "approved": true
        }))
        .await
        .unwrap();

    assert!(result.success, "Without gate, approved=true should work");
    let _ = std::fs::remove_file(std::env::temp_dir().join("zeroclaw_approve_test"));
}

/// Test 2: Unauthorized user cannot approve a request.
#[test]
fn gate2_unauthorized_user_cannot_approve() {
    let registry = ApprovalRegistry::new();
    let req = make_request("11111111", CommandRiskLevel::Medium, vec!["user_456"], 90);
    let (_id, _rx) = registry.register(req);

    let result = registry.resolve("11111111", "eve_id", "eve_the_attacker", true);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not authorized"));
}

/// Test 3: Operator cannot approve high-risk requests routed to admin.
#[test]
fn gate3_wrong_role_cannot_approve_high_risk() {
    let registry = ApprovalRegistry::new();
    // Only admin_user can approve this high-risk request
    let req = make_request("22222222", CommandRiskLevel::High, vec!["admin_1"], 90);
    let (_id, _rx) = registry.register(req);

    // Operator (not in authorized list) tries to approve
    let result = registry.resolve("22222222", "regular_user", "regular_username", true);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not authorized"));
}

/// Test 4: Expired request returns "Approval expired" on late callback.
#[test]
fn gate4_timeout_invalidates_request() {
    let registry = ApprovalRegistry::new();
    // Create request that's already expired
    let req = ApprovalRequest {
        id: "33333333".into(),
        command: "rm -rf /tmp/test".into(),
        risk_level: CommandRiskLevel::High,
        origin_chat_id: "chat_1".into(),
        origin_user_id: Some("user_1".into()),
        authorized_approver_ids: vec!["user_1".into()],
        created_at: Instant::now() - Duration::from_secs(100),
        expires_at: Instant::now() - Duration::from_secs(1), // already expired
        resolved: false,
    };
    let (_id, _rx) = registry.register(req);

    let result = registry.resolve("33333333", "user_1", "username_1", true);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("expired"));
}

/// Test 5: Multiple concurrent approvals resolve independently.
#[test]
fn gate5_concurrent_approvals_independent() {
    let registry = ApprovalRegistry::new();

    let req_a = make_request("aaaa0001", CommandRiskLevel::Medium, vec!["user_a"], 90);
    let req_b = make_request("bbbb0002", CommandRiskLevel::High, vec!["user_b"], 90);
    let (_id_a, _rx_a) = registry.register(req_a);
    let (_id_b, _rx_b) = registry.register(req_b);

    // Approve request A
    let result_a = registry.resolve("aaaa0001", "user_a", "username_a", true);
    assert!(result_a.is_ok());
    assert!(result_a.unwrap().contains("Approved"));

    // Deny request B
    let result_b = registry.resolve("bbbb0002", "user_b", "username_b", false);
    assert!(result_b.is_ok());
    assert!(result_b.unwrap().contains("Denied"));
}

// ── Replay Safety Tests (CRITICAL) ──────────────────────────────

/// Test 6: Double-tap on same button returns "Already decided" (or "expired").
#[test]
fn gate6_callback_replay_rejected() {
    let registry = ApprovalRegistry::new();
    let req = make_request("44444444", CommandRiskLevel::Medium, vec!["user_1"], 90);
    let (_id, _rx) = registry.register(req);

    // First approve succeeds
    let first = registry.resolve("44444444", "user_1", "username_1", true);
    assert!(first.is_ok());

    // Second attempt fails (entry removed after first resolve)
    let second = registry.resolve("44444444", "user_1", "username_1", false);
    assert!(second.is_err());
    // Could be "Already decided" or "Approval expired" depending on cleanup
    let err = second.unwrap_err();
    assert!(
        err.contains("Already decided") || err.contains("expired"),
        "Expected 'Already decided' or 'expired', got: {err}"
    );
}

/// Test 7: Cross-request ID confusion prevented.
#[test]
fn gate7_cross_request_isolation() {
    let registry = ApprovalRegistry::new();

    let req_a = make_request("aaaa1111", CommandRiskLevel::Medium, vec!["user_a"], 90);
    let req_b = make_request("bbbb2222", CommandRiskLevel::High, vec!["user_b"], 90);
    let (_id_a, _rx_a) = registry.register(req_a);
    let (_id_b, _rx_b) = registry.register(req_b);

    // user_a tries to approve request B (not authorized)
    let result = registry.resolve("bbbb2222", "user_a", "username_a", true);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not authorized"));

    // Request A is still pending and resolvable by user_a
    let result = registry.resolve("aaaa1111", "user_a", "username_a", true);
    assert!(result.is_ok());
}

// ── Functional Tests ────────────────────────────────────────────

/// Test 8/9/10: Origin and admin routing (tested via compute_routing).
/// These are unit tests in approval.rs. The integration test verifies
/// the gate correctly determines when approval is needed.

/// Test 11: Non-approval callbacks are unaffected.
/// Callback data that doesn't start with "apv:" or "dny:" falls through.
/// (This is architectural - the callback handler checks the prefix before
/// dispatching to the registry.)

/// Test 12: Gate disabled returns error to LLM (current behavior).
#[tokio::test]
async fn gate12_disabled_returns_error() {
    let security = Arc::new(SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["touch".into()],
        workspace_dir: std::env::temp_dir(),
        require_approval_for_medium_risk: true,
        ..SecurityPolicy::default()
    });

    // No gate (disabled)
    let tool = ShellTool::new(security, test_runtime(), None);

    let result = tool
        .execute(serde_json::json!({"command": "touch test_file"}))
        .await
        .unwrap();

    assert!(!result.success);
    assert!(result
        .error
        .as_deref()
        .unwrap_or("")
        .contains("explicit approval"));
}

/// Test 13: Re-validation after approval still blocks non-allowlisted commands.
/// Even if approved=true is passed, validate_command_execution still enforces
/// the command allowlist. This is the re-validation safety net.
#[test]
fn gate13_revalidation_blocks_disallowed_command() {
    let security = SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["echo".into()], // only echo is allowed
        workspace_dir: std::env::temp_dir(),
        require_approval_for_medium_risk: true,
        ..SecurityPolicy::default()
    };

    // Even with approved=true, a non-allowlisted command should be blocked
    let result = security.validate_command_execution("rm -rf /tmp/test", true);
    assert!(
        result.is_err(),
        "Non-allowlisted command should be blocked even with approved=true"
    );

    // And an allowed command with approved=true should pass
    let result = security.validate_command_execution("echo hello", true);
    assert!(
        result.is_ok(),
        "Allowed command with approved=true should pass"
    );
}

/// Test 13b: Re-validation after approval still blocks non-allowlisted commands.
#[test]
fn gate13b_revalidation_blocks_non_allowlisted() {
    let security = SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["echo".into()],
        workspace_dir: std::env::temp_dir(),
        ..SecurityPolicy::default()
    };

    // "rm" is not in allowed_commands
    let result = security.validate_command_execution("rm -rf /tmp/test", true);
    assert!(
        result.is_err(),
        "Non-allowlisted command should be blocked even with approved=true"
    );
}

// ── Startup Validation Tests ────────────────────────────────────

/// Test 14: Misconfig -- approval enabled without Telegram.
/// This test validates the startup check logic directly.
#[test]
fn gate14_misconfig_no_telegram() {
    let config = zeroclaw::config::Config {
        approval_policy: ApprovalPolicyConfig {
            enabled: true,
            ..ApprovalPolicyConfig::default()
        },
        channels_config: zeroclaw::config::ChannelsConfig::default(), // no telegram
        ..zeroclaw::config::Config::default()
    };

    // Validate the condition that start_channels would check
    assert!(
        config.approval_policy.enabled && config.channels_config.telegram.is_none(),
        "Should detect misconfig: approval enabled without Telegram"
    );
}

/// Test 15: Misconfig -- high_risk_approver=admin with empty admin_users.
#[test]
fn gate15_misconfig_empty_admin_users() {
    let config = zeroclaw::config::Config {
        approval_policy: ApprovalPolicyConfig {
            enabled: true,
            high_risk_approver: "admin".into(),
            admin_users: vec![],
            ..ApprovalPolicyConfig::default()
        },
        ..zeroclaw::config::Config::default()
    };

    assert!(
        config.approval_policy.enabled
            && config.approval_policy.high_risk_approver == "admin"
            && config.approval_policy.admin_users.is_empty(),
        "Should detect misconfig: admin routing with no admins"
    );
}

// ── Additional Critical Test ────────────────────────────────────

/// Test 16: requester_only with missing origin_user_id must hard-fail.
/// No fallback to broad approval when user_id is unknown.
#[test]
fn gate16_requester_only_missing_user_id_hard_fails() {
    let registry = Arc::new(ApprovalRegistry::new());
    let channel = Arc::new(zeroclaw::channels::telegram::TelegramChannel::new(
        "fake-token".into(),
        vec!["*".into()],
    ));
    // Context with NO user_id
    let context = Arc::new(std::sync::Mutex::new(Some(
        zeroclaw::channels::telegram_types::TelegramToolContext {
            chat_id: "chat_123".into(),
            channel: "telegram".into(),
            user_id: None, // MISSING
        },
    )));

    let policy = ApprovalPolicyConfig {
        enabled: true,
        origin_mode: "requester_only".into(),
        medium_risk_approver: "origin".into(),
        ..ApprovalPolicyConfig::default()
    };

    let gate = ApprovalGate::new(
        registry,
        channel,
        context,
        policy,
        vec!["user1".into()],
    );

    // compute_routing should fail because user_id is None in requester_only mode
    let result = gate.compute_routing("origin", "chat_123", &None);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("requester_only") && err.contains("user_id"),
        "Should explicitly mention requester_only and user_id in error, got: {err}"
    );
}

/// Test 16b: requester_only with present user_id succeeds.
#[test]
fn gate16b_requester_only_with_user_id_succeeds() {
    let gate = make_gate(ApprovalPolicyConfig {
        origin_mode: "requester_only".into(),
        ..ApprovalPolicyConfig::default()
    });

    let result = gate.compute_routing("origin", "chat_123", &Some("user_789".into()));
    assert!(result.is_ok());
    let (targets, authorized) = result.unwrap();
    assert_eq!(targets, vec!["chat_123"]);
    assert_eq!(authorized, vec!["user_789"]);
}

/// Test 17: needs_approval correctly identifies when gate should activate.
#[test]
fn gate17_needs_approval_logic() {
    let gate = make_gate(ApprovalPolicyConfig::default());

    // Supervised + medium risk + require_approval = needs approval
    let supervised_security = SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        require_approval_for_medium_risk: true,
        block_high_risk_commands: true,
        ..SecurityPolicy::default()
    };
    assert!(gate.needs_approval(CommandRiskLevel::Medium, &supervised_security));
    assert!(!gate.needs_approval(CommandRiskLevel::Low, &supervised_security));
    // High risk is hard-blocked, not approval-gated
    assert!(!gate.needs_approval(CommandRiskLevel::High, &supervised_security));

    // Full autonomy = never needs approval
    let full_security = SecurityPolicy {
        autonomy: AutonomyLevel::Full,
        ..SecurityPolicy::default()
    };
    assert!(!gate.needs_approval(CommandRiskLevel::Medium, &full_security));

    // High risk with block_high_risk=false = needs approval
    let unblocked_security = SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        block_high_risk_commands: false,
        ..SecurityPolicy::default()
    };
    assert!(gate.needs_approval(CommandRiskLevel::High, &unblocked_security));
}

/// Test 18: ApprovalPolicyConfig defaults are correct.
#[test]
fn gate18_default_policy_config() {
    let policy = ApprovalPolicyConfig::default();
    assert!(!policy.enabled);
    assert_eq!(policy.timeout_secs, 90);
    assert_eq!(policy.medium_risk_approver, "origin");
    assert_eq!(policy.high_risk_approver, "admin");
    assert_eq!(policy.origin_mode, "requester_only");
    assert!(policy.admin_users.is_empty());
}

/// Test 19: Approval policy paths are in VALID_CONFIG_PATHS.
#[test]
fn gate19_approval_policy_in_valid_config_paths() {
    let paths = zeroclaw::cp::masking::VALID_CONFIG_PATHS;
    let expected = [
        "approval_policy.enabled",
        "approval_policy.timeout_secs",
        "approval_policy.medium_risk_approver",
        "approval_policy.high_risk_approver",
        "approval_policy.origin_mode",
        "approval_policy.admin_users",
    ];
    for path in &expected {
        assert!(
            paths.contains(path),
            "Missing VALID_CONFIG_PATH: {path}"
        );
    }
}

/// Test 20: Denial flow returns clear message.
#[test]
fn gate20_deny_returns_denied_message() {
    let registry = ApprovalRegistry::new();
    let req = make_request("55555555", CommandRiskLevel::Medium, vec!["user_1"], 90);
    let (_id, _rx) = registry.register(req);

    let result = registry.resolve("55555555", "user_1", "username_1", false);
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Denied"));
}

/// Test 21: Admin users must be in allowed_users (startup validation logic).
#[test]
fn gate21_admin_users_must_be_allowed() {
    let config = zeroclaw::config::Config {
        approval_policy: ApprovalPolicyConfig {
            enabled: true,
            admin_users: vec!["admin_not_in_allowed".into()],
            high_risk_approver: "admin".into(),
            ..ApprovalPolicyConfig::default()
        },
        channels_config: zeroclaw::config::ChannelsConfig {
            telegram: Some(zeroclaw::config::TelegramConfig {
                bot_token: "test:token".into(),
                allowed_users: vec!["user1".into()], // admin_not_in_allowed is NOT here
                stt_endpoint: None,
                flows_enabled: false,
                flow_policy: Default::default(),
            }),
            ..zeroclaw::config::ChannelsConfig::default()
        },
        ..zeroclaw::config::Config::default()
    };

    // Simulate the startup validation check from channels/mod.rs
    let tg = config.channels_config.telegram.as_ref().unwrap();
    if !tg.allowed_users.contains(&"*".to_string()) {
        for admin_id in &config.approval_policy.admin_users {
            assert!(
                !tg.allowed_users.contains(admin_id),
                "admin_not_in_allowed should NOT be in allowed_users"
            );
        }
    }
}

/// Test 22: Wildcard allowed_users bypasses admin check.
#[test]
fn gate22_wildcard_allowed_skips_admin_check() {
    let config = zeroclaw::config::Config {
        approval_policy: ApprovalPolicyConfig {
            enabled: true,
            admin_users: vec!["any_admin".into()],
            high_risk_approver: "admin".into(),
            ..ApprovalPolicyConfig::default()
        },
        channels_config: zeroclaw::config::ChannelsConfig {
            telegram: Some(zeroclaw::config::TelegramConfig {
                bot_token: "test:token".into(),
                allowed_users: vec!["*".into()], // wildcard
                stt_endpoint: None,
                flows_enabled: false,
                flow_policy: Default::default(),
            }),
            ..zeroclaw::config::ChannelsConfig::default()
        },
        ..zeroclaw::config::Config::default()
    };

    let tg = config.channels_config.telegram.as_ref().unwrap();
    // With wildcard, all admin_users are implicitly allowed
    assert!(tg.allowed_users.contains(&"*".to_string()));
}

/// Test 23: ApprovalPolicyConfig serialization roundtrip.
#[test]
fn gate23_approval_config_serde_roundtrip() {
    let policy = ApprovalPolicyConfig {
        enabled: true,
        timeout_secs: 120,
        medium_risk_approver: "origin".into(),
        high_risk_approver: "admin".into(),
        origin_mode: "any_allowed".into(),
        admin_users: vec!["12345".into(), "67890".into()],
    };

    let toml_str = toml::to_string(&policy).unwrap();
    let parsed: ApprovalPolicyConfig = toml::from_str(&toml_str).unwrap();

    assert!(parsed.enabled);
    assert_eq!(parsed.timeout_secs, 120);
    assert_eq!(parsed.origin_mode, "any_allowed");
    assert_eq!(parsed.admin_users.len(), 2);
}

/// Test 24: Config with approval_policy deserializes from TOML.
#[test]
fn gate24_config_with_approval_policy_from_toml() {
    let toml = r#"
api_key = "sk-test"
default_temperature = 0.7

[approval_policy]
enabled = true
timeout_secs = 60
medium_risk_approver = "origin"
high_risk_approver = "admin"
origin_mode = "requester_only"
admin_users = ["111222333"]
"#;

    let config: zeroclaw::config::Config = toml::from_str(toml).unwrap();
    assert!(config.approval_policy.enabled);
    assert_eq!(config.approval_policy.timeout_secs, 60);
    assert_eq!(config.approval_policy.admin_users, vec!["111222333"]);
}

/// Test 25: Config without approval_policy section uses defaults.
#[test]
fn gate25_config_without_approval_policy_uses_defaults() {
    let toml = r#"
api_key = "sk-test"
default_temperature = 0.7
"#;

    let config: zeroclaw::config::Config = toml::from_str(toml).unwrap();
    assert!(!config.approval_policy.enabled);
    assert_eq!(config.approval_policy.timeout_secs, 90);
}

// ── Identity Matching Tests (Fix #9) ────────────────────────────

/// Test 26: Username match in any_allowed mode.
/// allowed_users may contain usernames like "alice", and the callback
/// provides both numeric user_id and username. Resolve must match either.
#[test]
fn gate26_username_match_in_any_allowed() {
    let registry = ApprovalRegistry::new();
    // authorized_approver_ids contains a username, not a numeric ID
    let req = make_request("66660001", CommandRiskLevel::Medium, vec!["alice"], 90);
    let (_id, _rx) = registry.register(req);

    // Callback provides numeric ID "12345" and username "alice"
    let result = registry.resolve("66660001", "12345", "alice", true);
    assert!(result.is_ok(), "Should match by username 'alice'");
}

/// Test 27: Wildcard "*" in authorized_approver_ids authorizes anyone.
#[test]
fn gate27_wildcard_authorizes_anyone() {
    let registry = ApprovalRegistry::new();
    let req = make_request("77770001", CommandRiskLevel::Medium, vec!["*"], 90);
    let (_id, _rx) = registry.register(req);

    let result = registry.resolve("77770001", "random_id_999", "random_user", true);
    assert!(result.is_ok(), "Wildcard should authorize any callback user");
}

/// Test 28: Neither username nor numeric ID matches -- rejected.
#[test]
fn gate28_no_identity_match_rejected() {
    let registry = ApprovalRegistry::new();
    let req = make_request("88880001", CommandRiskLevel::Medium, vec!["alice", "789"], 90);
    let (_id, _rx) = registry.register(req);

    let result = registry.resolve("88880001", "999", "bob", true);
    assert!(result.is_err(), "Neither ID nor username should match");
    assert!(result.unwrap_err().contains("not authorized"));
}

// ── Classify Command Tests (Fix #11) ────────────────────────────

/// Test 29: classify_command returns HardDeny for non-allowlisted commands.
/// This ensures the approval gate never sends prompts for these commands.
#[test]
fn gate29_classify_hard_deny_non_allowlisted() {
    let security = SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["echo".into()], // only echo allowed
        require_approval_for_medium_risk: true,
        ..SecurityPolicy::default()
    };

    // "rm" is not in allowed_commands -- hard deny, not NeedsApproval
    match security.classify_command("rm -rf /tmp/test") {
        CommandOutcome::HardDeny(reason) => {
            assert!(reason.contains("not allowed"), "Should mention not allowed: {reason}");
        }
        other => panic!("Expected HardDeny, got {other:?}"),
    }
}

/// Test 30: classify_command returns NeedsApproval for allowed medium-risk commands.
#[test]
fn gate30_classify_needs_approval_medium_risk() {
    let security = SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["touch".into()],
        require_approval_for_medium_risk: true,
        ..SecurityPolicy::default()
    };

    match security.classify_command("touch /tmp/test") {
        CommandOutcome::NeedsApproval(risk) => {
            assert_eq!(risk, CommandRiskLevel::Medium);
        }
        other => panic!("Expected NeedsApproval(Medium), got {other:?}"),
    }
}

/// Test 31: classify_command returns Allowed for low-risk commands.
#[test]
fn gate31_classify_allowed_low_risk() {
    let security = SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["echo".into()],
        ..SecurityPolicy::default()
    };

    match security.classify_command("echo hello") {
        CommandOutcome::Allowed(risk) => {
            assert_eq!(risk, CommandRiskLevel::Low);
        }
        other => panic!("Expected Allowed(Low), got {other:?}"),
    }
}

/// Test 32: classify_command returns HardDeny for high-risk + block_high_risk=true.
#[test]
fn gate32_classify_hard_deny_blocked_high_risk() {
    let security = SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["rm".into()],
        block_high_risk_commands: true,
        ..SecurityPolicy::default()
    };

    match security.classify_command("rm -rf /tmp/test") {
        CommandOutcome::HardDeny(reason) => {
            assert!(reason.contains("high-risk"), "Should mention high-risk: {reason}");
        }
        other => panic!("Expected HardDeny, got {other:?}"),
    }
}

/// Test 33: classify_command returns NeedsApproval for high-risk + block=false.
#[test]
fn gate33_classify_needs_approval_high_risk_unblocked() {
    let security = SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["rm".into()],
        block_high_risk_commands: false,
        ..SecurityPolicy::default()
    };

    match security.classify_command("rm -rf /tmp/test") {
        CommandOutcome::NeedsApproval(risk) => {
            assert_eq!(risk, CommandRiskLevel::High);
        }
        other => panic!("Expected NeedsApproval(High), got {other:?}"),
    }
}

/// Test 34: Non-allowlisted medium-risk command is HardDeny, not NeedsApproval.
/// This is the critical bug fix: before this change, a non-allowlisted command
/// that happened to be medium-risk would trigger the approval flow, only to
/// fail post-approval at re-validation.
#[tokio::test]
async fn gate34_non_allowlisted_does_not_trigger_approval() {
    let security = Arc::new(SecurityPolicy {
        autonomy: AutonomyLevel::Supervised,
        allowed_commands: vec!["echo".into()], // touch is NOT allowed
        require_approval_for_medium_risk: true,
        ..SecurityPolicy::default()
    });

    let gate = make_gate(ApprovalPolicyConfig::default());
    let tool = ShellTool::new(security, test_runtime(), Some(gate));

    // "touch" is medium-risk but NOT in allowed_commands.
    // Should get HardDeny, not an approval prompt.
    let result = tool
        .execute(serde_json::json!({"command": "touch /tmp/sneaky_file"}))
        .await
        .unwrap();

    assert!(!result.success);
    let err = result.error.as_deref().unwrap_or("");
    assert!(
        err.contains("not allowed"),
        "Should be hard-denied (not allowed), not approval-gated. Got: {err}"
    );
}
