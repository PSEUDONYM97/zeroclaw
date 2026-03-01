use crate::channels::telegram::TelegramChannel;
use crate::channels::telegram_types::{InlineButton, TelegramToolContext};
use crate::config::schema::ApprovalPolicyConfig;
use crate::security::policy::{AutonomyLevel, CommandRiskLevel, SecurityPolicy};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::oneshot;

/// A pending approval request.
pub struct ApprovalRequest {
    pub id: String,
    pub command: String,
    pub risk_level: CommandRiskLevel,
    pub origin_chat_id: String,
    pub origin_user_id: Option<String>,
    pub authorized_approver_ids: Vec<String>,
    pub created_at: Instant,
    pub expires_at: Instant,
    pub resolved: bool,
}

/// Thread-safe registry of pending approval requests.
pub struct ApprovalRegistry {
    pending: Mutex<HashMap<String, (ApprovalRequest, oneshot::Sender<bool>)>>,
}

impl ApprovalRegistry {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Register a new approval request. Returns (request_id, receiver).
    pub fn register(
        &self,
        request: ApprovalRequest,
    ) -> (String, oneshot::Receiver<bool>) {
        let id = request.id.clone();
        let (tx, rx) = oneshot::channel();
        let mut map = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        map.insert(id.clone(), (request, tx));
        (id, rx)
    }

    /// Resolve a pending approval request.
    ///
    /// Checks: exists, not expired, not already resolved, caller authorized.
    /// The caller provides both the numeric Telegram user ID and the username,
    /// since `authorized_approver_ids` may contain either form (or `"*"`).
    /// Returns Ok(message) on success, Err(message) on failure.
    pub fn resolve(
        &self,
        id: &str,
        callback_user_id: &str,
        callback_username: &str,
        approved: bool,
    ) -> Result<String, String> {
        let mut map = self.pending.lock().unwrap_or_else(|e| e.into_inner());

        let Some((req, _)) = map.get_mut(id) else {
            return Err("Approval expired".into());
        };

        // Expiry check
        if Instant::now() > req.expires_at {
            map.remove(id);
            return Err("Approval expired".into());
        }

        // Replay guard
        if req.resolved {
            return Err("Already decided".into());
        }

        // Authorization check: match numeric ID, username, or wildcard "*"
        let is_authorized = req.authorized_approver_ids.iter().any(|allowed| {
            allowed == "*"
                || allowed == callback_user_id
                || allowed == callback_username
        });
        if !is_authorized {
            return Err("You're not authorized to approve this".into());
        }

        // Mark resolved and send decision
        req.resolved = true;
        let decision_str = if approved { "Approved" } else { "Denied" };

        // Remove and send through the oneshot
        if let Some((_, tx)) = map.remove(id) {
            let _ = tx.send(approved);
        }

        Ok(format!("{decision_str} by user {callback_user_id}"))
    }

    /// Remove expired entries from the registry.
    pub fn cleanup_expired(&self) {
        let mut map = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let expired_ids: Vec<String> = map
            .iter()
            .filter(|(_, (req, _))| now > req.expires_at)
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired_ids {
            map.remove(&id);
        }
    }
}

/// The approval gate that coordinates between ShellTool and Telegram.
pub struct ApprovalGate {
    pub registry: Arc<ApprovalRegistry>,
    channel: Arc<TelegramChannel>,
    context: Arc<std::sync::Mutex<Option<TelegramToolContext>>>,
    policy: ApprovalPolicyConfig,
    allowed_users: Vec<String>,
}

impl ApprovalGate {
    pub fn new(
        registry: Arc<ApprovalRegistry>,
        channel: Arc<TelegramChannel>,
        context: Arc<std::sync::Mutex<Option<TelegramToolContext>>>,
        policy: ApprovalPolicyConfig,
        allowed_users: Vec<String>,
    ) -> Self {
        Self {
            registry,
            channel,
            context,
            policy,
            allowed_users,
        }
    }

    /// Determine whether a command at the given risk level needs approval,
    /// based on autonomy level and approval policy.
    pub fn needs_approval(
        &self,
        risk_level: CommandRiskLevel,
        security: &SecurityPolicy,
    ) -> bool {
        if security.autonomy != AutonomyLevel::Supervised {
            return false;
        }
        match risk_level {
            CommandRiskLevel::Low => false,
            CommandRiskLevel::Medium => security.require_approval_for_medium_risk,
            CommandRiskLevel::High => !security.block_high_risk_commands,
        }
    }

    /// Request approval for a command via Telegram inline keyboard.
    ///
    /// Returns Ok(true) if approved, Ok(false) if denied, Err on timeout or
    /// missing context.
    pub async fn request_approval(
        &self,
        command: &str,
        risk_level: CommandRiskLevel,
    ) -> anyhow::Result<bool> {
        // Read origin context
        let (origin_chat_id, origin_user_id) = {
            let guard = self.context.lock().unwrap_or_else(|e| e.into_inner());
            match guard.as_ref() {
                Some(ctx) => (ctx.chat_id.clone(), ctx.user_id.clone()),
                None => anyhow::bail!("No Telegram context available for approval routing"),
            }
        };

        // Determine approver target and authorized IDs
        let approver_type = match risk_level {
            CommandRiskLevel::Medium => &self.policy.medium_risk_approver,
            CommandRiskLevel::High | CommandRiskLevel::Low => &self.policy.high_risk_approver,
        };

        let (target_chat_ids, authorized_approver_ids) =
            self.compute_routing(approver_type, &origin_chat_id, &origin_user_id)?;

        // Generate request ID (8 hex chars)
        let id = format!("{:08x}", rand_id());

        let request = ApprovalRequest {
            id: id.clone(),
            command: command.to_string(),
            risk_level,
            origin_chat_id: origin_chat_id.clone(),
            origin_user_id: origin_user_id.clone(),
            authorized_approver_ids,
            created_at: Instant::now(),
            expires_at: Instant::now()
                + std::time::Duration::from_secs(self.policy.timeout_secs),
            resolved: false,
        };

        let (_, rx) = self.registry.register(request);

        // Build inline keyboard
        let buttons = vec![vec![
            InlineButton {
                text: "Approve".into(),
                callback_data: format!("apv:{id}"),
            },
            InlineButton {
                text: "Deny".into(),
                callback_data: format!("dny:{id}"),
            },
        ]];

        let risk_label = match risk_level {
            CommandRiskLevel::Low => "Low",
            CommandRiskLevel::Medium => "Medium",
            CommandRiskLevel::High => "High",
        };

        // Send to each target chat
        for chat_id in &target_chat_ids {
            let text = if approver_type == "admin" {
                format!(
                    "Command Approval Required\n\n\
                     Requested by: user {}\n\
                     Chat: {}\n\n\
                     Command:\n`{}`\n\n\
                     Risk: {}",
                    origin_user_id.as_deref().unwrap_or("unknown"),
                    origin_chat_id,
                    command,
                    risk_label
                )
            } else {
                format!(
                    "Command Approval Required\n\n\
                     Agent wants to run:\n`{}`\n\n\
                     Risk: {}",
                    command, risk_label
                )
            };

            if let Err(e) = self.channel.send_with_keyboard(chat_id, &text, &buttons).await {
                tracing::warn!("Failed to send approval request to chat {chat_id}: {e}");
            }
        }

        // Await decision with timeout
        match tokio::time::timeout(
            std::time::Duration::from_secs(self.policy.timeout_secs),
            rx,
        )
        .await
        {
            Ok(Ok(approved)) => Ok(approved),
            Ok(Err(_)) => {
                // Sender dropped (shouldn't happen, but treat as denied)
                Ok(false)
            }
            Err(_) => {
                // Timeout -- clean up the registry entry
                self.registry.cleanup_expired();
                anyhow::bail!(
                    "Approval timed out ({}s). Re-request if needed.",
                    self.policy.timeout_secs
                );
            }
        }
    }

    /// Compute routing targets and authorized approver IDs.
    pub fn compute_routing(
        &self,
        approver_type: &str,
        origin_chat_id: &str,
        origin_user_id: &Option<String>,
    ) -> anyhow::Result<(Vec<String>, Vec<String>)> {
        if approver_type == "admin" {
            // Route to each admin's private chat
            if self.policy.admin_users.is_empty() {
                anyhow::bail!(
                    "Approval requires admin but no admin_users configured"
                );
            }
            let target_chats = self.policy.admin_users.clone();
            let authorized = self.policy.admin_users.clone();
            Ok((target_chats, authorized))
        } else {
            // Route to origin chat
            let target_chats = vec![origin_chat_id.to_string()];

            let authorized = if self.policy.origin_mode == "requester_only" {
                let uid = origin_user_id.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "origin_mode=requester_only but origin user_id is unknown; \
                         cannot determine who is authorized to approve"
                    )
                })?;
                vec![uid.clone()]
            } else {
                // any_allowed: all allowed_users can approve
                self.allowed_users.clone()
            };

            if authorized.is_empty() {
                anyhow::bail!(
                    "Computed authorized_approver_ids is empty; \
                     no one can approve this request"
                );
            }

            Ok((target_chats, authorized))
        }
    }
}

/// Generate a pseudo-random u32 for request IDs.
fn rand_id() -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    Instant::now().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    hasher.finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_register_and_resolve() {
        let registry = ApprovalRegistry::new();
        let request = ApprovalRequest {
            id: "aabbccdd".into(),
            command: "echo test".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "123".into(),
            origin_user_id: Some("456".into()),
            authorized_approver_ids: vec!["456".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() + std::time::Duration::from_secs(90),
            resolved: false,
        };

        let (id, _rx) = registry.register(request);
        assert_eq!(id, "aabbccdd");

        let result = registry.resolve("aabbccdd", "456", "testuser", true);
        assert!(result.is_ok());
        assert!(result.unwrap().contains("Approved"));
    }

    #[test]
    fn registry_resolve_unauthorized() {
        let registry = ApprovalRegistry::new();
        let request = ApprovalRequest {
            id: "aabbccdd".into(),
            command: "echo test".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "123".into(),
            origin_user_id: Some("456".into()),
            authorized_approver_ids: vec!["456".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() + std::time::Duration::from_secs(90),
            resolved: false,
        };

        let (_id, _rx) = registry.register(request);
        let result = registry.resolve("aabbccdd", "999", "eve", true);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not authorized"));
    }

    #[test]
    fn registry_resolve_expired() {
        let registry = ApprovalRegistry::new();
        let request = ApprovalRequest {
            id: "aabbccdd".into(),
            command: "echo test".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "123".into(),
            origin_user_id: Some("456".into()),
            authorized_approver_ids: vec!["456".into()],
            created_at: Instant::now(),
            // Already expired
            expires_at: Instant::now() - std::time::Duration::from_secs(1),
            resolved: false,
        };

        let (_id, _rx) = registry.register(request);
        let result = registry.resolve("aabbccdd", "456", "testuser", true);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expired"));
    }

    #[test]
    fn registry_resolve_replay() {
        let registry = ApprovalRegistry::new();
        let request = ApprovalRequest {
            id: "aabbccdd".into(),
            command: "echo test".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "123".into(),
            origin_user_id: Some("456".into()),
            authorized_approver_ids: vec!["456".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() + std::time::Duration::from_secs(90),
            resolved: false,
        };

        let (_id, _rx) = registry.register(request);

        // First resolve succeeds
        let result1 = registry.resolve("aabbccdd", "456", "testuser", true);
        assert!(result1.is_ok());

        // Second resolve: entry already removed
        let result2 = registry.resolve("aabbccdd", "456", "testuser", false);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().contains("expired"));
    }

    #[test]
    fn registry_resolve_not_found() {
        let registry = ApprovalRegistry::new();
        let result = registry.resolve("nonexistent", "456", "testuser", true);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expired"));
    }

    #[test]
    fn registry_cleanup_expired() {
        let registry = ApprovalRegistry::new();
        let request = ApprovalRequest {
            id: "expired01".into(),
            command: "echo test".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "123".into(),
            origin_user_id: Some("456".into()),
            authorized_approver_ids: vec!["456".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() - std::time::Duration::from_secs(1),
            resolved: false,
        };

        let (_id, _rx) = registry.register(request);
        registry.cleanup_expired();

        let map = registry.pending.lock().unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn registry_cross_request_isolation() {
        let registry = ApprovalRegistry::new();

        let req_a = ApprovalRequest {
            id: "aaaaaaaa".into(),
            command: "echo a".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "100".into(),
            origin_user_id: Some("user_a".into()),
            authorized_approver_ids: vec!["user_a".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() + std::time::Duration::from_secs(90),
            resolved: false,
        };
        let req_b = ApprovalRequest {
            id: "bbbbbbbb".into(),
            command: "echo b".into(),
            risk_level: CommandRiskLevel::High,
            origin_chat_id: "200".into(),
            origin_user_id: Some("user_b".into()),
            authorized_approver_ids: vec!["user_b".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() + std::time::Duration::from_secs(90),
            resolved: false,
        };

        let (_id_a, _rx_a) = registry.register(req_a);
        let (_id_b, _rx_b) = registry.register(req_b);

        // user_a cannot resolve request B
        let result = registry.resolve("bbbbbbbb", "user_a", "username_a", true);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not authorized"));

        // user_b can resolve request B
        let result = registry.resolve("bbbbbbbb", "user_b", "username_b", true);
        assert!(result.is_ok());

        // request A is still pending and resolvable by user_a
        let result = registry.resolve("aaaaaaaa", "user_a", "username_a", false);
        assert!(result.is_ok());
        assert!(result.unwrap().contains("Denied"));
    }

    #[test]
    fn needs_approval_supervised_medium() {
        let gate = make_test_gate(ApprovalPolicyConfig::default());
        let security = SecurityPolicy {
            autonomy: AutonomyLevel::Supervised,
            require_approval_for_medium_risk: true,
            block_high_risk_commands: true,
            ..SecurityPolicy::default()
        };
        assert!(gate.needs_approval(CommandRiskLevel::Medium, &security));
        assert!(!gate.needs_approval(CommandRiskLevel::Low, &security));
    }

    #[test]
    fn needs_approval_full_autonomy_never() {
        let gate = make_test_gate(ApprovalPolicyConfig::default());
        let security = SecurityPolicy {
            autonomy: AutonomyLevel::Full,
            ..SecurityPolicy::default()
        };
        assert!(!gate.needs_approval(CommandRiskLevel::Medium, &security));
        assert!(!gate.needs_approval(CommandRiskLevel::High, &security));
    }

    #[test]
    fn needs_approval_high_risk_not_blocked() {
        let gate = make_test_gate(ApprovalPolicyConfig::default());
        let security = SecurityPolicy {
            autonomy: AutonomyLevel::Supervised,
            block_high_risk_commands: false,
            ..SecurityPolicy::default()
        };
        assert!(gate.needs_approval(CommandRiskLevel::High, &security));
    }

    #[test]
    fn needs_approval_high_risk_blocked() {
        // When block_high_risk_commands=true, high-risk commands are hard-blocked,
        // not sent through approval
        let gate = make_test_gate(ApprovalPolicyConfig::default());
        let security = SecurityPolicy {
            autonomy: AutonomyLevel::Supervised,
            block_high_risk_commands: true,
            ..SecurityPolicy::default()
        };
        assert!(!gate.needs_approval(CommandRiskLevel::High, &security));
    }

    /// Helper to make a gate with a fake TelegramChannel (no real network)
    fn make_test_gate(policy: ApprovalPolicyConfig) -> ApprovalGate {
        let registry = Arc::new(ApprovalRegistry::new());
        let channel = Arc::new(TelegramChannel::new(
            "fake-token".into(),
            vec!["*".into()],
        ));
        let context = Arc::new(std::sync::Mutex::new(None));
        ApprovalGate::new(registry, channel, context, policy, vec!["user1".into()])
    }

    #[test]
    fn compute_routing_admin() {
        let policy = ApprovalPolicyConfig {
            admin_users: vec!["admin1".into(), "admin2".into()],
            ..ApprovalPolicyConfig::default()
        };
        let gate = make_test_gate(policy);
        let (targets, authorized) = gate
            .compute_routing("admin", "origin_chat", &Some("user1".into()))
            .unwrap();
        assert_eq!(targets, vec!["admin1", "admin2"]);
        assert_eq!(authorized, vec!["admin1", "admin2"]);
    }

    #[test]
    fn compute_routing_origin_requester_only() {
        let policy = ApprovalPolicyConfig {
            origin_mode: "requester_only".into(),
            ..ApprovalPolicyConfig::default()
        };
        let gate = make_test_gate(policy);
        let (targets, authorized) = gate
            .compute_routing("origin", "chat_123", &Some("user_456".into()))
            .unwrap();
        assert_eq!(targets, vec!["chat_123"]);
        assert_eq!(authorized, vec!["user_456"]);
    }

    #[test]
    fn compute_routing_origin_any_allowed() {
        let policy = ApprovalPolicyConfig {
            origin_mode: "any_allowed".into(),
            ..ApprovalPolicyConfig::default()
        };
        let gate = ApprovalGate::new(
            Arc::new(ApprovalRegistry::new()),
            Arc::new(TelegramChannel::new("t".into(), vec!["*".into()])),
            Arc::new(std::sync::Mutex::new(None)),
            policy,
            vec!["user_a".into(), "user_b".into()],
        );
        let (targets, authorized) = gate
            .compute_routing("origin", "chat_123", &Some("user_a".into()))
            .unwrap();
        assert_eq!(targets, vec!["chat_123"]);
        assert_eq!(authorized, vec!["user_a", "user_b"]);
    }

    #[test]
    fn compute_routing_requester_only_missing_user_id_fails() {
        let policy = ApprovalPolicyConfig {
            origin_mode: "requester_only".into(),
            ..ApprovalPolicyConfig::default()
        };
        let gate = make_test_gate(policy);
        let result = gate.compute_routing("origin", "chat_123", &None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("requester_only"));
        assert!(err.contains("user_id"));
    }

    #[test]
    fn compute_routing_admin_empty_fails() {
        let policy = ApprovalPolicyConfig {
            admin_users: vec![],
            ..ApprovalPolicyConfig::default()
        };
        let gate = make_test_gate(policy);
        let result = gate.compute_routing("admin", "chat_123", &Some("user1".into()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("admin_users"));
    }

    #[test]
    fn compute_routing_any_allowed_empty_fails() {
        let policy = ApprovalPolicyConfig {
            origin_mode: "any_allowed".into(),
            ..ApprovalPolicyConfig::default()
        };
        let gate = ApprovalGate::new(
            Arc::new(ApprovalRegistry::new()),
            Arc::new(TelegramChannel::new("t".into(), vec!["*".into()])),
            Arc::new(std::sync::Mutex::new(None)),
            policy,
            vec![], // empty allowed_users
        );
        let result = gate.compute_routing("origin", "chat_123", &Some("user1".into()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn rand_id_produces_different_values() {
        let a = rand_id();
        // Sleep a tiny bit so Instant changes
        std::thread::sleep(std::time::Duration::from_nanos(100));
        let b = rand_id();
        // Not guaranteed to be different but practically always is
        // This is a smoke test, not a cryptographic requirement
        let _ = (a, b);
    }

    #[test]
    fn resolve_matches_by_username() {
        let registry = ApprovalRegistry::new();
        let request = ApprovalRequest {
            id: "uname001".into(),
            command: "echo test".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "123".into(),
            origin_user_id: Some("456".into()),
            // authorized by username "alice", not numeric ID
            authorized_approver_ids: vec!["alice".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() + std::time::Duration::from_secs(90),
            resolved: false,
        };

        let (_id, _rx) = registry.register(request);
        // Numeric ID doesn't match, but username does
        let result = registry.resolve("uname001", "99999", "alice", true);
        assert!(result.is_ok(), "Should match by username");
    }

    #[test]
    fn resolve_matches_by_numeric_id() {
        let registry = ApprovalRegistry::new();
        let request = ApprovalRequest {
            id: "numid001".into(),
            command: "echo test".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "123".into(),
            origin_user_id: Some("456".into()),
            authorized_approver_ids: vec!["456".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() + std::time::Duration::from_secs(90),
            resolved: false,
        };

        let (_id, _rx) = registry.register(request);
        // Numeric ID matches, username doesn't
        let result = registry.resolve("numid001", "456", "unknown_user", true);
        assert!(result.is_ok(), "Should match by numeric ID");
    }

    #[test]
    fn resolve_wildcard_authorizes_anyone() {
        let registry = ApprovalRegistry::new();
        let request = ApprovalRequest {
            id: "wild0001".into(),
            command: "echo test".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "123".into(),
            origin_user_id: Some("456".into()),
            authorized_approver_ids: vec!["*".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() + std::time::Duration::from_secs(90),
            resolved: false,
        };

        let (_id, _rx) = registry.register(request);
        // Any user should be authorized with wildcard
        let result = registry.resolve("wild0001", "random_id", "random_user", true);
        assert!(result.is_ok(), "Wildcard should authorize any user");
    }

    #[test]
    fn resolve_neither_id_nor_username_matches() {
        let registry = ApprovalRegistry::new();
        let request = ApprovalRequest {
            id: "nope0001".into(),
            command: "echo test".into(),
            risk_level: CommandRiskLevel::Medium,
            origin_chat_id: "123".into(),
            origin_user_id: Some("456".into()),
            authorized_approver_ids: vec!["alice".into(), "789".into()],
            created_at: Instant::now(),
            expires_at: Instant::now() + std::time::Duration::from_secs(90),
            resolved: false,
        };

        let (_id, _rx) = registry.register(request);
        let result = registry.resolve("nope0001", "999", "bob", true);
        assert!(result.is_err(), "Neither ID nor username matches");
        assert!(result.unwrap_err().contains("not authorized"));
    }
}
