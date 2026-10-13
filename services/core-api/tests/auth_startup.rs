use std::process::Command;

fn startup_error(settings: &[(&str, &str)]) -> String {
    // All cases fail during configuration, before any infrastructure is accessed.
    let output = Command::new(env!("CARGO_BIN_EXE_tackt-core-api"))
        .env_clear()
        .env(
            "CORE_API_OPERATOR_TOKEN",
            uuid::Uuid::new_v4().to_string().as_str(),
        )
        .envs(settings.iter().copied())
        .output()
        .unwrap();
    assert!(!output.status.success());
    String::from_utf8(output.stderr).unwrap()
}

#[test]
fn missing_auth_settings_fail_startup_instead_of_disabling_login() {
    assert!(startup_error(&[]).contains("CORE_API_AUTH_ISSUER is required"));
    assert!(
        startup_error(&[("CORE_API_AUTH_ISSUER", "https://api.example")])
            .contains("CORE_API_AUTH_FRONTEND_URI is required")
    );
    assert!(
        startup_error(&[
            ("CORE_API_AUTH_ISSUER", "https://api.example"),
            ("CORE_API_AUTH_FRONTEND_URI", "https://app.example"),
        ])
        .contains("CORE_API_AUTH_JWT_SECRET is required")
    );
}

#[test]
fn per_wallet_settings_are_rejected_with_shared_uri_migration_guidance() {
    for setting in ["CORE_API_AUTH_SOLANA_URI", "CORE_API_AUTH_EVM_URI"] {
        let error = startup_error(&[(setting, "https://app.example")]);
        assert!(error.contains(setting));
        assert!(error.contains("CORE_API_AUTH_FRONTEND_URI for both wallet methods"));
    }
}

#[test]
fn beta_invite_configuration_is_optional_but_rejects_invalid_values_without_echoing_them() {
    let secret = uuid::Uuid::new_v4().to_string();
    let jwt_secret = hex::encode(secret.as_bytes());
    let mut settings = vec![
        ("CORE_API_AUTH_ISSUER", "https://api.example"),
        ("CORE_API_AUTH_FRONTEND_URI", "https://app.example"),
        ("CORE_API_AUTH_JWT_SECRET", jwt_secret.as_str()),
    ];
    assert!(startup_error(&settings).contains("DATABASE_URL is required"));
    let invalid = [
        String::new(),
        format!(" {}", secret.as_str()),
        "x\ny".into(),
        "x".repeat(129),
    ];
    for code in &invalid {
        settings.push(("CORE_API_BETA_INVITE_CODE", code));
        let error = startup_error(&settings);
        assert!(error.contains("CORE_API_BETA_INVITE_CODE must contain"));
        assert!(!error.contains(secret.as_str()));
        settings.pop();
    }
    settings.push(("CORE_API_BETA_INVITE_CODE", secret.as_str()));
    assert!(startup_error(&settings).contains("DATABASE_URL is required"));
}

#[test]
fn removed_code_flow_configuration_is_rejected_without_echoing_credentials() {
    for field in ["client_secret", "redirect_url"] {
        let mut config = serde_json::json!({"name":"google", "issuer":"https://accounts.google.com", "client_id":"client"});
        config[field] = serde_json::json!("private-placeholder");
        let error = serde_json::from_value::<tackt_core_api::auth::ProviderConfig>(config)
            .err()
            .unwrap();
        assert!(error.to_string().contains("unknown field"));
        assert!(!error.to_string().contains("private-placeholder"));
    }
}
