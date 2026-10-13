//! HyperCore action signing compatible with hyperliquid-python-sdk.
//! https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/signing.py

use super::{SigningError, addresses_for_keys};
use serde::Serialize;
use serde_json::{Value, json, value::RawValue};
use sha3::{Digest, Keccak256};
use tackt_postgres::{PgRepository, UserSigningKeys};
use uuid::Uuid;

mod actions;
pub use actions::{
    Builder, Cancel, CancelByCloid, Grouping, L1Action, Modify, Order, OrderType, TimeInForce,
    TriggerKind, UserAction,
};

#[derive(Clone, Copy, Debug)]
pub enum Network {
    Mainnet,
    Testnet,
}
impl Network {
    fn source(self) -> &'static str {
        match self {
            Self::Mainnet => "a",
            Self::Testnet => "b",
        }
    }
    fn chain(self) -> &'static str {
        match self {
            Self::Mainnet => "Mainnet",
            Self::Testnet => "Testnet",
        }
    }
}

#[derive(Clone, Debug)]
pub enum Action {
    L1(L1Action),
    User(UserAction),
}

#[derive(Clone, Debug)]
pub struct HypercoreTransaction {
    pub action: Action,
    pub network: Network,
    /// Caller-assigned nonce, normally Unix milliseconds. Reuse it for an exact retry.
    pub nonce: u64,
    /// Optional vault/subaccount for L1 actions that permit one.
    pub vault_address: Option<String>,
    /// Unix milliseconds; supported by L1 actions only.
    pub expires_after: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct Signature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

/// Ready-to-submit /exchange body. Serialize directly to preserve action field order.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedAction {
    pub action: Box<RawValue>,
    pub nonce: u64,
    pub signature: Signature,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_after: Option<u64>,
}

#[derive(Debug)]
pub struct SignedTransaction {
    pub address: String,
    pub payload: SignedAction,
}

/// Signs with this user's stored key and returns the signed payload without broadcasting.
pub async fn sign(
    repo: &PgRepository,
    user_id: Uuid,
    transaction: HypercoreTransaction,
) -> Result<SignedTransaction, SigningError> {
    let keys = super::keys::load_user_keys(repo, user_id).await?;
    sign_with_keys(&keys, transaction)
}

fn sign_with_keys(
    keys: &UserSigningKeys,
    transaction: HypercoreTransaction,
) -> Result<SignedTransaction, SigningError> {
    let (action, digest, vault_address) = prepare(&transaction)?;
    let key = k256::ecdsa::SigningKey::from_slice(&keys.ethereum_private_key)
        .map_err(|_| SigningError::InvalidKey)?;
    let (signature, recovery) = key
        .sign_prehash_recoverable(&digest)
        .map_err(|_| SigningError::SigningFailed)?;
    if recovery.is_x_reduced() {
        return Err(SigningError::SigningFailed);
    }
    Ok(SignedTransaction {
        address: addresses_for_keys(keys)?.ethereum,
        payload: SignedAction {
            action,
            nonce: transaction.nonce,
            vault_address,
            expires_after: transaction.expires_after,
            signature: Signature {
                r: format!("0x{}", hex::encode(signature.r().to_bytes())),
                s: format!("0x{}", hex::encode(signature.s().to_bytes())),
                v: 27 + u8::from(recovery.is_y_odd()),
            },
        },
    })
}

type PreparedAction = (Box<RawValue>, [u8; 32], Option<String>);

fn prepare(transaction: &HypercoreTransaction) -> Result<PreparedAction, SigningError> {
    match &transaction.action {
        Action::L1(action) => {
            let mut action = action.clone();
            normalize_l1(&mut action)?;
            if transaction.vault_address.is_some()
                && matches!(
                    action,
                    L1Action::VaultTransfer { .. }
                        | L1Action::SubAccountTransfer { .. }
                        | L1Action::CreateSubAccount { .. }
                )
            {
                return Err(invalid(
                    "this action must be signed by the master account without vault_address",
                ));
            }
            let vault = transaction
                .vault_address
                .as_deref()
                .map(normalize_address)
                .transpose()?;
            let connection_id = action_hash(
                &action,
                transaction.nonce,
                vault.as_deref(),
                transaction.expires_after,
            )?;
            let agent = struct_hash(
                "Agent(string source,bytes32 connectionId)",
                &[hash(transaction.network.source()), connection_id],
            );
            let digest = typed_digest("Exchange", 1337, agent);
            let json = serde_json::value::to_raw_value(&action)
                .map_err(|_| SigningError::SigningFailed)?;
            Ok((json, digest, vault))
        }
        Action::User(action) => {
            if transaction.vault_address.is_some() || transaction.expires_after.is_some() {
                return Err(invalid(
                    "user-signed transfers do not accept vault_address or expires_after; use explicit subaccount fields",
                ));
            }
            let (payload, type_definition, fields) =
                user_payload(action, transaction.network, transaction.nonce)?;
            let digest = typed_digest(
                "HyperliquidSignTransaction",
                421614,
                struct_hash(type_definition, &fields),
            );
            Ok((
                serde_json::value::to_raw_value(&payload)
                    .map_err(|_| SigningError::SigningFailed)?,
                digest,
                None,
            ))
        }
    }
}

fn action_hash(
    action: &L1Action,
    nonce: u64,
    vault: Option<&str>,
    expires: Option<u64>,
) -> Result<[u8; 32], SigningError> {
    let mut bytes = rmp_serde::to_vec_named(action).map_err(|_| SigningError::SigningFailed)?;
    bytes.extend_from_slice(&nonce.to_be_bytes());
    match vault {
        None => bytes.push(0),
        Some(address) => {
            bytes.push(1);
            bytes.extend(hex::decode(&address[2..]).map_err(|_| invalid("invalid vault address"))?);
        }
    }
    if let Some(expires) = expires {
        bytes.push(0);
        bytes.extend_from_slice(&expires.to_be_bytes());
    }
    Ok(hash(bytes))
}

fn hash(value: impl AsRef<[u8]>) -> [u8; 32] {
    Keccak256::digest(value.as_ref()).into()
}
fn uint(value: u64) -> [u8; 32] {
    let mut result = [0; 32];
    result[24..].copy_from_slice(&value.to_be_bytes());
    result
}
fn struct_hash(definition: &str, fields: &[[u8; 32]]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(hash(definition));
    for field in fields {
        hasher.update(field);
    }
    hasher.finalize().into()
}
fn typed_digest(domain_name: &str, chain_id: u64, message: [u8; 32]) -> [u8; 32] {
    let domain = struct_hash(
        "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        &[hash(domain_name), hash("1"), uint(chain_id), [0; 32]],
    );
    let mut bytes = Vec::from([0x19, 0x01]);
    bytes.extend_from_slice(&domain);
    bytes.extend_from_slice(&message);
    hash(bytes)
}
fn invalid(message: &'static str) -> SigningError {
    SigningError::InvalidTransaction(message)
}
fn normalize_address(value: &str) -> Result<String, SigningError> {
    if !value.starts_with("0x")
        || value.len() != 42
        || !value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(invalid("expected a 0x-prefixed 20-byte address"));
    }
    Ok(value.to_ascii_lowercase())
}
fn decimal(value: &str) -> Result<String, SigningError> {
    if value.is_empty() || value.len() > 128 {
        return Err(invalid("invalid decimal amount"));
    }
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid(
            "amounts and prices must be unsigned decimal strings",
        ));
    }
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let fraction = fraction.trim_end_matches('0');
    Ok(if fraction.is_empty() {
        whole.into()
    } else {
        format!("{whole}.{fraction}")
    })
}
fn normalize_cloid(cloid: &mut String) -> Result<(), SigningError> {
    if !cloid.starts_with("0x")
        || cloid.len() != 34
        || !cloid.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(invalid("cloid must be 0x-prefixed 16-byte hex"));
    }
    cloid.make_ascii_lowercase();
    Ok(())
}
fn normalize_order(order: &mut Order) -> Result<(), SigningError> {
    order.price = decimal(&order.price)?;
    order.size = decimal(&order.size)?;
    if let OrderType::Trigger { trigger_px, .. } = &mut order.order_type {
        *trigger_px = decimal(trigger_px)?;
    }
    if let Some(cloid) = &mut order.cloid {
        normalize_cloid(cloid)?;
    }
    Ok(())
}
fn normalize_l1(action: &mut L1Action) -> Result<(), SigningError> {
    match action {
        L1Action::Order {
            orders, builder, ..
        } => {
            for order in orders {
                normalize_order(order)?;
            }
            if let Some(builder) = builder {
                builder.address = normalize_address(&builder.address)?;
            }
        }
        L1Action::BatchModify { modifies } => {
            for modify in modifies {
                normalize_order(&mut modify.order)?;
            }
        }
        L1Action::CancelByCloid { cancels } => {
            for cancel in cancels {
                normalize_cloid(&mut cancel.cloid)?;
            }
        }
        L1Action::SubAccountTransfer {
            sub_account_user, ..
        } => *sub_account_user = normalize_address(sub_account_user)?,
        L1Action::VaultTransfer { vault_address, .. } => {
            *vault_address = normalize_address(vault_address)?
        }
        _ => {}
    }
    Ok(())
}

type UserPayload = (Value, &'static str, Vec<[u8; 32]>);
fn user_payload(
    action: &UserAction,
    network: Network,
    nonce: u64,
) -> Result<UserPayload, SigningError> {
    let chain = network.chain();
    let (mut payload, definition, fields) = match action {
        UserAction::UsdSend {
            destination,
            amount,
        }
        | UserAction::Withdraw {
            destination,
            amount,
        } => {
            let destination = normalize_address(destination)?;
            let amount = decimal(amount)?;
            let (kind, definition) = if matches!(action, UserAction::UsdSend { .. }) {
                (
                    "usdSend",
                    "HyperliquidTransaction:UsdSend(string hyperliquidChain,string destination,string amount,uint64 time)",
                )
            } else {
                (
                    "withdraw3",
                    "HyperliquidTransaction:Withdraw(string hyperliquidChain,string destination,string amount,uint64 time)",
                )
            };
            (
                json!({"type":kind,"destination":destination,"amount":amount,"time":nonce}),
                definition,
                vec![hash(chain), hash(&destination), hash(&amount), uint(nonce)],
            )
        }
        UserAction::SpotSend {
            destination,
            token,
            amount,
        } => {
            let destination = normalize_address(destination)?;
            let amount = decimal(amount)?;
            (
                json!({"type":"spotSend","destination":destination,"token":token,"amount":amount,"time":nonce}),
                "HyperliquidTransaction:SpotSend(string hyperliquidChain,string destination,string token,string amount,uint64 time)",
                vec![
                    hash(chain),
                    hash(&destination),
                    hash(token),
                    hash(&amount),
                    uint(nonce),
                ],
            )
        }
        UserAction::UsdClassTransfer { amount, to_perp } => {
            let amount = decimal(amount)?;
            (
                json!({"type":"usdClassTransfer","amount":amount,"toPerp":to_perp,"nonce":nonce}),
                "HyperliquidTransaction:UsdClassTransfer(string hyperliquidChain,string amount,bool toPerp,uint64 nonce)",
                vec![
                    hash(chain),
                    hash(&amount),
                    uint(u64::from(*to_perp)),
                    uint(nonce),
                ],
            )
        }
        UserAction::SendAsset {
            destination,
            source_dex,
            destination_dex,
            token,
            amount,
            from_sub_account,
        } => {
            let destination = normalize_address(destination)?;
            let amount = decimal(amount)?;
            let from = from_sub_account
                .as_deref()
                .map(normalize_address)
                .transpose()?
                .unwrap_or_default();
            (
                json!({"type":"sendAsset","destination":destination,"sourceDex":source_dex,"destinationDex":destination_dex,"token":token,"amount":amount,"fromSubAccount":from,"nonce":nonce}),
                "HyperliquidTransaction:SendAsset(string hyperliquidChain,string destination,string sourceDex,string destinationDex,string token,string amount,string fromSubAccount,uint64 nonce)",
                vec![
                    hash(chain),
                    hash(&destination),
                    hash(source_dex),
                    hash(destination_dex),
                    hash(token),
                    hash(&amount),
                    hash(&from),
                    uint(nonce),
                ],
            )
        }
    };
    payload["signatureChainId"] = json!("0x66eee");
    payload["hyperliquidChain"] = json!(chain);
    Ok((payload, definition, fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signing::generate_user_signing_keys;

    fn user_action(value: &Value) -> UserAction {
        let text = |name: &str| value[name].as_str().unwrap().to_owned();
        match value["type"].as_str().unwrap() {
            "usdSend" => UserAction::UsdSend {
                destination: text("destination"),
                amount: text("amount"),
            },
            "spotSend" => UserAction::SpotSend {
                destination: text("destination"),
                token: text("token"),
                amount: text("amount"),
            },
            "withdraw3" => UserAction::Withdraw {
                destination: text("destination"),
                amount: text("amount"),
            },
            "usdClassTransfer" => UserAction::UsdClassTransfer {
                amount: text("amount"),
                to_perp: value["toPerp"].as_bool().unwrap(),
            },
            "sendAsset" => UserAction::SendAsset {
                destination: text("destination"),
                source_dex: text("sourceDex"),
                destination_dex: text("destinationDex"),
                token: text("token"),
                amount: text("amount"),
                from_sub_account: Some(text("fromSubAccount")),
            },
            other => panic!("unexpected fixture action: {other}"),
        }
    }

    // Generated with the official hyperliquid-python-sdk 0.24.0 using action_hash,
    // l1_payload and user_signed_payload + eth_account.messages.encode_typed_data.
    // These fixtures contain only public action payloads and digests, never private keys.
    #[test]
    fn signing_matches_official_sdk_vectors_for_both_schemes() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/hypercore_signing_vectors.json"
        ))
        .unwrap();
        let keys = generate_user_signing_keys();
        let public = k256::ecdsa::SigningKey::from_slice(&keys.ethereum_private_key).unwrap();
        for vector in fixture["vectors"].as_array().unwrap() {
            let action = if vector["scheme"] == "l1" {
                Action::L1(serde_json::from_value(vector["action"].clone()).unwrap())
            } else {
                Action::User(user_action(&vector["action"]))
            };
            let transaction = HypercoreTransaction {
                action,
                network: if vector["network"] == "mainnet" {
                    Network::Mainnet
                } else {
                    Network::Testnet
                },
                nonce: vector["nonce"].as_u64().unwrap(),
                vault_address: vector["vault_address"].as_str().map(str::to_owned),
                expires_after: vector["expires_after"].as_u64(),
            };
            let (action, digest, _) = prepare(&transaction).unwrap();
            assert_eq!(
                hex::encode(digest),
                vector["digest"],
                "SDK digest mismatch for {}",
                vector["action"]["type"]
            );
            assert_eq!(
                serde_json::from_str::<Value>(action.get()).unwrap(),
                vector["action"]
            );
            if let Action::L1(action) = &transaction.action {
                assert_eq!(
                    hex::encode(
                        action_hash(
                            action,
                            transaction.nonce,
                            transaction.vault_address.as_deref(),
                            transaction.expires_after
                        )
                        .unwrap()
                    ),
                    vector["connection_id"]
                );
            }
            let signed = sign_with_keys(&keys, transaction.clone()).unwrap();
            assert_eq!(signed.address, keys.addresses.as_ref().unwrap().ethereum);
            let signature = &signed.payload.signature;
            let mut bytes = hex::decode(&signature.r[2..]).unwrap();
            bytes.extend(hex::decode(&signature.s[2..]).unwrap());
            let recovered = k256::ecdsa::VerifyingKey::recover_from_prehash(
                &hex::decode(vector["digest"].as_str().unwrap()).unwrap(),
                &k256::ecdsa::Signature::from_slice(&bytes).unwrap(),
                k256::ecdsa::RecoveryId::new(signature.v == 28, false),
            )
            .unwrap();
            assert_eq!(&recovered, public.verifying_key());
            let retried = sign_with_keys(&keys, transaction).unwrap();
            assert_eq!(
                serde_json::to_string(&signed.payload).unwrap(),
                serde_json::to_string(&retried.payload).unwrap()
            );
        }
    }

    #[test]
    fn normalizes_wire_values_before_hashing_and_returning_them() {
        let mut action = L1Action::Order {
            orders: vec![Order {
                asset: 4,
                is_buy: true,
                price: "01670.100".into(),
                size: "0.014700".into(),
                reduce_only: false,
                order_type: OrderType::Limit {
                    tif: TimeInForce::Ioc,
                },
                cloid: None,
            }],
            grouping: Grouping::Na,
            builder: None,
        };
        normalize_l1(&mut action).unwrap();
        assert_eq!(
            hex::encode(action_hash(&action, 1677777606040, None, None).unwrap()),
            "0fcbeda5ae3c4950a548021552a4fea2226858c4453571bf3f24ba017eac2908"
        );
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.starts_with(
            "{\"type\":\"order\",\"orders\":[{\"a\":4,\"b\":true,\"p\":\"1670.1\",\"s\":\"0.0147\""
        ));
        assert!(!json.contains("cloid"));
        assert!(!json.contains("builder"));
        assert_eq!(
            normalize_address("0xABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD").unwrap(),
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
    }

    #[test]
    fn rejects_invalid_inputs_and_wrong_signing_scheme() {
        for amount in ["NaN", "-1", "1e5", ".1", "1.2.3", ""] {
            assert!(decimal(amount).is_err());
        }
        for address in ["", "0xabc", "0xgggggggggggggggggggggggggggggggggggggggg"] {
            assert!(normalize_address(address).is_err());
        }
        assert!(normalize_cloid(&mut "0x123".into()).is_err());
        assert!(serde_json::from_value::<L1Action>(json!({"type":"usdSend","destination":"0x1111111111111111111111111111111111111111","amount":"1"})).is_err());
        let mut transaction = HypercoreTransaction {
            action: Action::User(UserAction::UsdSend {
                destination: "0x1111111111111111111111111111111111111111".into(),
                amount: "1".into(),
            }),
            network: Network::Testnet,
            nonce: 1,
            vault_address: None,
            expires_after: Some(2),
        };
        assert!(prepare(&transaction).is_err());
        transaction.expires_after = None;
        transaction.vault_address = Some("0x1111111111111111111111111111111111111111".into());
        assert!(prepare(&transaction).is_err());
    }
}
