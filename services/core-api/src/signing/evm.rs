//! EIP-1559 transaction encoding and secp256k1 signing.

use super::{SigningError, addresses_for_keys};
use alloy_rlp::{Encodable, Header};
use ethnum::U256;
use sha3::{Digest, Keccak256};
use tackt_postgres::{PgRepository, UserSigningKeys};
use uuid::Uuid;

/// An EIP-1559 transaction with an empty access list. Values and fees are decimal wei strings.
#[derive(Debug)]
pub struct EthereumTransaction {
    pub chain_id: u64,
    pub nonce: u64,
    pub gas_limit: u64,
    pub max_fee_per_gas: String,
    pub max_priority_fee_per_gas: String,
    /// A 0x-prefixed recipient address; `None` for contract creation.
    pub to: Option<String>,
    pub value: String,
    /// 0x-prefixed calldata; use "0x" for an empty payload.
    pub data: String,
}

fn decimal_u128(value: &str) -> Result<u128, SigningError> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SigningError::InvalidTransaction(
            "fees must be unsigned decimal wei strings",
        ));
    }
    value
        .parse()
        .map_err(|_| SigningError::InvalidTransaction("fee exceeds u128"))
}

#[derive(Debug)]
pub struct SignedTransaction {
    pub address: String,
    pub signed_transaction: String,
    pub transaction_hash: String,
}

/// Signs with this user's stored key and returns the signed payload without broadcasting.
pub async fn sign(
    repo: &PgRepository,
    user_id: Uuid,
    transaction: EthereumTransaction,
) -> Result<SignedTransaction, SigningError> {
    let keys = super::keys::load_user_keys(repo, user_id).await?;
    sign_with_keys(&keys, transaction)
}

fn sign_with_keys(
    keys: &UserSigningKeys,
    input: EthereumTransaction,
) -> Result<SignedTransaction, SigningError> {
    let max_fee_per_gas = decimal_u128(&input.max_fee_per_gas)?;
    let max_priority_fee_per_gas = decimal_u128(&input.max_priority_fee_per_gas)?;
    if input.chain_id == 0 || input.gas_limit == 0 || max_priority_fee_per_gas > max_fee_per_gas {
        return Err(SigningError::InvalidTransaction(
            "chain_id and gas_limit must be positive and priority fee must not exceed max fee",
        ));
    }
    if input.value.is_empty() || !input.value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SigningError::InvalidTransaction(
            "value must be an unsigned decimal wei string",
        ));
    }
    let value = U256::from_str_radix(&input.value, 10)
        .map_err(|_| SigningError::InvalidTransaction("value exceeds uint256"))?;
    let to = match input.to {
        None => Vec::new(),
        Some(value) => {
            if !value.starts_with("0x") || value.len() != 42 {
                return Err(SigningError::InvalidTransaction(
                    "to must be a 0x-prefixed Ethereum address",
                ));
            }
            hex::decode(&value[2..])
                .map_err(|_| SigningError::InvalidTransaction("invalid recipient address"))?
        }
    };
    let data = input
        .data
        .strip_prefix("0x")
        .ok_or(SigningError::InvalidTransaction(
            "data must be 0x-prefixed hex",
        ))?;
    if data.len() > 262_144 {
        return Err(SigningError::InvalidTransaction(
            "transaction data exceeds 128 KiB",
        ));
    }
    let data = hex::decode(data)
        .map_err(|_| SigningError::InvalidTransaction("data must be even-length hexadecimal"))?;
    // EIP-1559 signing fields, in protocol order. This demo uses an empty access list.
    // https://eips.ethereum.org/EIPS/eip-1559#specification
    let mut fields = Vec::new();
    input.chain_id.encode(&mut fields);
    input.nonce.encode(&mut fields);
    max_priority_fee_per_gas.encode(&mut fields);
    max_fee_per_gas.encode(&mut fields);
    input.gas_limit.encode(&mut fields);
    to.as_slice().encode(&mut fields);
    encode_uint(&value.to_be_bytes(), &mut fields);
    data.as_slice().encode(&mut fields);
    Header {
        list: true,
        payload_length: 0,
    }
    .encode(&mut fields);
    let signing_hash = Keccak256::digest(ethereum_envelope(&fields));
    let key = k256::ecdsa::SigningKey::from_slice(&keys.ethereum_private_key)
        .map_err(|_| SigningError::InvalidKey)?;
    let (signature, recovery) = key
        .sign_prehash_recoverable(signing_hash.as_slice())
        .map_err(|_| SigningError::SigningFailed)?;
    if recovery.is_x_reduced() {
        return Err(SigningError::SigningFailed);
    }
    u8::from(recovery.is_y_odd()).encode(&mut fields);
    encode_uint(signature.r().to_bytes().as_slice(), &mut fields);
    encode_uint(signature.s().to_bytes().as_slice(), &mut fields);
    let signed = ethereum_envelope(&fields);
    Ok(SignedTransaction {
        address: addresses_for_keys(keys)?.ethereum,
        signed_transaction: format!("0x{}", hex::encode(&signed)),
        transaction_hash: format!("0x{}", hex::encode(Keccak256::digest(&signed))),
    })
}

fn encode_uint(bytes: &[u8], output: &mut Vec<u8>) {
    let first = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len());
    bytes[first..].encode(output);
}

fn ethereum_envelope(fields: &[u8]) -> Vec<u8> {
    let mut output = vec![0x02];
    Header {
        list: true,
        payload_length: fields.len(),
    }
    .encode(&mut output);
    output.extend_from_slice(fields);
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signing::generate_user_signing_keys;
    use alloy_rlp::Decodable;

    fn ethereum() -> EthereumTransaction {
        EthereumTransaction {
            chain_id: 11155111,
            nonce: 7,
            gas_limit: 50_000,
            max_fee_per_gas: "2000000000".into(),
            max_priority_fee_per_gas: "1000000000".into(),
            to: Some("0x1111111111111111111111111111111111111111".into()),
            value: "1000000000000000".into(),
            data: "0x1234".into(),
        }
    }

    #[test]
    fn ethereum_wire_transaction_recovers_the_selected_user_and_preserves_fields() {
        for keys in [generate_user_signing_keys(), generate_user_signing_keys()] {
            let SignedTransaction {
                address,
                signed_transaction,
                transaction_hash,
            } = sign_with_keys(&keys, ethereum()).unwrap();
            let bytes = hex::decode(&signed_transaction[2..]).unwrap();
            assert_eq!(bytes[0], 2);
            let mut fields = &bytes[1..];
            let header = Header::decode(&mut fields).unwrap();
            assert!(header.list);
            assert_eq!(fields.len(), header.payload_length);
            let unsigned_fields = fields;
            assert_eq!(u64::decode(&mut fields).unwrap(), 11155111);
            assert_eq!(u64::decode(&mut fields).unwrap(), 7);
            assert_eq!(u128::decode(&mut fields).unwrap(), 1_000_000_000);
            assert_eq!(u128::decode(&mut fields).unwrap(), 2_000_000_000);
            assert_eq!(u64::decode(&mut fields).unwrap(), 50_000);
            assert_eq!(
                Header::decode_bytes(&mut fields, false).unwrap(),
                &[0x11; 20]
            );
            assert_eq!(u128::decode(&mut fields).unwrap(), 1_000_000_000_000_000);
            assert_eq!(
                Header::decode_bytes(&mut fields, false).unwrap(),
                &[0x12, 0x34]
            );
            let access_list = Header::decode(&mut fields).unwrap();
            assert!(access_list.list);
            assert_eq!(access_list.payload_length, 0);
            let unsigned_fields = &unsigned_fields[..unsigned_fields.len() - fields.len()];
            let parity = u8::decode(&mut fields).unwrap();
            let r = Header::decode_bytes(&mut fields, false).unwrap();
            let s = Header::decode_bytes(&mut fields, false).unwrap();
            assert!(fields.is_empty());
            let mut signature = [0u8; 64];
            signature[32 - r.len()..32].copy_from_slice(r);
            signature[64 - s.len()..].copy_from_slice(s);
            let recovered = k256::ecdsa::VerifyingKey::recover_from_prehash(
                Keccak256::digest(ethereum_envelope(unsigned_fields)).as_slice(),
                &k256::ecdsa::Signature::from_slice(&signature).unwrap(),
                k256::ecdsa::RecoveryId::new(parity == 1, false),
            )
            .unwrap();
            assert_eq!(
                transaction_hash,
                format!("0x{}", hex::encode(Keccak256::digest(&bytes)))
            );
            let expected = k256::ecdsa::SigningKey::from_slice(&keys.ethereum_private_key).unwrap();
            assert_eq!(&recovered, expected.verifying_key());
            assert_eq!(address, keys.addresses.unwrap().ethereum);
        }
    }

    #[test]
    fn ethereum_rejects_invalid_quantities_fees_and_data() {
        let keys = generate_user_signing_keys();
        let mut invalid = Vec::new();
        let mut tx = ethereum();
        tx.value = "-1".into();
        invalid.push(tx);
        let mut tx = ethereum();
        tx.value = format!("1{}", "0".repeat(78));
        invalid.push(tx);
        let mut tx = ethereum();
        tx.max_fee_per_gas = "1".into();
        invalid.push(tx);
        let mut tx = ethereum();
        tx.data = "0x123".into();
        invalid.push(tx);
        let mut tx = ethereum();
        tx.chain_id = 0;
        invalid.push(tx);
        let mut tx = ethereum();
        tx.to = Some("bad-address".into());
        invalid.push(tx);
        for tx in invalid {
            assert!(matches!(
                sign_with_keys(&keys, tx),
                Err(SigningError::InvalidTransaction(_))
            ));
        }
    }
}
