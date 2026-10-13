use serde::{Deserialize, Serialize};

/// Field declaration order is the HyperCore MessagePack wire order. Do not sort these maps.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum L1Action {
    Order {
        orders: Vec<Order>,
        grouping: Grouping,
        #[serde(skip_serializing_if = "Option::is_none")]
        builder: Option<Builder>,
    },
    Cancel {
        cancels: Vec<Cancel>,
    },
    CancelByCloid {
        cancels: Vec<CancelByCloid>,
    },
    BatchModify {
        modifies: Vec<Modify>,
    },
    UpdateLeverage {
        asset: u32,
        is_cross: bool,
        leverage: u32,
    },
    UpdateIsolatedMargin {
        asset: u32,
        is_buy: bool,
        ntli: i64,
    },
    ScheduleCancel {
        #[serde(skip_serializing_if = "Option::is_none")]
        time: Option<u64>,
    },
    CreateSubAccount {
        name: String,
    },
    SubAccountTransfer {
        sub_account_user: String,
        is_deposit: bool,
        usd: u64,
    },
    VaultTransfer {
        vault_address: String,
        is_deposit: bool,
        usd: u64,
    },
    Noop,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Order {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "b")]
    pub is_buy: bool,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "s")]
    pub size: String,
    #[serde(rename = "r")]
    pub reduce_only: bool,
    #[serde(rename = "t")]
    pub order_type: OrderType,
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    pub cloid: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OrderType {
    Limit {
        tif: TimeInForce,
    },
    Trigger {
        is_market: bool,
        trigger_px: String,
        tpsl: TriggerKind,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum TimeInForce {
    Alo,
    Ioc,
    Gtc,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TriggerKind {
    Tp,
    Sl,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Grouping {
    Na,
    NormalTpsl,
    PositionTpsl,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Builder {
    #[serde(rename = "b")]
    pub address: String,
    /// Fee in tenths of a basis point.
    #[serde(rename = "f")]
    pub fee: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cancel {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "o")]
    pub order_id: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CancelByCloid {
    pub asset: u32,
    pub cloid: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Modify {
    pub oid: u64,
    pub order: Order,
}

/// HyperCore's user-signed transfer actions use a different EIP-712 domain from L1 actions.
/// Time/nonce and environment fields are supplied by HypercoreTransaction, never twice.
#[derive(Clone, Debug)]
pub enum UserAction {
    UsdSend {
        destination: String,
        amount: String,
    },
    SpotSend {
        destination: String,
        token: String,
        amount: String,
    },
    Withdraw {
        destination: String,
        amount: String,
    },
    UsdClassTransfer {
        amount: String,
        to_perp: bool,
    },
    SendAsset {
        destination: String,
        source_dex: String,
        destination_dex: String,
        token: String,
        amount: String,
        from_sub_account: Option<String>,
    },
}
