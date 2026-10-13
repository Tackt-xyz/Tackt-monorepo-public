//! Source-specific ingestion paths.

pub mod evm;
pub mod hyperliquid;
pub mod price_feed;

/// Runtime ingestion sources owned by the indexer.
///
/// This is intentionally separate from [`tackt_primitives::Source`], which
/// identifies persisted observation and account provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Source {
    Evm,
    Hyperliquid,
    PriceFeed,
}

impl Source {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Evm => "evm",
            Self::Hyperliquid => "hyperliquid",
            Self::PriceFeed => "price_feed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Source;

    #[test]
    fn source_names_are_stable() {
        assert_eq!(Source::Evm.as_str(), "evm");
        assert_eq!(Source::Hyperliquid.as_str(), "hyperliquid");
        assert_eq!(Source::PriceFeed.as_str(), "price_feed");
    }
}
