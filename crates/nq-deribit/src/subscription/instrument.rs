use serde::{Deserialize, Serialize};
use crate::{
    model::{
        currency::Currency,
        instrument::{InstrumentKind, InstrumentState},
    },
    gen_channel,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct InstrumentStateData {
    pub timestamp: u64,
    pub state: InstrumentState,
    pub instrument_name: String,
}

gen_channel!(InstrumentStateChannel, "instrument_state", InstrumentKind, Currency);

impl std::fmt::Display for InstrumentStateChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "instrument_state.{}.{}", self.0, self.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_display() {
        let ch = InstrumentStateChannel(InstrumentKind::Option, Currency::BTC);
        assert_eq!(ch.to_string(), "instrument_state.option.BTC");
    }

    #[test]
    fn test_channel_deserialize() {
        let ch: InstrumentStateChannel = serde_json::from_str("\"instrument_state.option.ETH\"").unwrap();
        assert_eq!(ch.0, InstrumentKind::Option);
        assert_eq!(ch.1, Currency::ETH);
    }
}
