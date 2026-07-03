use serde::{Deserialize, Serialize};

use crate::{gen_channel, model::index::IndexName};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeribitPriceIndexData {
    pub index_name: String,
    pub price: f64,
    pub timestamp: u64,
}

gen_channel!(DeribitPriceIndexChannel, "deribit_price_index", IndexName);

impl std::fmt::Display for DeribitPriceIndexChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deribit_price_index.{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::subscription::deribit_price_index::DeribitPriceIndexChannel;

    #[test]
    fn test() {
        #[derive(Deserialize, Serialize, Debug)]
        struct Test {
            channel: DeribitPriceIndexChannel,
        }

        let jsonstr = r#"{"channel":"deribit_price_index.btc_usdc"}"#;

        let t: Test = serde_json::from_str(jsonstr).unwrap();
        println!("test: {:?}", t);

        let newjsonstr = serde_json::to_string(&t).unwrap();

        assert_eq!(jsonstr, newjsonstr, "should eq");
    }
}
