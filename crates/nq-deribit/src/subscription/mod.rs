pub mod announcements;
pub mod book;
pub mod deribit_price_index;
pub mod deribit_price_ranking;
pub mod deribit_volatility_index;
pub mod estimated_expiration_price;
pub mod instrument;
pub mod markprice;
pub mod perpetual;
pub mod quote;
pub mod ticker;
pub mod trades;
pub mod user_changes;
pub mod user_orders;
pub mod user_portfolio;
pub mod user_trades;
pub mod deribit_price_statistics;

pub mod channel;

#[macro_export]
macro_rules! implements_for_channel {
    ($struct_name:ident) => {
        impl serde::Serialize for $struct_name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }
    };
}

#[cfg(test)]
mod tests {
    // use crate::subscription::channel::SubscriptionChannel;
    use std::fmt::Display;

    #[test]
    fn test() {
        struct Test {}

        impl Display for Test {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "this is test")
            }
        }

        implements_for_channel!(Test);

        let test = Test {};
        println!("{}", test);
    }
}
