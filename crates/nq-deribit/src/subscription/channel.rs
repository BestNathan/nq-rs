use std::fmt::Display;

pub trait Channel: Display {
    fn to_channel_str(&self) -> String {
        self.to_string()
    }
}

pub struct Channels<T: Channel>(Vec<T>);

impl<T> From<Vec<T>> for Channels<T>
where
    T: Channel,
{
    fn from(value: Vec<T>) -> Self {
        Channels(value)
    }
}

impl<T> Channels<T>
where
    T: Channel,
{
    pub fn to_subscription_strs(&self) -> Vec<String> {
        self.0.iter().map(|c| c.to_string()).collect()
    }
}

#[macro_export]
macro_rules! gen_channel_base {
    ($struct_name:ident, $($field_type:ty),*) => {
        #[derive(Debug, Clone)]
        pub struct $struct_name(
            $(pub $field_type),*
        );

        impl $crate::subscription::channel::Channel for $struct_name {}

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

#[macro_export]
macro_rules! gen_channel {
    ($struct_name:ident, $($prefix:literal),+, $($field_type:ty),*) => {
        $crate::gen_channel_base!($struct_name, $($field_type),*);

        impl<'de> serde::Deserialize<'de> for $struct_name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = <&str as serde::Deserialize<'de>>::deserialize(deserializer)?;
                let p = vec![$($prefix),+].join(".");
                let p = p.as_str();

                if !s.starts_with(p) {
                    return Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Str(s),
                        &format!("should prefixed with: {}", p).as_str(),
                    ))
                }

                let p = s.replace(format!("{}.", p).as_str(), "");
                let segments: Vec<_> = p.split(".").collect();

                let mut _values_iter = segments.iter();
                Ok($struct_name(
                    $(
                        _values_iter.next()
                            .ok_or_else(|| serde::de::Error::custom("insufficient fields"))?
                            .parse::<$field_type>()
                            .map_err(|_| serde::de::Error::invalid_type(
                                serde::de::Unexpected::Str(_values_iter.next().unwrap()),
                                &"valid field type"
                            ))?
                    ),*
                ))
            }
        }
    };
    ($struct_name:ident, $($prefix:literal),+) => {
        $crate::gen_channel_base!($struct_name,);

        impl<'de> serde::Deserialize<'de> for $struct_name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let s = <&str as serde::Deserialize<'de>>::deserialize(deserializer)?;
                let p = vec![$($prefix),+].join(".");
                let p = p.as_str();

                if !s.starts_with(p) {
                    return Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Str(s),
                        &format!("should prefixed with: {}", p).as_str(),
                    ))
                }

                Ok($struct_name())
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use crate::model::currency::Currency;

    #[test]
    fn test_gen_channel() {
        gen_channel!(Test, "a", "b", String, Currency);

        impl Display for Test {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "a.b.{}.{}", self.0, self.1)
            }
        }

        let t = Test("1".to_string(), Currency::BTC);

        let channel_str = serde_json::to_string(&t).unwrap();
        println!("channel: {}", channel_str);

        let t1 = serde_json::from_str::<Test>(&channel_str).unwrap();
        println!("t1: {:?}", t1);
    }
}
