use std::{fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Copy, Default)]
pub enum Interval {
    #[serde(rename = "raw")]
    Raw,
    #[serde(rename = "100ms")]
    #[default]
    Ms100,
    #[serde(rename = "agg2")]
    Agg2,
}

impl Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(*self))
    }
}

impl From<String> for Interval {
    fn from(s: String) -> Self {
        match s.as_str() {
            "raw" => Interval::Raw,
            "100ms" => Interval::Ms100,
            "agg2" => Interval::Agg2,
            _ => Interval::default(),
        }
    }
}

impl FromStr for Interval {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Interval::from(s.to_string()))
    }
}

impl From<Interval> for String {
    fn from(value: Interval) -> Self {
        match value {
            Interval::Raw => "raw".to_string(),
            Interval::Ms100 => "100ms".to_string(),
            Interval::Agg2 => "agg2".to_string(),
        }
    }
}
