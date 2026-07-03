use std::{fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(from = "String", into = "String")]
pub struct IndexName {
    base: String,
    quote: String,
}

impl IndexName {
    pub fn new(base: String, quote: String) -> Self {
        Self { quote, base }
    }

    pub fn new_upper(base: String, quote: String) -> Self {
        Self { quote: quote.to_uppercase(), base: base.to_uppercase() }
    }

    pub fn new_lower(base: String, quote: String) -> Self {
        Self { quote: quote.to_lowercase(), base: base.to_lowercase() }
    }

    pub fn to_uppper(&self) -> Self {
        Self::new_upper(self.base.clone(), self.quote.clone())
    }

    pub fn to_lower(&self) -> Self {
        Self::new_lower(self.base.clone(), self.quote.clone())
    }
}

impl Display for IndexName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.quote.is_empty() || self.base.is_empty() {
            write!(f, "")
        } else {
            write!(f, "{}_{}", self.base, self.quote)
        }
    }
}

impl FromStr for IndexName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(IndexName::from(s.to_string()))
    }
}

impl From<String> for IndexName {
    fn from(value: String) -> Self {
        let segments: Vec<_> = value.split("_").collect();
        match segments.as_slice() {
            [from, to] => IndexName::new(from.to_string(), to.to_string()),
            _ => IndexName::default(),
        }
    }
}

impl From<IndexName> for String {
    fn from(value: IndexName) -> Self {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::IndexName;

    #[test]
    fn test() {
        let r#in: IndexName = IndexName::new("btc".to_string(), "usdc".to_string());
        let jstr = serde_json::to_string(&r#in).unwrap();

        println!("json str: {}", jstr);

        let newin: IndexName = serde_json::from_str(&jstr).unwrap();
        println!("index name: {:?}", newin);
    }
}
