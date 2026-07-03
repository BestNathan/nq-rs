use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

use crate::gen_channel;

use crate::model::interval::Interval;

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    New,
    Change,
    Delete,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    Snapshot,
    Change,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BookLevel(
    pub Action,
    // price
    pub f64,
    // amount
    pub f64,
);

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BookData {
    pub asks: Vec<BookLevel>,
    pub bids: Vec<BookLevel>,
    pub change_id: i64,
    pub instrument_name: String,
    pub prev_change_id: Option<i64>,
    pub timestamp: u64,
    pub r#type: Type,
}

gen_channel!(BookChannel, "book", String, Interval);

impl std::fmt::Display for BookChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "book.{}.{}", self.0, self.1)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GroupedBookLevel(
    // price
    pub f64,
    // amount
    pub f64,
);

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GroupedBookData {
    pub asks: Vec<GroupedBookLevel>,
    pub bids: Vec<GroupedBookLevel>,
    pub change_id: i64,
    pub instrument_name: String,
    pub timestamp: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(untagged)]
pub enum BookGroup {
    #[serde(rename = "none")]
    None,
    #[serde(untagged)]
    Group(usize),
}

impl FromStr for BookGroup {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "none" => Ok(BookGroup::None),
            val => Ok(BookGroup::Group(usize::from_str(val)?)),
        }
    }
}

impl Display for BookGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BookGroup::None => write!(f, "none"),
            BookGroup::Group(i) => write!(f, "{}", i),
        }
    }
}

gen_channel!(GroupedBookChannel, "book", String, BookGroup, String, Interval);

impl std::fmt::Display for GroupedBookChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "book.{}.{}.{}.{}", self.0, self.1, self.2, self.3)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::model::interval::Interval;

    use super::BookChannel;

    #[test]
    fn test_channel() {
        let book_channel = BookChannel("BTC".to_owned(), Interval::Raw);
        println!("book channel serde_json: {}", serde_json::to_string(&book_channel).unwrap());

        #[derive(Serialize, Deserialize)]
        struct Test {
            channel: BookChannel,
            data: String,
        }

        let test = Test { channel: book_channel, data: String::from("data") };

        println!("test struct: {}", serde_json::to_string(&test).unwrap());
    }
}
