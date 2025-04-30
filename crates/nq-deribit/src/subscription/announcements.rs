use serde::{Deserialize, Serialize};

use crate::gen_channel;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AnnouncementsData {
    pub action: String,
    pub title: String,
    pub body: String,
    pub date: u64,
    pub id: u64,
    pub important: bool,
    pub number: u64,
}

gen_channel!(AnnouncementsChannel, "announcements");

impl std::fmt::Display for AnnouncementsChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "announcements")
    }
}

#[cfg(test)]
mod tests {
    use super::AnnouncementsChannel;

    #[test]
    fn test() {
        let a: AnnouncementsChannel = AnnouncementsChannel();
        let json_str = serde_json::to_string(&a).unwrap();
        println!("to json: {}", json_str);

        let a: AnnouncementsChannel = serde_json::from_str(&json_str).unwrap();
        println!("to AnnouncementsChannel: {:?}", a);
    }
}
