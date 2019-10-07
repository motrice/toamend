extern crate serde;
extern crate chrono;

use self::chrono::{DateTime, Utc};
use serde_json::Value;

#[derive(Serialize, Deserialize, Clone)]
pub struct Revision<'a> {
    pub id: &'a str,
    pub version: &'a str
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Action<'a> {
    CREATE{category:&'a str, content_type:&'a str, bucket:&'a str, env:&'a str},
    UPDATE(Revision<'a>),
    DELETE(Revision<'a>),     // delete this asset
    COPY(Revision<'a>),        // special create, init with value from another asset
    SEAL(Revision<'a>)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Sys<'a> {
    /// Unique ID of resource
    pub id: &'a str,  
    /// type of resource
    pub category: &'a str, 
    pub content_type: &'a str,
    pub bucket: &'a str,
    pub env: &'a str,
    pub published_count: u32,
    pub published_version: Option<&'a str>,
    pub version: &'a str,
    pub previous_version: Option<&'a str>,
    pub first_published_at: Option<&'a str>,
    pub published_at: Option<&'a str>,
    pub published_by: Option<&'a str>,
    pub sealed_at: Option<&'a str>,
    pub sealed_by: Option<&'a str>,
    pub created_at: Option<&'a str>,
    pub created_by: &'a str,
    pub updated_at: Option<&'a str>,
    pub updated_by: &'a str,
    pub payload_checksum: Option<&'a str>
}

impl<'a> Sys<'a> {
    pub fn first_published_at(&self) -> Option<DateTime<Utc>> {
        match self.first_published_at {
            Some(date_str) => match date_str.parse() {
                Ok(datetime) => Some(datetime),
                Err(parse_error) => {
                    println!("Parse error id={} version={} first_published_at={} error={}", self.id, self.version, date_str, parse_error);
                    None
                }
            },
            None => None
        }
    }
    pub fn published_at(&self) -> Option<DateTime<Utc>> {
        match self.published_at {
            Some(date_str) => match date_str.parse() {
                Ok(datetime) => Some(datetime),
                Err(parse_error) => {
                    println!("Parse error id={} version={} published_at={} error={}", self.id, self.version, date_str, parse_error);
                    None
                }
            },
            None => None
        }
    }
    pub fn created_at(&self) -> Option<DateTime<Utc>> {
        match self.created_at {
            Some(date_str) => match date_str.parse() {
                Ok(datetime) => Some(datetime),
                Err(parse_error) => {
                    println!("Parse error id={} version={} created_at={} error={}", self.id, self.version, date_str, parse_error);
                    None
                }
            },
            None => None
        }
    }
    pub fn updated_at(&self) -> Option<DateTime<Utc>> {
        match self.updated_at {
            Some(date_str) => match date_str.parse() {
                Ok(datetime) => Some(datetime),
                Err(parse_error) => {
                    println!("Parse error id={} version={} updated_at={} error={}", self.id, self.version, date_str, parse_error);
                    None
                }
            },
            None => None
        }
    }

}


/*
sys.type 	String 	Type of resource. 	All
sys.id 	String 	Unique ID of resource. 	All except arrays
sys.space 	Link 	Link to resource's space. 	Entries, assets, content types
sys.environment 	Link 	Link to resource's environment. 	Entries, assets, content types
sys.contentType 	Link 	Link to entry's content type. 	Entries
sys.publishedCounter 	Integer 	Number of times resource was published. 	Entries, assets, content types
sys.publishedVersion 	Integer 	Published version of resource. 	Entries, assets, content types
sys.version 	Integer 	Current version of resource. 	Entries, assets, content types
sys.firstPublishedAt 	Date 	Time resource was first published. 	Entries, assets, content types
sys.createdAt 	Date 	Time resource was created. 	Entries, assets, content types
sys.createdBy 	Link 	Link to creating user. 	Entries, assets, content types
sys.publishedAt 	Date 	Time resource was published. 	Entries, assets, content types
sys.publishedBy 	Link 	Link to publishing user. 	Entries, assets, content types
sys.updatedAt 	Date 	Time resource was updated. 	Entries, assets, content types
sys.updatedBy 	Link 	Link to updating user. 	Entries, assets, content types


id": "5KsDBWseXY6QegucYAoacS",
    "type": "Entry",
    "contentType": {
        */


#[derive(Serialize, Deserialize, Clone)]
pub struct LedgerCommand<'a, T: 'a> {
    pub tracking_id: &'a str,
    pub action: Action<'a>,
    pub payload: Option<T>,
    pub user_id: Option<&'a str>
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LedgerEvent<'a, T: 'a> {
    pub event_id: &'a str,
    pub action: Action<'a>,
    pub payload: Option<T>,
    pub sys: Sys<'a>
}

#[derive(Serialize, Deserialize, Clone)]
pub enum SubscriptionEvent<'a> {
  Open{conn_id: &'a str},
  Subscribe{conn_id: &'a str, topic:&'a str},
  Unsubscribe{conn_id: &'a str, topic: &'a str},
  Close{conn_id: &'a str}
}

impl<'a> std::fmt::Display for LedgerCommand<'a, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match serde_json::to_string(&self) {
            Ok(json_str) => write!(f, "Command(tracking_id: {} json_data: {})", self.tracking_id, json_str),
            Err(err) => write!(f, "Command(tracking_id: {} err: {})", self.tracking_id, err)
        }
        
    }
}

impl<'a> std::fmt::Display for LedgerEvent<'a, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match serde_json::to_string(&self) {
            Ok(json_str) => write!(f, "Event(event_id: {} json_data: {})", self.event_id, json_str),
            Err(err) => write!(f, "Event(event_id: {} err: {})", self.event_id, err)
        }
        
    }
}

impl<'a> std::fmt::Display for SubscriptionEvent<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match serde_json::to_string(&self) {
            Ok(json_str) => write!(f, "SubscriptionEvent(json_data: {})", json_str),
            Err(err) => write!(f, "SubscriptionEvent(err: {})", err)
        }
        
    }
}