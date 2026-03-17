// ---------------------------------------------------------------------------
// Event bus — in-process broadcast for cross-connection events.
//
// Uses tokio::broadcast so any number of tasks can subscribe. Events are
// fire-and-forget — if a receiver is slow, it misses events (lagged).
// This is fine because events are supplementary (presence, notifications),
// not critical data delivery (that goes through direct WebSocket sends).
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

const BUS_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_type: EventType,
    pub project_path: String,
    pub mr_iid: Option<u64>,
    pub user_id: Option<String>,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    ReviewStarted,
    ReviewComplete,
    CommentAction,
    FixStarted,
    FixProgress,
    FixComplete,
    MrUpdated,
    UserJoinedMr,
    UserLeftMr,
    /// A conflict was detected or resolved between in-flight MRs.
    ConflictUpdated,
    /// A cluster was created, updated, or dissolved.
    ClusterUpdated,
}

#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<Event>,
}

impl EventBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BUS_CAPACITY);
        Self { tx }
    }

    /// Publish an event. Returns the number of receivers that got it.
    /// Never fails — if no one is listening, the event is dropped.
    pub fn publish(&self, event: Event) -> usize {
        self.tx.send(event).unwrap_or(0)
    }

    /// Subscribe to all events. Returns a receiver stream.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }
}
