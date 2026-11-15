//! Where events come from: the node's `GET /v1/events` (contracts/node-events.md).
//! `EventSource` is the interface, `HttpEventSource` the real client and
//! `MemoryEventNode` a fake that behaves like the contract, for tests.

use std::future::Future;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::NodeError;

/// One event, in the envelope every node uses.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Event {
    pub seq: u64,
    /// RFC 3339 UTC, by the clock of the node that wrote it.
    pub time: String,
    pub node_id: String,
    pub kind: String,
    pub data: serde_json::Value,
}

/// A response of `GET /v1/events`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventPage {
    pub events: Vec<Event>,
    pub next: u64,
}

#[derive(Debug)]
pub enum FetchError {
    /// HTTP 410: `after` is older than the oldest event the node still keeps.
    CursorExpired { oldest_seq: u64 },
    /// Anything else: connection refused, timeout, a 5xx, a body we cannot read.
    Node(NodeError),
}

impl From<NodeError> for FetchError {
    fn from(e: NodeError) -> Self {
        Self::Node(e)
    }
}

pub trait EventSource: Send {
    /// Events with `seq > after`, at most `limit`. With `wait_ms > 0` the node may hold the
    /// request open until something new happens (long poll).
    fn fetch(
        &mut self,
        after: u64,
        limit: u32,
        wait_ms: u32,
    ) -> impl Future<Output = Result<EventPage, FetchError>> + Send;
}

// ------------------------------------------------------------------------------ real client

pub struct HttpEventSource {
    client: reqwest::Client,
    url: String,
}

impl HttpEventSource {
    pub fn new(base_url: &str) -> Result<Self, NodeError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            client,
            url: format!("{}/v1/events", base_url.trim_end_matches('/')),
        })
    }
}

impl EventSource for HttpEventSource {
    async fn fetch(
        &mut self,
        after: u64,
        limit: u32,
        wait_ms: u32,
    ) -> Result<EventPage, FetchError> {
        let resp = self
            .client
            .get(&self.url)
            .query(&[
                ("after", after),
                ("limit", u64::from(limit)),
                ("wait_ms", u64::from(wait_ms)),
            ])
            // the node may legitimately hold the request for wait_ms
            .timeout(Duration::from_millis(u64::from(wait_ms) + 15_000))
            .send()
            .await
            .map_err(NodeError::from)?;
        let status = resp.status();
        if status.as_u16() == 410 {
            let body: serde_json::Value = resp.json().await.map_err(NodeError::from)?;
            let oldest_seq = body["error"]["details"]["oldest_seq"]
                .as_u64()
                .ok_or_else(|| {
                    NodeError::Other(format!("HTTP 410 without error.details.oldest_seq: {body}"))
                })?;
            return Err(FetchError::CursorExpired { oldest_seq });
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(NodeError::Status {
                path: self.url.clone(),
                status: status.as_u16(),
                body,
            }
            .into());
        }
        Ok(resp.json::<EventPage>().await.map_err(NodeError::from)?)
    }
}

// ------------------------------------------------------------------------------ fake

/// A node's event log in memory, with the semantics of node-events.md: `seq` is the position
/// (1, 2, 3, ...), `after` and `limit` select a window, `next` is the last seq returned (or
/// `after` when empty), and an `after` older than the retained part gives `CursorExpired`.
/// It does not really wait for `wait_ms`: an empty answer comes back at once, as if the wait
/// had run out.
///
/// For tests: `set_visible` makes the log grow, `with_retention` drops old events from view,
/// `fail_next(n)` makes the next n requests fail like a dropped connection.
pub struct MemoryEventNode {
    events: Vec<Event>,
    visible: usize,
    retention: Option<usize>,
    fail_next: u32,
}

// Parts of this are only used by tests.
#[allow(dead_code)]
impl MemoryEventNode {
    pub fn new(events: Vec<Event>) -> Self {
        let visible = events.len();
        Self {
            events,
            visible,
            retention: None,
            fail_next: 0,
        }
    }

    pub fn set_visible(&mut self, n: usize) {
        self.visible = n.min(self.events.len());
    }

    pub fn with_retention(mut self, keep_last: usize) -> Self {
        self.retention = Some(keep_last);
        self
    }

    pub fn fail_next(&mut self, n: u32) {
        self.fail_next = n;
    }

    /// The answer for a request, without the injected failures.
    pub fn window(&self, after: u64, limit: u32) -> Result<EventPage, FetchError> {
        let limit = limit.clamp(1, 1000) as usize; // the contract's maximum is 1000
        let oldest = match self.retention {
            Some(keep) => self.visible.saturating_sub(keep) as u64 + 1,
            None => 1,
        };
        if after + 1 < oldest {
            return Err(FetchError::CursorExpired { oldest_seq: oldest });
        }
        let start = usize::try_from(after)
            .unwrap_or(usize::MAX)
            .min(self.visible);
        let end = (start + limit).min(self.visible);
        let events = self.events[start..end].to_vec();
        let next = events.last().map_or(after, |e| e.seq);
        Ok(EventPage { events, next })
    }
}

impl EventSource for MemoryEventNode {
    async fn fetch(
        &mut self,
        after: u64,
        limit: u32,
        _wait_ms: u32,
    ) -> Result<EventPage, FetchError> {
        if self.fail_next > 0 {
            self.fail_next -= 1;
            return Err(
                NodeError::Other("connection reset by peer (injected by the test)".into()).into(),
            );
        }
        self.window(after, limit)
    }
}
