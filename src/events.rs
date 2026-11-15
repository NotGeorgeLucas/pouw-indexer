//! Store the events a node tells us about (contracts/node-events.md).
//!
//! Rules this module keeps:
//!  * A page of events, the typed rows made from them and the new cursor are written in ONE
//!    transaction. A crash leaves either all of it or none, so a restart neither repeats an
//!    event nor skips one.
//!  * Every event is also kept as received in `node_events`, keyed by (node_id, seq). Replaying a
//!    page is harmless, and an event we do not understand is not lost.
//!  * The node promises seq 1, 2, 3 with no gaps. If a page breaks that, we stop with an
//!    error instead of storing something wrong.
//!  * A node that dropped old events (HTTP 410) costs us observations, never blocks: the lost
//!    range is written to `event_gaps` and we continue from the oldest event it still has.

use std::sync::Arc;
use std::time::{Duration, Instant};

use pouw_api::event::NodeId;
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::error::{BadEvent, EventError, StoreError};
use crate::event_source::{Event, EventSource, FetchError};
use crate::metrics::Metrics;
use crate::store::{self, Parsed, Store};

pub struct EventConfig {
    /// The node whose events these are. Events that name another node are refused.
    pub node: NodeId,
    /// Events asked per request (the node's maximum is 1000).
    pub page: u32,
    /// How long a long-poll request may be held by the node.
    pub long_poll_ms: u32,
    /// Pause between requests when there was nothing new, and in polling mode.
    pub poll_interval: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    /// Consecutive failures before we stop long-polling and poll with short requests.
    pub fallback_after: u32,
    /// Consecutive good polls before we try long-polling again.
    pub recover_after: u32,
}

impl EventConfig {
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            page: 1000,
            long_poll_ms: 20_000,
            poll_interval: Duration::from_secs(2),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(30),
            fallback_after: 3,
            recover_after: 10,
        }
    }
}

impl From<serde_json::Error> for BadEvent {
    fn from(e: serde_json::Error) -> Self {
        Self(e.to_string())
    }
}

// ------------------------------------------------------------------------------ parsing

fn hex_id(s: &str, prefix: &str) -> Result<[u8; 32], BadEvent> {
    let h = s
        .strip_prefix(prefix)
        .ok_or_else(|| BadEvent(format!("{s:?} does not start with {prefix:?}")))?;
    let v = hex::decode(h).map_err(|_| BadEvent(format!("{s:?} is not hex")))?;
    v.try_into()
        .map_err(|_| BadEvent(format!("{s:?} is not 32 bytes")))
}

/// A field element: "0x" and 62 hex digits (31 bytes).
fn field_element(s: &str) -> Result<[u8; 31], BadEvent> {
    let h = s
        .strip_prefix("0x")
        .ok_or_else(|| BadEvent(format!("{s:?} does not start with 0x")))?;
    let v = hex::decode(h).map_err(|_| BadEvent(format!("{s:?} is not hex")))?;
    v.try_into()
        .map_err(|_| BadEvent(format!("{s:?} is not 31 bytes")))
}

fn int(v: u64) -> Result<i64, BadEvent> {
    i64::try_from(v).map_err(|_| BadEvent(format!("{v} does not fit in a Postgres BIGINT")))
}

#[derive(Deserialize)]
struct TxReceived {
    txid: String,
    tx_kind: String,
    sender: String,
    fee: u64,
}
#[derive(Deserialize)]
struct TxRejected {
    txid: Option<String>,
    reason_code: String,
    message: String,
}
#[derive(Deserialize)]
struct ProofProduced {
    txid: String,
    circuit: String,
    system: String,
    prove_ms: u64,
    proof_hash: String,
}
#[derive(Deserialize)]
struct LotteryResult {
    txid: String,
    proof_hash: String,
    circuit: String,
    won: bool,
    eta: String,
    mask: String,
    usage_counter: u64,
}
#[derive(Deserialize)]
struct BlockReceived {
    #[serde(alias = "id")]
    hash: String,
    height: u64,
    from_peer: String,
}
#[derive(Deserialize)]
struct BlockVerified {
    #[serde(alias = "id")]
    hash: String,
    height: u64,
    verify_ms: u64,
    proof_count: u64,
    ok: bool,
}
#[derive(Deserialize)]
struct BlockAccepted {
    #[serde(alias = "id")]
    hash: String,
    height: u64,
}
#[derive(Deserialize)]
struct BlockRejected {
    #[serde(alias = "id")]
    hash: String,
    height: Option<u64>,
    reason_code: String,
}
#[derive(Deserialize)]
struct BlockOrphaned {
    #[serde(alias = "id")]
    hash: String,
    height: u64,
}
#[derive(Deserialize)]
struct MempoolSnapshot {
    pending_txs: u64,
    pending_proof_requests: u64,
    by_circuit: serde_json::Value,
}

fn parse(ev: &Event) -> Result<Parsed, BadEvent> {
    let d = ev.data.clone();
    Ok(match ev.kind.as_str() {
        "tx_received" => {
            let t: TxReceived = serde_json::from_value(d)?;
            if !matches!(
                t.tx_kind.as_str(),
                "transfer" | "proof_request" | "register_circuit"
            ) {
                return Err(BadEvent(format!("unknown tx_kind {:?}", t.tx_kind)));
            }
            Parsed::TxReceived {
                txid: hex_id(&t.txid, "tx:")?,
                kind: t.tx_kind,
                sender: hex_id(&t.sender, "addr:")?,
                fee: int(t.fee)?,
            }
        }
        "tx_rejected" => {
            let t: TxRejected = serde_json::from_value(d)?;
            let txid = t.txid.as_deref().map(|s| hex_id(s, "tx:")).transpose()?;
            Parsed::TxRejected {
                txid,
                reason: t.reason_code,
                message: t.message,
            }
        }
        "proof_produced" => {
            let t: ProofProduced = serde_json::from_value(d)?;
            Parsed::ProofProduced {
                txid: hex_id(&t.txid, "tx:")?,
                txid_text: t.txid,
                circuit: hex_id(&t.circuit, "circ:")?,
                system: t.system,
                prove_ms: int(t.prove_ms)?,
                proof_hash: hex_id(&t.proof_hash, "proof:")?,
            }
        }
        "lottery_result" => {
            let t: LotteryResult = serde_json::from_value(d)?;
            Parsed::Lottery {
                txid: hex_id(&t.txid, "tx:")?,
                proof_hash: hex_id(&t.proof_hash, "proof:")?,
                circuit: hex_id(&t.circuit, "circ:")?,
                won: t.won,
                eta: field_element(&t.eta)?,
                mask: field_element(&t.mask)?,
                counter: int(t.usage_counter)?,
            }
        }
        "block_received" => {
            let t: BlockReceived = serde_json::from_value(d)?;
            Parsed::BlockReceived {
                hash: hex_id(&t.hash, "blk:")?,
                height: int(t.height)?,
                from_peer: t.from_peer,
            }
        }
        "block_verified" => {
            let t: BlockVerified = serde_json::from_value(d)?;
            Parsed::BlockVerified {
                hash: hex_id(&t.hash, "blk:")?,
                height: int(t.height)?,
                verify_ms: int(t.verify_ms)?,
                proofs: int(t.proof_count)?,
                ok: t.ok,
            }
        }
        "block_accepted" => {
            let t: BlockAccepted = serde_json::from_value(d)?;
            Parsed::BlockAccepted {
                hash: hex_id(&t.hash, "blk:")?,
                height: int(t.height)?,
            }
        }
        "block_rejected" => {
            let t: BlockRejected = serde_json::from_value(d)?;
            Parsed::BlockRejected {
                hash: hex_id(&t.hash, "blk:")?,
                height: t.height.map(int).transpose()?,
                reason: t.reason_code,
            }
        }
        "block_orphaned" => {
            let t: BlockOrphaned = serde_json::from_value(d)?;
            Parsed::BlockOrphaned {
                hash: hex_id(&t.hash, "blk:")?,
                height: int(t.height)?,
            }
        }
        "mempool_snapshot" => {
            let t: MempoolSnapshot = serde_json::from_value(d)?;
            Parsed::Snapshot {
                pending_txs: int(t.pending_txs)?,
                pending_requests: int(t.pending_proof_requests)?,
                by_circuit: t.by_circuit,
            }
        }
        _ => Parsed::RawOnly,
    })
}

// ------------------------------------------------------------------------------ one page

#[derive(Debug, PartialEq, Eq)]
pub struct PageStats {
    pub stored: usize,
    /// Events of a known kind whose content did not make sense. Kept raw, no typed row.
    pub bad: usize,
}

/// Store a page that must continue right after `after`: seq after+1, after+2, ...
/// Safe to call again with the same events.
pub async fn ingest_page(
    store: &Store,
    node: &NodeId,
    after: u64,
    events: &[Event],
) -> Result<PageStats, EventError> {
    for (i, e) in events.iter().enumerate() {
        let want = after + 1 + i as u64;
        if e.seq != want {
            return Err(EventError::Contract(format!(
                "{node}: expected event {want} but got {}",
                e.seq
            )));
        }
        if e.node_id != node.as_str() {
            return Err(EventError::Contract(format!(
                "{node}: got an event of node {:?}, is the source address right?",
                e.node_id
            )));
        }
        if e.time.len() < 20 || !e.time.ends_with('Z') || !e.time.contains('T') {
            return Err(EventError::Contract(format!(
                "{node}: event {} has a time that is not RFC 3339 UTC: {:?}",
                e.seq, e.time
            )));
        }
    }
    let Some(last) = events.last() else {
        return Ok(PageStats { stored: 0, bad: 0 });
    };

    let mut tx = store.begin().await?;
    store::insert_events(&mut tx, node, events).await?;
    let mut bad = 0;
    for e in events {
        match parse(e) {
            Ok(p) => store::apply_event(&mut tx, node, e, &p).await?,
            Err(err) => {
                bad += 1;
                warn!(
                    node = node.as_str(),
                    seq = e.seq,
                    kind = e.kind,
                    "kept raw, not understood: {err}"
                );
            }
        }
    }
    store::set_event_cursor(&mut tx, node, last.seq).await?;
    tx.commit().await.map_err(StoreError::from)?;
    Ok(PageStats {
        stored: events.len(),
        bad,
    })
}

// ------------------------------------------------------------------------------ one request

#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// A page was stored.
    Stored(usize),
    /// Nothing new.
    Empty,
    /// The node had dropped events from..=to before we asked. Recorded, cursor moved on.
    Gap { from: u64, to: u64 },
}

/// Ask once (after the cursor) and store what comes back.
pub async fn ingest_step<S: EventSource>(
    store: &Store,
    src: &mut S,
    cfg: &EventConfig,
    m: &Metrics,
    wait_ms: u32,
) -> Result<Step, EventError> {
    let gauge = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
    let cursor = store.event_cursor(&cfg.node).await?;
    m.event_cursor
        .with_label_values(&[cfg.node.as_str()])
        .set(gauge(cursor));
    match src.fetch(cursor, cfg.page, wait_ms).await {
        Ok(page) if page.events.is_empty() => Ok(Step::Empty),
        Ok(page) => {
            let stats = ingest_page(store, &cfg.node, cursor, &page.events).await?;
            m.events_ingested.inc_by(stats.stored as u64);
            m.events_unparsed.inc_by(stats.bad as u64);
            m.event_cursor
                .with_label_values(&[cfg.node.as_str()])
                .set(gauge(cursor + stats.stored as u64));
            Ok(Step::Stored(stats.stored))
        }
        Err(FetchError::CursorExpired { oldest_seq }) => {
            // The spec: blocks cover what blocks can say; the observations in the dropped
            // window are lost. We write the loss down and continue from the oldest event.
            let from = cursor + 1;
            let to = oldest_seq.saturating_sub(1);
            if to < from {
                return Err(EventError::Contract(format!(
                    "the node says cursor {cursor} expired but its oldest event is {oldest_seq}"
                )));
            }
            let mut tx = store.begin().await?;
            store::record_event_gap(&mut tx, &cfg.node, from, to).await?;
            store::set_event_cursor(&mut tx, &cfg.node, to).await?;
            tx.commit().await.map_err(StoreError::from)?;
            m.event_gaps.inc();
            Ok(Step::Gap { from, to })
        }
        Err(FetchError::Node(e)) => Err(e.into()),
    }
}

/// Keep asking until the node has nothing new (used by tests).
#[allow(dead_code)]
pub async fn ingest_until_caught_up<S: EventSource>(
    store: &Store,
    src: &mut S,
    cfg: &EventConfig,
    m: &Metrics,
) -> Result<(), EventError> {
    for _ in 0..100_000 {
        if ingest_step(store, src, cfg, m, 0).await? == Step::Empty {
            return Ok(());
        }
    }
    Err(EventError::Contract(
        "event ingestion did not settle".into(),
    ))
}

// ------------------------------------------------------------------------------ the loop

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// Requests the node may hold open until something happens.
    LongPoll,
    /// Short requests every `poll_interval`. Used while long requests keep failing
    /// (a proxy that cuts them, for example), until short ones work reliably again.
    Poll,
}

/// The background task of `serve` for one node. Never returns. A failure (node down,
/// connection dropped, bad answer) is counted and retried after a growing pause; after
/// `fallback_after` failures in a row it switches to plain polling, and goes back to long
/// polling after `recover_after` good polls. The cursor lives in the database, so every
/// retry continues exactly where the last stored page ended.
pub async fn run<S: EventSource>(store: Store, mut src: S, cfg: EventConfig, m: Arc<Metrics>) {
    let node = cfg.node.as_str();
    let mut mode = Mode::LongPoll;
    let (mut failures, mut good_polls) = (0u32, 0u32);
    loop {
        let started = Instant::now();
        let wait_ms = if mode == Mode::LongPoll {
            cfg.long_poll_ms
        } else {
            0
        };
        match ingest_step(&store, &mut src, &cfg, &m, wait_ms).await {
            Ok(step) => {
                failures = 0;
                if let Step::Gap { from, to } = step {
                    warn!(
                        node,
                        from, to, "the node no longer had these events, recorded as a gap"
                    );
                }
                if mode == Mode::Poll {
                    good_polls += 1;
                    if good_polls >= cfg.recover_after {
                        info!(node, "connection is stable, back to long polling");
                        mode = Mode::LongPoll;
                        good_polls = 0;
                        m.event_polling_mode.with_label_values(&[node]).set(0);
                    }
                }
                if step == Step::Empty {
                    // never spin: wait out the rest of the interval (a long poll already took it)
                    if let Some(rest) = cfg.poll_interval.checked_sub(started.elapsed()) {
                        tokio::time::sleep(rest).await;
                    }
                }
            }
            Err(e) => {
                failures += 1;
                good_polls = 0;
                if matches!(
                    e,
                    EventError::Store(StoreError::Db(_) | StoreError::Migrate(_))
                ) {
                    m.store_errors.inc();
                    error!(node, failures, "database fault: {e}");
                } else {
                    m.event_source_errors.inc();
                    warn!(node, failures, "{e}");
                }
                if failures >= cfg.fallback_after && mode == Mode::LongPoll {
                    warn!(
                        node,
                        failures, "failures in a row, switching to plain polling"
                    );
                    mode = Mode::Poll;
                    m.event_polling_mode.with_label_values(&[node]).set(1);
                }
                let pause = cfg
                    .backoff_base
                    .saturating_mul(1 << failures.min(6))
                    .min(cfg.backoff_max);
                tokio::time::sleep(pause).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::error::NodeError;
    use crate::event_source::{HttpEventSource, MemoryEventNode};
    use crate::fixtures::read_events;

    /// A recorded stream of the miner (it has proofs, lottery results and block events).
    const MINER_1: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/mock_data/pouw-fixtures/events/miner-1.jsonl"
    );

    fn id(s: &str) -> NodeId {
        s.parse().unwrap()
    }

    fn fast_cfg(node: &str) -> EventConfig {
        EventConfig {
            poll_interval: Duration::from_millis(5),
            backoff_base: Duration::from_millis(2),
            backoff_max: Duration::from_millis(20),
            ..EventConfig::new(id(node))
        }
    }

    async fn count(store: &Store, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(store.pool())
            .await
            .unwrap()
    }

    fn kind_count(events: &[Event], kind: &str) -> i64 {
        events.iter().filter(|e| e.kind == kind).count() as i64
    }

    /// seq 1..=n, each exactly once
    async fn assert_no_gaps_no_duplicates(store: &Store, node: &str, n: i64) {
        let row: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT count(*), count(DISTINCT seq), min(seq), max(seq) FROM node_events WHERE node_id = $1",
        )
        .bind(node)
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row, (n, n, 1, n));
        assert_eq!(store.event_cursor(&id(node)).await.unwrap() as i64, n);
    }

    #[sqlx::test]
    async fn the_recorded_stream_is_stored_in_every_table(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let events = read_events(MINER_1).unwrap();
        let n = events.len() as i64;
        let mut node = MemoryEventNode::new(events.clone());
        let m = Metrics::new();
        ingest_until_caught_up(&store, &mut node, &fast_cfg("miner-1"), &m)
            .await
            .unwrap();

        assert_no_gaps_no_duplicates(&store, "miner-1", n).await;
        assert_eq!(
            count(&store, "SELECT count(*) FROM tx_observations").await,
            kind_count(&events, "tx_received")
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM tx_rejections").await,
            kind_count(&events, "tx_rejected")
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM mempool_snapshots").await,
            kind_count(&events, "mempool_snapshot")
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM proof_attempts").await,
            kind_count(&events, "proof_produced")
        );
        // losing proofs are the wasted work
        let lost = events
            .iter()
            .filter(|e| e.kind == "lottery_result" && e.data["won"] == false)
            .count() as i64;
        assert!(lost > 0, "the fixture should contain losing proofs");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM proof_attempts WHERE won = false"
            )
            .await,
            lost
        );
        let unfinished =
            "SELECT count(*) FROM proof_attempts WHERE started_at IS NULL OR won IS NULL";
        assert_eq!(count(&store, unfinished).await, 0);
        let blocks: HashSet<&str> = events
            .iter()
            .filter(|e| e.kind.starts_with("block_"))
            .map(|e| e.data["hash"].as_str().unwrap())
            .collect();
        assert_eq!(
            count(&store, "SELECT count(*) FROM block_observations").await,
            blocks.len() as i64
        );
        let orphaned = "SELECT count(*) FROM block_observations WHERE orphaned_at IS NOT NULL";
        assert_eq!(
            count(&store, orphaned).await,
            kind_count(&events, "block_orphaned")
        );
        assert!(
            m.render()
                .contains(&format!("pouw_indexer_events_ingested_total {n}"))
        );
    }

    #[sqlx::test]
    async fn restart_and_replay_change_nothing(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let events = read_events(MINER_1).unwrap();
        let n = events.len();
        let mut node = MemoryEventNode::new(events.clone());
        let m = Metrics::new();
        let cfg = fast_cfg("miner-1");

        // the node has only the first part when we start ...
        node.set_visible(200);
        ingest_until_caught_up(&store, &mut node, &cfg, &m)
            .await
            .unwrap();
        assert_no_gaps_no_duplicates(&store, "miner-1", 200).await;
        // ... the indexer "restarts" (state is only in the database) and the node has grown
        node.set_visible(n);
        ingest_until_caught_up(&store, &mut node, &cfg, &m)
            .await
            .unwrap();
        ingest_until_caught_up(&store, &mut node, &cfg, &m)
            .await
            .unwrap();
        assert_no_gaps_no_duplicates(&store, "miner-1", n as i64).await;
        let attempts = count(&store, "SELECT count(*) FROM proof_attempts").await;

        // the same page delivered again (a retry after a lost answer) is harmless
        ingest_page(&store, &id("miner-1"), 0, &events[..50])
            .await
            .unwrap();
        ingest_page(&store, &id("miner-1"), 100, &events[100..160])
            .await
            .unwrap();
        assert_no_gaps_no_duplicates(&store, "miner-1", n as i64).await;
        assert_eq!(
            count(&store, "SELECT count(*) FROM proof_attempts").await,
            attempts
        );
    }

    #[sqlx::test]
    async fn a_page_that_skips_an_event_is_refused(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let events = read_events(MINER_1).unwrap();
        let mut holey = events[..10].to_vec();
        holey.remove(4);
        assert!(matches!(
            ingest_page(&store, &id("miner-1"), 0, &holey).await,
            Err(EventError::Contract(_))
        ));
        // an event of another node is refused too (wrong address configured)
        assert!(
            ingest_page(&store, &id("node-a"), 0, &events[..5])
                .await
                .is_err()
        );
        // nothing was written
        assert_eq!(count(&store, "SELECT count(*) FROM node_events").await, 0);
        assert_eq!(store.event_cursor(&id("miner-1")).await.unwrap(), 0);
    }

    #[sqlx::test]
    async fn odd_events_are_kept_raw_and_do_not_stop_ingestion(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let ev = |seq: u64, kind: &str, data: serde_json::Value| Event {
            seq,
            time: "2026-10-19T14:00:00.000Z".into(),
            node_id: "n".into(),
            kind: kind.into(),
            data,
        };
        let events = vec![
            ev(1, "something_from_the_future", serde_json::json!({"x": 1})),
            ev(
                2,
                "tx_received",
                serde_json::json!({"txid": "tx:zz", "tx_kind": "transfer", "sender": "addr:00", "fee": 1}),
            ),
            ev(3, "peer_connected", serde_json::json!({"peer_id": "p"})),
        ];
        let stats = ingest_page(&store, &id("n"), 0, &events).await.unwrap();
        assert_eq!(stats, PageStats { stored: 3, bad: 1 });
        assert_eq!(count(&store, "SELECT count(*) FROM node_events").await, 3);
        assert_eq!(
            count(&store, "SELECT count(*) FROM tx_observations").await,
            0
        );
        assert_eq!(store.event_cursor(&id("n")).await.unwrap(), 3);
    }

    #[sqlx::test]
    async fn events_the_node_dropped_are_recorded_as_a_gap(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let events = read_events(MINER_1).unwrap();
        let n = events.len() as u64;
        let mut node = MemoryEventNode::new(events).with_retention(100);
        let m = Metrics::new();
        ingest_until_caught_up(&store, &mut node, &fast_cfg("miner-1"), &m)
            .await
            .unwrap();

        let oldest = n - 99;
        let gap: (i64, i64) =
            sqlx::query_as("SELECT from_seq, to_seq FROM event_gaps WHERE node_id = 'miner-1'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(gap, (1, oldest as i64 - 1));
        assert_eq!(count(&store, "SELECT count(*) FROM node_events").await, 100);
        assert_eq!(
            count(&store, "SELECT min(seq) FROM node_events").await,
            oldest as i64
        );
        assert_eq!(store.event_cursor(&id("miner-1")).await.unwrap(), n);
        assert!(m.render().contains("pouw_indexer_event_gaps_total 1"));
    }

    #[sqlx::test]
    async fn a_dropped_connection_is_retried_and_falls_back_to_polling(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let events = read_events(MINER_1).unwrap();
        let n = events.len() as i64;
        let mut node = MemoryEventNode::new(events);
        node.fail_next(4); // four failed requests in a row, then the connection works again
        let m = Arc::new(Metrics::new());
        // never go back to long polling in this test, so the mode can be checked at the end
        let cfg = EventConfig {
            recover_after: 1_000_000,
            ..fast_cfg("miner-1")
        };

        let task = tokio::spawn(run(store.clone(), node, cfg, m.clone()));
        let deadline = Instant::now() + Duration::from_secs(15);
        while store.event_cursor(&id("miner-1")).await.unwrap() as i64 != n {
            assert!(Instant::now() < deadline, "the loop did not recover");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        task.abort();

        assert_no_gaps_no_duplicates(&store, "miner-1", n).await;
        let text = m.render();
        assert!(
            text.contains("pouw_indexer_event_source_errors_total 4"),
            "{text}"
        );
        assert!(
            text.contains("pouw_indexer_event_polling_mode{node_id=\"miner-1\"} 1"),
            "{text}"
        );
    }

    /// Serve a MemoryEventNode the way a real node would: GET /v1/events, JSON, 410 when expired.
    async fn serve(node: MemoryEventNode) -> String {
        use axum::{
            Json, Router,
            extract::{Query, State},
            http::StatusCode,
            routing::get,
        };
        async fn handler(
            State(node): State<Arc<MemoryEventNode>>,
            Query(q): Query<HashMap<String, u64>>,
        ) -> (StatusCode, Json<serde_json::Value>) {
            let after = q.get("after").copied().unwrap_or(0);
            let limit = u32::try_from(q.get("limit").copied().unwrap_or(100)).unwrap_or(1000);
            match node.window(after, limit) {
                Ok(page) => (StatusCode::OK, Json(serde_json::to_value(page).unwrap())),
                Err(FetchError::CursorExpired { oldest_seq }) => (
                    StatusCode::GONE,
                    Json(serde_json::json!({"error": {"code": "cursor_expired",
                        "message": format!("oldest available seq is {oldest_seq}"),
                        "details": {"oldest_seq": oldest_seq}}})),
                ),
                Err(FetchError::Node(_)) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({})),
                ),
            }
        }
        let app = Router::new()
            .route("/v1/events", get(handler))
            .with_state(Arc::new(node));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[sqlx::test]
    async fn the_http_client_reads_pages_and_reports_outages(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let events = read_events(MINER_1).unwrap();
        let n = events.len() as i64;
        let url = serve(MemoryEventNode::new(events.clone())).await;
        let mut src = HttpEventSource::new(&url).unwrap();

        // pages continue where the cursor is
        let first = src.fetch(0, 5, 0).await.unwrap();
        assert_eq!((first.events.len(), first.next), (5, 5));
        let second = src.fetch(5, 5, 0).await.unwrap();
        assert_eq!(second.events[0].seq, 6);
        // and it all ends up in the database through the HTTP client
        let m = Metrics::new();
        ingest_until_caught_up(&store, &mut src, &fast_cfg("miner-1"), &m)
            .await
            .unwrap();
        assert_no_gaps_no_duplicates(&store, "miner-1", n).await;

        // 410 becomes CursorExpired with the oldest seq
        let old_url = serve(MemoryEventNode::new(events).with_retention(50)).await;
        match HttpEventSource::new(&old_url)
            .unwrap()
            .fetch(0, 10, 0)
            .await
        {
            Err(FetchError::CursorExpired { oldest_seq }) => assert_eq!(oldest_seq as i64, n - 49),
            other => panic!("expected CursorExpired, got {other:?}"),
        }

        // a node that is not there: an ordinary error, not a panic
        let dead = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("http://{}", l.local_addr().unwrap())
        }; // the listener is dropped here, nothing listens on that port any more
        assert!(matches!(
            HttpEventSource::new(&dead).unwrap().fetch(0, 10, 0).await,
            Err(FetchError::Node(NodeError::Request(_)))
        ));
    }

    /// Events as the node's own types write them (pouw-api, event.rs): block events carry `id`,
    /// and `block_accepted` has no bucket fields.
    #[sqlx::test]
    async fn events_in_the_nodes_own_format_are_understood(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let blk = format!("blk:{}", "ab".repeat(32));
        let miner = format!("addr:{}", "cd".repeat(32));
        let ev = |seq: u64, kind: &str, data: serde_json::Value| Event {
            seq,
            time: "2026-11-08T10:00:00.000Z".into(),
            node_id: "node-1".into(),
            kind: kind.into(),
            data,
        };
        let events = vec![
            ev(
                1,
                "block_received",
                serde_json::json!({"id": blk, "height": 7, "from_peer": "p1"}),
            ),
            ev(
                2,
                "block_verified",
                serde_json::json!({"id": blk, "height": 7, "verify_ms": 4, "proof_count": 1, "ok": true}),
            ),
            ev(
                3,
                "block_accepted",
                serde_json::json!({"id": blk, "height": 7, "miner": miner, "tx_count": 2, "proof_count": 1}),
            ),
            ev(
                4,
                "block_orphaned",
                serde_json::json!({"id": blk, "height": 7}),
            ),
        ];
        let stats = ingest_page(&store, &id("node-1"), 0, &events)
            .await
            .unwrap();
        assert_eq!(stats, PageStats { stored: 4, bad: 0 });
        let row: (i64, bool, bool) =
            sqlx::query_as("SELECT height, accepted_at IS NOT NULL, orphaned_at IS NOT NULL FROM block_observations")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(row, (7, true, true));
    }
}
