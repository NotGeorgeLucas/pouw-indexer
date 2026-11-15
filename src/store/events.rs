//! Node events: the log as received, and the typed rows made from it.

use pouw_api::event::NodeId;

use super::{Store, Tx, num};
use crate::error::StoreError;
use crate::event_source::Event;

/// An event turned into database-ready values. Building it checks everything (id lengths,
/// numbers that fit), so a bad event is caught here and never fails the SQL.
pub enum Parsed {
    TxReceived {
        txid: [u8; 32],
        kind: String,
        sender: [u8; 32],
        fee: i64,
    },
    TxRejected {
        txid: Option<[u8; 32]>,
        reason: String,
        message: String,
    },
    ProofProduced {
        txid_text: String,
        txid: [u8; 32],
        circuit: [u8; 32],
        system: String,
        prove_ms: i64,
        proof_hash: [u8; 32],
    },
    Lottery {
        txid: [u8; 32],
        proof_hash: [u8; 32],
        circuit: [u8; 32],
        won: bool,
        eta: [u8; 31],
        mask: [u8; 31],
        counter: i64,
    },
    BlockReceived {
        hash: [u8; 32],
        height: i64,
        from_peer: String,
    },
    BlockVerified {
        hash: [u8; 32],
        height: i64,
        verify_ms: i64,
        proofs: i64,
        ok: bool,
    },
    BlockAccepted {
        hash: [u8; 32],
        height: i64,
    },
    BlockRejected {
        hash: [u8; 32],
        height: Option<i64>,
        reason: String,
    },
    BlockOrphaned {
        hash: [u8; 32],
        height: i64,
    },
    Snapshot {
        pending_txs: i64,
        pending_requests: i64,
        by_circuit: serde_json::Value,
    },
    /// Kept only in node_events: proof_request_started, proof_verified, peer_*, node_started,
    /// and any kind this version does not know.
    RawOnly,
}

pub async fn apply_event(
    tx: &mut Tx<'_>,
    node: &NodeId,
    ev: &Event,
    p: &Parsed,
) -> Result<(), StoreError> {
    let t = ev.time.as_str();
    match p {
        Parsed::TxReceived {
            txid,
            kind,
            sender,
            fee,
        } => {
            sqlx::query(
                "INSERT INTO tx_observations (node_id, txid, tx_kind, sender, fee, received_at) \
                 VALUES ($1, $2, $3, $4, $5, $6::timestamptz) ON CONFLICT (node_id, txid) DO NOTHING",
            )
            .bind(node.as_str()).bind(&txid[..]).bind(kind).bind(&sender[..]).bind(fee).bind(t)
            .execute(&mut **tx).await?;
        }
        Parsed::TxRejected {
            txid,
            reason,
            message,
        } => {
            sqlx::query(
                "INSERT INTO tx_rejections (node_id, seq, txid, reason_code, message, rejected_at) \
                 VALUES ($1, $2, $3, $4, $5, $6::timestamptz) ON CONFLICT (node_id, seq) DO NOTHING",
            )
            .bind(node.as_str()).bind(ev.seq as i64).bind(txid.as_ref().map(|x| x.to_vec())).bind(reason).bind(message).bind(t)
            .execute(&mut **tx).await?;
        }
        Parsed::ProofProduced {
            txid_text,
            txid,
            circuit,
            system,
            prove_ms,
            proof_hash,
        } => {
            // started_at: the proof_request_started this node logged earlier for the same request
            sqlx::query(
                "INSERT INTO proof_attempts (node_id, proof_hash, request_txid, circuit_id, proof_system, \
                     started_at, produced_at, prove_ms) \
                 VALUES ($1, $2, $3, $4, $5, \
                     (SELECT e.time FROM node_events e WHERE e.node_id = $1 AND e.kind = 'proof_request_started' \
                        AND e.data ->> 'txid' = $8 AND e.seq < $9 ORDER BY e.seq DESC LIMIT 1), \
                     $6::timestamptz, $7) \
                 ON CONFLICT (node_id, proof_hash) DO UPDATE SET request_txid = EXCLUDED.request_txid, \
                     circuit_id = EXCLUDED.circuit_id, proof_system = EXCLUDED.proof_system, \
                     started_at = COALESCE(proof_attempts.started_at, EXCLUDED.started_at), \
                     produced_at = EXCLUDED.produced_at, prove_ms = EXCLUDED.prove_ms",
            )
            .bind(node.as_str()).bind(&proof_hash[..]).bind(&txid[..]).bind(&circuit[..]).bind(system)
            .bind(t).bind(prove_ms).bind(txid_text).bind(ev.seq as i64)
            .execute(&mut **tx).await?;
        }
        Parsed::Lottery {
            txid,
            proof_hash,
            circuit,
            won,
            eta,
            mask,
            counter,
        } => {
            sqlx::query(
                "INSERT INTO proof_attempts (node_id, proof_hash, request_txid, circuit_id, won, eta, mask, usage_counter) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                 ON CONFLICT (node_id, proof_hash) DO UPDATE SET won = EXCLUDED.won, eta = EXCLUDED.eta, \
                     mask = EXCLUDED.mask, usage_counter = EXCLUDED.usage_counter",
            )
            .bind(node.as_str()).bind(&proof_hash[..]).bind(&txid[..]).bind(&circuit[..]).bind(won)
            .bind(&eta[..]).bind(&mask[..]).bind(counter)
            .execute(&mut **tx).await?;
        }
        Parsed::BlockReceived {
            hash,
            height,
            from_peer,
        } => {
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, received_at, from_peer) \
                 VALUES ($1, $2, $3, $4::timestamptz, $5) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET height = EXCLUDED.height, \
                     received_at = COALESCE(block_observations.received_at, EXCLUDED.received_at), \
                     from_peer = COALESCE(block_observations.from_peer, EXCLUDED.from_peer)",
            )
            .bind(node.as_str()).bind(&hash[..]).bind(height).bind(t).bind(from_peer)
            .execute(&mut **tx).await?;
        }
        Parsed::BlockVerified {
            hash,
            height,
            verify_ms,
            proofs,
            ok,
        } => {
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, verified_at, verify_ms, verified_ok, verified_proofs) \
                 VALUES ($1, $2, $3, $4::timestamptz, $5, $6, $7) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET height = EXCLUDED.height, \
                     verified_at = EXCLUDED.verified_at, verify_ms = EXCLUDED.verify_ms, \
                     verified_ok = EXCLUDED.verified_ok, verified_proofs = EXCLUDED.verified_proofs",
            )
            .bind(node.as_str()).bind(&hash[..]).bind(height).bind(t).bind(verify_ms).bind(ok).bind(proofs)
            .execute(&mut **tx).await?;
        }
        Parsed::BlockAccepted { hash, height } => {
            // accepted again after an orphaning: the block is part of the chain once more
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, accepted_at) \
                 VALUES ($1, $2, $3, $4::timestamptz) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET height = EXCLUDED.height, \
                     accepted_at = EXCLUDED.accepted_at, orphaned_at = NULL",
            )
            .bind(node.as_str())
            .bind(&hash[..])
            .bind(height)
            .bind(t)
            .execute(&mut **tx)
            .await?;
        }
        Parsed::BlockRejected {
            hash,
            height,
            reason,
        } => {
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, rejected_at, reject_reason) \
                 VALUES ($1, $2, $3, $4::timestamptz, $5) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET \
                     height = COALESCE(EXCLUDED.height, block_observations.height), \
                     rejected_at = EXCLUDED.rejected_at, reject_reason = EXCLUDED.reject_reason",
            )
            .bind(node.as_str()).bind(&hash[..]).bind(height).bind(t).bind(reason)
            .execute(&mut **tx).await?;
        }
        Parsed::BlockOrphaned { hash, height } => {
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, orphaned_at) \
                 VALUES ($1, $2, $3, $4::timestamptz) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET height = EXCLUDED.height, orphaned_at = EXCLUDED.orphaned_at",
            )
            .bind(node.as_str()).bind(&hash[..]).bind(height).bind(t)
            .execute(&mut **tx).await?;
        }
        Parsed::Snapshot {
            pending_txs,
            pending_requests,
            by_circuit,
        } => {
            sqlx::query(
                "INSERT INTO mempool_snapshots (node_id, observed_at, pending_txs, pending_proof_requests, by_circuit) \
                 VALUES ($1, $2::timestamptz, $3, $4, $5) ON CONFLICT DO NOTHING",
            )
            .bind(node.as_str()).bind(t).bind(pending_txs).bind(pending_requests).bind(by_circuit)
            .execute(&mut **tx).await?;
        }
        Parsed::RawOnly => {}
    }
    Ok(())
}

/// Every event as received, keyed by (node, seq): storing a page twice changes nothing.
pub async fn insert_events(
    tx: &mut Tx<'_>,
    node: &NodeId,
    events: &[Event],
) -> Result<(), StoreError> {
    let seqs = events
        .iter()
        .map(|e| num(e.seq))
        .collect::<Result<Vec<i64>, _>>()?;
    let times: Vec<&str> = events.iter().map(|e| e.time.as_str()).collect();
    let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
    let data: Vec<String> = events.iter().map(|e| e.data.to_string()).collect();
    sqlx::query(
        "INSERT INTO node_events (node_id, seq, time, kind, data) \
         SELECT $1, u.s, u.t::timestamptz, u.k, u.d::jsonb \
         FROM UNNEST($2::bigint[], $3::text[], $4::text[], $5::text[]) AS u(s, t, k, d) \
         ON CONFLICT (node_id, seq) DO NOTHING",
    )
    .bind(node.as_str())
    .bind(&seqs)
    .bind(&times)
    .bind(&kinds)
    .bind(&data)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The cursor never moves back, even if a page is replayed.
pub async fn set_event_cursor(tx: &mut Tx<'_>, node: &NodeId, seq: u64) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO ingest_cursors (node_id, last_seq) VALUES ($1, $2) \
         ON CONFLICT (node_id) DO UPDATE SET last_seq = GREATEST(ingest_cursors.last_seq, EXCLUDED.last_seq)",
    )
    .bind(node.as_str())
    .bind(num(seq)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Events from..=to of a node are lost for good: it no longer had them when we asked.
pub async fn record_event_gap(
    tx: &mut Tx<'_>,
    node: &NodeId,
    from: u64,
    to: u64,
) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO event_gaps (node_id, from_seq, to_seq) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
        .bind(node.as_str())
        .bind(num(from)?)
        .bind(num(to)?)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

impl Store {
    /// Last event stored for a node, 0 before the first.
    pub async fn event_cursor(&self, node: &NodeId) -> Result<u64, StoreError> {
        let v: Option<i64> =
            sqlx::query_scalar("SELECT last_seq FROM ingest_cursors WHERE node_id = $1")
                .bind(node.as_str())
                .fetch_optional(&self.pool)
                .await?;
        super::unsigned(v.unwrap_or(0))
    }
}
