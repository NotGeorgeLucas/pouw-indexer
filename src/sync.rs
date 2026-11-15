//! Keep the database in step with a node: follow its chain, resume after a restart, undo the
//! blocks the node dropped in a reorganisation, and keep what is derived from the chain
//! (account balances, statistics) up to date.
//!
//! The idea in one paragraph: the checkpoint (`sync_state`) says "I have written the node's
//! chain up to this block". Each round asks the node for the next page of blocks. If the first
//! block of that page does not point at the checkpoint block, the node's chain changed under
//! us: walk back to the newest block both sides still agree on (the common ancestor), mark
//! everything above it as orphaned (kept, never deleted), and continue from there. A page and
//! the new checkpoint are written in one transaction, so a crash at any moment leaves the
//! database consistent. After a round, the addresses the blocks touched are copied from the
//! node again (a queue in the database, so a crash loses nothing) and the statistics views
//! are refreshed.

use std::sync::Arc;
use std::time::Duration;

use pouw_api::event::NodeId;
use tracing::{error, info, warn};

use crate::error::{StoreError, SyncError};
use crate::metrics::Metrics;
use crate::node::{MAX_BLOCKS_PER_CALL, NodeClient};
use crate::store::{self, Checkpoint, Store};

pub struct SyncConfig {
    /// The followed node, the key of its row in `sync_state`.
    pub node: NodeId,
    /// How long to wait when we are caught up.
    pub poll: Duration,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Progress {
    /// Nothing new.
    CaughtUp,
    /// A page of blocks was written.
    Advanced { blocks: usize },
    /// The node's chain changed; `depth` blocks were marked orphaned.
    Reorg { depth: u64 },
}

/// Addresses copied from the node per request round.
const ACCOUNTS_PER_BATCH: u32 = 100;

/// The node's chain no longer contains our checkpoint block. Find the newest height where
/// our canonical block and the node's block are the same, orphan everything above it and
/// move the checkpoint back there.
async fn reorganise<N: NodeClient>(
    store: &Store,
    node: &mut N,
    cfg: &SyncConfig,
    m: &Metrics,
    checkpoint_height: u64,
    node_height: u64,
) -> Result<Progress, SyncError> {
    let mut h = checkpoint_height.min(node_height);
    let ancestor = loop {
        let ours = store.canonical_hash_at(h).await?;
        let theirs = node
            .blocks(h, h)
            .await?
            .into_iter()
            .next()
            .map(|b| b.id.hash().0);
        if let Some(same) = ours.filter(|o| Some(*o) == theirs) {
            break Checkpoint {
                height: h,
                hash: same,
            };
        }
        h = h.checked_sub(1).ok_or_else(|| {
            SyncError::BadPage(
                "the node's chain shares no block with the database, not even the genesis".into(),
            )
        })?;
    };

    let mut tx = store.begin().await?;
    store::orphan_above(&mut tx, ancestor.height).await?;
    store::save_checkpoint(&mut tx, &cfg.node, ancestor).await?;
    tx.commit().await.map_err(StoreError::from)?;

    let depth = checkpoint_height.saturating_sub(ancestor.height);
    m.reorgs.inc();
    m.last_reorg_depth
        .set(i64::try_from(depth).unwrap_or(i64::MAX));
    m.indexed_height
        .set(i64::try_from(ancestor.height).unwrap_or(i64::MAX));
    Ok(Progress::Reorg { depth })
}

/// Write one page of blocks, or handle a reorganisation, or report caught up.
async fn advance<N: NodeClient>(
    store: &Store,
    node: &mut N,
    cfg: &SyncConfig,
    m: &Metrics,
) -> Result<Progress, SyncError> {
    let head = node.head().await?;
    let gauge = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
    m.node_height.set(gauge(head.height));
    let checkpoint = store.checkpoint(&cfg.node).await?;

    let next = match checkpoint {
        None => 0,
        Some(at) => {
            m.indexed_height.set(gauge(at.height));
            m.lag_blocks.set(gauge(head.height) - gauge(at.height));
            if at.height >= head.height {
                if at.height == head.height && at.hash == head.hash {
                    return Ok(Progress::CaughtUp);
                }
                // the node's chain is shorter than ours, or has another block at our height
                return reorganise(store, node, cfg, m, at.height, head.height).await;
            }
            at.height + 1
        }
    };

    let to = head.height.min(next + MAX_BLOCKS_PER_CALL as u64 - 1);
    let blocks = node.blocks(next, to).await?;
    if blocks.is_empty() {
        return Err(SyncError::BadPage(format!(
            "the node returned no blocks for heights {next}..{to}"
        )));
    }

    // The page must be one unbroken chain, and must continue from our checkpoint.
    for (i, b) in blocks.iter().enumerate() {
        let want = next + i as u64;
        if b.header.height.get() != want {
            return Err(SyncError::BadPage(format!(
                "height {} where {want} was expected",
                b.header.height.get()
            )));
        }
        if i > 0 && b.header.parent.hash().0 != blocks[i - 1].id.hash().0 {
            return Err(SyncError::BadPage(format!(
                "block {want} does not point at block {}",
                want - 1
            )));
        }
    }
    if let Some(at) = checkpoint.filter(|at| blocks[0].header.parent.hash().0 != at.hash) {
        return reorganise(store, node, cfg, m, at.height, head.height).await;
    }

    let started = std::time::Instant::now();
    let mut tx = store.begin().await?;
    let mut added = 0u64;
    for b in &blocks {
        if store::insert_block(&mut tx, b).await? {
            added += 1;
        }
    }
    let last = blocks.last().expect("checked non-empty");
    let tip = Checkpoint {
        height: last.header.height.get(),
        hash: last.id.hash().0,
    };
    store::save_checkpoint(&mut tx, &cfg.node, tip).await?;
    tx.commit().await.map_err(StoreError::from)?;

    m.batch_seconds.observe(started.elapsed().as_secs_f64());
    m.blocks_written.inc_by(added);
    m.indexed_height.set(gauge(tip.height));
    m.lag_blocks.set(gauge(head.height) - gauge(tip.height));
    Ok(Progress::Advanced {
        blocks: blocks.len(),
    })
}

/// Copy the queued addresses from the node. Each batch is read from the node first and written
/// in one transaction, so no database transaction waits on the network.
async fn copy_accounts<N: NodeClient>(
    store: &Store,
    node: &mut N,
    m: &Metrics,
) -> Result<(), SyncError> {
    loop {
        let batch = store.accounts_to_refresh(ACCOUNTS_PER_BATCH).await?;
        if batch.is_empty() {
            return Ok(());
        }
        let node_height = node.head().await?.height;
        let mut copied = Vec::with_capacity(batch.len());
        for r in &batch {
            copied.push(node.account(r.address).await?);
        }
        let mut tx = store.begin().await?;
        for (r, account) in batch.iter().zip(copied) {
            match account {
                Some(a) => {
                    store::upsert_account(&mut tx, &r.address, a.balance.0, a.nonce.0, node_height)
                        .await?
                }
                None => store::delete_account(&mut tx, &r.address).await?,
            }
            store::finish_refresh(&mut tx, r).await?;
        }
        tx.commit().await.map_err(StoreError::from)?;
        m.accounts_copied.inc_by(batch.len() as u64);
    }
}

/// One round: advance the chain, then refresh what is derived from it.
pub async fn sync_step<N: NodeClient>(
    store: &Store,
    node: &mut N,
    cfg: &SyncConfig,
    m: &Metrics,
) -> Result<Progress, SyncError> {
    let progress = advance(store, node, cfg, m).await?;
    if progress != Progress::CaughtUp {
        store.refresh_stats().await?;
    }
    copy_accounts(store, node, m).await?;
    Ok(progress)
}

/// Keep going until there is nothing new (used by tests).
#[allow(dead_code)]
pub async fn sync_until_caught_up<N: NodeClient>(
    store: &Store,
    node: &mut N,
    cfg: &SyncConfig,
    m: &Metrics,
) -> Result<(), SyncError> {
    for _ in 0..100_000 {
        if sync_step(store, node, cfg, m).await? == Progress::CaughtUp {
            return Ok(());
        }
    }
    Err(SyncError::BadPage(
        "sync did not settle after 100000 rounds".into(),
    ))
}

/// The background task of `serve`. Never returns. A failure (node down, bad data, database) is
/// counted, logged and retried with a growing pause, so the HTTP server stays up.
pub async fn run<N: NodeClient>(store: Store, mut node: N, cfg: SyncConfig, m: Arc<Metrics>) {
    let mut failures = 0u32;
    loop {
        match sync_step(&store, &mut node, &cfg, &m).await {
            Ok(Progress::CaughtUp) => {
                failures = 0;
                tokio::time::sleep(cfg.poll).await;
            }
            Ok(Progress::Advanced { blocks }) => {
                failures = 0;
                info!(node = cfg.node.as_str(), blocks, "wrote blocks");
            }
            Ok(Progress::Reorg { depth }) => {
                failures = 0;
                warn!(
                    node = cfg.node.as_str(),
                    depth, "the node reorganised, blocks marked orphaned"
                );
            }
            Err(e) => {
                failures += 1;
                if e.is_local_fault() {
                    m.store_errors.inc();
                    error!(node = cfg.node.as_str(), failures, "database fault: {e}");
                } else {
                    m.node_errors.inc();
                    warn!(node = cfg.node.as_str(), failures, "the node: {e}");
                }
                tokio::time::sleep(Duration::from_secs(1 << failures.min(5))).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::time::Instant;

    use pouw_api::view::BlockView;
    use pouw_types::TxKind;

    use super::*;
    use crate::fakechain::{ChainBuilder, demo_chain};
    use crate::node::MemoryNode;
    use crate::store::Hash;

    fn cfg() -> SyncConfig {
        SyncConfig {
            node: "test-node".parse().unwrap(),
            poll: Duration::from_millis(10),
        }
    }

    async fn count(store: &Store, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(store.pool())
            .await
            .unwrap()
    }

    async fn canonical_hashes(store: &Store) -> Vec<Vec<u8>> {
        sqlx::query_scalar("SELECT hash FROM blocks WHERE canonical ORDER BY height")
            .fetch_all(store.pool())
            .await
            .unwrap()
    }

    fn hashes(chain: &[BlockView]) -> Vec<Vec<u8>> {
        chain.iter().map(|b| b.id.hash().0.to_vec()).collect()
    }

    /// Every address the blocks touch, as the node counts them: miners, senders, recipients.
    fn addresses(chain: &[BlockView]) -> BTreeSet<Hash> {
        let mut out = BTreeSet::new();
        for b in chain {
            out.insert(b.header.miner.0);
            for t in b.txs.iter().chain(b.proof_txs.iter().map(|p| &p.request)) {
                out.insert(t.tx.body.sender.0);
                if let TxKind::Transfer { to, .. } = &t.tx.body.kind {
                    out.insert(to.0);
                }
            }
        }
        out
    }

    /// Two branches that share blocks 0..=10: `a` has 20 blocks, `b` has 14 (blocks 11..=13 differ).
    fn two_branches() -> (Vec<BlockView>, Vec<BlockView>) {
        let mut builder = ChainBuilder::new(42);
        let mut a = builder.take(11);
        let mut rival = builder.reseeded(7);
        a.extend(builder.take(9));
        let mut b = a[..=10].to_vec();
        b.extend(rival.take(3));
        (a, b)
    }

    #[sqlx::test]
    async fn restart_in_the_middle_does_not_duplicate(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let chain = demo_chain(20);
        let mut node = MemoryNode::new(chain[..7].to_vec());
        let m = Metrics::new();
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(count(&store, "SELECT count(*) FROM blocks").await, 7);

        // "restart": the node has more blocks now, and we sync again from the checkpoint
        node.set_chain(chain.clone());
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();
        // and once more with nothing new
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();

        assert_eq!(canonical_hashes(&store).await, hashes(&chain));
        let proofs: i64 = chain.iter().map(|b| b.proof_txs.len() as i64).sum();
        assert_eq!(
            count(&store, "SELECT count(*) FROM proof_txs").await,
            proofs
        );
        assert_eq!(count(&store, "SELECT count(*) FROM circuits").await, 3);
        assert_eq!(count(&store, "SELECT height FROM sync_state").await, 19);
    }

    #[sqlx::test]
    async fn the_database_follows_a_reorganisation_and_back(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let (a, b) = two_branches();
        assert_eq!((a.len(), b.len()), (20, 14));
        let mut node = MemoryNode::new(a.clone());
        let m = Metrics::new();

        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(canonical_hashes(&store).await, hashes(&a));

        // the node switches to branch b
        node.set_chain(b.clone());
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(
            canonical_hashes(&store).await,
            hashes(&b),
            "database matches the node"
        );
        // the dropped blocks are kept, marked, with their transactions
        assert_eq!(
            count(&store, "SELECT count(*) FROM blocks WHERE NOT canonical").await,
            9
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM blocks WHERE NOT canonical AND orphaned_at IS NOT NULL"
            )
            .await,
            9
        );
        let in_orphans: i64 = a[11..].iter().map(|x| x.proof_txs.len() as i64).sum();
        let orphaned_proofs = "SELECT count(*) FROM proof_txs p JOIN blocks bl ON bl.hash = p.block_hash WHERE NOT bl.canonical";
        assert_eq!(count(&store, orphaned_proofs).await, in_orphans);
        assert_eq!(count(&store, "SELECT height FROM sync_state").await, 13);
        let text = m.render();
        assert!(text.contains("pouw_indexer_reorgs_total 1"), "{text}");
        assert!(text.contains("pouw_indexer_last_reorg_depth 9"), "{text}");

        // the explorer API sees it: an orphaned block is there and says so, and the statistics
        // were refreshed after the reorganisation
        let orphan_hash = format!("blk:{}", hex::encode(a[15].id.hash().0));
        let canonical: bool =
            sqlx::query_scalar("SELECT canonical FROM api.blocks WHERE hash = $1")
                .bind(&orphan_hash)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert!(!canonical);
        assert_eq!(
            count(&store, "SELECT indexed_height FROM api.stats").await,
            13
        );

        // and the node goes back to branch a: orphaned blocks become canonical again
        node.set_chain(a.clone());
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(canonical_hashes(&store).await, hashes(&a));
        assert_eq!(
            count(&store, "SELECT count(*) FROM blocks WHERE NOT canonical").await,
            3
        );
        assert_eq!(count(&store, "SELECT count(*) FROM blocks").await, 23);
        assert_eq!(
            count(&store, "SELECT indexed_height FROM api.stats").await,
            19
        );
    }

    #[sqlx::test]
    async fn a_shorter_node_chain_orphans_the_extra_blocks(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let a = demo_chain(20);
        let mut node = MemoryNode::new(a.clone());
        let m = Metrics::new();
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();

        node.set_chain(a[..=12].to_vec());
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(canonical_hashes(&store).await, hashes(&a[..=12]));
        assert_eq!(
            count(&store, "SELECT count(*) FROM blocks WHERE NOT canonical").await,
            7
        );
    }

    #[sqlx::test]
    async fn ten_thousand_blocks_are_written_fast(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let mut node = MemoryNode::new(ChainBuilder::new(1).take_empty(10_000));
        let m = Metrics::new();

        let started = Instant::now();
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();
        let took = started.elapsed();
        println!("10000 blocks written in {took:?}");

        assert_eq!(
            count(&store, "SELECT count(*) FROM blocks WHERE canonical").await,
            10_000
        );
        assert!(took < Duration::from_secs(120), "took {took:?}");
        assert!(
            m.render()
                .contains("pouw_indexer_blocks_written_total 10000")
        );
    }

    #[sqlx::test]
    async fn balances_are_copied_from_the_node_and_follow_a_reorganisation(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let (a, b) = two_branches();
        let mut node = MemoryNode::new(a.clone());
        let mut expected: HashMap<Hash, (u64, u64)> = HashMap::new();
        for (i, addr) in addresses(&a).into_iter().enumerate() {
            expected.insert(addr, (1_000 + i as u64, i as u64));
            node.set_account(addr, 1_000 + i as u64, i as u64);
        }
        let m = Metrics::new();
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();

        // the table matches what the node says for every address the chain touched
        assert_eq!(
            count(&store, "SELECT count(*) FROM accounts").await as usize,
            expected.len()
        );
        for (addr, want) in &expected {
            assert_eq!(store.account(addr).await.unwrap(), Some(*want));
        }
        assert_eq!(
            count(&store, "SELECT count(*) FROM account_refresh").await,
            0,
            "the queue is empty"
        );
        // the API view shows it with the usual ids
        let (shown,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM api.accounts WHERE address LIKE 'addr:%'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(shown as usize, expected.len());

        // reorganisation: balances of everything the dropped blocks touched changed on the node
        let dropped = addresses(&a[11..]);
        for addr in &dropped {
            let (balance, nonce) = expected[addr];
            node.set_account(*addr, balance + 7, nonce + 1);
            expected.insert(*addr, (balance + 7, nonce + 1));
        }
        node.set_chain(b.clone());
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();
        for addr in &dropped {
            assert_eq!(
                store.account(addr).await.unwrap(),
                Some(expected[addr]),
                "refreshed after the reorganisation"
            );
        }
        assert_eq!(
            count(&store, "SELECT count(*) FROM account_refresh").await,
            0
        );
    }

    #[sqlx::test]
    async fn network_stats_count_the_canonical_chain_per_minute(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let (a, b) = two_branches();
        let mut node = MemoryNode::new(a.clone());
        let m = Metrics::new();
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();

        let totals = |chain: &[BlockView]| {
            let proofs: i64 = chain.iter().map(|x| x.proof_txs.len() as i64).sum();
            let txs: i64 = chain
                .iter()
                .map(|x| (x.txs.len() + x.proof_txs.len()) as i64)
                .sum();
            (chain.len() as i64, proofs, txs)
        };
        let sums = "SELECT coalesce(sum(blocks), 0)::bigint, coalesce(sum(proofs), 0)::bigint, coalesce(sum(txs), 0)::bigint FROM network_stats";
        let row: (i64, i64, i64) = sqlx::query_as(sums).fetch_one(store.pool()).await.unwrap();
        assert_eq!(row, totals(&a));
        // blocks are 5 to 14 seconds apart in the fake chain
        let avg: f64 = sqlx::query_scalar("SELECT avg(avg_block_time_s) FROM network_stats")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert!((5.0..=15.0).contains(&avg), "average block time {avg}");

        // after a reorganisation only the canonical chain counts
        node.set_chain(b.clone());
        sync_until_caught_up(&store, &mut node, &cfg(), &m)
            .await
            .unwrap();
        let row: (i64, i64, i64) = sqlx::query_as(sums).fetch_one(store.pool()).await.unwrap();
        assert_eq!(row, totals(&b));
    }
}
