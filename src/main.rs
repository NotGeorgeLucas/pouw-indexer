//! pouw-indexer: follows nodes, stores their blocks and events in Postgres for the explorer
//! (Hasura) and Grafana. This file only wires the parts together.
//!
//! Commands: `serve` (default), `seed [file]`, `gen-fixtures [count] [file]`.

mod api;
mod config;
mod error;
mod event_source;
mod events;
mod fakechain;
mod fixtures;
mod metrics;
mod node;
mod store;
mod sync;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::config::{BlockSource, EventSourceKind, IndexerConfig};
use crate::error::ConfigError;
use crate::event_source::{HttpEventSource, MemoryEventNode};
use crate::events::EventConfig;
use crate::metrics::Metrics;
use crate::node::{HttpNode, MemoryNode};
use crate::store::Store;
use crate::sync::SyncConfig;

const DEFAULT_FIXTURES: &str = "tests/fixtures/blocks.jsonl";

#[tokio::main]
async fn main() -> ExitCode {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e:#}");
            // 2 = bad configuration, 1 = anything else
            if e.downcast_ref::<ConfigError>().is_some() {
                ExitCode::from(2)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}

async fn run() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None | Some("serve") => serve().await,
        Some("seed") => seed(&args.next().unwrap_or_else(|| DEFAULT_FIXTURES.into())).await,
        Some("gen-fixtures") => {
            let count = args
                .next()
                .map_or(Ok(20), |n| n.parse())
                .context("count is not a number")?;
            gen_fixtures(
                count,
                &args.next().unwrap_or_else(|| DEFAULT_FIXTURES.into()),
            )
        }
        Some(other) => bail!(
            "unknown command {other:?}. Use `serve`, `seed [file]` or `gen-fixtures [count] [file]`"
        ),
    }
}

/// `indexer serve`: follow the configured nodes and serve /health and /metrics.
async fn serve() -> anyhow::Result<()> {
    let config = IndexerConfig::from_env()?;
    let store = Store::connect(&config.database_url).await?;
    let metrics = Arc::new(Metrics::new());

    match config.blocks {
        Some(BlockSource::Http(url)) => {
            info!(
                node = config.node_id.as_str(),
                url, "following the blocks of a node"
            );
            let cfg = SyncConfig {
                node: config.node_id,
                poll: Duration::from_secs(2),
            };
            tokio::spawn(sync::run(
                store.clone(),
                HttpNode::new(&url)?,
                cfg,
                metrics.clone(),
            ));
        }
        Some(BlockSource::Fixtures(path)) => {
            info!(
                node = config.node_id.as_str(),
                path, "following the blocks in a file"
            );
            let node = MemoryNode::new(fixtures::read_blocks(&path)?);
            let cfg = SyncConfig {
                node: config.node_id,
                poll: Duration::from_secs(2),
            };
            tokio::spawn(sync::run(store.clone(), node, cfg, metrics.clone()));
        }
        None => {
            info!("blocks are not followed: set NODE_URL (or NODE_FIXTURES for a file of blocks)")
        }
    }

    if config.events.is_empty() {
        info!(
            "events are not followed: set EVENT_SOURCES (or EVENT_FIXTURES for a recorded stream)"
        );
    }
    for source in config.events {
        let cfg = EventConfig::new(source.node);
        match source.kind {
            EventSourceKind::Http(url) => {
                info!(node = cfg.node.as_str(), url, "reading events");
                tokio::spawn(events::run(
                    store.clone(),
                    HttpEventSource::new(&url)?,
                    cfg,
                    metrics.clone(),
                ));
            }
            EventSourceKind::Fixtures(path) => {
                info!(node = cfg.node.as_str(), path, "reading recorded events");
                let src = MemoryEventNode::new(fixtures::read_events(&path)?);
                tokio::spawn(events::run(store.clone(), src, cfg, metrics.clone()));
            }
        }
    }

    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .context("cannot bind BIND_ADDR")?;
    info!(addr = %config.bind, "listening");
    axum::serve(listener, api::router(store, metrics)).await?;
    Ok(())
}

/// `indexer seed [file]`: load blocks into the database. The file has one block per line in
/// the JSON of the node API (what `gen-fixtures` writes).
async fn seed(path: &str) -> anyhow::Result<()> {
    let config = IndexerConfig::from_env()?;
    let store = Store::connect(&config.database_url).await?;
    let blocks = fixtures::read_blocks(path)?;
    let mut tx = store.begin().await?;
    let mut added = 0;
    for b in &blocks {
        if store::insert_block(&mut tx, b).await? {
            added += 1;
        }
    }
    tx.commit().await?;
    store.refresh_stats().await?;
    info!(
        "seed: {} blocks read from {path}, {added} new, {} already there",
        blocks.len(),
        blocks.len() - added
    );
    Ok(())
}

/// `indexer gen-fixtures [count] [file]`: write a fake chain, made with the node's own types.
fn gen_fixtures(count: usize, path: &str) -> anyhow::Result<()> {
    fixtures::write_blocks(path, &fakechain::demo_chain(count))?;
    info!("gen-fixtures: wrote {count} blocks to {path}");
    Ok(())
}
