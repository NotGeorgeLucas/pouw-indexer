//! What the indexer needs from a node: its head, ranges of blocks and accounts, as the node API
//! serves them (`pouw_api::view`). A real HTTP client and an in-memory node for tests.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use pouw_api::view::{AccountView, BlockView, BlocksPage, HeaderView};
use pouw_types::crypto::Address;
use pouw_types::{Amount, Nonce};

use crate::error::NodeError;
use crate::store::Hash;

/// The node's current chain tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    pub height: u64,
    pub hash: Hash,
}

/// The node API serves at most this many blocks per call.
pub const MAX_BLOCKS_PER_CALL: usize = pouw_api::view::MAX_BLOCKS_PER_CALL as usize;

pub trait NodeClient: Send {
    /// Tip of the node's canonical chain.
    fn head(&mut self) -> impl Future<Output = Result<Head, NodeError>> + Send;
    /// Canonical blocks with `from <= height <= to`, in height order, at most
    /// `MAX_BLOCKS_PER_CALL`. Heights the node does not have are left out.
    fn blocks(
        &mut self,
        from: u64,
        to: u64,
    ) -> impl Future<Output = Result<Vec<BlockView>, NodeError>> + Send;
    /// Balance and nonce of an address, `None` for an address the chain has not seen.
    fn account(
        &mut self,
        address: Hash,
    ) -> impl Future<Output = Result<Option<AccountView>, NodeError>> + Send;
}

// ------------------------------------------------------------------------------ real client

/// The node's REST API (`GET /v1/blocks/head`, `/v1/blocks?from=&to=`, `/v1/accounts/{addr}`).
pub struct HttpNode {
    client: reqwest::Client,
    base: String,
}

impl HttpNode {
    pub fn new(base_url: &str) -> Result<Self, NodeError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()?;
        Ok(Self {
            client,
            base: base_url.trim_end_matches('/').to_string(),
        })
    }

    async fn get(&self, path: &str, query: &[(&str, u64)]) -> Result<reqwest::Response, NodeError> {
        Ok(self
            .client
            .get(format!("{}{path}", self.base))
            .query(query)
            .send()
            .await?)
    }

    /// The answer if it is a success, otherwise the status and body as an error.
    async fn expect_ok(
        resp: reqwest::Response,
        path: &str,
    ) -> Result<reqwest::Response, NodeError> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.text().await.unwrap_or_default();
        Err(NodeError::Status {
            path: path.to_string(),
            status: status.as_u16(),
            body,
        })
    }
}

impl NodeClient for HttpNode {
    async fn head(&mut self) -> Result<Head, NodeError> {
        let path = pouw_api::path::HEAD;
        let resp = Self::expect_ok(self.get(path, &[]).await?, path).await?;
        let h: HeaderView = resp.json().await?;
        Ok(Head {
            height: h.header.height.get(),
            hash: h.id.hash().0,
        })
    }

    async fn blocks(&mut self, from: u64, to: u64) -> Result<Vec<BlockView>, NodeError> {
        let path = pouw_api::path::BLOCKS;
        let resp =
            Self::expect_ok(self.get(path, &[("from", from), ("to", to)]).await?, path).await?;
        let page: BlocksPage = resp.json().await?;
        Ok(page.blocks)
    }

    async fn account(&mut self, address: Hash) -> Result<Option<AccountView>, NodeError> {
        let path = pouw_api::path::ACCOUNT.replace("{addr}", &Address(address).to_string());
        let resp = self.get(&path, &[]).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::expect_ok(resp, &path).await?.json().await?))
    }
}

// ------------------------------------------------------------------------------ in memory

/// A node that lives in memory, for tests and for running without a node. `set_chain` swaps the
/// whole chain, which is how tests simulate a reorganisation.
#[derive(Default)]
pub struct MemoryNode {
    chain: Vec<BlockView>,
    accounts: HashMap<Hash, (u64, u64)>,
}

// Parts of this are only used by tests.
#[allow(dead_code)]
impl MemoryNode {
    pub fn new(chain: Vec<BlockView>) -> Self {
        Self {
            chain,
            accounts: HashMap::new(),
        }
    }

    pub fn set_chain(&mut self, chain: Vec<BlockView>) {
        self.chain = chain;
    }

    /// What `GET /v1/accounts/{addr}` answers for this address.
    pub fn set_account(&mut self, address: Hash, balance: u64, nonce: u64) {
        self.accounts.insert(address, (balance, nonce));
    }
}

impl NodeClient for MemoryNode {
    async fn head(&mut self) -> Result<Head, NodeError> {
        let last = self.chain.last().ok_or(NodeError::NoBlocks)?;
        Ok(Head {
            height: last.header.height.get(),
            hash: last.id.hash().0,
        })
    }

    async fn blocks(&mut self, from: u64, to: u64) -> Result<Vec<BlockView>, NodeError> {
        Ok(self
            .chain
            .iter()
            .filter(|b| (from..=to).contains(&b.header.height.get()))
            .take(MAX_BLOCKS_PER_CALL)
            .cloned()
            .collect())
    }

    async fn account(&mut self, address: Hash) -> Result<Option<AccountView>, NodeError> {
        Ok(self
            .accounts
            .get(&address)
            .map(|&(balance, nonce)| AccountView {
                address: Address(address),
                balance: Amount(balance),
                nonce: Nonce(nonce),
            }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::{Json, Router};

    use super::*;
    use crate::fakechain::ChainBuilder;
    use crate::metrics::Metrics;
    use crate::store::Store;
    use crate::sync::{SyncConfig, sync_until_caught_up};

    type Chain = Arc<Vec<BlockView>>;

    /// Serve a chain like the node API does: head, blocks (max 100 per call) and one account.
    async fn serve(chain: Vec<BlockView>, known: Hash) -> String {
        async fn head(State(c): State<Chain>) -> Json<HeaderView> {
            let b = c.last().expect("not empty");
            Json(HeaderView {
                id: b.id,
                header: b.header,
            })
        }
        async fn blocks(
            State(c): State<Chain>,
            Query(q): Query<HashMap<String, u64>>,
        ) -> Json<BlocksPage> {
            let (from, to) = (q["from"], q["to"]);
            let blocks = c
                .iter()
                .filter(|b| (from..=to).contains(&b.header.height.get()))
                .take(MAX_BLOCKS_PER_CALL)
                .cloned()
                .collect();
            Json(BlocksPage { blocks })
        }
        async fn account(
            State(known): State<Hash>,
            Path(addr): Path<String>,
        ) -> Result<Json<AccountView>, StatusCode> {
            if addr == Address(known).to_string() {
                Ok(Json(AccountView {
                    address: Address(known),
                    balance: Amount(500),
                    nonce: Nonce(3),
                }))
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        let app = Router::new()
            .route("/v1/blocks/head", get(head))
            .route("/v1/blocks", get(blocks))
            .with_state(Arc::new(chain))
            .route("/v1/accounts/{addr}", get(account).with_state(known));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[sqlx::test]
    async fn the_http_node_is_followed_over_several_pages(pool: sqlx::PgPool) {
        // 250 blocks need three requests of at most 100
        let chain = ChainBuilder::new(5).take_empty(250);
        let url = serve(chain.clone(), [9; 32]).await;
        let mut node = HttpNode::new(&url).unwrap();

        let head = node.head().await.unwrap();
        assert_eq!((head.height, head.hash), (249, chain[249].id.hash().0));
        assert_eq!(node.blocks(0, 1000).await.unwrap().len(), 100);

        let store = Store::new(pool);
        let cfg = SyncConfig {
            node: "http".parse().unwrap(),
            poll: Duration::from_millis(10),
        };
        sync_until_caught_up(&store, &mut node, &cfg, &Metrics::new())
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM blocks WHERE canonical")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(n, 250);
    }

    #[tokio::test]
    async fn accounts_are_read_and_an_unknown_address_is_none() {
        let url = serve(ChainBuilder::new(5).take_empty(3), [9; 32]).await;
        let mut node = HttpNode::new(&url).unwrap();
        let a = node.account([9; 32]).await.unwrap().expect("known address");
        assert_eq!((a.balance.0, a.nonce.0), (500, 3));
        assert!(node.account([8; 32]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_node_that_is_down_is_an_error_not_a_panic() {
        let dead = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("http://{}", l.local_addr().unwrap())
        };
        assert!(matches!(
            HttpNode::new(&dead).unwrap().head().await,
            Err(NodeError::Request(_))
        ));
    }

    #[test]
    fn an_address_is_written_with_its_prefix_in_the_url() {
        assert!(Address([1; 32]).to_string().starts_with("addr:"));
    }
}
