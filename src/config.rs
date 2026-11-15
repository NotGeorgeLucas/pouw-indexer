//! All configuration, read in one place. Nothing else in the crate reads the environment.
//!
//! | variable         | meaning                                                   | default        |
//! | ---------------- | --------------------------------------------------------- | -------------- |
//! | `DATABASE_URL`   | Postgres connection string                                | required       |
//! | `BIND_ADDR`      | address of /health and /metrics                           | `0.0.0.0:8000` |
//! | `NODE_URL`       | REST API of the node whose blocks are indexed             | blocks off     |
//! | `NODE_FIXTURES`  | a file of blocks to follow instead of a node              | blocks off     |
//! | `NODE_ID`        | name of that node in `sync_state`                         | `node`         |
//! | `EVENT_SOURCES`  | `node-id=url,...` event streams of nodes                  | events off     |
//! | `EVENT_FIXTURES` | `node-id=file,...` recorded event streams                 | events off     |

use std::net::SocketAddr;

use pouw_api::event::NodeId;

use crate::error::ConfigError;

#[derive(Debug, PartialEq, Eq)]
pub enum BlockSource {
    Http(String),
    Fixtures(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum EventSourceKind {
    Http(String),
    Fixtures(String),
}

#[derive(Debug, PartialEq, Eq)]
pub struct EventSourceConfig {
    pub node: NodeId,
    pub kind: EventSourceKind,
}

#[derive(Debug, PartialEq, Eq)]
pub struct IndexerConfig {
    pub database_url: String,
    pub bind: SocketAddr,
    pub blocks: Option<BlockSource>,
    pub node_id: NodeId,
    pub events: Vec<EventSourceConfig>,
}

impl IndexerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// `get` answers for a variable name. Separate from the environment so tests need none.
    pub fn from_vars(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let non_empty = |name: &str| get(name).filter(|v| !v.trim().is_empty());

        let database_url = non_empty("DATABASE_URL").ok_or(ConfigError::Missing("DATABASE_URL"))?;
        let bind = non_empty("BIND_ADDR").unwrap_or_else(|| "0.0.0.0:8000".into());
        let bind = bind.parse().map_err(|e| ConfigError::Invalid {
            var: "BIND_ADDR",
            reason: format!("{bind:?}: {e}"),
        })?;
        let node_id = match non_empty("NODE_ID") {
            Some(v) => v
                .parse()
                .map_err(|e: pouw_api::event::BadNodeId| ConfigError::Invalid {
                    var: "NODE_ID",
                    reason: e.to_string(),
                })?,
            None => NodeId::default(),
        };
        let blocks = match (non_empty("NODE_URL"), non_empty("NODE_FIXTURES")) {
            (Some(url), None) => Some(BlockSource::Http(url)),
            (None, Some(path)) => Some(BlockSource::Fixtures(path)),
            (None, None) => None,
            (Some(_), Some(_)) => {
                return Err(ConfigError::Invalid {
                    var: "NODE_URL",
                    reason: "NODE_URL and NODE_FIXTURES are both set, choose one".into(),
                });
            }
        };

        let mut events = Vec::new();
        for (var, http) in [("EVENT_SOURCES", true), ("EVENT_FIXTURES", false)] {
            for (name, target) in pairs(var, non_empty(var))? {
                let node: NodeId =
                    name.parse()
                        .map_err(|e: pouw_api::event::BadNodeId| ConfigError::Invalid {
                            var,
                            reason: e.to_string(),
                        })?;
                let kind = if http {
                    EventSourceKind::Http(target)
                } else {
                    EventSourceKind::Fixtures(target)
                };
                events.push(EventSourceConfig { node, kind });
            }
        }
        Ok(Self {
            database_url,
            bind,
            blocks,
            node_id,
            events,
        })
    }
}

/// `a=1,b=2` -> [("a", "1"), ("b", "2")]
fn pairs(var: &'static str, value: Option<String>) -> Result<Vec<(String, String)>, ConfigError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    value
        .split(',')
        .map(|p| {
            p.split_once('=')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .filter(|(k, v)| !k.is_empty() && !v.is_empty())
                .ok_or_else(|| ConfigError::Invalid {
                    var,
                    reason: format!("{p:?} is not name=value"),
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn cfg(vars: &[(&str, &str)]) -> Result<IndexerConfig, ConfigError> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect();
        IndexerConfig::from_vars(|k| map.get(k).cloned())
    }

    #[test]
    fn only_the_database_is_required() {
        let c = cfg(&[("DATABASE_URL", "postgres://x")]).unwrap();
        assert_eq!(c.bind, "0.0.0.0:8000".parse().unwrap());
        assert_eq!(
            (c.blocks, c.events.len(), c.node_id.as_str()),
            (None, 0, "node")
        );
        assert!(matches!(
            cfg(&[]),
            Err(ConfigError::Missing("DATABASE_URL"))
        ));
    }

    #[test]
    fn a_node_and_event_streams() {
        let c = cfg(&[
            ("DATABASE_URL", "postgres://x"),
            ("NODE_URL", "http://node-1:8000"),
            ("NODE_ID", "node-1"),
            (
                "EVENT_SOURCES",
                "node-1=http://node-1:8000, miner.2=http://m2:8000",
            ),
        ])
        .unwrap();
        assert_eq!(
            c.blocks,
            Some(BlockSource::Http("http://node-1:8000".into()))
        );
        assert_eq!(c.events.len(), 2);
        assert_eq!(c.events[1].node.as_str(), "miner.2");
    }

    #[test]
    fn bad_values_are_named() {
        let base = ("DATABASE_URL", "postgres://x");
        for bad in [
            vec![base, ("BIND_ADDR", "not an address")],
            vec![base, ("NODE_ID", "has space")],
            vec![base, ("EVENT_SOURCES", "no-equals-sign")],
            vec![base, ("EVENT_SOURCES", "bad id=http://x")],
            vec![base, ("NODE_URL", "http://a"), ("NODE_FIXTURES", "f")],
        ] {
            assert!(
                matches!(cfg(&bad), Err(ConfigError::Invalid { .. })),
                "{bad:?}"
            );
        }
    }
}
