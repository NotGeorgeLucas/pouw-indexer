//! Reading and writing fixture files. Command line and tests only, so errors are `anyhow`.

use anyhow::Context;
use pouw_api::view::BlockView;

use crate::event_source::Event;

fn read_lines<T: serde::de::DeserializeOwned>(path: &str, what: &str) -> anyhow::Result<Vec<T>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
    text.lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty())
        .map(|(i, l)| {
            serde_json::from_str(l).with_context(|| format!("{path} line {}: not {what}", i + 1))
        })
        .collect()
}

/// One block per line, in the JSON of the node API.
pub fn read_blocks(path: &str) -> anyhow::Result<Vec<BlockView>> {
    read_lines(path, "a block")
}

pub fn write_blocks(path: &str, blocks: &[BlockView]) -> anyhow::Result<()> {
    let mut out = String::new();
    for b in blocks {
        out.push_str(&serde_json::to_string(b)?);
        out.push('\n');
    }
    std::fs::write(path, out).with_context(|| format!("cannot write {path}"))
}

/// One event per line.
pub fn read_events(path: &str) -> anyhow::Result<Vec<Event>> {
    read_lines(path, "an event")
}
