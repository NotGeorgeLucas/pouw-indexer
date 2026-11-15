-- A node id is not free text. The node validates it (pouw-api NodeId: 1 to 64 characters of
-- [A-Za-z0-9._-]), so the database checks the same rule instead of trusting every writer.

CREATE DOMAIN node_id AS TEXT CHECK (VALUE ~ '^[A-Za-z0-9._-]{1,64}$');

ALTER TABLE sync_state         ALTER COLUMN node_id TYPE node_id;
ALTER TABLE ingest_cursors     ALTER COLUMN node_id TYPE node_id;
ALTER TABLE node_events        ALTER COLUMN node_id TYPE node_id;
ALTER TABLE event_gaps         ALTER COLUMN node_id TYPE node_id;
ALTER TABLE tx_rejections      ALTER COLUMN node_id TYPE node_id;
ALTER TABLE block_observations ALTER COLUMN node_id TYPE node_id;
ALTER TABLE tx_observations    ALTER COLUMN node_id TYPE node_id;
ALTER TABLE mempool_snapshots  ALTER COLUMN node_id TYPE node_id;
ALTER TABLE proof_attempts     ALTER COLUMN node_id TYPE node_id;
