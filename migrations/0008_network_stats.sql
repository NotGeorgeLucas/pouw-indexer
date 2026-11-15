-- Chain statistics per minute for Grafana (pouw-deploy: the read-only user `grafana` reads the
-- public schema). A view over the canonical chain, so a reorganisation corrects it by itself.
--   blocks            blocks with block_time in that minute
--   transfers, proofs coin transfers and proof transactions in them (proofs = proofs per minute)
--   txs               all transactions including proofs and circuit registrations
--   avg_block_time_s  mean seconds since the previous canonical block
--   useful_work       sum of the constraint counts of the circuits of those proofs

CREATE VIEW network_stats AS
WITH canon AS (
    SELECT b.hash, b.block_time, b.leaf_count,
           extract(epoch FROM b.block_time - lag(b.block_time) OVER (ORDER BY b.height)) AS gap_s
    FROM blocks b
    WHERE b.canonical
),
per_block AS (
    SELECT c.block_time, c.leaf_count, c.gap_s,
           (SELECT count(*) FROM transfers t WHERE t.block_hash = c.hash) AS transfers,
           (SELECT count(*) FROM proof_txs p WHERE p.block_hash = c.hash) AS proofs,
           (SELECT coalesce(sum(ci.constraint_count), 0)
              FROM proof_txs p JOIN circuits ci ON ci.circuit_id = p.circuit_id
             WHERE p.block_hash = c.hash) AS useful_work
    FROM canon c
)
SELECT date_trunc('minute', block_time)  AS minute,
       count(*)                          AS blocks,
       sum(transfers)::bigint            AS transfers,
       sum(proofs)::bigint               AS proofs,
       sum(leaf_count)::bigint           AS txs,
       avg(gap_s)::float8                AS avg_block_time_s,
       sum(useful_work)::bigint          AS useful_work
FROM per_block
GROUP BY 1;
