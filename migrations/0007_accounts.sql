-- Balance and nonce of an address, copied from the node (GET /v1/accounts/{addr}), as the
-- web API contract says ("After v0": copied by the indexer from the node). The indexer does not
-- compute balances from blocks, so it cannot disagree with the node about the rules.

CREATE TABLE accounts (
    address      BYTEA PRIMARY KEY CHECK (octet_length(address) = 32),
    balance      BIGINT NOT NULL CHECK (balance >= 0),
    nonce        BIGINT NOT NULL CHECK (nonce >= 0),       -- nonce of the next transaction
    node_height  BIGINT NOT NULL,                          -- tip of the node when it was copied
    copied_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Addresses whose account may have changed and must be copied again. It is filled in the same
-- transaction that writes or orphans a block, so a crash cannot lose a request, and emptied
-- after the copy. An address is listed again if a later block touches it meanwhile.
CREATE TABLE account_refresh (
    address       BYTEA PRIMARY KEY CHECK (octet_length(address) = 32),
    requested_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Every address a block touches: its miner, senders and recipients of transfers, registrants
-- of circuits, requesters of proofs.
CREATE FUNCTION touch_block_accounts(block bytea) RETURNS void
    LANGUAGE sql AS $$
    INSERT INTO account_refresh (address)
    SELECT address FROM (
        SELECT miner AS address FROM blocks WHERE hash = block
        UNION SELECT sender FROM transfers WHERE block_hash = block
        UNION SELECT recipient FROM transfers WHERE block_hash = block
        UNION SELECT registrant FROM circuits WHERE registered_block = block
        UNION SELECT requester FROM proof_txs WHERE block_hash = block
    ) touched
    ON CONFLICT (address) DO UPDATE SET requested_at = clock_timestamp()
$$;

-- For the explorer API (Hasura tracks the api schema). Same conventions as 0002.
CREATE VIEW api.accounts AS
SELECT api.hex('addr:', address) AS address,
       balance,
       nonce,
       node_height,
       copied_at AS updated_at
FROM accounts;
