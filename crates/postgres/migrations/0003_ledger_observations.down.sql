DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM ledger.observation_cursor)
       OR EXISTS (SELECT 1 FROM ledger.deposits)
       OR EXISTS (SELECT 1 FROM ledger.chain_state) THEN
        RAISE EXCEPTION 'restore a pre-migration backup; observation processing has started';
    END IF;
END $$;

DROP TABLE ledger.chain_state;
DROP TABLE ledger.deposits;
DROP TABLE ledger.observation_cursor;
DROP TABLE observations;
