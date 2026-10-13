#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <database-url>" >&2
  exit 2
fi
database_url=$1
psql "$database_url" --set=ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM ledger.postings GROUP BY transaction_id,asset
    HAVING sum(CASE side WHEN 'debit' THEN quantity ELSE -quantity END)<>0
  ) THEN RAISE EXCEPTION 'journal contains an unbalanced transaction'; END IF;
  IF EXISTS (
    SELECT 1 FROM ledger.current_balances b
    FULL JOIN (
      SELECT account_id,
        sum(CASE side WHEN 'debit' THEN quantity ELSE 0 END) debit_quantity,
        sum(CASE side WHEN 'credit' THEN quantity ELSE 0 END) credit_quantity
      FROM ledger.postings GROUP BY account_id
    ) p USING(account_id)
    WHERE b.account_id IS NULL OR p.account_id IS NULL
       OR b.debit_quantity<>p.debit_quantity OR b.credit_quantity<>p.credit_quantity
  ) THEN RAISE EXCEPTION 'projection differs from journal reconstruction'; END IF;
END $$;
SELECT count(*) AS transactions,
       (SELECT count(*) FROM ledger.postings) AS postings,
       (SELECT count(*) FROM ledger.accounts) AS accounts
FROM ledger.transactions;
SQL
