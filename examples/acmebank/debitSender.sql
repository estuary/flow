-- Debit the sender if they have an account with sufficient funds.
UPDATE current_balances
SET balance = balance - $amount
WHERE account = $sender AND balance >= $amount;

-- Publish the transfer enriched with outcome and sender balance.
-- Use SQLite's CHANGES() function to check if the prior UPDATE matched any rows.
-- Or, a special sweep account 'DEPOSIT' is always approved.
WITH t AS (SELECT $id, $sender, $recipient, $amount)
SELECT t.*,
    CASE WHEN CHANGES() OR $sender = 'DEPOSIT'
        THEN 'approve' ELSE 'deny' END AS outcome,
    COALESCE(b.balance, 0) AS sender_balance
FROM t
LEFT OUTER JOIN current_balances b ON $sender = b.account;