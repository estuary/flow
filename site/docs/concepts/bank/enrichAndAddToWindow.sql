-- Enrich the transfer with a nested array of other transfers in the window.
WITH w AS (
    SELECT JSON_GROUP_ARRAY(JSON_OBJECT(
        'id', id,
        'recipient', recipient,
        'amount', amount
    )) AS window
    FROM transfers WHERE sender = $sender
)
SELECT $id, $sender, $recipient, $amount, w.* FROM w;

-- Add the current transfer to the window.
INSERT INTO transfers (id, sender, recipient, amount)
VALUES ($id, $sender, $recipient, $amount);