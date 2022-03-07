CREATE TABLE builds (
  "account_id" BIGINT NOT NULL REFERENCES accounts(id),
  "catalog" JSON NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL,
  "id" BIGINT PRIMARY KEY NOT NULL DEFAULT id_generator(),
  "state" JSONB NOT NULL,
  "updated_at" TIMESTAMPTZ NOT NULL
);

-- Index for efficiently identifying builds that are queued,
-- which is a small subset of the overall builds that exist.
CREATE UNIQUE INDEX builds_id_where_queued ON builds USING BTREE (id)
WHERE state->>'type' = 'queued';