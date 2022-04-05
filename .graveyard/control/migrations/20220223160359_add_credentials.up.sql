CREATE TABLE credentials (
  "id" BIGINT PRIMARY KEY NOT NULL DEFAULT id_generator(),
  "account_id" BIGINT NOT NULL REFERENCES accounts("id"),
  "issuer" TEXT NOT NULL,
  "subject" TEXT NOT NULL,
  "session_token" TEXT NOT NULL,
  "expires_at" TIMESTAMPTZ NOT NULL,
  "last_authorized_at" TIMESTAMPTZ NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL,
  "updated_at" TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX idx_credentials_issuer_subject ON credentials("issuer", "subject");
CREATE UNIQUE INDEX idx_credentials_session_token ON credentials("session_token");
