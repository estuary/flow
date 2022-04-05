CREATE TABLE accounts (
  "id" BIGINT PRIMARY KEY NOT NULL DEFAULT id_generator(),
  "display_name" TEXT NOT NULL,
  "email" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "unique_name" TEXT NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL,
  "updated_at" TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX idx_account_id ON accounts("id");
CREATE UNIQUE INDEX idx_account_email ON accounts("email");
CREATE UNIQUE INDEX idx_account_name ON accounts("name");
CREATE UNIQUE INDEX idx_account_unique_name ON accounts("unique_name");
