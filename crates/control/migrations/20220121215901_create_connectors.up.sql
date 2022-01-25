CREATE TABLE connectors (
  "id" BIGINT NOT NULL DEFAULT id_generator(),
  PRIMARY KEY (id),
  "type" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "owner" TEXT NOT NULL,
  "description" TEXT NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL,
  "updated_at" TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_connectors_type ON connectors("type");
CREATE UNIQUE INDEX idx_connectors_name ON connectors("name");
CREATE INDEX idx_connectors_type_name ON connectors("type", "name");
