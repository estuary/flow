CREATE TABLE connector_images (
  "id" BIGINT NOT NULL DEFAULT id_generator(),
  PRIMARY KEY (id),
  "connector_id" BIGINT NOT NULL REFERENCES connectors("id"),
  "name" TEXT NOT NULL,
  "tag" TEXT NOT NULL,
  "digest" TEXT NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL,
  "updated_at" TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_connector_images_image ON connector_images("name");
