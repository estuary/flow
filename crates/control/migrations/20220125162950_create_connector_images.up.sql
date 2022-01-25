CREATE TABLE connector_images (
  "id" BIGINT NOT NULL DEFAULT id_generator(),
  PRIMARY KEY (id),
  "connector_id" BIGINT NOT NULL REFERENCES connectors("id"),
  "image" TEXT NOT NULL,
  "tag" TEXT NOT NULL,
  "sha256" TEXT NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL,
  "updated_at" TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_connector_images_image ON connector_images("image");
CREATE UNIQUE INDEX idx_connector_images_image_tag ON connector_images("image", "tag");
CREATE UNIQUE INDEX idx_connector_images_sha256 ON connector_images("sha256");
