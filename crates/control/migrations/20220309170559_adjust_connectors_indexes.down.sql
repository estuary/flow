DROP INDEX idx_connectors_name;
DROP INDEX idx_connectors_type_name;

CREATE UNIQUE INDEX idx_connectors_name ON connectors("name");
CREATE INDEX idx_connectors_type_name ON connectors("type", "name");
