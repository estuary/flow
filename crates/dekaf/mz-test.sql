/*
Start local materialize as:

  docker run --network host --rm -it -v mzdata:/mzdata -p 6875:6875 -p 6876:6876 materialize/materialized

Apply this test schema as:

  psql postgres://materialize@localhost:6875/materialize --file mz-test.sql


When running using a local data-plane-gateway, use the SSL_CERT_FILE to allow
its self-signed certificate, as in:

  SSL_CERT_FILE=/home/${USER}/estuary/data-plane-gateway/local-tls-cert.pem

*/

DROP SECRET IF EXISTS estuary_token CASCADE;

CREATE SECRET estuary_token AS '...';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'localhost',
    SECURITY PROTOCOL = 'SASL_PLAINTEXT',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = '{}',
    SASL PASSWORD = SECRET estuary_token
);

CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://localhost:9093',
    USERNAME = '{}',
    PASSWORD = SECRET estuary_token
);

CREATE SOURCE my_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'demo/wikipedia/recentchange-sampled')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT
  ;
