BEGIN;

CREATE ROLE dekaf NOLOGIN BYPASSRLS;
GRANT dekaf TO authenticator;
GRANT USAGE ON SCHEMA public TO dekaf;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO dekaf;
GRANT SELECT,INSERT,UPDATE,DELETE ON public.registered_avro_schemas TO dekaf;

END;