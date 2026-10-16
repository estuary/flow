ALTER TABLE public.publications ADD COLUMN draft_content_hash text;

COMMENT ON COLUMN public.publications.draft_content_hash IS
    'SHA-256 of the expanded draft authorized at submission; NULL skips verification for legacy publications';
