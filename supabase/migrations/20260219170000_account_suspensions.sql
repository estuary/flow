-- Account suspension tracking for the deletion lifecycle.
-- One row per suspended user. Deleted automatically when auth.users
-- is removed (FK cascade) or when the user is unsuspended.

CREATE TABLE internal.account_suspensions (
    user_id        uuid PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    reason         text NOT NULL,
    suspended_at   timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE internal.account_suspensions IS
  'Active account suspensions. Row is deleted when user is expired (via FK cascade) or unsuspended.';
