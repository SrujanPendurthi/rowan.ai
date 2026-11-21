CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid NOT NULL REFERENCES workspaces(id),
    email text NOT NULL,
    password_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX users_email_lower_idx ON users (lower(email));

ALTER TABLE api_keys
    ADD COLUMN kind text NOT NULL DEFAULT 'agent' CHECK (kind IN ('agent', 'session')),
    ADD COLUMN user_id uuid REFERENCES users(id),
    ADD COLUMN expires_at timestamptz,
    ADD CONSTRAINT api_keys_session_shape CHECK (kind = 'agent' OR (user_id IS NOT NULL AND expires_at IS NOT NULL));
