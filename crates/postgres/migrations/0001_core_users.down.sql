DROP TABLE user_identities;
DROP FUNCTION reject_user_identity_mutation();
DROP TABLE user_accounts;
DROP TABLE users;
DROP TYPE user_lifecycle_status;
DROP TYPE indexer_source;

-- Retain pgcrypto: other schemas may use this shared extension.
