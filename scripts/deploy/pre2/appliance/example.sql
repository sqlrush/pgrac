-- Synthetic example already installed in the appliance. Run only in a new database.
CREATE SCHEMA demo;
CREATE TABLE demo.accounts (
    id integer PRIMARY KEY,
    balance bigint NOT NULL DEFAULT 0,
    label text NOT NULL
);
INSERT INTO demo.accounts
SELECT i, 0, 'Account ' || i FROM generate_series(1, 50000) AS i;
ANALYZE demo.accounts;
