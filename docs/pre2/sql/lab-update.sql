\set id random(1, 10000)
BEGIN;
UPDATE lab.accounts SET balance = balance + 1 WHERE id = :id;
COMMIT;
