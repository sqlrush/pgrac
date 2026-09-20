-- One committed increment per transaction. Author: SqlRush <sqlrush@gmail.com>
\set id random(1, :rows)
BEGIN;
UPDATE demo_account SET value=value+1 WHERE id=:id;
COMMIT;
