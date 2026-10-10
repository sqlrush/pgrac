\set key random(1, 50000)
BEGIN;
UPDATE demo.idx_lookup SET payload = CASE WHEN left(payload, 1) = 'x' THEN repeat('y', 80) ELSE repeat('x', 80) END WHERE id = :key;
COMMIT;
