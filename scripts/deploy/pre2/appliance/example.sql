CREATE SCHEMA demo;
CREATE TABLE demo.idx_lookup (
    id integer NOT NULL PRIMARY KEY,
    payload varchar(100) NOT NULL
) WITH (fillfactor=90);
INSERT INTO demo.idx_lookup SELECT g, repeat('x',80) FROM generate_series(1,50000) g;
ANALYZE demo.idx_lookup;
