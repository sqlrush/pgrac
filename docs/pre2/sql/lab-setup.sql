-- PGRAC PRE2 v0.135.0 VM 镜像测试手册（docs/pre2/vm-test-guide.md）
-- 7.0 准备实验数据：在节点 0 上执行一次（pgnode 0，然后 \i ~/lab-setup.sql）。

CREATE SCHEMA lab;

CREATE TABLE lab.accounts (
    id      integer PRIMARY KEY,
    balance bigint  NOT NULL
);
INSERT INTO lab.accounts (id, balance)
SELECT g, 1000 FROM generate_series(1, 10000) AS g;

CREATE TABLE lab.hot (
    id integer PRIMARY KEY,
    v  integer NOT NULL
);
INSERT INTO lab.hot SELECT g, 0 FROM generate_series(1, 20) AS g;

CREATE TABLE lab.lock_demo (
    id integer PRIMARY KEY,
    v  integer NOT NULL
);
INSERT INTO lab.lock_demo VALUES (1, 0), (2, 0);

CREATE TABLE lab.tlock_demo (
    id integer PRIMARY KEY
);
INSERT INTO lab.tlock_demo VALUES (1);

ANALYZE lab.accounts;
ANALYZE lab.hot;
ANALYZE lab.lock_demo;
ANALYZE lab.tlock_demo;
