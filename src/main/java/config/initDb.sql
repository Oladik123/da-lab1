DROP TABLE IF EXISTS nodes, tags;

CREATE TABLE IF NOT EXISTS nodes
(
    id        INTEGER PRIMARY KEY,
    node_id   INTEGER,
    version   INTEGER,
    timestamp date,
    uid       INTEGER,
    "user"    VARCHAR(100),
    changeset INTEGER,
    lat       double precision,
    lon       double precision


);

CREATE TABLE IF NOT EXISTS tags
(
    id      serial PRIMARY KEY,
    key     VARCHAR(100),
    value   VARCHAR(100),
    node_id INTEGER references nodes (id)
);