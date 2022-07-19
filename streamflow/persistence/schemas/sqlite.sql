CREATE TABLE workflow
(
    id     INTEGER PRIMARY KEY,
    name   TEXT UNIQUE,
    status INTEGER,
    type   TEXT
);

CREATE TABLE step
(
    id       INTEGER PRIMARY KEY,
    name     TEXT,
    workflow INTEGER,
    status   INTEGER,
    type     TEXT,
    params   BLOB,
    FOREIGN KEY (workflow) REFERENCES workflow (id)
);

CREATE TABLE port
(
    id       INTEGER PRIMARY KEY,
    name     TEXT,
    workflow INTEGER,
    type     TEXT,
    params   BLOB,
    FOREIGN KEY (workflow) REFERENCES workflow (id)
);


CREATE table deployment
(
    id       INTEGER PRIMARY KEY,
    name     TEXT,
    type     TEXT,
    params   BLOB,
    external INTEGER
);


CREATE table target
(
    id         INTEGER PRIMARY KEY,
    deployment INTEGER,
    locations  INTEGER,
    service    TEXT,
    workdir    TEXT,
    FOREIGN KEY (deployment) REFERENCES deployment (id)
);


CREATE TABLE command
(
    id         INTEGER PRIMARY KEY,
    step       INTEGER,
    cmd        TEXT,
    output     BLOB,
    status     INTEGER,
    start_time INTEGER,
    end_time   INTEGER,
    FOREIGN KEY (step) REFERENCES step (id)
);

CREATE TABLE dependency
(
    step INTEGER,
    port INTEGER,
    type INTEGER,
    name TEXT,
    PRIMARY KEY (step, port, type, name),
    FOREIGN KEY (step) REFERENCES step (id),
    FOREIGN KEY (port) REFERENCES port (id)
)
