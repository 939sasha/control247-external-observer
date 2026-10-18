CREATE TABLE monitor (
  id INTEGER PRIMARY KEY CHECK(id=1),
  version INTEGER NOT NULL, last_slot INTEGER NOT NULL,
  failures INTEGER NOT NULL, phase TEXT NOT NULL,
  episode INTEGER NOT NULL, checked_at INTEGER NOT NULL,
  receipt TEXT NOT NULL
);
INSERT INTO monitor VALUES(1,0,-1,0,'unknown',0,0,'initial');
CREATE TABLE notices (
  id TEXT PRIMARY KEY, kind TEXT NOT NULL, episode INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('pending','claimed','sent','uncertain','failed')),
  created INTEGER NOT NULL, claimed INTEGER, finished INTEGER
);
CREATE INDEX notices_pending ON notices(state,created);
