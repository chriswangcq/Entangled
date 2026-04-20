import pytest
from fastapi.testclient import TestClient
from entangled.app.factory import create_app
from entangled.app.config import ServiceConfig
from entangled.app.state import get_db, init_database
from entangled.app.auth import verify_service_or_user

import sqlite3

def override_auth():
    return {"user_id": "test"}

@pytest.fixture
def app():
    config = ServiceConfig(
        jwt_secret="test",
        host="127.0.0.1",
        port=19900,
        db_path=":memory:"
    )
    application = create_app(config)
    application.dependency_overrides[verify_service_or_user] = override_auth
    return application

class FakeDatabase:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def fetchone(self, sql, params=()):
        cur = self._conn.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None

    def fetchall(self, sql, params=()):
        cur = self._conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    db = FakeDatabase(conn)
    # Create the outbox table directly for the test
    db.execute("""
        CREATE TABLE message_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE NOT NULL,
            agent_id TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            delivered_at INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            locked_by TEXT,
            locked_until INTEGER,
            permanent_failure INTEGER NOT NULL DEFAULT 0
        )
    """)
    return db

@pytest.fixture
def client(app, db):
    app.dependency_overrides[get_db] = lambda: db
    with TestClient(app) as c:
        yield c

def test_claim_empty(client):
    res = client.post("/v1/outbox/claim", json={
        "worker_id": "w1"
    })
    assert res.status_code == 200
    assert res.json() == {"rows": [], "count": 0}

def test_claim_and_mark_delivered(client, db):
    # Insert a message
    db.execute("""
        INSERT INTO message_outbox 
        (message_id, agent_id, trigger_type, payload_json, created_at)
        VALUES ('msg_1', 'agt_1', 'USER_MESSAGE', '{}', 1000)
    """)
    
    # Claim it
    res = client.post("/v1/outbox/claim", json={
        "worker_id": "w1",
        "claim_ttl_ms": 30000
    })
    assert res.status_code == 200
    data = res.json()
    assert data["count"] == 1
    row = data["rows"][0]
    assert row["message_id"] == "msg_1"
    
    # Check DB locked
    db_row = db.execute("SELECT * FROM message_outbox WHERE id = ?", (row["id"],)).fetchone()
    assert db_row["locked_by"] == "w1"
    assert db_row["locked_until"] is not None
    
    # Try claiming with another worker, should get 0
    res2 = client.post("/v1/outbox/claim", json={
        "worker_id": "w2"
    })
    assert res2.json()["count"] == 0
    
    # Mark delivered
    res3 = client.post("/v1/outbox/mark_delivered", json={
        "ids": [row["id"]]
    })
    assert res3.status_code == 200
    assert res3.json() == {"updated": 1}
    
    # Check DB delivered
    db_row2 = db.execute("SELECT * FROM message_outbox WHERE id = ?", (row["id"],)).fetchone()
    assert db_row2["delivered_at"] is not None
    assert db_row2["locked_by"] is None
    
def test_claim_and_mark_failed_transient(client, db):
    db.execute("""
        INSERT INTO message_outbox 
        (message_id, agent_id, trigger_type, payload_json, created_at)
        VALUES ('msg_2', 'agt_1', 'USER_MESSAGE', '{}', 1000)
    """)
    res = client.post("/v1/outbox/claim", json={"worker_id": "w1"})
    row_id = res.json()["rows"][0]["id"]
    
    res2 = client.post("/v1/outbox/mark_failed", json={
        "id": row_id,
        "kind": "network",
        "error": "timeout",
        "permanent": False,
        "retry_delay_ms": 1000
    })
    assert res2.status_code == 200
    
    db_row = db.execute("SELECT * FROM message_outbox WHERE id = ?", (row_id,)).fetchone()
    assert db_row["attempts"] == 1
    assert db_row["last_error"] == "network: timeout"
    assert db_row["locked_by"] is None
    assert db_row["locked_until"] is not None # should be bumped

def test_claim_and_mark_failed_permanent(client, db):
    db.execute("""
        INSERT INTO message_outbox 
        (message_id, agent_id, trigger_type, payload_json, created_at)
        VALUES ('msg_3', 'agt_1', 'USER_MESSAGE', '{}', 1000)
    """)
    res = client.post("/v1/outbox/claim", json={"worker_id": "w1"})
    row_id = res.json()["rows"][0]["id"]
    
    res2 = client.post("/v1/outbox/mark_failed", json={
        "id": row_id,
        "kind": "no_owner",
        "error": "agent has no owner",
        "permanent": True
    })
    assert res2.status_code == 200
    
    db_row = db.execute("SELECT * FROM message_outbox WHERE id = ?", (row_id,)).fetchone()
    # TD-6 (2026-04-21): attempts stays truthful (just +1), permanent_failure
    # flag is what keeps the row out of future claims. Pre-TD-6 this
    # assertion was ``attempts >= 5`` because the code sprayed 999999 on
    # the attempts column.
    assert db_row["attempts"] == 1
    assert db_row["permanent_failure"] == 1
    assert db_row["locked_by"] is None

    # Should not be claimed again because permanent_failure = 1
    res3 = client.post("/v1/outbox/claim", json={"worker_id": "w2", "max_attempts": 5})
    assert res3.json()["count"] == 0

def test_claim_ttl_expires_and_reclaims(client, db):
    db.execute("""
        INSERT INTO message_outbox 
        (message_id, agent_id, trigger_type, payload_json, created_at)
        VALUES ('msg_recover', 'agt_1', 'user_message', '{}', 1000)
    """)
    
    # Worker A claims with 100ms TTL
    r1 = client.post("/v1/outbox/claim", json={"worker_id": "w_a", "claim_ttl_ms": 100})
    assert r1.json()["count"] == 1
    row_id = r1.json()["rows"][0]["id"]
    
    # Worker B immediately claims -> 0 rows (A holds lock)
    r2 = client.post("/v1/outbox/claim", json={"worker_id": "w_b", "claim_ttl_ms": 100})
    assert r2.json()["count"] == 0
    
    # Wait for TTL expiration
    import time
    time.sleep(0.15)
    
    # Worker B claims -> gets SAME row (this is the real recovery semantic)
    r3 = client.post("/v1/outbox/claim", json={"worker_id": "w_b", "claim_ttl_ms": 100})
    assert r3.json()["count"] == 1
    assert r3.json()["rows"][0]["id"] == row_id
    
    # Verify locked_by swapped
    db_row = db.execute("SELECT locked_by FROM message_outbox WHERE id = ?", (row_id,)).fetchone()
    assert db_row["locked_by"] == "w_b"
