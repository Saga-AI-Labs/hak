"""HAK v1 conformance suite — C1-C18 per spec v0.5.1 §15.

The executable interpretation of the spec (D9/D41): the v1 gate.
Run:  cd service && python3 -m pytest conformance.py -v

Error envelope shape (D26): {"error": {"code": ..., "message": ..., "detail"?}}.
"""

# HAK — inter-agent messaging bus. Copyright (C) 2026 asb (operator seat).
# SPDX-License-Identifier: AGPL-3.0-only
# This file is part of HAK. See LICENSE for the full notice.

from __future__ import annotations

import json
import os
import secrets
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

# Fresh environment per run: temp DB + uploads dir (set before importing hak)
_TMP = tempfile.mkdtemp(prefix="hak_conf_")
os.environ["HAK_DB"] = str(Path(_TMP) / "hak.db")
os.environ["HAK_UPLOADS"] = str(Path(_TMP) / "uploads")

import hak  # noqa: E402

hak.init_db()
from fastapi.testclient import TestClient  # noqa: E402

client = TestClient(hak.app)

SECRET = {}  # seat -> bearer secret


# ---------------------------------------------------------------- helpers

def hdr(tok):
    return {"Authorization": f"Bearer {tok}"}


@pytest.fixture(scope="session", autouse=True)
def operator_token():
    """Operator token via direct insert (bootstrap prints to stdout; we need
    the secret programmatically). Same hash discipline as the real bootstrap."""
    sec = secrets.token_urlsafe(32)
    with hak.write_tx() as con:
        con.execute("INSERT INTO tokens (token_id, seat, token_hash, created_at) VALUES (?,?,?,?)",
                    ("t_conf_operator", "operator", hak.sha256_hex(sec.encode()), hak.now_iso()))
    SECRET["operator"] = sec
    yield sec


def issue_token(seat):
    r = client.post("/v1/tokens", json={"seat": seat}, headers=hdr(SECRET["operator"]))
    assert r.status_code == 201, r.text
    j = r.json()
    SECRET[seat] = j["token"]
    return j


def make_room(name, charter_extra=None):
    charter = {
        "purpose": "conformance",
        "admins": ["operator"],
        "dispatch": {"human": {"consumes": ["*"], "emits": ["chat"]},
                     "agent": {"consumes": ["*"], "emits": ["chat", "status", "task_request",
                                                           "task_result", "artifact_ref",
                                                           "review_verdict", "retraction"]}},
        "claim_policy": {"default_ttl_min": 30, "write_mandatory_for_repo_paths": True,
                         "share_capacities": {"gpu": 2, "file": 2}},
        "attachment_policy": {"max_file_bytes": 1024 * 1024, "max_unreferenced_bytes": None},
    }
    if charter_extra:
        charter.update(charter_extra)
    r = client.post("/v1/rooms", json={"name": name, "charter": charter},
                    headers=hdr(SECRET["operator"]))
    assert r.status_code == 201, r.text
    return r.json()


def join_and_approve(room, seat, tok):
    r = client.post(f"/v1/rooms/{room}/join", headers=hdr(tok))
    assert r.status_code == 200, r.text
    r = client.post(f"/v1/rooms/{room}/members/{seat}/approve", headers=hdr(SECRET["operator"]))
    assert r.status_code == 200, r.text


def post_msg(room, tok, body, **fields):
    payload = {"type": "chat", "body": body}
    payload.update(fields)
    return client.post(f"/v1/rooms/{room}/messages", json=payload, headers=hdr(tok))


def claim(room, tok, resource, kind, units=1, ttl_min=None):
    payload = {"resource_uri": resource, "kind": kind, "units": units}
    if ttl_min is not None:
        payload["ttl_min"] = ttl_min
    return client.post(f"/v1/rooms/{room}/scopes", json=payload, headers=hdr(tok))


def err_code(r):
    return r.json()["error"]["code"]


# ---------------------------------------------------------------- C1 scope conflict

def test_C1_racing_exclusive_claims():
    make_room("c1-room")
    t_a = issue_token("pi-203")["token"]
    t_b = issue_token("pi-50")["token"]
    join_and_approve("c1-room", "pi-203", t_a)
    join_and_approve("c1-room", "pi-50", t_b)
    results = []

    def do(tok):
        results.append(claim("c1-room", tok, "gpu://render-a", "exclusive"))

    th = [threading.Thread(target=do, args=(t,)) for t in (t_a, t_b)]
    [t.start() for t in th]
    [t.join() for t in th]
    codes = sorted(r.status_code for r in results)
    assert codes == [201, 409], f"expected one 201 one 409, got {codes}"
    conf = next(r for r in results if r.status_code == 409)
    assert err_code(conf) == "scope_conflict"
    holder = conf.json()["error"]["detail"]["conflicting"]["holder"]
    assert holder in ("pi-203", "pi-50")
    # exactly one claim event for the resource (loser wrote nothing)
    with hak.db() as con:
        n = con.execute("SELECT COUNT(*) c FROM scope_events WHERE room='c1-room' AND action='claim'").fetchone()["c"]
    assert n == 1


def test_C1_live_claim_conflicts_until_expiry():
    make_room("c1b-room")
    t_a = issue_token("pi-203")["token"]
    join_and_approve("c1b-room", "pi-203", t_a)
    assert claim("c1b-room", t_a, "gpu://x", "exclusive").status_code == 201
    t_b = issue_token("pi-50")["token"]
    join_and_approve("c1b-room", "pi-50", t_b)
    r2 = claim("c1b-room", t_b, "gpu://x", "exclusive")
    assert r2.status_code == 409  # still live
    # force-expire: expired-but-not-lapsed is reclaimable without release (D15)
    with hak.db() as con:
        con.execute("UPDATE scope_events SET expires_at='2000-01-01T00:00:00+00:00' "
                    "WHERE room='c1b-room' AND resource_uri='gpu://x'")
    r3 = claim("c1b-room", t_b, "gpu://x", "exclusive")
    assert r3.status_code == 201


# ---------------------------------------------------------------- C2 self-reclaim + expiry

def test_C2_self_reclaim_refreshes():
    make_room("c2-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c2-room", "pi-203", t)
    r1 = claim("c2-room", t, "file:///repo/a", "write")
    assert r1.status_code == 201
    r2 = claim("c2-room", t, "file:///repo/a", "write")
    assert r2.status_code == 200 and r2.json()["scope_id"] == r1.json()["scope_id"]
    # the refresh appended a renew event; both holder-visible
    with hak.db() as con:
        n = con.execute("SELECT COUNT(*) c FROM scope_events WHERE room='c2-room'").fetchone()["c"]
    assert n == 2


def test_C2_ttl_expiry_frees_resource():
    make_room("c2b-room")
    t_a = issue_token("pi-203")["token"]
    join_and_approve("c2b-room", "pi-203", t_a)
    assert claim("c2b-room", t_a, "gpu://y", "exclusive").status_code == 201
    # force-expire, then another seat claims — no 409 (D15 liveness projection)
    with hak.db() as con:
        con.execute("UPDATE scope_events SET expires_at='2000-01-01T00:00:00+00:00' "
                    "WHERE room='c2b-room' AND resource_uri='gpu://y'")
    t_b = issue_token("pi-50")["token"]
    join_and_approve("c2b-room", "pi-50", t_b)
    r2 = claim("c2b-room", t_b, "gpu://y", "exclusive")
    assert r2.status_code == 201


# ---------------------------------------------------------------- C3 handover

def test_C3_handover_pull():
    make_room("c3-room")
    t_a = issue_token("pi-203")["token"]
    t_b = issue_token("pi-50")["token"]
    join_and_approve("c3-room", "pi-203", t_a)
    join_and_approve("c3-room", "pi-50", t_b)
    r = post_msg("c3-room", t_a, "handover for pi-50", type="artifact_ref",
                 meta={"kind": "handover", "for_seat": "pi-50"},
                 refs=[{"uri": "https://github.com/x/y/commit/a945438", "note": "addendum"}])
    assert r.status_code == 201, r.text
    seq = r.json()["seq"]
    # targeted pull: for_seat=me resolves to the caller server-side (D25/D10)
    r2 = client.get("/v1/rooms/c3-room/messages?meta_kind=handover&for_seat=me",
                    headers=hdr(t_b))
    assert r2.status_code == 200
    assert any(m["seq"] == seq for m in r2.json()["messages"])
    # another seat's pull sees nothing addressed elsewhere
    r3 = client.get("/v1/rooms/c3-room/messages?meta_kind=handover&for_seat=me",
                    headers=hdr(t_a))
    assert not any(m["seq"] == seq for m in r3.json()["messages"])


# ---------------------------------------------------------------- C4 presence

def test_C4_presence_strip_data():
    make_room("c4-room")
    t_a = issue_token("pi-203")["token"]
    t_b = issue_token("pi-50")["token"]
    join_and_approve("c4-room", "pi-203", t_a)
    join_and_approve("c4-room", "pi-50", t_b)
    r = post_msg("c4-room", t_a, "working", type="status",
                 meta={"kind": "status", "state": "working_on", "ref": "file:///repo"})
    assert r.status_code == 201
    r = post_msg("c4-room", t_b, "done", type="status",
                 meta={"kind": "status", "state": "done"})
    assert r.status_code == 201
    # latest status per seat is recorded (done clearing the strip is a UI rule, D43)
    r = client.get("/v1/rooms/c4-room/messages?type=status", headers=hdr(t_a))
    states = {m["from"]["seat"]: m["meta"]["state"] for m in r.json()["messages"]}
    assert states == {"pi-203": "working_on", "pi-50": "done"}


# ---------------------------------------------------------------- C5a/C5b canonical form

def test_C5a_canonical_replay():
    make_room("c5-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c5-room", "pi-203", t)
    ids = []
    for i in range(3):
        r = post_msg("c5-room", t, f"msg {i}",
                     meta={"kind": "status", "state": "working_on"})
        assert r.status_code == 201
        ids.append(r.json()["id"])
    with hak.db() as con:
        rows = con.execute("SELECT * FROM messages WHERE room='c5-room' ORDER BY seq").fetchall()
    for row in rows:
        recomposed = hak.envelope_out(row)
        # canonical serialization is idempotent under JCS (D31): re-serialize
        # the parsed output and it must be byte-identical
        c1 = hak.canonicalize(recomposed)
        c2 = hak.canonicalize(json.loads(c1))
        assert c1 == c2, f"JCS round-trip unstable for {row['id']}"


def test_C5b_projector_rebuild():
    make_room("c5b-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c5b-room", "pi-203", t)
    claim("c5b-room", t, "gpu://z", "exclusive")
    post_msg("c5b-room", t, "hello")
    # the projector is a pure function of the event log + wall clock: replay
    # must yield the identical live set (D2/D15)
    def live_set():
        con = hak.sqlite3.connect(hak.DB_PATH)
        con.row_factory = hak.sqlite3.Row
        try:
            return {r["scope_id"]: r["expires_at"] for r in hak._live_claims(con, "c5b-room", "gpu://z")}
        finally:
            con.close()
    first = live_set()
    second = live_set()  # replay #2
    assert first == second and len(first) == 1


# ---------------------------------------------------------------- C6 idempotent emission (D47)

def test_C6_idempotent_emission():
    make_room("c6-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c6-room", "pi-203", t)
    fields = {"client_msg_id": "pi203-c6-1", "body": "idempotent hello"}
    r1 = post_msg("c6-room", t, **fields)
    assert r1.status_code == 201, r1.text
    r2 = post_msg("c6-room", t, **fields)
    assert r2.status_code == 200 and r2.json()["id"] == r1.json()["id"]  # D47
    # different content, same key -> 409
    r3 = post_msg("c6-room", t, client_msg_id="pi203-c6-1", body="DIFFERENT")
    assert r3.status_code == 409
    # exactly one envelope exists
    with hak.db() as con:
        n = con.execute("SELECT COUNT(*) c FROM messages WHERE room='c6-room' AND client_msg_id='pi203-c6-1'").fetchone()["c"]
    assert n == 1


# ---------------------------------------------------------------- C7 cursor discipline

def test_C7_cursor_crash_replay():
    make_room("c7-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c7-room", "pi-203", t)
    for i in range(5):
        post_msg("c7-room", t, f"batch {i}")
    # seq 1 = room_create, seq 2 = member_approve (D6 audit envelopes); then
    # 5 posts -> seqs 3..7. Client processed 2 (cursor=2), crash, replay -> 3..7
    r = client.get("/v1/rooms/c7-room/messages?since=2", headers=hdr(t))
    msgs = r.json()["messages"]
    assert [m["seq"] for m in msgs] == [3, 4, 5, 6, 7]
    assert r.json()["next_since"] == 7


# ---------------------------------------------------------------- C8 loop guard

def test_C8_loop_guard():
    make_room("c8-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c8-room", "pi-203", t)
    # unmarked response type rejected
    r = post_msg("c8-room", t, "result without marker", type="task_result")
    assert r.status_code == 422 and err_code(r) == "response_marker_required"
    # marked is fine
    r2 = post_msg("c8-room", t, "result with marker", type="task_result",
                  meta={"kind": "response"})
    assert r2.status_code == 201
    # every response-kind message carries the marker (server half of the guard)
    r3 = client.get("/v1/rooms/c8-room/messages?meta_kind=response", headers=hdr(t))
    assert all(m["meta"]["kind"] == "response" for m in r3.json()["messages"])


# ---------------------------------------------------------------- C9 retraction rendering

def test_C9_retraction():
    make_room("c9-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c9-room", "pi-203", t)
    r1 = post_msg("c9-room", t, "original claim")
    assert r1.status_code == 201
    orig_id = r1.json()["id"]
    r2 = post_msg("c9-room", t, "retracting", type="retraction", reply_to=orig_id)
    assert r2.status_code == 201, r2.text
    # original still present (visible-but-marked, D17)
    r3 = client.get(f"/v1/rooms/c9-room/messages/{orig_id}", headers=hdr(t))
    assert r3.status_code == 200
    # retraction envelope renders
    r4 = client.get(f"/v1/rooms/c9-room/messages/{r2.json()['id']}", headers=hdr(t))
    assert r4.status_code == 200


# ---------------------------------------------------------------- C10 server-owned fields

def test_C10_forged_provenance():
    make_room("c10-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c10-room", "pi-203", t)
    # each server-owned field is rejected at the schema edge (D10)
    for forged in [
        {"type": "chat", "body": "x", "seq": 999},
        {"type": "chat", "body": "x", "id": "m_x"},
        {"type": "chat", "body": "x", "ts": "2020-01-01T00:00:00Z"},
        {"type": "chat", "body": "x", "from": {"seat": "pi-50"}},
        {"type": "chat", "body": "x", "room": "other-room"},
        {"type": "chat", "body": "x", "meta": {"kind": "admin-op", "op": "x"}},
    ]:
        r = client.post("/v1/rooms/c10-room/messages", json=forged, headers=hdr(t))
        assert r.status_code == 422, (forged, r.text)
    # nothing was written
    with hak.db() as con:
        n = con.execute("SELECT COUNT(*) c FROM messages WHERE room='c10-room' AND from_seat != 'hak'").fetchone()["c"]
    assert n == 0


# ---------------------------------------------------------------- C11 membership state machine

def test_C11_membership_state_machine():
    make_room("c11-room")
    t = issue_token("quinn")["token"]
    r = client.post("/v1/rooms/c11-room/join", headers=hdr(t))
    assert r.status_code == 200 and r.json()["status"] == "pending"
    # pending: everything but join 403
    r = post_msg("c11-room", t, "not yet")
    assert r.status_code == 403
    # approve -> member
    client.post("/v1/rooms/c11-room/members/quinn/approve", headers=hdr(SECRET["operator"]))
    r = post_msg("c11-room", t, "now a member")
    assert r.status_code == 201
    # revoke -> revoked; the seat's tokens die immediately (D19)
    client.post("/v1/rooms/c11-room/members/quinn/revoke", headers=hdr(SECRET["operator"]))
    r = post_msg("c11-room", t, "revoked")
    assert r.status_code == 401
    # new token: revoked membership -> 403
    t2 = issue_token("quinn")["token"]
    r = post_msg("c11-room", t2, "still revoked membership")
    assert r.status_code == 403
    # rejoin -> new pending; re-approve -> member again
    r = client.post("/v1/rooms/c11-room/join", headers=hdr(t2))
    assert r.json()["status"] == "pending"
    client.post("/v1/rooms/c11-room/members/quinn/approve", headers=hdr(SECRET["operator"]))
    r = post_msg("c11-room", t2, "back")
    assert r.status_code == 201
    # approve of a revoked seat without re-join is 422 (D36)
    client.post("/v1/rooms/c11-room/members/quinn/revoke", headers=hdr(SECRET["operator"]))
    r = client.post("/v1/rooms/c11-room/members/quinn/approve", headers=hdr(SECRET["operator"]))
    assert r.status_code == 422


def test_C11_operator_non_revocable():
    make_room("c11b-room")
    r = client.post("/v1/rooms/c11b-room/members/operator/revoke",
                    headers=hdr(SECRET["operator"]))
    assert r.status_code == 422 and err_code(r) == "operator_non_revocable"


# ---------------------------------------------------------------- C12 file authorization (D29)

def test_C12_file_authorization():
    make_room("c12-room")
    make_room("c12-other")
    t_a = issue_token("pi-203")["token"]
    t_b = issue_token("pi-50")["token"]
    join_and_approve("c12-room", "pi-203", t_a)
    join_and_approve("c12-room", "pi-50", t_b)
    join_and_approve("c12-other", "pi-203", t_a)
    # upload to c12-room
    r = client.post("/v1/files", headers=hdr(t_a), data={"room": "c12-room"},
                    files={"file": ("a.txt", b"hello", "text/plain")})
    assert r.status_code == 201, r.text
    fid = r.json()["file_id"]
    # member of same room downloads fine
    assert client.get(f"/v1/files/{fid}", headers=hdr(t_b)).status_code == 200
    # non-member of the file's room: 403 even with a valid token
    t_c = issue_token("glm-flash")["token"]
    join_and_approve("c12-other", "glm-flash", t_c)
    r3 = client.get(f"/v1/files/{fid}", headers=hdr(t_c))
    assert r3.status_code == 403
    # cross-room reference rejected at POST (D29)
    r4 = post_msg("c12-other", t_a, "cross-room ref", attachments=[{"file_id": fid}])
    assert r4.status_code == 422
    # pending member cannot upload
    t_d = issue_token("kimi")["token"]
    client.post("/v1/rooms/c12-room/join", headers=hdr(t_d))
    r5 = client.post("/v1/files", headers=hdr(t_d), data={"room": "c12-room"},
                     files={"file": ("b.txt", b"x", "text/plain")})
    assert r5.status_code == 403


# ---------------------------------------------------------------- C13 upload/reference/GC race (D30/D44)

def test_C13_gc_and_grace():
    make_room("c13-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c13-room", "pi-203", t)
    # fresh file: within grace, never GC'd
    r = client.post("/v1/files", headers=hdr(t), data={"room": "c13-room"},
                    files={"file": ("fresh.txt", b"fresh", "text/plain")})
    fid_fresh = r.json()["file_id"]
    # aged, unreferenced, past grace: swept (DB-authoritative, D44)
    r = client.post("/v1/files", headers=hdr(t), data={"room": "c13-room"},
                    files={"file": ("old.txt", b"old", "text/plain")})
    fid_old = r.json()["file_id"]
    with hak.db() as con:
        con.execute("UPDATE files SET created_at='2020-01-01T00:00:00+00:00' WHERE file_id=?",
                    (fid_old,))
    stats = hak.sweep_once()
    with hak.db() as con:
        fresh = con.execute("SELECT * FROM files WHERE file_id=?", (fid_fresh,)).fetchone()
        old = con.execute("SELECT * FROM files WHERE file_id=?", (fid_old,)).fetchone()
    assert fresh["deletion_pending"] == 0            # grace (D30)
    assert old["deletion_pending"] == 1             # committed
    assert old["deleted_at"] is not None            # unlink completed
    assert not (hak.UPLOADS_DIR / fid_old).exists()  # bytes gone
    # aged file, still referenced by an envelope: survives
    r = client.post("/v1/files", headers=hdr(t), data={"room": "c13-room"},
                    files={"file": ("kept.txt", b"kept", "text/plain")})
    fid_kept = r.json()["file_id"]
    post_msg("c13-room", t, "this one stays", attachments=[{"file_id": fid_kept}])
    with hak.db() as con:
        con.execute("UPDATE files SET created_at='2020-01-01T00:00:00+00:00' WHERE file_id=?",
                    (fid_kept,))
    hak.sweep_once()
    with hak.db() as con:
        kept = con.execute("SELECT * FROM files WHERE file_id=?", (fid_kept,)).fetchone()
    assert kept["deletion_pending"] == 0            # referenced -> survives (D23)
    # deletion envelope exists in the room (D40 audit trail)
    with hak.db() as con:
        env = con.execute("SELECT * FROM messages WHERE room='c13-room' AND json_extract(meta,'$.op')='attachment_delete'").fetchone()
    assert env is not None
    # post-sweep reference to the deleted file -> 422 (never referenceable, D44)
    r2 = post_msg("c13-room", t, "too late", attachments=[{"file_id": fid_old}])
    assert r2.status_code == 422


# ---------------------------------------------------------------- C14 JCS idempotency equivalence (D31)

def test_C14_jcs_equivalence():
    make_room("c14-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c14-room", "pi-203", t)
    # first POST: keys in one order
    r1 = client.post("/v1/rooms/c14-room/messages", headers=hdr(t), json={
        "type": "chat", "body": "jcs", "client_msg_id": "k1",
        "to": None, "meta": None})
    assert r1.status_code == 201, r1.text
    # retry: different key order, omitted-vs-null optionals — same semantics
    r2 = client.post("/v1/rooms/c14-room/messages", headers=hdr(t), json={
        "client_msg_id": "k1", "meta": None, "body": "jcs", "type": "chat"})
    assert r2.status_code == 200 and r2.json()["id"] == r1.json()["id"]
    # semantic change -> 409 (a loud conflict, never a silent duplicate)
    r3 = client.post("/v1/rooms/c14-room/messages", headers=hdr(t), json={
        "client_msg_id": "k1", "meta": None, "body": "jcs v2", "type": "chat"})
    assert r3.status_code == 409


# ---------------------------------------------------------------- C15 pagination (D33/D35)

def test_C15_pagination():
    make_room("c15-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c15-room", "pi-203", t)
    for i in range(5):
        post_msg("c15-room", t, f"m{i}")
    # seq 1 = admin envelope; posts are 2..6. since EXCLUSIVE, until INCLUSIVE
    r = client.get("/v1/rooms/c15-room/messages?since=1&until=3", headers=hdr(t))
    assert [m["seq"] for m in r.json()["messages"]] == [2, 3]
    r = client.get("/v1/rooms/c15-room/messages?since=1&until=3&order=desc", headers=hdr(t))
    assert [m["seq"] for m in r.json()["messages"]] == [3, 2]
    r = client.get("/v1/rooms/c15-room/messages?since=9999", headers=hdr(t))
    assert r.status_code == 200 and r.json()["messages"] == []
    # limit + continuation: since=0 limit=2 -> 1,2; next_since=2 -> 3,4
    r = client.get("/v1/rooms/c15-room/messages?limit=2", headers=hdr(t))
    body = r.json()
    assert [m["seq"] for m in body["messages"]] == [1, 2] and body["has_more"]
    r = client.get(f"/v1/rooms/c15-room/messages?since={body['next_since']}&limit=2",
                   headers=hdr(t))
    body = r.json()
    assert [m["seq"] for m in body["messages"]] == [3, 4] and body["has_more"]
    # scope history cursor mirrors the messages cursor (D35)
    claim("c15-room", t, "gpu://p15", "exclusive")
    r = client.get("/v1/rooms/c15-room/scopes?history=1&since=0", headers=hdr(t))
    assert [e["scope_seq"] for e in r.json()["events"]] == [1]


# ---------------------------------------------------------------- C16 renew liveness (D34)

def test_C16_renew_liveness():
    make_room("c16-room")
    t = issue_token("pi-203")["token"]
    join_and_approve("c16-room", "pi-203", t)
    r = claim("c16-room", t, "gpu://r16", "exclusive")
    sid = r.json()["scope_id"]
    t_b = issue_token("pi-50")["token"]
    join_and_approve("c16-room", "pi-50", t_b)
    # wrong seat -> 403
    r2 = client.post(f"/v1/rooms/c16-room/scopes/{sid}/renew", headers=hdr(t_b))
    assert r2.status_code == 403
    # holder renews fine
    r3 = client.post(f"/v1/rooms/c16-room/scopes/{sid}/renew", headers=hdr(t))
    assert r3.status_code == 200
    # force-expire; renew -> 404 AND no event appended (D34)
    with hak.db() as con:
        con.execute("UPDATE scope_events SET expires_at='2000-01-01T00:00:00+00:00' "
                    "WHERE scope_id=? AND action IN ('claim','renew')", (sid,))
    with hak.db() as con:
        before = con.execute("SELECT COUNT(*) c FROM scope_events WHERE room='c16-room'").fetchone()["c"]
    r4 = client.post(f"/v1/rooms/c16-room/scopes/{sid}/renew", headers=hdr(t))
    assert r4.status_code == 404
    with hak.db() as con:
        after = con.execute("SELECT COUNT(*) c FROM scope_events WHERE room='c16-room'").fetchone()["c"]
    assert after == before  # no renew event for a not-live claim


# ---------------------------------------------------------------- C17 retraction authorization (D37)

def test_C17_retraction_authorization():
    make_room("c17-room")
    t_a = issue_token("pi-203")["token"]
    t_b = issue_token("pi-50")["token"]
    join_and_approve("c17-room", "pi-203", t_a)
    join_and_approve("c17-room", "pi-50", t_b)
    orig = post_msg("c17-room", t_a, "original").json()
    # non-author cannot retract
    r = post_msg("c17-room", t_b, "not mine", type="retraction", reply_to=orig["id"])
    assert r.status_code == 403
    # admin (operator) may retract any
    r2 = post_msg("c17-room", SECRET["operator"], "admin retraction", type="retraction",
                  reply_to=orig["id"])
    assert r2.status_code == 201
    # retraction-of-retraction rejected
    r3 = post_msg("c17-room", t_a, "no un-retract", type="retraction",
                  reply_to=r2.json()["id"])
    assert r3.status_code == 422
    # duplicate retraction of the same target -> 409 with pointer
    r4 = post_msg("c17-room", t_a, "dup", type="retraction", reply_to=orig["id"])
    assert r4.status_code == 409
    assert r4.json()["error"]["detail"]["effective_retraction"] == r2.json()["id"]


# ---------------------------------------------------------------- C18 admin invariants (D32/D40/D44/D45)

def test_C18_admin_invariants():
    r = make_room("c18-room")
    assert "operator" in r["charter"]["admins"]  # D32 invariant
    # admin authority requires member status (D45): revoke the creator, then
    # the creator (still listed in charter.admins) cannot administer
    t_a = issue_token("pi-203")["token"]
    join_and_approve("c18-room", "pi-203", t_a)
    # make pi-203 an admin via charter at creation:
    r_charter = make_room("c18b-room", charter_extra={"admins": ["operator", "pi-203"]})
    join_and_approve("c18b-room", "pi-203", t_a)
    join_and_approve("c18b-room", "pi-50", issue_token("pi-50")["token"])
    # pi-203 (admin, member) approves fine
    r = client.post("/v1/rooms/c18b-room/members/pi-50/approve", headers=hdr(t_a))
    assert r.status_code == 200
    # revoke pi-203's membership; charter still lists them as admin.
    # their old tokens died with the revoke (D19); a FRESH token shows the
    # D45 gate cleanly: valid token, revoked membership -> 403
    client.post("/v1/rooms/c18b-room/members/pi-203/revoke", headers=hdr(SECRET["operator"]))
    client.post("/v1/rooms/c18b-room/join", headers=hdr(issue_token("kimi")["token"]))
    t_a2 = issue_token("pi-203")["token"]
    r = client.post("/v1/rooms/c18b-room/members/kimi/approve", headers=hdr(t_a2))
    assert r.status_code == 403  # admin authority gated on member status (D45)
    # every mutation + its admin envelope committed together (D40)
    with hak.db() as con:
        row = con.execute("SELECT status FROM memberships WHERE room='c18b-room' AND seat='pi-203'").fetchone()
        env = con.execute("SELECT id FROM messages WHERE room='c18b-room' AND json_extract(meta,'$.op')='member_revoke' AND json_extract(meta,'$.target')='pi-203'").fetchone()
    assert row["status"] == "revoked" and env is not None


# ---------------------------------------------------------------- spec extras

def test_admin_op_client_rejected():
    make_room("x-admin-op")
    t = issue_token("pi-203")["token"]
    join_and_approve("x-admin-op", "pi-203", t)
    r = post_msg("x-admin-op", t, "fake admin op", meta={"kind": "admin-op", "op": "x"})
    assert r.status_code == 422


def test_share_capacity_exhaustion():
    make_room("x-share")
    t_a = issue_token("pi-203")["token"]
    t_b = issue_token("pi-50")["token"]
    join_and_approve("x-share", "pi-203", t_a)
    join_and_approve("x-share", "pi-50", t_b)
    # charter share_capacities: gpu: 2
    assert claim("x-share", t_a, "gpu://cap", "share", units=1).status_code == 201
    assert claim("x-share", t_b, "gpu://cap", "share", units=1).status_code == 201
    # same seat re-claim is a refresh (D16) — stays within capacity
    assert claim("x-share", t_a, "gpu://cap", "share", units=1).status_code == 200
    # a third seat exceeds the total -> 409
    t_c = issue_token("glm-flash")["token"]
    join_and_approve("x-share", "glm-flash", t_c)
    r = claim("x-share", t_c, "gpu://cap", "share", units=1)
    assert r.status_code == 409
    assert r.json()["error"]["detail"]["current_units"] == 2
    # capacity is per (room, resource) — same scheme, different URI (D34)
    assert claim("x-share", t_a, "gpu://cap2", "share", units=1).status_code == 201
    assert claim("x-share", t_b, "gpu://capX", "share", units=1).status_code == 201


def test_share_self_refresh_respects_capacity():
    make_room("x-share2")
    t = issue_token("pi-203")["token"]
    join_and_approve("x-share2", "pi-203", t)
    assert claim("x-share2", t, "gpu://r", "share", units=1).status_code == 201
    # refresh to units=2 fills capacity exactly
    r = claim("x-share2", t, "gpu://r", "share", units=2)
    assert r.status_code == 200
    # refresh to units=3 exceeds -> 409 (no bypass via self-refresh)
    r2 = claim("x-share2", t, "gpu://r", "share", units=3)
    assert r2.status_code == 409


def test_ttl_clamped_to_charter():
    make_room("x-ttl")
    t = issue_token("pi-203")["token"]
    join_and_approve("x-ttl", "pi-203", t)
    # charter default_ttl_min=30; request 60 -> clamped to 30 (Fix B/D28)
    import hak as _h
    from datetime import datetime, timedelta, timezone
    before = datetime.now(timezone.utc)
    r = claim("x-ttl", t, "gpu://ttl", "exclusive", ttl_min=60)
    assert r.status_code == 201
    exp = datetime.fromisoformat(r.json()["expires_at"])
    assert exp < before + timedelta(minutes=31)
    assert exp > before + timedelta(minutes=29)
    # request above hard max -> 422
    r2 = claim("x-ttl", t, "gpu://ttl2", "exclusive", ttl_min=9999)
    assert r2.status_code == 422


def test_body_cap_413():
    make_room("x-body")
    t = issue_token("pi-203")["token"]
    join_and_approve("x-body", "pi-203", t)
    big = "x" * (64 * 1024 + 1)
    r = client.post("/v1/rooms/x-body/messages", headers=hdr(t),
                    json={"type": "chat", "body": big})
    assert r.status_code == 413 and err_code(r) == "body_too_large"
    # exactly at cap is fine
    r2 = client.post("/v1/rooms/x-body/messages", headers=hdr(t),
                     json={"type": "chat", "body": "x" * (64 * 1024)})
    assert r2.status_code == 201


def test_file_cap_413():
    make_room("x-fcap")
    t = issue_token("pi-203")["token"]
    join_and_approve("x-fcap", "pi-203", t)
    big = b"z" * (1024 * 1024 + 1)  # charter max_file_bytes = 1 MiB
    r = client.post("/v1/files", headers=hdr(t), data={"room": "x-fcap"},
                    files={"file": ("big.bin", big, "application/octet-stream")})
    assert r.status_code == 413 and err_code(r) == "file_too_large"


def test_scope_history_pagination_d35():
    make_room("x-hist")
    t = issue_token("pi-203")["token"]
    join_and_approve("x-hist", "pi-203", t)
    claim("x-hist", t, "gpu://h1", "exclusive")
    claim("x-hist", t, "gpu://h2", "exclusive")
    r = client.get("/v1/rooms/x-hist/scopes?history=1&since=0", headers=hdr(t))
    assert [e["scope_seq"] for e in r.json()["events"]] == [1, 2]
    r2 = client.get("/v1/rooms/x-hist/scopes?history=1&since=1", headers=hdr(t))
    assert [e["scope_seq"] for e in r2.json()["events"]] == [2]


def test_health_and_error_envelope():
    r = client.get("/v1/health", headers=hdr(SECRET["operator"]))
    assert r.status_code == 200 and r.json() == {"status": "ok", "service": "hak", "version": "v1"}
    assert client.get("/v1/health").status_code == 401
    # D26 shape on a semantic error
    t = issue_token("pi-203")["token"]
    make_room("x-err")
    join_and_approve("x-err", "pi-203", t)
    r = post_msg("x-err", t, "ok", type="bogus")
    body = r.json()["error"]
    assert body["code"] == "invalid_type"
    assert "message" in body


def test_room_admin_only_creation():
    # after the first room exists, non-admin seats cannot create rooms (Q5)
    t = issue_token("stranger")["token"]
    r = client.post("/v1/rooms", json={"name": "strangers-room", "charter": {
        "purpose": "x", "admins": ["stranger"]}},
        headers=hdr(t))
    assert r.status_code == 403
    # admin can create; creator is inserted into admins (D32)
    r2 = client.post("/v1/rooms", json={"name": "admin-made", "charter": {
        "purpose": "x", "admins": ["operator"]}},
        headers=hdr(SECRET["operator"]))
    assert r2.status_code == 201
    assert "operator" in r2.json()["charter"]["admins"]


def test_conflict_matrix_symmetry_d34():
    # share vs write is 409 in BOTH directions (Kimi K3 / D34)
    make_room("x-sym")
    t_a = issue_token("pi-203")["token"]
    t_b = issue_token("pi-50")["token"]
    join_and_approve("x-sym", "pi-203", t_a)
    join_and_approve("x-sym", "pi-50", t_b)
    # live share blocks a new write
    assert claim("x-sym", t_a, "gpu://sym", "share").status_code == 201
    r = claim("x-sym", t_b, "gpu://sym", "write")
    assert r.status_code == 409 and r.json()["error"]["detail"]["conflicting"]["kind"] == "share"
    # and the other direction: live write blocks a new share
    assert claim("x-sym", t_b, "gpu://sym2", "write").status_code == 201
    r2 = claim("x-sym", t_a, "gpu://sym2", "share")
    assert r2.status_code == 409 and r2.json()["error"]["detail"]["conflicting"]["kind"] == "write"
    # read-exclusive vs share, both directions
    assert claim("x-sym", t_a, "gpu://sym3", "read-exclusive").status_code == 201
    r3 = claim("x-sym", t_b, "gpu://sym3", "share")
    assert r3.status_code == 409 and r3.json()["error"]["detail"]["conflicting"]["kind"] == "read-exclusive"


def test_attachment_element_shapes():
    # regression: attachments as a list of STRINGS used to 500 deep in
    # _validate_envelope (AttributeError on .get). Valid schema must never 500.
    make_room("x-att-shapes")
    t = issue_token("pi-203")["token"]
    join_and_approve("x-att-shapes", "pi-203", t)
    # upload a real file so the happy path is testable
    r = client.post("/v1/files", headers=hdr(t), data={"room": "x-att-shapes"},
                    files={"file": ("a.txt", b"x", "text/plain")})
    fid = r.json()["file_id"]
    # (1) string shorthand is LEGAL and stored as {file_id: ...}
    r1 = post_msg("x-att-shapes", t, "shorthand", attachments=[fid])
    assert r1.status_code == 201, r1.text
    stored = client.get(f"/v1/rooms/x-att-shapes/messages/{r1.json()['id']}",
                        headers=hdr(t)).json()
    assert stored["attachments"] == [{"file_id": fid}], stored["attachments"]
    # (2) dict form still works
    r2 = post_msg("x-att-shapes", t, "dict form", attachments=[{"file_id": fid, "name": "a.txt"}])
    assert r2.status_code == 201
    # (3) malformed elements -> 422 with a NAMED field, never a 500
    for bad in (["not-a-file-id-at-all"], [42], [None], ["f_ok", 42], [{"nope": 1}], [True]):
        rb = post_msg("x-att-shapes", t, "bad", attachments=bad)
        assert rb.status_code == 422, (bad, rb.status_code, rb.text[:200])
    # (4) refs element shapes: string shorthand legal, garbage 422
    r3 = post_msg("x-att-shapes", t, "ref shorthand", refs=["https://example.com/x"])
    assert r3.status_code == 201, r3.text
    r4 = post_msg("x-att-shapes", t, "ref dict", refs=[{"uri": "https://example.com/y", "note": "n"}])
    assert r4.status_code == 201
    for bad in ([{"nope": 1}], [5], [["nested"]]):
        rb = post_msg("x-att-shapes", t, "bad ref", refs=bad)
        assert rb.status_code == 422, (bad, rb.status_code)


def test_dm_transparency():
    # Q2: transparent DMs — to.seat is a filter hint, never an access barrier
    make_room("x-dm")
    t_a = issue_token("pi-203")["token"]
    t_b = issue_token("pi-50")["token"]
    join_and_approve("x-dm", "pi-203", t_a)
    join_and_approve("x-dm", "pi-50", t_b)
    r = post_msg("x-dm", t_a, "psst", to={"seat": "pi-50"})
    assert r.status_code == 201
    # any member can still read it (transparency, Q2/D25)
    r2 = client.get("/v1/rooms/x-dm/messages", headers=hdr(t_b))
    assert any(m["body"] == "psst" for m in r2.json()["messages"])
    r3 = client.get("/v1/rooms/x-dm/messages?to=pi-50", headers=hdr(t_a))
    assert any(m["body"] == "psst" for m in r3.json()["messages"])


# ---------- v2 conformance: C19-C26 (spec rev 2, ratified V7) ----------

def _enable_wake(room):
    r = client.post(f"/v1/rooms/{room}/charter", headers=hdr(SECRET["operator"]),
                    json={"patch": {"wake_hooks": {"enabled": True,
                          "policy": "woken turns: read/analyze/report only; no repo writes, "
                                    "training, or push without a human decision; status envelope "
                                    "naming the triggering seq (F15)"}}})
    assert r.status_code == 200, r.text


def _b64(u):
    import base64
    return base64.urlsafe_b64encode(u.encode()).decode().rstrip("=")


def test_C19_registry_crud_and_audit():
    make_room("v2-reg")
    t = issue_token("pi-203")["token"]
    join_and_approve("v2-reg", "pi-203", t)
    # non-admin cannot register
    r = client.post("/v1/rooms/v2-reg/assets", headers=hdr(t), json={
        "asset_uri": "gpu://rtx4090", "kind": "gpu", "capacity": 1})
    assert r.status_code == 403
    # admin registers
    r = client.post("/v1/rooms/v2-reg/assets", headers=hdr(SECRET["operator"]), json={
        "asset_uri": "gpu://rtx4090", "kind": "gpu", "capacity": 1,
        "access": [{"seat": "pi-203", "level": "use"}],
        "facts": {"model": "RTX 4090", "vram_gb": 24, "host": "host://ai"},
        "notes": "single unit, exclusive"})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["kind"] == "gpu" and body["capacity"] == 1 and body["asset_seq"] == 1
    # idempotent re-register with identical facts -> 200
    r2 = client.post("/v1/rooms/v2-reg/assets", headers=hdr(SECRET["operator"]), json={
        "asset_uri": "gpu://rtx4090", "kind": "gpu", "capacity": 1,
        "access": [{"seat": "pi-203", "level": "use"}],
        "facts": {"model": "RTX 4090", "vram_gb": 24, "host": "host://ai"},
        "notes": "single unit, exclusive"})
    assert r2.status_code == 200 and r2.json()["asset_seq"] == 1
    # conflicting facts -> 409
    r3 = client.post("/v1/rooms/v2-reg/assets", headers=hdr(SECRET["operator"]), json={
        "asset_uri": "gpu://rtx4090", "kind": "gpu", "capacity": 1, "facts": {"model": "A100"}})
    assert r3.status_code == 409 and err_code(r3) == "asset_conflict"
    # member can list; audit envelope exists
    rl = client.get("/v1/rooms/v2-reg/assets", headers=hdr(t))
    assert rl.status_code == 200 and len(rl.json()["assets"]) == 1
    with hak.db() as con:
        env = con.execute("SELECT 1 FROM messages WHERE room='v2-reg' "
                          "AND json_extract(meta,'$.op')='asset_register'").fetchone()
    assert env is not None
    # retire is a tombstone
    rd = client.delete(f"/v1/rooms/v2-reg/assets/{_b64('gpu://rtx4090')}",
                       headers=hdr(SECRET["operator"]))
    assert rd.status_code == 200
    rl2 = client.get("/v1/rooms/v2-reg/assets", headers=hdr(t))
    assert rl2.json()["assets"] == []


def test_C20_credential_collision_alias_key():
    """F1: collision = same (location, alias) + different fingerprint.
    Same location, DIFFERENT aliases is NOT a collision."""
    make_room("v2-cred")
    T = SECRET["operator"]
    # two aliases at one location -> no collision
    for alias, fp in (("github", "SHA256:aaa"), ("gx10", "SHA256:bbb")):
        r = client.post("/v1/rooms/v2-cred/assets", headers=hdr(T), json={
            "asset_uri": f"cred://host/.ssh/{alias}", "kind": "credential",
            "facts": {"kind": "ssh", "location": "/root/.ssh/config", "alias": alias,
                      "fingerprint": fp, "owner": "operator"}})
        assert r.status_code == 201, r.text
    la = client.get("/v1/rooms/v2-cred/assets?kind=credential", headers=hdr(T)).json()
    assert la["alerts"] == [], la["alerts"]
    # same (location, alias), different fingerprint -> collision
    r = client.post("/v1/rooms/v2-cred/assets", headers=hdr(T), json={
        "asset_uri": "cred://host/.ssh/github2", "kind": "credential",
        "facts": {"kind": "ssh", "location": "/root/.ssh/config", "alias": "github",
                  "fingerprint": "SHA256:ccc", "owner": "pi-50"}})
    assert r.status_code == 201
    la2 = client.get("/v1/rooms/v2-cred/assets?kind=credential", headers=hdr(T)).json()
    collisions = [a for a in la2["alerts"] if a["kind"] == "collision"]
    assert len(collisions) == 1 and collisions[0]["alias"] == "github"
    assert set(collisions[0]["fingerprints"]) == {"SHA256:aaa", "SHA256:ccc"}
    # secret material is rejected loudly (D49.3)
    rbad = client.post("/v1/rooms/v2-cred/assets", headers=hdr(T), json={
        "asset_uri": "cred://host/.ssh/leaky", "kind": "credential",
        "facts": {"fingerprint": "SHA256:x", "location": "/l", "private_key": "-----BEGIN"}})
    assert rbad.status_code == 422 and err_code(rbad) == "credential_material_rejected"


def test_C20b_credential_missing_state():
    """F1/#113: destroy-by-absence. An explicit admin verify derives MISSING."""
    make_room("v2-miss")
    T = SECRET["operator"]
    client.post("/v1/rooms/v2-miss/assets", headers=hdr(T), json={
        "asset_uri": "cred://host/.ssh/alpha", "kind": "credential",
        "facts": {"fingerprint": "SHA256:a", "location": "/l", "alias": "alpha"}})
    client.post("/v1/rooms/v2-miss/assets", headers=hdr(T), json={
        "asset_uri": "cred://host/.ssh/beta", "kind": "credential",
        "facts": {"fingerprint": "SHA256:b", "location": "/l", "alias": "beta"}})
    # verify: alpha present, beta GONE (the #113 case)
    rv = client.post("/v1/rooms/v2-miss/assets/verify", headers=hdr(T),
                     json={"present": ["cred://host/.ssh/alpha"], "detail": "post-rebuild scan"})
    assert rv.status_code == 200 and rv.json()["missing"] == ["cred://host/.ssh/beta"]
    la = client.get("/v1/rooms/v2-miss/assets?kind=credential", headers=hdr(T)).json()
    missing = [a for a in la["alerts"] if a["kind"] == "missing"]
    assert len(missing) == 1 and missing[0]["asset_uri"] == "cred://host/.ssh/beta"
    # non-admin cannot verify
    t = issue_token("pi-203")["token"]
    join_and_approve("v2-miss", "pi-203", t)
    assert client.post("/v1/rooms/v2-miss/assets/verify", headers=hdr(t),
                       json={"present": []}).status_code == 403


def test_C21_assets_scopes_and_identity():
    """F4/F8/F3: registry anchors scope capacity; identity reuses D34;
    path component is case-sensitive."""
    make_room("v2-link")
    T = SECRET["operator"]
    t = issue_token("pi-50")["token"]
    join_and_approve("v2-link", "pi-50", t)
    # unregistered asset: warn-not-block (advisory, D54)
    r0 = claim("v2-link", t, "gpu://rtx4090", "exclusive")
    assert r0.status_code == 201
    assert client.delete(f"/v1/rooms/v2-link/scopes/{r0.json()['scope_id']}",
                         headers=hdr(t)).status_code == 204
    # register capacity 1 -> single exclusive claim; second is 409 (existing D22 path)
    client.post("/v1/rooms/v2-link/assets", headers=hdr(T), json={
        "asset_uri": "gpu://rtx4090", "kind": "gpu", "capacity": 1})
    t2 = issue_token("pi-203")["token"]
    join_and_approve("v2-link", "pi-203", t2)
    assert claim("v2-link", t, "gpu://rtx4090", "exclusive").status_code == 201
    r2 = claim("v2-link", t2, "gpu://rtx4090", "exclusive")
    assert r2.status_code == 409
    # identity: case-sensitive path (F3), scheme normalized (F8)
    client.post("/v1/rooms/v2-link/assets", headers=hdr(T), json={
        "asset_uri": "repo://Saga-AI-Labs/hak", "kind": "repo",
        "facts": {"remote": "https://github.com/Saga-AI-Labs/hak"}})
    client.post("/v1/rooms/v2-link/assets", headers=hdr(T), json={
        "asset_uri": "REPO://saga-ai-labs/hak", "kind": "repo",
        "facts": {"remote": "https://github.com/saga-ai-labs/hak"}})
    names = [a["asset_uri"] for a in client.get("/v1/rooms/v2-link/assets?kind=repo",
                                                headers=hdr(T)).json()["assets"]]
    assert "repo://Saga-AI-Labs/hak" in names and "repo://saga-ai-labs/hak" in names, names
    # canonical file form enforced
    rbad = client.post("/v1/rooms/v2-link/assets", headers=hdr(T), json={
        "asset_uri": "file://tmp/x", "kind": "artifact"})
    assert rbad.status_code == 422 and err_code(rbad) == "invalid_asset_uri"


def test_C22_C25_wake_hooks_hardened():
    """D50 + C22/C25: filters, metadata-only, has_attachment, self-post
    suppression, HMAC over ts+nonce+body, disable payload names the hole."""
    import hashlib as _h, hmac as _hmac, threading as _th
    from http.server import BaseHTTPRequestHandler, HTTPServer

    received = []
    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(n).decode()
            received.append((body, self.headers.get("X-HAK-Timestamp"),
                             self.headers.get("X-HAK-Nonce"),
                             self.headers.get("X-HAK-Signature")))
            self.send_response(200); self.end_headers()
        def log_message(self, *a): pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    _th.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_port}/wake"

    make_room("v2-wake")
    _enable_wake("v2-wake")
    t = issue_token("pi-50")["token"]
    join_and_approve("v2-wake", "pi-50", t)
    # wake-hooks disabled in another room -> 409
    make_room("v2-nowake")
    join_and_approve("v2-nowake", "pi-50", t)
    rdis = client.post("/v1/rooms/v2-nowake/subscriptions", headers=hdr(t),
                       json={"url": url, "filter": {}})
    assert rdis.status_code == 409 and err_code(rdis) == "wake_hooks_disabled"
    # subscribe on task_request
    rsub = client.post("/v1/rooms/v2-wake/subscriptions", headers=hdr(t),
                       json={"url": url, "filter": {"type": "task_request"}})
    assert rsub.status_code == 201, rsub.text
    secret = rsub.json()["secret"]
    # a chat must NOT wake; a task_request to pi-50 MUST
    post_msg("v2-wake", t, "just chatting")
    assert hak.deliver_pending_wakes()["delivered"] == 0
    rq = post_msg("v2-wake", SECRET["operator"], "work for you", type="task_request")
    assert rq.status_code == 201
    st = hak.deliver_pending_wakes()
    assert st["delivered"] == 1, st
    body, ts, nonce, sig = received[0]
    payload = json.loads(body)
    # metadata only: no body text, but has_attachment present (F14)
    assert "body" not in payload and payload["has_attachment"] is False
    assert payload["type"] == "task_request" and payload["seq"] == rq.json()["seq"]
    # HMAC base is ts.nonce.body (F5)
    expect = _hmac.new(secret.encode(), f"{ts}.{nonce}.{body}".encode(), _h.sha256).hexdigest()
    assert sig == "sha256=" + expect
    assert nonce and ts
    # self-post never wakes (F9)
    before = len(received)
    post_msg("v2-wake", t, "my own request", type="task_request")
    assert hak.deliver_pending_wakes()["delivered"] == 0
    assert len(received) == before
    # for_seat filter
    rsub2 = client.post("/v1/rooms/v2-wake/subscriptions", headers=hdr(t),
                        json={"url": url, "filter": {"for_seat": "pi-50"}})
    assert rsub2.status_code == 201
    post_msg("v2-wake", SECRET["operator"], "handover", type="artifact_ref",
             meta={"kind": "handover", "for_seat": "pi-50"})
    assert hak.deliver_pending_wakes()["delivered"] == 1
    assert json.loads(received[-1][0])["for_seat"] == "pi-50"
    # invalid filter key -> 422 (F7: no query language)
    rbad = client.post("/v1/rooms/v2-wake/subscriptions", headers=hdr(t),
                       json={"url": url, "filter": {"regex": ".*"}})
    assert rbad.status_code == 422 and err_code(rbad) == "invalid_wake_filter"
    # failing endpoint -> disabled after N attempts with the F10 payload
    rsub3 = client.post("/v1/rooms/v2-wake/subscriptions", headers=hdr(t),
                        json={"url": "http://127.0.0.1:9/dead", "filter": {"type": "review_verdict"}})
    sub3 = rsub3.json()["sub_id"]
    post_msg("v2-wake", SECRET["operator"], "a verdict", type="review_verdict",
             meta={"kind": "publication"})
    base = datetime.now(timezone.utc)
    for i in range(hak.WAKE_MAX_ATTEMPTS + 1):
        hak.deliver_pending_wakes(now=(base + timedelta(seconds=60 * i)).isoformat())
    with hak.db() as con:
        s = con.execute("SELECT * FROM subscriptions WHERE sub_id=?", (sub3,)).fetchone()
        env = con.execute("SELECT body FROM messages WHERE room='v2-wake' "
                          "AND json_extract(meta,'$.op')='subscription_disabled'").fetchone()
    assert s["disabled_at"] is not None
    assert s["first_undelivered_seq"] is not None
    assert env is not None and "first_undelivered_seq" in env["body"]
    srv.shutdown()


def test_C23_publication_matrix():
    """F16/F17/F18: the four-row D52 matrix."""
    make_room("v2-pub")
    t = issue_token("pi-203")["token"]
    join_and_approve("v2-pub", "pi-203", t)
    # unsolicited publication
    r1 = post_msg("v2-pub", t, "standalone review", type="review_verdict",
                  meta={"kind": "publication"})
    assert r1.status_code == 201, r1.text
    # solicited answer
    req = post_msg("v2-pub", t, "please review", type="task_request")
    r2 = post_msg("v2-pub", t, "answering", type="review_verdict", reply_to=req.json()["id"],
                  meta={"kind": "response"})
    assert r2.status_code == 201
    # responds AND publishes: publication + reply_to -> 201 (F16/F17)
    r3 = post_msg("v2-pub", t, "answer that is also an artifact", type="review_verdict",
                  reply_to=req.json()["id"], meta={"kind": "publication"})
    assert r3.status_code == 201, r3.text
    # retraction carrying publication kind (F18)
    r4 = post_msg("v2-pub", t, "retracting my finding", type="retraction",
                  reply_to=r1.json()["id"], meta={"kind": "publication"})
    assert r4.status_code == 201, r4.text
    # absent/out-of-set kind -> 422
    r5 = post_msg("v2-pub", t, "no kind", type="review_verdict")
    assert r5.status_code == 422
    r6 = post_msg("v2-pub", t, "bad kind", type="review_verdict", meta={"kind": "verdict"})
    assert r6.status_code == 422


def test_C24_body_format_and_markdown_safety():
    """D51 + F19: stored raw, round-tripped, JCS-stable; the server never renders."""
    make_room("v2-md")
    t = issue_token("pi-203")["token"]
    join_and_approve("v2-md", "pi-203", t)
    md = "# Heading\n\n`k_sparse_ratio` and `/media/data/coding/hak` and **bold**\n\n```\nfoo_bar_baz\n```\n"
    r = post_msg("v2-md", t, md, type="chat", body_format="markdown")
    assert r.status_code == 201, r.text
    got = client.get(f"/v1/rooms/v2-md/messages/{r.json()['id']}", headers=hdr(t)).json()
    assert got["body_format"] == "markdown"
    assert got["body"] == md                     # byte-for-byte, never rendered server-side
    # default is text
    r2 = post_msg("v2-md", t, "plain")
    g2 = client.get(f"/v1/rooms/v2-md/messages/{r2.json()['id']}", headers=hdr(t)).json()
    assert g2["body_format"] == "text"
    # invalid format -> 422
    r3 = post_msg("v2-md", t, "x", body_format="html")
    assert r3.status_code == 422
    # JCS: retry of the same markdown is idempotent (body_format is hashed)
    r4 = post_msg("v2-md", t, md, type="chat", body_format="markdown", client_msg_id=None)
    assert r4.status_code == 201                     # new key, new message — but hashable
    # rendering contract is client-side; assert the hazard strings survive the API
    assert "k_sparse_ratio" in got["body"] and "foo_bar_baz" in got["body"]


def test_wake_filter_validation_and_wildcards():
    """Regression for bdh-cl #271: `{"type":"any"}` was accepted and silently
    never fired. Values that can never match are now a 422; '*' is a wildcard
    for every filter key; empty filter means all."""
    make_room("v2-wf")
    _enable_wake("v2-wf")
    t = issue_token("pi-203")["token"]
    join_and_approve("v2-wf", "pi-203", t)
    U = "http://127.0.0.1:9/nowhere"
    # impossible values -> 422 (loud), not a subscription that never rings
    for bad in ({"type": "any"}, {"type": "text"}, {"meta_kind": "verdict"},
                {"sender": ""}, {"nope": "x"}, {"type": 5}):
        r = client.post("/v1/rooms/v2-wf/subscriptions", headers=hdr(t),
                        json={"url": U, "filter": bad})
        assert r.status_code == 422 and err_code(r) == "invalid_wake_filter", (bad, r.status_code, r.text[:160])
        assert "empty filter" in r.json()["error"]["message"].lower() or \
               "wildcard" in r.json()["error"]["message"].lower() or \
               "must be" in r.json()["error"]["message"].lower(), r.json()["error"]["message"]
    # legal forms accepted: empty, '*', specific
    for ok in ({}, {"type": "*"}, {"meta_kind": "publication"}, {"sender": "*"},
               {"for_seat": "pi-203", "type": "artifact_ref"}):
        r = client.post("/v1/rooms/v2-wf/subscriptions", headers=hdr(t),
                        json={"url": U, "filter": ok})
        assert r.status_code == 201, (ok, r.text[:160])


def test_wake_wildcard_actually_matches():
    """'*' must match, not merely validate."""
    make_room("v2-ww")
    _enable_wake("v2-ww")
    t = issue_token("pi-50")["token"]
    join_and_approve("v2-ww", "pi-50", t)
    # wildcard type: a chat (from another seat) wakes it
    rsub = client.post("/v1/rooms/v2-ww/subscriptions", headers=hdr(t),
                       json={"url": "http://127.0.0.1:9/x", "filter": {"type": "*"}})
    assert rsub.status_code == 201
    assert client.delete(f"/v1/rooms/v2-ww/subscriptions/{rsub.json()['sub_id']}",
                         headers=hdr(t)).status_code == 200
    # sender wildcard + specific meta_kind
    rsub2 = client.post("/v1/rooms/v2-ww/subscriptions", headers=hdr(t),
                        json={"url": "http://127.0.0.1:9/x",
                              "filter": {"sender": "*", "meta_kind": "handover"}})
    sid = rsub2.json()["sub_id"]
    # A wake ATTEMPT (not a successful delivery) is the observable signal here,
    # since the test receiver is a dead port: attempt == the filter matched.
    # pi-50's own handover must NOT wake itself (F9) -> no attempt at all.
    post_msg("v2-ww", t, "self handover", type="artifact_ref",
             meta={"kind": "handover", "for_seat": "pi-203"})
    st0 = hak.deliver_pending_wakes()
    assert st0["delivered"] == 0 and st0["failed"] == 0, st0
    # operator's handover to pi-50: sender="*" matches, meta_kind matches -> attempt
    post_msg("v2-ww", SECRET["operator"], "for you", type="artifact_ref",
             meta={"kind": "handover", "for_seat": "pi-50"})
    st = hak.deliver_pending_wakes()
    assert st["failed"] == 1, st              # attempted => the wildcard matched
    client.delete(f"/v1/rooms/v2-ww/subscriptions/{sid}", headers=hdr(t))
    # audit envelopes must name which subscription (create/delete correlation)
    with hak.db() as con:
        bodies = [r["body"] for r in con.execute(
            "SELECT body FROM messages WHERE room='v2-ww' AND "
            "json_extract(meta,'$.op') IN ('subscription_created','subscription_deleted')")]
    assert any(sid in b for b in bodies), bodies
    assert any("filter" in b for b in bodies), bodies


def test_reply_to_must_exist_and_be_in_room():
    """Regression for the live 500 on bdh-cl: reply_to pointing at a nonexistent
    id hit the messages.reply_to FOREIGN KEY and surfaced as an ASGI 500. A
    schema-valid request must never 500 — it is a 422 with the id format spelled
    out (the usual mistake is passing the seq number)."""
    make_room("v2-rt")
    make_room("v2-rt-other")
    t = issue_token("pi-203")["token"]
    join_and_approve("v2-rt", "pi-203", t)
    join_and_approve("v2-rt-other", "pi-203", t)
    # unknown id -> 422, never 500
    r = post_msg("v2-rt", t, "reply to nothing", reply_to="m_v2-rt_0000000999")
    assert r.status_code == 422, (r.status_code, r.text[:200])
    assert err_code(r) == "reply_to_unknown"
    assert "id_format" in r.json()["error"]["detail"]
    # a bare seq number is rejected the same way (the actual new-seat mistake)
    r2 = post_msg("v2-rt", t, "seq as id", reply_to="7")
    assert r2.status_code == 422 and err_code(r2) == "reply_to_unknown"
    # cross-room reply is rejected (id exists, wrong room)
    other = post_msg("v2-rt-other", t, "elsewhere").json()
    r3 = post_msg("v2-rt", t, "cross-room reply", reply_to=other["id"])
    assert r3.status_code == 422 and err_code(r3) == "reply_to_unknown"
    # empty/blank reply_to slips past a truthiness guard but names no envelope:
    # it must 422, never trip the reply_to FK as a 500 (live 500, 2026-09-24)
    for blank in ("", "   "):
        rb = post_msg("v2-rt", t, "blank reply_to", reply_to=blank)
        assert rb.status_code == 422, (repr(blank), rb.status_code, rb.text[:200])
        assert err_code(rb) == "reply_to_empty"
    # valid same-room reply still works, for every type that takes one
    base = post_msg("v2-rt", t, "the original").json()
    for typ, meta in (("chat", None), ("artifact_ref", {"kind": "handover"}),
                      ("task_result", {"kind": "response"})):
        kw = {"reply_to": base["id"]}
        if meta:
            kw["meta"] = meta
        rr = post_msg("v2-rt", t, f"{typ} reply", type=typ, **kw)
        assert rr.status_code == 201, (typ, rr.status_code, rr.text[:200])
    # exactly the three valid replies were stored; rejected ones left nothing
    with hak.db() as con:
        replies = con.execute(
            "SELECT COUNT(*) c FROM messages WHERE room='v2-rt' AND reply_to=?",
            (base["id"],)).fetchone()["c"]
        dangling = con.execute(
            "SELECT COUNT(*) c FROM messages m WHERE m.room='v2-rt' AND m.reply_to IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM messages p WHERE p.id=m.reply_to AND p.room=m.room)"
        ).fetchone()["c"]
    assert replies == 3, replies
    assert dangling == 0, dangling
