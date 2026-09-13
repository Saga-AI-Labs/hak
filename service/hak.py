"""HAK service — FastAPI app, spec v0.5.1 (D1-D48, C1-C18).

Run:  HAK_DB=path/to/hak.db HAK_UPLOADS=path/to/uploads uvicorn hak:app --port 8890
Bootstrap (no admin token left):  python3 hak.py --bootstrap --seat operator
"""

# HAK — inter-agent messaging bus. Copyright (C) 2026 asb (operator seat).
# SPDX-License-Identifier: AGPL-3.0-only
# This file is part of HAK. See LICENSE for the full notice.

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import urllib.error
import urllib.request
import sqlite3
import sys
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, model_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

from canonical import canonicalize

def _load_toml_config() -> dict:
    """Config file: HAK_CONFIG if set, else hak.toml next to this file.
    Bad TOML → warning + ignore (service still boots on defaults)."""
    path = os.environ.get("HAK_CONFIG")
    candidates = [Path(path)] if path else [Path(__file__).resolve().parent / "hak.toml"]
    for c in candidates:
        if c.is_file():
            try:
                import tomllib
                with open(c, "rb") as f:
                    return tomllib.load(f)
            except Exception as e:
                print(f"[hak] warning: config {c} unreadable, ignoring: {e}", file=sys.stderr)
                return {}
    return {}


_CFG = _load_toml_config()


def _cfg(key: str, env: str, default):
    """Precedence: environment variable > config file > built-in default.
    String-typed: callers convert (int/Path) as needed."""
    if os.environ.get(env):
        return os.environ[env]
    if _CFG.get(key) is not None:
        return _CFG[key]
    return default


_SERVE_DIR = Path(__file__).resolve().parent
_DATA_DEFAULT = os.environ.get("HAK_DATA", str(_SERVE_DIR / "data"))

DB_PATH = str(_cfg("db", "HAK_DB", os.path.join(_DATA_DEFAULT, "hak.db")))
UPLOADS_DIR = Path(str(_cfg("uploads", "HAK_UPLOADS", os.path.join(_DATA_DEFAULT, "uploads"))))
SCHEMA_PATH = _SERVE_DIR / "schema.sql"
SWEEP_INTERVAL = int(_cfg("sweep_interval", "HAK_SWEEP_INTERVAL", 3600))  # 0 = off
WAKE_POLL_SEC = int(_cfg("wake_poll_sec", "HAK_WAKE_POLL_SEC", 2))        # 0 = off (D50)
BIND_HOST = str(_cfg("host", "HAK_HOST", "127.0.0.1"))
BIND_PORT = int(_cfg("port", "HAK_PORT", 8890))

ROOM_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")
TYPES = {"chat", "status", "task_request", "task_result", "artifact_ref",
         "review_verdict", "retraction"}
KINDS = {"status", "handover", "response", "publication", "admin-op"}   # D52: +publication
BODY_FORMATS = {"text", "markdown"}                                      # D51
ASSET_KINDS = {"host", "gpu", "repo", "credential", "artifact"}          # D49
WAKE_MAX_ATTEMPTS = 5
WAKE_BACKOFF_SEC = 10
WAKE_RATE_PER_MIN = 20                 # F9: per-subscription storm cap
STATES = {"working_on", "waiting_on", "blocked", "done"}
SCOPE_KINDS = {"write", "read-exclusive", "exclusive", "share"}
ADMIN_OPS = {"member_approve", "member_revoke", "token_issue", "token_revoke",
             "token_revoke_all", "token_bootstrap", "room_create",
             "charter_update", "attachment_delete",
             "asset_register", "asset_update", "asset_retire", "asset_verify",
             "subscription_created", "subscription_deleted",
             "subscription_disabled", "wake_rate_limited"}
MAX_BODY_BYTES = 64 * 1024            # Q8/D42
HARD_TTL_MAX = 1440                    # D28 Fix B
GRACE_HOURS = 24                      # D30
GC_DAYS = 30                           # D18/D23
ALLOWED_URI_SCHEMES = {"http", "https", "git", "ssh", "file"}  # D43

_write_lock = threading.Lock()         # serializes write transactions (single writer)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@contextmanager
def db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    try:
        yield con
        con.commit()
    finally:
        con.close()


@contextmanager
def write_tx():
    """Single-writer transaction (D15/D40): BEGIN IMMEDIATE under the process
    lock — sqlite3's default isolation would otherwise convert our explicit
    BEGIN IMMEDIATE into a no-op and queue writes as implicit snapshots."""
    with _write_lock:
        con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys = ON")
        try:
            con.execute("BEGIN IMMEDIATE")
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


def init_db() -> None:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    with db() as con:
        con.executescript(SCHEMA_PATH.read_text())
        con.execute("PRAGMA journal_mode = WAL")
        _migrate(con)


def _migrate(con: sqlite3.Connection) -> None:
    """In-place, additive migration (V6). v1 data is never rewritten; only new
    columns/tables appear. Idempotent: safe on every boot.
    Rollback note (F20): downgrading to v0.5.1 works because v1 never reads
    `body_format`; the v1 code simply ignores it. Take a DB backup before the
    first v2 boot anyway."""
    cols = {r[1] for r in con.execute("PRAGMA table_info(messages)")}
    if "body_format" not in cols:                    # D51
        con.execute("ALTER TABLE messages ADD COLUMN body_format TEXT NOT NULL DEFAULT 'text'")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def error(status: int, code: str, message: str, detail: Any = None) -> HTTPException:
    """D26 error envelope: {"error": {"code", "message", ...}}."""
    body = {"error": {"code": code, "message": message}}
    if detail is not None:
        body["error"]["detail"] = detail
    return HTTPException(status_code=status, detail=body)


# ---------------------------------------------------------------- auth

def bearer_token(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise error(401, "unauthorized", "Bearer token required")
    return auth[7:].strip()


def seat_from_token(request: Request) -> tuple[str, sqlite3.Row]:
    """Resolve token -> (seat, token_row). 401 on invalid/revoked (D24)."""
    raw = bearer_token(request)
    h = sha256_hex(raw.encode())
    with db() as con:
        row = con.execute("SELECT * FROM tokens WHERE token_hash=?", (h,)).fetchone()
    if row is None or row["revoked_at"]:
        raise error(401, "unauthorized", "Invalid or revoked token")
    return row["seat"], row


def require_room_member(request: Request, room: str) -> tuple[str, sqlite3.Row]:
    seat, tok = seat_from_token(request)
    with db() as con:
        m = con.execute(
            "SELECT status FROM memberships WHERE room=? AND seat=?", (room, seat)
        ).fetchone()
        if m is None:
            raise error(403, "forbidden", "Not a member of this room (join first)")
        if m["status"] != "member":
            raise error(403, "forbidden", f"Membership status is {m['status']}")
        # activity semantics: any authenticated request refreshes last_poll (D27/D43)
        con.execute(
            "INSERT INTO member_state (room, seat, last_poll) VALUES (?,?,?) "
            "ON CONFLICT(room, seat) DO UPDATE SET last_poll=excluded.last_poll",
            (room, seat, now_iso()))
    return seat, tok


def is_admin(con: sqlite3.Connection, room: str, seat: str) -> bool:
    """D32/D45: admins is the sole authority; admin authority requires member
    status; operator non-revocable."""
    room_row = con.execute("SELECT charter FROM rooms WHERE name=?", (room,)).fetchone()
    if room_row is None:
        return False
    charter = json.loads(room_row[0])
    if seat not in charter.get("admins", []):
        return False
    m = con.execute("SELECT status FROM memberships WHERE room=? AND seat=?", (room, seat)).fetchone()
    return m is not None and m["status"] == "member"


def require_room_admin(request: Request, room: str) -> str:
    seat, _ = seat_from_token(request)
    with db() as con:
        _require_room_exists(con, room)
        if not is_admin(con, room, seat):
            raise error(403, "forbidden", "Admin role required (member-status admin)")
    return seat


def _require_room_exists(con: sqlite3.Connection, room: str) -> None:
    if con.execute("SELECT 1 FROM rooms WHERE name=?", (room,)).fetchone() is None:
        raise error(404, "not_found", f"Room {room} not found")


# ---------------------------------------------------------------- system envelopes (D6/D40)

def envelope_id(room: str, seq: int) -> str:
    """Globally unique: id is the PK across all rooms, seq is per-room (D10)."""
    return f"m_{room}_{seq:010d}"


def append_admin_envelope(con: sqlite3.Connection, room: str, op: str,
                          target: str, body_text: str) -> None:
    """Append the admin-op system envelope INSIDE the caller's write_tx —
    mutation + envelope are one transaction (D40)."""
    seq = next_seq(con, room)
    ts = now_iso()
    con.execute(
        "INSERT INTO messages (id, room, seq, from_seat, backend, to_seat, type,"
        " body, meta, ts) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (envelope_id(room, seq), room, seq, "hak", None, None, "chat", body_text,
         json.dumps({"kind": "admin-op", "op": op, "target": target}), ts),
    )


def next_seq(con: sqlite3.Connection, room: str) -> int:
    row = con.execute("SELECT COALESCE(MAX(seq),0)+1 AS n FROM messages WHERE room=?", (room,)).fetchone()
    return row["n"]


# ---------------------------------------------------------------- models

class Attachment(BaseModel):
    """Schema-validated attachment element: file_id required, name optional.
    A bare string ('f_...') is accepted and normalized to {file_id: <string>} —
    both spellings are legal on the wire (D29 never pinned element shape),
    and pydantic must reject anything else with a NAMED field error, never a
    500 deep in _validate_envelope."""
    model_config = ConfigDict(extra="forbid")

    file_id: str
    name: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _string_shorthand(cls, v):
        if isinstance(v, str):
            return {"file_id": v}
        return v


class Ref(BaseModel):
    """Reference element: uri + optional note; string shorthand accepted."""
    model_config = ConfigDict(extra="forbid")

    uri: str
    note: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _string_shorthand(cls, v):
        if isinstance(v, str):
            return {"uri": v}
        return v


class EnvelopeIn(BaseModel):
    """Client-creatable fields only. Server-owned fields (seq, id, ts, room,
    from, and meta.kind='admin-op') are rejected at the schema edge (D10):
    extra=forbid turns any of them into a 422 before a write happens."""
    model_config = ConfigDict(extra="forbid")

    client_msg_id: str | None = None
    backend: str | None = None
    to: dict | None = None
    type: str
    reply_to: str | None = None
    body: str
    attachments: list[Attachment] | None = None
    refs: list[Ref] | None = None
    meta: dict | None = None
    body_format: str | None = None                   # D51: text (default) | markdown

    @model_validator(mode="after")
    def _check_body_format(self):
        if self.body_format is not None and self.body_format not in BODY_FORMATS:
            raise ValueError("body_format must be 'text' or 'markdown'")
        return self


class RoomIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    charter: dict


class ScopeIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_uri: str
    kind: str
    units: int = 1
    note: str | None = None
    ttl_min: int | None = None


class ReadIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seq: int


class AccessIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    seat: str
    level: str = "use"          # use | read | admin | none


class AssetIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    asset_uri: str
    kind: str
    owner_seat: str | None = None
    capacity: int | None = None
    access: list[AccessIn] = []
    facts: dict = {}
    notes: str | None = None


class AssetVerifyIn(BaseModel):
    # C20b: explicit admin verification against the live host. The client
    # reports which registered credentials it found; the server derives MISSING.
    model_config = ConfigDict(extra="forbid")
    present: list[str] = []
    detail: str | None = None


class SubscriptionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str
    filter: dict = {}
    secret: str | None = None
    seat: str | None = None          # admin only: subscribe on behalf of a seat


class TokenIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seat: str


# ---------------------------------------------------------------- app

@asynccontextmanager
async def lifespan(app):
    """Startup: ensure schema; start the sweeper (D18/D23) and the wake-delivery
    worker (D50). Shutdown: stop both. Both are idempotent and also exposed
    synchronously for tests/ops."""
    init_db()
    stop = threading.Event()
    threads = []
    if SWEEP_INTERVAL > 0:
        def sweep_loop():
            while not stop.wait(SWEEP_INTERVAL):
                try:
                    sweep_once()
                except Exception as e:  # never kill the service over GC
                    print(f"[hak] sweep pass failed: {e}", file=sys.stderr)

        t = threading.Thread(target=sweep_loop, name="hak-sweeper", daemon=True)
        t.start()
        threads.append(t)
    if WAKE_POLL_SEC > 0:
        def wake_loop():
            while not stop.wait(WAKE_POLL_SEC):
                try:
                    deliver_pending_wakes()
                except Exception as e:  # a failed wake never kills the service
                    print(f"[hak] wake delivery pass failed: {e}", file=sys.stderr)

        t = threading.Thread(target=wake_loop, name="hak-wakes", daemon=True)
        t.start()
        threads.append(t)
    yield
    stop.set()
    for t in threads:
        t.join(timeout=5)


app = FastAPI(title="HAK", version="v1", lifespan=lifespan)

# Human viewer: read-only lens on the same API (no protocol surface, no
# writes). Mounted same-origin, so the browser needs no CORS and no second
# service. Deployments without service/ui/ behave exactly as before.
_UI_DIR = Path(__file__).resolve().parent / "ui"
if (_UI_DIR / "index.html").is_file():
    from fastapi.staticfiles import StaticFiles  # noqa: E402 (optional dep, only when ui/ present)

    app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")


@app.exception_handler(StarletteHTTPException)
def http_exc_handler(request: Request, exc: StarletteHTTPException):
    """Uniform D26 envelope: our raised errors and framework 404/405 alike.
    The 413 rewrite for oversized bodies (D42) happens in post_message."""
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        return JSONResponse(status_code=exc.status_code, content=exc.detail)
    code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "invalid_request")
    return JSONResponse(status_code=exc.status_code, content={
        "error": {"code": code, "message": str(exc.detail)}})


@app.get("/v1/whoami")
def whoami(request: Request):
    """Identity of the bearer token — the UI needs its seat for DM/reply/
    admin/presence affordances. Nothing beyond the seat (D43 spirit)."""
    seat, _ = seat_from_token(request)
    return {"seat": seat}


@app.get("/v1/health")
def health(request: Request):
    seat_from_token(request)  # authenticated (D43)
    return {"status": "ok", "service": "hak", "version": "v1"}


# ------------------------------------------------- rooms & membership

@app.get("/v1/rooms")
def list_rooms(request: Request):
    seat, _ = seat_from_token(request)
    with db() as con:
        rows = con.execute(
            "SELECT r.name, r.charter FROM rooms r JOIN memberships m ON m.room=r.name "
            "WHERE m.seat=? AND m.status='member'", (seat,)).fetchall()
    return [{"name": r["name"], "charter": json.loads(r["charter"])} for r in rows]


@app.post("/v1/rooms", status_code=201)
def create_room(payload: RoomIn, request: Request):
    seat, _ = seat_from_token(request)
    if not ROOM_RE.match(payload.name):
        raise error(422, "invalid_room_name", "Room name must match ^[a-z0-9][a-z0-9._-]{0,62}$")
    with write_tx() as con:
        has_any_room = con.execute("SELECT 1 FROM rooms LIMIT 1").fetchone() is not None
        # First room: any authenticated seat may create (bootstrap); later: admin-only (Q5).
        if has_any_room and not _is_admin_anywhere(con, seat):
            raise error(403, "forbidden", "Room creation is admin-only in v1")
        if con.execute("SELECT 1 FROM rooms WHERE name=?", (payload.name,)).fetchone():
            raise error(409, "room_exists", f"Room {payload.name} already exists")
        charter = dict(payload.charter)
        charter["name"] = payload.name
        # materialize defaults (D46)
        cp = charter.setdefault("claim_policy", {})
        cp.setdefault("default_ttl_min", 30)
        cp.setdefault("write_mandatory_for_repo_paths", True)
        cp.setdefault("share_capacities", {})
        ap = charter.setdefault("attachment_policy", {})
        ap.setdefault("max_file_bytes", 26214400)
        ap.setdefault("max_unreferenced_bytes", None)
        charter.setdefault("admins", [])
        if seat not in charter["admins"]:
            charter["admins"].append(seat)     # creator becomes admin (D32)
        if "operator" not in charter["admins"]:
            charter["admins"].append("operator")  # D32 invariant
        con.execute("INSERT INTO rooms (name, charter, created_at) VALUES (?,?,?)",
                    (payload.name, canonicalize(charter), now_iso()))
        con.execute("INSERT INTO memberships (room, seat, status) VALUES (?,?, 'member')",
                    (payload.name, seat))
        con.execute("INSERT OR IGNORE INTO member_state (room, seat) VALUES (?,?)",
                    (payload.name, seat))
        con.execute("INSERT OR IGNORE INTO memberships (room, seat, status) VALUES (?,?, 'member')",
                    (payload.name, "operator"))  # operator auto-member (D32)
        con.execute("INSERT OR IGNORE INTO member_state (room, seat) VALUES (?,?)",
                    (payload.name, "operator"))
        append_admin_envelope(con, payload.name, "room_create", seat,
                              f"Room created by {seat}; charter stored.")
    return {"name": payload.name, "charter": charter}


@app.get("/v1/rooms/{room}")
def get_room(room: str, request: Request):
    require_room_member(request, room)
    with db() as con:
        r = con.execute("SELECT * FROM rooms WHERE name=?", (room,)).fetchone()
        # full member projection incl. presence (last_poll/last_read_seq) —
        # peer staleness is observable from the room view itself (D27/D2
        # projector state; same data as /members, no extra request)
        members = con.execute(
            "SELECT m.seat, m.status, ms.last_read_seq, ms.last_poll "
            "FROM memberships m LEFT JOIN member_state ms "
            "ON ms.room=m.room AND ms.seat=m.seat "
            "WHERE m.room=? ORDER BY m.seat", (room,)).fetchall()
    return {"name": r["name"], "charter": json.loads(r["charter"]),
            "members": [dict(m) for m in members]}


@app.post("/v1/rooms/{room}/join")
def join_room(room: str, request: Request):
    seat, _ = seat_from_token(request)
    with write_tx() as con:
        _require_room_exists(con, room)
        m = con.execute("SELECT status FROM memberships WHERE room=? AND seat=?",
                        (room, seat)).fetchone()
        if m and m["status"] == "member":
            return {"status": "member", "note": "already member (idempotent re-join, D36)"}
        if m and m["status"] == "revoked":
            con.execute("UPDATE memberships SET status='pending' WHERE room=? AND seat=?",
                        (room, seat))  # new pending row semantics (D36)
        else:
            con.execute("INSERT INTO memberships (room, seat, status) VALUES (?,?, 'pending')",
                       (room, seat))
    return {"status": "pending"}


@app.post("/v1/rooms/{room}/members/{seat}/approve")
def approve_member(room: str, seat: str, request: Request):
    admin = require_room_admin(request, room)
    with write_tx() as con:
        _require_room_exists(con, room)
        m = con.execute("SELECT status FROM memberships WHERE room=? AND seat=?",
                        (room, seat)).fetchone()
        if m is None:
            raise error(404, "not_found", f"{seat} has not joined")
        if m["status"] == "member":
            return {"status": "member", "note": "already member (idempotent)"}
        if m["status"] == "revoked":
            # re-approve of a revoked seat is forbidden: the seat must re-join
            # (new pending) first — re-admission is an explicit act (D36)
            raise error(422, "membership_revoked", f"{seat} must re-join first (D36)")
        con.execute("UPDATE memberships SET status='member' WHERE room=? AND seat=?",
                    (room, seat))
        con.execute("INSERT OR IGNORE INTO member_state (room, seat) VALUES (?,?)", (room, seat))
        append_admin_envelope(con, room, "member_approve", seat,
                              f"{admin} approved {seat} as member.")
    return {"status": "member"}


@app.post("/v1/rooms/{room}/members/{seat}/revoke")
def revoke_member(room: str, seat: str, request: Request):
    admin = require_room_admin(request, room)
    if seat == "operator":
        raise error(422, "operator_non_revocable",
                    "operator membership is non-revocable in v1 (D45)")
    with write_tx() as con:
        _require_room_exists(con, room)
        m = con.execute("SELECT status FROM memberships WHERE room=? AND seat=?",
                        (room, seat)).fetchone()
        if m is None:
            raise error(404, "not_found", f"{seat} is not a member")
        con.execute("UPDATE memberships SET status='revoked' WHERE room=? AND seat=?",
                    (room, seat))
        # kill all the seat's tokens (D19)
        con.execute("UPDATE tokens SET revoked_at=? WHERE seat=? AND revoked_at IS NULL",
                    (now_iso(), seat))
        append_admin_envelope(con, room, "member_revoke", seat,
                              f"{admin} revoked {seat}; all tokens killed (D19).")
    return {"status": "revoked"}


@app.get("/v1/rooms/{room}/members")
def list_members(room: str, request: Request):
    require_room_member(request, room)
    with db() as con:
        _require_room_exists(con, room)
        rows = con.execute(
            "SELECT m.seat, m.status, ms.last_read_seq, ms.last_poll "
            "FROM memberships m LEFT JOIN member_state ms ON ms.room=m.room AND ms.seat=m.seat "
            "WHERE m.room=? ORDER BY m.seat", (room,)).fetchall()
    return [dict(r) for r in rows]


# ------------------------------------------------- messages

@app.post("/v1/rooms/{room}/messages", status_code=201)
def post_message(room: str, payload: EnvelopeIn, request: Request):
    seat, _ = require_room_member(request, room)
    if len(payload.body.encode("utf-8")) > MAX_BODY_BYTES:
        raise error(413, "body_too_large",
                    f"body exceeds {MAX_BODY_BYTES} bytes (D42)")
    _validate_envelope(room, payload, seat, con=None)
    body_hash = canonicalize(payload.model_dump(exclude_none=False))
    with write_tx() as con:
        _require_room_exists(con, room)
        _validate_envelope(room, payload, seat, con=con)  # re-check under lock
        if payload.client_msg_id:
            prev = con.execute(
                "SELECT * FROM messages WHERE room=? AND from_seat=? AND client_msg_id=?",
                (room, seat, payload.client_msg_id)).fetchone()
            if prev:
                if prev["idem_hash"] == body_hash:
                    return JSONResponse(status_code=200, content=envelope_out(prev))
                raise error(409, "idempotency_conflict",
                            "client_msg_id reused with different content (D12/D47)",
                            {"original_id": prev["id"]})
        seq = next_seq(con, room)
        mid = envelope_id(room, seq)
        con.execute(
            "INSERT INTO messages (id, room, seq, client_msg_id, idem_hash, from_seat,"
            " backend, to_seat, type, reply_to, body, attachments, refs, meta, ts, body_format)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (mid, room, seq, payload.client_msg_id, body_hash, seat, payload.backend,
             (payload.to or {}).get("seat") if payload.to else None, payload.type,
             payload.reply_to, payload.body,
             canonicalize([a.model_dump(exclude_none=True) for a in payload.attachments]) if payload.attachments else None,
             canonicalize([r.model_dump(exclude_none=True) for r in payload.refs]) if payload.refs else None,
             canonicalize(payload.meta) if payload.meta else None, now_iso(),
             payload.body_format or "text"))
        row = con.execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
        enqueue_wakes(con, room, row)      # D50: atomic with the envelope (D40 discipline)
    return envelope_out(row)


def _validate_envelope(room: str, p: EnvelopeIn, seat: str, con) -> None:
    if p.type not in TYPES:
        raise error(422, "invalid_type", f"type must be one of {sorted(TYPES)}")
    if p.type in ("task_result", "review_verdict"):
        # D13 + D52 matrix: response (solicited, with reply_to) OR publication
        # (stands alone, may also carry reply_to — F16/F17).
        kind = (p.meta or {}).get("kind")
        if kind == "response":
            pass                                       # solicited answer
        elif kind == "publication":
            pass                                       # unsolicited and/or stand-alone (F16)
        else:
            raise error(422, "response_marker_required",
                        "task_result/review_verdict require meta.kind='response' (solicited) "
                        "or 'publication' (stand-alone) — D13/D52)")
    if p.meta:
        kind = p.meta.get("kind")
        if kind not in KINDS:
            raise error(422, "invalid_meta_kind",
                        f"meta.kind must be one of {sorted(KINDS)} (closed set, D39)")
        if kind == "admin-op":
            raise error(422, "admin_op_reserved",
                        "admin-op is emitted only by the bus (D39/D6)")
        if kind == "status":
            if p.meta.get("state") not in STATES:
                raise error(422, "invalid_status_state",
                            "meta.state must be working_on|waiting_on|blocked|done")
    if p.type == "retraction":
        if not p.reply_to:
            raise error(422, "retraction_requires_reply_to",
                        "retraction requires reply_to (D37)")
        if con is not None:
            target = con.execute("SELECT * FROM messages WHERE id=?", (p.reply_to,)).fetchone()
            if target is None:
                raise error(422, "retraction_target_unknown", "reply_to message not found")
            if target["type"] == "retraction":
                raise error(422, "retraction_of_retraction",
                            "a retraction cannot target a retraction (D37)")
            if target["from_seat"] != seat and not is_admin(con, room, seat):
                raise error(403, "retraction_forbidden",
                            "only the author or an admin may retract (D37)")
            prior = con.execute(
                "SELECT id FROM messages WHERE type='retraction' AND reply_to=?",
                (p.reply_to,)).fetchone()
            if prior:
                raise error(409, "duplicate_retraction",
                            "target already retracted (D37)",
                            {"effective_retraction": prior["id"]})
    if con is not None and p.attachments:
        for a in p.attachments:
            # normalize at the boundary: elements arrive as validated
            # Attachment models here — but never trust shape past the edge
            fid = a.file_id if hasattr(a, "file_id") else (a.get("file_id", "") if isinstance(a, dict) else str(a))
            f = con.execute("SELECT * FROM files WHERE file_id=?", (fid,)).fetchone()
            if f is None or f["deletion_pending"]:
                raise error(422, "attachment_not_available",
                            f"file {fid} unknown or deletion_pending (D44)")
            if f["room"] != room:
                raise error(422, "cross_room_file",
                            f"file {fid} belongs to room {f['room']} (D29)")


def envelope_out(row: sqlite3.Row) -> dict:
    def j(x):
        return json.loads(x) if x else None
    return {
        "seq": row["seq"], "id": row["id"], "client_msg_id": row["client_msg_id"],
        "room": row["room"], "ts": row["ts"],
        "from": {"seat": row["from_seat"], "backend": row["backend"]},
        "to": {"seat": row["to_seat"]} if row["to_seat"] else None,
        "type": row["type"], "reply_to": row["reply_to"], "body": row["body"],
        "attachments": j(row["attachments"]), "refs": j(row["refs"]), "meta": j(row["meta"]),
        "body_format": (row["body_format"] if "body_format" in row.keys() else "text") or "text",
    }


@app.get("/v1/rooms/{room}/messages")
def get_messages(room: str, request: Request,
                 since: int = 0, until: int | None = None,
                 from_seat: str | None = None, to: str | None = None,
                 type: str | None = None, thread: str | None = None,
                 meta_kind: str | None = None, for_seat: str | None = None,
                 limit: int = 100, order: str = "asc"):
    seat, _ = require_room_member(request, room)
    with db() as con:
        _require_room_exists(con, room)
        q = "SELECT * FROM messages WHERE room=? AND seq > ?"
        args: list[Any] = [room, since]
        if until is not None:
            q += " AND seq <= ?"
            args.append(until)
        if from_seat:
            q += " AND from_seat = ?"
            args.append(from_seat)
        if to == "null":
            q += " AND to_seat IS NULL"
        elif to:
            q += " AND to_seat = ?"
            args.append(to)
        if type:
            q += " AND type = ?"
            args.append(type)
        if thread:
            q += " AND reply_to = ?"   # direct replies only (D25)
            args.append(thread)
        if meta_kind:
            q += " AND json_extract(meta,'$.kind') = ?"
            args.append(meta_kind)
        if for_seat == "me":
            for_seat = seat               # resolves server-side (D25/D10)
        if for_seat:
            q += " AND json_extract(meta,'$.for_seat') = ?"
            args.append(for_seat)
        limit = max(1, min(limit, 500))
        q += " ORDER BY seq ASC LIMIT ?"   # bounds before order (D33); asc canonical
        args.append(limit + 1)             # detect continuation
        rows = con.execute(q, args).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    next_since = rows[-1]["seq"] if rows else since  # computed before desc flip (D33)
    if order == "desc":
        rows = rows[::-1]                  # presentation only (D33)
    out = [envelope_out(r) for r in rows]
    return {"messages": out, "has_more": has_more, "next_since": next_since}


@app.get("/v1/rooms/{room}/messages/{mid}")
def get_message(room: str, mid: str, request: Request):
    require_room_member(request, room)
    with db() as con:
        row = con.execute("SELECT * FROM messages WHERE room=? AND id=?", (room, mid)).fetchone()
    if row is None:
        raise error(404, "not_found", "message not found")
    return envelope_out(row)


@app.post("/v1/rooms/{room}/read")
def mark_read(room: str, payload: ReadIn, request: Request):
    seat, _ = require_room_member(request, room)
    with db() as con:
        con.execute(
            "INSERT INTO member_state (room, seat, last_read_seq) VALUES (?,?,?) "
            "ON CONFLICT(room, seat) DO UPDATE SET last_read_seq=excluded.last_read_seq",
            (room, seat, payload.seq))
    return {"ok": True}


# ------------------------------------------------- scopes (D14/D15/D22/D34)

def _normalize_resource(uri: str) -> tuple[str, str]:
    """scheme lowercased, exact-URI conflict identity (D34)."""
    if "://" not in uri:
        raise error(422, "invalid_resource_uri", "resource_uri must be scheme://rest")
    scheme, rest = uri.split("://", 1)
    scheme = scheme.lower()
    if not scheme or not rest:
        raise error(422, "invalid_resource_uri", "empty scheme or path")
    return scheme, f"{scheme}://{rest}"


def _live_claims(con: sqlite3.Connection, room: str, resource: str) -> list[sqlite3.Row]:
    """Project live claims: last event per scope_id wins; live = last action in
    (claim, renew) and expires_at > now (D15)."""
    now = now_iso()
    rows = con.execute(
        "SELECT * FROM scope_events WHERE room=? AND resource_uri=? ORDER BY scope_seq",
        (room, resource)).fetchall()
    last: dict[str, sqlite3.Row] = {}
    for r in rows:
        last[r["scope_id"]] = r
    return [r for r in last.values()
            if r["action"] in ("claim", "renew") and r["expires_at"] > now]


def _share_units(con: sqlite3.Connection, room: str, resource: str) -> int:
    return sum(r["units"] for r in _live_claims(con, room, resource) if r["kind"] == "share")


@app.post("/v1/rooms/{room}/scopes", status_code=201)
def post_scope(room: str, payload: ScopeIn, request: Request):
    seat, _ = require_room_member(request, room)
    scheme, resource = _normalize_resource(payload.resource_uri)
    if payload.kind not in SCOPE_KINDS:
        raise error(422, "invalid_scope_kind",
                    f"kind must be one of {sorted(SCOPE_KINDS)} (reserve dropped, D20)")
    if payload.kind == "share" and payload.units < 1:
        raise error(422, "invalid_units", "share units must be >= 1 (D34)")
    with write_tx() as con:
        _require_room_exists(con, room)
        charter = json.loads(con.execute("SELECT charter FROM rooms WHERE name=?", (room,)).fetchone()[0])
        ttl_default = charter["claim_policy"]["default_ttl_min"]
        if payload.ttl_min is not None:
            if payload.ttl_min > HARD_TTL_MAX:
                raise error(422, "ttl_too_large", f"ttl_min above hard max {HARD_TTL_MAX}")
            ttl = min(payload.ttl_min, ttl_default)  # clamp (Fix B, D28)
        else:
            ttl = ttl_default
        expires = (datetime.now(timezone.utc) + timedelta(minutes=ttl)).isoformat(timespec="milliseconds")
        live = _live_claims(con, room, resource)
        # self-reclaim refresh (D16)
        own = [r for r in live if r["seat"] == seat and r["kind"] == payload.kind]
        if own:
            if payload.kind == "share":
                cap = charter["claim_policy"]["share_capacities"].get(scheme)
                if cap is not None:
                    used = _share_units(con, room, resource)
                    if used - own[0]["units"] + payload.units > cap:
                        raise error(409, "share_capacity_exhausted",
                                    "Share capacity exhausted (D22/D34)",
                                    {"current_units": used, "capacity": cap})
            sseq = next_scope_seq(con, room)
            con.execute(
                "INSERT INTO scope_events (scope_seq, room, scope_id, seat, action,"
                " resource_uri, kind, units, note, ttl_min, expires_at, ts)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (sseq, room, own[0]["scope_id"], seat, "renew", resource,
                 payload.kind, payload.units, payload.note, ttl, expires, now_iso()))
            return JSONResponse(status_code=200, content={
                "scope_id": own[0]["scope_id"], "scope_seq": sseq, "expires_at": expires})
        # conflict matrix (D22, symmetric per D34): the relation applies
        # direction-independently — a live share blocks a new write, exactly as
        # a live write blocks a new share.
        conflict = None
        if payload.kind == "exclusive":
            if live:
                conflict = live[0]
        elif payload.kind in ("write", "read-exclusive"):
            for r in live:
                if r["kind"] in ("exclusive", "write", "read-exclusive", "share"):
                    conflict = r
                    break
        elif payload.kind == "share":
            for r in live:
                if r["kind"] in ("exclusive", "write", "read-exclusive"):
                    conflict = r
                    break
            # share vs share is allowed while capacity holds (checked below)
        if conflict is not None:
            raise error(409, "scope_conflict", "Conflicting live claim (D22)",
                        {"conflicting": {"holder": conflict["seat"], "kind": conflict["kind"],
                                          "expires_at": conflict["expires_at"]}})
        if payload.kind == "share":
            cap = charter["claim_policy"]["share_capacities"].get(scheme)
            if cap is not None:
                used = _share_units(con, room, resource)
                if used + payload.units > cap:
                    raise error(409, "share_capacity_exhausted",
                                "Share capacity exhausted (D22/D34)",
                                {"current_units": used, "capacity": cap})
        sseq = next_scope_seq(con, room)
        sid = f"s_{room[:8]}_{sseq:06d}_{secrets.token_hex(3)}"
        con.execute(
            "INSERT INTO scope_events (scope_seq, room, scope_id, seat, action,"
            " resource_uri, kind, units, note, ttl_min, expires_at, ts)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (sseq, room, sid, seat, "claim", resource, payload.kind, payload.units,
             payload.note, ttl, expires, now_iso()))
    return {"scope_id": sid, "scope_seq": sseq, "expires_at": expires}


def next_scope_seq(con: sqlite3.Connection, room: str) -> int:
    row = con.execute("SELECT COALESCE(MAX(scope_seq),0)+1 AS n FROM scope_events WHERE room=?",
                      (room,)).fetchone()
    return row["n"]


@app.post("/v1/rooms/{room}/scopes/{scope_id}/renew")
def renew_scope(room: str, scope_id: str, request: Request):
    seat, _ = require_room_member(request, room)
    with write_tx() as con:
        _require_room_exists(con, room)
        rows = con.execute("SELECT * FROM scope_events WHERE room=? AND scope_id=? ORDER BY scope_seq",
                           (room, scope_id)).fetchall()
        if not rows:
            raise error(404, "not_found", "scope not found")
        last = rows[-1]
        live = last["action"] in ("claim", "renew") and last["expires_at"] > now_iso()
        if not live:
            raise error(404, "not_live", "Claim not live at transaction start (D34)")
        if last["seat"] != seat:
            raise error(403, "forbidden", "Only the holder may renew")
        charter = json.loads(con.execute("SELECT charter FROM rooms WHERE name=?", (room,)).fetchone()[0])
        ttl = charter["claim_policy"]["default_ttl_min"]
        expires = (datetime.now(timezone.utc) + timedelta(minutes=ttl)).isoformat(timespec="milliseconds")
        sseq = next_scope_seq(con, room)
        con.execute(
            "INSERT INTO scope_events (scope_seq, room, scope_id, seat, action,"
            " resource_uri, kind, units, note, ttl_min, expires_at, ts)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (sseq, room, scope_id, seat, "renew", last["resource_uri"], last["kind"],
             last["units"], None, ttl, expires, now_iso()))
    return {"scope_id": scope_id, "expires_at": expires}


@app.delete("/v1/rooms/{room}/scopes/{scope_id}", status_code=204)
def release_scope(room: str, scope_id: str, request: Request):
    seat, _ = require_room_member(request, room)
    with write_tx() as con:
        _require_room_exists(con, room)
        rows = con.execute("SELECT * FROM scope_events WHERE room=? AND scope_id=? ORDER BY scope_seq",
                           (room, scope_id)).fetchall()
        if not rows:
            raise error(404, "not_found", "scope not found")
        last = rows[-1]
        live = last["action"] in ("claim", "renew") and last["expires_at"] > now_iso()
        if not live:
            return Response(status_code=204)   # idempotent release of a dead claim
        if last["seat"] != seat and not is_admin(con, room, seat):
            raise error(403, "forbidden", "Only the holder or an admin may release")
        sseq = next_scope_seq(con, room)
        con.execute(
            "INSERT INTO scope_events (scope_seq, room, scope_id, seat, action,"
            " resource_uri, kind, units, note, ttl_min, expires_at, ts)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (sseq, room, scope_id, seat, "release", last["resource_uri"], last["kind"],
             last["units"], None, 0, last["expires_at"], now_iso()))
    return Response(status_code=204)


@app.get("/v1/rooms/{room}/scopes")
def get_scopes(room: str, request: Request, history: int = 0, since: int = 0):
    """active: last-event-per-scope projector, expiry-filtered (D2/D15).
    history: raw event log paged by scope_seq, since exclusive (D35)."""
    require_room_member(request, room)
    now = now_iso()
    with db() as con:
        _require_room_exists(con, room)
        if history:
            rows = con.execute(
                "SELECT * FROM scope_events WHERE room=? AND scope_seq > ? ORDER BY scope_seq",
                (room, since)).fetchall()
            return {"events": [dict(r) for r in rows]}
        rows = con.execute("SELECT * FROM scope_events WHERE room=?", (room,)).fetchall()
    last: dict[str, sqlite3.Row] = {}
    for r in rows:
        last[r["scope_id"]] = r
    live = [r for r in last.values()
            if r["action"] in ("claim", "renew") and r["expires_at"] > now]
    return {"active": [dict(r) for r in live]}


# ------------------------------------------------- files (D29/D30/D44)

@app.post("/v1/files", status_code=201)
async def upload_file(request: Request):
    seat, _ = seat_from_token(request)
    form = await request.form()
    room = form.get("room")
    if not room:
        raise error(422, "room_required", "Upload requires the target room (D29)")
    with db() as con:
        _require_room_exists(con, room)
        m = con.execute("SELECT status FROM memberships WHERE room=? AND seat=?",
                        (room, seat)).fetchone()
        if m is None or m["status"] != "member":
            raise error(403, "forbidden", "Upload requires member status (D29/D36)")
        charter = json.loads(con.execute("SELECT charter FROM rooms WHERE name=?", (room,)).fetchone()[0])
        max_bytes = charter["attachment_policy"]["max_file_bytes"]
    upload = form.get("file")
    if upload is None or not hasattr(upload, "read"):
        raise error(422, "file_required", "multipart file field 'file' required")
    data = await upload.read()          # capped by starlette's in-memory size guard
    if len(data) > max_bytes:
        raise error(413, "file_too_large", f"File exceeds charter cap {max_bytes} bytes")
    file_id = "f_" + secrets.token_hex(8)
    (UPLOADS_DIR / file_id).write_bytes(data)
    # content-type: client's, upgraded from the filename when generic/missing —
    # the UI viewer (text vs image rendering) depends on it
    ext_types = {".txt": "text/plain", ".md": "text/markdown", ".json": "application/json",
                 ".csv": "text/csv", ".log": "text/plain", ".yaml": "text/yaml",
                 ".yml": "text/yaml", ".py": "text/x-python", ".png": "image/png",
                 ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif",
                 ".webp": "image/webp", ".svg": "image/svg+xml", ".pdf": "application/pdf"}
    ct = upload.content_type
    if not ct or ct == "application/octet-stream":
        ct = ext_types.get(Path(upload.filename or "").suffix.lower(), ct or "application/octet-stream")
    with write_tx() as con:
        # re-check membership under the lock (TOCTOU against revoke)
        m = con.execute("SELECT status FROM memberships WHERE room=? AND seat=?",
                        (room, seat)).fetchone()
        if m is None or m["status"] != "member":
            (UPLOADS_DIR / file_id).unlink(missing_ok=True)
            raise error(403, "forbidden", "Upload requires member status (D29/D36)")
        con.execute(
            "INSERT INTO files (file_id, room, uploader, name, content_type, sha256,"
            " size, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (file_id, room, seat, upload.filename or "unnamed", ct,
             sha256_hex(data), len(data), now_iso()))
    return {"file_id": file_id, "sha256": sha256_hex(data), "size": len(data),
            "name": upload.filename, "content_type": ct, "room": room}


@app.get("/v1/files/{file_id}")
def get_file(file_id: str, request: Request):
    seat, _ = seat_from_token(request)
    with db() as con:
        f = con.execute("SELECT * FROM files WHERE file_id=?", (file_id,)).fetchone()
        if f is None:
            raise error(404, "not_found", "file not found")
        if f["deletion_pending"]:
            raise error(404, "not_found", "file pending deletion (D44)")
        if f["room"]:
            m = con.execute("SELECT status FROM memberships WHERE room=? AND seat=?",
                            (f["room"], seat)).fetchone()
            is_adm = is_admin(con, f["room"], seat)
            if (m is None or m["status"] != "member") and not is_adm:
                raise error(403, "forbidden", "Room members only (D29)")
        path = UPLOADS_DIR / file_id
    if not path.exists():
        raise error(404, "not_found", "file content missing (sweep retry pending, D44)")
    return FileResponse(path, filename=f["name"], media_type=f["content_type"])


# ------------------------------------------------- admin/ops (D6/D19/D24/D40)

@app.post("/v1/tokens", status_code=201)
def issue_token(payload: TokenIn, request: Request):
    """Issue a token for a seat. Requires admin somewhere OR bootstrap state
    (no rooms yet). Token issuance is global; any room admin may issue (LAN
    trust model, D19). Admin check + insert + envelope in ONE transaction —
    revoke cannot interleave (D40)."""
    seat, _ = seat_from_token(request)
    token_id = "t_" + secrets.token_hex(6)
    secret = secrets.token_urlsafe(32)
    with write_tx() as con:
        any_room = con.execute("SELECT name FROM rooms LIMIT 1").fetchone()
        if any_room and not _is_admin_anywhere(con, seat):
            raise error(403, "forbidden", "Token issuance requires admin role")
        con.execute("INSERT INTO tokens (token_id, seat, token_hash, created_at) VALUES (?,?,?,?)",
                    (token_id, payload.seat, sha256_hex(secret.encode()), now_iso()))
        # envelope in the first room for audit (D40); global op, room-scoped trail
        room_row = con.execute("SELECT name FROM rooms ORDER BY created_at LIMIT 1").fetchone()
        if room_row:
            append_admin_envelope(con, room_row["name"], "token_issue", payload.seat,
                                  f"Token {token_id} issued for {payload.seat}.")
    return {"token_id": token_id, "token": secret, "note": "shown once (D43)"}


@app.post("/v1/tokens/{token_id}/revoke")
def revoke_token(token_id: str, request: Request):
    seat, _ = seat_from_token(request)
    with write_tx() as con:
        any_room = con.execute("SELECT name FROM rooms LIMIT 1").fetchone()
        if any_room and not _is_admin_anywhere(con, seat):
            raise error(403, "forbidden", "Token revocation requires admin role")
        row = con.execute("SELECT * FROM tokens WHERE token_id=?", (token_id,)).fetchone()
        if row is None:
            raise error(404, "not_found", "token not found")
        con.execute("UPDATE tokens SET revoked_at=? WHERE token_id=?", (now_iso(), token_id))
        room_row = con.execute("SELECT name FROM rooms ORDER BY created_at LIMIT 1").fetchone()
        if room_row:
            append_admin_envelope(con, room_row["name"], "token_revoke", row["seat"],
                                  f"Token {token_id} of {row['seat']} revoked.")
    return {"revoked": token_id}


# ------------------------------------------------- charter update (D32 future-note, v2)

CHARTER_MUTABLE = {"purpose", "claim_policy", "attachment_policy", "wake_hooks"}
CHARTER_IMMUTABLE = {"name", "admins"}     # v2.0: admin-set changes are a separate decision


class CharterPatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    patch: dict


@app.post("/v1/rooms/{room}/charter")
def update_charter(room: str, payload: CharterPatchIn, request: Request):
    """D32 anticipated this: 'A future mutation endpoint MUST emit
    admin-op: charter_update'. `admins` and `name` are immutable in v2.0."""
    seat = require_room_admin(request, room)
    illegal = CHARTER_IMMUTABLE & set(payload.patch)
    if illegal:
        raise error(422, "charter_immutable",
                    f"{sorted(illegal)} cannot be changed by this endpoint in v2.0 (D32/D49)")
    unknown = set(payload.patch) - CHARTER_MUTABLE - CHARTER_IMMUTABLE
    if unknown:
        raise error(422, "charter_unknown_keys", f"unknown charter keys: {sorted(unknown)}")
    with write_tx() as con:
        r = con.execute("SELECT charter FROM rooms WHERE name=?", (room,)).fetchone()
        if r is None:
            raise error(404, "not_found", f"Room {room} not found")
        ch = json.loads(r["charter"])
        for k, v in payload.patch.items():
            if isinstance(v, dict) and isinstance(ch.get(k), dict):
                ch[k].update(v)                      # shallow merge for policy blocks
            else:
                ch[k] = v
        if ch.get("wake_hooks", {}).get("enabled") and not ch["wake_hooks"].get("policy"):
            raise error(422, "wake_charter_required",
                        "enabling wake-hooks requires wake_hooks.policy — the woken-turn "
                        "authorization charter accepted by the operator (F15/V7)")
        con.execute("UPDATE rooms SET charter=? WHERE name=?", (canonicalize(ch), room))
        append_admin_envelope(con, room, "charter_update", seat,
                              f"{seat} updated charter keys: {sorted(payload.patch)}.")
    return {"name": room, "charter": ch}


# ------------------------------------------------- wake-hooks: D50 (ratified V7)

WAKE_FILTER_KEYS = {"for_seat", "type", "meta_kind", "sender"}   # F7: closed, no query language


def _wake_enabled(con: sqlite3.Connection, room: str) -> bool:
    """F15: wake-hooks are opt-in per room, and enabling them accepts the
    woken-turn authorization charter (stored in the charter as the policy)."""
    row = con.execute("SELECT charter FROM rooms WHERE name=?", (room,)).fetchone()
    if row is None:
        return False
    ch = json.loads(row["charter"])
    return bool(ch.get("wake_hooks", {}).get("enabled"))


def _wild_match(want: str, have: str | None) -> bool:
    """'*' is the documented wildcard for EVERY filter key (empty filter == any)."""
    return want == "*" or (have is not None and want == have)


def _validate_wake_filter(flt: dict) -> None:
    """Reject filter values that can never match. An unvalidated value is the
    worst failure mode this API has: the subscription looks healthy, fires
    never, and reports nothing. (bdh-cl #271: `type:"any"` — my own UI label
    for an empty filter — was registered as a literal type value.)"""
    unknown = set(flt) - WAKE_FILTER_KEYS
    if unknown:
        raise error(422, "invalid_wake_filter",
                    f"filter keys must be within {sorted(WAKE_FILTER_KEYS)} — no query "
                    "language (F7); an EMPTY filter means 'all'")
    for k, v in flt.items():
        if not isinstance(v, str) or not v:
            raise error(422, "invalid_wake_filter",
                        f"{k} must be a non-empty string or '*'")
        if k == "type" and v != "*" and v not in TYPES:
            raise error(422, "invalid_wake_filter",
                        f"type must be '*' or one of {sorted(TYPES)} — 'any' is not a "
                        "type; use an empty filter or '*' for all")
        if k == "meta_kind" and v != "*" and v not in KINDS:
            raise error(422, "invalid_wake_filter",
                        f"meta_kind must be '*' or one of {sorted(KINDS)}")


def _filter_matches(flt: dict, row: sqlite3.Row) -> bool:
    meta = (json.loads(row["meta"]) or {}) if row["meta"] else {}
    if "for_seat" in flt and not _wild_match(flt["for_seat"], meta.get("for_seat")):
        return False
    if "type" in flt and not _wild_match(flt["type"], row["type"]):
        return False
    # NB: meta_kind="admin-op" never fires — system envelopes are appended by
    # the service, not posted through this path (D6), so they do not wake seats.
    if "meta_kind" in flt and not _wild_match(flt["meta_kind"], meta.get("kind")):
        return False
    if "sender" in flt and not _wild_match(flt["sender"], row["from_seat"]):
        return False
    return True


def enqueue_wakes(con: sqlite3.Connection, room: str, row: sqlite3.Row) -> int:
    """Queue wake notifications INSIDE the posting transaction (atomic, D40).
    Metadata only — never the body (D50). A seat's own posts never wake itself (F9)."""
    subs = con.execute(
        "SELECT * FROM subscriptions WHERE room=? AND disabled_at IS NULL", (room,)).fetchall()
    n = 0
    for s in subs:
        if s["seat"] == row["from_seat"]:
            continue                                   # self-post never wakes
        flt = json.loads(s["filter"])
        if not _filter_matches(flt, row):
            continue
        meta = json.loads(row["meta"]) if row["meta"] else {}
        has_att = bool(row["attachments"])
        payload = {
            "subscription_id": s["sub_id"], "room": room, "seq": row["seq"],
            "type": row["type"], "meta_kind": meta.get("kind"),
            "for_seat": meta.get("for_seat"), "sender": row["from_seat"],
            "has_attachment": has_att,                   # F14
        }
        cur = con.execute(
            "INSERT OR IGNORE INTO wake_queue (sub_id, room, seq, payload, next_attempt_at,"
            " created_at) VALUES (?,?,?,?,?,?)",
            (s["sub_id"], room, row["seq"], canonicalize(payload), now_iso(), now_iso()))
        n += cur.rowcount
    return n


def _sign_wake(secret: str, ts: str, nonce: str, body: str) -> str:
    base = f"{ts}.{nonce}.{body}".encode()               # F5: timestamp+nonce+body
    return hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()


def deliver_pending_wakes(limit: int = 50, now: str | None = None) -> dict:
    """Bounded-retry delivery worker. Also called synchronously by conformance
    (with `now` advanced, the sweep-style test hook). A missed wake is never a
    lost message — the cursor API is the source of truth."""
    stats = {"delivered": 0, "failed": 0, "disabled": 0, "rate_limited": 0}
    now_dt = datetime.fromisoformat(now) if now else datetime.now(timezone.utc)
    now_s = now_dt.isoformat(timespec="milliseconds")
    with db() as con:
        due = con.execute(
            "SELECT * FROM wake_queue WHERE delivered_at IS NULL AND next_attempt_at<=?"
            " ORDER BY qid LIMIT ?", (now_s, limit)).fetchall()
    for q in due:
        with db() as con:
            s = con.execute("SELECT * FROM subscriptions WHERE sub_id=?", (q["sub_id"],)).fetchone()
            if s is None or s["disabled_at"]:
                with write_tx() as w:
                    w.execute("UPDATE wake_queue SET delivered_at=? WHERE qid=?",
                              (now_s, q["qid"]))     # drop orphan
                continue
            # F9: per-subscription rate cap over the last minute
            recent = con.execute(
                "SELECT COUNT(*) c FROM wake_queue WHERE sub_id=? AND delivered_at IS NOT NULL"
                " AND delivered_at > ?", (q["sub_id"],
                (now_dt - timedelta(minutes=1)).isoformat(timespec="milliseconds"))
            ).fetchone()["c"]
            if recent >= WAKE_RATE_PER_MIN:
                with write_tx() as w:
                    w.execute("UPDATE wake_queue SET delivered_at=?, last_error=? WHERE qid=?",
                              (now_s, "rate_limited", q["qid"]))
                    append_admin_envelope(w, q["room"], "wake_rate_limited", s["seat"],
                                          f"Wake rate cap ({WAKE_RATE_PER_MIN}/min) reached for "
                                          f"{s['seat']}; notifications dropped (visible, not silent).")
                stats["rate_limited"] += 1
                continue
        ts = now_s
        nonce = secrets.token_hex(12)
        body = q["payload"]
        sig = _sign_wake(s["secret"], ts, nonce, body)
        req = urllib.request.Request(
            s["url"], data=body.encode(), method="POST",
            headers={"Content-Type": "application/json", "X-HAK-Timestamp": ts,
                     "X-HAK-Nonce": nonce, "X-HAK-Signature": "sha256=" + sig})
        ok, err = False, None
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                ok = 200 <= resp.status < 300
                err = None if ok else f"http {resp.status}"
        except Exception as e:                            # noqa: BLE001 — any failure = retry
            err = str(e)[:200]
        with write_tx() as w:
            if ok:
                w.execute("UPDATE wake_queue SET delivered_at=?, last_error=NULL WHERE qid=?",
                          (ts, q["qid"]))
                w.execute("UPDATE subscriptions SET last_delivered_seq=?, failure_count=0 "
                          "WHERE sub_id=?", (q["seq"], q["sub_id"]))
                stats["delivered"] += 1
            else:
                attempts = q["attempts"] + 1
                if attempts >= WAKE_MAX_ATTEMPTS:
                    # F10: name the hole — what was missed while deaf
                    first_missed = w.execute(
                        "SELECT MIN(seq) m FROM wake_queue WHERE sub_id=? AND delivered_at IS NULL",
                        (q["sub_id"],)).fetchone()["m"]
                    w.execute("UPDATE subscriptions SET disabled_at=?, disabled_reason=?,"
                              " failure_count=?, first_undelivered_seq=? WHERE sub_id=?",
                              (ts, err, attempts, first_missed, q["sub_id"]))
                    w.execute("UPDATE wake_queue SET delivered_at=?, last_error=? WHERE qid=?",
                              (ts, "subscription disabled", q["qid"]))
                    append_admin_envelope(
                        w, q["room"], "subscription_disabled", s["seat"],
                        f"Subscription for {s['seat']} disabled after {attempts} failed deliveries "
                        f"({err}). last_delivered_seq={s['last_delivered_seq']} "
                        f"first_undelivered_seq={first_missed} — re-enable and pull from there.")
                    stats["disabled"] += 1
                else:
                    nxt = (now_dt + timedelta(seconds=WAKE_BACKOFF_SEC * attempts)).isoformat(
                        timespec="milliseconds")
                    w.execute("UPDATE wake_queue SET attempts=?, next_attempt_at=?, last_error=? "
                              "WHERE qid=?", (attempts, nxt, err, q["qid"]))
                    w.execute("UPDATE subscriptions SET failure_count=? WHERE sub_id=?",
                              (attempts, q["sub_id"]))
                    stats["failed"] += 1
    return stats


@app.post("/v1/rooms/{room}/subscriptions", status_code=201)
def create_subscription(room: str, payload: SubscriptionIn, request: Request):
    seat = require_room_member(request, room)[0]
    with db() as con:
        _require_room_exists(con, room)
        if not _wake_enabled(con, room):
            raise error(409, "wake_hooks_disabled",
                        "This room has not enabled wake-hooks; enabling them accepts the "
                        "woken-turn authorization charter (F15). Ask an admin:")
    _validate_wake_filter(payload.filter)          # 422 on values that could never match
    target_seat = seat
    if payload.seat and payload.seat != seat:
        with db() as con:
            if not is_admin(con, room, seat):
                raise error(403, "forbidden", "only admins may subscribe on behalf of a seat")
        target_seat = payload.seat
    sub_id = "w_" + secrets.token_hex(6)
    secret = payload.secret or secrets.token_urlsafe(32)
    with write_tx() as con:
        con.execute(
            "INSERT INTO subscriptions (sub_id, room, seat, url, filter, secret, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (sub_id, room, target_seat, payload.url, canonicalize(payload.filter), secret,
             now_iso()))
        append_admin_envelope(con, room, "subscription_created", target_seat,
                              f"{seat} created a wake subscription for {target_seat} "
                              f"(filter {canonicalize(payload.filter)}).")
    return {"sub_id": sub_id, "seat": target_seat, "secret": secret,
            "note": "the secret is the HMAC key for X-HAK-Signature; store it on the consumer"}


@app.get("/v1/rooms/{room}/subscriptions")
def list_subscriptions(room: str, request: Request):
    seat = require_room_member(request, room)[0]
    with db() as con:
        if is_admin(con, room, seat):
            rows = con.execute("SELECT * FROM subscriptions WHERE room=? ORDER BY created_at",
                               (room,)).fetchall()
        else:
            rows = con.execute("SELECT * FROM subscriptions WHERE room=? AND seat=? "
                               "ORDER BY created_at", (room, seat)).fetchall()
    return {"subscriptions": [
        {"sub_id": r["sub_id"], "seat": r["seat"], "url": r["url"],
         "filter": json.loads(r["filter"]), "created_at": r["created_at"],
         "disabled_at": r["disabled_at"], "disabled_reason": r["disabled_reason"],
         "last_delivered_seq": r["last_delivered_seq"],
         "first_undelivered_seq": r["first_undelivered_seq"],
         "failure_count": r["failure_count"]} for r in rows]}


@app.delete("/v1/rooms/{room}/subscriptions/{sub_id}", status_code=200)
def delete_subscription(room: str, sub_id: str, request: Request):
    seat = require_room_member(request, room)[0]
    with write_tx() as con:
        s = con.execute("SELECT * FROM subscriptions WHERE room=? AND sub_id=?",
                        (room, sub_id)).fetchone()
        if s is None:
            raise error(404, "not_found", "subscription not found")
        if s["seat"] != seat and not is_admin(con, room, seat):
            raise error(403, "forbidden", "only the owning seat or an admin may delete")
        con.execute("DELETE FROM subscriptions WHERE sub_id=?", (sub_id,))
        con.execute("UPDATE wake_queue SET delivered_at=?, last_error='subscription deleted' "
                    "WHERE sub_id=? AND delivered_at IS NULL", (now_iso(), sub_id))
        append_admin_envelope(con, room, "subscription_deleted", s["seat"],
                              f"{seat} deleted the wake subscription for {s['seat']}.")
    return {"deleted": sub_id}


# ------------------------------------------------- assets: D49 registry

def _norm_asset_uri(uri: str) -> str:
    """D34 normalization, with the path component CASE-SENSITIVE (F3/F8):
    scheme lowercased, everything after '://' byte-exact. Registry identity and
    scope identity must agree or entries and claims drift (pi-50's probe)."""
    if "://" not in uri:
        raise error(422, "invalid_asset_uri", "asset_uri must be scheme://path")
    scheme, rest = uri.split("://", 1)
    scheme = scheme.lower()
    if not scheme or not rest:
        raise error(422, "invalid_asset_uri", "empty scheme or path")
    if scheme == "file" and not rest.startswith("/"):
        raise error(422, "invalid_asset_uri",
                    "canonical file form is file:/// (three slashes) + absolute path (D34)")
    return f"{scheme}://{rest}"


def _asset_row_out(r: sqlite3.Row) -> dict:
    return {
        "asset_uri": r["asset_uri"], "kind": r["kind"], "owner_seat": r["owner_seat"],
        "capacity": r["capacity"], "access": json.loads(r["access"]),
        "facts": json.loads(r["facts"]), "notes": r["notes"],
        "created_at": r["created_at"], "updated_at": r["updated_at"],
        "updated_by": r["updated_by"], "asset_seq": r["asset_seq"],
        "retired": bool(r["retired_at"]),
    }


def _credential_alerts(con: sqlite3.Connection, room: str) -> list[dict]:
    """C20 + C20b, server-derived:
    collision = same (location, alias) with DIFFERENT fingerprint (F1);
    missing   = last explicit verify did not find a registered credential (F1/#113).
    Same location with different aliases is NOT a collision (a healthy ssh config)."""
    rows = con.execute(
        "SELECT * FROM assets WHERE room=? AND kind='credential' AND retired_at IS NULL",
        (room,)).fetchall()
    by_key: dict[tuple, list[sqlite3.Row]] = {}
    for r in rows:
        f = json.loads(r["facts"])
        key = (str(f.get("location", "")), str(f.get("alias", "")))
        by_key.setdefault(key, []).append(r)
    alerts: list[dict] = []
    for (loc, alias), group in by_key.items():
        prints = {str(json.loads(r["facts"]).get("fingerprint", "")) for r in group}
        if len(prints) > 1:
            alerts.append({"kind": "collision", "location": loc, "alias": alias,
                           "fingerprints": sorted(prints),
                           "assets": [r["asset_uri"] for r in group]})
    for r in rows:
        chk = con.execute(
            "SELECT status FROM asset_checks WHERE room=? AND asset_uri=? "
            "ORDER BY checked_at DESC LIMIT 1", (room, r["asset_uri"])).fetchone()
        if chk is not None and chk["status"] == "missing":
            alerts.append({"kind": "missing", "asset_uri": r["asset_uri"],
                           "owner_seat": r["owner_seat"]})
    return alerts


@app.get("/v1/rooms/{room}/assets")
def list_assets(room: str, request: Request, kind: str | None = None):
    require_room_member(request, room)
    with db() as con:
        _require_room_exists(con, room)
        q = "SELECT * FROM assets WHERE room=? AND retired_at IS NULL"
        args: list[Any] = [room]
        if kind:
            q += " AND kind=?"
            args.append(kind)
        q += " ORDER BY kind, asset_uri"
        rows = con.execute(q, args).fetchall()
        alerts = _credential_alerts(con, room)
    return {"assets": [_asset_row_out(r) for r in rows], "alerts": alerts}


@app.post("/v1/rooms/{room}/assets", status_code=201)
def register_asset(room: str, payload: AssetIn, request: Request):
    seat = require_room_admin(request, room)          # V2: admins only
    if payload.kind not in ASSET_KINDS:
        raise error(422, "invalid_asset_kind", f"kind must be one of {sorted(ASSET_KINDS)}")
    uri = _norm_asset_uri(payload.asset_uri)
    owner = payload.owner_seat or seat
    facts = dict(payload.facts)
    if payload.kind == "credential":
        # D49.3: facts only, never material. Reject the obvious mistakes loudly.
        for banned in ("key", "private_key", "secret", "token", "passphrase", "password"):
            if banned in facts:
                raise error(422, "credential_material_rejected",
                            f"'{banned}' is secret material; the registry stores fingerprints "
                            "and locations only — secrets stay on the host (D49.3)")
        if not facts.get("fingerprint") or not facts.get("location"):
            raise error(422, "credential_facts_incomplete",
                        "credential facts require at least 'fingerprint' and 'location' "
                        "(and 'alias' for collision detection, C20)")
    access = [{"seat": a.seat, "level": a.level} for a in payload.access]
    with write_tx() as con:
        _require_room_exists(con, room)
        prev = con.execute("SELECT * FROM assets WHERE room=? AND asset_uri=?",
                           (room, uri)).fetchone()
        nxt = con.execute("SELECT COALESCE(MAX(asset_seq),0)+1 AS n FROM assets WHERE room=?",
                          (room,)).fetchone()["n"]
        if prev and not prev["retired_at"]:
            if prev["kind"] != payload.kind or canonicalize(json.loads(prev["facts"])) != canonicalize(facts):
                raise error(409, "asset_conflict",
                            "asset exists with different kind/facts; use PATCH to update (D49)")
            return JSONResponse(status_code=200, content=_asset_row_out(prev))  # idempotent
        ts = now_iso()
        con.execute(
            "INSERT INTO assets (room, asset_uri, kind, owner_seat, capacity, access, facts,"
            " notes, created_at, updated_at, updated_by, retired_at, asset_seq)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(room, asset_uri) DO UPDATE SET kind=excluded.kind,"
            " owner_seat=excluded.owner_seat, capacity=excluded.capacity, access=excluded.access,"
            " facts=excluded.facts, notes=excluded.notes, updated_at=excluded.updated_at,"
            " updated_by=excluded.updated_by, retired_at=NULL",
            (room, uri, payload.kind, owner, payload.capacity, canonicalize(access),
             canonicalize(facts), payload.notes, ts, ts, seat, None, nxt))
        append_admin_envelope(con, room, "asset_register", uri,
                              f"{seat} registered {payload.kind} asset {uri} (owner {owner}).")
        row = con.execute("SELECT * FROM assets WHERE room=? AND asset_uri=?",
                          (room, uri)).fetchone()
    return _asset_row_out(row)


@app.get("/v1/rooms/{room}/assets/{asset_uri_b64}")
def get_asset(room: str, asset_uri_b64: str, request: Request):
    require_room_member(request, room)
    uri = base64.urlsafe_b64decode(asset_uri_b64 + "=" * (-len(asset_uri_b64) % 4)).decode()
    with db() as con:
        r = con.execute("SELECT * FROM assets WHERE room=? AND asset_uri=?",
                        (room, uri)).fetchone()
        if r is None:
            raise error(404, "not_found", "asset not registered")
        claims = con.execute(
            "SELECT * FROM scope_events WHERE room=? AND resource_uri=? ORDER BY scope_seq",
            (room, uri)).fetchall()
    out = _asset_row_out(r)
    out["claim_history"] = [dict(c) for c in claims]
    return out


@app.patch("/v1/rooms/{room}/assets/{asset_uri_b64}")
def update_asset(room: str, asset_uri_b64: str, payload: AssetIn, request: Request):
    seat = require_room_admin(request, room)
    uri = base64.urlsafe_b64decode(asset_uri_b64 + "=" * (-len(asset_uri_b64) % 4)).decode()
    facts = dict(payload.facts)
    access = [{"seat": a.seat, "level": a.level} for a in payload.access]
    with write_tx() as con:
        r = con.execute("SELECT * FROM assets WHERE room=? AND asset_uri=?",
                        (room, uri)).fetchone()
        if r is None or r["retired_at"]:
            raise error(404, "not_found", "asset not registered (or retired)")
        con.execute(
            "UPDATE assets SET owner_seat=?, capacity=?, access=?, facts=?, notes=?,"
            " updated_at=?, updated_by=? WHERE room=? AND asset_uri=?",
            (payload.owner_seat or r["owner_seat"], payload.capacity, canonicalize(access),
             canonicalize(facts), payload.notes, now_iso(), seat, room, uri))
        append_admin_envelope(con, room, "asset_update", uri, f"{seat} updated asset {uri}.")
        row = con.execute("SELECT * FROM assets WHERE room=? AND asset_uri=?",
                          (room, uri)).fetchone()
    return _asset_row_out(row)


@app.delete("/v1/rooms/{room}/assets/{asset_uri_b64}", status_code=200)
def retire_asset(room: str, asset_uri_b64: str, request: Request):
    seat = require_room_admin(request, room)
    uri = base64.urlsafe_b64decode(asset_uri_b64 + "=" * (-len(asset_uri_b64) % 4)).decode()
    with write_tx() as con:
        r = con.execute("SELECT * FROM assets WHERE room=? AND asset_uri=?",
                        (room, uri)).fetchone()
        if r is None:
            raise error(404, "not_found", "asset not registered")
        con.execute("UPDATE assets SET retired_at=?, updated_at=?, updated_by=? "
                    "WHERE room=? AND asset_uri=?", (now_iso(), now_iso(), seat, room, uri))
        append_admin_envelope(con, room, "asset_retire", uri,
                              f"{seat} retired asset {uri} (tombstone; history preserved).")
    return {"retired": uri}


@app.post("/v1/rooms/{room}/assets/verify")
def verify_assets(room: str, payload: AssetVerifyIn, request: Request):
    """C20b: explicit admin verification (never a background probe). The admin
    reports which registered credentials were found on the host; the server
    records present/missing per asset and surfaces MISSING in list_assets.
    This is the #113 case: destruction by absence, invisible to a collision check."""
    seat = require_room_admin(request, room)
    present = {_norm_asset_uri(u) for u in payload.present}
    with write_tx() as con:
        rows = con.execute(
            "SELECT asset_uri FROM assets WHERE room=? AND kind='credential' "
            "AND retired_at IS NULL", (room,)).fetchall()
        missing = []
        for r in rows:
            status = "present" if r["asset_uri"] in present else "missing"
            if status == "missing":
                missing.append(r["asset_uri"])
            con.execute(
                "INSERT INTO asset_checks (room, asset_uri, checked_at, status, detail, checked_by)"
                " VALUES (?,?,?,?,?,?)",
                (room, r["asset_uri"], now_iso(), status, payload.detail, seat))
        append_admin_envelope(con, room, "asset_verify", seat,
                              f"{seat} verified credentials: {len(rows) - len(missing)} present, "
                              f"{len(missing)} missing.")
    return {"verified": len(rows), "missing": missing}


# ------------------------------------------------- GC sweep (D18/D23/D30/D44)

def sweep_once() -> dict:
    """The only deleter (D23). DB-authoritative (D44): mark + envelope commit;
    unlink after commit, retried by later sweeps until successful.
    24h grace (D30): files younger than 24h are never GC'd, regardless of
    reference status. Age GC: older than GC_DAYS and unreferenced.
    Convergence: a row is fully deleted when deletion_pending=1 AND the unlink
    finally succeeded (recorded as deleted_at); pending rows are retried every
    pass and never re-swept as fresh."""
    stats = {"marked": 0, "unlinked": 0}
    with db() as con:
        files = con.execute("SELECT * FROM files").fetchall()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=GC_DAYS)   # older than this + unreferenced -> GC
    grace = now - timedelta(hours=GRACE_HOURS)  # younger than this -> never GC
    for f in files:
        if f["deleted_at"]:
            continue                          # fully gone
        if f["deletion_pending"]:
            # retry the outstanding unlink (D44) — may already be absent
            p = UPLOADS_DIR / f["file_id"]
            try:
                p.unlink(missing_ok=True)
            except OSError:
                continue                      # retried next pass
            with write_tx() as con:
                con.execute("UPDATE files SET deleted_at=? WHERE file_id=?",
                            (now_iso(), f["file_id"]))
            stats["unlinked"] += 1
            continue
        created = datetime.fromisoformat(f["created_at"])
        if created > grace or created > cutoff:
            continue                          # within grace, or not yet GC-age
        if _is_referenced(f["file_id"], f["room"]):
            continue
        # 1) authoritative commit: mark + envelope, one transaction (D40/D44)
        with write_tx() as con:
            con.execute("UPDATE files SET deletion_pending=1 WHERE file_id=?", (f["file_id"],))
            append_admin_envelope(con, f["room"], "attachment_delete", f["file_id"],
                                  f"Attachment {f['name']} unreferenced for {GC_DAYS}d; "
                                  f"deletion committed (DB-authoritative, D44).")
        stats["marked"] += 1
        # 2) best-effort unlink now; retried by later sweeps until successful
        p = UPLOADS_DIR / f["file_id"]
        try:
            p.unlink(missing_ok=True)
        except OSError:
            continue
        with write_tx() as con:
            con.execute("UPDATE files SET deleted_at=? WHERE file_id=? AND deletion_pending=1",
                        (now_iso(), f["file_id"]))
        stats["unlinked"] += 1
    return stats


def _is_admin_anywhere(con: sqlite3.Connection, seat: str) -> bool:
    rooms = con.execute(
        "SELECT room FROM memberships WHERE seat=? AND status='member'", (seat,)).fetchall()
    return any(is_admin(con, r["room"], seat) for r in rooms)


def _is_referenced(file_id: str, room: str) -> bool:
    """D23: referenced = file_id appears in any envelope's attachments or refs
    of that room (retracted envelopes still count)."""
    with db() as con:
        rows = con.execute("SELECT attachments, refs FROM messages WHERE room=?", (room,)).fetchall()
    for r in rows:
        for field in ("attachments", "refs"):
            if r[field] and file_id in r[field]:
                return True
    return False


# ------------------------------------------------- CLI bootstrap (D24)

def bootstrap_operator(label: bool = True) -> str:
    """D24 recovery: ROTATE the operator token. Every live operator token is
    revoked first — recovery running means the old secret is presumed lost —
    then a fresh one is issued and a token_bootstrap envelope appended.
    Returns the secret."""
    init_db()
    secret = secrets.token_urlsafe(32)
    with write_tx() as con:
        con.execute("UPDATE tokens SET revoked_at=? WHERE seat='operator' AND revoked_at IS NULL",
                    (now_iso(),))
        con.execute(
            "INSERT INTO tokens (token_id, seat, token_hash, created_at) VALUES (?,?,?,?)",
            ("t_bootstrap_" + secrets.token_hex(4), "operator",
             sha256_hex(secret.encode()), now_iso()))
        room_row = con.execute("SELECT name FROM rooms ORDER BY created_at LIMIT 1").fetchone()
        if room_row:
            append_admin_envelope(con, room_row["name"], "token_bootstrap", "operator",
                                  "Operator admin token rotated via host-local bootstrap (D24).")
    if label:
        print(f"operator token (shown once): {secret}")
    return secret


def has_live_operator_token() -> bool:
    if not Path(DB_PATH).exists():
        return False
    with db() as con:
        row = con.execute(
            "SELECT 1 FROM tokens WHERE seat='operator' AND revoked_at IS NULL").fetchone()
    return row is not None


def token_file_live(token_file: str) -> bool:
    """The file's secret hashes to a live (unrevoked) token row.
    Missing/empty/tampered file → False: the secret is not usable."""
    p = Path(token_file)
    if not p.exists():
        return False
    tok = p.read_text().strip()
    if not tok:
        return False
    with db() as con:
        row = con.execute(
            "SELECT 1 FROM tokens WHERE token_hash=? AND revoked_at IS NULL",
            (sha256_hex(tok.encode()),)).fetchone()
    return row is not None


def backup_to(dest: str) -> Path:
    """D38: hak.db and uploads/ are one backup unit. The DB is copied via the
    SQLite Online Backup API (a plain copy of a live WAL db can miss committed
    state); uploads are copied afterwards. A restore needs BOTH."""
    dest_dir = Path(dest)
    if dest_dir.exists() and any(dest_dir.iterdir()):
        raise SystemExit(f"backup destination not empty: {dest_dir}")
    dest_dir.mkdir(parents=True)
    src = sqlite3.connect(DB_PATH)
    dst = sqlite3.connect(dest_dir / "hak.db")
    with dst:
        src.backup(dst)          # online backup API — safe under WAL traffic
    src.close()
    dst.close()
    shutil.copytree(UPLOADS_DIR, dest_dir / "uploads", dirs_exist_ok=True)
    return dest_dir


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--print-config":
        # Shell-sourceable resolved values (env > hak.toml > defaults) for run.sh.
        print(f"HAK_DB='{DB_PATH}'")
        print(f"HAK_UPLOADS='{UPLOADS_DIR}'")
        print(f"HAK_SWEEP_INTERVAL='{SWEEP_INTERVAL}'")
        print(f"HAK_HOST='{BIND_HOST}'")
        print(f"HAK_PORT='{BIND_PORT}'")
    elif len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        if "--seat" in sys.argv and "operator" in sys.argv[sys.argv.index("--seat") + 1:]:
            bootstrap_operator()
        else:
            print("usage: python3 hak.py --bootstrap --seat operator")
    elif len(sys.argv) > 1 and sys.argv[1] == "--ensure-operator":
        # Reconcile with run.sh's token file. Single source of truth for the
        # file↔DB state — the caller must NOT re-check on its own (that dual
        # check diverged once and wrote an empty token file).
        #   file live in DB      → no-op (empty stdout, exit 0)
        #   file gone/invalid    → D24 recovery: rotate (old secret presumed lost)
        # No --token-file (manual use): check-only, never rotates — use
        # --bootstrap --seat operator for an unconditional rotation.
        if "--token-file" in sys.argv:
            tf = sys.argv[sys.argv.index("--token-file") + 1]
            if token_file_live(tf):
                print("operator token file present and live; no action", file=sys.stderr)
                sys.exit(0)
            print("file missing/invalid but operator token(s) live — rotating "
                  "(D24: secret presumed lost)", file=sys.stderr)
            print(bootstrap_operator(label=False))
        elif has_live_operator_token():
            print("a live operator token exists; not rotating. If its secret is "
                  "lost: hak.py --bootstrap --seat operator (unconditional), "
                  "or --ensure-operator --token-file to reconcile with run.sh",
                  file=sys.stderr)
            sys.exit(0)
        else:
            print(bootstrap_operator(label=False))
    elif len(sys.argv) > 1 and sys.argv[1] == "--sweep":
        init_db()
        print(json.dumps(sweep_once()))
    elif len(sys.argv) > 1 and sys.argv[1] == "--backup":
        if len(sys.argv) < 3:
            print("usage: python3 hak.py --backup <dir>")
            sys.exit(2)
        p = backup_to(sys.argv[2])
        print(f"backup written: {p} (db + uploads; restore needs both, D38)")
    else:
        print("usage: python3 hak.py --bootstrap --seat operator | --ensure-operator | --sweep | --backup <dir>")
