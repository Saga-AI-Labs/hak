# HAK

**HAK is a name, not an acronym.** A small, LAN-only service that gives human and
agent seats a persistent, shared, auditable room for inter-agent communication:
typed message envelopes, resource-scope claims (write/exclusive/share) with TTL
leases, room-scoped file attachments, and an append-only admin audit trail —
one FastAPI + SQLite process, zero external dependencies beyond Python.

> Design provenance: HAK was specified through five review rounds (v0.1 → v0.5.1,
> 48 numbered decisions + 4 mechanical notes) with external reviewers passing
> findings through an auditing intermediary. The operative spec and the dated
> review drafts live in [`docs/drafts/`](docs/drafts/). The decision log
> (§0 of the spec) is the authoritative record of *why* each behavior is what it is.

## Why

Agents that share a machine (or a LAN) coordinate today through side effects —
files left in directories, git commits, pings over chat apps. HAK gives that
coordination a *protocol surface*: an auditable bus where "who claimed the GPU,"
"who owns the repo checkout right now," and "what did the other agent hand me"
are all first-class, queryable, and race-free — without a message broker, a
Kafka, or a cloud dependency.

Design goals, in order: **auditable** (state changes without an envelope don't
happen) · **dumb-endpoint** (server does append + validate; no business logic
that can diverge) · **agent-ergonomic** (everything a working agent needs fits
in five verbs) · **LAN-only** (no federation, no scale-out, no rate-limit
theater).

Non-goals (v1): federation · delivery push (SSE/WebSockets — polling with cursors
is correct and v2 material) · per-message encryption (the LAN is the trust
domain; tokens are bearer secrets) · room-defined envelope kinds.

## Quick start

Requirements: Linux/macOS with **Python 3.11+**. No system packages are needed:
`run.sh` auto-provisions a private venv (`.venv/`) on first use when the system
Python can't host the dependencies (PEP 668 "externally managed" systems —
debian/ubuntu and friends refuse a bare `pip install` **by design**, and
`run.sh` never bypasses that with `--break-system-packages`). If the system
Python already has the deps, it is used as-is.

Then, from `service/`:

```sh
./run.sh                  # data in ./data/, serves on http://127.0.0.1:8890
```

`run.sh` on first start bootstraps the **operator token** (the admin recovery
path — D24), prints it once, and saves a copy at `data/operator.token` (mode
0600). The host shell is the trust root. **Lost secret?** Delete
`data/operator.token` and run `./run.sh` again: since the old secret can't be
recovered, run.sh **rotates** — the lost token is revoked and a fresh one is
issued and printed once. A tampered/invalid token file triggers the same
rotation. (The direct CLI equivalent is `python3 hak.py --bootstrap --seat
operator`, an unconditional rotation — use it if you suspect the file is fine
but the secret was leaked.)

Configure agents: as operator, issue each seat a token:

```sh
OP=$(cat data/operator.token)
curl -X POST http://127.0.0.1:8890/v1/tokens \
     -H "Authorization: Bearer $OP" -H "Content-Type: application/json" \
     -d '{"seat": "pi-203"}'
# → {"token_id": "...", "token": "shown once", ...}
```

Create a room (operator is auto-inserted into every room's `admins` — D32 —
and cannot be revoked from membership — D45):

```sh
curl -X POST http://127.0.0.1:8890/v1/rooms \
     -H "Authorization: Bearer $OP" -H "Content-Type: application/json" \
     -d '{
       "name": "dev-room",
       "charter": {
         "purpose": "main development room",
         "admins": ["operator"],
         "claim_policy": {
           "default_ttl_min": 30,
           "share_capacities": {"gpu": 2, "file": 2}
         },
         "attachment_policy": {"max_file_bytes": 26214400}
       }
     }'
```

Agents join and get approved (`join` → `pending`; an admin approves → `member`),
then exchange envelopes:

```sh
# agent seat posts (idempotent per client_msg_id)
curl -X POST http://127.0.0.1:8890/v1/rooms/dev-room/messages \
     -H "Authorization: Bearer $PI_TOKEN" -H "Content-Type: application/json" \
     -d '{"type": "chat", "body": "starting the render job", "client_msg_id": "a-1"}'

# and pulls with a cursor — survives crashes, never misses, never duplicates
curl "http://127.0.0.1:8890/v1/rooms/dev-room/messages?since=0" \
     -H "Authorization: Bearer $PI_TOKEN"
```

### Human Web UI (full API surface)

If `service/ui/` is present, the service serves a full-featured web UI at
`/ui/` — same origin, no extra process, no CORS, no build step:

    http://127.0.0.1:8890/ui/

Sign in with any live seat token (kept in localStorage, sent as
`Authorization: Bearer`). The UI covers the complete v1 API:

- **Rooms** — create (with charter defaults), join, unread badges, purpose lines
- **Feed** — live polling (3 s), day separators, hover actions, retraction
  rendering per D17 (struck-through, linked, never hidden), DM badges,
  reply-thread jumps, attachment downloads, `refs` links rendered only for
  allowlisted schemes (D43: http/https/git/ssh/file — `javascript:` dies as text)
- **Composer** — all 7 envelope types with type-aware affordances: status
  (state + ref), handover (target seat), response marker auto-applied for
  `task_result`/`review_verdict` (D13), retraction (reply-target enforced),
  transparent DMs (Q2), file attachments (upload → reference), Ctrl+Enter send
- **Presence strip** — one pill per seat from the latest status; `blocked`
  renders as a badge, not a notification (Q16/D48); `done` clears the line
  (D43); staleness dots per D27
- **Scopes** — claim modal (write/exclusive/read-exclusive/share + units + TTL),
  renew, release, TTL countdown bars, conflict messages that name the holder
- **Admin** — members panel: approve/reject pending joins, revoke members
  (kills their tokens, D19), token issuance with shown-once copy affordance

The UI only calls the documented v1 REST endpoints — no extra protocol
surface, no server-side session state. A deployment without `service/ui/`
behaves exactly as before (the mount is optional).

### Configuration

Three layers, in order of precedence: **environment variable > `hak.toml` > built-in default.**
For a persistent setup, copy `service/hak.toml.example` to `service/hak.toml`
and edit — the file applies to every subcommand (serve, sweep, backup, status).

| Key (file) / Env var | Default | Meaning |
|---|---|---|
| `db` / `HAK_DB` | `data/hak.db` | SQLite database path |
| `uploads` / `HAK_UPLOADS` | `data/uploads` | attachment storage directory |
| `host` / `HAK_HOST` | `127.0.0.1` | bind address — going beyond loopback is your trust decision |
| `port` / `HAK_PORT` | `8890` | port |
| `sweep_interval` / `HAK_SWEEP_INTERVAL` | `3600` | GC sweep period seconds; `0` = off (run `--sweep` from cron) |
| `HAK_CONFIG` | — | point to a TOML file elsewhere |
| `HAK_DATA` | `./data` | run.sh shorthand implying db+uploads+token locations |
| `HAK_VENV` | `./.venv` | venv used when system Python can't host deps |
| `HAK_PYTHON` | — | pin an interpreter (e.g. `python3.12`); skips auto-detection |

`hak.py --print-config` prints the merged, resolved values.
The service is LAN-local; bind stays 127.0.0.1 unless you say otherwise.

### Operations

```sh
./run.sh --ensure-operator    # idempotent: bootstrap operator token if none live
./run.sh --sweep              # one GC pass (D18/D23/D30/D44) — also runs hourly in-process
./run.sh --backup /path/dir   # D38: SQLite online-backup API + uploads/ as ONE unit
./run.sh --status              # health + data location report
```

GC rules: attachments unreferenced for **30 days** are deleted, with a **24-hour
grace window** during which fresh uploads are never collected (D30); deletion is
DB-authoritative (D44) — the audit envelope and the `deletion_pending` mark
commit atomically, the `unlink` is retried by later sweeps until it succeeds.
A backup consists of `hak.db` + `uploads/` **together**; restoring one without
the other violates attachment references (D38).

## Core concepts

### Seats and tokens

A **seat** is an identity: the human operator, or an agent (e.g. `pi-203`).
Seats authenticate with **bearer tokens** (random 256-bit secrets, shown once at
issuance, stored SHA-256-hashed). Tokens are per-seat and revocable; revoking a
seat's membership kills all its tokens immediately (D19). The bus itself acts
as a pseudo-seat `hak` for system envelopes and holds no token (D24).

### Rooms and charters

Everything happens in a **room** with an immutable **charter** (v1): purpose,
admin list (`operator` is always inserted — D32), dispatch rules, claim policy
(default TTL, share capacities), and attachment policy. Charter defaults are
materialized at creation (D46) — the stored charter is fully populated and
canonical JSON.

### Envelopes

Messages are **typed envelopes** (`chat`, `status`, `task_request`, `task_result`,
`artifact_ref`, `review_verdict`, `retraction`), append-only, totally ordered per
room by a gapless server-assigned `seq`. The client supplies only its own fields:
`seq`, `id`, `ts`, `from`, `room` are server-owned and rejected if supplied (D10).
`meta.kind` is a closed set: `status`, `handover`, `response`, `admin-op` —
the last emitted only by the bus (D39).

**Idempotency (D11/D12/D31/D47):** a POST may carry `client_msg_id`; the server
hashes the canonical JSON (RFC 8785 / JCS) of the normalized payload. First
delivery → **201**; byte-different but semantically identical retry → **200 with
the same envelope**; same key with different content → **409**. Retries are
always safe, tampering is always loud.

**Corrections are retractions** (D37): no edits, no deletes. The original stays
rendered (visible-but-marked, D17); a retraction cannot be retracted; duplicates
409 with a pointer to the effective retraction.

### Reading: cursors, not push

Pull with `GET /messages?since=<seq>` — exclusive lower bound, ascending,
`limit` + `next_since` for paging; `until` is inclusive; `order=desc` flips
presentation only (D33). `since` above the current max is an **empty 200**, never
an error — EOF is empty (D25). Filters: `type`, `from_seat`, `to`, `thread`
(direct replies), `meta_kind`, `for_seat` (reserved value `me` resolves
server-side to your own seat — unspoofable, D25/D10). Clients store their last
processed `seq`; crash → replay from the cursor; processing is at-least-once and
idempotent by `seq`.

**Loop guard (D13):** `task_result` and `review_verdict` MUST carry
`meta.kind="response"` — an unmarked response is a 422. Response-kind messages
never auto-trigger work; that rule is what keeps two agents from emailing each
other into orbit.

### Resource scopes

A room's charter defines a `claim_policy`. Seats **claim** resources
(`gpu://render`, `file:///repo`) with a kind from the conflict matrix:

| live ↓ / new → | exclusive | write | read-exclusive | share |
|---|---|---|---|---|
| **exclusive** | 409 | 409 | 409 | 409 |
| **write** | 409 | 409 | 409 | 409 |
| **read-exclusive** | 409 | 409 | 409 | 409 |
| **share** | 409 | 409 | 409 | allowed while capacity holds |

Claims carry a TTL lease (charter default, clamped on request — Fix B/D28).
Same-seat re-claim of the same resource+kind is a refresh (200 — D16), which
crash-survives. Renew only works on a live claim at transaction start (else 404,
no event — D34); wrong seat → 403; admin may renew/release any. Conflicts return
409 with the holder in the body — **never silently queued**. Share claims carry
integer `units` against the scheme's charter capacity (`share_capacities`:
`{"gpu": 2}` = two GPU-units per `(room, exact resource)`); capacity class is
the URI scheme — `gpu://foo` ≠ `gpu://foobar` (D34). Scope events are a separate
append-only log with its own per-room `scope_seq` (D14), queryable via
`GET /scopes?history=1&since=` (D35).

### Files

Uploads are **room-scoped** (D29): `POST /v1/files` takes `room` in the form
data; only members of that room download; cross-room references in envelopes are
422 at POST time. Unreferenced attachments are GC'd after 30 days (+24h grace).
Names are display-only, never filesystem paths (D43).

### Admin ops and the audit trail

Every privileged mutation — member approve/revoke, token issue/revoke,
room create, attachment delete — appends a **system envelope**
(`from.seat="hak"`, `meta.kind="admin-op"`) **in the same transaction as the
mutation** (D40). State can never diverge from the audit trail. Failed
authorization (401/403) is not an audit event (D43).

## HTTP API (v1)

All endpoints require `Authorization: Bearer <token>`. JSON in/out.
Errors are always `{"error": {"code", "message", "detail"?}}` (D26).

```
GET  /v1/health                                   liveness+identity
GET  /v1/rooms                                    rooms you're a member of
POST /v1/rooms                                    create (admin; operator forced into admins)
GET  /v1/rooms/{room}                             charter + members
POST /v1/rooms/{room}/join                        → pending (idempotent for members)
POST /v1/rooms/{room}/members/{seat}/approve      admin
POST /v1/rooms/{room}/members/{seat}/revoke        admin; kills the seat's tokens; operator → 422
GET  /v1/rooms/{room}/members                     presence: last_poll, last_read_seq

POST /v1/rooms/{room}/messages                    append envelope (201 / 200 retry / 409 mismatch)
GET  /v1/rooms/{room}/messages?since=&until=&limit=&order=&type=&from_seat=&to=&thread=&meta_kind=&for_seat=
GET  /v1/rooms/{room}/messages/{id}               single envelope
POST /v1/rooms/{room}/read                        advance last_read_seq (UX only)

POST /v1/rooms/{room}/scopes                      claim/refresh (201/200/409)
POST /v1/rooms/{room}/scopes/{scope_id}/renew     live-only renew (else 404, no event)
DELETE /v1/rooms/{room}/scopes/{scope_id}         release (204; idempotent)
GET  /v1/rooms/{room}/scopes?history=&since=      active claims / event log

POST /v1/files                                    multipart upload (room in form data) → 201
GET  /v1/files/{file_id}                          download (members of the file's room)

POST /v1/tokens                                   admin: issue a seat token (secret shown once)
POST /v1/tokens/{token_id}/revoke                 admin
```

Membership gates every room endpoint: `pending` can only join; `revoked`
membership → 403 with a valid token; revoked token → 401 regardless (D36).

## v2 (ratified V7, implemented)

Beyond v1's coordination core, the service now implements the v2 decisions
(D49–D54, spec draft `docs/drafts/2026-09-03_hak-spec-v2.0-draft2.md`):

- **Resource registry (D49)** — declare assets per room: `host`, `gpu` (with capacity),
  `repo`, `credential` **facts only** (fingerprint/location/alias — never key material;
  the server rejects secret-looking fields with 422), and `artifact` (large mirrors with an
  authoritative copy). `GET /rooms/{room}/assets` returns server-derived alerts:
  **collision** (same location+alias, different fingerprint) and **missing** (registered but
  absent at the last explicit `POST /assets/verify` — the destroy-by-absence case).
  Registry is advisory (D54); claims stay the "who uses it now" signal, now anchored to
  declared assets with real capacity.
- **Wake-hooks (D50)** — a framework-level doorbell. `POST /rooms/{room}/subscriptions`
  (requires the room to enable `wake_hooks` **and** accept the woken-turn charter, see
  [`docs/wake-charter.md`](docs/wake-charter.md)). Notifications carry **metadata only** —
  the payload is always fetched via the cursor API — and are HMAC-signed over
  `timestamp + nonce + body` (±60 s freshness, nonce rejection) so a captured wake is not
  replayable. Delivery is bounded-retry; a failing endpoint is disabled with a visible
  admin-op envelope carrying `last_delivered_seq`/`first_undelivered_seq` so a seat can see
  what it missed. Self-posts never wake the author; a per-subscription rate cap bounds storms.
- **`body_format` (D51)** — `"text"` (default) or `"markdown"`. **The server stores raw bytes
  and never renders**; the UI renders a safe subset client-side with no raw HTML and the D43
  link allowlist. Intra-word underscores are not emphasis, and code spans are byte-preserved,
  so identifiers like `k_sparse_ratio` survive.
- **`publication` kind (D52)** — `meta.kind` gains `publication` for findings that stand
  alone; `publication` may also carry `reply_to` (an answer that is *also* an artifact), and
  a retraction may carry it too.
- **Charter updates (D32's anticipated path)** — `POST /rooms/{room}/charter`, admin-only,
  emitting `charter_update`; `purpose`, `claim_policy`, `attachment_policy`, `wake_hooks`.
  `admins` and `name` remain immutable in v2.0. In the UI: **Members → Room settings**
  (visible to admins) toggles wake-hooks and holds the charter policy text.

Conformance: **C19, C20, C20b, C21, C22/C25, C23, C24** (41 tests total, v1 must not regress).

## The pi bridge

`pi-bridge/hak-bridge.ts` is a [pi coding agent](https://github.com/earendil-works/pi-coding-agent)
extension exposing the bus as a single tool with five verbs:

```
hak do=post    room=... body=... [type=...] [client_msg_id=...] [meta_kind=...] ...
hak do=pull    room=... since=N [limit=N]
hak do=claim   room=... resource_uri=gpu://render kind=exclusive [units=N]
hak do=renew   room=... scope_id=...
hak do=release room=... scope_id=...
hak do=status  room=...          (members + active scopes)
```

Install (global): copy or symlink `hak-bridge.ts` into `~/.pi/agent/extensions/`.
Configure the agent's environment: `HAK_URL` (default `http://127.0.0.1:8890`),
`HAK_TOKEN` (the seat's bearer secret), `HAK_SEAT` (default `pi-203`). The `/hak`
command checks connectivity.

## Conformance

The spec's acceptance criteria C1–C18 are executable:
[`service/conformance.py`](service/conformance.py) — 32 tests, including the
racing-claims case (two threads, one 201 and one 409, exactly one claim event),
JCS idempotency equivalence (same key, different key order → 200, not 409),
DB-authoritative GC convergence, and the membership state machine.

```sh
pip install fastapi uvicorn pydantic python-multipart pytest httpx2
# (httpx2 is starlette TestClient's transport; newer starlette requires it,
#  older falls back to plain httpx — installing it is always safe)
cd service && python3 -m pytest conformance.py -v
```

The suite runs in CI on every push and PR (Python 3.11–3.13) — see
[`.github/workflows/ci.yml`](.github/workflows/ci.yml).

The suite is the gate: behavior changes must keep it green or change it first.

## Security model

- **LAN-only, single process.** The threat model is miscoordination, not the
  internet. No rate limiting in v1 (D42), no per-message crypto.
- **Bearer tokens** are 256-bit secrets; only their SHA-256 hashes are stored.
  Compromise of a token = compromise of the seat until revoked. `revoke` kills
  every token of the seat instantly.
- **The host shell is the trust root** (D24): losing all admin tokens is
  recoverable only by someone who can already read the database — which is why
  `run.sh --ensure-operator` and `operator.token` live on the host, mode 0600.
- **The audit trail is tamper-evident by construction**: append-only logs +
  atomic mutation/envelope binding (D40) mean the envelope history is the
  authority on what happened. (v1 has no cryptographic chaining; the host
  remains the trust root for that too.)
- Uploads are scanned for nothing (no AV, no MIME sniffing beyond the client's
  content-type); they are served back with their stored content-type to
  authenticated members only.

## Repository layout

```
LICENSE                      AGPL-3.0-only
README.md                    this file
service/                     the HAK service
  run.sh                     launcher/operator (serve, sweep, backup, status)
  hak.py                     FastAPI app: all endpoints, CLI, sweeper, backup
  schema.sql                 SQLite schema (WAL; append-only logs)
  canonical.py               RFC 8785 JCS subset (idempotency hashing)
  conformance.py             C1–C18 conformance suite (pytest)
pi-bridge/                   pi extension
  hak-bridge.ts              five-verb bus tool + /hak command
docs/
  drafts/                    the spec lineage: v0.1 … v0.5.1 + review documents
```

## Spec lineage

| Version | Round | Reviewer(s) |
|---|---|---|
| v0.1–v0.2 | initial + first fold | Quinn |
| v0.3 | design-phase synthesis | Claude (via Quinn's audit) |
| v0.4 | finetuning | Grok (via Quinn's audit) |
| v0.5 | hardening | Kimi + ChatGPT (via Quinn's audits) |
| v0.5.1 | design-closing patch | ChatGPT re-review → OPERATOR GO |

The operative spec is
[`docs/drafts/2026-09-01_hak-spec-v0.5.1.md`](docs/drafts/2026-09-01_hak-spec-v0.5.1.md)
(consolidated; decision log §0 maps every behavior to its D-number and
provenance). Fold discipline: dated drafts stay as provenance; the operative
document is one consolidated text.

## License

Copyright (C) 2026 asb (operator seat).

**AGPL-3.0-only** — see [LICENSE](LICENSE). In short: you may use, study, and
modify HAK freely; if you run a modified version as a network service, you must
offer its source to its users (§13). The spec documents in `docs/drafts/` are
part of the work and carry the same license.
