# Transport Recv Dispatch Refactor

**Date**: 2026-06-29
**Status**: design-approved

## Problem

The WebSocket message receive path has two critical flaws:

1. **`ws_call()` directly consumes `transport.recv()`** during the setup phase (heartbeat, auth, resubscribe). It loops calling `recv()` and discards any non-matching messages — subscription data, heartbeats, etc. are silently lost.

2. **No single consumer of `recv()`**. During setup, `protocol::ws_call` calls `transport.recv()` directly. During the main eventloop, `connection::eventloop` calls it via `select!`. This split means there's no architectural guarantee that messages flow through a single dispatch point.

3. **Ping/pong keepalive leaks upward**. The ping timer lives in Connection's `select!` when it's a WebSocket-level concern that Transport should own.

4. **Pong timeout is a hard error**. Not receiving a pong doesn't mean the connection is dead — Deribit has its own application-level heartbeat (`public/set_heartbeat`), and many servers don't implement WS ping/pong at all.

## Design

### Layer responsibilities (after)

```
Transport:  raw WS I/O + internal ping/pong keepalive
Connection: message bus — dedicated recv dispatch loop, call() mechanism
Protocol:   consumer — uses call() for requests, registers notification handler
```

### 1. Transport: self-contained keepalive

**`recv()` gains an internal ping timer.** Instead of blocking on `ws.next()` indefinitely, `recv()` uses an internal `select!`:

```
loop {
    select! {
        msg = ws.next()          → handle text/binary/ping/pong/close
        _ = sleep_until(ping_at) → send Ping frame, reset ping_at
    }
}
```

**Pong timeout demoted to warn-only.** The server may not send pongs — Deribit has its own heartbeat mechanism. Pong timeout becomes a `warn!` log line, resetting the timestamp to avoid log spam. It does NOT return an error or trigger reconnection:

```rust
if last_pong.elapsed() > self.pong_timeout {
    warn!("pong timeout — server may not support WS ping/pong");
    self.last_pong = Some(Instant::now());
}
```

**`send_ping()` removed from the `Transport` trait.** No caller needs it anymore.

**`PongTimeout` variant removed from `TransportError`.** It's no longer an error case.

### 2. Connection: dedicated recv dispatch

The eventloop becomes recv-centric. `transport.recv()` is called in exactly **one** `select!` branch:

```
select! {
    biased;
    () = ct.cancelled()              → return Ok(())
    msg = self.message_rx.recv_async()  → transport.send(msg)
    result = transport.recv()        → dispatch (see below)
    (id, tx) = self.responser_rx.recv_async() → register in responser_map
}
```

**The recv dispatch branch:**

```
let text = transport.recv()?;
match classify(&text) {
    HasId(id) => {
        if let Some(tx) = responser_map.remove(&id) {
            let _ = tx.send(text);
        }
        // If no waiter for this id, it's a stale/unsolicited response — warn + drop
    }
    HasMethod(method) => {
        let actions = protocol.handle_notification(method, &text);
        for action in actions {
            match action {
                OutgoingAction::Send(payload) => transport.send(payload)?,
            }
        }
    }
    _ => warn!("unrecognized message, dropping"),
}
```

**Removed:**
- `message_map: HashMap<i64, String>` — the early-arrival buffer hack. The recv loop is always running before any `call()` registers a waiter, so early arrivals aren't possible.
- Ping timer branch — moved into Transport.

**`call_api()`** is the unified request mechanism. Used by both normal operations (`subscribe`, `unsubscribe`) and Protocol's setup sequence:

1. Serialize request, get id from global generator
2. Send payload on `message_tx`
3. Send `(id, oneshot_tx)` on `responser_tx`
4. Await `oneshot_rx` with timeout
5. Deserialize response

Signature unchanged from caller's perspective.

### 3. Protocol: consumer of call() + notification handler

**Removed:**
- `ws_call()` — the entire method. No more direct `transport.recv()` calls.
- Manual id ranges (700_000, 800_000, 900_000) — global id generator handles it.

**`run_setup()`** takes a `call()` interface instead of `&mut impl Transport`:

```
run_setup(caller):
    caller.call(heartbeat_payload)  → oneshot → Ok/Err
    caller.call(auth_payload)       → oneshot → Ok/Err (non-fatal)
    caller.call(subscribe_payloads) → oneshot → Ok/Err (batched)
```

**New: notification handler.** Protocol registers a callback at construction time:

```rust
fn handle_notification(&self, method: &str, data: &str) -> Vec<OutgoingAction>

enum OutgoingAction {
    Send(String),
}
```

Dispatch:
| method | action |
|--------|--------|
| `heartbeat` | metrics counter + `OutgoingAction::Send(public/test)` |
| `subscription` | broadcast to pool subscribers, no outgoing |
| anything else | warn log, no outgoing |

**Setup flow with notification safety:**
1. Connection creates Protocol, Protocol registers notification handler immediately
2. Protocol calls `run_setup()` — uses `call()` for heartbeat/auth/resubscribe
3. During setup, any arriving notifications are handled by the already-registered handler (no data loss)
4. Setup completes, Protocol is fully initialized

## Data Flow (after)

```
┌─────────────────────────────────────────────────────────┐
│ Connection Eventloop                                    │
│                                                         │
│  select! {                                              │
│    message_rx  ──► transport.send()                     │
│    transport.recv() ──► classify:                       │
│      has "id"     → responser_map.remove(id).send(text) │
│      has "method" → protocol.handle_notification(m, t)  │
│    responser_rx  ──► responser_map.insert(id, tx)       │
│  }                                                      │
│                                                         │
│  call_api(payload):                                     │
│    message_tx.send(payload)                             │
│    responser_tx.send((id, oneshot_tx))                  │
│    await oneshot_rx                                     │
│                                                         │
│  Transport.recv() (internal):                           │
│    select! {                                            │
│      ws.next()    → text/binary/ping/pong/close         │
│      sleep_until  → send Ping, reset timer              │
│    }                                                    │
│    pong timeout → warn! only (no error)                 │
└─────────────────────────────────────────────────────────┘
```

## Files changed

| File | Changes |
|------|---------|
| `transport.rs` | Move ping timer into `recv()`, demote pong timeout to warn, remove `send_ping()` from trait, remove `PongTimeout` error variant |
| `connection.rs` | Remove `message_map`, remove ping timer branch, simplify select! to 4 branches, add notification dispatch in recv branch, add `OutgoingAction` type |
| `protocol.rs` | Remove `ws_call()`, change `run_setup()` to use `call()` interface, add `handle_notification()` callback, remove manual id ranges |

## Non-changes

- `call_api()` signature stays the same — callers (`subscribe`, `unsubscribe`, `resubscribe_all`) are unaffected
- `Transport` trait otherwise unchanged — `connect`, `send`, `recv`, `close`, `is_connected` stay
- `ConnectionConfig` unchanged
- Pool, subscription broadcast — unchanged
- Reconnect loop — same outer/inner pattern, just the inner loop is simplified
