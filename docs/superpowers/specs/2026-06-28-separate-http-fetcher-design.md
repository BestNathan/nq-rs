# Separate HTTP Fetcher from WebSocket Connection

**Date**: 2026-06-28
**Status**: approved

## Problem

`InstrumentFetcher` calls `Connection.call_api()` which routes REST-like API requests (e.g., `get_instruments`) through the WebSocket connection. This couples option discovery to WebSocket connection health. When connections enter a reconnect death spiral, option discovery also fails — compounding the problem.

## Design

Decouple REST API calls from the WebSocket connection by giving `InstrumentFetcher` its own HTTP client.

### Before

```
InstrumentFetcher ──call_api()──▶ Connection ──WebSocket──▶ Deribit
                                   (shared WS)
```

### After

```
InstrumentFetcher ──HTTP GET──▶ reqwest::Client ──proxy──▶ Deribit REST API
                                   (independent)

subscribe/resubscribe ──call_api()──▶ Connection ──WebSocket──▶ Deribit
                                      (unchanged)
```

### Changes

| Component | Change | Detail |
|-----------|--------|--------|
| `InstrumentFetcher` | Replace `Arc<Connection>` with `reqwest::Client` | `fetch_options()` sends `GET /api/v2/public/get_instruments?currency=BTC&kind=option` |
| `main.rs` | Build HTTP client, pass to Fetcher | Reuses `ALL_PROXY` env var (reqwest reads it automatically) |
| `Fetcher::fetch_options()` | Parse JSON response into `GetInstrumentsResponse` | Response format is `{ "jsonrpc": "2.0", "result": [...], "id": ... }` — same as WS, but wrapped in HTTP |

### NOT changed

- `Connection`, `ConnectionPool`, `call_api()`, biased select, batch delays — all preserved
- `SubscriptionManager`, `TickerRouter` — unchanged
- subscribe/resubscribe flow through WebSocket — unchanged

### Error handling

- HTTP errors (non-2xx) → `warn!` and return empty vec for that currency (same as current behavior)
- Timeout → same as current (reqwest default timeout, configurable later)
- Proxy unreachable → same as current (reqwest error surfaced as `anyhow::Error`)

## Rationale

- **Conservative scope**: Only touches Fetcher and main.rs; Connection untouched.
- **Eliminates coupling**: Fetcher no longer needs a live WebSocket to discover options.
- **Simpler debugging**: REST API calls are visible in logs as plain HTTP requests.
- **Lays groundwork**: If we later want to move subscribe to HTTP REST or make other API calls independent, the pattern is established.
