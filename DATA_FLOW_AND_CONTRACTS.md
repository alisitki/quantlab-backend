# Data Flow and Contracts

This document defines the data models, event schemas, and runtime contracts used throughout QuantLab.

---

## Exchange Sources

Data is collected from three cryptocurrency futures exchanges via WebSocket:

| Exchange | WebSocket URL | Streams |
|----------|---------------|---------|
| Binance | `wss://fstream.binance.com/stream?streams=` | BBO, Trade, Mark Price, Funding |
| Bybit | `wss://stream.bybit.com/v5/public/linear` | BBO, Trade, Mark Price, Funding, Open Interest |
| OKX | `wss://ws.okx.com:8443/ws/v5/public` | BBO, Trade, Mark Price, Funding, Open Interest |

### Collected Symbols (Default)

```
BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT,
LINKUSDT, ADAUSDT, AVAXUSDT, LTCUSDT, MATICUSDT
```

---

## Event Type Definitions

### Stream Types

```python
class StreamType(Enum):
    BBO = "bbo"                   # Best Bid/Offer
    TRADE = "trade"               # Trade execution
    OPEN_INTEREST = "open_interest"
    FUNDING = "funding"           # Funding rate
    MARK_PRICE = "mark_price"     # Mark/index price
```

### Common Fields (All Events)

| Field | Type | Description |
|-------|------|-------------|
| `ts_event` | int64 | Event timestamp (nanoseconds, exchange time) |
| `ts_recv` | int64 | Receive timestamp (nanoseconds, collector time) |
| `exchange` | string | Exchange identifier (binance, bybit, okx) |
| `symbol` | string | Trading pair (e.g., BTCUSDT) |
| `stream` | string | Stream type |
| `stream_version` | int8 | Schema version (currently 1) |

### BBO Event

```python
@dataclass
class BBOEvent:
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str = "bbo"
    bid_price: float    # Best bid price
    bid_qty: float      # Best bid quantity
    ask_price: float    # Best ask price
    ask_qty: float      # Best ask quantity
    stream_version: int = 1
```

### Trade Event

```python
@dataclass
class TradeEvent:
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str = "trade"
    price: float        # Trade price
    qty: float          # Trade quantity
    side: int           # 1 = buy, -1 = sell
    trade_id: str       # Exchange trade ID
    stream_version: int = 1
```

### Mark Price Event

```python
@dataclass
class MarkPriceEvent:
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str = "mark_price"
    mark_price: float   # Mark price
    index_price: float  # Index price (optional)
    stream_version: int = 1
```

### Funding Event

```python
@dataclass
class FundingEvent:
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str = "funding"
    funding_rate: float   # Funding rate
    next_funding_ts: int  # Next funding timestamp
    stream_version: int = 1
```

### Open Interest Event

```python
@dataclass
class OpenInterestEvent:
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str = "open_interest"
    open_interest: float  # Open interest value
    stream_version: int = 1
```

### Alignment Event (RAM-Only)

```python
@dataclass
class AlignmentEvent:
    """
    RAM-only event for gap tracking alignment after reconnects.
    NEVER written to storage.
    """
    exchange: str
    symbol: str
    bbo_ts: int = 0
    trade_ts: int = 0
    mark_price_ts: int = 0
    funding_ts: int = 0
    open_interest_ts: int = 0
```

---

## Parquet Contract

### PyArrow Schemas

All events are stored in Parquet format with the following schemas:

**BBO Schema:**
```python
BBO_SCHEMA = pa.schema([
    ("ts_event", pa.int64()),
    ("ts_recv", pa.int64()),
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("stream", pa.string()),
    ("stream_version", pa.int8()),
    ("bid_price", pa.float64()),
    ("bid_qty", pa.float64()),
    ("ask_price", pa.float64()),
    ("ask_qty", pa.float64()),
])
```

**Trade Schema:**
```python
TRADE_SCHEMA = pa.schema([
    ("ts_event", pa.int64()),
    ("ts_recv", pa.int64()),
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("stream", pa.string()),
    ("stream_version", pa.int8()),
    ("price", pa.float64()),
    ("qty", pa.float64()),
    ("side", pa.int8()),
    ("trade_id", pa.string()),
])
```

### Partitioning

Spool files use Hive-style partitioning:
```
{SPOOL_DIR}/exchange={X}/stream={Y}/symbol={Z}/date={YYYYMMDD}/part-{timestamp}-{seq}.parquet
```

Compact files in S3:
```
s3://{bucket}/exchange=binance/stream=bbo/symbol=btcusdt/date=20260115/data.parquet
```

---

## Strategy Runtime Input Contract

### Event Object (passed to strategy.onEvent)

The strategy receives each event with the following guaranteed fields:

| Field | Type | Description |
|-------|------|-------------|
| `ts_event` | BigInt | Event timestamp (nanoseconds) |
| `seq` | BigInt | Sequence number for ordering |
| `cursor` | string | Base64 encoded cursor for resume |
| `symbol` | string | Trading pair |
| `stream` | string | Stream type |
| `bid_price` | number | Best bid (BBO only) |
| `ask_price` | number | Best ask (BBO only) |
| *stream-specific* | varies | Additional fields per stream type |

### Context Object (passed to strategy.onEvent)

```typescript
interface RuntimeContext {
    runId: string;
    cursor: {
        ts_event: BigInt;
        seq: BigInt;
        encoded: string;
    };
    placeOrder: (intent: OrderIntent) => FillResult;
    getExecutionState: () => ExecutionStateSnapshot;
}
```

### Order Intent

```typescript
interface OrderIntent {
    symbol: string;
    side: 'BUY' | 'SELL';
    qty: number;
    ts_event?: BigInt;  // Optional, defaults to current event
}
```

---

## Execution Output Contract

### Fill Result

```typescript
interface FillResult {
    id: string;           // Deterministic: "fill_N"
    orderId: string;      // Reference: "ord_N"
    symbol: string;
    side: 'BUY' | 'SELL';
    qty: number;
    fillPrice: number;    // BUY@ask, SELL@bid
    fillValue: number;    // qty * fillPrice
    fee: number;          // fillValue * feeRate (default 0.0004)
    ts_event: BigInt;
}
```

### Execution Semantics (v1)

| Property | Value |
|----------|-------|
| Latency | Zero (same-tick execution) |
| Slippage | Zero (exact BBO price) |
| Partial Fills | Not supported |
| Price Source | BBO stream required |
| Fill ID | Counter-based deterministic |

---

## Cursor Encoding Contract

### Cursor Structure (v1)

```typescript
interface ReplayCursor {
    v: 1;                 // Version
    ts_event: string;     // Stringified BigInt
    seq: string;          // Stringified BigInt
}
```

### Encoding

```javascript
// Encode cursor to base64
const json = JSON.stringify({ v: 1, ts_event: "123456789", seq: "1" });
const encoded = Buffer.from(json, 'utf-8').toString('base64');

// Decode cursor from base64
const json = Buffer.from(encoded, 'base64').toString('utf-8');
const cursor = JSON.parse(json);
cursor.ts_event = BigInt(cursor.ts_event);
cursor.seq = BigInt(cursor.seq);
```

### Resume Logic

Resume starts from the **next** event after the cursor:
```sql
WHERE (ts_event > cursor.ts_event)
   OR (ts_event = cursor.ts_event AND seq > cursor.seq)
ORDER BY ts_event ASC, seq ASC
```

---

## Archive Data Contract

### Run Archive Structure (S3)

```
replay_runs/replay_run_id={id}/
├── manifest.json
├── decisions.jsonl
└── stats.json
```

### manifest.json

```json
{
    "replay_run_id": "replay_abc123",
    "seed": "test-seed",
    "manifest_id": "compact-20260115-btcusdt",
    "parquet_path": "s3://bucket/path/data.parquet",
    "started_at": "2026-01-15T00:00:00.000Z",
    "finished_at": "2026-01-15T23:59:59.999Z",
    "stop_reason": "STREAM_END"
}
```

### decisions.jsonl

```jsonl
{"replay_run_id":"replay_abc123","cursor":"eyJ2IjoxLCJ0c19ldm...","ts_event":"1736899200000000000","decision":{"symbol":"BTCUSDT","side":"BUY","qty":0.1}}
{"replay_run_id":"replay_abc123","cursor":"eyJ2IjoxLCJ0c19ldm...","ts_event":"1736899260000000000","decision":{"symbol":"BTCUSDT","side":"SELL","qty":0.1}}
```

### stats.json

```json
{
    "emitted_event_count": 1000000,
    "decision_count": 42,
    "duration_ms": 86400000
}
```

---

## Canonical Serialization (StateSerializer)

### Purpose

Deterministic JSON serialization for hashing and state comparison.

### Rules

1. Object keys are **sorted alphabetically**
2. BigInt values are converted to strings with `n` suffix: `123n` → `"123n"`
3. Undefined values are **omitted**
4. Null is preserved
5. Arrays maintain order

### Usage

```javascript
import { canonicalStringify, canonicalParse } from './StateSerializer.js';

// Serialize (deterministic output)
const json = canonicalStringify({ b: 1, a: BigInt(123) });
// Output: '{"a":"123n","b":1}'

// Parse (restores BigInt)
const obj = canonicalParse(json);
// obj.a === 123n (BigInt)
```

### Critical Invariant

```javascript
canonicalStringify(canonicalParse(x)) === x  // Always true
```

---

## ORDERING_CONTRACT

### Rules

1. **Global event order**: `ts_event ASC, seq ASC`
2. **Uniqueness**: `(ts_event, seq)` must be unique per dataset
3. **Cursor fields**: Must match ORDERING_COLUMNS exactly

### Constants

```javascript
export const ORDERING_COLUMNS = Object.freeze(['ts_event', 'seq']);
export const SQL_ORDER_CLAUSE = 'ORDER BY ts_event ASC, seq ASC';
export const ORDERING_VERSION = 1;
```

### Violation Consequences

| Violation | Result |
|-----------|--------|
| Duplicate `(ts_event, seq)` | Non-deterministic replay |
| Out-of-order events | Hash verification fails |
| Missing cursor field | Resume corruption |

---

*This document is the authoritative source for QuantLab data contracts.*
