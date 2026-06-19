# API + HTTP Data Ingestion Crash Course

Purpose: a practical study guide for understanding and improving API-based
data extraction code like `airflow/include/scripts/soda_ingest.py`.

This note focuses on the pieces we have discussed so far:

- HTTP APIs as data sources.
- Python `requests.Session`.
- Retry behavior, timeouts, status codes, and rate limits.
- Config-driven extractors.
- API client classes, static methods, properties, and dependency injection.
- Interview-ready mental models for ingestion reliability.

It is written as a learning artifact, not as a prescription that the current
repo must preserve its existing design.

---

## 1. The Mental Model

An API ingestion task is a remote read wrapped in engineering discipline.

At the simplest level:

```text
source API
  -> HTTP requests
  -> JSON responses
  -> pagination
  -> raw files or raw tables
  -> metadata about what happened
```

The job is not merely to "call an endpoint." A good ingestion task answers:

- What exact endpoint am I reading?
- What parameters define this extract?
- How do I authenticate?
- How do I page through the source?
- What happens if the API returns a temporary error?
- What happens if the task fails halfway through?
- Can I retry safely?
- Can I backfill a past time window?
- How do I know how many records I extracted?
- What raw artifact did I produce?

For interviews, frame API extraction as a reliability problem, not just a
Python scripting problem.

---

## 2. HTTP Basics For Data Engineers

HTTP is a client-server protocol. Your extractor is the client. The API server
owns the data. Your code sends requests and receives responses.

A typical request has:

```text
method  GET
url     https://data.sfgov.org/resource/i98e-djp9.json
query   ?$limit=1000&$offset=0&$order=data_loaded_at
headers X-App-Token: ...
body    usually empty for GET
```

A typical response has:

```text
status code  200
headers      content-type, retry-after, rate-limit headers, etc.
body         JSON payload
```

Important concepts:

- `GET` retrieves data. It should not mutate server state.
- `POST` usually submits data and may create side effects.
- Query parameters are part of the request boundary.
- Headers carry metadata such as auth tokens, content negotiation, and retry
  hints.
- Status codes tell you whether the response is successful, client-broken, or
  server-broken.

Useful status code categories:

```text
2xx  success
3xx  redirect
4xx  client-side problem: bad request, unauthorized, not found, rate limited
5xx  server-side or upstream problem
```

For ingestion retry logic, these matter most:

```text
429  Too Many Requests: rate limited
500  Internal Server Error: generic server failure
502  Bad Gateway: upstream failure
503  Service Unavailable: temporary overload/maintenance
504  Gateway Timeout: upstream timeout
```

These are often worth retrying for read-only API calls.

---

## 3. Why Use A Session?

In Python, you can call:

```python
import requests

response = requests.get("https://api.example.com/orders")
```

That works, but production-ish extractors usually use a session:

```python
import requests

session = requests.Session()
response = session.get("https://api.example.com/orders", timeout=60)
```

A `requests.Session` lets you reuse settings across requests:

- headers
- auth
- cookies
- retry adapters
- connection pooling

For paginated extraction, you may call the same API hundreds or thousands of
times. A session keeps the HTTP client configuration in one place.

In `soda_ingest.py`, the session factory is:

```python
def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    token = os.environ.get("DATASF_APP_TOKEN")
    if token:
        s.headers["X-App-Token"] = token
    return s
```

What this is doing:

- Create one reusable HTTP session.
- Configure retries for transient failures.
- Retry only `GET`, which is safe for this extraction use case.
- Add an API token if present.
- Keep secrets in environment variables, not source code.

Best-practice version with a settings object:

```python
from dataclasses import dataclass
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class ApiSettings:
    token: str | None
    timeout_seconds: int = 60
    retries: int = 5
    backoff_factor: float = 1.5

    @classmethod
    def from_env(cls) -> "ApiSettings":
        return cls(token=os.environ.get("DATASF_APP_TOKEN"))


def build_session(settings: ApiSettings) -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=settings.retries,
        backoff_factor=settings.backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    if settings.token:
        session.headers["X-App-Token"] = settings.token

    return session
```

Why this version can be nicer:

- Configuration loading is separate from session construction.
- Tests can create `ApiSettings(token="fake")`.
- Timeouts and retry choices are explicit.
- `respect_retry_after_header=True` makes rate-limit behavior more polite when
  the server provides a `Retry-After` header.

---

## 4. Timeouts Are Not Optional

Every API request in a pipeline should have a timeout.

Bad:

```python
requests.get(url)
```

Better:

```python
requests.get(url, timeout=60)
```

Even better when you want separate connect/read control:

```python
requests.get(url, timeout=(5, 60))
```

Meaning:

```text
connect timeout: 5 seconds to establish the connection
read timeout:    60 seconds waiting for response data
```

Why it matters:

- Without timeouts, a task can hang forever.
- Hanging tasks clog orchestration slots.
- Retries only help if a failure actually surfaces.

Interview line:

> I always set explicit HTTP timeouts because an ingestion job should fail
> predictably rather than hang indefinitely.

---

## 5. Retry Logic

Retries are for temporary failures, not broken requests.

Usually retry:

```text
429, 500, 502, 503, 504
connection resets
temporary DNS/network failures
```

Usually do not retry blindly:

```text
400 Bad Request
401 Unauthorized
403 Forbidden
404 Not Found
422 Unprocessable Entity
```

Those often mean your request, auth, or endpoint is wrong.

Basic retry setup:

```python
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests


def session_with_retries() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session
```

Backoff means waiting longer between attempts:

```text
try now
wait a little
try again
wait longer
try again
...
```

Jitter means adding randomness to the wait so many clients do not retry at the
same moment. That matters more in large distributed systems, but it is a useful
interview concept.

Important nuance:

- Retrying `GET` is usually safe because `GET` should only retrieve data.
- Retrying `POST` can create duplicates unless the API supports idempotency
  keys.

---

## 6. Config-Driven Extraction

If many endpoints share extraction behavior but differ in metadata, use a
configuration object.

Current project pattern:

```python
@dataclass
class DatasetConfig:
    name: str
    dataset_id: str
    date_field: str
    order_field: str
    epoch: date
    page_size: int = field(default=1000)

    @property
    def endpoint(self) -> str:
        return f"https://data.sfgov.org/resource/{self.dataset_id}.json"
```

This separates:

```text
what dataset to extract
```

from:

```text
how extraction works
```

Example:

```python
PERMITS = DatasetConfig(
    name="permits",
    dataset_id="i98e-djp9",
    date_field="data_loaded_at",
    order_field="permit_number",
    epoch=date(2013, 1, 1),
)
```

More defensive version:

```python
from dataclasses import dataclass
from datetime import date
import re


SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class EndpointConfig:
    name: str
    dataset_id: str
    cursor_field: str
    tie_breaker_field: str
    epoch: date
    page_size: int = 1000
    base_url: str = "https://data.sfgov.org/resource"

    def __post_init__(self) -> None:
        for field_name in (self.cursor_field, self.tie_breaker_field):
            if not SAFE_IDENTIFIER.match(field_name):
                raise ValueError(f"Unsafe API field name: {field_name}")

        if self.page_size <= 0:
            raise ValueError("page_size must be positive")

    @property
    def endpoint(self) -> str:
        return f"{self.base_url}/{self.dataset_id}.json"
```

What this teaches:

- `@dataclass(frozen=True)` makes the config immutable after creation.
- `__post_init__` validates config after dataclass initialization.
- `endpoint` is derived from `base_url` and `dataset_id`.
- Store independent facts; compute mechanical derivations.

---

## 7. Properties

A property is a method accessed like an attribute.

```python
@property
def endpoint(self) -> str:
    return f"{self.base_url}/{self.dataset_id}.json"
```

Usage:

```python
config.endpoint
```

not:

```python
config.endpoint()
```

Use a property when:

- The value feels like data.
- The value is derived from other fields.
- You do not want duplicate sources of truth.

In this case:

```text
dataset_id is stored
endpoint is computed
```

That keeps the endpoint from being defined in two places.

---

## 8. API Client Classes

An API client class should usually own:

- session
- timeout
- request construction
- response validation
- pagination logic

It should not own:

- warehouse loading
- dbt transforms
- dashboard logic
- orchestration-specific state

Minimal example:

```python
from collections.abc import Iterator
from typing import Any
import requests


class ApiClient:
    def __init__(self, session: requests.Session, timeout: int = 60) -> None:
        self._session = session
        self._timeout = timeout

    def fetch_page(self, url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        response = self._session.get(url, params=params, timeout=self._timeout)
        response.raise_for_status()
        payload = response.json()

        if not isinstance(payload, list):
            raise TypeError("Expected API response to be a JSON array")

        return payload

    def iter_records(
        self,
        url: str,
        base_params: dict[str, Any],
        page_size: int,
    ) -> Iterator[dict[str, Any]]:
        offset = 0

        while True:
            params = base_params | {"$limit": page_size, "$offset": offset}
            batch = self.fetch_page(url, params)

            if not batch:
                return

            yield from batch

            if len(batch) < page_size:
                return

            offset += page_size
```

Why use a class here?

- The session is stateful and reusable.
- The timeout is shared across requests.
- Tests can inject a fake session.
- The client gives a clear home to API-specific behavior.

---

## 9. Static Methods

In `soda_ingest.py`, methods like this are static:

```python
@staticmethod
def order_clause(config: DatasetConfig) -> str:
    if config.order_field == config.date_field:
        return f"`{config.date_field}`"
    return f"`{config.date_field}`, `{config.order_field}`"
```

A static method:

- lives on the class
- does not receive `self`
- does not access instance state
- is mostly a namespaced helper

This could also be a normal function:

```python
def order_clause(config: DatasetConfig) -> str:
    ...
```

When to use a static method:

- The helper is tightly related to the class concept.
- You want to call it as `SodaClient.order_clause(config)`.
- It does not need object state.

When to prefer a standalone function:

- The logic is generic.
- The class is becoming a junk drawer.
- The helper would be useful outside the client.

Interview line:

> Static methods are useful for namespacing pure helper logic near a class, but
> if the helper is broadly reusable I prefer a module-level function.

---

## 10. Query Parameters

Prefer passing query parameters via `params=`.

Good:

```python
session.get(
    "https://data.sfgov.org/resource/i98e-djp9.json",
    params={
        "$limit": 1000,
        "$offset": 0,
        "$order": "`data_loaded_at`, `permit_number`",
    },
    timeout=60,
)
```

Avoid hand-concatenating URLs:

```python
url = f"{base_url}?$limit={limit}&$offset={offset}&$order={order}"
```

Why:

- `requests` handles encoding.
- Params are easier to inspect in tests.
- It separates endpoint identity from query options.

---

## 11. Pagination

APIs rarely return all records in one response. Pagination is the process of
fetching records page by page.

Common styles:

```text
limit/offset pagination
cursor pagination
page-token pagination
keyset pagination
date-window pagination
```

### Limit/Offset

Current project style:

```python
params = {
    "$limit": config.page_size,
    "$offset": offset,
}
```

Mental model:

```text
page 1: offset 0,    limit 1000
page 2: offset 1000, limit 1000
page 3: offset 2000, limit 1000
```

Pros:

- Easy to understand.
- Common in REST-like APIs.
- Good for learning.

Cons:

- Can get slow at large offsets.
- Can miss or duplicate rows if the underlying dataset changes while paging.
- Needs stable ordering.

### Stable Ordering

When paging, always request deterministic order if the API supports it.

Good:

```text
order by updated_at, id
```

Risky:

```text
no order clause
```

Without stable ordering, page boundaries can shift.

In the current project:

```python
def order_clause(config: DatasetConfig) -> str:
    if config.order_field == config.date_field:
        return f"`{config.date_field}`"
    return f"`{config.date_field}`, `{config.order_field}`"
```

This is the right instinct: order by the cursor timestamp plus a tie-breaker.

### Cursor / Keyset Pagination

For large or frequently changing datasets, keyset pagination is often more
robust:

```sql
WHERE (updated_at, id) > (:last_updated_at, :last_id)
ORDER BY updated_at, id
LIMIT 1000
```

In API parameter form:

```python
params = {
    "updated_after": last_updated_at.isoformat(),
    "id_after": last_id,
    "limit": 1000,
}
```

Pros:

- Usually faster than large offsets.
- More stable for changing datasets.
- Naturally supports composite cursors.

Cons:

- API must support it.
- State handling is more complex.

---

## 12. Incremental Extraction

Incremental extraction means fetching only new or changed records.

Common patterns:

### Full Refresh

```text
Fetch everything every time.
```

Pros:

- Simple.
- No cursor state.

Cons:

- Expensive.
- Slow.
- Bad for large datasets.

### High-Watermark Extraction

```sql
WHERE updated_at > :last_successful_watermark
```

Pros:

- Efficient.
- Common.
- Easy to reason about at first.

Cons:

- Timestamp ties can be dangerous.
- Late-arriving records can be missed.
- Partial failures can corrupt state if the watermark advances too early.

More robust high-watermark:

```text
cursor = (updated_at, id)
```

Query:

```sql
WHERE updated_at > :last_updated_at
   OR (updated_at = :last_updated_at AND id > :last_id)
ORDER BY updated_at, id
```

### Airflow Interval Extraction

```sql
WHERE updated_at >= :data_interval_start
  AND updated_at <  :data_interval_end
```

Pros:

- Functional partition boundary.
- Natural backfills.
- Same interval should produce same extract.

Cons:

- Requires the source to answer historical windows reliably.
- Needs a plan for late-arriving updates.
- Usually still needs dedupe downstream.

Interview line:

> I prefer explicit extraction windows when the source supports them, because
> retries and backfills are easier to reason about. If I use a high-watermark,
> I store committed cursor state separately and update it only after the load
> succeeds.

---

## 13. Raw Output Format

For API ingestion, raw output should preserve source records with minimal
transformation.

Common choices:

```text
NDJSON
JSON files
CSV
Parquet
raw warehouse table with VARIANT/JSON column
```

NDJSON means newline-delimited JSON:

```json
{"id":1,"status":"open"}
{"id":2,"status":"closed"}
```

Why NDJSON is good for extraction:

- One record per line.
- Easy to stream.
- Easy to append/write incrementally.
- Friendly to many warehouse loaders.
- Avoids holding one giant JSON array in memory.

Good raw file naming:

```text
raw/{source_name}/{logical_date}/records.ndjson
raw/permits/2024/03/20/permits.json
```

For strict reproducibility, include extract metadata:

```text
raw/{source}/{interval_start}_{interval_end}/{run_id}.ndjson
```

Tradeoff:

- `run_id` in the path preserves every attempt.
- A canonical partition path makes overwrite/retry semantics easier.

---

## 14. Observability Metadata

An extractor should produce enough metadata to debug a run without re-running
it.

Capture:

```text
source name
endpoint
extract lower bound
extract upper bound
page size
records fetched
records written
bytes written
started_at
finished_at
duration_seconds
status
error message
destination path
airflow dag_id/task_id/run_id when applicable
```

Avoid logging:

```text
API tokens
passwords
PII payloads
full signed URLs
```

Interview line:

> I want extraction metadata to answer three questions: what input boundary did
> we ask for, what raw artifact did we write, and when did we commit state?

---

## 15. Dependency Injection

Dependency injection means passing dependencies in rather than creating them
deep inside the code.

Good:

```python
class ApiClient:
    def __init__(self, session: requests.Session, timeout: int = 60) -> None:
        self._session = session
        self._timeout = timeout
```

Less testable:

```python
class ApiClient:
    def __init__(self) -> None:
        self._session = requests.Session()
```

Why injection matters:

- Tests can pass fake sessions.
- Production can pass configured sessions.
- You can swap implementations without changing business logic.

Example test seam:

```python
from unittest.mock import MagicMock


def fake_response(payload):
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_client_uses_expected_params():
    session = MagicMock()
    session.get.return_value = fake_response([])

    client = ApiClient(session=session, timeout=30)
    list(client.iter_records(
        url="https://api.example.com/orders",
        base_params={"status": "open"},
        page_size=100,
    ))

    params = session.get.call_args.kwargs["params"]
    assert params["status"] == "open"
    assert params["$limit"] == 100
    assert params["$offset"] == 0
```

No network call. Fast test. Clear assertion.

---

## 16. Functional Design In Extraction

Functional data engineering favors explicit inputs and outputs.

Good function shape:

```python
def build_params(config: EndpointConfig, cursor: Cursor) -> dict[str, object]:
    return {
        "$where": f"`{config.cursor_field}` > '{cursor.timestamp}'",
        "$order": f"`{config.cursor_field}`, `{config.tie_breaker_field}`",
        "$limit": config.page_size,
    }
```

This function is easy to test because it has:

- explicit inputs
- no network
- no filesystem
- no environment variables
- deterministic output

Side effects should live at the edge:

```text
HTTP client: network side effects
writer: filesystem/S3 side effects
state store: cursor commit side effects
```

Pipeline core should be mostly pure:

```text
config + cursor -> request params
record -> parsed cursor
records -> stats
run metadata -> result object
```

This is the useful blend:

- OOP for stateful resources like clients and writers.
- Functional helpers for deterministic transformations.
- Dataclasses for typed domain values.

---

## 17. A Reference Extractor Skeleton

This example is intentionally compact but interview-friendly.

```python
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


@dataclass(frozen=True)
class EndpointConfig:
    name: str
    endpoint: str
    cursor_field: str
    tie_breaker_field: str
    page_size: int = 1000


@dataclass(frozen=True)
class Cursor:
    timestamp: datetime
    tie_breaker: str


@dataclass(frozen=True)
class ExtractWindow:
    lower: Cursor
    upper: Cursor | None = None


def to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Expected timezone-aware datetime")
    return value.astimezone(timezone.utc)


def build_where(config: EndpointConfig, lower: Cursor) -> str:
    ts = to_utc(lower.timestamp).isoformat(timespec="milliseconds")
    return (
        f"`{config.cursor_field}` > '{ts}' OR "
        f"(`{config.cursor_field}` = '{ts}' "
        f"AND `{config.tie_breaker_field}` > '{lower.tie_breaker}')"
    )


def build_order(config: EndpointConfig) -> str:
    return f"`{config.cursor_field}`, `{config.tie_breaker_field}`"


class ApiExtractor:
    def __init__(self, session: requests.Session, timeout: int = 60) -> None:
        self._session = session
        self._timeout = timeout

    def iter_records(
        self,
        config: EndpointConfig,
        window: ExtractWindow,
    ) -> Iterator[dict[str, Any]]:
        offset = 0

        while True:
            params = {
                "$where": build_where(config, window.lower),
                "$order": build_order(config),
                "$limit": config.page_size,
                "$offset": offset,
            }

            response = self._session.get(
                config.endpoint,
                params=params,
                timeout=self._timeout,
            )
            response.raise_for_status()
            batch = response.json()

            if not batch:
                return

            yield from batch

            if len(batch) < config.page_size:
                return

            offset += config.page_size
```

Discussion points:

- `EndpointConfig`, `Cursor`, and `ExtractWindow` are immutable domain objects.
- `build_where` and `build_order` are pure functions.
- `ApiExtractor` owns the network session.
- The generator streams records page by page.
- The cursor is composite, not only timestamp-based.

---

## 18. Interview Checklist

When asked "How would you ingest data from an API?", cover:

```text
1. Understand the API contract
   - auth
   - endpoint shape
   - pagination
   - filtering
   - ordering
   - rate limits
   - schema stability

2. Choose extraction boundary
   - full refresh
   - high-watermark cursor
   - interval/window extraction
   - CDC/webhooks if available

3. Make requests reliable
   - sessions
   - timeouts
   - retries
   - backoff/jitter
   - rate-limit handling

4. Make extraction deterministic
   - stable ordering
   - composite cursors
   - explicit intervals
   - immutable raw outputs or safe overwrites

5. Preserve raw data
   - NDJSON/Parquet/raw table
   - metadata columns
   - no destructive transformations in extract step

6. Commit state safely
   - only after successful write/load
   - separate state table from raw data
   - track run attempts

7. Test without real network
   - fake sessions
   - fake responses
   - assert request params
   - test pagination edges
   - test empty results and error responses

8. Observe production behavior
   - records fetched
   - bytes written
   - duration
   - retries
   - failures
   - destination path
```

---

## 19. Common Pitfalls

### No timeout

The task can hang forever.

### No stable order while paginating

Rows can be duplicated or skipped across pages.

### Retrying unsafe operations

Retrying `POST` without idempotency keys can create duplicates.

### Watermark advanced before data is durable

Can cause silent data loss after partial failure.

### Timestamp-only cursor

Rows sharing the same timestamp can be missed.

### Treating 429 like a normal error

Rate limits should usually trigger backoff and respect `Retry-After`.

### Logging secrets

Never log auth headers or full signed URLs.

### Transforming too much during extraction

Raw ingestion should preserve source data. Clean and model later.

---

## 20. How This Maps To `soda_ingest.py`

Current strengths:

- Uses `DatasetConfig` to avoid copy-pasted extractors.
- Uses `requests.Session`.
- Configures retry behavior for transient HTTP failures.
- Uses explicit timeouts.
- Uses `params=` rather than hand-building query URLs.
- Uses stable ordering for pagination.
- Streams records with generators.
- Writes NDJSON rather than one giant in-memory object.
- Allows fake clients/writers in tests.

Future learning/refactor themes:

- Make config/result types more consistently immutable.
- Keep UTC datetimes timezone-aware internally.
- Consider compound cursors.
- Consider interval-based extraction using Airflow data intervals.
- Separate environment loading from client/session construction.
- Add local writer or DuckDB writer behind the same extraction interface.
- Track richer extraction run metadata.

---

## 21. Outside Reading

Core HTTP:

- [MDN: Overview of HTTP](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Overview)
- [MDN: HTTP request methods](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Methods)
- [MDN: HTTP response status codes](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status)

Python HTTP clients:

- [Requests: Advanced Usage](https://requests.readthedocs.io/en/latest/user/advanced/)
- [urllib3 Retry documentation](https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.retry.Retry)

Socrata / DataSF API mechanics:

- [Socrata: API Endpoints](https://dev.socrata.com/docs/endpoints.html)
- [Socrata: LIMIT clause](https://dev.socrata.com/docs/queries/limit.html)
- [Socrata: OFFSET clause](https://dev.socrata.com/docs/queries/offset.html)
- [Socrata: ORDER BY clause](https://dev.socrata.com/docs/queries/order.html)

Retries and idempotency:

- [AWS Architecture Blog: Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
- [Stripe: Idempotent Requests](https://docs.stripe.com/api/idempotent_requests)

Orchestration and intervals:

- [Airflow: DAG Runs and Data Intervals](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html)

Python language features:

- [Python docs: dataclasses](https://docs.python.org/3/library/dataclasses.html)
- [Python docs: property](https://docs.python.org/3/library/functions.html#property)
- [Python docs: staticmethod](https://docs.python.org/3/library/functions.html#staticmethod)

Books:

- *Designing Data-Intensive Applications* by Martin Kleppmann.
- *Fundamentals of Data Engineering* by Joe Reis and Matt Housley.
- *HTTP: The Definitive Guide* by David Gourley and Brian Totty.
- *Architecture Patterns with Python* by Harry Percival and Bob Gregory.

---

## 22. Practice Prompts

Use these to test whether you really understand the material.

1. Why is `requests.Session` better than repeated `requests.get` calls for a
   paginated extractor?
2. Which HTTP status codes would you retry for a read-only API extraction task?
3. Why is a timeout necessary even when retries are configured?
4. What can go wrong with offset pagination?
5. Why does stable ordering matter when paging?
6. What is the difference between high-watermark extraction and interval-based
   extraction?
7. Why can timestamp-only watermarks miss records?
8. What is the purpose of a tie-breaker field?
9. Why should raw extraction preserve source records with minimal transformation?
10. How would you test pagination without calling the real API?
11. When would you use a property instead of storing a value directly?
12. When is a static method appropriate?
13. What should happen to cursor state if the raw write succeeds but warehouse
    loading fails?
14. How would you adapt the extractor to write locally for DuckDB instead of S3?

