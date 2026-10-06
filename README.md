# Log Follower

A Go Server-Sent Events service that tails NDJSON segments written by a
producer using a strict sealing model:

```
segment-0001.ndjson
segment-0002.ndjson
...
```

* A segment is appended to **only while it is the highest-numbered file**.
* Creating the next segment **seals** the previous one forever.
* At most **6 segments** exist, each at most **8 KiB**.
* The producer rotates by creating the next segment and later deleting the
  oldest. Copying/truncating/replacing a file in place is **not** a supported
  rotation style and is explicitly reported.

The service only reads the mounted directory (mounted read-only in Compose).

## Run

```bash
# Fix the run identifier for the lifetime of a producer/service incarnation.
LOG_RUN_ID="$(uuidgen -r)" docker compose up --build
```

| Env var           | Default              | Meaning                                        |
|-------------------|----------------------|------------------------------------------------|
| `LOG_DIR`         | `/var/log/segments`  | Directory containing `segment-NNNN.ndjson`     |
| `LOG_RUN_ID`      | random, logged once  | Service run code carried in every event        |
| `POLL_INTERVAL_MS`| `20`                 | Directory/file polling period                  |
| `EVENT_BUFFER`    | `64`                 | Per-connection queued events before disconnect |
| `LISTEN_ADDR`     | `:8080`             | Listen address                                 |

## API

### `GET /logs` (SSE)

Without a cursor the stream starts at `segment-0001.ndjson` offset `0`; if
segment 1 is missing the request is refused instead of jumping forward.

Resume with the standard SSE header (or `?last-event-id=...`):

```
Last-Event-ID: <id from the last fully processed event>
```

Every frame's `id:` is an opaque base64url cursor:
`{v, run_id, segment, offset}` where **`offset` is a byte position immediately
after a `\n`** (the first complete record after the cursor is delivered).
Character/rune offsets are rejected; offsets are never used to split a line
between the tail of one segment and the head of the next.

Record frames:

```
event: record
id: <cursor through this record's newline>
data: {"type":"record","run_id":"...","segment":2,"end_offset":132,"result":{...parsed JSON...}}
```

A complete newline-delimited line that is not valid UTF-8/JSON is still
delivered and never consumes following lines:

```
event: invalid_record
data: {"type":"invalid_record",...,"raw_b64":"<exact offending bytes>",
       "error":{"code":"invalid_json","message":"..."}}
```

Fatal stream problems are delivered as a terminal `event: error` frame and the
connection is closed; resumability errors on connect return JSON instead of
upgrading to SSE.

Other endpoints: `GET /healthz`, `GET /run-id`, and a test-only
`POST /test/read-barrier` that returns after every live follower finishes one
directory poll (used by tests to synchronize appends/rotations with reads).

### Evidence-gap rules (never silently skipped)

| Condition                                                | Code                                       | Where      |
|----------------------------------------------------------|--------------------------------------------|------------|
| Cursor `run_id` differs from the service                 | `run_id_changed`                           | 409        |
| Cursor segment deleted / active segment disappears       | `cursor_segment_deleted`                   | 409 / SSE  |
| A sequence number is missing between cursor and newest   | `segment_gap`                              | 409 / SSE  |
| Offset is not right after a real newline byte            | `offset_not_newline_boundary`              | 400        |
| Offset past EOF                                          | `offset_beyond_eof`                        | 409        |
| Sealed segment has no final newline (truncated record)   | `sealed_segment_without_final_newline`     | 409 / SSE  |
| Same filename but different inode/device (file replaced) | `segment_replaced`                         | SSE        |
| Active segment shrank (copy/truncate rotation)           | `active_segment_truncated`                 | SSE        |
| Sealed segment was modified/shrank                       | `sealed_segment_modified`                  | SSE        |
| More than 6 files, or a file larger than 8 KiB           | `too_many_segments` / `segment_too_large`  | 409        |
| Subscriber exceeded `EVENT_BUFFER`                       | `slow_consumer`                            | SSE        |

A half line in the newest segment is simply buffered until completion; when
the segment is sealed with the half line unfinished, the subscription reports
truncation and stops without emitting bytes from the new segment.

## Client behavior notes

* Save the `id:` only after the whole record frame was processed. Reconnect by
  sending it as `Last-Event-ID`.
* Connections are independent: each has its own goroutine and bounded queue; a
  slow consumer is terminated (`slow_consumer`) and never blocks others.
* Files are identified by `(st_dev, st_ino)`, so replacing a segment under the
  same name is detected while following, not just at open.

## Develop and test

```bash
go test -race ./...
docker compose build
```

The test suite uses real files (`os.OpenFile` append, `rename`, truncate,
unlink), the read barrier to order writes versus polls, and covers: split
UTF-8 mid-record, disconnect/reconnect with `Last-Event-ID`, ring rotation and
historical-segment deletion, exact byte-offset verification against file
contents, explicit evidence-gap rejections, inode replacement, slow-consumer
isolation, and SSE framing through `httptest`.
