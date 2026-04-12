# Raw TCP SSE PoC

This project is a low-level proof of concept that demonstrates how Server-Sent Events (SSE) work on top of plain TCP sockets and minimal HTTP response formatting.

Instead of using a web framework, the server in `raw_tcp_server.py` manually:

- reads the first HTTP request line,
- serves static files (`index.html`, `app.js`),
- opens an SSE stream for `/sse`,
- pushes event frames over an open TCP connection.

## What this PoC demonstrates

1. SSE is just an HTTP response with `Content-Type: text/event-stream`.
2. The connection remains open while the server keeps writing SSE-formatted messages.
3. Each SSE message is text-framed, for example `data: ...\n\n`.
4. Custom events can be sent with `event: name` + `data: ...`.

## Request and response flow

1. Browser loads `index.html` from `GET /`.
2. Browser loads `app.js` from `GET /app.js`.
3. `app.js` creates `new EventSource("/sse")`.
4. Browser sends `GET /sse`.
5. Server responds with:
	- `HTTP/1.1 200 OK`
	- `Content-Type: text/event-stream`
	- `Cache-Control: no-cache`
	- `Connection: keep-alive`
6. Server writes 5 events (`data: Event 0` ... `data: Event 4`) every 2 seconds.
7. Server writes a custom termination event:

```text
event: end
data: Stream ended

```

8. Frontend listens for `end` and calls `eventSource.close()`.

## File roles

- `raw_tcp_server.py`: Async TCP server that manually speaks basic HTTP + SSE.
- `index.html`: Minimal UI with a log panel.
- `app.js`: Creates `EventSource`, handles normal messages (`onmessage`), and handles the custom `end` event.

## Run locally

From the project root:

```bash
python raw_tcp_server.py
```

Then open:

`http://127.0.0.1:8000`

You should see events appear in the log every 2 seconds, followed by an end message.

## Important limitations (intentional for learning)

This is a learning PoC, not a production HTTP server. It intentionally skips many concerns:

- only inspects the first request line (ignores most headers),
- minimal HTTP parsing and routing,
- no MIME/type negotiation beyond hardcoded paths,
- no robust connection lifecycle handling,
- no concurrency controls beyond asyncio defaults,
- no retry/id SSE fields,
- no production-grade error handling or observability.

## Why this is useful

By manually constructing the HTTP and SSE frames, this PoC makes it easier to internalize that:

- SSE is unidirectional server-to-client streaming,
- framing (`data: ...\n\n`) matters,
- connection persistence is key,
- browser `EventSource` behavior maps directly to these raw protocol details.
