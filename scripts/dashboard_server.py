#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMMENTS_FILE = ROOT / "data" / "comments.json"
MAX_BODY_BYTES = 512 * 1024


def read_comments() -> dict[str, str]:
    if not COMMENTS_FILE.exists():
        return {}
    try:
        raw = COMMENTS_FILE.read_text(encoding="utf-8")
        data = json.loads(raw) if raw.strip() else {}
        if not isinstance(data, dict):
            return {}
        out: dict[str, str] = {}
        for k, v in data.items():
            if isinstance(k, str) and isinstance(v, str):
                out[k] = v
        return out
    except Exception:
        return {}


def write_comments(data: dict[str, str]) -> None:
    COMMENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = COMMENTS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, COMMENTS_FILE)


class DashboardHandler(SimpleHTTPRequestHandler):
    def _send_json(self, status: int, payload: dict | list) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/api/comments":
            self._send_json(HTTPStatus.OK, read_comments())
            return
        if self.path == "/":
            self.path = "/dashboard.html"
        super().do_GET()

    def do_POST(self) -> None:
        if self.path != "/api/comments":
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
            return

        length_raw = self.headers.get("Content-Length", "0")
        try:
            length = int(length_raw)
        except ValueError:
            self.send_error(HTTPStatus.BAD_REQUEST, "Invalid Content-Length")
            return

        if length <= 0 or length > MAX_BODY_BYTES:
            self.send_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Body too large")
            return

        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self.send_error(HTTPStatus.BAD_REQUEST, "Invalid JSON")
            return

        if not isinstance(payload, dict):
            self.send_error(HTTPStatus.BAD_REQUEST, "Payload must be an object")
            return

        clean: dict[str, str] = {}
        for key, value in payload.items():
            if isinstance(key, str) and isinstance(value, str):
                clean[key] = value[:2000]

        write_comments(clean)
        self._send_json(HTTPStatus.OK, {"ok": True, "saved": len(clean)})

    def log_message(self, fmt: str, *args: object) -> None:
        return


def main() -> None:
    COMMENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not COMMENTS_FILE.exists():
        write_comments({})

    os.chdir(ROOT)
    host = "127.0.0.1"
    port = 8000
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard server: http://{host}:{port}/dashboard.html")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
