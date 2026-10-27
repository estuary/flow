"""A toy HTTP API of keyed records, backed by SQLite, which the example
materialization writes to as a reverse-ETL connector writes to a SaaS API.

    PUT  /tables/{table}       Create a table, if it doesn't exist.
    POST /tables/{table}/get   Body {"keys": [key, ...]}: the stored {"key", "record"}
                               of each key which exists, as a JSON array.
    POST /tables/{table}/put   Body {"rows": [{"key", "record"}, ...]}: upsert each row,
                               or delete it if its record is null.

Each request commits on its own. `python api.py dump` prints every table's
records, ordered by table and key, without their document metadata.
"""

import http.server
import json
import pathlib
import re
import sqlite3
import sys
from typing import Any

DATABASE = pathlib.Path("/tmp/api.sqlite")
TABLE = re.compile(r"^/tables/([a-z_]+)(/get|/put)?$")


def connect() -> sqlite3.Connection:
    return sqlite3.connect(DATABASE, isolation_level=None, timeout=30)


class Handler(http.server.BaseHTTPRequestHandler):
    def do_PUT(self) -> None:
        match = TABLE.match(self.path)
        if not match or match.group(2):
            return self.send_error(404)
        with connect() as db:
            db.execute(f"CREATE TABLE IF NOT EXISTS {match.group(1)} (key TEXT PRIMARY KEY, record TEXT NOT NULL)")
        self.reply(None)

    def do_POST(self) -> None:
        match = TABLE.match(self.path)
        if not match or not match.group(2):
            return self.send_error(404)
        table, action = match.group(1), match.group(2)
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))

        with connect() as db:
            if action == "/get":
                keys = [json.dumps(key) for key in body["keys"]]
                rows = db.execute(
                    f"SELECT key, record FROM {table} WHERE key IN ({','.join('?' * len(keys))})", keys
                ).fetchall()
                self.reply([{"key": json.loads(key), "record": json.loads(record)} for key, record in rows])
            else:
                for row in body["rows"]:
                    key = json.dumps(row["key"])
                    if row["record"] is None:
                        db.execute(f"DELETE FROM {table} WHERE key = ?", [key])
                    else:
                        db.execute(
                            f"INSERT INTO {table} (key, record) VALUES (?, ?) "
                            "ON CONFLICT (key) DO UPDATE SET record = excluded.record",
                            [key, json.dumps(row["record"], sort_keys=True)],
                        )
                self.reply(None)

    def reply(self, body: Any) -> None:
        content = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


def dump() -> None:
    with connect() as db:
        tables = [name for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        for table in tables:
            for key, record in db.execute(f"SELECT key, record FROM {table} ORDER BY key"):
                record = json.loads(record)
                # Document metadata, such as its UUID, differs with each run.
                if isinstance(record, dict):
                    record.pop("_meta", None)
                print(json.dumps([table, json.loads(key), record], sort_keys=True))


if __name__ == "__main__":
    if sys.argv[1:] == ["dump"]:
        dump()
    else:
        http.server.ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
