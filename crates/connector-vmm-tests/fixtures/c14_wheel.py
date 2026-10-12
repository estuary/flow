"""Writes C14's synthetic wheel, `c14pkg`, the same bytes on every run, into
the directory named by its argument, and prints what the test needs of it.

`render` checks the digest of the 3 MiB payload the wheel ships, so only a
whole install can answer, and the payload is incompressible so that a cut
lands mid-body however the server frames it."""

import base64
import hashlib
import json
import sys
import zipfile

NAME, VERSION = "c14pkg", "1.0.0"
PAYLOAD_BYTES = 3 << 20
DIST_INFO = f"{NAME}-{VERSION}.dist-info"

payload = bytearray()
counter = 0
while len(payload) < PAYLOAD_BYTES:
    payload += hashlib.sha256(b"c14:%d" % counter).digest()
    counter += 1
payload = bytes(payload[:PAYLOAD_BYTES])
payload_sha256 = hashlib.sha256(payload).hexdigest()

module = f'''import hashlib
from importlib import resources

VERSION = "{VERSION}"
DIGEST = "{payload_sha256}"


def render(n: int) -> str:
    data = resources.files(__name__).joinpath("payload.bin").read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != DIGEST:
        raise RuntimeError(f"c14pkg payload digest {{digest}} is not {{DIGEST}}")
    return f"c14pkg {{VERSION}} n={{n}} square={{n * n}} payload={{digest[:16]}}"
'''

files = [
    (f"{NAME}/__init__.py", module.encode()),
    (f"{NAME}/payload.bin", payload),
    (f"{DIST_INFO}/METADATA", f"Metadata-Version: 2.1\nName: {NAME}\nVersion: {VERSION}\n".encode()),
    (
        f"{DIST_INFO}/WHEEL",
        b"Wheel-Version: 1.0\nGenerator: c14_wheel.py\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    ),
]


def record_line(path: str, content: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=").decode()
    return f"{path},sha256={digest},{len(content)}\n"


record = "".join(record_line(path, content) for path, content in files)
files.append((f"{DIST_INFO}/RECORD", (record + f"{DIST_INFO}/RECORD,,\n").encode()))

wheel = f"{sys.argv[1]}/{NAME}-{VERSION}-py3-none-any.whl"
with zipfile.ZipFile(wheel, "w") as archive:
    for path, content in files:
        # A fixed time and mode, so the archive's bytes never vary.
        info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
        info.external_attr = 0o644 << 16
        archive.writestr(info, content, compress_type=zipfile.ZIP_STORED)

with open(wheel, "rb") as f:
    content = f.read()
print(
    json.dumps(
        {
            "path": wheel,
            "bytes": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
            "payload_sha256": payload_sha256,
            "version": VERSION,
        }
    )
)
