"""Append-only write-ahead journal with per-line checksums.

Each accepted alert is journaled before it is handed to the triage queue
and before the device is acked. On restart the gateway replays the
journal to rebuild its dedupe set and re-offer every entry to triage.

Lines are JSON with a crc32 suffix so a torn tail (crash mid-write) or a
corrupted line is detected and skipped instead of poisoning recovery.
"""

import json
import os
import zlib


class Journal:
    def __init__(self, path):
        self.path = path
        self._fh = None
        self.appends = 0
        self._open()

    def _open(self):
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")

    def append(self, record):
        body = json.dumps(record, separators=(",", ":"), sort_keys=True)
        crc = zlib.crc32(body.encode("utf-8")) & 0xFFFFFFFF
        self._fh.write("%s|%08x\n" % (body, crc))
        self._fh.flush()
        self.appends += 1

    def close(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def reopen(self):
        if self._fh is None:
            self._open()

    def replay(self):
        """Yield every intact record in append order. Skips torn/corrupt lines."""
        skipped = 0
        records = []
        if not os.path.exists(self.path):
            return records, skipped
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line:
                    continue
                if "|" not in line:
                    skipped += 1
                    continue
                body, _, crc_hex = line.rpartition("|")
                try:
                    crc = int(crc_hex, 16)
                except ValueError:
                    skipped += 1
                    continue
                if (zlib.crc32(body.encode("utf-8")) & 0xFFFFFFFF) != crc:
                    skipped += 1
                    continue
                try:
                    records.append(json.loads(body))
                except json.JSONDecodeError:
                    skipped += 1
        return records, skipped
