"""Atomic gzip archive for exact raw provider responses."""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from dawnwatcher.domain.quotes import (
    QuoteProvider,
    QuoteSymbol,
    RawArchiveRecord,
    RawQuoteBatch,
)


class RawQuoteArchive:
    """Write and replay immutable raw quote batches."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def write(self, batch: RawQuoteBatch) -> RawArchiveRecord:
        """Atomically archive response bytes and request metadata."""
        digest = hashlib.sha256(batch.body).hexdigest()
        day_directory = self.root / batch.fetched_at.date().isoformat() / batch.provider.value
        day_directory.mkdir(parents=True, exist_ok=True)
        timestamp = batch.fetched_at.strftime("%H%M%S_%f")
        destination = day_directory / f"{timestamp}_{uuid4().hex[:8]}.json.gz"
        temporary = destination.with_name(f".{destination.name}.tmp")
        envelope = {
            "schema_version": 2,
            "provider": batch.provider.value,
            "requested_symbols": [symbol.ts_code for symbol in batch.requested_symbols],
            "requested_at": batch.requested_at.isoformat(),
            "fetched_at": batch.fetched_at.isoformat(),
            "elapsed_ms": batch.elapsed_ms,
            "status_code": batch.status_code,
            "encoding": batch.encoding,
            "request_url": batch.request_url,
            "body_sha256": digest,
            "body_base64": base64.b64encode(batch.body).decode("ascii"),
        }
        try:
            with gzip.open(temporary, "wt", encoding="utf-8") as handle:
                json.dump(envelope, handle, ensure_ascii=False, separators=(",", ":"))
            os.link(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return RawArchiveRecord(
            provider=batch.provider,
            path=str(destination.resolve()),
            sha256=digest,
            size_bytes=len(batch.body),
        )

    def read(self, path: Path) -> RawQuoteBatch:
        """Load and verify an archived batch for deterministic replay."""
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            envelope = json.load(handle)
        body = base64.b64decode(envelope["body_base64"], validate=True)
        digest = hashlib.sha256(body).hexdigest()
        if digest != envelope["body_sha256"]:
            raise ValueError("raw quote archive checksum mismatch")
        provider = QuoteProvider(envelope["provider"])
        symbols = tuple(QuoteSymbol.parse(value) for value in envelope["requested_symbols"])
        return RawQuoteBatch(
            provider=provider,
            requested_symbols=symbols,
            requested_at=datetime.fromisoformat(envelope["requested_at"]),
            fetched_at=datetime.fromisoformat(envelope["fetched_at"]),
            elapsed_ms=float(envelope["elapsed_ms"]),
            status_code=int(envelope["status_code"]),
            encoding=str(envelope["encoding"]),
            request_url=str(envelope["request_url"]),
            body=body,
        )
