"""Tests for the isolated Sina/Tencent diagnostic comparison path."""

from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path

import httpx

from regimebeacon.config import Settings
from regimebeacon.diagnostics.comparison import DiagnosticComparisonRunner
from regimebeacon.providers.collector import parse_symbols
from tests.quote_samples import tencent_line


def test_comparison_writes_each_inconsistency_to_separate_log(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path / "data", _env_file=None)
    symbols = parse_symbols(["600000.SH"])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "hq.sinajs.cn":
            fields = [""] * 32
            fields[0] = "浦发银行"
            fields[1:6] = ["8.99", "8.98", "9.10", "9.10", "8.97"]
            fields[8] = "100"
            fields[9] = "1000"
            fields[10] = "10"
            fields[11] = "9.09"
            fields[20] = "20"
            fields[21] = "9.11"
            fields[30] = "2026-09-24"
            fields[31] = "10:00:00"
            body = f'var hq_str_sh600000="{",".join(fields)}";'.encode("gb18030")
            return httpx.Response(200, content=body)
        return httpx.Response(200, content=tencent_line().encode("gb18030"))

    async def exercise() -> dict[str, object]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            async with DiagnosticComparisonRunner(
                settings,
                symbols,
                expected_trade_date=date(2026, 9, 24),
                report_directory=tmp_path / "report",
                client=client,
                archive_raw=False,
            ) as runner:
                return await runner.collect_once(1)

    cycle = asyncio.run(exercise())
    assert cycle["quality_counts"] == {"conflicted": 1}
    lines = (tmp_path / "report" / "inconsistencies.jsonl").read_text().splitlines()
    records = [json.loads(line) for line in lines]
    assert records
    assert all(record["event"] == "market.source_comparison.inconsistency" for record in records)
    assert any(record["field"] == "latest" and record["state"] == "conflict" for record in records)
