#!/usr/bin/env python3
"""Build the first auditable A-share monitoring pool from Tushare data.

The generated pool is deliberately a market-observation sample, not an index
replication portfolio or a list of trading recommendations.  Every member has
a machine-readable selection bucket and a human-readable Chinese reason.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts

SHANGHAI = ZoneInfo("Asia/Shanghai")

FIXED_TARGET = 320
DYNAMIC_PER_BUCKET = 10
CORE_STOCK_TARGET = 16


@dataclass(frozen=True)
class ReferenceSpec:
    ts_code: str
    role: str
    target: str
    proxy_quality: str
    reason: str


REFERENCES = (
    ReferenceSpec(
        "510300.SH",
        "broad_benchmark",
        "沪深300",
        "direct",
        "宽基基准：跟踪沪深300，用于衡量大盘蓝筹及个股相对大盘强弱。",
    ),
    ReferenceSpec(
        "510500.SH",
        "broad_benchmark",
        "中证500",
        "direct",
        "宽基基准：跟踪中证500，用于衡量中盘股整体强弱。",
    ),
    ReferenceSpec(
        "512100.SH",
        "broad_benchmark",
        "中证1000",
        "direct",
        "宽基基准：跟踪中证1000，用于衡量小盘股整体强弱。",
    ),
    ReferenceSpec(
        "563300.SH",
        "broad_benchmark",
        "中证2000",
        "direct",
        "宽基基准：跟踪中证2000，用于补充微盘和小盘风格观察。",
    ),
    ReferenceSpec(
        "510050.SH",
        "broad_benchmark",
        "上证50",
        "direct",
        "宽基基准：跟踪上证50，用于观察沪市核心权重股。",
    ),
    ReferenceSpec(
        "588000.SH",
        "broad_benchmark",
        "科创50",
        "direct",
        "板块基准：跟踪科创50，用于观察科创板核心公司的整体方向。",
    ),
    ReferenceSpec(
        "159915.SZ",
        "broad_benchmark",
        "创业板指",
        "direct",
        "板块基准：跟踪创业板指，用于观察创业板核心公司的整体方向。",
    ),
    ReferenceSpec(
        "563360.SH",
        "broad_benchmark",
        "中证A500",
        "direct",
        "宽基基准：跟踪中证A500，用于观察行业均衡的大盘核心资产。",
    ),
    ReferenceSpec(
        "159825.SZ",
        "industry_benchmark",
        "农林牧渔",
        "direct",
        "行业代理：覆盖农业主题，用作申万一级「农林牧渔」相对强弱基准。",
    ),
    ReferenceSpec(
        "159870.SZ",
        "industry_benchmark",
        "基础化工",
        "direct",
        "行业代理：覆盖细分化工产业，用作申万一级「基础化工」相对强弱基准。",
    ),
    ReferenceSpec(
        "515210.SH",
        "industry_benchmark",
        "钢铁",
        "direct",
        "行业代理：覆盖钢铁产业，用作申万一级「钢铁」相对强弱基准。",
    ),
    ReferenceSpec(
        "512400.SH",
        "industry_benchmark",
        "有色金属",
        "direct",
        "行业代理：跟踪申万有色金属，用作「有色金属」相对强弱基准。",
    ),
    ReferenceSpec(
        "159997.SZ",
        "industry_benchmark",
        "电子",
        "direct",
        "行业代理：覆盖电子产业，用作申万一级「电子」相对强弱基准。",
    ),
    ReferenceSpec(
        "159328.SZ",
        "industry_benchmark",
        "家用电器",
        "direct",
        "行业代理：覆盖家电龙头，用作申万一级「家用电器」相对强弱基准。",
    ),
    ReferenceSpec(
        "515170.SH",
        "industry_benchmark",
        "食品饮料",
        "direct",
        "行业代理：覆盖细分食品饮料产业，用作「食品饮料」相对强弱基准。",
    ),
    ReferenceSpec(
        "512010.SH",
        "industry_benchmark",
        "医药生物",
        "partial",
        "行业近似代理：覆盖沪深300医药卫生成份，用作「医药生物」方向参考；对中小市值覆盖有限。",
    ),
    ReferenceSpec(
        "159611.SZ",
        "industry_benchmark",
        "公用事业",
        "partial",
        "行业近似代理：覆盖电力公用事业，用作「公用事业」方向参考；不完整覆盖燃气等子行业。",
    ),
    ReferenceSpec(
        "159662.SZ",
        "industry_benchmark",
        "交通运输",
        "direct",
        "行业代理：覆盖交通运输行业，用作申万一级「交通运输」相对强弱基准。",
    ),
    ReferenceSpec(
        "512200.SH",
        "industry_benchmark",
        "房地产",
        "direct",
        "行业代理：覆盖全指房地产，用作申万一级「房地产」相对强弱基准。",
    ),
    ReferenceSpec(
        "159766.SZ",
        "industry_benchmark",
        "社会服务",
        "partial",
        "行业近似代理：旅游主题可反映部分社会服务景气；不代表教育、人力服务等全部子行业。",
    ),
    ReferenceSpec(
        "159745.SZ",
        "industry_benchmark",
        "建筑材料",
        "direct",
        "行业代理：覆盖全指建筑材料，用作申万一级「建筑材料」相对强弱基准。",
    ),
    ReferenceSpec(
        "159635.SZ",
        "industry_benchmark",
        "建筑装饰",
        "partial",
        "行业近似代理：基建主题覆盖建筑装饰主要权重；对装修装饰等子行业覆盖有限。",
    ),
    ReferenceSpec(
        "516160.SH",
        "industry_benchmark",
        "电力设备",
        "partial",
        "行业近似代理：新能源主题覆盖电力设备主要方向；并非完整申万行业复制。",
    ),
    ReferenceSpec(
        "512660.SH",
        "industry_benchmark",
        "国防军工",
        "direct",
        "行业代理：覆盖军工产业，用作申万一级「国防军工」相对强弱基准。",
    ),
    ReferenceSpec(
        "159998.SZ",
        "industry_benchmark",
        "计算机",
        "direct",
        "行业代理：覆盖计算机主题，用作申万一级「计算机」相对强弱基准。",
    ),
    ReferenceSpec(
        "512980.SH",
        "industry_benchmark",
        "传媒",
        "direct",
        "行业代理：覆盖传媒产业，用作申万一级「传媒」相对强弱基准。",
    ),
    ReferenceSpec(
        "515880.SH",
        "industry_benchmark",
        "通信",
        "partial",
        "行业近似代理：覆盖通信设备，用作「通信」方向参考；运营商等覆盖可能不足。",
    ),
    ReferenceSpec(
        "512800.SH",
        "industry_benchmark",
        "银行",
        "direct",
        "行业代理：覆盖中证银行，用作申万一级「银行」相对强弱基准。",
    ),
    ReferenceSpec(
        "512070.SH",
        "industry_benchmark",
        "非银金融",
        "direct",
        "行业代理：覆盖沪深300非银行金融，用作「非银金融」相对强弱基准。",
    ),
    ReferenceSpec(
        "159512.SZ",
        "industry_benchmark",
        "汽车",
        "direct",
        "行业代理：覆盖全指汽车，用作申万一级「汽车」相对强弱基准。",
    ),
    ReferenceSpec(
        "159886.SZ",
        "industry_benchmark",
        "机械设备",
        "direct",
        "行业代理：覆盖细分机械设备产业，用作「机械设备」相对强弱基准。",
    ),
    ReferenceSpec(
        "515220.SH",
        "industry_benchmark",
        "煤炭",
        "direct",
        "行业代理：覆盖煤炭产业，用作申万一级「煤炭」相对强弱基准。",
    ),
    ReferenceSpec(
        "561360.SH",
        "industry_benchmark",
        "石油石化",
        "partial",
        "行业近似代理：油气产业覆盖石油石化主要链条，用作行业方向参考。",
    ),
    ReferenceSpec(
        "512580.SH",
        "industry_benchmark",
        "环保",
        "direct",
        "行业代理：覆盖环保产业，用作申万一级「环保」相对强弱基准。",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-file", type=Path, default=Path("token"))
    parser.add_argument("--as-of", help="completed trading day in YYYYMMDD form")
    parser.add_argument("--output-dir", type=Path, default=Path("config/stock_pools/initial-v1"))
    return parser.parse_args()


def frame(api: Any, name: str, **kwargs: Any) -> pd.DataFrame:
    result = getattr(api, name)(**kwargs)
    if result is None or result.empty:
        raise RuntimeError(f"Tushare {name} returned no data for {kwargs}")
    return result


def completed_trade_dates(api: Any, requested: str | None) -> list[str]:
    today = datetime.now(SHANGHAI).date()
    end = requested or (today.fromordinal(today.toordinal() - 1)).strftime("%Y%m%d")
    start = (
        datetime.strptime(end, "%Y%m%d")
        .date()
        .fromordinal(datetime.strptime(end, "%Y%m%d").date().toordinal() - 60)
    ).strftime("%Y%m%d")
    calendar = frame(
        api,
        "trade_cal",
        exchange="SSE",
        start_date=start,
        end_date=end,
        fields="cal_date,is_open",
    )
    dates = sorted(calendar.loc[calendar["is_open"] == 1, "cal_date"].astype(str).tolist())
    if len(dates) < 20:
        raise RuntimeError(f"only {len(dates)} open dates available before {end}")
    if requested and requested not in dates:
        raise RuntimeError(f"--as-of {requested} is not an open trading day")
    return dates[-20:]


def load_universe(api: Any, trade_dates: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    as_of = trade_dates[-1]
    stocks = frame(
        api,
        "stock_basic",
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,market,list_date",
    )
    latest_daily = frame(
        api,
        "daily",
        trade_date=as_of,
        fields="ts_code,trade_date,close,pct_chg,vol,amount",
    )
    latest_basic = frame(
        api,
        "daily_basic",
        trade_date=as_of,
        fields=("ts_code,trade_date,close,turnover_rate,volume_ratio,total_mv,circ_mv"),
    )

    amount_frames = []
    for trade_date in trade_dates:
        daily = frame(
            api,
            "daily",
            trade_date=trade_date,
            fields="ts_code,trade_date,amount",
        )
        amount_frames.append(daily)
    amounts = pd.concat(amount_frames, ignore_index=True)
    liquidity = amounts.groupby("ts_code", as_index=False).agg(
        avg_amount_20d_tushare=("amount", "mean"), observed_days=("amount", "count")
    )

    classes = frame(api, "index_classify", level="L1", src="SW2021")
    member_frames = []
    for index_code in sorted(classes["index_code"].astype(str)):
        members = frame(api, "index_member_all", l1_code=index_code)
        member_frames.append(
            members.loc[members["is_new"] == "Y", ["ts_code", "l1_code", "l1_name"]]
        )
    members = pd.concat(member_frames, ignore_index=True)
    members = members.drop_duplicates(subset=["ts_code"], keep="first")

    universe = stocks.merge(latest_daily, on="ts_code", how="inner")
    universe = universe.merge(
        latest_basic.drop(columns=["trade_date", "close"]), on="ts_code", how="inner"
    )
    universe = universe.merge(liquidity, on="ts_code", how="inner")
    universe = universe.merge(members, on="ts_code", how="inner")
    universe["exchange"] = universe["ts_code"].str.rsplit(".", n=1).str[-1]
    universe["circ_mv_cny"] = universe["circ_mv"] * 10_000
    universe["total_mv_cny"] = universe["total_mv"] * 10_000
    universe["latest_amount_cny"] = universe["amount"] * 1_000
    universe["avg_amount_20d_cny"] = universe["avg_amount_20d_tushare"] * 1_000
    as_of_date = datetime.strptime(as_of, "%Y%m%d").date()
    universe["listing_age_days"] = universe["list_date"].map(
        lambda value: (as_of_date - datetime.strptime(str(value), "%Y%m%d").date()).days
    )
    universe = universe[
        (universe["market"] == "主板")
        & (universe["exchange"].isin(["SH", "SZ"]))
        & (universe["listing_age_days"] >= 60)
        & (universe["observed_days"] >= 15)
        & (~universe["name"].str.contains(r"ST|退", regex=True, na=False))
        & (universe["avg_amount_20d_cny"] > 0)
        & (universe["circ_mv_cny"] > 0)
    ].copy()

    universe["industry_size"] = universe.groupby("l1_name")["ts_code"].transform("size")
    universe["industry_cap_rank"] = universe.groupby("l1_name")["circ_mv_cny"].rank(
        method="first", ascending=False
    )
    universe["industry_liquidity_rank"] = universe.groupby("l1_name")["avg_amount_20d_cny"].rank(
        method="first", ascending=False
    )
    return universe.reset_index(drop=True), classes


def rank_int(value: Any) -> int:
    return int(round(float(value)))


def base_industry_selection(universe: pd.DataFrame) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for industry, group in universe.groupby("l1_name", sort=True):
        group = group.copy()
        count = len(group)
        first_cut = math.ceil(count / 3)
        second_cut = math.ceil(2 * count / 3)
        tiers = (
            ("large_cap", "大市值层", group[group["industry_cap_rank"] <= first_cut], 2),
            (
                "mid_cap",
                "中市值层",
                group[
                    (group["industry_cap_rank"] > first_cut)
                    & (group["industry_cap_rank"] <= second_cut)
                ],
                3,
            ),
            ("small_cap", "小市值层", group[group["industry_cap_rank"] > second_cut], 2),
        )
        industry_codes: set[str] = set()
        for bucket, label, candidates, target in tiers:
            candidates = candidates.sort_values(
                ["avg_amount_20d_cny", "circ_mv_cny", "ts_code"],
                ascending=[False, False, True],
            )
            for _, row in candidates.head(target).iterrows():
                industry_codes.add(str(row["ts_code"]))
                selected.append(
                    member_record(
                        row,
                        role="fixed_representative",
                        bucket=bucket,
                        reason=(
                            f"固定代表：申万一级「{industry}」{label}；行业内流通市值第"
                            f"{rank_int(row['industry_cap_rank'])}/{count}、近20个交易日平均成交额第"
                            f"{rank_int(row['industry_liquidity_rank'])}/{count}，用于兼顾行业规模层次和可交易活跃度。"
                        ),
                    )
                )
        remaining = group[~group["ts_code"].isin(industry_codes)].sort_values(
            ["avg_amount_20d_cny", "circ_mv_cny", "ts_code"], ascending=[False, False, True]
        )
        if remaining.empty:
            raise RuntimeError(
                f"industry {industry} has fewer than eight eligible main-board stocks"
            )
        row = remaining.iloc[0]
        selected.append(
            member_record(
                row,
                role="fixed_representative",
                bucket="liquidity_anchor",
                reason=(
                    f"固定代表：申万一级「{industry}」流动性锚；近20个交易日平均成交额行业第"
                    f"{rank_int(row['industry_liquidity_rank'])}/{count}，补充该行业最活跃且未重复的交易标的。"
                ),
            )
        )
    return selected


def add_coverage_members(
    universe: pd.DataFrame, selected: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    selected_codes = {item["ts_code"] for item in selected}
    while len(selected) < FIXED_TARGET:
        counts = pd.Series([item["industry_l1"] for item in selected]).value_counts().to_dict()
        candidates = universe[~universe["ts_code"].isin(selected_codes)].copy()
        candidates["selected_industry_count"] = candidates["l1_name"].map(counts).fillna(0)
        row = candidates.sort_values(
            ["selected_industry_count", "avg_amount_20d_cny", "circ_mv_cny", "ts_code"],
            ascending=[True, False, False, True],
        ).iloc[0]
        industry = str(row["l1_name"])
        count = rank_int(row["industry_size"])
        selected.append(
            member_record(
                row,
                role="fixed_representative",
                bucket="coverage_fill",
                reason=(
                    f"覆盖加密：补足固定代表池至{FIXED_TARGET}只，并优先增加样本较少的申万一级"
                    f"「{industry}」；近20个交易日平均成交额行业第"
                    f"{rank_int(row['industry_liquidity_rank'])}/{count}。"
                ),
            )
        )
        selected_codes.add(str(row["ts_code"]))
    return selected


def add_dynamic_members(
    universe: pd.DataFrame, fixed: list[dict[str, Any]], as_of: str
) -> list[dict[str, Any]]:
    excluded = {item["ts_code"] for item in fixed}
    dynamic: list[dict[str, Any]] = []
    specs = (
        ("daily_gainer", "pct_chg", False, "涨幅", "%", "捕捉当日强势方向"),
        ("daily_loser", "pct_chg", True, "跌幅", "%", "捕捉当日风险和弱势方向"),
        ("volume_ratio", "volume_ratio", False, "量比", "", "捕捉成交速度异常放大"),
        ("turnover_rate", "turnover_rate", False, "换手率", "%", "捕捉筹码交换活跃标的"),
    )
    for bucket, field, ascending, label, suffix, purpose in specs:
        candidates = universe[~universe["ts_code"].isin(excluded)].copy()
        candidates = candidates.dropna(subset=[field]).sort_values(
            [field, "avg_amount_20d_cny", "ts_code"],
            ascending=[ascending, False, True],
        )
        for rank, (_, row) in enumerate(candidates.head(DYNAMIC_PER_BUCKET).iterrows(), start=1):
            value = float(row[field])
            if bucket == "daily_loser":
                value_text = f"{value:.2f}%"
            elif suffix:
                value_text = f"{value:.2f}{suffix}"
            else:
                value_text = f"{value:.2f}"
            dynamic.append(
                member_record(
                    row,
                    role="dynamic_observer",
                    bucket=bucket,
                    reason=(
                        f"动态异动（{format_date(as_of)}）：固定池外{label}筛选第{rank}，"
                        f"指标值{value_text}；{purpose}。动态席位应在下一交易日重算。"
                    ),
                )
            )
            excluded.add(str(row["ts_code"]))
    return dynamic


def member_record(row: pd.Series, role: str, bucket: str, reason: str) -> dict[str, Any]:
    return {
        "ts_code": str(row["ts_code"]),
        "name": str(row["name"]),
        "instrument_type": "stock",
        "role": role,
        "sampling_tier": "satellite_60s",
        "interval_seconds": 60,
        "industry_l1": str(row["l1_name"]),
        "market": str(row["market"]),
        "exchange": str(row["exchange"]),
        "selection_bucket": bucket,
        "selection_reason": reason,
        "industry_benchmark_symbol": None,
        "proxy_quality": None,
        "circ_mv_cny": round(float(row["circ_mv_cny"]), 2),
        "avg_amount_20d_cny": round(float(row["avg_amount_20d_cny"]), 2),
        "latest_amount_cny": round(float(row["latest_amount_cny"]), 2),
        "turnover_rate": round(float(row["turnover_rate"]), 4),
        "volume_ratio": round(float(row["volume_ratio"]), 4),
        "pct_chg": round(float(row["pct_chg"]), 4),
        "list_date": str(row["list_date"]),
    }


def reference_records(api: Any, as_of: str) -> list[dict[str, Any]]:
    codes = {spec.ts_code for spec in REFERENCES}
    basics = frame(
        api,
        "fund_basic",
        market="E",
        status="L",
        fields="ts_code,name,list_date,management,fund_type",
    )
    daily = frame(
        api,
        "fund_daily",
        trade_date=as_of,
        fields="ts_code,trade_date,close,pct_chg,vol,amount",
    )
    basics = basics[basics["ts_code"].isin(codes)].set_index("ts_code")
    daily = daily[daily["ts_code"].isin(codes)].set_index("ts_code")
    missing = codes - set(basics.index) | codes - set(daily.index)
    if missing:
        raise RuntimeError(f"reference ETF data missing: {sorted(missing)}")
    records = []
    for spec in REFERENCES:
        basic = basics.loc[spec.ts_code]
        quote = daily.loc[spec.ts_code]
        records.append(
            {
                "ts_code": spec.ts_code,
                "name": str(basic["name"]),
                "instrument_type": "etf",
                "role": spec.role,
                "sampling_tier": "core_15s",
                "interval_seconds": 15,
                "industry_l1": spec.target if spec.role == "industry_benchmark" else None,
                "market": "场内基金",
                "exchange": spec.ts_code.rsplit(".", 1)[-1],
                "selection_bucket": spec.target,
                "selection_reason": spec.reason,
                "industry_benchmark_symbol": None,
                "proxy_quality": spec.proxy_quality,
                "circ_mv_cny": None,
                "avg_amount_20d_cny": None,
                "latest_amount_cny": round(float(quote["amount"]) * 1_000, 2),
                "turnover_rate": None,
                "volume_ratio": None,
                "pct_chg": round(float(quote["pct_chg"]), 4),
                "list_date": str(basic["list_date"]),
            }
        )
    return records


def assign_benchmarks_and_core(members: list[dict[str, Any]]) -> None:
    industry_map = {
        item.target: item.ts_code for item in REFERENCES if item.role == "industry_benchmark"
    }
    for member in members:
        if member["instrument_type"] == "stock":
            member["industry_benchmark_symbol"] = industry_map.get(member["industry_l1"])

    stock_members = [item for item in members if item["instrument_type"] == "stock"]
    liquid_stocks = sorted(
        (item for item in stock_members if item["role"] == "fixed_representative"),
        key=lambda item: (-float(item["avg_amount_20d_cny"]), item["ts_code"]),
    )[:CORE_STOCK_TARGET]
    for member in liquid_stocks:
        member["sampling_tier"] = "core_15s"
        member["interval_seconds"] = 15
        member["selection_reason"] += " 同时因固定池内近20日平均成交额居前，进入15秒核心采样层。"


def format_date(value: str) -> str:
    return datetime.strptime(value, "%Y%m%d").date().isoformat()


def serializable(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: serializable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serializable(item) for item in value]
    if isinstance(value, (pd.Timestamp, date)):
        return value.isoformat()
    return value


def write_outputs(
    output_dir: Path,
    members: list[dict[str, Any]],
    universe: pd.DataFrame,
    classes: pd.DataFrame,
    trade_dates: list[str],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    as_of = trade_dates[-1]
    members = sorted(
        members,
        key=lambda item: (
            0 if item["instrument_type"] == "stock" else 1,
            item["industry_l1"] or "",
            item["role"],
            item["ts_code"],
        ),
    )
    metadata = build_metadata(members, universe, classes, trade_dates)
    document = {"metadata": metadata, "members": members}
    (output_dir / "pool.json").write_text(
        json.dumps(serializable(document), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    pd.DataFrame(members).to_csv(output_dir / "pool.csv", index=False, encoding="utf-8-sig")

    core = sorted(item["ts_code"] for item in members if item["sampling_tier"] == "core_15s")
    satellite = sorted(
        item["ts_code"] for item in members if item["sampling_tier"] == "satellite_60s"
    )
    all_symbols = sorted(item["ts_code"] for item in members)
    stock_symbols = sorted(
        item["ts_code"] for item in members if item["instrument_type"] == "stock"
    )
    write_symbol_file(output_dir / "core_15s.txt", core)
    write_symbol_file(output_dir / "satellite_60s.txt", satellite)
    write_symbol_file(output_dir / "all_symbols.txt", all_symbols)
    write_symbol_file(output_dir / "stocks.txt", stock_symbols)

    industry_proxies = {
        spec.target: {
            "ts_code": spec.ts_code,
            "proxy_quality": spec.proxy_quality,
            "reason": spec.reason,
        }
        for spec in REFERENCES
        if spec.role == "industry_benchmark"
    }
    uncovered = sorted(set(classes["industry_name"].astype(str)) - set(industry_proxies))
    (output_dir / "industry_proxies.json").write_text(
        json.dumps(
            {
                "as_of_trade_date": format_date(as_of),
                "source_industry_classification": "申万行业分类2021版（Tushare SW2021）",
                "proxies": industry_proxies,
                "uncovered_industries": uncovered,
                "fallback_policy": "没有可信行业ETF代理时不伪造映射；仅计算相对宽基强弱。",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    stock_to_benchmark = {
        item["ts_code"]: item["industry_benchmark_symbol"]
        for item in members
        if item["instrument_type"] == "stock" and item["industry_benchmark_symbol"]
    }
    (output_dir / "industry_benchmarks.json").write_text(
        json.dumps(stock_to_benchmark, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (output_dir / "selection_reasons.md").write_text(
        render_reasons(members, metadata), encoding="utf-8"
    )
    (output_dir / "README.md").write_text(render_readme(metadata), encoding="utf-8")


def write_symbol_file(path: Path, symbols: list[str]) -> None:
    path.write_text("\n".join(symbols) + "\n", encoding="utf-8")


def build_metadata(
    members: list[dict[str, Any]],
    universe: pd.DataFrame,
    classes: pd.DataFrame,
    trade_dates: list[str],
) -> dict[str, Any]:
    stocks = [item for item in members if item["instrument_type"] == "stock"]
    fixed = [item for item in stocks if item["role"] == "fixed_representative"]
    dynamic = [item for item in stocks if item["role"] == "dynamic_observer"]
    references = [item for item in members if item["instrument_type"] == "etf"]
    selected_codes = {item["ts_code"] for item in stocks}
    universe_cap = float(universe["circ_mv_cny"].sum())
    universe_amount = float(universe["avg_amount_20d_cny"].sum())
    selected_universe = universe[universe["ts_code"].isin(selected_codes)]
    counts_by_market = pd.Series([item["market"] for item in stocks]).value_counts().to_dict()
    counts_by_industry = (
        pd.Series([item["industry_l1"] for item in stocks]).value_counts().to_dict()
    )
    return {
        "pool_id": "initial-v1",
        "purpose": "A股全市场粗粒度盘中观察，不构成投资建议或可交易指数复制组合",
        "generated_at": datetime.now(SHANGHAI).isoformat(),
        "as_of_trade_date": format_date(trade_dates[-1]),
        "liquidity_window": {
            "trading_days": len(trade_dates),
            "from": format_date(trade_dates[0]),
            "to": format_date(trade_dates[-1]),
        },
        "sources": [
            "Tushare stock_basic",
            "Tushare daily",
            "Tushare daily_basic",
            "Tushare index_classify SW2021",
            "Tushare index_member_all SW2021",
            "Tushare fund_basic",
            "Tushare fund_daily",
        ],
        "selection_rules": {
            "fixed_representatives": "沪深主板内，31个申万一级行业各8只分层基础样本，再按行业均衡填充至320只",
            "dynamic_observers": "沪深主板固定池外按单日涨幅、跌幅、量比、换手率各10只且互不重复；每个交易日盘后重算",
            "eligibility": "仅沪深主板；上市至少60自然日、近20个交易日至少15日有行情、非ST/退市标记、成交额和流通市值有效",
            "references": "8只宽基/板块ETF和26只行业ETF代理；5个无可信ETF代理的行业明确留空",
            "sampling": "全部ETF和固定池中近20日平均成交额最高的16只股票进入15秒核心层，其余60秒采样",
        },
        "counts": {
            "total_instruments": len(members),
            "stocks": len(stocks),
            "fixed_stocks": len(fixed),
            "dynamic_stocks": len(dynamic),
            "reference_etfs": len(references),
            "core_15s": sum(item["sampling_tier"] == "core_15s" for item in members),
            "satellite_60s": sum(item["sampling_tier"] == "satellite_60s" for item in members),
            "industries": len(counts_by_industry),
        },
        "counts_by_market": counts_by_market,
        "counts_by_industry": counts_by_industry,
        "eligible_universe_size": len(universe),
        "coverage": {
            "eligible_free_float_market_cap_pct": round(
                float(selected_universe["circ_mv_cny"].sum()) / universe_cap * 100, 2
            ),
            "eligible_average_20d_amount_pct": round(
                float(selected_universe["avg_amount_20d_cny"].sum()) / universe_amount * 100, 2
            ),
        },
        "industry_classification_count": len(classes),
    }


def render_reasons(members: list[dict[str, Any]], metadata: dict[str, Any]) -> str:
    lines = [
        "# 初始股票池逐只入选理由",
        "",
        f"数据截止：{metadata['as_of_trade_date']}；共 {metadata['counts']['total_instruments']} 只证券，"
        f"其中股票 {metadata['counts']['stocks']} 只、参考 ETF {metadata['counts']['reference_etfs']} 只。",
        "",
        "> 这是市场监测样本，不是买入名单。动态观察位应在每个交易日盘后重算。",
        "",
    ]
    stocks = [item for item in members if item["instrument_type"] == "stock"]
    for industry in sorted({str(item["industry_l1"]) for item in stocks}):
        lines.extend(
            [
                f"## {industry}",
                "",
                "| 代码 | 名称 | 角色 | 采样 | 入选理由 |",
                "|---|---|---|---:|---|",
            ]
        )
        for item in (row for row in stocks if row["industry_l1"] == industry):
            role = "固定代表" if item["role"] == "fixed_representative" else "动态观察"
            lines.append(
                f"| {item['ts_code']} | {item['name']} | {role} | {item['interval_seconds']}秒 | "
                f"{item['selection_reason']} |"
            )
        lines.append("")
    lines.extend(
        [
            "## 参考 ETF",
            "",
            "| 代码 | 名称 | 类型/对应范围 | 代理质量 | 入选理由 |",
            "|---|---|---|---|---|",
        ]
    )
    for item in (row for row in members if row["instrument_type"] == "etf"):
        role = "宽基/板块" if item["role"] == "broad_benchmark" else f"行业：{item['industry_l1']}"
        lines.append(
            f"| {item['ts_code']} | {item['name']} | {role} | {item['proxy_quality']} | "
            f"{item['selection_reason']} |"
        )
    return "\n".join(lines) + "\n"


def render_readme(metadata: dict[str, Any]) -> str:
    counts = metadata["counts"]
    coverage = metadata["coverage"]
    return f"""# 初始股票池 v1

这是用于粗粒度观察 A 股全市场的第一版采样池，不是交易推荐或指数复制组合。

- 数据截止：{metadata["as_of_trade_date"]}
- 总数：{counts["total_instruments"]}（股票 {counts["stocks"]}；ETF {counts["reference_etfs"]}）
- 股票构成：固定代表 {counts["fixed_stocks"]}；动态观察 {counts["dynamic_stocks"]}
- 调度：核心层 {counts["core_15s"]} 只每 15 秒；卫星层 {counts["satellite_60s"]} 只每 60 秒
- 覆盖：合资格股票自由流通市值 {coverage["eligible_free_float_market_cap_pct"]}%；近20日平均成交额 {coverage["eligible_average_20d_amount_pct"]}%

文件说明：

- `pool.json`：完整、机器可读的池定义和生成元数据。
- `pool.csv`：便于筛选、排序和人工评审；每只证券都有 `selection_reason`。
- `selection_reasons.md`：按申万一级行业列出每只股票及其入选理由。
- `core_15s.txt`：15 秒核心采样层。
- `satellite_60s.txt`：60 秒卫星采样层；与核心层互斥。
- `all_symbols.txt`：全部证券代码。
- `stocks.txt`：仅股票代码，不含 ETF。
- `industry_proxies.json`：行业到 ETF 的代理关系、质量和明确缺口。
- `industry_benchmarks.json`：供特征计算直接读取的“股票代码 -> 行业基准 ETF”映射。

生成命令：

```bash
.venv/bin/python scripts/generate_initial_stock_pool.py --as-of {metadata["as_of_trade_date"].replace("-", "")}
```

股票范围严格限定为沪深主板。固定池以 31 个申万一级行业各 8 只分层样本起步，再按行业均衡填充到 320 只。动态 40 只按照上一完整交易日的涨幅、跌幅、量比和换手率各选 10 只，互不重复。入选资格排除创业板、科创板、北交所、上市不足 60 天、近 20 个交易日行情不足 15 天，以及名称带 ST/退市标记的股票。

行业 ETF 代理不是强行全覆盖：纺织服饰、轻工制造、商贸零售、综合、美容护理暂未配置可信代理，相关股票只计算相对宽基强弱；标记为 `partial` 的 ETF 也仅作方向参考。
"""


def validate(members: list[dict[str, Any]], classes: pd.DataFrame) -> None:
    codes = [item["ts_code"] for item in members]
    if len(codes) != len(set(codes)):
        duplicates = sorted(code for code in set(codes) if codes.count(code) > 1)
        raise RuntimeError(f"duplicate pool members: {duplicates}")
    if any(not item["selection_reason"].strip() for item in members):
        raise RuntimeError("every pool member must have a selection reason")
    if len(members) >= 500:
        raise RuntimeError(f"pool must stay below 500 members, got {len(members)}")
    stock_members = [item for item in members if item["instrument_type"] == "stock"]
    fixed = [item for item in stock_members if item["role"] == "fixed_representative"]
    dynamic = [item for item in stock_members if item["role"] == "dynamic_observer"]
    if len(fixed) != FIXED_TARGET or len(dynamic) != 4 * DYNAMIC_PER_BUCKET:
        raise RuntimeError(f"unexpected fixed/dynamic counts: {len(fixed)}/{len(dynamic)}")
    actual_industries = {item["industry_l1"] for item in fixed}
    expected_industries = set(classes["industry_name"].astype(str))
    if actual_industries != expected_industries:
        raise RuntimeError(f"industry mismatch: {sorted(expected_industries - actual_industries)}")
    invalid_stocks = [
        item["ts_code"]
        for item in stock_members
        if item["market"] != "主板"
        or item["exchange"] not in {"SH", "SZ"}
        or "ST" in item["name"].upper()
        or "退" in item["name"]
    ]
    if invalid_stocks:
        raise RuntimeError(f"non-main-board or risk stocks entered the pool: {invalid_stocks}")
    core_count = sum(item["sampling_tier"] == "core_15s" for item in members)
    if core_count != len(REFERENCES) + CORE_STOCK_TARGET:
        raise RuntimeError(f"unexpected core count: {core_count}")


def main() -> None:
    args = parse_args()
    token = args.token_file.read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError(f"empty Tushare token file: {args.token_file}")
    api = ts.pro_api(token)
    trade_dates = completed_trade_dates(api, args.as_of)
    universe, classes = load_universe(api, trade_dates)
    fixed = add_coverage_members(universe, base_industry_selection(universe))
    dynamic = add_dynamic_members(universe, fixed, trade_dates[-1])
    references = reference_records(api, trade_dates[-1])
    members = fixed + dynamic + references
    assign_benchmarks_and_core(members)
    validate(members, classes)
    write_outputs(args.output_dir, members, universe, classes, trade_dates)
    print(
        json.dumps(
            {
                "output_dir": str(args.output_dir),
                "as_of_trade_date": format_date(trade_dates[-1]),
                "members": len(members),
                "stocks": len(fixed) + len(dynamic),
                "reference_etfs": len(references),
                "core_15s": sum(item["sampling_tier"] == "core_15s" for item in members),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
