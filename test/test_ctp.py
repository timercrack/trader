# coding=utf-8

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ctp_native.gateway import PybindGateway
from runtime_config import runtime_config


def _event_logger(topic, data):
    watch_topics = {
        "OnFrontConnected",
        "OnFrontDisconnected",
        "OnFrontConnectedMd",
        "OnFrontDisconnectedMd",
        "OnRspAuthenticate",
        "OnRspUserLogin",
        "OnRspUserLoginMd",
        "OnRspSettlementInfoConfirm",
        "OnRspError",
    }
    if topic in watch_topics:
        print(f"[event] {topic}: {data}")


async def main():
    warmup_s = float(os.getenv("CTP_TEST_WARMUP_SECONDS") or "5")
    tick_timeout_s = float(os.getenv("CTP_TEST_TICK_TIMEOUT_SECONDS") or "10")
    instrument_id = (
        os.getenv("CTP_TEST_INSTRUMENT")
        or runtime_config.get("CTP_NATIVE", "test_instrument", fallback="")
        or "IF99"
    )

    request_id = 1000

    def next_request_id() -> int:
        nonlocal request_id
        request_id += 1
        return request_id

    async def resolve_query_target():
        preferred_rows = await gw.request(
            "ReqQryInstrument",
            {
                "RequestID": next_request_id(),
                "InstrumentID": instrument_id,
            },
        )
        preferred_rows = [row for row in preferred_rows if not row.get("empty", False)]
        if preferred_rows:
            row = preferred_rows[0]
            return row.get("InstrumentID", instrument_id), row.get("ExchangeID", "")

        pos_rows = await gw.request(
            "ReqQryInvestorPositionDetail",
            {
                "RequestID": next_request_id(),
            },
        )
        for row in pos_rows:
            if row.get("empty", False):
                continue
            inst = str(row.get("InstrumentID", ""))
            if not inst or len(inst) > 6:
                continue
            inst_rows = await gw.request(
                "ReqQryInstrument",
                {
                    "RequestID": next_request_id(),
                    "InstrumentID": inst,
                },
            )
            inst_rows = [x for x in inst_rows if not x.get("empty", False)]
            if inst_rows:
                return inst_rows[0].get("InstrumentID", inst), inst_rows[0].get("ExchangeID", "")
            return inst, ""

        return instrument_id, ""

    async def resolve_position_instrument(fallback_inst: str):
        pos_rows = await gw.request(
            "ReqQryInvestorPositionDetail",
            {
                "RequestID": next_request_id(),
            },
        )
        for row in pos_rows:
            if row.get("empty", False):
                continue
            inst = str(row.get("InstrumentID", "")).strip()
            if inst:
                return inst
        return fallback_inst

    gw = PybindGateway()
    loop = asyncio.get_running_loop()
    first_tick_future = loop.create_future()
    subscription_target = ""

    def event_logger(topic, data):
        _event_logger(topic, data)
        if topic != "OnRtnDepthMarketData":
            return
        if not subscription_target:
            return
        if str(data.get("InstrumentID", "")) != subscription_target:
            return
        if first_tick_future.done():
            return
        loop.call_soon_threadsafe(first_tick_future.set_result, data)

    gw.set_event_callback(event_logger)
    await gw.start()
    try:
        if warmup_s > 0:
            print(f"[warmup] waiting {warmup_s:.1f}s for CTP login/settlement readiness...")
            await asyncio.sleep(warmup_s)

        query_inst, query_exchange = await resolve_query_target()
        print(f"[query-target] instrument={query_inst} exchange={query_exchange or '(auto)'}")

        subscription_target = await resolve_position_instrument(query_inst)
        print(f"[md-subscribe] instrument={subscription_target}")
        sub_rows = await gw.request("SubscribeMarketData", [subscription_target])
        print(f"[SubscribeMarketData] rows={len(sub_rows)} first={sub_rows[0] if sub_rows else {}}")

        try:
            tick = await asyncio.wait_for(first_tick_future, timeout=tick_timeout_s)
            print(
                "[tick] received "
                f"instrument={tick.get('InstrumentID', '')} "
                f"last={tick.get('LastPrice', '')} "
                f"volume={tick.get('Volume', '')} "
                f"time={tick.get('UpdateTime', '')}"
            )
        except asyncio.TimeoutError:
            print(f"[tick] timeout after {tick_timeout_s:.1f}s, no tick received for {subscription_target}")
        finally:
            unsub_rows = await gw.request("UnSubscribeMarketData", [subscription_target])
            print(f"[UnSubscribeMarketData] rows={len(unsub_rows)} first={unsub_rows[0] if unsub_rows else {}}")

        reqs = [
            ("ReqQryTradingAccount", {"RequestID": next_request_id()}),
            (
                "ReqQryInvestorPositionDetail",
                {
                    "RequestID": next_request_id(),
                },
            ),
            (
                "ReqQryInstrumentCommissionRate",
                {
                    "RequestID": next_request_id(),
                    "InstrumentID": query_inst,
                    "ExchangeID": query_exchange,
                },
            ),
            (
                "ReqQryInstrumentMarginRate",
                {
                    "RequestID": next_request_id(),
                    "InstrumentID": query_inst,
                    "ExchangeID": query_exchange,
                    "HedgeFlag": "1",
                },
            ),
            (
                "ReqQryOrder",
                {
                    "RequestID": next_request_id(),
                    "InstrumentID": query_inst,
                    "ExchangeID": query_exchange,
                    "InsertTimeStart": "00:00:00",
                    "InsertTimeEnd": "23:59:59",
                },
            ),
            (
                "ReqQryTrade",
                {
                    "RequestID": next_request_id(),
                    "InstrumentID": query_inst,
                    "ExchangeID": query_exchange,
                    "TradeTimeStart": "00:00:00",
                    "TradeTimeEnd": "23:59:59",
                },
            ),
        ]

        for name, payload in reqs:
            try:
                rows = await gw.request(name, payload)
                count = len(rows)
                first = rows[0] if rows else {}
                print(f"[{name}] rows={count} first={first}")
            except Exception as e:
                print(f"[{name}] exception={type(e).__name__}: {e}")
                print("native链路已触发，但当前环境未完成登录/前置不可达。")
                break
    finally:
        await gw.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[interrupt] user cancelled, exiting gracefully.")
