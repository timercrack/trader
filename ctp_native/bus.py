# coding=utf-8
from __future__ import annotations

import asyncio
import fnmatch
import inspect
import logging
from typing import Any
import orjson

from ctp_native.gateway import NativeGateway, get_gateway
from ctp_native.state_store import state_store
from runtime_config import runtime_config

logger = logging.getLogger('NativeBus')


class LocalPubSub:
    def __init__(self, bus: 'LocalEventBus', ignore_subscribe_messages: bool = True):
        self._bus = bus
        self._ignore_subscribe_messages = ignore_subscribe_messages
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._patterns: set[str] = set()
        self.in_pubsub = False
        self._closed = False

    async def psubscribe(self, *patterns: str):
        for pattern in patterns:
            self._patterns.add(pattern)
            self._bus.register(pattern, self)
            if not self._ignore_subscribe_messages:
                await self._queue.put({'type': 'psubscribe', 'pattern': pattern, 'channel': None, 'data': 1})
        self.in_pubsub = len(self._patterns) > 0

    async def punsubscribe(self, *patterns: str):
        targets = set(patterns) if patterns else set(self._patterns)
        for pattern in list(targets):
            self._bus.unregister(pattern, self)
            self._patterns.discard(pattern)
            if not self._ignore_subscribe_messages:
                await self._queue.put({'type': 'punsubscribe', 'pattern': pattern, 'channel': None, 'data': 0})
        self.in_pubsub = len(self._patterns) > 0
        await self._queue.put({'type': 'punsubscribe', 'pattern': None, 'channel': None, 'data': 0})

    async def unsubscribe(self):
        await self.punsubscribe()

    async def close(self):
        if self._closed:
            return
        await self.punsubscribe()
        self._closed = True

    async def _push(self, pattern: str, channel: str, data: Any):
        await self._queue.put({'type': 'pmessage', 'pattern': pattern, 'channel': channel, 'data': data})

    async def listen(self):
        while True:
            msg = await self._queue.get()
            yield msg
            if msg.get('type') == 'punsubscribe' and not self.in_pubsub:
                break


class LocalEventBus:
    def __init__(self):
        self._subs: dict[str, set[LocalPubSub]] = {}

    def register(self, pattern: str, sub: LocalPubSub):
        self._subs.setdefault(pattern, set()).add(sub)

    def unregister(self, pattern: str, sub: LocalPubSub):
        subs = self._subs.get(pattern)
        if not subs:
            return
        subs.discard(sub)
        if not subs:
            self._subs.pop(pattern, None)

    async def publish(self, channel: str, payload: Any):
        for pattern, subs in list(self._subs.items()):
            if not fnmatch.fnmatch(channel, pattern):
                continue
            for sub in list(subs):
                await sub._push(pattern, channel, payload)


class LocalRedisLikeClient:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._bus = LocalEventBus()
        self._gateway: NativeGateway = get_gateway()
        self._request_format = runtime_config.get('MSG_CHANNEL', 'request_format', fallback='MSG:CTP:REQ:{}')
        self._trade_response_format = runtime_config.get('MSG_CHANNEL', 'trade_response_format', fallback='MSG:CTP:RSP:TRADE:{}:{}')
        self._market_response_format = runtime_config.get('MSG_CHANNEL', 'market_response_format', fallback='MSG:CTP:RSP:MARKET:{}:{}')
        self._request_prefix = self._request_format.split('{}')[0]
        self._gateway.set_event_callback(self._on_gateway_event_sync)

    async def start(self):
        await self._gateway.start()

    async def stop(self):
        await self._gateway.stop()
        state_store.dump_snapshot()

    def pubsub(self, ignore_subscribe_messages: bool = True):
        return LocalPubSub(self._bus, ignore_subscribe_messages=ignore_subscribe_messages)

    def get(self, key: str):
        return state_store.get(key)

    def set(self, key: str, value: Any, ex: int | None = None):
        state_store.set(key, value, ex=ex)

    def publish(self, channel: str, payload: Any):
        if channel.startswith(self._request_prefix):
            req_name = channel[len(self._request_prefix):]
            self._schedule(self._handle_request(req_name, payload))
            return 1
        self._schedule(self._bus.publish(channel, payload))
        return 1

    async def _on_gateway_event(self, topic: str, data: dict[str, Any]):
        if topic in {
            'OnFrontConnected',
            'OnFrontDisconnected',
            'OnFrontConnectedMd',
            'OnFrontDisconnectedMd',
            'OnRspAuthenticate',
            'OnRspUserLogin',
            'OnRspUserLoginMd',
            'OnRspSettlementInfoConfirm',
            'OnRspError',
        }:
            logger.debug('gateway event %s: %s', topic, data)
        # 统一兼容历史 channel 风格
        if topic.startswith('OnRtnDepthMarketData'):
            inst = data.get('InstrumentID', 'UNKNOWN')
            channel = self._market_response_format.format('OnRtnDepthMarketData', inst)
        else:
            order_ref = data.get('OrderRef', data.get('RequestID', 0))
            channel = self._trade_response_format.format(topic, order_ref)
        await self._bus.publish(channel, orjson.dumps(data))

    def _on_gateway_event_sync(self, topic: str, data: dict[str, Any]):
        self._schedule(self._on_gateway_event(topic, data))

    async def _handle_request(self, req_name: str, payload: Any):
        try:
            data = orjson.loads(payload) if payload else {}
        except Exception:
            data = payload
        request_id = 0
        if isinstance(data, dict):
            request_id = int(data.get('RequestID', 0) or 0)
        try:
            rows = await self._gateway.request(req_name, data)
            rows = rows or []
            if req_name.startswith('ReqQry'):
                rsp_name = 'OnRspQry' + req_name[6:]
                await self._publish_rows(self._trade_response_format.format(rsp_name, request_id), rows)
            elif req_name in {'SubscribeMarketData', 'UnSubscribeMarketData'}:
                rsp_name = 'OnRspSubMarketData' if req_name == 'SubscribeMarketData' else 'OnRspUnSubMarketData'
                await self._publish_rows(self._market_response_format.format(rsp_name, 0), rows)
            elif req_name == 'ReqOrderAction':
                await self._publish_rows(self._trade_response_format.format('OnRspOrderAction', 0), rows)
            else:
                # ReqOrderInsert 无需同步回包，回报由 OnRtnOrder/OnRtnTrade 异步事件提供
                pass
        except Exception as e:
            logger.warning('native request failed: %s payload=%s err=%s', req_name, data, repr(e), exc_info=True)
            err_row = {'ErrorID': -1, 'ErrorMsg': repr(e), 'RequestID': request_id, 'bIsLast': True}
            if req_name in {'SubscribeMarketData', 'UnSubscribeMarketData'}:
                await self._bus.publish(self._market_response_format.format('OnRspError', request_id), orjson.dumps(err_row))
            else:
                await self._bus.publish(self._trade_response_format.format('OnRspError', request_id), orjson.dumps(err_row))

    async def _publish_rows(self, channel: str, rows: list[dict[str, Any]]):
        if not rows:
            await self._bus.publish(channel, orjson.dumps({'empty': True, 'bIsLast': True}))
            return
        for idx, row in enumerate(rows):
            if 'bIsLast' not in row:
                row['bIsLast'] = idx == len(rows) - 1
            await self._bus.publish(channel, orjson.dumps(row))

    def _schedule(self, coro):
        if inspect.isawaitable(coro):
            self._loop.call_soon_threadsafe(asyncio.create_task, coro)  # type: ignore[arg-type]
