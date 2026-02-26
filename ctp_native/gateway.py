# coding=utf-8
from __future__ import annotations

import logging
import importlib
import sys
import os
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Callable

from runtime_config import runtime_config

logger = logging.getLogger('NativeGateway')
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/')


class NativeGateway(ABC):
    @abstractmethod
    async def start(self):
        raise NotImplementedError()

    @abstractmethod
    async def stop(self):
        raise NotImplementedError()

    @abstractmethod
    async def request(self, req_name: str, payload: Any) -> list[dict[str, Any]]:
        raise NotImplementedError()

    @abstractmethod
    def set_event_callback(self, cb: Callable[[str, dict[str, Any]], Any]):
        raise NotImplementedError()


class PlaceholderGateway(NativeGateway):
    def __init__(self):
        self._cb = None

    async def start(self):
        logger.warning('当前使用 PlaceholderGateway，请接入 pybind11 Thost 原生扩展。')

    async def stop(self):
        return None

    async def request(self, req_name: str, payload: Any) -> list[dict[str, Any]]:
        request_id = 0
        if isinstance(payload, dict):
            request_id = int(payload.get('RequestID', 0) or 0)
        return [{
            'ErrorID': -1,
            'ErrorMsg': f'Native gateway is not implemented for {req_name}',
            'RequestID': request_id,
            'bIsLast': True,
            'empty': True,
        }]

    def set_event_callback(self, cb: Callable[[str, dict[str, Any]], Any]):
        self._cb = cb


class PybindGateway(NativeGateway):
    def __init__(self):
        self._cb = None
        self._client = None
        self._fallback_reason = None
        self._request_timeout_ms = 10000

    async def start(self):
        try:
            def _resolve(path: str) -> str:
                """将相对路径解析为相对于项目根目录的绝对路径"""
                if path and not os.path.isabs(path):
                    return os.path.normpath(os.path.join(REPO_ROOT, path))
                return path

            dll_dir = _resolve(runtime_config.get('CTP_NATIVE', 'dll_dir', fallback=f'{REPO_ROOT}/native/ctp_bridge/api/win').strip())
            if os.name == 'nt' and dll_dir and os.path.isdir(dll_dir):
                os.add_dll_directory(dll_dir)
            module_path = _resolve(runtime_config.get('CTP_NATIVE', 'module_path', fallback=f'{REPO_ROOT}/native/ctp_bridge/build/Release').strip())
            if module_path and module_path not in sys.path:
                sys.path.insert(0, module_path)
            module_name = runtime_config.get('CTP_NATIVE', 'module', fallback='ctp_bridge_native')
            class_name = runtime_config.get('CTP_NATIVE', 'client_class', fallback='CtpClient')
            module = importlib.import_module(module_name)
            client_cls = getattr(module, class_name)
            self._client = client_cls()

            login_cfg = {
                'trade_front': runtime_config.get('CTP_NATIVE', 'trade_front', fallback='').strip(),
                'market_front': runtime_config.get('CTP_NATIVE', 'market_front', fallback='').strip(),
                'broker_id': runtime_config.get('CTP_NATIVE', 'broker_id', fallback='').strip(),
                'investor_id': runtime_config.get('CTP_NATIVE', 'investor_id', fallback='').strip(),
                'password': runtime_config.get('CTP_NATIVE', 'password', fallback='').strip(),
                'appid': runtime_config.get('CTP_NATIVE', 'appid', fallback='').strip(),
                'authcode': runtime_config.get('CTP_NATIVE', 'authcode', fallback='').strip(),
                'userinfo': runtime_config.get('CTP_NATIVE', 'userinfo', fallback='').strip(),
            }
            missing = [k for k, v in login_cfg.items() if not v]
            if missing:
                raise RuntimeError(f'CTP_NATIVE required config missing: {", ".join(missing)}')

            if hasattr(self._client, 'configure'):
                self._request_timeout_ms = runtime_config.getint('CTP_NATIVE', 'request_timeout_ms', fallback=10000)
                self._client.configure({
                    **login_cfg,
                    'ip': runtime_config.get('CTP_NATIVE', 'ip', fallback='1.2.3.4'),
                    'mac': runtime_config.get('CTP_NATIVE', 'mac', fallback='02:03:04:5a:6b:7c'),
                    'flow_path': _resolve(runtime_config.get('CTP_NATIVE', 'flow_path', fallback=f'{REPO_ROOT}/native/ctp_bridge/flow')),
                    'request_timeout_ms': self._request_timeout_ms,
                })
            if self._cb and hasattr(self._client, 'set_event_callback'):
                self._client.set_event_callback(self._cb)
            if hasattr(self._client, 'start'):
                self._client.start()
            self._fallback_reason = None
        except Exception as e:
            self._client = None
            self._fallback_reason = repr(e)
            logger.warning('Pybind gateway unavailable, fallback to placeholder mode: %s', self._fallback_reason)

    async def stop(self):
        if self._client and hasattr(self._client, 'stop'):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._client.stop)

    async def request(self, req_name: str, payload: Any) -> list[dict[str, Any]]:
        if not self._client:
            request_id = 0
            if isinstance(payload, dict):
                request_id = int(payload.get('RequestID', 0) or 0)
            return [{
                'ErrorID': -1,
                'ErrorMsg': f'Pybind gateway unavailable: {self._fallback_reason}',
                'RequestID': request_id,
                'bIsLast': True,
                'empty': True,
            }]
        if not hasattr(self._client, 'request'):
            raise RuntimeError('pybind client missing request(req_name, payload) method')
        loop = asyncio.get_running_loop()
        timeout_s = max(float(self._request_timeout_ms) / 1000.0 + 2.0, 3.0)
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(None, self._client.request, req_name, payload),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f'Pybind request timeout after {timeout_s:.1f}s: {req_name}') from exc
        if result is None:
            return []
        if isinstance(result, dict):
            return [result]
        return list(result)

    def set_event_callback(self, cb: Callable[[str, dict[str, Any]], Any]):
        self._cb = cb
        if self._client and hasattr(self._client, 'set_event_callback'):
            self._client.set_event_callback(cb)


def get_gateway() -> NativeGateway:
    gateway_name = runtime_config.get('CTP_NATIVE', 'gateway', fallback='placeholder').lower().strip()
    if gateway_name == 'pybind':
        return PybindGateway()
    if gateway_name == 'placeholder':
        return PlaceholderGateway()
    logger.warning('未知 native gateway=%s，自动回退 placeholder', gateway_name)
    return PlaceholderGateway()
