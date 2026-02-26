# coding=utf-8
#
# Copyright 2016 timercrack
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import atexit
import logging
import queue
import threading
import time
from typing import Optional

import orjson
import requests

from runtime_config import runtime_config

logger = logging.getLogger('WeixinNotifier')


class WeixinNotifier:
    def __init__(self):
        self._corp_id = runtime_config.get('WEIXIN', 'CorpID', fallback='').strip()
        self._secret = runtime_config.get('WEIXIN', 'Secret', fallback='').strip()
        self._to_user = runtime_config.get('WEIXIN', 'ToUser', fallback='@all').strip() or '@all'
        self._agent_id = runtime_config.getint('WEIXIN', 'AgentID', fallback=0)
        self._token: Optional[str] = None
        self._token_expire_at = 0.0
        self._queue: queue.Queue[str] = queue.Queue(maxsize=1000)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def enabled(self) -> bool:
        return bool(self._corp_id and self._secret)

    def start(self):
        if not self.enabled:
            logger.info('企业微信推送未启用：缺少 weixin.CorpID 或 weixin.Secret')
            return
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name='weixin-notifier', daemon=True)
        self._thread.start()
        atexit.register(self.stop)

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def enqueue(self, message: str):
        if not self.enabled:
            return
        text = (message or '').strip()
        if not text:
            return
        try:
            self._queue.put_nowait(text)
        except queue.Full:
            try:
                _ = self._queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(text)
            except queue.Full:
                pass

    def _run(self):
        while not self._stop_event.is_set():
            try:
                message = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._send_text(message)
            except Exception as e:
                logger.warning('企业微信消息发送失败: %s', repr(e), exc_info=True)

    def _get_access_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expire_at - 60:
            return self._token
        resp = requests.get(
            'https://qyapi.weixin.qq.com/cgi-bin/gettoken',
            params={'corpid': self._corp_id, 'corpsecret': self._secret},
            timeout=10,
        )
        data = resp.json()
        err_code = data.get('errcode', -1)
        if err_code != 0:
            raise RuntimeError(f'gettoken failed: errcode={err_code}, errmsg={data.get("errmsg", "")!r}')
        token = str(data.get('access_token', '') or '').strip()
        if not token:
            raise RuntimeError('gettoken failed: empty access_token')
        self._token = token
        expires_in = int(data.get('expires_in', 7200) or 7200)
        self._token_expire_at = now + expires_in
        return token

    def _send_text(self, text: str):
        token = self._get_access_token()
        payload = {
            'touser': self._to_user,
            'msgtype': 'text',
            'agentid': self._agent_id,
            'text': {'content': text},
            'safe': 0,
        }
        resp = requests.post(
            f'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}',
            data=orjson.dumps(payload),
            headers={'Content-Type': 'application/json; charset=utf-8'},
            timeout=10,
        )
        data = resp.json()
        err_code = data.get('errcode', -1)
        if err_code != 0:
            raise RuntimeError(f'send message failed: errcode={err_code}, errmsg={data.get("errmsg", "")!r}')


class WeixinLogHandler(logging.Handler):
    def __init__(self, notifier: WeixinNotifier):
        super().__init__()
        self._notifier = notifier

    def emit(self, record: logging.LogRecord):
        try:
            self._notifier.enqueue(self.format(record))
        except Exception:
            self.handleError(record)


_notifier_singleton: Optional[WeixinNotifier] = None


def get_weixin_notifier() -> WeixinNotifier:
    global _notifier_singleton
    if _notifier_singleton is None:
        _notifier_singleton = WeixinNotifier()
    return _notifier_singleton


def install_weixin_log_handler(root_logger: logging.Logger):
    notifier = get_weixin_notifier()
    notifier.start()
    if not notifier.enabled:
        return
    for handler in root_logger.handlers:
        if isinstance(handler, WeixinLogHandler):
            return
    handler = WeixinLogHandler(notifier)
    handler.setLevel(runtime_config.get('LOG', 'weixin_level', fallback='ERROR'))
    handler.setFormatter(logging.Formatter(runtime_config.get('LOG', 'weixin_format', fallback='[%(levelname)s] %(message)s')))
    root_logger.addHandler(handler)
