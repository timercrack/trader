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
import sys
import os
import django
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.environ["DJANGO_SETTINGS_MODULE"] = "dashboard.settings"
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()
import logging
from logging import handlers
from strategy.brother2 import TradeStrategy
from utils.read_config import config_file, app_dir, config
from weixin_notifier import install_weixin_log_handler


if __name__ == '__main__':
    if not os.path.exists(app_dir.user_log_dir):
        os.makedirs(app_dir.user_log_dir)
    log_file = os.path.join(app_dir.user_log_dir, 'trader.log')
    file_handler = handlers.RotatingFileHandler(log_file, encoding='utf-8', maxBytes=1024*1024, backupCount=1)
    general_formatter = logging.Formatter(config.get('LOG', 'format'))
    file_handler.setFormatter(general_formatter)
    file_handler.setLevel(config.get('LOG', 'file_level', fallback='DEBUG'))
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(general_formatter)
    console_handler.setLevel('DEBUG')
    logger = logging.getLogger()
    logger.setLevel('DEBUG')
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    install_weixin_log_handler(logger)
    logger = logging.getLogger("main")
    pid_path = os.path.join(app_dir.user_cache_dir, 'trader.pid')
    if not os.path.exists(pid_path):
        if not os.path.exists(app_dir.user_cache_dir):
            os.makedirs(app_dir.user_cache_dir)
    with open(pid_path, 'w') as pid_file:
        pid_file.write(str(os.getpid()))
    print('Big Brother is watching you!')
    print('used config file:', config_file)
    print('log stored in:', app_dir.user_log_dir)
    print('pid file:', pid_path)
    TradeStrategy(name='大哥2.2').run()
