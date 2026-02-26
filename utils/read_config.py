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
import copy
import xml.etree.ElementTree as ET
import configparser
from configparser import MissingSectionHeaderError
from appdirs import AppDirs
import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _default_module_path() -> str:
    if os.name == 'nt':
        return os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'build', 'Release').replace('\\', '/')
    return os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'build').replace('\\', '/')


DEFAULT_CONFIG_DATA = {
    'trade': {
        'command_timeout': 5,
        'ignore_inst': '',
        'transport': 'native',
    },
    'ctp_native': {
        'gateway': 'pybind',
        'module': 'ctp_bridge_native',
        'client_class': 'CtpClient',
        'module_path': _default_module_path(),
        'dll_dir': os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'api', 'win').replace('\\', '/'),
        'trade_front': 'tcp://xxx:xxx',
        'market_front': 'tcp://xxx:xxx',
        'broker_id': '9999',
        'investor_id': '123456',
        'password': 'passwd',
        'appid': 'xxx',
        'authcode': 'xxx',
        'userinfo': 'xxx',
        'flow_path': os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'flow').replace('\\', '/'),
        'request_timeout_ms': 10000,
        'test_instrument': 'IF99',
    },
    'log': {
        'level': 'DEBUG',
        'format': '%(asctime)s %(name)s [%(levelname)s] %(message)s',
        'weixin_level': 'INFO',
        'weixin_format': '[%(levelname)s] %(message)s',
    },
    'host': {
        'ip': '1.2.3.4',
        'mac': '02:03:04:5a:6b:7c',
    },
    'ssh_tunnel': {
        'enabled': False,
        'host': '127.0.0.1',
        'port': 22,
        'local_node': 'localhost',
        'private_key_linux': '/root/.ssh/id_ed25519',
        'private_key_win': 'C:\\Users\\timer\\.ssh\\id_ed25519.ppk',
    },
    'weixin': {
        'Token': '',
        'EncodingAESKey': '',
        'CorpID': '',
        'Secret': '',
    },
}

app_dir = AppDirs('trader')
root_yaml_file = os.path.join(REPO_ROOT, 'config.yaml')
root_ini_file = os.path.join(REPO_ROOT, 'config.ini')


def _deep_merge_dict(base: dict, override: dict) -> dict:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def _normalize_yaml_data(data: dict | None) -> dict:
    if not isinstance(data, dict):
        return {}
    normalized = {}
    for section, section_values in data.items():
        section_name = str(section).strip().lower()
        if not section_name or not isinstance(section_values, dict):
            continue
        normalized[section_name] = {}
        for key, value in section_values.items():
            normalized[section_name][str(key).strip()] = value
    return normalized


def _load_ini_data(path: str) -> dict:
    parser = configparser.ConfigParser(interpolation=None)
    try:
        try:
            parser.read(path, encoding='utf-8')
        except UnicodeDecodeError:
            parser.read(path, encoding='gb18030')
        if parser.sections():
            out = {}
            for section in parser.sections():
                out[section.lower()] = {k: v for k, v in parser.items(section)}
            return out
    except MissingSectionHeaderError:
        pass

    raw = {}
    with open(path, 'rt', encoding='utf-8', errors='ignore') as file_obj:
        for line in file_obj:
            s = line.strip()
            if not s or s.startswith(';') or s.startswith('#') or '=' not in s:
                continue
            key, value = s.split('=', 1)
            raw[key.strip()] = value.strip()

    mapped = copy.deepcopy(DEFAULT_CONFIG_DATA)
    ctp_mapping = {
        'trade': 'trade_front',
        'market': 'market_front',
        'broker': 'broker_id',
        'investor': 'investor_id',
        'passwd': 'password',
        'appid': 'appid',
        'authcode': 'authcode',
        'userinfo': 'userinfo',
        'module_path': 'module_path',
    }
    for src_key, dst_key in ctp_mapping.items():
        if src_key in raw:
            mapped['ctp_native'][dst_key] = raw[src_key]
    if 'ip' in raw:
        mapped['host']['ip'] = raw['ip']
    if 'mac' in raw:
        mapped['host']['mac'] = raw['mac']
    return mapped


def _write_yaml(path: str, data: dict):
    with open(path, 'wt', encoding='utf-8') as file_obj:
        yaml.safe_dump(data, file_obj, allow_unicode=True, sort_keys=False)


def _ensure_yaml_config() -> tuple[str, dict]:
    if os.path.exists(root_yaml_file):
        with open(root_yaml_file, 'rt', encoding='utf-8') as file_obj:
            yaml_data = yaml.safe_load(file_obj) or {}
        return root_yaml_file, _normalize_yaml_data(yaml_data)

    if os.path.exists(root_ini_file):
        ini_data = _load_ini_data(root_ini_file)
        merged = _deep_merge_dict(DEFAULT_CONFIG_DATA, _normalize_yaml_data(ini_data))
        _write_yaml(root_yaml_file, merged)
        print('migrate config file:', root_ini_file, '->', root_yaml_file)
        return root_yaml_file, merged

    _write_yaml(root_yaml_file, DEFAULT_CONFIG_DATA)
    print('create config file:', root_yaml_file)
    return root_yaml_file, copy.deepcopy(DEFAULT_CONFIG_DATA)


def _to_config_parser(data: dict) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    for section, section_values in data.items():
        section_name = section.upper()
        if not parser.has_section(section_name):
            parser.add_section(section_name)
        for key, value in section_values.items():
            parser.set(section_name, key, '' if value is None else str(value))
    return parser


config_file, _config_data = _ensure_yaml_config()
config = _to_config_parser(_config_data)

ctp_errors = {}
if os.name == 'nt':
    ctp_xml_path = os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'api', 'win', 'error.xml')
else:
    ctp_xml_path = os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'api', 'linux', 'error.xml')

with open(ctp_xml_path, 'rb') as f:
    xml_text = f.read().decode('utf-8', errors='replace')
if xml_text.lstrip().startswith('<?xml'):
    xml_text = '\n'.join(xml_text.splitlines()[1:])
_ctp_root = ET.fromstring(xml_text)

for error in _ctp_root:
    ctp_errors[int(error.attrib['value'])] = error.attrib['prompt']
