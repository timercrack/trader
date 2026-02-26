# coding=utf-8
import configparser
import copy
import os
from configparser import MissingSectionHeaderError

import yaml


REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
ROOT_YAML_FILE = os.path.join(REPO_ROOT, 'config.yaml')
ROOT_INI_FILE = os.path.join(REPO_ROOT, 'config.ini')


def _default_module_path() -> str:
    if os.name == 'nt':
        return os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'build', 'Release').replace('\\', '/')
    return os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'build').replace('\\', '/')


_DEFAULT_RUNTIME_DATA = {
    'ctp_native': {
        'gateway': 'pybind',
        'module': 'ctp_bridge_native',
        'client_class': 'CtpClient',
        'module_path': _default_module_path(),
        'dll_dir': os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'api', 'win').replace('\\', '/'),
        'trade_front': 'tcp://180.168.146.187:10001',
        'market_front': 'tcp://180.168.146.187:10011',
        'broker_id': '',
        'investor_id': '',
        'password': '',
        'appid': '',
        'authcode': '',
        'userinfo': '',
        'flow_path': os.path.join(REPO_ROOT, 'native', 'ctp_bridge', 'flow').replace('\\', '/'),
        'request_timeout_ms': 10000,
        'test_instrument': 'IF99',
    },
    'trade': {
        'command_timeout': 5,
        'ignore_inst': 'WH,bb,JR,RI,RS,LR,PM,im',
        'transport': 'native',
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

    mapped = copy.deepcopy(_DEFAULT_RUNTIME_DATA)
    mapping = {
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
    for src_key, dst_key in mapping.items():
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


def _ensure_yaml_config() -> str:
    if os.path.exists(ROOT_YAML_FILE):
        return ROOT_YAML_FILE
    if os.path.exists(ROOT_INI_FILE):
        ini_data = _load_ini_data(ROOT_INI_FILE)
        yaml_data = _deep_merge_dict(_DEFAULT_RUNTIME_DATA, _normalize_yaml_data(ini_data))
        _write_yaml(ROOT_YAML_FILE, yaml_data)
        print('migrate config file:', ROOT_INI_FILE, '->', ROOT_YAML_FILE)
        return ROOT_YAML_FILE
    _write_yaml(ROOT_YAML_FILE, _DEFAULT_RUNTIME_DATA)
    print('create config file:', ROOT_YAML_FILE)
    return ROOT_YAML_FILE


def _to_config_parser(yaml_data: dict) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    for section, section_values in yaml_data.items():
        section_name = section.upper()
        if not parser.has_section(section_name):
            parser.add_section(section_name)
        for key, value in section_values.items():
            parser.set(section_name, key, '' if value is None else str(value))
    return parser


def load_runtime_config() -> configparser.ConfigParser:
    yaml_file = _ensure_yaml_config()
    with open(yaml_file, 'rt', encoding='utf-8') as file_obj:
        yaml_data = yaml.safe_load(file_obj) or {}
    normalized = _normalize_yaml_data(yaml_data)
    return _to_config_parser(normalized)


runtime_config = load_runtime_config()
