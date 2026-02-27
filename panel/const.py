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
from django.db import models


class ContractType(models.TextChoices):
    STOCK = 'STOCK', '股票'
    FUTURE = 'FUTURE', '期货'
    OPTION = 'OPTION', '期权'


class ExchangeType(models.TextChoices):
    SHFE = 'SHFE', '上期所'
    DCE = 'DCE', '大商所'
    CZCE = 'CZCE', '郑商所'
    CFFEX = 'CFFEX', '中金所'
    INE = 'INE', '上期能源'
    GFEX = 'GFEX', '广交所'


class SectionType(models.TextChoices):
    Stock = 'Stock', '股票'
    Bond = 'Bond', '债券'
    Metal = 'Metal', '基本金属'
    Agricultural = 'Agricultural', '农产品'
    EnergyChemical = 'EnergyChemical', '能源化工'
    BlackMaterial = 'BlackMaterial', '黑色建材'


class SortType(models.TextChoices):
    Stock = 'Stock', '股票'
    Bond = 'Bond', '债券'
    Rare = 'Rare', '贵金属'
    Metal = 'Metal', '基本金属'
    EdibleOil = 'EdibleOil', '食用油'
    Feed = 'Feed', '动物饲料'
    Cotton = 'Cotton', '棉'
    EnergyChemical = 'EnergyChemical', '能源化工'
    BlackMaterial = 'BlackMaterial', '黑色建材'


class AddressType(models.TextChoices):
    TRADE = 'TRADE', '交易'
    MARKET = 'MARKET', '行情'


class OperatorType(models.TextChoices):
    TELECOM = 'TELECOM', '电信'
    UNICOM = 'UNICOM', '联通'


class DirectionType(models.TextChoices):
    LONG = '0', '多'
    SHORT = '1', '空'


class CombOffsetFlag(models.TextChoices):  # 订单开平标志
    Open = '0', '开'
    Close = '1', '平'
    ForceClose = '2', '强平'
    CloseToday = '3', '平'
    CloseYesterday = '4', '平昨'
    ForceOff = '5', '强减'
    LocalForceClose = '6', '本地强平'


class OffsetFlag(models.TextChoices):  # 开平标志
    Open = '0', '开'
    Close = '1', '平'
    ForceClose = '2', '强平'
    CloseToday = '3', '平今'
    CloseYesterday = '4', '平昨'
    ForceOff = '5', '强减'
    LocalForceClose = '6', '本地强平'


class OrderStatus(models.TextChoices):  # 报单状态
    AllTraded = '0', '全部成交'
    PartTradedQueueing = '1', '部分成交还在队列中'
    PartTradedNotQueueing = '2', '部分成交不在队列中'
    NoTradeQueueing = '3', '未成交还在队列中'
    NoTradeNotQueueing = '4', '未成交不在队列中'
    Canceled = '5', '撤单'
    Unknown = 'a', '未知'
    NotTouched = 'b', '尚未触发'
    Touched = 'c', '已触发'


class OrderSubmitStatus(models.TextChoices):  # 报单提交状态
    InsertSubmitted = '0', '已经提交'
    CancelSubmitted = '1', '撤单已经提交'
    ModifySubmitted = '2', '修改已经提交'
    Accepted = '3', '已经接受'
    InsertRejected = '4', '报单已经被拒绝'
    CancelRejected = '5', '撤单已经被拒绝'
    ModifyRejected = '6', '改单已经被拒绝'


DCE_NAME_CODE = {
    '豆一': 'a',
    '豆二': 'b',
    '胶合板': 'bb',
    '玉米': 'c',
    '玉米淀粉': 'cs',
    '纤维板': 'fb',
    '铁矿石': 'i',
    '焦炭': 'j',
    '鸡蛋': 'jd',
    '焦煤': 'jm',
    '聚乙烯': 'l',
    '豆粕': 'm',
    '棕榈油': 'p',
    '聚丙烯': 'pp',
    '聚氯乙烯': 'v',
    '苯乙烯': 'eb',
    '乙二醇': 'eg',
    '液化石油气': 'pg',
    '生猪': 'lh',
    '粳米': 'rr',
    '豆油': 'y',
    '原木': 'lg',
    '纯苯': 'bz',
}

MONTH_CODE = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z"
}


KT_MARKET = {
    'DL': 'DCE',
    'DY': 'DCE',
    'SQ': 'SHFE',
    'SY': 'SHFE',
    'ZJ': 'CFFEX',
    'ZZ': 'CZCE',
    'ZY': 'CZCE',
}


class SignalType(models.TextChoices):
    ROLL_CLOSE = 'ROLL_CLOSE', '换月平旧'
    ROLL_OPEN = 'ROLL_OPEN', '换月开新'
    BUY = 'BUY', '买开'
    SELL_SHORT = 'SELL_SHORT', '卖开'
    SELL = 'SELL', '卖平'
    BUY_COVER = 'BUY_COVER', '买平'


class PriorityType(models.IntegerChoices):
    LOW = 0, '低'
    Normal = 1, '普通'
    High = 2, '高'
