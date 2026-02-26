# coding=utf-8
# pyright: reportAttributeAccessIssue=false, reportOptionalMemberAccess=false, reportOptionalSubscript=false, reportOptionalOperand=false, reportArgumentType=false, reportOperatorIssue=false, reportGeneralTypeIssues=false, reportCallIssue=false
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
import asyncio
import re
from abc import abstractmethod
from collections import defaultdict
import datetime
from decimal import Decimal
from typing import Any
import logging
from django.db.models import Q, F, Sum
from django.utils import timezone
import orjson
from strategy import BaseModule
from utils.func_container import RegisterCallback
from utils.read_config import config, ctp_errors
from utils import ApiStruct, price_round, is_trading_day, update_from_shfe, update_from_dce, update_from_czce, update_from_cffex, \
    get_contracts_argument, calc_main_inst, str_to_number, get_next_id, ORDER_REF_SIGNAL_ID_START, update_from_gfex
from panel.models import *

logger = logging.getLogger('CTPApi')
HANDLER_TIME_OUT = config.getint('TRADE', 'command_timeout', fallback=10)


class BaseTradeStrategy(BaseModule):
    """
    交易策略公共基类，封装 CTP 通信、账户/持仓管理、回调处理、定时任务等共用逻辑。
    子类只需实现 calc_signal()，可选覆盖 get_signal_instruments() / get_margin_threshold()。
    """

    def __init__(self, name: str):
        super().__init__()
        self._market_response_format = config.get('MSG_CHANNEL', 'market_response_format', fallback='MSG:CTP:RSP:MARKET:{}:{}')
        self._trade_response_format = config.get('MSG_CHANNEL', 'trade_response_format', fallback='MSG:CTP:RSP:TRADE:{}:{}')
        self._request_format = config.get('MSG_CHANNEL', 'request_format', fallback='MSG:CTP:REQ:{}')
        self._ignore_inst_list = config.get('TRADE', 'ignore_inst', fallback="WH,bb,JR,RI,RS,LR,PM,im").split(',')
        self._strategy = Strategy.objects.get(name=name)
        self._inst_ids = self._strategy.instruments.all().values_list('product_code', flat=True)
        self._broker = self._strategy.broker
        self._fake = self._broker.fake  # 虚拟资金
        self._current = self._broker.current  # 当前动态权益
        self._pre_balance = self._broker.pre_balance  # 静态权益
        self._cash = self._broker.cash  # 可用资金
        self._shares = dict()  # { instrument : position }
        self._cur_account = None
        self._margin = self._broker.margin  # 占用保证金
        self._withdraw = 0  # 出金
        self._deposit = 0  # 入金
        self._activeOrders = dict()  # 未成交委托单
        self._cur_pos = dict()  # 持有头寸
        self._re_extract_code = re.compile(r'([a-zA-Z]*)(\d+)')  # 提合约字母部分 IF1509 -> IF
        self._re_extract_name = re.compile('(.*?)([0-9]+)(.*?)$')  # 提取合约文字部分
        trading_day = self.redis_client.get("TradingDay")
        if not trading_day:
            trading_day = timezone.localtime().strftime('%Y%m%d')
            self.redis_client.set('TradingDay', trading_day)
        last_trading_day = self.redis_client.get("LastTradingDay")
        if not last_trading_day:
            last_trading_day = trading_day
            self.redis_client.set('LastTradingDay', last_trading_day)
        self._trading_day = timezone.make_aware(datetime.datetime.strptime(trading_day + '08', '%Y%m%d%H'))
        self._last_trading_day = timezone.make_aware(datetime.datetime.strptime(last_trading_day + '08', '%Y%m%d%H'))
        # CTP 连接状态
        self._trade_connected: bool = False

    # ═══════════════════════════════════════════════════════════════════
    # 参数读取
    # ═══════════════════════════════════════════════════════════════════

    def get_param(self, code: str, default=None):
        """从 DB Param 表读取参数值，优先返回 int_value → float_value → str_value，未找到则返回 default"""
        try:
            p = self._strategy.param_set.get(code=code)
            for val in [p.int_value, p.float_value, p.str_value]:
                if val is not None:
                    return val
            return default
        except Exception:
            return default

    # ═══════════════════════════════════════════════════════════════════
    # 启动 / 账户 / 持仓 / 合约
    # ═══════════════════════════════════════════════════════════════════

    async def start(self):
        await self.install()
        self.redis_client.set('HEARTBEAT:TRADER', 1, ex=61)
        today = timezone.localtime()
        now = int(today.strftime('%H%M'))
        _, trading = await is_trading_day(today)
        if trading and (820 <= now <= 1550 or 2010 <= now <= 2359):
            await self._session_init()
        # 延迟检查 CTP 连接状态
        self.io_loop.call_later(10, asyncio.create_task, self._check_initial_connection())

    async def _check_initial_connection(self):
        if not self._trade_connected:
            logger.info('CTP 前置未连接，可能处于非交易时段，等待自动重连...')

    async def _session_init(self):
        """盘前初始化：刷新账户、撤销未成交订单、同步持仓"""
        if not self._trade_connected:
            logger.warning('CTP 交易前置未连接，跳过 session_init')
            return
        try:
            await self.refresh_account()
            order_list = await self.query('Order') or []
            for order in order_list:
                # 未成交订单
                if int(order['OrderStatus']) in range(1, 5) and order['OrderSubmitStatus'] == ApiStruct.OSS_Accepted:
                    direct_str = DirectionType.values[order['Direction']]
                    logger.info(f"撤销未成交订单: 合约{order['InstrumentID']} {direct_str}单 {order['VolumeTotal']}手 价格{order['LimitPrice']}")
                    await self.cancel_order(order)
                # 已成交订单
                elif order['OrderSubmitStatus'] == ApiStruct.OSS_Accepted:
                    self.save_order(order)
            await self.refresh_position()
        except Exception as e:
            logger.warning(f'_session_init 发生错误: {repr(e)}', exc_info=True)

    async def refresh_account(self):
        try:
            logger.debug('更新账户')
            account = await self.query('TradingAccount')
            account = account[0]
            self._withdraw = Decimal(account['Withdraw'])
            self._deposit = Decimal(account['Deposit'])
            # 虚拟=虚拟(原始)-入金+出金
            fake = self._fake - self._deposit + self._withdraw
            if fake < 1:
                fake = 0
            # 静态权益=上日结算+入金金额-出金金额
            self._pre_balance = Decimal(account['PreBalance']) + self._deposit - self._withdraw
            # 动态权益=静态权益+平仓盈亏+持仓盈亏-手续费
            self._current = self._pre_balance + Decimal(account['CloseProfit']) + Decimal(account['PositionProfit']) - Decimal(account['Commission'])
            self._margin = Decimal(account['CurrMargin'])
            self._cash = Decimal(account['Available'])
            self._cur_account = account
            self._broker.cash = self._cash
            self._broker.current = self._current
            self._broker.pre_balance = self._pre_balance
            self._broker.margin = self._margin
            self._broker.save(update_fields=['cash', 'current', 'pre_balance', 'margin'])
            logger.debug(f"更新账户,可用资金: {self._cash:,.0f} 静态权益: {self._pre_balance:,.0f} 动态权益: {self._current:,.0f} "
                         f"出入金: {self._withdraw - self._deposit:,.0f} 虚拟: {fake:,.0f}")
        except Exception as e:
            logger.warning(f'refresh_account 发生错误: {repr(e)}', exc_info=True)

    async def refresh_position(self):
        try:
            logger.debug('更新持仓...')
            pos_list = await self.query('InvestorPositionDetail') or []
            self._cur_pos.clear()
            for pos in pos_list:
                if 'empty' in pos and pos['empty'] is True or len(pos['InstrumentID']) > 6:
                    continue
                if pos['Volume'] > 0:
                    old_pos = self._cur_pos.get(pos['InstrumentID'])
                    if old_pos is None:
                        self._cur_pos[pos['InstrumentID']] = pos
                    else:
                        old_pos['OpenPrice'] = (old_pos['OpenPrice'] * old_pos['Volume'] + pos['OpenPrice'] * pos['Volume']) / (old_pos['Volume'] + pos['Volume'])
                        old_pos['Volume'] += pos['Volume']
                        old_pos['PositionProfitByTrade'] += pos['PositionProfitByTrade']
                        old_pos['Margin'] += pos['Margin']
            Trade.objects.filter(~Q(code__in=self._cur_pos.keys()), close_time__isnull=True).delete()  # 删除不存在的头寸
            for _, pos in self._cur_pos.items():
                p_code = self._re_extract_code.match(pos['InstrumentID']).group(1)
                inst = Instrument.objects.get(product_code=p_code)
                trade = Trade.objects.filter(broker=self._broker, strategy=self._strategy, instrument=inst, code=pos['InstrumentID'], close_time__isnull=True,
                                             direction=DirectionType.values[pos['Direction']]).first()
                bar = DailyBar.objects.filter(code=pos['InstrumentID']).order_by('-time').first()
                profit = (bar.close - Decimal(pos['OpenPrice'])) * pos['Volume'] * inst.volume_multiple
                if pos['Direction'] == DirectionType.values[DirectionType.SHORT]:
                    profit *= -1
                if trade:
                    trade.shares = (trade.closed_shares if trade.closed_shares else 0) + pos['Volume']
                    trade.filled_shares = trade.shares
                    trade.profit = profit
                    trade.save(update_fields=['shares', 'filled_shares', 'profit'])
                else:
                    Trade.objects.create(
                        broker=self._broker, strategy=self._strategy, instrument=inst, code=pos['InstrumentID'], profit=profit, filled_shares=pos['Volume'],
                        direction=DirectionType.values[pos['Direction']], avg_entry_price=Decimal(pos['OpenPrice']), shares=pos['Volume'],
                        open_time=timezone.make_aware(datetime.datetime.strptime(pos['OpenDate'] + '08', '%Y%m%d%H')), frozen_margin=Decimal(pos['Margin']),
                        cost=pos['Volume'] * Decimal(pos['OpenPrice']) * inst.fee_money * inst.volume_multiple + pos['Volume'] * inst.fee_volume)
            logger.debug('更新持仓完成!')
        except Exception as e:
            logger.warning(f'refresh_position 发生错误: {repr(e)}', exc_info=True)

    async def refresh_instrument(self):
        try:
            logger.debug("更新合约...")
            inst_dict = defaultdict(dict)
            inst_list = await self.query('Instrument') or []
            for inst in inst_list:
                if inst['empty']:
                    continue
                if inst['IsTrading'] == 1 and chr(inst['ProductClass']) == ApiStruct.PC_Futures:
                    if inst['ProductID'] in self._ignore_inst_list or inst['LongMarginRatio'] > 1:
                        continue
                    inst_dict[inst['ProductID']][inst['InstrumentID']] = dict()
                    inst_dict[inst['ProductID']][inst['InstrumentID']]['exchange'] = inst['ExchangeID']
                    inst_dict[inst['ProductID']][inst['InstrumentID']]['name'] = inst['InstrumentName']
                    inst_dict[inst['ProductID']][inst['InstrumentID']]['multiple'] = inst['VolumeMultiple']
                    inst_dict[inst['ProductID']][inst['InstrumentID']]['price_tick'] = inst['PriceTick']
            for code in inst_dict.keys():
                all_inst = ','.join(sorted(inst_dict[code].keys()))
                inst_data = list(inst_dict[code].values())[0]
                valid_name = self._re_extract_name.match(inst_data['name'])
                if valid_name is not None:
                    valid_name = valid_name.group(1)
                else:
                    valid_name = inst_data['name']
                if valid_name == code:
                    valid_name = ''
                inst_data['name'] = valid_name
                inst, created = Instrument.objects.update_or_create(product_code=code)
                print(f"inst:{inst} created:{created} main_code:{inst.main_code}")
                update_field_list = list()
                # 更新主力合约的保证金和手续费
                if inst.main_code:
                    margin_rate = await self.query('InstrumentMarginRate', InstrumentID=inst.main_code)
                    inst.margin_rate = margin_rate[0]['LongMarginRatioByMoney']
                    fee = await self.query('InstrumentCommissionRate', InstrumentID=inst.main_code)
                    inst.fee_money = Decimal(fee[0]['CloseRatioByMoney'])
                    inst.fee_volume = Decimal(fee[0]['CloseRatioByVolume'])
                    update_field_list += ['margin_rate', 'fee_money', 'fee_volume']
                if created:
                    inst.name = inst_data['name']
                    inst.volume_multiple = inst_data['multiple']
                    inst.price_tick = inst_data['price_tick']
                    update_field_list += ['name', 'volume_multiple', 'price_tick']
                elif inst.main_code:
                    inst.all_inst = all_inst
                    update_field_list.append('all_inst')
                inst.save(update_fields=update_field_list)
            logger.debug("更新合约完成!")
        except Exception as e:
            logger.warning(f'refresh_instrument 发生错误: {repr(e)}', exc_info=True)

    # ═══════════════════════════════════════════════════════════════════
    # 持仓查询
    # ═══════════════════════════════════════════════════════════════════

    def getShares(self, instrument: str):
        # 这个函数只能处理持有单一方向仓位的情况，若同时持有多空的头寸，返回结果不正确
        shares = 0
        pos_price = 0
        for pos in self._shares[instrument]:
            pos_price += pos['Volume'] * pos['OpenPrice']
            shares += pos['Volume'] * (-1 if pos['Direction'] == DirectionType.SHORT else 1)
        return shares, pos_price / abs(shares), self._shares[instrument][0]['OpenDate']

    def getPositions(self, inst_id: int):
        # 这个函数只能处理持有单一方向仓位的情况，若同时持有多空的头寸，返回结果不正确
        return self._shares[inst_id][0]

    # ═══════════════════════════════════════════════════════════════════
    # CTP 查询通信
    # ═══════════════════════════════════════════════════════════════════

    def async_query(self, query_type: str, **kwargs):
        request_id = get_next_id()
        kwargs['RequestID'] = request_id
        self.redis_client.publish(self._request_format.format('ReqQry' + query_type), orjson.dumps(kwargs))

    @staticmethod
    async def query_reader(pb):
        msg_list = []
        async for msg in pb.listen():
            msg_dict = orjson.loads(msg['data'])
            if 'empty' not in msg_dict or not msg_dict['empty']:
                msg_list.append(msg_dict)
            if 'bIsLast' not in msg_dict or msg_dict['bIsLast']:
                return msg_list

    async def query(self, query_type: str, **kwargs):
        sub_client = None
        channel_rsp_qry, channel_rsp_err = None, None
        try:
            sub_client = self.redis_client.pubsub(ignore_subscribe_messages=True)
            request_id = get_next_id()
            kwargs['RequestID'] = request_id
            channel_rsp_qry = self._trade_response_format.format('OnRspQry' + query_type, request_id)
            channel_rsp_err = self._trade_response_format.format('OnRspError', request_id)
            await sub_client.psubscribe(channel_rsp_qry, channel_rsp_err)
            task = asyncio.create_task(self.query_reader(sub_client))
            self.redis_client.publish(self._request_format.format('ReqQry' + query_type), orjson.dumps(kwargs))
            await asyncio.wait_for(task, HANDLER_TIME_OUT)
            await sub_client.punsubscribe()
            await sub_client.close()
            return task.result()
        except Exception as e:
            logger.warning(f'{query_type} 发生错误: {repr(e)}', exc_info=True)
            if sub_client and channel_rsp_qry:
                await sub_client.unsubscribe()
                await sub_client.close()
            return None

    async def SubscribeMarketData(self, inst_ids: list):
        sub_client = None
        channel_rsp_dat, channel_rsp_err = None, None
        try:
            sub_client = self.redis_client.pubsub(ignore_subscribe_messages=True)
            channel_rsp_dat = self._market_response_format.format('OnRspSubMarketData', 0)
            channel_rsp_err = self._market_response_format.format('OnRspError', 0)
            await sub_client.psubscribe(channel_rsp_dat, channel_rsp_err)
            task = asyncio.create_task(self.query_reader(sub_client))
            self.redis_client.publish(self._request_format.format('SubscribeMarketData'), orjson.dumps(inst_ids))
            await asyncio.wait_for(task, HANDLER_TIME_OUT)
            await sub_client.punsubscribe()
            await sub_client.close()
            return task.result()
        except Exception as e:
            logger.warning(f'SubscribeMarketData 发生错误: {repr(e)}', exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_rsp_dat:
                await sub_client.unsubscribe()
                await sub_client.close()
            return None

    async def UnSubscribeMarketData(self, inst_ids: list):
        sub_client = None
        channel_rsp_dat, channel_rsp_err = None, None
        try:
            sub_client = self.redis_client.pubsub(ignore_subscribe_messages=True)
            channel_rsp_dat = self._market_response_format.format('OnRspUnSubMarketData', 0)
            channel_rsp_err = self._market_response_format.format('OnRspError', 0)
            await sub_client.psubscribe(channel_rsp_dat, channel_rsp_err)
            task = asyncio.create_task(self.query_reader(sub_client))
            self.redis_client.publish(self._request_format.format('UnSubscribeMarketData'), orjson.dumps(inst_ids))
            await asyncio.wait_for(task, HANDLER_TIME_OUT)
            await sub_client.punsubscribe()
            await sub_client.close()
            return task.result()
        except Exception as e:
            logger.warning(f'UnSubscribeMarketData 发生错误: {repr(e)}', exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_rsp_dat:
                await sub_client.unsubscribe()
                await sub_client.close()
            return None

    # ═══════════════════════════════════════════════════════════════════
    # 下单 / 撤单
    # ═══════════════════════════════════════════════════════════════════

    def ReqOrderInsert(self, sig: Signal):
        try:
            request_id = get_next_id()
            autoid = Autonumber.objects.create()
            order_ref = f"{autoid.id:07}{sig.id:05}"
            param_dict = dict()
            param_dict['RequestID'] = request_id
            param_dict['OrderRef'] = order_ref
            param_dict['InstrumentID'] = sig.code
            param_dict['VolumeTotalOriginal'] = sig.volume
            param_dict['LimitPrice'] = float(sig.price)
            match sig.type:
                case SignalType.BUY | SignalType.SELL_SHORT:
                    param_dict['Direction'] = ApiStruct.D_Buy if sig.type == SignalType.BUY else ApiStruct.D_Sell
                    param_dict['CombOffsetFlag'] = ApiStruct.OF_Open
                    logger.info(f'{sig.instrument} {sig.type}{sig.volume}手 价格: {sig.price}')
                case SignalType.BUY_COVER | SignalType.SELL:
                    param_dict['Direction'] = ApiStruct.D_Buy if sig.type == SignalType.BUY_COVER else ApiStruct.D_Sell
                    param_dict['CombOffsetFlag'] = ApiStruct.OF_Close
                    pos = Trade.objects.filter(
                        broker=self._broker, strategy=self._strategy, code=sig.code, shares=sig.volume, close_time__isnull=True,
                        direction=DirectionType.values[DirectionType.SHORT] if sig.type == SignalType.BUY_COVER else DirectionType.values[
                            DirectionType.LONG]).first()
                    if pos.open_time.astimezone().date() == timezone.localtime().date() and pos.instrument.exchange == ExchangeType.SHFE:
                        param_dict['CombOffsetFlag'] = ApiStruct.OF_CloseToday  # 上期所区分平今和平昨
                    logger.info(f'{sig.instrument} {sig.type}{sig.volume}手 价格: {sig.price}')
                case SignalType.ROLL_CLOSE:
                    param_dict['CombOffsetFlag'] = ApiStruct.OF_Close
                    pos = Trade.objects.filter(broker=self._broker, strategy=self._strategy, code=sig.code, shares=sig.volume, close_time__isnull=True).first()
                    param_dict['Direction'] = ApiStruct.D_Sell if pos.direction == DirectionType.values[DirectionType.LONG] else ApiStruct.D_Buy
                    if pos.open_time.astimezone().date() == timezone.localtime().date() and pos.instrument.exchange == ExchangeType.SHFE:
                        param_dict['CombOffsetFlag'] = ApiStruct.OF_CloseToday  # 上期所区分平今和平昨
                    logger.info(f'{sig.code}->{sig.instrument.main_code} {pos.direction}头换月平旧{sig.volume}手 价格: {sig.price}')
                case SignalType.ROLL_OPEN:
                    param_dict['CombOffsetFlag'] = ApiStruct.OF_Open
                    pos = Trade.objects.filter(
                        Q(close_time__isnull=True) | Q(close_time__date__gte=timezone.localtime().now().date()),
                        broker=self._broker, strategy=self._strategy, code=sig.instrument.last_main, shares=sig.volume).first()
                    param_dict['Direction'] = ApiStruct.D_Buy if pos.direction == DirectionType.values[DirectionType.LONG] else ApiStruct.D_Sell
                    logger.info(f'{pos.code}->{sig.code} {pos.direction}头换月开新{sig.volume}手 价格: {sig.price}')
            self.redis_client.publish(self._request_format.format('ReqOrderInsert'), orjson.dumps(param_dict))
        except Exception as e:
            logger.warning(f'ReqOrderInsert 发生错误: {repr(e)}', exc_info=True)

    async def cancel_order(self, order: dict):
        sub_client = None
        channel_rsp_odr_act, channel_rsp_err = None, None
        try:
            sub_client = self.redis_client.pubsub(ignore_subscribe_messages=True)
            request_id = get_next_id()
            order['RequestID'] = request_id
            channel_rtn_odr = self._trade_response_format.format('OnRtnOrder', order['OrderRef'])
            channel_rsp_odr_act = self._trade_response_format.format('OnRspOrderAction', 0)
            channel_rsp_err = self._trade_response_format.format('OnRspError', request_id)
            await sub_client.psubscribe(channel_rtn_odr, channel_rsp_odr_act, channel_rsp_err)
            task = asyncio.create_task(self.query_reader(sub_client))
            self.redis_client.publish(self._request_format.format('ReqOrderAction'), orjson.dumps(order))
            await asyncio.wait_for(task, HANDLER_TIME_OUT)
            await sub_client.punsubscribe()
            await sub_client.close()
            result = task.result()[0]
            if 'ErrorID' in result:
                err_id = result['ErrorID']
                err_msg = ctp_errors.get(err_id, f"未知错误码:{err_id}")
                logger.warning(f"撤销订单出错: {err_msg}")
                return False
            return True
        except Exception as e:
            logger.warning('cancel_order 发生错误: %s', repr(e), exc_info=True)
            if sub_client and sub_client.in_pubsub and channel_rsp_odr_act:
                await sub_client.unsubscribe()
                await sub_client.close()
            return False

    # ═══════════════════════════════════════════════════════════════════
    # 行情回调
    # ═══════════════════════════════════════════════════════════════════

    @RegisterCallback(channel='MSG:CTP:RSP:MARKET:OnRtnDepthMarketData:*')
    async def OnRtnDepthMarketData(self, channel, tick: dict):
        try:
            inst = channel.split(':')[-1]
            tick['UpdateTime'] = datetime.datetime.strptime(tick['UpdateTime'], "%Y%m%d %H:%M:%S:%f")
            logger.debug('inst=%s, tick: %s', inst, tick)
        except Exception as ee:
            logger.warning('OnRtnDepthMarketData 发生错误: %s', repr(ee), exc_info=True)

    @staticmethod
    def get_trade_string(trade: dict) -> str:
        if trade['OffsetFlag'] == OffsetFlag.Open:
            open_direct = DirectionType.values[trade['Direction']]
        else:
            open_direct = DirectionType.values[DirectionType.LONG] if trade['Direction'] == DirectionType.SHORT else DirectionType.values[DirectionType.SHORT]
        return f"{trade['ExchangeID']}.{trade['InstrumentID']} {OffsetFlag.values[trade['OffsetFlag']]}{open_direct}已成交{trade['Volume']}手 " \
               f"价格:{trade['Price']} 时间:{trade['TradeTime']} 订单号: {trade['OrderRef']}"

    # ═══════════════════════════════════════════════════════════════════
    # 成交 / 订单回调
    # ═══════════════════════════════════════════════════════════════════

    @RegisterCallback(channel='MSG:CTP:RSP:TRADE:OnRtnTrade:*')
    async def OnRtnTrade(self, channel, trade: dict):
        try:
            if len(trade['InstrumentID']) > 6:
                return
            signal = None
            new_trade = False
            trade_completed = False
            order_ref: str = channel.split(':')[-1]
            manual_trade = int(order_ref) < 10000
            if not manual_trade:
                signal = Signal.objects.get(id=int(order_ref[ORDER_REF_SIGNAL_ID_START:]))
            logger.info(f"成交回报: {self.get_trade_string(trade)}")
            inst = Instrument.objects.get(product_code=self._re_extract_code.match(trade['InstrumentID']).group(1))
            order = Order.objects.filter(order_ref=order_ref, code=trade['InstrumentID']).order_by('-send_time').first()
            trade_cost = trade['Volume'] * Decimal(trade['Price']) * inst.fee_money * inst.volume_multiple + trade['Volume'] * inst.fee_volume
            trade_margin = trade['Volume'] * Decimal(trade['Price']) * inst.margin_rate
            now = timezone.localtime()
            trade_time = timezone.make_aware(datetime.datetime.strptime(trade['TradeDate'] + trade['TradeTime'], '%Y%m%d%H:%M:%S'))
            if trade_time.date() > now.date():
                trade_time.replace(year=now.year, month=now.month, day=now.day)
            if trade['OffsetFlag'] == OffsetFlag.Open:  # 开仓
                last_trade = Trade.objects.filter(
                    broker=self._broker, strategy=self._strategy, instrument=inst, code=trade['InstrumentID'], open_time__lte=trade_time,
                    close_time__isnull=True,
                    direction=DirectionType.values[trade['Direction']]).first() if manual_trade else Trade.objects.filter(open_order=order).first()
                if last_trade is None:
                    new_trade = True
                    last_trade = Trade.objects.create(
                        broker=self._broker, strategy=self._strategy, instrument=inst, code=trade['InstrumentID'], open_order=order if order else None,
                        direction=DirectionType.values[trade['Direction']], open_time=trade_time, shares=order.volume if order else trade['Volume'],
                        cost=trade_cost, filled_shares=trade['Volume'], avg_entry_price=trade['Price'], frozen_margin=trade_margin)
                if order is None or order.status == OrderStatus.values[OrderStatus.AllTraded]:
                    trade_completed = True
                if (not new_trade and not manual_trade) or (trade_completed and not new_trade and manual_trade):
                    last_trade.avg_entry_price = (last_trade.avg_entry_price * last_trade.filled_shares + trade['Volume'] * Decimal(trade['Price'])) / \
                                                 (last_trade.filled_shares + trade['Volume'])
                    last_trade.filled_shares += trade['Volume']
                    if trade_completed and not new_trade and manual_trade:
                        last_trade.shares += trade['Volume']
                    last_trade.cost += trade_cost
                    last_trade.frozen_margin += trade_margin
                    last_trade.save()
            else:  # 平仓
                open_direct = DirectionType.values[DirectionType.LONG] if trade['Direction'] == DirectionType.SHORT else DirectionType.values[DirectionType.SHORT]
                last_trade = Trade.objects.filter(Q(closed_shares__isnull=True) | Q(closed_shares__lt=F('shares')), shares=F('filled_shares'),
                                                  broker=self._broker, strategy=self._strategy, instrument=inst, code=trade['InstrumentID'],
                                                  direction=open_direct).first()
                logger.debug(f'trade={last_trade}')
                if last_trade:
                    if last_trade.closed_shares and last_trade.avg_exit_price:
                        last_trade.avg_exit_price = (last_trade.avg_exit_price * last_trade.closed_shares + trade['Volume'] * Decimal(trade['Price'])) / \
                                                    (last_trade.closed_shares + trade['Volume'])
                        last_trade.closed_shares += trade['Volume']
                    else:
                        last_trade.avg_exit_price = trade['Volume'] * Decimal(trade['Price']) / trade['Volume']
                        last_trade.closed_shares = trade['Volume']
                    last_trade.cost += trade_cost
                    last_trade.close_order = order
                    if last_trade.closed_shares == last_trade.shares:  # 全部成交
                        trade_completed = True
                        last_trade.close_time = trade_time
                        if last_trade.direction == DirectionType.values[DirectionType.LONG]:
                            profit_point = last_trade.avg_exit_price - last_trade.avg_entry_price
                        else:
                            profit_point = last_trade.avg_entry_price - last_trade.avg_exit_price
                        last_trade.profit = profit_point * last_trade.shares * inst.volume_multiple
                    last_trade.save(force_update=True)
            logger.debug(f"new_trade:{new_trade} manual_trade:{manual_trade} trade_completed:{trade_completed} "
                         f"order:{order} signal: {signal}")
            if trade_completed and not manual_trade:
                signal.processed = True
                signal.save(update_fields=['processed'])
                order.signal = signal
                order.save(update_fields=['signal'])
        except Exception as ee:
            logger.warning(f'OnRtnTrade 发生错误: {repr(ee)}', exc_info=True)

    @staticmethod
    def save_order(order: dict):
        try:
            if int(order['OrderRef']) < 10000:  # 非本程序生成订单
                return None, None
            signal = Signal.objects.get(id=int(order['OrderRef'][ORDER_REF_SIGNAL_ID_START:]))
            odr, created = Order.objects.update_or_create(
                code=order['InstrumentID'], order_ref=order['OrderRef'], defaults={
                    'broker': signal.strategy.broker, 'strategy': signal.strategy, 'instrument': signal.instrument, 'front': order['FrontID'],
                    'session': order['SessionID'], 'price': order['LimitPrice'], 'volume': order['VolumeTotalOriginal'],
                    'direction': DirectionType.values[order['Direction']], 'status': OrderStatus.values[order['OrderStatus']],
                    'offset_flag': CombOffsetFlag.values[order['CombOffsetFlag']], 'update_time': timezone.localtime(),
                    'send_time': timezone.make_aware(datetime.datetime.strptime(order['InsertDate'] + order['InsertTime'], '%Y%m%d%H:%M:%S'))})
            if order['OrderStatus'] == ApiStruct.OST_Canceled:  # 删除错误订单
                odr.delete()
                return None, None
            now = timezone.localtime().date()
            if created and odr.send_time.date() > timezone.localtime().date():  # 夜盘成交时返回的时间是下一个交易日，需要改成今天
                odr.send_time.replace(year=now.year, month=now.month, day=now.day)
                odr.save(update_fields=['send_time'])
            odr.signal = signal
            return odr, created
        except Exception as ee:
            logger.warning(f'save_order 发生错误: {repr(ee)}', exc_info=True)
            return None, None

    @staticmethod
    def get_order_string(order: dict) -> str:
        off_set_flag = CombOffsetFlag.values[order['CombOffsetFlag']] if order['CombOffsetFlag'] in CombOffsetFlag.values else \
            OffsetFlag.values[order['CombOffsetFlag']]
        order_str = f"订单号:{order['OrderRef']},{order['ExchangeID']}.{order['InstrumentID']} {off_set_flag}{DirectionType.values[order['Direction']]}" \
                    f"{order['VolumeTotalOriginal']}手 价格:{order['LimitPrice']} 报单时间:{order['InsertTime']} " \
                    f"提交状态:{OrderSubmitStatus.values[order['OrderSubmitStatus']]} "
        if order['OrderStatus'] != OrderStatus.Unknown:
            order_str += f"成交状态:{OrderStatus.values[order['OrderStatus']]} 消息:{order['StatusMsg']} "
            if order['OrderStatus'] == OrderStatus.PartTradedQueueing:
                order_str += f"成交数量:{order['VolumeTraded']} 剩余数量:{order['VolumeTotal']}"
        return order_str

    @RegisterCallback(channel='MSG:CTP:RSP:TRADE:OnRtnOrder:*')
    async def OnRtnOrder(self, _: str, order: dict):
        try:
            if len(order['InstrumentID']) > 6:
                return
            if order["OrderSysID"]:
                logger.debug(f"订单回报: {self.get_order_string(order)}")
            order_obj, created = self.save_order(order)
            if not order_obj:
                return
            signal = order_obj.signal
            inst = Instrument.objects.get(product_code=self._re_extract_code.match(order['InstrumentID']).group(1))
            # 处理由于委托价格超出交易所涨跌停板而被撤单的报单，将委托价格下调50%，重新报单
            if order['OrderStatus'] == OrderStatus.Canceled and order['OrderSubmitStatus'] == OrderSubmitStatus.InsertRejected:
                last_bar = DailyBar.objects.filter(exchange=inst.exchange, code=order['InstrumentID']).order_by('-time').first()
                volume = int(order['VolumeTotalOriginal'])
                price = Decimal(order['LimitPrice'])
                if order['CombOffsetFlag'] == CombOffsetFlag.Open:
                    if order['Direction'] == DirectionType.LONG:
                        delta = (price - last_bar.settlement) * Decimal(0.5)
                        price = price_round(last_bar.settlement + delta, inst.price_tick)
                        if delta / last_bar.settlement < 0.01:
                            logger.warning(f"{inst} 新价格: {price} 过低难以成交，放弃报单!")
                            return
                        logger.info(f"{inst} 以价格 {price} 开多{volume}手 重新报单...")
                        signal.price = price
                        self.io_loop.call_soon(self.ReqOrderInsert, signal)
                    else:
                        delta = (last_bar.settlement - price) * Decimal(0.5)
                        price = price_round(last_bar.settlement - delta, inst.price_tick)
                        if delta / last_bar.settlement < 0.01:
                            logger.warning(f"{inst} 新价格: {price} 过低难以成交，放弃报单!")
                            return
                        logger.info(f"{inst} 以价格 {price} 开空{volume}手 重新报单...")
                        signal.price = price
                        self.io_loop.call_soon(self.ReqOrderInsert, signal)
                else:
                    if order['Direction'] == DirectionType.LONG:
                        delta = (price - last_bar.settlement) * Decimal(0.5)
                        price = price_round(last_bar.settlement + delta, inst.price_tick)
                        if delta / last_bar.settlement < 0.01:
                            logger.warning(f"{inst} 新价格: {price} 过低难以成交，放弃报单!")
                            return
                        logger.info(f"{inst} 以价格 {price} 买平{volume}手 重新报单...")
                        signal.price = price
                        self.io_loop.call_soon(self.ReqOrderInsert, signal)
                    else:
                        delta = (last_bar.settlement - price) * Decimal(0.5)
                        price = price_round(last_bar.settlement - delta, inst.price_tick)
                        if delta / last_bar.settlement < 0.01:
                            logger.warning(f"{inst} 新价格: {price} 过低难以成交，放弃报单!")
                            return
                        logger.info(f"{inst} 以价格 {price} 卖平{volume}手 重新报单...")
                        signal.price = price
                        self.io_loop.call_soon(self.ReqOrderInsert, signal)
        except Exception as ee:
            logger.warning(f'OnRtnOrder 发生错误: {repr(ee)}', exc_info=True)

    # ═══════════════════════════════════════════════════════════════════
    # 定时任务
    # ═══════════════════════════════════════════════════════════════════

    @RegisterCallback(crontab='30 8 * * *')
    async def refresh_trading_day(self):
        """每日08:30刷新交易日状态（常驻模式下跨日更新）"""
        today = timezone.localtime()
        _, trading = await is_trading_day(today)
        if trading:
            self._last_trading_day = self._trading_day
            self._trading_day = timezone.make_aware(datetime.datetime.strptime(today.strftime('%Y%m%d') + '08', '%Y%m%d%H'))
            self.redis_client.set('LastTradingDay', self._last_trading_day.strftime('%Y%m%d'))
            self.redis_client.set('TradingDay', today.strftime('%Y%m%d'))
            logger.info(f'交易日刷新: TradingDay={today.strftime("%Y%m%d")}, LastTradingDay={self._last_trading_day.strftime("%Y%m%d")}')
        else:
            logger.info(f'今日{today.strftime("%Y-%m-%d")}是非交易日，程序待机中。')

    async def _has_night_session_tonight(self) -> bool:
        """判断今晚是否有夜盘。明天是交易日则有夜盘；周五时检查下周一。"""
        today = timezone.localtime()
        tomorrow = today + datetime.timedelta(days=1)
        _, trading = await is_trading_day(tomorrow)
        if trading:
            return True
        # 今天是周五，明天周六不是交易日，检查下周一
        if today.isoweekday() == 5:
            monday = today + datetime.timedelta(days=3)
            _, trading = await is_trading_day(monday)
            return trading
        return False

    @RegisterCallback(crontab='*/1 * * * *')
    async def heartbeat(self):
        self.redis_client.set('HEARTBEAT:TRADER', 1, ex=301)
        self.redis_client.set('STATE:CTP_CONNECTED', int(self._trade_connected), ex=301)

    # ═══════════════════════════════════════════════════════════════════
    # CTP 连接状态回调
    # ═══════════════════════════════════════════════════════════════════

    @RegisterCallback(channel='MSG:CTP:RSP:TRADE:OnFrontConnected:*')
    async def on_ctp_connected(self, channel, data: dict):
        self._trade_connected = True
        self.redis_client.set('STATE:CTP_CONNECTED', 1)
        logger.info('CTP 前置已连接')

    @RegisterCallback(channel='MSG:CTP:RSP:TRADE:OnFrontDisconnected:*')
    async def on_ctp_disconnected(self, channel, data: dict):
        self._trade_connected = False
        self.redis_client.set('STATE:CTP_CONNECTED', 0)
        reason = data.get('Reason', '未知') if isinstance(data, dict) else '未知'
        logger.info(f'CTP 前置断开连接，原因: {reason}')

    @RegisterCallback(channel='MSG:CTP:RSP:TRADE:OnRspUserLogin:*')
    async def on_ctp_login(self, channel, data: dict):
        if not isinstance(data, dict):
            return
        error_id = data.get('ErrorID', -1)
        if error_id == 0:
            self._trade_connected = True
            self.redis_client.set('STATE:CTP_CONNECTED', 1)
            trading_day = data.get('TradingDay', '')
            logger.info(f'CTP 登录成功，交易日: {trading_day}')
            # 如果在交易时段，自动执行盘前初始化
            today = timezone.localtime()
            now = int(today.strftime('%H%M'))
            _, trading = await is_trading_day(today)
            night_session = await self._has_night_session_tonight()
            if trading and (850 <= now <= 1550) or night_session and (2050 <= now <= 2359):
                logger.info('登录后自动执行 session_init...')
                await self._session_init()
        else:
            error_msg = data.get('ErrorMsg', '未知错误')
            logger.warning(f'CTP 登录失败，ErrorID={error_id}，{error_msg}')

    # ═══════════════════════════════════════════════════════════════════
    # 盘前初始化定时任务
    # ═══════════════════════════════════════════════════════════════════

    @RegisterCallback(crontab='50 8 * * *')
    async def session_init_day(self):
        _, trading = await is_trading_day(timezone.localtime())
        if trading:
            logger.info('日盘盘前初始化...')
            await self._session_init()

    @RegisterCallback(crontab='15 15 * * *')
    async def session_init_afternoon(self):
        _, trading = await is_trading_day(timezone.localtime())
        if trading:
            logger.info('收盘后刷新账户持仓...')
            await self._session_init()

    @RegisterCallback(crontab='50 20 * * *')
    async def session_init_night(self):
        if await self._has_night_session_tonight():
            logger.info('夜盘盘前初始化...')
            await self._session_init()
        else:
            logger.info('今晚没有夜盘。')

    @RegisterCallback(crontab='55 8 * * *')
    async def processing_signal1(self):
        await asyncio.sleep(5)
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询日盘信号..')
            for sig in Signal.objects.filter(~Q(instrument__exchange=ExchangeType.CFFEX), trigger_time__gte=self._last_trading_day, strategy=self._strategy,
                                             instrument__night_trade=False, processed=False).order_by('-priority'):
                logger.info(f'发现日盘信号: {sig}')
                self.io_loop.call_soon(self.ReqOrderInsert, sig)
            if (self._trading_day - self._last_trading_day).days > 3:
                logger.info(f'假期后第一天，处理节前未成交夜盘信号.')
                self.io_loop.call_soon(asyncio.create_task, self.processing_signal3())

    @RegisterCallback(crontab='1 9 * * *')
    async def check_signal1_processed(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询遗漏的日盘信号..')
            for sig in Signal.objects.filter(~Q(instrument__exchange=ExchangeType.CFFEX), trigger_time__gte=self._last_trading_day, strategy=self._strategy,
                                             instrument__night_trade=False, processed=False).order_by('-priority'):
                logger.info(f'发现遗漏信号: {sig}')
                self.io_loop.call_soon(self.ReqOrderInsert, sig)

    @RegisterCallback(crontab='25 9 * * *')
    async def processing_signal2(self):
        await asyncio.sleep(5)
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询股指和国债信号..')
            for sig in Signal.objects.filter(instrument__exchange=ExchangeType.CFFEX, trigger_time__gte=self._last_trading_day, strategy=self._strategy,
                                             instrument__night_trade=False, processed=False).order_by('-priority'):
                logger.info(f'发现股指和国债信号: {sig}')
                self.io_loop.call_soon(self.ReqOrderInsert, sig)

    @RegisterCallback(crontab='31 9 * * *')
    async def check_signal2_processed(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if trading:
            logger.debug('查询遗漏的股指和国债信号..')
            for sig in Signal.objects.filter(instrument__exchange=ExchangeType.CFFEX, trigger_time__gte=self._last_trading_day, strategy=self._strategy,
                                             instrument__night_trade=False, processed=False).order_by('-priority'):
                logger.info(f'发现遗漏的股指和国债信号: {sig}')
                self.io_loop.call_soon(self.ReqOrderInsert, sig)

    @RegisterCallback(crontab='55 20 * * *')
    async def processing_signal3(self):
        await asyncio.sleep(5)
        if not await self._has_night_session_tonight():
            return
        logger.debug('查询夜盘信号..')
        for sig in Signal.objects.filter(
                trigger_time__gte=self._last_trading_day, strategy=self._strategy, instrument__night_trade=True, processed=False).order_by('-priority'):
            logger.info(f'发现夜盘信号: {sig}')
            self.io_loop.call_soon(self.ReqOrderInsert, sig)

    @RegisterCallback(crontab='1 21 * * *')
    async def check_signal3_processed(self):
        if not await self._has_night_session_tonight():
            return
        logger.debug('查询遗漏的夜盘信号..')
        for sig in Signal.objects.filter(
                trigger_time__gte=self._last_trading_day, strategy=self._strategy, instrument__night_trade=True, processed=False).order_by('-priority'):
            logger.info(f'发现遗漏的夜盘信号: {sig}')
            self.io_loop.call_soon(self.ReqOrderInsert, sig)

    @RegisterCallback(crontab='20 15 * * *')
    async def refresh_all(self):
        day = timezone.localtime()
        _, trading = await is_trading_day(day)
        if not trading:
            logger.info('今日是非交易日, 不更新任何数据。')
            return
        await self.refresh_account()
        await self.refresh_position()
        await self.refresh_instrument()
        logger.debug('全部更新完成!')

    @RegisterCallback(crontab='30 15 * * *')
    async def update_equity(self):
        today, trading = await is_trading_day(timezone.localtime())
        if trading:
            dividend = Performance.objects.filter(
                broker=self._broker, day__lt=today.date()).aggregate(Sum('dividend'))['dividend__sum']
            if dividend is None:
                dividend = Decimal(0)
            dividend = dividend + self._deposit - self._withdraw
            # 虚拟=虚拟(原始)-入金+出金
            self._fake = self._fake - self._deposit + self._withdraw
            if self._fake < 1:
                self._fake = 0
            self._broker.fake = self._fake
            self._broker.save(update_fields=['fake'])
            unit = dividend + self._fake
            nav = (self._current + self._fake) / unit  # 单位净值
            accumulated = self._current / (unit - self._fake)  # 累计净值
            Performance.objects.update_or_create(broker=self._broker, day=today.date(), defaults={
                'used_margin': self._margin, 'dividend': self._deposit - self._withdraw, 'fake': self._fake, 'capital': self._current, 'unit_count': unit,
                'NAV': nav, 'accumulated': accumulated})
            logger.info(f"动态权益: {self._current:,.0f}({self._current / 10000:.1f}万) "
                        f"静态权益: {self._pre_balance:,.0f}({self._pre_balance / 10000:.1f}万) "
                        f"可用资金: {self._cash:,.0f}({self._cash / 10000:.1f}万) "
                        f"保证金占用: {self._margin:,.0f}({self._margin / 10000:.1f}万) "
                        f"虚拟资金: {self._fake:,.0f}({self._fake / 10000:.1f}万) 当日入金: {self._deposit:,.0f} "
                        f"当日出金: {self._withdraw:,.0f} 单位净值: {nav:,.2f} 累计净值: {accumulated:,.2f}")

    @RegisterCallback(crontab='0 17 * * *')
    async def collect_quote(self, tasks=None):
        try:
            day = timezone.localtime()
            _, trading = await is_trading_day(day)
            if not trading:
                logger.info('今日是非交易日, 不计算任何数据。')
                return
            logger.debug(f'{day}盘后计算,获取交易所日线数据..')
            if tasks is None:
                tasks = [update_from_shfe, update_from_dce, update_from_czce, update_from_cffex, update_from_gfex, get_contracts_argument]
            result = await asyncio.gather(*[func(day) for func in tasks], return_exceptions=True)
            if all(result):
                self.io_loop.call_soon(self.calculate, day)
            else:
                failed_tasks = [tasks[i] for i, rst in enumerate(result) if not rst]
                self.io_loop.call_later(10 * 60, asyncio.create_task, self.collect_quote(failed_tasks))
        except Exception as e:
            logger.warning(f'collect_quote 发生错误: {repr(e)}', exc_info=True)
        logger.debug('盘后计算完毕!')

    # ═══════════════════════════════════════════════════════════════════
    # 涨跌停价计算
    # ═══════════════════════════════════════════════════════════════════

    def calc_up_limit(self, inst: Instrument, bar: DailyBar):
        settlement = bar.settlement
        limit_ratio = str_to_number(self.redis_client.get(f"LIMITRATIO:{inst.exchange}:{inst.product_code}:{bar.code}"))
        if limit_ratio is None:
            limit_ratio = float(inst.up_limit_ratio or 0)
        price_tick = inst.price_tick
        price = price_round(settlement * (Decimal(1) + Decimal(limit_ratio)), price_tick)
        return price - price_tick

    def calc_down_limit(self, inst: Instrument, bar: DailyBar):
        settlement = bar.settlement
        limit_ratio = str_to_number(self.redis_client.get(f"LIMITRATIO:{inst.exchange}:{inst.product_code}:{bar.code}"))
        if limit_ratio is None:
            limit_ratio = float(inst.down_limit_ratio or 0)
        price_tick = inst.price_tick
        price = price_round(settlement * (Decimal(1) - Decimal(limit_ratio)), price_tick)
        return price + price_tick

    # ═══════════════════════════════════════════════════════════════════
    # 模板方法: calculate + 可覆盖钩子
    # ═══════════════════════════════════════════════════════════════════

    def get_signal_instruments(self) -> set[str]:
        """返回需要计算信号的品种代码集合。子类可覆盖以扩展品种池。"""
        p_code_set = set(self._inst_ids)
        for code in self._cur_pos.keys():
            p_code_set.add(self._re_extract_code.match(code).group(1))
        return p_code_set

    def get_margin_threshold(self) -> float:
        """返回保证金/权益告警阈值。子类可覆盖。"""
        return 0.8

    def calculate(self, day, create_main_bar=True):
        try:
            p_code_set = self.get_signal_instruments()
            all_margin = 0
            for inst in Instrument.objects.all().order_by('section', 'exchange', 'name'):
                if create_main_bar:
                    logger.debug(f'生成连续合约: {inst.name}')
                    calc_main_inst(inst, day)
                if inst.product_code in p_code_set:
                    logger.debug(f'计算交易信号: {inst.name}')
                    sig, margin = self.calc_signal(inst, day)
                    all_margin += margin
            threshold = self.get_margin_threshold()
            if self._current and self._current > 0 and (all_margin + self._margin) / self._current > threshold:
                logger.info(f"!! 风险提示 !! 开仓保证金共计: {all_margin:.0f}({all_margin / 10000:.1f}万) "
                            f"账户风险度将达到: {100 * (all_margin + self._margin) / self._current:.0f}% "
                            f"超过{threshold * 100:.0f}%阈值!")
        except Exception as e:
            logger.warning(f'calculate 发生错误: {repr(e)}', exc_info=True)

    @abstractmethod
    def calc_signal(self, inst: Instrument, day: datetime.datetime) -> tuple[Any | None, Decimal]:
        """计算交易信号，子类必须实现。返回 (signal_type | None, estimated_margin)"""
        ...
