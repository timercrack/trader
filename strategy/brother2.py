# coding=utf-8
# pyright: reportAttributeAccessIssue=false, reportOptionalMemberAccess=false, reportOptionalSubscript=false, reportOptionalOperand=false, reportArgumentType=false, reportOperatorIssue=false, reportGeneralTypeIssues=false
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
import datetime
from decimal import Decimal
from typing import Any
import logging
from django.db.models import Sum
from talib import ATR
from strategy.base_strategy import BaseTradeStrategy
from utils import price_round
from panel.models import *

logger = logging.getLogger('CTPApi')


class TradeStrategy(BaseTradeStrategy):
    """大哥2.2 — 经典通道突破趋势跟踪策略"""

    def __init__(self, name: str):
        super().__init__(name)

    def calc_signal(self, inst: Instrument, day: datetime.datetime) -> tuple[Any | None, Decimal]:
        try:
            break_n = self.get_param('BreakPeriod')
            atr_n = self.get_param('AtrPeriod')
            long_n = self.get_param('LongPeriod')
            short_n = self.get_param('ShortPeriod')
            stop_n = self.get_param('StopLoss')
            risk = self.get_param('Risk')
            # 只读取最近400条记录，减少运算量
            df = to_df(MainBar.objects.filter(time__lte=day.date(), exchange=inst.exchange, product_code=inst.product_code).order_by('-time').values_list(
                'time', 'open', 'high', 'low', 'close')[:400], index_col='time', parse_dates=['time'])
            df = df.iloc[::-1]  # 日期升序排列
            df["atr"] = ATR(df.high, df.low, df.close, timeperiod=atr_n)
            df["short_trend"] = df.close
            df["long_trend"] = df.close
            for idx in range(1, df.shape[0]):  # 手动计算SMA
                df.short_trend[idx] = (df.short_trend[idx - 1] * (short_n - 1) + df.close[idx]) / short_n
                df.long_trend[idx] = (df.long_trend[idx - 1] * (long_n - 1) + df.close[idx]) / long_n
            df["high_line"] = df.close.rolling(window=break_n).max()
            df["low_line"] = df.close.rolling(window=break_n).min()
            idx = -1
            buy_sig = df.short_trend[idx] > df.long_trend[idx] and price_round(df.close[idx], inst.price_tick) >= price_round(df.high_line[idx - 1], inst.price_tick)
            sell_sig = df.short_trend[idx] < df.long_trend[idx] and price_round(df.close[idx], inst.price_tick) <= price_round(df.low_line[idx - 1], inst.price_tick)
            pos = Trade.objects.filter(close_time__isnull=True, broker=self._broker, strategy=self._strategy, instrument=inst, shares__gt=0).first()
            roll_over = False
            if pos:
                roll_over = pos.code != inst.main_code and pos.code < inst.main_code
            elif self._strategy.force_opens.filter(id=inst.id).exists() and not buy_sig and not sell_sig:
                logger.info(f'强制开仓: {inst}')
                if df.short_trend[idx] > df.long_trend[idx]:
                    buy_sig = True
                else:
                    sell_sig = True
                self._strategy.force_opens.remove(inst)
            signal = signal_code = price = volume = volume_ori = use_margin = None
            priority = PriorityType.LOW
            if pos:
                # 多头持仓
                if pos.direction == DirectionType.LONG.label:
                    first_pos = pos
                    while hasattr(first_pos, 'open_order') and first_pos.open_order and first_pos.open_order.signal and first_pos.open_order.signal.type == \
                            SignalType.ROLL_OPEN:
                        last_pos = Trade.objects.filter(
                            close_order__signal__type=SignalType.ROLL_CLOSE, instrument=first_pos.instrument, strategy=first_pos.strategy,
                            shares=first_pos.shares, direction=first_pos.direction, close_time__date=first_pos.open_time.date()).first()
                        if last_pos is None:
                            break
                        logger.debug(f"发现换月前持仓:{last_pos} 开仓时间: {last_pos.open_time}")
                        first_pos = last_pos
                    pos_idx = df.index.get_loc(first_pos.open_time.astimezone().date().isoformat())
                    # 多头止损
                    if df.close[idx] <= df.high[pos_idx:idx].max() - df.atr[pos_idx - 1] * stop_n:
                        signal = SignalType.SELL
                        signal_code = pos.code
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        price = self.calc_down_limit(inst, last_bar)
                        priority = PriorityType.High
                    # 多头换月
                    elif roll_over:
                        signal = SignalType.ROLL_OPEN
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        new_bar = DailyBar.objects.filter(exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                        price = self.calc_up_limit(inst, new_bar)
                        priority = PriorityType.Normal
                        Signal.objects.update_or_create(
                            code=pos.code, strategy=self._strategy, instrument=inst, type=SignalType.ROLL_CLOSE, trigger_time=day,
                            defaults={'price': self.calc_down_limit(inst, last_bar), 'volume': volume, 'priority': priority, 'processed': False})
                # 空头持仓
                else:
                    first_pos = pos
                    while hasattr(first_pos, 'open_order') and first_pos.open_order and first_pos.open_order.signal \
                            and first_pos.open_order.signal.type == SignalType.ROLL_OPEN:
                        last_pos = Trade.objects.filter(
                            close_order__signal__type=SignalType.ROLL_CLOSE, instrument=first_pos.instrument, strategy=first_pos.strategy,
                            shares=first_pos.shares, direction=first_pos.direction, close_time__date=first_pos.open_time.date()).first()
                        if last_pos is None:
                            break
                        logger.debug(f"发现换月前持仓:{last_pos} 开仓时间: {last_pos.open_time}")
                        first_pos = last_pos
                    pos_idx = df.index.get_loc(first_pos.open_time.astimezone().date().isoformat())
                    # 空头止损
                    if df.close[idx] >= df.low[pos_idx:idx].min() + df.atr[pos_idx - 1] * stop_n:
                        signal = SignalType.BUY_COVER
                        signal_code = pos.code
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        price = self.calc_up_limit(inst, last_bar)
                        priority = PriorityType.High
                    # 空头换月
                    elif roll_over:
                        signal = SignalType.ROLL_OPEN
                        volume = pos.shares
                        last_bar = DailyBar.objects.filter(exchange=inst.exchange, code=pos.code, time=day.date()).first()
                        new_bar = DailyBar.objects.filter(exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                        price = self.calc_down_limit(inst, new_bar)
                        priority = PriorityType.Normal
                        Signal.objects.update_or_create(
                            code=pos.code, strategy=self._strategy, instrument=inst, type=SignalType.ROLL_CLOSE, trigger_time=day,
                            defaults={'price': self.calc_up_limit(inst, last_bar), 'volume': volume, 'priority': priority, 'processed': False})
            # 开新仓
            elif buy_sig or sell_sig:
                start_cash = Performance.objects.last().unit_count
                # 原始扎堆儿
                profit = Trade.objects.filter(strategy=self._strategy, instrument__section=inst.section).aggregate(sum=Sum('profit'))['sum']
                profit = profit if profit else 0
                risk_each = Decimal(df.atr[idx]) * Decimal(inst.volume_multiple)
                volume_ori = (start_cash + profit) * risk / risk_each
                volume = round(volume_ori)
                print(f"{inst}: ({start_cash:,.0f} + {profit:,.0f}) / {risk_each:,.0f} = {volume_ori}")
                if volume > 0:
                    new_bar = DailyBar.objects.filter(exchange=inst.exchange, code=inst.main_code, time=day.date()).first()
                    if new_bar is None:
                        logger.info(f"未找到新日线数据，{inst.exchange} {inst.main_code} {day.date()}")
                    use_margin = new_bar.settlement * inst.volume_multiple * inst.margin_rate * volume
                    price = self.calc_up_limit(inst, new_bar) if buy_sig else self.calc_down_limit(inst, new_bar)
                    signal = SignalType.BUY if buy_sig else SignalType.SELL_SHORT
                else:
                    logger.info(f"做{'多' if buy_sig else '空'}{inst},单手风险:{risk_each:.0f},超出风控额度，放弃。")
            if signal:
                use_margin = use_margin if use_margin else 0
                sig, _ = Signal.objects.update_or_create(
                    code=signal_code if signal_code else inst.main_code, strategy=self._strategy, instrument=inst, type=signal, trigger_time=day,
                    defaults={'price': price, 'volume': volume, 'priority': priority, 'processed': False})
                volume_ori = volume_ori if volume_ori else volume
                logger.info(f"新信号: {sig}({volume_ori:.1f}手) "
                            f"预估保证金: {use_margin:.0f}({use_margin / 10000:.1f}万)")
                return signal, Decimal(use_margin)
        except Exception as e:
            logger.warning(f'calc_signal 发生错误: {repr(e)}', exc_info=True)
            return None, Decimal(0)
        return None, Decimal(0)
