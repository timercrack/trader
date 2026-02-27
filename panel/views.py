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
import datetime
import orjson

from django.db.models import Q
from django.views.decorators.cache import cache_page
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse
from django.utils import timezone
from django.views.generic import TemplateView
import logging
import pytz

from panel.models import *

logger = logging.getLogger('panel.view')


class CustomBaseView(LoginRequiredMixin, TemplateView):
    def get_context_data(self, **kwargs):
        context = super(CustomBaseView, self).get_context_data(**kwargs)
        context['cur_stra'] = Strategy.objects.get(id=self.request.GET.get('strategy'))
        context['stra_list'] = Strategy.objects.all()
        return context


class StatusView(CustomBaseView):
    def get_context_data(self, **kwargs):
        context = super(StatusView, self).get_context_data(**kwargs)
        stra = context['cur_stra']
        trades = Trade.objects.filter(strategy=stra, close_time__isnull=True).values_list(
            'frozen_margin', flat=True)
        context['current'] = stra.broker.current
        context['pre_balance'] = stra.broker.pre_balance
        context['margin'] = round(100 * sum(trades) / stra.broker.current, 1)
        context['pos_list'] = Trade.objects.filter(strategy=stra, close_time__isnull=True).order_by('-profit')
        context['open_list'] = Signal.objects.filter(
            Q(type=SignalType.BUY) | Q(type=SignalType.SELL_SHORT), strategy=stra, processed=False)
        context['close_list'] = Signal.objects.filter(
            Q(type=SignalType.BUY_COVER) | Q(type=SignalType.SELL), strategy=stra,
            processed=False).values_list('code', flat=True)
        context['roll_list'] = Signal.objects.filter(
            type=SignalType.ROLL_CLOSE, strategy=stra, processed=False).values_list('code', flat=True)
        return context


class PerformanceView(CustomBaseView):
    pass


class CorrelationView(CustomBaseView):
    def get_context_data(self, **kwargs):
        context = super(CorrelationView, self).get_context_data(**kwargs)
        sections = context['cur_stra'].instruments.order_by('section').values_list('section', flat=True).distinct()
        inst_list = list()
        for sec in sections:
            inst_list.append((SectionType(sec).label, context['cur_stra'].instruments.filter(
                section=sec).order_by('-exchange')))
        context['inst_list'] = inst_list
        context['strategy_inst'] = context['cur_stra'].instruments.values_list('id', flat=True)
        return context


class InstrumentView(CustomBaseView):
    def get_context_data(self, **kwargs):
        context = super(InstrumentView, self).get_context_data(**kwargs)
        exchanges = Instrument.objects.all().values_list('exchange', flat=True).distinct()
        inst_list = dict()
        for ex in exchanges:
            inst_list[ExchangeType(ex).label] = context['cur_stra'].instruments.filter(exchange=ex)
        context['inst_list'] = inst_list
        return context


# @cache_page(3600 * 24)
def nav_data(request):
    q = Performance.objects.filter(broker__strategy__id=request.GET.get('strategy')).order_by(
        '-day').values_list('day', 'NAV')
    rst = []
    for day, val in q:
        rst.append([day.isoformat(), float(val)])
    return JsonResponse(rst, safe=False)


@cache_page(3600 * 24)
def bar_data(request):
    try:
        inst_id = request.GET['inst_id']
        inst = Instrument.objects.get(id=inst_id)
        break_n = Strategy.objects.first().param_set.get(code='BreakPeriod').int_value + 1
        q = MainBar.objects.filter(product_code=inst.product_code).order_by('time').values_list(
            'time', 'open', 'close', 'low', 'high')
        rst = {'up': [], 'down': [], 'k': [], 'x': [], 'trade': [], 'title': str(inst)}
        for day, oo, cc, ll, hh in q:
            rst['x'].append(day.isoformat())
            rst['k'].append([float(oo), float(cc), float(ll), float(hh)])
            rst['up'].append(max(x[2] for x in rst['k'][-break_n:]))
            rst['down'].append(min(x[2] for x in rst['k'][-break_n:]))
        for t in Trade.objects.filter(
                instrument_id=inst_id, strategy_id=request.GET.get('strategy')).order_by('open_time'):
            if t.close_time is None:
                close_price = rst['k'][-1][1]
                close_time = rst['x'][-1]
                if t.profit is None:
                    continue
            else:
                close_price = t.avg_exit_price
                close_time = timezone.localtime(t.close_time, pytz.FixedOffset(480)).date().isoformat()
            rst['trade'].append([
                {
                    'name': '{}至{} {}仓{}手'.format(
                        t.open_time, close_time, t.direction, t.shares),
                    'coord': [timezone.localtime(t.open_time, pytz.FixedOffset(480)).date().isoformat(),
                              t.avg_entry_price],
                    'lineStyle': {
                        'normal': {
                            'color': '#00f' if t.profit > 0 else '#fff'
                        }
                    },
                },
                {
                    'coord': [close_time, close_price]
                }])
        return JsonResponse(rst, safe=False)
    except Exception as e:
        logger.error('bar_data failed: %s', e, exc_info=True)


def calc_corr(year: int, inst_list: list):
    category = list()
    day = datetime.datetime.today()
    price_dict = dict()
    begin_day = day.replace(year=day.year - year)
    for inst in Instrument.objects.filter(id__in=inst_list):
        category.append(inst.name)
        price_dict[inst.product_code] = to_df(MainBar.objects.filter(
            time__gte=begin_day.date(), exchange=inst.exchange,
            product_code=inst.product_code).order_by('time').values_list('time', 'close'))
        price_dict[inst.product_code].index = pd.DatetimeIndex(price_dict[inst.product_code].time)
        price_dict[inst.product_code]['price'] = price_dict[inst.product_code].close.pct_change()
    return category, pd.DataFrame({k: v.price for k, v in price_dict.items()}).corr()


@cache_page(3600 * 24)
def corr_data(request):
    try:
        year = int(request.GET.get('year'))
        insts = orjson.loads(request.GET.get('insts'))
        category, corr_pd = calc_corr(year, insts)
        length = corr_pd.shape[0]
        corr_x = pd.DataFrame([corr_pd.iloc[i, j] for i in range(length) for j in range(i+1, length)])
        return JsonResponse({
            'data': [[category[i], category[j], round(corr_pd.iloc[i, j], 2)]
                     for i in range(length) for j in range(i+1, length)],
            'score': round((((1 - (corr_x.abs() ** 2).mean()[0]) * 100) - 80) * 5, 1),
            'index': category}, safe=False)
    except Exception as e:
        logger.error('corr_data failed: %s', e, exc_info=True)


def status_data(request):
    try:
        stra = Strategy.objects.get(id=request.GET.get('strategy'))
        return JsonResponse({
            'section': [
                Trade.objects.filter(
                    strategy=stra, close_time__isnull=True,
                    instrument__section=SectionType.Stock).count(),
                Trade.objects.filter(
                    strategy=stra, close_time__isnull=True,
                    instrument__section=SectionType.Bond).count(),
                Trade.objects.filter(
                    strategy=stra, close_time__isnull=True,
                    instrument__section=SectionType.Metal).count(),
                Trade.objects.filter(
                    strategy=stra, close_time__isnull=True,
                    instrument__section=SectionType.Agricultural).count(),
                Trade.objects.filter(
                    strategy=stra, close_time__isnull=True,
                    instrument__section=SectionType.EnergyChemical).count(),
                Trade.objects.filter(
                    strategy=stra, close_time__isnull=True,
                    instrument__section=SectionType.BlackMaterial).count()
            ],
            'long': Trade.objects.filter(
                    strategy=stra, close_time__isnull=True, direction=DirectionType.LONG).count(),
            'short': Trade.objects.filter(
                    strategy=stra, close_time__isnull=True, direction=DirectionType.SHORT).count()
        }, safe=False)
    except Exception as e:
        logger.error('status_data failed: %s', e, exc_info=True)
