import backtrader as bt
import pandas as pd
import pytz
from datetime import time

class DailyTrendStrategy(bt.Strategy):
    def __init__(self):
        self.onh = None
        self.onl = None
        self.onlbot = None
        self.openmin = None
        self.openbot = None
        self.openbar = None
        self.onbar = None
        self.closebar = None
        self.closepoint2 = None
        self.trendline = None

        self.order = None
        self.stop_order = None
        self.take_order = None
        self.openProfitFocus = None
        self.openOneCandle = None
        self.trade_open_price = None

    def next(self):
        dt = self.datas[0].datetime.datetime(0)
        eastern = pytz.timezone('US/Eastern')
        dt_est = dt.astimezone(eastern)
        current_time = dt_est.time()

        inside_session = (time(18, 0) <= current_time or current_time < time(9, 30))
        trading_session = time(9, 30) <= current_time < time(15, 45)

        if inside_session:
            if self.onh is None:
                self.onh = self.data.high[0]
                self.onl = self.data.low[0]
                self.onlbot = min(self.data.open[0], self.data.close[0])
                self.onbar = len(self)
            else:
                self.onh = max(self.onh, self.data.high[0])
                if self.data.low[0] < self.onl:
                    self.onl = self.data.low[0]
                    self.onbar = len(self)
                    self.onlbot = min(self.data.open[0], self.data.close[0])

        if time(9, 15) <= current_time < time(9, 30):
            self.openmin = self.data.low[0]
            self.openbot = min(self.data.open[0], self.data.close[0])
            self.openbar = len(self)

        if time(15, 45) <= current_time <= time(16, 0) and self.openbar and self.onbar:
            self.closebar = len(self)
            try:
                self.closepoint2 = self.openmin + (((self.openmin + self.openbot)/2 - (self.onl + self.onlbot)/2) / (self.openbar - self.onbar)) * (self.closebar - self.openbar)
            except ZeroDivisionError:
                self.closepoint2 = self.onl

        if self.openbar and len(self) >= self.openbar:
            # Prevent division by zero if openbar and onbar are the same
            try:
                self.trendline = self.openmin + (((self.openmin + self.openbot)/2 - (self.onl + self.onlbot)/2) / (self.openbar - self.onbar)) * (len(self) - self.openbar)
            except ZeroDivisionError:
                self.trendline = self.onl

        # Buy Conditions
        if trading_session and self.trendline:
            price = self.data.close[0]
            open_ = self.data.open[0]
            close = self.data.close[0]
            high = self.data.high[0]
            low = self.data.low[0]

            cond1 = low < self.trendline and open_ < close and open_ > self.trendline
            cond2 = open_ < self.trendline and close > self.trendline and (close - open_) > 1 and open_ + (close - open_) * 0.6 > self.trendline
            trendBuyCondition = cond1 or cond2

            if trendBuyCondition and not self.position:
                self.trade_open_price = close
                stop_price = close - 20
                take_profit_price = close + 40
                self.openProfitFocus = close + 35
                self.openOneCandle = len(self) + 1

                self.order = self.buy()
                self.stop_order = self.sell(exectype=bt.Order.Stop, price=stop_price)
                self.take_order = self.sell(exectype=bt.Order.Limit, price=take_profit_price)

        # Exits
        if self.position:
            if self.data.close[0] >= self.openProfitFocus:
                self.close()
            elif len(self) == self.openOneCandle and self.data.close[0] - self.data.close[-1] < -4:
                self.close()
            elif len(self) > self.openOneCandle and self.data.close[0] - self.data.close[-1] < -4:
                self.close()
            elif not trading_session:
                self.close()

# print("✅ Module loaded!")
# print(DailyTrendStrategy)