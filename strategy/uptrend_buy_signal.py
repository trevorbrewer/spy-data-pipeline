import backtrader as bt
import pandas as pd
import pytz
from datetime import time
import csv

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

        self.dataclose = {data: data.close for data in self.datas}
        self.order = None
        self.trades = []  # To store trade logs
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
            # price = self.data.close[0]
            open_ = self.data.open[0]
            close = self.data.close[0]
            # high = self.data.high[0]
            low = self.data.low[0]

            cond1 = low < self.trendline and open_ < close and open_ > self.trendline
            cond2 = open_ < self.trendline and close > self.trendline and (close - open_) > 1 and open_ + (close - open_) * 0.6 > self.trendline
            trendBuyCondition = cond1 or cond2

            if trendBuyCondition and not self.position:
                self.trade_open_price = close
                stop_price = close*0.997
                take_profit_price = close*1.01
                self.openProfitFocus = close*1.01
                self.openOneCandle = len(self) + 1

                self.order = self.buy(size=100)
                self.stop_order = self.sell(exectype=bt.Order.Stop, price=stop_price,size=100)
                self.take_order = self.sell(exectype=bt.Order.Limit, price=take_profit_price,size=100)

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

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        data = order.data
        symbol = data._name

        if order.status in [order.Completed]:
            if order.isbuy():
                # self.log(f"BUY EXECUTED: {symbol} @ {order.executed.price:.2f}")
                self.order_type = "BUY"
            else:
                # self.log(f"SELL EXECUTED: {symbol} @ {order.executed.price:.2f}")
                self.order_type = "SELL"

            self.trades.append({
                'datetime': self.data.datetime.datetime().strftime("%Y-%m-%d %H:%M:%S"),
                'symbol': symbol,
                'type': self.order_type,
                'price': order.executed.price,
                'size': order.executed.size,
                'value': order.executed.value,
                'commission': order.executed.comm,
                'pnl': 0
            })

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f"ORDER FAILED for {symbol}")

        self.order = None


    def notify_trade(self, trade):
        if trade.isclosed:
            symbol = trade.data._name
            # self.log(f"TRADE CLOSED: {symbol} Profit: {trade.pnl:.2f}")
            self.trades.append({
                'datetime': self.data.datetime.datetime().strftime("%Y-%m-%d %H:%M:%S"),
                'symbol': symbol,
                'type': 'CLOSE',
                'price': trade.price,
                'size': trade.size,
                'commission': 0,
                'pnl': trade.pnl
            })


    def log(self, txt):
        dt = self.datas[0].datetime.datetime(0)
        # print(f'{dt.isoformat()}, {txt}')


    def stop(self):
        with open('data/trade_report.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.trades[0].keys())
            writer.writeheader()
            writer.writerows(self.trades)

# print("✅ Module loaded!")
# print(DailyTrendStrategy)