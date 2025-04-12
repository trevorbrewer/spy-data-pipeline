import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import backtrader as bt
import pandas as pd
from strategy.uptrend_buy_signal import DailyTrendStrategy
from s3_utils.s3_loader import download_s3_file
from datetime import datetime, timedelta

# Filter to the last year of data
one_year_ago = pd.Timestamp.now(tz='UTC') - pd.DateOffset(days=7)


class PandasDataWithDatetime(bt.feeds.PandasData):
    params = (
        ('datetime', 'datetime'),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('openinterest', -1),
    )

def run_backtest():
    data_path = download_s3_file()

    df = pd.read_csv(data_path,dtype={'Unnamed: 0':'str','symbol':'str','open':'float64','high':'float64','low':'float64',
                                      'close':'float64','volume':'int64'},low_memory=False)
    df.rename(columns={ df.columns[0]: "datetime" }, inplace = True)
    df = df.iloc[:, :7]
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])  # drop rows with invalid datetime
    df = df[df['datetime'] >= one_year_ago]
    # df.set_index('datetime', inplace=True)
    # df = df.sort_index()

    data = PandasDataWithDatetime(dataname=df)
    # print(data)

    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    cerebro.addstrategy(DailyTrendStrategy)
    cerebro.broker.set_cash(100000)
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # cerebro.plot(style='candlestick')

if __name__ == "__main__":
    run_backtest()
