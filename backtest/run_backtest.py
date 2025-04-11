import backtrader as bt
import pandas as pd
from scripts.uptrend_buy_signal import DailyTrendStrategy
from s3_utils.s3_loader import download_s3_file

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

    df = pd.read_csv(data_path, parse_dates=['datetime'])
    df.set_index('datetime', inplace=True)
    df = df.sort_index()

    data = PandasDataWithDatetime(dataname=df)

    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    cerebro.addstrategy(DailyTrendStrategy)
    cerebro.broker.set_cash(100000)
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.plot(style='candlestick')

if __name__ == "__main__":
    run_backtest()
