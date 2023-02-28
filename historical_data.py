import datetime
import time
import yfinance as yf
import pandas as pd
import pytz
import schedule

url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
tickers = pd.read_csv(url)
sp500_list = tickers['Symbol'].to_list()
# valid intervals; 1m, 2m, 5m, 15m, 30m, 66m, 98m, 1h, 1d, 5d, 1wk, 1mo, 3mo
def download_data():
# NewYork Time
    tz = pytz.timezone('America/New_York')
    now = datetime.datetime.now(tz)

    start = now - datetime.timedelta(days=1)
    end = now

    start_str = start.strftime('%Y-%m-%d')
    end_str = end.strftime('%Y-%m-%d')

    data_new = yf.download(tickers=sp500_list, interval='1d', start=start_str, end=end_str)
    print("Data downloaded at", end_str)

download_data()
# 每隔一天执行一次schedule.every(1).day.at("g :90").do(download data)
#后台执行
while True:
    schedule.run_pending()
    time.sleep(1)