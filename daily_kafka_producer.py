from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import yfinance as yf
import pandas as pd
import schedule
import time
import datetime
import pytz

# 创 Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Kafka 集群地址
    value_serializer=lambda m: json.dumps(m).encode('ascii')  # 消息序列化方法
)

def download_data():
    # Newyork time zone
    tz = pytz.timezone('America/New_York')
    now = datetime.datetime.now(tz)

    #start = now - datetime.timedelta(days=2)
    end = now- datetime.timedelta(days=1)

    #start_str = start.strftime('%Y-%m-%d')
    start_str = "2012-01-01"
    end_str = end.strftime('%Y-%m-%d')

    data_new = yf.download(tickers=sp500_list, interval="1d", start=start_str, end=end_str)
    print("Data downloaded at", end_str)

    # 发送数据到 Kafka
    for index, row in data_new.iterrows():
        producer.send('my_topic', value=row.to_dict())

# 第一次执行
download_data()

# execute every day
schedule.every(1).day.at("10:00").do(download_data)

# 后台执行
while True:
    schedule.run_pending()
    time.sleep(1)
