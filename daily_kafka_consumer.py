from kafka import KafkaConsumer
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas_market_calendars as mcal
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, lag, stddev , mean
from pyspark.sql.window import Window

# 创建 Kafka 消费者
consumer = KafkaConsumer(
    'my_topic',  # Consumer topic name
    bootstrap_servers=['localhost:9092'],  # Kafka ip
    auto_offset_reset='earliest',  # consumer from earliest
    enable_auto_commit=True,  # 自动提交消费位移
    value_deserializer=lambda m: json.loads(m.decode('ascii'))  # 消息反序列化方法
)

# Read data from Kafka
data = []
for message in consumer:
    value = message.value
    data.append(value)
    #if len(data) >= 100:
    #    break

# read dataframe
df = pd.DataFrame(data)

spark = SparkSession.builder \
    .appName('MyPySparkApp') \
    .master('spark://master:7077') \
    .config('spark.executor.memory', '4g') \
    .getOrCreate()


# 开盘日历 Get Open date Calendar from NYSE
nyse = mcal.get_calendar('NYSE')

# Start and End time
start_date = pd.to_datetime('2012-01-03', utc=True)
end_date = pd.to_datetime('2022-02-15', utc=True)

# Start and end open date
schedule = nyse.schedule(start_date=start_date, end_date=end_date)

# change data to list
dates = schedule.index.tolist()

# change date to string
date_strings = [date.strftime('%Y-%m-%d') for date in dates]
df['Date'] = date_strings

df = spark.createDataFrame(df)


url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
tickers = pd.read_csv(url)
sp500_list = tickers['Symbol'].to_list()
sp500_list = sorted(sp500_list)

# 配置MySQL连接信息
url = "jdbc:mysql://sh-cynosdbmysql-grp-ce02sdme.sql.tencentcdb.com:20905/eco_data"


properties = {
    "user": "eco",
    "password": "yM7dxU*PzmnWB6p",
    "driver": "com.mysql.jdbc.Driver",
}

for i in range(1,503):
######### i= 1
    subset = df.select(col(df.columns[0]), col(df.columns[i]), col(df.columns[i+503]), col(df.columns[i+503*2]), col(df.columns[i+503*3]), col(df.columns[i+503*4]))
    subset = subset.withColumnRenamed(subset.columns[1], f'{sp500_list[i-1]}_adj_close')
    subset = subset.withColumnRenamed(subset.columns[2], f'{sp500_list[i-1]}_close')
    subset = subset.withColumnRenamed(subset.columns[3], f'{sp500_list[i-1]}_high')
    subset = subset.withColumnRenamed(subset.columns[4], f'{sp500_list[i-1]}_low')
    subset = subset.withColumnRenamed(subset.columns[5], f'{sp500_list[i-1]}_volume')

# subset1 = df.select('Date', sp500_list[i-1]+'_adj_close', sp500_list[i-1]+'_close', sp500_list[i-1]+'_high', sp500_list[i-1]+'_low', sp500_list[i-1]+'_volume')

# subset1 = subset1.withColumnRenamed(subset1.columns[1], f'{sp500_list[i-1]}_adj_close')
# subset1 = subset1.withColumnRenamed(subset1.columns[2], f'{sp500_list[i-1]}_close')
# subset1 = subset1.withColumnRenamed(subset1.columns[3], f'{sp500_list[i-1]}_high')
# subset1 = subset1.withColumnRenamed(subset1.columns[4], f'{sp500_list[i-1]}_low')
# subset1 = subset1.withColumnRenamed(subset1.columns[5], f'{sp500_list[i-1]}_volume')

    result = subset.join(subset1, on="Date")

    # 日涨幅
    subset = subset.withColumn(
        f'{sp500_list[i-1]}_daily_returns',
        (col(f'{sp500_list[i-1]}_close') - lag(col(f'{sp500_list[i-1]}_close')).over(Window.orderBy("date"))) /
        lag(col(f'{sp500_list[i-1]}_close')).over(Window.orderBy("date"))
    )

    # 日收益率
    subset = subset.withColumn(
        f'{sp500_list[i-1]}_daily_pct_change',
        (col(f'{sp500_list[i-1]}_close') - lag(col(f'{sp500_list[i-1]}_close'), 2).over(Window.orderBy("date"))) /
        lag(col(f'{sp500_list[i-1]}_close'), 2).over(Window.orderBy("date"))
    )

    # 周涨幅
    subset = subset.withColumn(
        f'{sp500_list[i-1]}_daily_pct_change',
        (col(f'{sp500_list[i-1]}_close') - lag(col(f'{sp500_list[i-1]}_close'), 5).over(Window.orderBy("date"))) /
        lag(col(f'{sp500_list[i-1]}_close'), 5).over(Window.orderBy("date"))
    )

    # 月涨幅
    subset = subset.withColumn(
        f'{sp500_list[i-1]}_daily_pct_change',
        (col(f'{sp500_list[i-1]}_close') - lag(col(f'{sp500_list[i-1]}_close'), 21).over(Window.orderBy("date"))) /
        lag(col(f'{sp500_list[i-1]}_close'), 21).over(Window.orderBy("date"))
    )

    # 波动率 volatility
    subset = subset.withColumn(f'{sp500_list[i-1]}_volatility', stddev(col(
        f'{sp500_list[i-1]}_daily_returns')).over(Window.orderBy("date").rowsBetween(-20, 0))
    )


    # K线
    window_sizes = [5, 10, 20, 40, 60]

    for window_size in window_sizes:
        window = Window.partitionBy().orderBy('date').rowsBetween(-(window_size - 1), 0)
        subset = subset.withColumn(f'{sp500_list[i-1]}_{window_size}_day_ma', mean(col(f'{sp500_list[i-1]}_close')).over(window))

    table_name = f"feature_{sp500_list[i-1]}"
    # if table exist
    if not spark.read.jdbc(url=url, table=table_name, properties=properties):
        # create table
        spark.read.jdbc(url=url, table=table_name, properties=properties).limit(0).write.jdbc(
            url=url,
            table=table_name,
            mode="overwrite",
            properties=properties
        )

    # append data into table
    df.write.jdbc(
        url=url,
        table=table_name,
        mode="append",
        properties=properties
    )