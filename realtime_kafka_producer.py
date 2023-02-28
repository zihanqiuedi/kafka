import time
from json import dumps
import pandas_reader as pdr
import pandas as pd
from kafka import KafkaProducer

url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
tickers = pd.read_csv(url)
sp500_list = tickers['Symbol'].to_list()
sp500_list = sorted(sp500_list)

producer = KafkaProducer(bootstrap_servers=['localhost:9094'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

while True:
    current_price = pdr.get_quote_yahoo(sp500_list[:5])["regularMarketPrice"]
    current_time = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M")
    
    # Store current_price current_time into two DataFrame
    price_df = pd.DataFrame(current_price).rename(columns={ 'regularMarketPrice': 'Prince'})
    time_df = pd.DataFrame({'Time': [current_time]*len(sp500_list[:5])}, index=sp500_list[:5])
    
    # Merge price_df time_df
    result_df = price_df.join(time_df)
    
    # Send data to kafka
    producer.send('sp500', value=result_df.to_dict(orient='records'))

    # 1min wait
    time.sleep(60)
