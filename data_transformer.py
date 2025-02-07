import json
import boto3
import random
import datetime
import yfinance as yf
import pandas as pd

kinesis = boto3.client('kinesis', "us-east-1")
tickers = ['FB','SHOP','BYND','NFLX','PINS','SQ','TTD','OKTA','SNAP','DDOG']
df_stocks = yf.download(tickers = tickers, start='2021-11-30', 
                        end='2021-12-01', interval = '5m', group_by = 'ticker') #download from yfinance
df_stocks.reset_index(inplace=True) #make datetime its own column 


def getReferrer(t,i):
    data = {
        'high':round(df_stocks[tickers[t]].High.iloc[i],2), #high
        'low':round(df_stocks[tickers[t]].Low.iloc[i],2), #low
        'ts':df_stocks.Datetime.iloc[i].isoformat(), #ts
        'name': tickers[t] #name
        }
    return data
    

def lambda_handler(event, context):
     # TODO implement
    for t in range(len(tickers)):
        for i in range(len(df_stocks)):
            data = json.dumps(getReferrer(t,i))+"\n"
            print(data)
            out = kinesis.put_record(
                    StreamName="sta9760f2021stream1",
                    Data=data,
                    PartitionKey="partitionkey")
            print(out)
    return {
        'statusCode': 200,
        'body': data
    }
