from datetime import timedelta
import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'DB'))
from DB import DBUpdater 
from DB import MarketDB
import pymysql
import pandas as pd
from datetime import datetime
import matplotlib.dates as mdates
from slacker import Slacker
# from mpl_finance import candlestick_ohlc
import matplotlib.pyplot as plt

dbu = DBUpdater.DBUpdater()
# dbu.execute_daily()
mk = MarketDB.MarketDB()

stocks =mk.get_comp_info()

slack = Slacker('!!자신의 슬랙 API 토큰!!')

checkList = []  # 매수 종목 체크 
idx = 1

for code in mk.codes:
    df = mk.get_daily_price(code, (datetime.today() - timedelta(100)).strftime('%Y-%m-%d') , (datetime.today()).strftime('%Y-%m-%d'))

    line60 = df.close.ewm(span = 60).mean()
    line130 = df.close.ewm(span = 130).mean()
    macd = line60 - line130
    signal = macd.ewm(span = 45).mean()
    macdhist = macd - signal

    df = df.assign(line60 = line60 , line130 = line130 , macd = macd ,signal = signal , macdhist= macdhist).dropna()
    df['number'] = df.index.map(mdates.date2num) # 날짜  인덱스를 숫자형으로 변환
    ohlc = df[['number' , 'open' , 'high', 'low', 'close']]

    ndays_high = df.high.rolling(window = 14, min_periods = 1).max()
    ndays_low = df.low.rolling(window = 14, min_periods = 1).min()
    fast_k  = (df.close - ndays_low) / (ndays_high - ndays_low) * 100
    slow_d = fast_k.rolling(window = 3).mean()
    df = df.assign(fast_k = fast_k , slow_d = slow_d).dropna()

    
    for i in range(1 , len(df.close)):
        if df.line130.values[i - 1] < df.line130.values[i] and \
            df.slow_d.values[i - 1] >= 20 and df.slow_d.values[i] < 20 and datetime.today().strftime('%Y-%m-%d') == df['date'][i].strftime('%Y-%m-%d'):
            checkList.append(code)
            attach_dict = {'color' : '#ff0000' ,
                        'author_name' : mk.codes[code] , 
                        'auther_link' : 'https://finance.naver.com/item/main.nhn?code={}'.format(code),
                        'text' : "{} 종가 {}원 Slow 스토캐스틱 {:.2f} % 매수 급소 포착!!!".format(df.date[i] , df.close[i] , df.slow_d[i]),
                        'image_url' : "https://ssl.pstatic.net/imgfinance/chart/item/area/day/{}.png?sidcode=1613875384265".format(code)
                        }

            attach_list = [attach_dict]   
            slack.chat.post_message('#stock' ,attachments = attach_list)

print("매수점 포착 종목 리스트 : " )
print(checkList)

