from sqlalchemy import create_engine
import pandas as pd
#DB 에서 ticker데이터 불러오기
engine = create_engine('mysql+pymysql://root:password@localhost:3306/stock_db')
query = """ select * from kor_ticker where 기준일 = (select max(기준일) from kor_ticker) and 종목구분 = '보통주'; """
ticker_list = pd.read_sql(query, con=engine)
engine.dispose()
ticker_list.head()


# adj price 
from dateutil.relativedelta import relativedelta
import requests as rq
from io import BytesIO
from datetime import date

i = 0 # 향후 for문을 통해서 i값만 변경하면 모든 종목의 주가를 다운 받을수 있다. 
ticker = ticker_list['종목코드'][i]
fr = (date.today() + relativedelta(years=-5)).strftime("%Y%m%d") #5years back
to = (date.today()).strftime("%Y%m%d")

url = f'''https://fchart.stock.naver.com/siseJson.nhn?symbol={ticker}&requestType=1
&startTime={fr}&endTime={to}&timeframe=day'''

data = rq.get(url).content # bring the 'content' part
data_price = pd.read_csv(BytesIO(data)) # to binary스트림 형태로 만든후 read_csv함수로 데이터 읽어오기
data_price.head() 

import re
# extract only what we need
price = data_price.iloc[:,0:6] # 외국인 소진율, unnamed제외
price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
price = price.dropna() #last row is na
price['날짜'] = price['날짜'].str.extract('(\d+)') # extract only #
price['날짜'] = pd.to_datetime(price['날짜'])
price['종목코드'] = ticker
# price.head()

