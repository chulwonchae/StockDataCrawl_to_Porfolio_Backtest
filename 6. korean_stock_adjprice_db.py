
import pymysql
from sqlalchemy import create_engine
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
import requests as rq
import time
from tqdm import tqdm
from io import BytesIO

# connect to DB
engine = create_engine('mysql+pymysql://root:Cjfdnjs1!@localhost:3306/stock_db')
con = pymysql.connect(user = 'root',
                      passwd = 'Cjfdnjs1!',
                      host = '127.0.0.1',
                      db = 'stock_db',
                      charset='utf8')
mycursor = con.cursor()

# ticker list
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
""", con=engine)

# save into DB query (write a query first)
query = """
    insert into kor_price (날짜, 시가, 고가, 저가, 종가, 거래량, 종목코드)
    values (%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    시가 = new.시가, 고가 = new.고가, 저가 = new.저가,
    종가 = new.종가, 거래량 = new.거래량;
"""
# if error shows
error_list = []
# for all ticker, download and save into DB
for i in tqdm(range(0,len(ticker_list))): #tqdm은 진행 상태 확인용
    #ticker
    ticker = ticker_list['종목코드'][i]
    #select date (5 years)
    fr = (date.today() + relativedelta(years=-5)).strftime("%Y%m%d")
    to = (date.today()).strftime("%Y%m%d")
    
    # if error appear, ignore and go to next loop
    try: 
         url = f'''https://fchart.stock.naver.com/siseJson.nhn?symbol={ticker}&requestType=1&startTime={fr}&endTime={to}&timeframe=day'''
         
         #download data
         data = rq.get(url).content
         data_price = pd.read_csv(BytesIO(data))
         
         #data cleaning
         price = data_price.iloc[:,0:6]
         price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
         price = price.dropna()
         price['날짜'] = price['날짜'].str.extract('(\d+)')
         price['날짜'] = pd.to_datetime(price['날짜'])
         price['종목코드'] = ticker
         
         #save to DB
         args = price.values.tolist()
         mycursor.executemany(query,args)
         con.commit()
         
    except:
        # error, save the error in the list
        print(ticker)
        error_list.append(ticker)
        
    time.sleep(2)

engine.dispose()
con.close()