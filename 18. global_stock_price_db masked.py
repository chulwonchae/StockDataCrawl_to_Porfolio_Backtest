# US_stock price to DB
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import yfinance as yf
import time
from tqdm import tqdm

# connect db
engine = create_engine('mysql+pymysql://root:password@localhost:3306/stock_db')
con = pymysql.connect(user = 'root',
                      passwd = 'password',
                      host = '127.0.0.1',
                      db = 'stock_db',
                      charset='utf8')

mycursor = con.cursor()

# ticker list
ticker_list = pd.read_sql("""select * from global_ticker where date = (select max(date) from global_ticker) and country = 'United States';""", con=engine)

# db upsert
query = """
    insert into global_price (Date, High, Low, Open, Close, Volume, `Adj Close`, ticker)
    values (%s, %s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    High = new.High, Low = new.Low, Open = new.Open, Close = new.Close,
    Volume = new.Volume, `Adj Close` = new.`Adj Close`;
"""

# if error occurs
error_list = []

# stock price
for i in tqdm(range(0, len(ticker_list))):

    # ticker
    ticker = ticker_list['Symbol'][i] # 미국의 경우 티커만 입력하면 되지만, 다른국가는 '티커.국가코드' 형태로

    # if error, 보통 인베스팅이랑 야후랑 리스트가 다르니까 에러 발생
    try:

        # download price
        price = yf.download(ticker, progress=False)

        # data cleaning
        price = price.reset_index()
        price['ticker'] = ticker

        # save to db
        args = price.values.tolist()
        mycursor.executemany(query, args)
        con.commit()

    except:

        # pass error
        print(ticker)
        error_list.append(ticker)

    # time sleep
    time.sleep(2)

engine.dispose()
con.close()

# 1. DB에 연결한다.
# 2. 기준일이 최대, 즉 최근일 기준 보통주에 해당하며, 미국 종목의 리스트(ticker_list)만 불러온다.
# 3. DB에 저장할 쿼리(query)를 입력한다.
# 4. 페이지 오류, 통신 오류 등 오류가 발생한 티커명을 저장할 리스트(error_list)를 만든다.
# 5. for문을 통해 전종목 주가를 다운로드 받으며, 진행상황을 알기위해 `tqdm()` 함수를 이용한다.
# 6. `download()` 함수를 통해 야후 파이낸스에서 주가를 받은 후 클렌징 처리한다. 그 후 주가 데이터를 DB에 저장한다.
# 7. `try except`문을 통해 오류가 발생시 'error_list'에 티커를 저장한다.
# 8. 무한 크롤링을 방지하기 위해 한 번의 루프가 끝날 때마다 타임슬립을 적용한다.
# 9. 모든 작업이 끝나면 DB와의 연결을 종료한다.