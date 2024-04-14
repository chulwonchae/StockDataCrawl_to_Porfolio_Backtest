from sqlalchemy import create_engine
import pymysql
import pandas as pd
from yahooquery import Ticker
import time
from tqdm import tqdm
import numpy as np

# connect db
engine = create_engine('mysql+pymysql://root:password@localhost:3306/stock_db')
con = pymysql.connect(user = 'root',
                      passwd = 'password',
                      host = '127.0.0.1',
                      db = 'stock_db',
                      charset='utf8')

mycursor = con.cursor()

# ticker list
ticker_list = pd.read_sql("""
select * from global_ticker
where date = (select max(date) from global_ticker)
and country = 'United States';
""", con=engine)

# db upsert
query_fs = """
    insert into global_fs (ticker, date, account, value, freq)
    values (%s,%s,%s,%s,%s) as new
    on duplicate key update
    value = new.value;
"""

# if error occurs
error_list = []

# fs data 
for i in tqdm(range(0, len(ticker_list))):

    # ticker
    ticker = ticker_list['Symbol'][i]

    # if error
    try:

        # download
        data = Ticker(ticker)
        
        # fs_year
        data_y = data.all_financial_data(frequency = 'a')
        data_y.reset_index(inplace = True)
        data_y = data_y.loc[:, ~data_y.columns.isin(['periodType', 'currencyCode'])]
        data_y = data_y.melt(id_vars = ['symbol', 'asOfDate'])
        data_y = data_y.replace([np.nan], None)
        data_y['freq'] = 'y'
        data_y.columns = ['ticker', 'date', 'account', 'value', 'freq']
        
        
        # fs_q
        data_q = data.all_financial_data(frequency = 'q')
        data_q.reset_index(inplace = True)
        data_q = data_q.loc[:, ~data_q.columns.isin(['periodType', 'currencyCode'])]
        data_q = data_q.melt(id_vars = ['symbol', 'asOfDate'])
        data_q = data_q.replace([np.nan], None)
        data_q['freq'] = 'q'
        data_q.columns = ['ticker', 'date', 'account', 'value', 'freq']
        
        # merge
        data_fs = pd.concat([data_y, data_q], axis=0)

        # save to db
        args = data_fs.values.tolist()
        mycursor.executemany(query_fs, args)
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
# 5. for문을 통해 전종목 재무제표를 다운로드 받으며, 진행상황을 알기위해 tqdm() 함수를 이용한다.
# 6. `Ticker()` 함수를 이용해 종목 데이터를 받은 후 연간 및 분기 재무제표를 구한다. 
# 7. 이후, 두 테이블을 concat() 함수를 통해 행으로 묶어준다.
# 8. 재무제표 데이터를 DB에 저장한다.
# 9. 무한 크롤링을 방지하기 위해 한 번의 루프가 끝날 때마다 타임슬립을 적용한다.
# 10. 모든 작업이 끝나면 DB와의 연결을 종료한다.