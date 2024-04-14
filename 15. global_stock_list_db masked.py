from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from datetime import datetime
import math
import pandas as pd
import numpy as np
from tqdm import tqdm
import time

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
nationcode = '5'
url = f'''https://investing.com/stock-screener/?sp=country::
{nationcode}|sector::a|industry::a|equityType::ORD%3Ceq_market_cap;1'''
driver.get(url)

WebDriverWait(driver, 10).until(EC.visibility_of_element_located(
    (By.XPATH, '//*[@id="resultsTable"]/tbody')))
# 'Screener Results'에 해당하는 부분은 종목이 들어있는 테이블이 로딩된 이후 나타난다. 따라서 `WebDriverWait()` 함수를 통해 해당 테이블이 로딩될 때까지 기다리며, 테이블의 XPATH는 '//*[@id="resultsTable"]/tbody' 이다.
# 종목수로 페이지수 계산
end_num = driver.find_element(By.CLASS_NAME, value='js-total-results').text
end_num = math.ceil(int(end_num) / 50)


all_data_df = []

for i in tqdm(range(1, end_num + 1)):

    url = f'''https://investing.com/stock-screener/?sp=country::
        {nationcode}|sector::a|industry::a|equityType::ORD%3Ceq_market_cap;{i}'''
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located(
            (By.XPATH, '//*[@id="resultsTable"]/tbody')))
    except:
        time.sleep(1)
        driver.refresh()
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located(
            (By.XPATH, '//*[@id="resultsTable"]/tbody')))

    html = BeautifulSoup(driver.page_source, 'lxml')

    html_table = html.select(
        'table.genTbl.openTbl.resultsStockScreenerTbl.elpTbl')
    df_table = pd.read_html(html_table[0].prettify())
    df_table_select = df_table[0][['Name', 'Symbol',
                                   'Exchange',  'Sector', 'Market Cap']]

    all_data_df.append(df_table_select)

    time.sleep(2)

all_data_df_bind = pd.concat(all_data_df, axis=0)

data_country = html.find(class_='js-search-input inputDropDown')['value']
all_data_df_bind['country'] = data_country
all_data_df_bind['date'] = datetime.today().strftime('%Y-%m-%d')
all_data_df_bind = all_data_df_bind[~all_data_df_bind['Name'].isnull()]
all_data_df_bind = all_data_df_bind[all_data_df_bind['Exchange'].isin(
    ['NASDAQ', 'NYSE', 'NYSE Amex'])]
all_data_df_bind = all_data_df_bind.drop_duplicates(['Symbol'])
all_data_df_bind.reset_index(inplace=True, drop=True)
all_data_df_bind = all_data_df_bind.replace({np.nan: None})

driver.quit()

# 1. 빈 리스트(all_data_df)를 생성한다.
# 2. for문을 통해 전체 페이지에서 종목명과 티커 등의 정보를 크롤링한다.
# 3. f-string을 통해 각 페이지에 해당하는 URL을 생성한 후 페이지를 연다.
# 4. `WebDriverWait()` 함수를 통해 테이블이 로딩될때 까지 기다린다. 또한 간혹 페이지 오류가 발생할 때가 있으므로, try except문을 이용해 오류 발생 시 1초간 기다린 후 `refresh()`를 통해 새로고침을 하여 다시 테이블이 로딩되길 기다린다.
# 5. HTML 정보를 불러온 후, 테이블에 해당하는 부분을 선택한다.
# 6. 원하는 열만 선택한다.
# 7. `append()` 메서드를 통해 해당 테이블을 리스트에 추가한다.
# 8. 2초가 일시정지를 한다.
# 9. for문이 끝나면 `concat()` 함수를 통해 리스트 내 모든 데이터프레임을 행으로 묶어준다.
# 10. 국가명에 해당하는 부분을 추출한 뒤, 'country' 열에 입력한다.
# 11. 'date' 열에 오늘 날짜를 입력한다.
# 12. 일부 종목의 경우 종목명이 빈칸으로 들어오므로 이를 제거한다.
# 13. 'Exchange' 열에서 거래가 가능한 거래소만 선택한다.
# 14. 일부 종목의 경우 중복된 결과가 들어오기도 하므로 `drop_duplicates()` 메서드를 통해 Symbol이 겹치는 경우 한개만 남겨준다.
# 14. `reset_index()` 메서드를 통해 인덱스를 초기화한다.
# 15. nan을 None으로 변경한다.
# 16. 드라이브롤 종료한다.

import pymysql

con = pymysql.connect(user = 'root',
                      passwd = 'password',
                      host = '127.0.0.1',
                      db = 'stock_db',
                      charset='utf8')

mycursor = con.cursor()
query = """
    insert into global_ticker (Name, Symbol, Exchange, Sector, `Market Cap`, country, date)
    values (%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    name=new.name,Exchange=new.Exchange,Sector=new.Sector,
    `Market Cap`=new.`Market Cap`; 
"""

args = all_data_df_bind.values.tolist()

mycursor.executemany(query, args)
con.commit()

con.close()