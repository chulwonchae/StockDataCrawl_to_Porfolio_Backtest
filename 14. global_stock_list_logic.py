from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import math
import pandas as pd

# invseting.com -> markets -> stocks -> stock screener
# 일본 누르니까 국가 country코드가 5에서 35로,  주식 종류에 해당하는 equityType부분이 a에서 ORD로 변경. 

# 우선 크롬드라이버 설정, 미국 보통주 다운
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
url = 'https://www.investing.com/stock-screener/?sp=country::5|sector::a|industry::a|equityType::ORD%3Ceq_market_cap;1'
driver.get(url)


html = BeautifulSoup(driver.page_source, 'lxml') #드리이버페이지 소스를 뷰티블 객체로. 
# 국가 있는부분 스크롤 을 inspection들어가면 클래스 정보 나옴
html.find(class_='js-search-input inputDropDown')['value'] #이건 미국
# 테이블 부분 인스펙션
html_table = html.select('table.genTbl.openTbl.resultsStockScreenerTbl.elpTbl')

df_table = pd.read_html(html_table[0].prettify()) # 뷰티플숩에서 파싱 한 파서 트리를 유니코드 형태로 다시 돌려줌
df_table_result = df_table[0]

df_table_select = df_table[0][['Name', 'Symbol', 'Exchange',  'Sector', 'Market Cap']]
df_table_select.head()

end_num = driver.find_element(By.CLASS_NAME, value = 'js-total-results').text
print(math.ceil(int(end_num) / 50))
driver.quit()