from sqlalchemy import create_engine
import pandas as pd

# http://comp.fnguide.com/
# 개별종목의 재무제표 탭 -> 주소 마지막 티커 뒷부분은 생략해도 괜찮음

engine = create_engine('mysql+pymysql://root:password@localhost:3306/stock_db')
query = """
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
"""
ticker_list = pd.read_sql(query, con=engine)
engine.dispose()

i = 0
ticker = ticker_list['종목코드'][i]

url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{ticker}'
data = pd.read_html(url, displayed_only=False)

[item.head(3) for item in data]
# 총 6개 0 : 포괄손익계산서(연간), 1:포괄손익계산서(분기), 2: 재무상태표(연간), 3: 재무상태표(분기), 4:현금흐름표(연간), 5:현금흐름표(분기)

# print(data[0].columns.tolist(), '\n',
#       data[2].columns.tolist(), '\n',
#       data[4].columns.tolist()
#      )

#포괄손익계산서 테이블에는 '전년동기', '전년동기(%)' 열이 있으며, 이는 필요하지 않은 내용이므로 삭제
data_fs_y = pd.concat([data[0].iloc[:, ~data[0].columns.str.contains('전년동기')], data[2], data[4]])
data_fs_y = data_fs_y.rename(columns={data_fs_y.columns[0]: "계정"})
# 1. 포괄손익계산서 중 '전년동기'라는 글자가 들어간 열을 제외한 데이터를 선택
# 2. `concat()` 함수를 이용해 포괄손익계산서, 재무상태표, 현금흐름표 세개 테이블을 하나로 묶음
# 3. `rename()` 메서드를 통해 첫번째 열 이름(IFRS 혹은 IFRS(연결)을 '계정'으로 변경
#data_fs_y.head()

import requests as rq
from bs4 import BeautifulSoup
import re

page_data = rq.get(url) #원하는 페이지 데이터 불러온 후, 
page_data_html = BeautifulSoup(page_data.content, 'html.parser') # 컨텐츠 부분 뷰티플 스푸 객체

fiscal_data = page_data_html.select('div.corp_group1 > h2') # 첫번째는 종목코드, 두번쨰가 결산 데이터
fiscal_data_text = fiscal_data[1].text
fiscal_data_text = re.findall('[0-9]+', fiscal_data_text)
#n 월 결산 형태로 텍스트 구성, 정규 표현식으로 숫자에 해당부분 만 추출
print(fiscal_data_text)
# 결산월에 해당하는 부분만 선택. 이를 통해 연간 재무제표 해당하는 열만 선택
data_fs_y = data_fs_y.loc[:, (data_fs_y.columns == '계정') |
                          (data_fs_y.columns.str[-2:].isin(fiscal_data_text))]
data_fs_y.head()
# 열 이름이 '계정', 그리고 재무제표의 월이 결산월과 같은부분만 선택.

# 클린징 ; nan항목 삭제 해도 괜찮음
data_fs_y[data_fs_y.loc[:, ~data_fs_y.columns.isin(['계정'])].isna().all(
    axis=1)].head()
data_fs_y['계정'].value_counts(ascending=False).head()

#또한 동일한 계정명이 여러번 반복. 대부분 중요하지 않으니, 하나만 남기고 삭제
def clean_fs(df, ticker, frequency):

    df = df[~df.loc[:, ~df.columns.isin(['계정'])].isna().all(axis=1)]
    df = df.drop_duplicates(['계정'], keep='first')
    df = pd.melt(df, id_vars='계정', var_name='기준일', value_name='값')
    df = df[~pd.isnull(df['값'])]
    df['계정'] = df['계정'].replace({'계산에 참여한 계정 펼치기': ''}, regex=True)
    df['기준일'] = pd.to_datetime(df['기준일']) + pd.tseries.offsets.MonthEnd()
    df['기준일'] = pd.to_datetime(df['기준일'],format='%Y-%m-%d')
    df['종목코드'] = ticker
    df['공시구분'] = frequency

    return df

# 1. 입력값으로는 데이터프레임, 티커, 공시구분(연간/분기)가 필요하다.
# 2. 먼저 연도의 데이터가 NaN인 항목은 제외한다.
# 3. 계정명이 중복되는 경우 `drop_duplicates()` 함수를 이용해 첫번째에 위치하는 데이터만 남긴다.
# 4. `melt()` 함수를 이용해 열로 긴 데이터를 행으로 긴 데이터로 변경한다.
# 5. 계정값이 없는 항목은 제외한다.
# 6. [계산에 참여한 계정 펼치기]라는 글자는 페이지의 [+]에 해당하는 부분이므로 `replace()` 메서드를 통해 제거한다.
# 7. `to_datetime()` 메서드를 통해 기준일을 'yyyy-mm' 형태로 바꾼 후, `MonthEnd()`를 통해 월말에 해당하는 일을 붙인다.
# 8. '종목코드' 열에는 티커를 입력한다.
# 9. '공시구분' 열에는 연간 혹은 분기에 해당하는 값을 입력한다.

data_fs_y_clean = clean_fs(data_fs_y, ticker, 'y')
data_fs_y_clean.head()


# 분기 데이터

data_fs_q = pd.concat(
    [data[1].iloc[:, ~data[1].columns.str.contains('전년동기')], data[3], data[5]])
data_fs_q = data_fs_q.rename(columns={data_fs_q.columns[0]: "계정"})
data_fs_q_clean = clean_fs(data_fs_q, ticker, 'q')

data_fs_q_clean.head()

data_fs_bind = pd.concat([data_fs_y_clean, data_fs_q_clean])