# 크롤링 순서 : html정보 받기 -> 속성/태그 -> 클렌징
import requests as rq
from bs4 import BeautifulSoup
# GET/POST method
# date
url = "https://finance.naver.com/sise/sise_deposit.naver"
data = rq.get(url) # resp.200 works / 해당 페이지 내용 가져오기 겟방식
data_html = BeautifulSoup(data.content) #data.content  : html -> beautiful soup 객체로 생성 / 개발자 도구에 나왔던 글씨들 
# select_one메서드를 통해 해당 태그의 데이터 추출 (날짜가 여러개 나오니가 )
parse_day = data_html.select_one('div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text # find where the date is/ text part only
#클래스가 subtop_sise_graph2 인 div태크-> 클래스가 subtop_chart_note인 ul태그 -> li태크 -> 클래스가 tah인 span태그 에서 텍스트 정보만 
parse_day # 날짜 추출

import re

biz_day = re.findall('[0-9]+', parse_day) # extract only number / '[0-9]+'는 모든숫자 의미 하는 표현식 
biz_day = ''.join(biz_day)
biz_day #YYYYMMDD

# korean stock ticker from data.krx.co.kr
# 한국거래소의 업종분류 현황 및 개별 지표 크롤링 
# POST방식은 url과 쿼리 필요
import requests as rq
from io import BytesIO
import pandas as pd
# 해당데이터들은 크롤링이 아니라 엑셀 버튼 클릭, 파일로 받을수 있으나, 크롤링으로 바로 파이썬에서 부를수 있음
# KRX사이트 -> 기본통계 -> 주식 -> 세부안내 -> 업종분류현황 에서 csv다운 버튼 개발자도구
# [12025] 업종분류 현황
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd' # generage.cmd -> request url
gen_otp_stk = { # payload
    'mktId': 'STK', # 코스닥은 KSQ
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
}
# 'headers 부분의 referer없으면 로봇으로 인식
headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text #receive OTP 
otp_stk
# need otp to download the data
# 결국 위에는  otp를 url에 보내려고 
down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd' # download.cmd -> request url
down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR') # binary로 
sector_stk.head()

## kosdaq

gen_otp_ksq = {
    'mktId': 'KSQ',  
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
}
otp_ksq = rq.post(gen_otp_url, gen_otp_ksq, headers=headers).text
otp_ksq

down_sector_ksq = rq.post(down_url, {'code': otp_ksq}, headers=headers)
sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding='EUC-KR')
sector_ksq.head()

## kospi + kosdaq 합치기
krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop=True)
krx_sector['종목명'] = krx_sector['종목명'].str.strip() #공백 있는 종목명들 공백 제거
krx_sector['기준일'] =  biz_day
krx_sector.head()

## individual stock ratio [12021] PER/PBR/배당수익률(개별종목)
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_data = {
    'searchType': '1',
    'mktId': 'ALL',
    'trdDd': biz_day,
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
}
headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
otp = rq.post(gen_otp_url, gen_otp_data, headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
krx_ind = rq.post(down_url, {'code': otp}, headers=headers)

krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR')
krx_ind['종목명'] = krx_ind['종목명'].str.strip()
krx_ind['기준일'] = biz_day

krx_ind.head()



# data cleaning
# only exist in one side (업종분류vs지표; 공통으로 존재하지 않는 종목) 
diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명'])))
diff
kor_ticker = pd.merge(krx_sector,
                      krx_ind,
                      on=krx_sector.columns.intersection( # 공통존재
                          krx_ind.columns).tolist(),
                      how='outer')

# spec
kor_ticker[kor_ticker['종목명'].str.contains('스펙|[0-9]+호')]['종목명']

# preferred stock
kor_ticker[kor_ticker['종목코드'].str[-1] != '0']['종목명']

# Reit
kor_ticker[kor_ticker['종목명'].str.endswith('리츠')]['종목명']

import numpy as np
kor_ticker['종목구분'] = np.where(kor_ticker['종목명'].str.contains('스팩|제[0-9]+호'), '스팩',\
                       np.where(kor_ticker['종목코드'].str[-1:] != '0', '우선주',\
                       np.where(kor_ticker['종목명'].str.endswith('리츠'), '리츠',\
                       np.where(kor_ticker['종목명'].isin(diff),  '기타','보통주'))))
kor_ticker = kor_ticker.reset_index(drop=True)
kor_ticker.columns = kor_ticker.columns.str.replace(' ', '')
kor_ticker = kor_ticker[['종목코드', '종목명', '시장구분', '종가',\
                         '시가총액', '기준일', 'EPS', '선행EPS', 'BPS', '주당배당금', '종목구분']]
kor_ticker = kor_ticker.replace({np.nan: None}) #nan can not be saved in SQL; change into none
kor_ticker['기준일'] = pd.to_datetime(kor_ticker['기준일'])


# upsert to db
import pymysql
con = pymysql.connect(user = 'root',
                      passwd = 'password',
                      host = '127.0.0.1',
                      db = 'stock_db',
                      charset='utf8')

mycursor = con.cursor() #cursor ent.
query = f"""
    insert into kor_ticker (종목코드,종목명,시장구분,종가,시가총액,기준일,EPS,선행EPS,BPS,주당배당금,종목구분)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update
    종목명=new.종목명,시장구분=new.시장구분,종가=new.종가,시가총액=new.시가총액,EPS=new.EPS,선행EPS=new.선행EPS,
    BPS=new.BPS,주당배당금=new.주당배당금,종목구분 = new.종목구분;
"""
#밑에 두줄이 upsert; key를 제외한 업데이트 할 두 컬럼 / primary key(종목코드, 기준일)
# on duplicate key update (프라이머리 키가 중복 되면 업데이트 해줘라)
# 다음줄은 primary키를 빼고 나머지 열들 

# update 'new' except primary keys.  
args = kor_ticker.values.tolist() #to list
mycursor.executemany(query,args) # send query to SQL db
con.commit()
con.close()

# sector from wise_index (source : FNguide)
import json
import requests as rq
import pandas as pd
# [WISE SECTOR INDEX → WICS → 에너지]
url = f'''https://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={biz_day}&sec_cd=G10'''
data = rq.get(url).json()
#data.keys() #['info', 'list', 'sector', 'size']
data['list'] # sector - stock
data['sector'] # sector code
data['size']
 
data['list'][0]

data_pd = pd.json_normalize(data['list'])

import time
import json
import requests as rq
import pandas as pd
from tqdm import tqdm
data_sector = []

sector_code = [
    'G25', 'G35', 'G50', 'G40', 'G10', 'G20', 'G55', 'G30', 'G15', 'G45'
]


for i in tqdm(sector_code):
    url = f'''http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={biz_day}&sec_cd={i}'''    
    data = rq.get(url).json()
    data_pd = pd.json_normalize(data['list'])

    data_sector.append(data_pd)

    time.sleep(2)

kor_sector = pd.concat(data_sector, axis = 0)
kor_sector = kor_sector[['IDX_CD', 'CMP_CD', 'CMP_KOR', 'SEC_NM_KOR']] # what we need
kor_sector['기준일'] = biz_day
kor_sector['기준일'] = pd.to_datetime(kor_sector['기준일'])

# sector -> SQL DB
con = pymysql.connect(user = 'root',
                      passwd = 'password',
                      host = '127.0.0.1',
                      db = 'stock_db',
                      charset='utf8')

mycursor = con.cursor()
query = f"""
    insert into kor_sector (IDX_CD, CMP_CD, CMP_KOR, SEC_NM_KOR, 기준일)
    values (%s,%s,%s,%s,%s) as new
    on duplicate key update
    IDX_CD = new.IDX_CD, CMP_KOR = new.CMP_KOR, SEC_NM_KOR = new.SEC_NM_KOR
"""

args = kor_sector.values.tolist()

mycursor.executemany(query, args)
con.commit()
con.close()