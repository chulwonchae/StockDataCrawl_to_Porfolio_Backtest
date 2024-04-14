# 패키지 불러오기
from sqlalchemy import create_engine
import pandas as pd

# DB 연결
engine = create_engine('mysql+pymysql://root:password@localhost:3306/stock_db')

# 티커 리스트
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker) 
	and 종목구분 = '보통주';
""", con=engine)

# 삼성전자 분기 재무제표
sample_fs = pd.read_sql("""
select * from kor_fs
where 공시구분 = 'q'
and 종목코드 = '005930'
and 계정 in ('당기순이익', '자본', '영업활동으로인한현금흐름', '매출액'); 
""", con=engine)
# 분기, 삼성전자, 계정항목 쟤네들만 불러옴
engine.dispose()



sample_fs = sample_fs.sort_values(['종목코드', '계정', '기준일'])
sample_fs.head()

sample_fs['ttm'] = sample_fs.groupby(
    ['종목코드', '계정'], as_index=False)['값'].rolling(window=4,
                                                 min_periods=4).sum()['값']
sample_fs

# 1. 종목코드와 계정을 기준으로 `groupby()` 함수를 통해 그룹을 묶으며, `as_index=False`를 통해 그룹 라벨을 인덱스로 사용하지 않는다.
# 2. `rolling()` 메서드를 통해 4개 기간씩 합계를 구하며, `min_periods` 인자를 통해 데이터가 최소 4개는 있을 경우에만 값을 구한다. 즉 4개 분기 데이터를 통해 TTM 값을 계산하며, 12개월치 데이터가 없을 경우는 계산을 하지 않는다.

import numpy as np

sample_fs['ttm'] = np.where(sample_fs['계정'] == '자본',
                            sample_fs['ttm'] / 4, sample_fs['ttm'])
sample_fs = sample_fs.groupby(['계정', '종목코드']).tail(1)
sample_fs.head()

# 1. '자본' 항목은 재무상태표에 해당하는 항목이므로 합이 아닌 4로 나누어 평균을 구하며, 타 헝목은 4분기 기준 합을 그대로 사용한다.
# 2. 계정과 종목코드별 그룹을 나누 후 `tail(1)` 함수를 통해 가장 최근 데이터만 선택한다.

#가치지표 중 분모에 해당하는 재무제표 값들을 계산을 했으며, 분자에 해당하는 시가총액은 티커 리스트에서 구할 수 있다.

sample_fs_merge = sample_fs[['계정', '종목코드', 'ttm']].merge(
    ticker_list[['종목코드', '시가총액', '기준일']], on='종목코드')
sample_fs_merge['시가총액'] = sample_fs_merge['시가총액']/100000000
sample_fs_merge.head()
# 위에서 계산한 테이블과 티커 리스트 중 필요한 열만 선택해 테이블을 합친다. 재무제표 데이터의 경우 단위가 억원인 반면 시가총액은 원이므로, 시가총액을 억으로 나눠 단위를 맞춰준다.

sample_fs_merge['value'] = sample_fs_merge['시가총액'] / sample_fs_merge['ttm']
sample_fs_merge['지표'] = np.where(
    sample_fs_merge['계정'] == '매출액', 'PSR',
    np.where(
        sample_fs_merge['계정'] == '영업활동으로인한현금흐름', 'PCR',
        np.where(sample_fs_merge['계정'] == '자본', 'PBR',
                 np.where(sample_fs_merge['계정'] == '당기순이익', 'PER', None))))

sample_fs_merge

#분자(시가총액)를 분모(TTM 기준 재무제표 데이터)로 나누어 가치지표를 계산한 후, 각 지표명을 입력한다.
#마지막으로 배당수익률의 경우 티커리스트의 데이터를 통해 계산할 수 있다.

ticker_list_sample = ticker_list[ticker_list['종목코드'] == '005930'].copy()
ticker_list_sample['DY'] = ticker_list_sample['주당배당금'] / ticker_list_sample['종가']

ticker_list_sample.head()