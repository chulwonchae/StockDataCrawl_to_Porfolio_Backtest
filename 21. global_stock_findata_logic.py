from yahooquery import Ticker
import numpy as np
# yahooquery패키지로 다운
data = Ticker('AAPL')
data.asset_profile # company info
data.summary_detail # valuation,price etc.
data.history().head()

data_y = data.all_financial_data(frequency = 'a') # a : annual
#`data.balance_sheet()` 재무상태표,
# `data.cash_flow()` 현금흐름표, 
# `data.income_statement()` 손익계산서를 각각 받을 수도 있지만, 
# `data.all_financial_data()`을 통해 세 개의 테이블을 한 번에 받을 수도 있다.

data_y.reset_index(inplace = True)
data_y = data_y.loc[:, ~data_y.columns.isin(['periodType', 'currencyCode'])]
data_y = data_y.melt(id_vars = ['symbol', 'asOfDate']) # 세로 긴 형태로
data_y = data_y.replace([np.nan], None)
data_y['freq'] = 'y'
data_y.columns = ['ticker', 'date', 'account', 'value', 'freq']
# data_y.head()

data_q = data.all_financial_data(frequency = 'q')
data_q.reset_index(inplace = True)
data_q = data_q.loc[:, ~data_q.columns.isin(['periodType', 'currencyCode'])]
data_q = data_q.melt(id_vars = ['symbol', 'asOfDate'])
data_q = data_q.replace([np.nan], None)
data_q['freq'] = 'q'
data_q.columns = ['ticker', 'date', 'account', 'value', 'freq']
# data_q.head()


