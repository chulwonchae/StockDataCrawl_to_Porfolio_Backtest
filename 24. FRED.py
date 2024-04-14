# FRED Data through Pandas

import pandas_datareader as web
import pandas as pd
t10y2y = web.DataReader('T10Y2Y', 'fred', start='1990-01-01')
t10y3m = web.DataReader('T10Y3M', 'fred', start='1990-01-01')
# M1, M2
M1 = web.DataReader('M1', 'fred', start='1990-01-01')
M2 = web.DataReader('M2', 'fred', start='1990-01-01')


rate_diff = pd.concat([t10y2y, t10y3m], axis=1)
rate_diff.columns = ['10Y - 2Y', '10Y - 3M']
rate_diff.tail()

import matplotlib.pyplot as plt
import numpy as np
import yfinance as yf

# 주가지수 다운로드
sp = yf.download('^GSPC', start='1990-01-01')

plt.rc('font', family='AppleGothic')
plt.rc('axes', unicode_minus=False)

fig, ax1 = plt.subplots(figsize=(10, 6))

ax1.plot(t10y2y, color = 'black', linewidth = 0.5, label = '10Y-2Y')
ax1.plot(t10y3m, color = 'gray', linewidth = 0.5, label = '10Y-3M')
ax1.axhline(y=0, color='r', linestyle='dashed')
ax1.set_ylabel('장단기 금리차')
ax1.legend(loc = 'lower right')

ax2 = ax1.twinx()
ax2.plot(np.log(sp['Close']), label = 'S&P500')
ax2.set_ylabel('S&P500 지수(로그)')
ax2.legend(loc = 'upper right')

plt.show()