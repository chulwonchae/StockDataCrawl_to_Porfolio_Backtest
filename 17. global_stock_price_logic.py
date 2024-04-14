import yfinance as yf
price = yf.download('AAPL', progress = False)
#price = yf.download('AAPL', start = '2000-01-01', progress = False)
# [1 of 1 completed] 부분에 해당하는 진행과정을 출력하고 싶지 않을 시, `progress=False` 인자를 추가

price = yf.download('AAPL', progress = False)
price = yf.download('AAPL', start = '2000-01-01', progress = False)
price.head()

# 일본 국가 코드 T
#price = yf.download("8035.T", progress = False)
#price.head()