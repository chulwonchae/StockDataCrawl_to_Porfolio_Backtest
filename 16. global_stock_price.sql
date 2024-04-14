use stock_db;

create table global_price
(
    Date date,
    High double,
    Low double,
    Open double,
    Close double,
    Volume double,
    `Adj Close` double,
    ticker varchar(20),
    primary key(Date, ticker)
);