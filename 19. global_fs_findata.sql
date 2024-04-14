use stock_db;

create table global_fs
(
    ticker varchar(20),        
    date date,
    account varchar(100),
    value double,
    freq varchar(1),
    
    primary key(ticker, date, account, freq)
);