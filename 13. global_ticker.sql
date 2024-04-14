use stock_db;

create table global_ticker
(
    Name varchar(50) not null,
    Symbol varchar(30),
    Exchange varchar(30),
    Sector varchar(30),
    `Market Cap` varchar(10),
    country varchar(20),    
    date date,
    primary key(Symbol, country, date)
);