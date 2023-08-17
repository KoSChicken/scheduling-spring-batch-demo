create table trade
(
    isin     varchar(20)    null,
    quantity bigint         null,
    price    decimal(10, 2) null,
    customer varchar(40)    null
);
