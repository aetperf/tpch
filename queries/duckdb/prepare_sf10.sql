
create schema tpch;

create table tpch.region as (select * from 'D:\OpenData\TPCH\data\10\region.parquet');
create table tpch.nation as (select * from 'D:\OpenData\TPCH\data\10\nation.parquet');
create table tpch.part as (select * from 'D:\OpenData\TPCH\data\10\part.parquet');
create table tpch.supplier as (select * from 'D:\OpenData\TPCH\data\10\supplier.parquet');
create table tpch.partsupp as (select * from 'D:\OpenData\TPCH\data\10\partsupp.parquet');
create table tpch.orders as (select * from 'D:\OpenData\TPCH\data\10\orders.parquet' order by 1,2);
create table tpch.lineitem as (select * from 'D:\OpenData\TPCH\data\10\lineitem.parquet' order by 1);

create table tpch.lineitem as (select * from 'D:\OpenData\TPCH\data\10\lineitem.parquet' where order by 1);



CREATE SCHEMA parquetviews;

CREATE VIEW parquetviews.partsupp AS SELECT * FROM 'D:\OpenData\TPCH\data\10\partsupp.parquet';
CREATE VIEW parquetviews.part AS SELECT * FROM 'D:\OpenData\TPCH\data\10\part.parquet';
CREATE VIEW parquetviews.supplier AS SELECT * FROM 'D:\OpenData\TPCH\data\10\supplier.parquet';
CREATE VIEW parquetviews.nation AS SELECT * FROM 'D:\OpenData\TPCH\data\10\nation.parquet';
CREATE VIEW parquetviews.region AS SELECT * FROM 'D:\OpenData\TPCH\data\10\region.parquet';
CREATE VIEW parquetviews.lineitem AS SELECT * FROM 'D:\OpenData\TPCH\data\10\lineitem.parquet';
CREATE VIEW parquetviews.orders AS SELECT * FROM 'D:\OpenData\TPCH\data\10\orders.parquet';
CREATE VIEW parquetviews.customer AS SELECT * FROM 'D:\OpenData\TPCH\data\10\customer.parquet';