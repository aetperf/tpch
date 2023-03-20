create database if not exists "D:\OpenData\TPCH\data\hyper\tpcf_sf10.hyper";

attach database "D:\OpenData\TPCH\data\hyper\tpcf_sf10.hyper" as tpch_sf10;

create table "region" as (select * from 'D:\OpenData\TPCH\data\10\region.parquet');
create table "nation" as (select * from 'D:\OpenData\TPCH\data\10\nation.parquet');
create table "part" as (select * from 'D:\OpenData\TPCH\data\10\part.parquet');
create table "customer" as (select * from 'D:\OpenData\TPCH\data\10\customer.parquet');
create table "supplier" as (select * from 'D:\OpenData\TPCH\data\10\supplier.parquet');
create table "partsupp" as (select * from 'D:\OpenData\TPCH\data\10\partsupp.parquet');
create table "orders" as (select * from 'D:\OpenData\TPCH\data\10\orders.parquet' order by 1,2);
create table "lineitem" as (select * from 'D:\OpenData\TPCH\data\10\lineitem.parquet' order by 1);
