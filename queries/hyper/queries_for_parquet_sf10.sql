CREATE DATABASE IF NOT exists "D:\OpenData\TPCH\data\hyper\tpcf_sf10_parquet.hyper";

attach database "D:\OpenData\TPCH\data\hyper\tpcf_sf10_parquet.hyper" as tpch_sf10_parquet;


create temporary external table "REGION" for 'D:\OpenData\TPCH\data\10\region.parquet';
create temporary external table "NATION" for 'D:\OpenData\TPCH\data\10\nation.parquet';
create temporary external table "PART" for 'D:\OpenData\TPCH\data\10\part.parquet';
create temporary external table "CUSTOMER" for 'D:\OpenData\TPCH\data\10\customer.parquet';
create temporary external table "SUPPLIER" for 'D:\OpenData\TPCH\data\10\supplier.parquet';
create temporary external table "PARTSUPP" for 'D:\OpenData\TPCH\data\10\partsupp.parquet';
create temporary external table "ORDERS" for 'D:\OpenData\TPCH\data\10\orders.parquet';
create temporary external table "LINEITEM" for 'D:\OpenData\TPCH\data\10\LINEITEM.parquet';



SELECT
    --QUERY01
    "L_RETURNFLAG",
    "L_LINESTATUS",
    SUM("L_QUANTITY") AS SUM_QTY,
    SUM("L_EXTENDEDPRICE") AS SUM_BASE_PRICE,
    SUM("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) AS SUM_DISC_PRICE,
    SUM("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT") * (1 + "L_TAX")) AS SUM_CHARGE,
    AVG("L_QUANTITY") AS AVG_QTY,
    AVG("L_EXTENDEDPRICE") AS AVG_PRICE,
    AVG("L_DISCOUNT") AS AVG_DISC,
    COUNT(*) AS COUNT_ORDER
FROM
    "LINEITEM"
WHERE
    "l_shipdate" <= CAST('1998-09-02' AS DATE)
GROUP BY
    "L_RETURNFLAG",
    "L_LINESTATUS"
ORDER BY
    "L_RETURNFLAG",
    "L_LINESTATUS";


SELECT
    --QUERY02	
    "S_ACCTBAL",
    "S_NAME",
    "N_NAME",
    "P_PARTKEY",
    "P_MFGR",
    "S_ADDRESS",
    "S_PHONE",
    "S_COMMENT"
FROM
    "PART",
    "SUPPLIER",
    "PARTSUPP",
    "NATION",
    "REGION"
WHERE
    "P_PARTKEY" = "PS_PARTKEY"
    AND "S_SUPPKEY" = "PS_SUPPKEY"
    AND "P_SIZE" = 15
    AND "P_TYPE" LIKE '%BRASS'
    AND "S_NATIONKEY" = "N_NATIONKEY"
    AND "N_REGIONKEY" = "R_REGIONKEY"
    AND "R_NAME" = 'EUROPE'
    AND "PS_SUPPLYCOST" = (
        SELECT
            MIN("PS_SUPPLYCOST")
        FROM
            "PARTSUPP",
            "SUPPLIER",
            "NATION",
            "REGION"
        WHERE
            "P_PARTKEY" = "PS_PARTKEY"
            AND "S_SUPPKEY" = "PS_SUPPKEY"
            AND "S_NATIONKEY" = "N_NATIONKEY"
            AND "N_REGIONKEY" = "R_REGIONKEY"
            AND "R_NAME" = 'EUROPE'
    )
ORDER BY
    "S_ACCTBAL" DESC,
    "N_NAME",
    "S_NAME",
    "P_PARTKEY"
LIMIT 100;



SELECT
    --QUERY03
    "L_ORDERKEY",
    SUM("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) AS REVENUE,
    "o_orderdate",
    "O_SHIPPRIORITY"
FROM
    "CUSTOMER",
    "ORDERS",
    "LINEITEM"
WHERE
    "C_MKTSEGMENT" = 'BUILDING'
    AND "C_CUSTKEY" = "O_CUSTKEY"
    AND "L_ORDERKEY" = "O_ORDERKEY"
    AND "o_orderdate" < CAST('1995-03-15' AS DATE)
    AND "l_shipdate" > CAST('1995-03-15' AS DATE)
GROUP BY
    "L_ORDERKEY",
    "o_orderdate",
    "O_SHIPPRIORITY"
ORDER BY
    REVENUE DESC,
    "o_orderdate"
LIMIT 10;




SELECT
    --QUERY04
    "O_ORDERPRIORITY",
    COUNT(*) AS ORDER_COUNT
FROM
    "ORDERS"
WHERE
    "o_orderdate" >= CAST('1993-07-01' AS DATE)
    AND "o_orderdate" < CAST('1993-10-01' AS DATE)
    AND EXISTS (
        SELECT
            *
        FROM
            "LINEITEM"
        WHERE
            "L_ORDERKEY" = "O_ORDERKEY"
            AND "l_commitdate" < "l_receiptdate"
    )
GROUP BY
    "O_ORDERPRIORITY"
ORDER BY
    "O_ORDERPRIORITY";


SELECT
    --QUERY05
    "N_NAME",
    SUM("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) AS REVENUE
FROM
    "CUSTOMER",
    "ORDERS",
    "LINEITEM",
    "SUPPLIER",
    "NATION",
    "REGION"
WHERE
    "C_CUSTKEY" = "O_CUSTKEY"
    AND "L_ORDERKEY" = "O_ORDERKEY"
    AND "L_SUPPKEY" = "S_SUPPKEY"
    AND "C_NATIONKEY" = "S_NATIONKEY"
    AND "S_NATIONKEY" = "N_NATIONKEY"
    AND "N_REGIONKEY" = "R_REGIONKEY"
    AND "R_NAME" = 'ASIA'
    AND "o_orderdate" >= CAST('1994-01-01' AS DATE)
    AND "o_orderdate" < CAST('1995-01-01' AS DATE)
GROUP BY
    "N_NAME"
ORDER BY
    REVENUE DESC;



SELECT
    --QUERY06
    SUM("L_EXTENDEDPRICE" * "L_DISCOUNT") AS REVENUE
FROM
    "LINEITEM"
WHERE
    "l_shipdate" >= CAST('1994-01-01' AS DATE)
    AND "l_shipdate" < CAST('1995-01-01' AS DATE)
    AND "L_DISCOUNT" BETWEEN 0.05
    AND 0.07
    AND "L_QUANTITY" < 24;


SELECT
    --QUERY07
    SUPP_NATION,
    CUST_NATION,
    "L_YEAR",
    SUM(VOLUME) AS REVENUE
FROM
    (
        SELECT
            N1."N_NAME" AS SUPP_NATION,
            N2."N_NAME" AS CUST_NATION,
            YEAR(
                "l_shipdate"
            ) AS "L_YEAR",
            "L_EXTENDEDPRICE" * (1 - "L_DISCOUNT") AS VOLUME
        FROM
            "SUPPLIER",
            "LINEITEM",
            "ORDERS",
            "CUSTOMER",
            "NATION" N1,
            "NATION" N2
        WHERE
            "S_SUPPKEY" = "L_SUPPKEY"
            AND "O_ORDERKEY" = "L_ORDERKEY"
            AND "C_CUSTKEY" = "O_CUSTKEY"
            AND "S_NATIONKEY" = N1."N_NATIONKEY"
            AND "C_NATIONKEY" = N2."N_NATIONKEY"
            AND (
                (
                    N1."N_NAME" = 'FRANCE'
                    AND N2."N_NAME" = 'GERMANY'
                )
                OR (
                    N1."N_NAME" = 'GERMANY'
                    AND N2."N_NAME" = 'FRANCE'
                )
            )
            AND "l_shipdate" BETWEEN CAST('1995-01-01' AS DATE)
            AND CAST('1996-12-31' AS DATE)
    ) AS SHIPPING
GROUP BY
    SUPP_NATION,
    CUST_NATION,
    "L_YEAR"
ORDER BY
    SUPP_NATION,
    CUST_NATION,
    "L_YEAR";


SELECT
    --QUERY08
    "O_YEAR",
    SUM(
        CASE
            WHEN "NATION" = 'BRAZIL' THEN "VOLUME"
            ELSE 0
        END
    ) / SUM("VOLUME") AS MKT_SHARE
FROM
    (
        SELECT
            YEAR("o_orderdate") AS "O_YEAR",
            "L_EXTENDEDPRICE" * (1 - "L_DISCOUNT") AS "VOLUME",
            N2."N_NAME" AS "NATION"
        FROM
            "PART",
            "SUPPLIER",
            "LINEITEM",
            "ORDERS",
            "CUSTOMER",
            "NATION" N1,
            "NATION" N2,
            "REGION"
        WHERE
            "P_PARTKEY" = "L_PARTKEY"
            AND "S_SUPPKEY" = "L_SUPPKEY"
            AND "L_ORDERKEY" = "O_ORDERKEY"
            AND "O_CUSTKEY" = "C_CUSTKEY"
            AND "C_NATIONKEY" = N1."N_NATIONKEY"
            AND N1."N_REGIONKEY" = "R_REGIONKEY"
            AND "R_NAME" = 'AMERICA'
            AND "S_NATIONKEY" = N2."N_NATIONKEY"
            AND "o_orderdate" BETWEEN CAST('1995-01-01' AS DATE)
            AND CAST('1996-12-31' AS DATE)
            AND "P_TYPE" = 'ECONOMY ANODIZED STEEL'
    ) AS "ALL_NATIONS"
GROUP BY
    "O_YEAR"
ORDER BY
    "O_YEAR";


SELECT
    --QUERY09
    "NATION",
    "O_YEAR",
    SUM("AMOUNT") AS SUM_PROFIT
FROM
    (
        SELECT
            "N_NAME" AS "NATION",
            YEAR("o_orderdate") AS "O_YEAR",
            "L_EXTENDEDPRICE" * (1 - "L_DISCOUNT") - "PS_SUPPLYCOST" * "L_QUANTITY" AS "AMOUNT"
        FROM
            "PART",
            "SUPPLIER",
            "LINEITEM",
            "PARTSUPP",
            "ORDERS",
            "NATION"
        WHERE
            "S_SUPPKEY" = "L_SUPPKEY"
            AND "PS_SUPPKEY" = "L_SUPPKEY"
            AND "PS_PARTKEY" = "L_PARTKEY"
            AND "P_PARTKEY" = "L_PARTKEY"
            AND "O_ORDERKEY" = "L_ORDERKEY"
            AND "S_NATIONKEY" = "N_NATIONKEY"
            AND "P_NAME" LIKE '%green%'
    ) AS PROFIT
GROUP BY
    "NATION",
    "O_YEAR"
ORDER BY
    "NATION",
    "O_YEAR" DESC;


SELECT
    --QUERY10
    "C_CUSTKEY",
    "C_NAME",
    SUM("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) AS "REVENUE",
    "C_ACCTBAL",
    "N_NAME",
    "C_ADDRESS",
    "C_PHONE",
    "C_COMMENT"
FROM
    "CUSTOMER",
    "ORDERS",
    "LINEITEM",
    "NATION"
WHERE
    "C_CUSTKEY" = "O_CUSTKEY"
    AND "L_ORDERKEY" = "O_ORDERKEY"
    AND "o_orderdate" >= CAST('1993-10-01' AS DATE)
    AND "o_orderdate" < CAST('1994-01-01' AS DATE)
    AND "L_RETURNFLAG" = 'R'
    AND "C_NATIONKEY" = "N_NATIONKEY"
GROUP BY
    "C_CUSTKEY",
    "C_NAME",
    "C_ACCTBAL",
    "C_PHONE",
    "N_NAME",
    "C_ADDRESS",
    "C_COMMENT"
ORDER BY
    "REVENUE" DESC
LIMIT 20;


SELECT
    --QUERY11
    "PS_PARTKEY",
    SUM("PS_SUPPLYCOST" * "PS_AVAILQTY") AS "VALUE"
FROM
    "PARTSUPP",
    "SUPPLIER",
    "NATION"
WHERE
    "PS_SUPPKEY" = "S_SUPPKEY"
    AND "S_NATIONKEY" = "N_NATIONKEY"
    AND "N_NAME" = 'GERMANY'
GROUP BY
    "PS_PARTKEY"
HAVING
    SUM("PS_SUPPLYCOST" * "PS_AVAILQTY") > (
        SELECT
            SUM("PS_SUPPLYCOST" * "PS_AVAILQTY") * (0.0001/10)
            -- SUM("PS_SUPPLYCOST" * "PS_AVAILQTY") * 1
        FROM
            "PARTSUPP",
            "SUPPLIER",
            "NATION"
        WHERE
            "PS_SUPPKEY" = "S_SUPPKEY"
            AND "S_NATIONKEY" = "N_NATIONKEY"
            AND "N_NAME" = 'GERMANY'
    )
ORDER BY
    "VALUE" DESC;
    
    

SELECT
    --QUERY12
    "L_SHIPMODE",
    SUM(
        CASE
            WHEN "O_ORDERPRIORITY" = '1-URGENT'
            OR "O_ORDERPRIORITY" = '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS HIGH_LINE_COUNT,
    SUM(
        CASE
            WHEN "O_ORDERPRIORITY" <> '1-URGENT'
            AND "O_ORDERPRIORITY" <> '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS LOW_LINE_COUNT
FROM
    "ORDERS",
    "LINEITEM"
WHERE
    "O_ORDERKEY" = "L_ORDERKEY"
    AND "L_SHIPMODE" IN ('MAIL', 'SHIP')
    AND "l_commitdate" < "l_receiptdate"
    AND "l_shipdate" < "l_commitdate"
    AND "l_receiptdate" >= CAST('1994-01-01' AS DATE)
    AND "l_receiptdate" < CAST('1995-01-01' AS DATE)
GROUP BY
    "L_SHIPMODE"
ORDER BY
    "L_SHIPMODE";



    
SELECT
    --QUERY13
    "C_COUNT",
    COUNT(*) AS "CUSTDIST"
FROM
    (
        SELECT
            "C_CUSTKEY",
            COUNT("O_ORDERKEY") AS "C_COUNT"
        FROM
            "CUSTOMER"
            LEFT OUTER JOIN "ORDERS" ON "C_CUSTKEY" = "O_CUSTKEY"
            AND "O_COMMENT" NOT LIKE '%SPECIAL%REQUESTS%'
        GROUP BY
            "C_CUSTKEY"
    ) AS C_ORDERS
GROUP BY
    "C_COUNT"
ORDER BY
    "CUSTDIST" DESC,
    "C_COUNT" DESC;




SELECT
    --QUERY14
    100.00 * SUM(
        CASE
            WHEN "P_TYPE" LIKE 'PROMO%' THEN "L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")
            ELSE 0
        END
    ) / SUM("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) AS "PROMO_REVENUE"
FROM
    "LINEITEM",
    "PART"
WHERE
    "L_PARTKEY" = "P_PARTKEY" AND "l_shipdate" >= CAST( '1995-09-01' AS DATE) AND "l_shipdate" < CAST('1995-10-01' AS DATE)
;

SELECT
    --QUERY15
    "S_SUPPKEY",
    "S_NAME",
    "S_ADDRESS",
    "S_PHONE",
    "TOTAL_REVENUE"
FROM
    "SUPPLIER",
    (
        SELECT
            "L_SUPPKEY" AS "SUPPLIER_NO",
            SUM("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) AS "TOTAL_REVENUE"
        FROM
            "LINEITEM"
        WHERE
            "l_shipdate" >= CAST('1996-01-01' AS DATE)
            AND "l_shipdate" < CAST('1996-04-01' AS DATE)
        GROUP BY
            "L_SUPPKEY"
    ) REVENUE0
WHERE
    "S_SUPPKEY" = "SUPPLIER_NO"
    AND "TOTAL_REVENUE" = (
        SELECT
            MAX("TOTAL_REVENUE")
        FROM
            (
                SELECT
                    "L_SUPPKEY" AS "SUPPLIER_NO",
                    SUM("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) AS "TOTAL_REVENUE"
                FROM
                    "LINEITEM"
                WHERE
                    "l_shipdate" >= CAST('1996-01-01' AS DATE)
                    AND "l_shipdate" < CAST('1996-04-01' AS DATE)
                GROUP BY
                    "L_SUPPKEY"
            ) REVENUE1
    )
ORDER BY
    "S_SUPPKEY"
;


SELECT
    --QUERY16
    "P_BRAND",
    "P_TYPE",
    "P_SIZE",
    COUNT(DISTINCT "PS_SUPPKEY") AS "SUPPLIER_CNT"
FROM
    "PARTSUPP",
    "PART"
WHERE
    "P_PARTKEY" = "PS_PARTKEY"
    AND "P_BRAND" <> 'BRAND#45'
    AND "P_TYPE" NOT LIKE 'MEDIUM POLISHED%'
    AND "P_SIZE" IN (
        49,
        14,
        23,
        45,
        19,
        3,
        36,
        9
    )
    AND "PS_SUPPKEY" NOT IN (
        SELECT
            "S_SUPPKEY"
        FROM
            "SUPPLIER"
        WHERE
            "S_COMMENT" LIKE '%"CUSTOMER"%COMPLAINTS%'
    )
GROUP BY
    "P_BRAND",
    "P_TYPE",
    "P_SIZE"
ORDER BY
    "SUPPLIER_CNT" DESC,
    "P_BRAND",
    "P_TYPE",
    "P_SIZE";


SELECT
    --QUERY17
    SUM("L_EXTENDEDPRICE") / 7.0 AS AVG_YEARLY
FROM
    "LINEITEM",
    "PART"
WHERE
    "P_PARTKEY" = "L_PARTKEY"
    AND "P_BRAND" = 'Brand#23'
    AND "P_CONTAINER" = 'MED BOX'
    AND "L_QUANTITY" < (
        SELECT
            0.2 * AVG("L_QUANTITY")
        FROM
            "LINEITEM"
        WHERE
            "L_PARTKEY" = "P_PARTKEY"
    );


SELECT
    --QUERY18
    "C_NAME",
    "C_CUSTKEY",
    "O_ORDERKEY",
    "o_orderdate",
    "O_TOTALPRICE",
    SUM("L_QUANTITY")
FROM
    "CUSTOMER",
    "ORDERS",
    "LINEITEM"
WHERE
    "O_ORDERKEY" IN (
        SELECT
            "L_ORDERKEY"
        FROM
            "LINEITEM"
        GROUP BY
            "L_ORDERKEY"
        HAVING
            SUM("L_QUANTITY") > 300
    )
    AND "C_CUSTKEY" = "O_CUSTKEY"
    AND "O_ORDERKEY" = "L_ORDERKEY"
GROUP BY
    "C_NAME",
    "C_CUSTKEY",
    "O_ORDERKEY",
    "o_orderdate",
    "O_TOTALPRICE"
ORDER BY
    "O_TOTALPRICE" DESC,
    "o_orderdate"
LIMIT 100;


SELECT
    --QUERY19
    SUM("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT")) AS "REVENUE"
FROM
    "LINEITEM",
    "PART"
WHERE
    (
        "P_PARTKEY" = "L_PARTKEY"
        AND "P_BRAND" = 'Brand#12'
        AND "P_CONTAINER" IN (
            'SM CASE',
            'SM BOX',
            'SM PACK',
            'SM PKG'
        )
        AND "L_QUANTITY" >= 1
        AND "L_QUANTITY" <= 1 + 10
        AND "P_SIZE" BETWEEN 1
        AND 5
        AND "L_SHIPMODE" IN ('AIR', 'AIR REG')
        AND "L_SHIPINSTRUCT" = 'DELIVER IN PERSON'
    )
    OR (
        "P_PARTKEY" = "L_PARTKEY"
        AND "P_BRAND" = 'Brand#23'
        AND "P_CONTAINER" IN (
            'MED BAG',
            'MED BOX',
            'MED PKG',
            'MED PACK'
        )
        AND "L_QUANTITY" >= 10
        AND "L_QUANTITY" <= 10 + 10
        AND "P_SIZE" BETWEEN 1
        AND 10
        AND "L_SHIPMODE" IN ('AIR', 'AIR REG')
        AND "L_SHIPINSTRUCT" = 'DELIVER IN PERSON'
    )
    OR (
        "P_PARTKEY" = "L_PARTKEY"
        AND "P_BRAND" = 'Brand#34'
        AND "P_CONTAINER" IN (
            'LG CASE',
            'LG BOX',
            'LG PACK',
            'LG PKG'
        )
        AND "L_QUANTITY" >= 20
        AND "L_QUANTITY" <= 20 + 10
        AND "P_SIZE" BETWEEN 1
        AND 15
        AND "L_SHIPMODE" IN ('AIR', 'AIR REG')
        AND "L_SHIPINSTRUCT" = 'DELIVER IN PERSON'
    );


SELECT
    --QUERY20
    "S_NAME",
    "S_ADDRESS"
FROM
    "SUPPLIER",
    "NATION"
WHERE
    "S_SUPPKEY" IN (
        SELECT
            "PS_SUPPKEY"
        FROM
            "PARTSUPP"
        WHERE
            "PS_PARTKEY" IN (
                SELECT
                    "P_PARTKEY"
                FROM
                    "PART"
                WHERE
                    "P_NAME" LIKE 'forest%'
            )
            AND "PS_AVAILQTY" > (
                SELECT
                    0.5 * SUM("L_QUANTITY")
                FROM
                    "LINEITEM"
                WHERE
                    "L_PARTKEY" = "PS_PARTKEY"
                    AND "L_SUPPKEY" = "PS_SUPPKEY"
                    AND "l_shipdate" >= CAST('1994-01-01' AS DATE)
                    AND "l_shipdate" < CAST('1995-01-01' AS DATE)
            )
    )
    AND "S_NATIONKEY" = "N_NATIONKEY"
    AND "N_NAME" = 'CANADA'
ORDER BY
    "S_NAME";


SELECT
    --QUERY21
    "S_NAME",
    COUNT(*) AS "NUMWAIT"
FROM
    "SUPPLIER",
    "LINEITEM" L1,
    "ORDERS",
    "NATION"
WHERE
    "S_SUPPKEY" = L1."L_SUPPKEY"
    AND "O_ORDERKEY" = L1."L_ORDERKEY"
    AND "O_ORDERSTATUS" = 'F'
    AND L1."l_receiptdate" > L1."l_commitdate"
    AND EXISTS (
        SELECT
            *
        FROM
            "LINEITEM" L2
        WHERE
            L2."L_ORDERKEY" = L1."L_ORDERKEY"
            AND L2."L_SUPPKEY" <> L1."L_SUPPKEY"
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            "LINEITEM" L3
        WHERE
            L3."L_ORDERKEY" = L1."L_ORDERKEY"
            AND L3."L_SUPPKEY" <> L1."L_SUPPKEY"
            AND L3."l_receiptdate" > L3."l_commitdate"
    )
    AND "S_NATIONKEY" = "N_NATIONKEY"
    AND "N_NAME" = 'SAUDI ARABIA'
GROUP BY
    "S_NAME"
ORDER BY
    "NUMWAIT" DESC,
    "S_NAME"
LIMIT 100;



SELECT
    --QUERY22
    "CNTRYCODE",
    COUNT(*) AS "NUMCUST",
    SUM("C_ACCTBAL") AS "TOTACCTBAL"
FROM
    (
        SELECT
            SUBSTRING("C_PHONE", 1, 2) AS "CNTRYCODE",
            "C_ACCTBAL"
        FROM
            "CUSTOMER"
        WHERE
            SUBSTRING("C_PHONE", 1, 2) IN (
                '13',
                '31',
                '23',
                '29',
                '30',
                '18',
                '17'
            )
            AND "C_ACCTBAL" > (
                SELECT
                    AVG("C_ACCTBAL")
                FROM
                    "CUSTOMER"
                WHERE
                    "C_ACCTBAL" > 0.00
                    AND SUBSTRING("C_PHONE", 1, 2) IN (
                        '13',
                        '31',
                        '23',
                        '29',
                        '30',
                        '18',
                        '17'
                    )
            )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    "ORDERS"
                WHERE
                    "O_CUSTKEY" = "C_CUSTKEY"
            )
    ) AS CUSTSALE
GROUP BY
    "CNTRYCODE"
ORDER BY
    "CNTRYCODE";