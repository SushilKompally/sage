/*
-- Description: Incremental Load Script for Silver Layer - entry Table
-- Script Name: silver_entry.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'entry' data from Bronze to Silver.
--     Applies macro-based cleaning/casting and appends source metadata.
-- Change History:
--     05-Jan-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='ACCOUNTNO',
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('sage_bronze', 'entry') }}
    WHERE 1 = 1
    {{ incremental_filter() }}
),

cleaned AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        {{ safe_integer('ACCOUNTNO', 38, 0) }}              AS ACCOUNTNO,

        -- ===========================
        -- IDENTIFIERS / STRINGS
        -- ===========================
        {{ clean_string('ACCOUNTTITLE') }}                AS ACCOUNTTITLE,
        {{ clean_string('BASECURR') }}                    AS BASECURR,
        {{ clean_string('BATCHNO') }}                     AS BATCHNO,
        {{ clean_string('BATCHTITLE') }}                  AS BATCHTITLE,
        {{ clean_string('CLASSID') }}                     AS CLASSID,
        {{ clean_string('CLASSNAME') }}                   AS CLASSNAME,
        {{ clean_string('CREATEDBYLOGINID') }}            AS CREATEDBYLOGINID,
        {{ clean_string('CURRENCY') }}                    AS CURRENCY,
        {{ clean_string('CUSTOMERID') }}                  AS CUSTOMERID,
        {{ clean_string('CUSTOMERNAME') }}                AS CUSTOMERNAME,
        {{ clean_string('DEPARTMENT') }}                  AS DEPARTMENT,
        {{ clean_string('DEPARTMENTTITLE') }}             AS DEPARTMENTTITLE,
        {{ clean_string('DESCRIPTION') }}                 AS DESCRIPTION,
        {{ clean_string('DOCUMENT') }}                    AS DOCUMENT,
        {{ clean_string('ITEMID') }}                      AS ITEMID,
        {{ clean_string('ITEMNAME') }}                    AS ITEMNAME,
        {{ clean_string('LOCATION') }}                    AS LOCATION,
        {{ clean_string('LOCATIONNAME') }}                AS LOCATIONNAME,
        {{ clean_string('MODIFIEDBYLOGINID') }}           AS MODIFIEDBYLOGINID,
        {{ clean_string('PROJECTID') }}                   AS PROJECTID,
        {{ clean_string('PROJECTNAME') }}                 AS PROJECTNAME,
        {{ clean_string('RECORD_URL') }}                  AS RECORD_URL,
        {{ clean_string('STATE') }}                       AS STATE,
        {{ clean_string('VENDORNAME') }}                  AS VENDORNAME,
        {{ clean_string('WAREHOUSEID') }}                 AS WAREHOUSEID,
        {{ clean_string('WAREHOUSENAME') }}               AS WAREHOUSENAME,

        -- ===========================
        -- NUMERICS
        -- ===========================
        {{ safe_integer('ACCOUNTKEY', 38, 0) }}            AS ACCOUNTKEY,
        {{ safe_integer('BATCH_NUMBER', 38, 0) }}          AS BATCH_NUMBER,
        {{ safe_integer('CLASSDIMKEY', 38, 0) }}           AS CLASSDIMKEY,
        {{ safe_integer('CREATEDBY', 38, 0) }}             AS CREATEDBY,
        {{ safe_integer('CUSTOMERDIMKEY', 38, 0) }}        AS CUSTOMERDIMKEY,
        {{ safe_integer('DEPARTMENTKEY', 38, 0) }}         AS DEPARTMENTKEY,
        {{ safe_integer('GLDIMREVENUE_CENTER', 38, 0) }}   AS GLDIMREVENUE_CENTER,
        {{ safe_integer('ITEMDIMKEY', 38, 0) }}            AS ITEMDIMKEY,
        {{ safe_integer('LINE_NO', 38, 0) }}               AS LINE_NO,
        {{ safe_integer('LOCATIONKEY', 38, 0) }}           AS LOCATIONKEY,
        {{ safe_integer('MODIFIEDBY', 38, 0) }}            AS MODIFIEDBY,
        {{ safe_integer('PROJECTDIMKEY', 38, 0) }}         AS PROJECTDIMKEY,
        {{ safe_integer('RDEPRECIATION_SUMMARY', 38, 0) }} AS RDEPRECIATION_SUMMARY,
        {{ safe_integer('TR_TYPE', 38, 0) }}               AS TR_TYPE,
        {{ safe_integer('USERNO', 38, 0) }}                AS USERNO,
        {{ safe_integer('VENDORDIMKEY', 38, 0) }}          AS VENDORDIMKEY,
        {{ safe_integer('VENDORID', 38, 0) }}              AS VENDORID,
        {{ safe_integer('WAREHOUSEDIMKEY', 38, 0) }}       AS WAREHOUSEDIMKEY,

        -- ===========================
        -- MEASURES
        -- ===========================
        AMOUNT                                             AS AMOUNT,
        TRX_AMOUNT                                         AS TRX_AMOUNT,
        EXCHANGE_RATE                                      AS EXCHANGE_RATE,

        -- ===========================
        -- BOOLEANS
        -- ===========================
        TRY_CAST(ADJ AS BOOLEAN)                            AS ADJ,
        TRY_CAST(BILLABLE AS BOOLEAN)                       AS BILLABLE,
        TRY_CAST(STATISTICAL AS BOOLEAN)                    AS STATISTICAL,

        -- ===========================
        -- DATES / TIMESTAMPS
        -- ===========================
        TRY_CAST(BATCH_DATE AS DATE)                        AS BATCH_DATE,
        TRY_CAST(CLRDATE AS DATE)                           AS CLRDATE,
        TRY_CAST(ENTRY_DATE AS DATE)                        AS ENTRY_DATE,
        TRY_CAST(EXCH_RATE_DATE AS DATE)                    AS EXCH_RATE_DATE,
        WHENCREATED                                        AS WHENCREATED,
        WHENMODIFIED                                       AS WHENMODIFIED,

        -- ===========================
        -- SILVER LOAD AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()                                 AS SILVER_LOAD_DATE,



    FROM raw
)

SELECT *
FROM cleaned
