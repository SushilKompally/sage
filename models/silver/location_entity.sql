/*
-- Description: Incremental Load Script for Silver Layer - location_entity Table
-- Script Name: silver_location_entity.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'location_entity' data from Bronze to Silver.
--     Applies macro-based cleaning/casting and appends source metadata.
-- Change History:
--     05-Jan-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='RECORDNO',
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('sage_bronze', 'location_entity') }}
    WHERE 1 = 1
    {{ incremental_filter() }}
),

cleaned AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        {{ safe_integer('RECORDNO', 38, 0) }}                    AS RECORDNO,

        -- ===========================
        -- STRINGS / IDENTIFIERS
        -- ===========================
        {{ clean_string('ACCOUNTINGTYPE') }}                    AS ACCOUNTINGTYPE,
        {{ clean_string('ADDRESSCOUNTRYDEFAULT') }}             AS ADDRESSCOUNTRYDEFAULT,
        {{ clean_string('BUSINESSDAYS') }}                      AS BUSINESSDAYS,
        {{ clean_string('CUSTTITLE') }}                         AS CUSTTITLE,
        {{ clean_string('ENTITY') }}                            AS ENTITY,
        {{ clean_string('FEDERALID') }}                         AS FEDERALID,
        {{ clean_string('LEGALCOUNTRYCODE') }}                  AS LEGALCOUNTRYCODE,
        {{ clean_string('LOCATIONID') }}                        AS LOCATIONID,
        {{ clean_string('NAME') }}                              AS NAME,
        {{ clean_string('OPCOUNTRY') }}                         AS OPCOUNTRY,
        {{ clean_string('RECORD_URL') }}                        AS RECORD_URL,
        {{ clean_string('REPORTPRINTAS') }}                     AS REPORTPRINTAS,
        {{ clean_string('STATUS') }}                            AS STATUS,
        {{ clean_string('TAXID') }}                             AS TAXID,
        {{ clean_string('WEEKENDS') }}                          AS WEEKENDS,

        -- ===========================
        -- NUMERICS
        -- ===========================
        {{ safe_integer('CREATEDBY', 38, 0) }}                  AS CREATEDBY,
        {{ safe_integer('FIRSTMONTH', 38, 0) }}                 AS FIRSTMONTH,
        {{ safe_integer('FIRSTMONTHTAX', 38, 0) }}              AS FIRSTMONTHTAX,
        {{ safe_integer('MODIFIEDBY', 38, 0) }}                 AS MODIFIEDBY,

        -- ===========================
        -- BOOLEANS
        -- ===========================
        TRY_CAST(DEFAULTPARTIALEXEMPT AS BOOLEAN)                AS DEFAULTPARTIALEXEMPT,
        TRY_CAST(ENABLELEGALCONTACT AS BOOLEAN)                 AS ENABLELEGALCONTACT,
        TRY_CAST(ENABLELEGALCONTACT_TPAR AS BOOLEAN)            AS ENABLELEGALCONTACT_TPAR,
        TRY_CAST(HAS_IE_RELATION AS BOOLEAN)                    AS HAS_IE_RELATION,
        TRY_CAST(ISLIMITEDENTITY AS BOOLEAN)                    AS ISLIMITEDENTITY,
        TRY_CAST(ISROOT AS BOOLEAN)                             AS ISROOT,
        TRY_CAST(PARTIALEXEMPT AS BOOLEAN)                      AS PARTIALEXEMPT,

        -- ===========================
        -- DATES / TIMESTAMPS
        -- ===========================
        TRY_CAST(STARTOPEN AS DATE)                             AS STARTOPEN,
        TRY_CAST(STATUTORYREPORTINGPERIOD AS DATE)              AS STATUTORYREPORTINGPERIOD,
        WHENCREATED                                             AS WHENCREATED,
        WHENMODIFIED                                            AS WHENMODIFIED,

        -- ===========================
        -- SILVER LOAD AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()                                     AS SILVER_LOAD_DATE,


    FROM raw
)

SELECT *
FROM cleaned
