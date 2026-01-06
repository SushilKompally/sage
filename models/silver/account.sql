
/*
-- Description: Incremental Load Script for Silver Layer - account Table
-- Script Name: silver_account.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'account' from Bronze to Silver.
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
    FROM {{ source('sage_bronze', 'account') }}
    WHERE 1 = 1
    {{ incremental_filter() }}
),

cleaned AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        {{ safe_integer('RECORDNO', 38, 0) }}                      AS RECORDNO,

        -- ===========================
        -- DETAILS (strings)
        -- ===========================
        {{ clean_string('ACCOUNTNO') }}                             AS ACCOUNTNO,
        {{ clean_string('ACCOUNTTYPE') }}                           AS ACCOUNTTYPE,
        {{ clean_string('ALTERNATIVEACCOUNT') }}                    AS ALTERNATIVEACCOUNT,
        {{ clean_string('CATEGORY') }}                              AS CATEGORY,
        {{ clean_string('CLOSINGACCOUNTTITLE') }}                   AS CLOSINGACCOUNTTITLE,
        {{ clean_string('CLOSINGTYPE') }}                           AS CLOSINGTYPE,
        {{ clean_string('CREATEDBYLOGINID') }}                      AS CREATEDBYLOGINID,
        {{ clean_string('MODIFIEDBYLOGINID') }}                     AS MODIFIEDBYLOGINID,
        {{ clean_string('NORMALBALANCE') }}                         AS NORMALBALANCE,
        {{ clean_string('RECORD_URL') }}                            AS RECORD_URL,
        {{ clean_string('STATUS') }}                                AS STATUS,
        {{ clean_string('TITLE') }}                                 AS TITLE,

        -- ===========================
        -- NUMERICS
        -- ===========================
        {{ safe_integer('CATEGORYKEY', 38, 0) }}                    AS CATEGORYKEY,
        {{ safe_integer('CLOSETOACCTKEY', 38, 0) }}                 AS CLOSETOACCTKEY,
        {{ safe_integer('CLOSINGACCOUNTNO', 38, 0) }}               AS CLOSINGACCOUNTNO,
        {{ safe_integer('CREATEDBY', 38, 0) }}                      AS CREATEDBY,
        {{ safe_integer('MODIFIEDBY', 38, 0) }}                     AS MODIFIEDBY,

        -- ===========================
        -- BOOLEANS
        -- ===========================
        ENABLE_GLMATCHING                    AS ENABLE_GLMATCHING,
        REQUIRECLASS                          AS REQUIRECLASS,
        REQUIRECUSTOMER                       AS REQUIRECUSTOMER,
        REQUIREDEPT                           AS REQUIREDEPT,
        REQUIREEMPLOYEE                       AS REQUIREEMPLOYEE,
        REQUIREGLDIMREVENUE_CENTER            AS REQUIREGLDIMREVENUE_CENTER,
        REQUIREITEM                  AS REQUIREITEM,
        REQUIRELOC                            AS REQUIRELOC,
        REQUIREPROJECT                        AS REQUIREPROJECT,
        REQUIREVENDOR                         AS REQUIREVENDOR,
        REQUIREWAREHOUSE                      AS REQUIREWAREHOUSE,
        SUBLEDGERCONTROLON                    AS SUBLEDGERCONTROLON,
        TAXABLE                               AS TAXABLE,

        -- ===========================
        -- DATES / TIMESTAMPS FROM SOURCE
        -- ===========================
        WHENCREATED                     AS WHENCREATED,
       WHENMODIFIED              AS WHENMODIFIED,
       ISDELETED as is_deleted,

        -- ===========================
        -- SILVER / LOAD AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()                                         AS SILVER_LOAD_DATE,

        -- ===========================
        -- METADATA (from source_metadata())
        -- ===========================
        _INGESTION_TIMESTAMP,
        _SOURCE_OBJECT,
        _BATCH_ID
    FROM raw
)

SELECT *
FROM cleaned
