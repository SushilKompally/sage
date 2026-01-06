/*
-- Description: Incremental Load Script for Silver Layer - batch Table
-- Script Name: silver_batch.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'batch' data
--     from Bronze to Silver using macro-based cleansing.
-- Change History:
--     05-Jan-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='batchno',
    incremental_strategy='merge',
) }}

WITH raw AS (
    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('sage_bronze', 'batch') }}
    WHERE 1 = 1
    {{ incremental_filter() }}
),

cleaned AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        {{ safe_integer('batchno', 38, 0) }}              AS batchno,

        -- ===========================
        -- STRINGS
        -- ===========================
        {{ clean_string_lower('baselocation') }}          AS baselocation,
        {{ clean_string_lower('baselocation_no') }}       AS baselocation_no,
        {{ clean_string_lower('batch_title') }}           AS batch_title,
        {{ clean_string_lower('createdbyloginid') }}      AS createdbyloginid,
        {{ clean_string_lower('journal') }}               AS journal,
        {{ clean_string_lower('megaentityid') }}          AS megaentityid,
        {{ clean_string_lower('megaentityname') }}        AS megaentityname,
        {{ clean_string_lower('modifiedbyid') }}          AS modifiedbyid,
        {{ clean_string_lower('modifiedbyloginid') }}     AS modifiedbyloginid,
        {{ clean_string_lower('module') }}                AS module,
        {{ clean_string_lower('record_url') }}            AS record_url,
        {{ clean_string_lower('referenceno') }}           AS referenceno,
        {{ clean_string_lower('state') }}                 AS state,
        {{ clean_string_lower('supdocid') }}              AS supdocid,
        {{ clean_string_lower('taximplications') }}       AS taximplications,
        {{ clean_string_lower('transactionsource') }}    AS transactionsource,
        {{ clean_string_lower('userinfo_loginid') }}      AS userinfo_loginid,

        -- ===========================
        -- NUMERICS
        -- ===========================
        {{ safe_integer('createdby') }}                   AS createdby,
        {{ safe_integer('megaentitykey') }}               AS megaentitykey,
        {{ safe_integer('modifiedby') }}                  AS modifiedby,
        {{ safe_integer('prbatchkey') }}                  AS prbatchkey,
        {{ safe_integer('rdepreciation_schedule') }}      AS rdepreciation_schedule,
        {{ safe_integer('recordno') }}                    AS recordno,
        {{ safe_integer('reversedkey') }}                 AS reversedkey,
        {{ safe_integer('rfixed_assets_log') }}           AS rfixed_assets_log,
        {{ safe_integer('rpesentry') }}                   AS rpesentry,
        {{ safe_integer('schopkey') }}                    AS schopkey,
        {{ safe_integer('supdockey') }}                   AS supdockey,
        {{ safe_integer('userkey') }}                     AS userkey,

        -- ===========================
        -- BOOLEANS
        -- ===========================
        COALESCE(TRY_TO_BOOLEAN(statistical), FALSE)      AS statistical,

        -- ===========================
        -- DATES
        -- ===========================
        {{ safe_date('batch_date') }}                     AS batch_date,
        {{ safe_date('reversed') }}                       AS reversed,
        {{ safe_date('reversedfrom') }}                   AS reversedfrom,

        -- ===========================
        -- TIMESTAMPS
        -- ===========================
        {{ safe_timestamp_ntz('modified') }}              AS modified,
        {{ safe_timestamp_ntz('whencreated') }}           AS whencreated,
        {{ safe_timestamp_ntz('whenmodified') }}          AS whenmodified,

        -- ===========================
        -- SILVER AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                AS silver_load_date,


    FROM raw
)

SELECT *
FROM cleaned
