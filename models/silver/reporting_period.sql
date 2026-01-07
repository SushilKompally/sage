/*
-- Description: Incremental Load Script for Silver Layer - reporting_period Table
-- Script Name: silver_reporting_period.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'reporting_period' data
--     from Bronze to Silver using macro-based cleansing.
-- Change History:
--     05-Jan-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='recordno',
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('sage_bronze', 'reporting_period') }}
    WHERE 1 = 1
    {{ incremental_filter() }}
),

cleaned AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        recordno,

        -- ===========================
        -- FLAGS / NUMERICS
        -- ===========================
        budgeting,
        datetype,

        -- ===========================
        -- DATES
        -- ===========================
        {{ safe_date('start_date') }}                   AS start_date,
        {{ safe_date('end_date') }}                     AS end_date,

        -- ===========================
        -- STRINGS
        -- ===========================
        {{ clean_string_lower('header_1') }}            AS header_1,
        {{ clean_string_lower('header_2') }}            AS header_2,
        {{ clean_string_lower('name') }}                AS name,
        {{ clean_string_lower('status') }}              AS status,
        {{ clean_string_lower('record_url') }}          AS record_url,

        -- ===========================
        -- SOURCE TIMESTAMPS
        -- ===========================
        whencreated,
        whenmodified,

        -- ===========================
        -- SILVER AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ              AS silver_load_date,


    FROM raw
)

SELECT *
FROM cleaned
