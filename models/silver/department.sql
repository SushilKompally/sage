/*
-- Description: Incremental Load Script for Silver Layer - department Table
-- Script Name: silver_department.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'department' data
--     from Bronze to Silver using macro-based cleansing.
-- Change History:
--     05-Jan-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='departmentid',
    incremental_strategy='merge',
) }}

WITH raw AS (
    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('sage_bronze', 'department') }}
    WHERE 1 = 1
    {{ incremental_filter() }}
),

cleaned AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        {{ clean_string_lower('departmentid') }}      AS departmentid,

        -- ===========================
        -- STRINGS
        -- ===========================
        {{ clean_string_lower('custtitle') }}         AS custtitle,
        {{ clean_string_lower('parentid') }}          AS parentid,
        {{ clean_string_lower('parentname') }}        AS parentname,
        {{ clean_string_lower('record_url') }}        AS record_url,
        {{ clean_string_lower('status') }}            AS status,
        {{ clean_string_lower('supervisorid') }}      AS supervisorid,
        {{ clean_string_lower('supervisorname') }}    AS supervisorname,
        {{ clean_string_lower('title') }}             AS title,

        -- ===========================
        -- NUMERICS
        -- ===========================
        {{ safe_integer('createdby') }}               AS createdby,
        {{ safe_integer('modifiedby') }}              AS modifiedby,
        {{ safe_integer('parentkey') }}               AS parentkey,
        {{ safe_integer('recordno') }}                AS recordno,
        {{ safe_integer('supervisorkey') }}           AS supervisorkey,

        -- ===========================
        -- TIMESTAMPS
        -- ===========================
        {{ safe_timestamp_ntz('whencreated') }}       AS whencreated,
        {{ safe_timestamp_ntz('whenmodified') }}      AS whenmodified,

        -- ===========================
        -- SILVER AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ            AS silver_load_date,


    FROM raw
)

SELECT *
FROM cleaned
