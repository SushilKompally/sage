/*
-- Description: Incremental Load Script for Silver Layer - employee Table
-- Script Name: silver_employee.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'employee' data
--     from Bronze to Silver using macro-based cleansing.
-- Change History:
--     05-Jan-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='employeeid',
    incremental_strategy='merge',
) }}

WITH raw AS (
    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('sage_bronze', 'employee') }}
    WHERE 1 = 1
    {{ incremental_filter() }}
),

cleaned AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        {{ clean_string_lower('employeeid') }}                    AS employeeid,

        -- ===========================
        -- STRINGS
        -- ===========================
        {{ clean_string_lower('contact_name') }}                  AS contact_name,
        {{ clean_string_lower('departmentid') }}                  AS departmentid,
        {{ clean_string_lower('employeetype') }}                  AS employeetype,
        {{ clean_string_lower('entity') }}                        AS entity,
        {{ clean_string_lower('filepaymentservice') }}            AS filepaymentservice,
        {{ clean_string_lower('locationid') }}                    AS locationid,
        {{ clean_string_lower('megaentityid') }}                  AS megaentityid,
        {{ clean_string_lower('megaentityname') }}                AS megaentityname,
        {{ clean_string_lower('personalinfo_companyname') }}      AS personalinfo_companyname,
        {{ clean_string_lower('personalinfo_contactname') }}      AS personalinfo_contactname,
        {{ clean_string_lower('personalinfo_email_1') }}          AS personalinfo_email_1,
        {{ clean_string_lower('personalinfo_firstname') }}        AS personalinfo_firstname,
        {{ clean_string_lower('personalinfo_initial') }}          AS personalinfo_initial,
        {{ clean_string_lower('personalinfo_lastname') }}         AS personalinfo_lastname,
        {{ clean_string_lower('personalinfo_mailaddress_country') }}     AS personalinfo_mailaddress_country,
        {{ clean_string_lower('personalinfo_mailaddress_countrycode') }} AS personalinfo_mailaddress_countrycode,
        {{ clean_string_lower('personalinfo_phone_1') }}          AS personalinfo_phone_1,
        {{ clean_string_lower('personalinfo_printas') }}          AS personalinfo_printas,
        {{ clean_string_lower('record_url') }}                    AS record_url,
        {{ clean_string_lower('status') }}                        AS status,
        {{ clean_string_lower('supervisorid') }}                  AS supervisorid,
        {{ clean_string_lower('supervisorname') }}                AS supervisorname,
        {{ clean_string_lower('title') }}                         AS title,

        -- ===========================
        -- NUMERICS
        -- ===========================
        {{ safe_integer('contactkey') }}                           AS contactkey,
        {{ safe_integer('createdby') }}                            AS createdby,
        {{ safe_integer('departmentkey') }}                        AS departmentkey,
        {{ safe_integer('emptypekey') }}                           AS emptypekey,
        {{ safe_integer('locationkey') }}                          AS locationkey,
        {{ safe_integer('megaentitykey') }}                        AS megaentitykey,
        {{ safe_integer('modifiedby') }}                           AS modifiedby,
        {{ safe_integer('parentkey') }}                            AS parentkey,
        {{ safe_integer('recordno') }}                             AS recordno,

        -- ===========================
        -- BOOLEANS
        -- ===========================
        TRY_CAST(generic AS BOOLEAN)                               AS generic,
        TRY_CAST(mergepaymentreq AS BOOLEAN)                      AS mergepaymentreq,
        TRY_CAST(paymentnotify AS BOOLEAN)                        AS paymentnotify,
        TRY_CAST(postactualcost AS BOOLEAN)                       AS postactualcost,

        -- ===========================
        -- TIMESTAMPS
        -- ===========================
        {{ safe_timestamp_ntz('whencreated') }}                    AS whencreated,
        {{ safe_timestamp_ntz('whenmodified') }}                   AS whenmodified,

        -- ===========================
        -- SILVER AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                         AS silver_load_date,


    FROM raw
)

SELECT *
FROM cleaned
