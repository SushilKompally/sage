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
        employeeid,

        -- ===========================
        -- STRINGS
        -- ===========================
        contact_name,
        departmentid,
        employeetype,
        entity,
        filepaymentservice,
        locationid,
        megaentityid,
        megaentityname,
        personalinfo_companyname,
        personalinfo_contactname,
        personalinfo_email_1,
        personalinfo_firstname,
        personalinfo_initial,
        personalinfo_lastname,
        personalinfo_mailaddress_country,
        personalinfo_mailaddress_countrycode,
        personalinfo_phone_1,
        personalinfo_printas,
        record_url,
        status,
        supervisorid,
        supervisorname,
        title,

        -- ===========================
        -- NUMERICS
        -- ===========================
        contactkey,
        createdby,
        departmentkey,
        emptypekey,
        locationkey,
        megaentitykey,
        modifiedby,
        parentkey,
        recordno,

        -- ===========================
        -- BOOLEANS
        -- ===========================
        generic ,
        mergepaymentreq,
        paymentnotify,
        postactualcost ,

        -- ===========================
        -- TIMESTAMPS
        -- ===========================
        whencreated,
        whenmodified,

        -- ===========================
        -- SILVER AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                         AS silver_load_date,


    FROM raw
)

SELECT *
FROM cleaned
