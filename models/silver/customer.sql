/*
-- Description: Incremental Load Script for Silver Layer - customer Table
-- Script Name: silver_customer.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'customer' from Bronze to Silver.
--     Applies macro-based cleaning/casting and appends source metadata.
-- Change History:
--     05-Jan-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='ACCOUNTKEY',
    incremental_strategy='merge'
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('sage_bronze', 'customer') }}
    WHERE 1 = 1
    {{ incremental_filter() }}

),

cleaned AS (

    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        {{ safe_integer('ACCOUNTKEY', 38, 0) }}            AS ACCOUNTKEY,

        -- ===========================
        -- FOREIGN / BUSINESS KEYS
        -- ===========================
        {{ safe_integer('ARACCOUNT', 38, 0) }}          AS ARACCOUNT,
        {{ safe_integer('CUSTTYPEKEY', 38, 0) }}        AS CUSTTYPEKEY,
        {{ safe_integer('DISPLAYCONTACTKEY', 38, 0) }} AS DISPLAYCONTACTKEY,
        {{ safe_integer('MEGAENTITYKEY', 38, 0) }}      AS MEGAENTITYKEY,
        {{ safe_integer('TERMSKEY', 38, 0) }}           AS TERMSKEY,

        -- ===========================
        -- DETAILS (strings)
        -- ===========================
        {{ clean_string('ARACCOUNTTITLE') }}            AS ARACCOUNTTITLE,
        {{ clean_string('ARINVOICEPRINTTEMPLATEID') }}  AS ARINVOICEPRINTTEMPLATEID,
        {{ clean_string('CREATEDBYLOGINID') }}          AS CREATEDBYLOGINID,
        {{ clean_string('CUSTOMERID') }}                AS CUSTOMERID,
        {{ clean_string('CUSTTYPE') }}                  AS CUSTTYPE,
        {{ clean_string('DELIVERY_OPTIONS') }}          AS DELIVERY_OPTIONS,
        {{ clean_string('ENTITY') }}                    AS ENTITY,
        {{ clean_string('MEGAENTITYID') }}              AS MEGAENTITYID,
        {{ clean_string('MEGAENTITYNAME') }}            AS MEGAENTITYNAME,
        {{ clean_string('MODIFIEDBYLOGINID') }}         AS MODIFIEDBYLOGINID,
        {{ clean_string('NAME') }}                      AS NAME,
        {{ clean_string('OBJECTRESTRICTION') }}         AS OBJECTRESTRICTION,
        {{ clean_string('PRCLST_OVERRIDE') }}           AS PRCLST_OVERRIDE,
        {{ clean_string('RECORD_URL') }}                AS RECORD_URL,
        {{ clean_string('STATUS') }}                    AS STATUS,
        {{ clean_string('TERMNAME') }}                  AS TERMNAME,
        {{ clean_string('TERMVALUE') }}                 AS TERMVALUE,
        {{ clean_string('TERRITORYID') }}               AS TERRITORYID,
        {{ clean_string('TERRITORYKEY') }}              AS TERRITORYKEY,
        {{ clean_string('TERRITORYNAME') }}             AS TERRITORYNAME,

        -- ===========================
        -- DISPLAY CONTACT DETAILS
        -- ===========================
        {{ clean_string('DISPLAYCONTACT_CELLPHONE') }}  AS DISPLAYCONTACT_CELLPHONE,
        {{ clean_string('DISPLAYCONTACT_COMPANYNAME') }} AS DISPLAYCONTACT_COMPANYNAME,
        {{ clean_string('DISPLAYCONTACT_CONTACTNAME') }} AS DISPLAYCONTACT_CONTACTNAME,
        {{ clean_string('DISPLAYCONTACT_EMAIL_1') }}     AS DISPLAYCONTACT_EMAIL_1,
        {{ clean_string('DISPLAYCONTACT_EMAIL_2') }}     AS DISPLAYCONTACT_EMAIL_2,
        {{ clean_string('DISPLAYCONTACT_FIRSTNAME') }}   AS DISPLAYCONTACT_FIRSTNAME,
        {{ clean_string('DISPLAYCONTACT_INITIAL') }}     AS DISPLAYCONTACT_INITIAL,
        {{ clean_string('DISPLAYCONTACT_LASTNAME') }}    AS DISPLAYCONTACT_LASTNAME,
        {{ clean_string('DISPLAYCONTACT_PHONE_1') }}     AS DISPLAYCONTACT_PHONE_1,
        {{ clean_string('DISPLAYCONTACT_PRINTAS') }}     AS DISPLAYCONTACT_PRINTAS,
        {{ clean_string('DISPLAYCONTACT_STATUS') }}      AS DISPLAYCONTACT_STATUS,

        -- ===========================
        -- ADDRESS
        -- ===========================
        {{ clean_string('DISPLAYCONTACT_MAILADDRESS_ADDRESS_1') }} AS MAILADDRESS_LINE1,
        {{ clean_string('DISPLAYCONTACT_MAILADDRESS_ADDRESS_2') }} AS MAILADDRESS_LINE2,
        {{ clean_string('DISPLAYCONTACT_MAILADDRESS_ADDRESS_3') }} AS MAILADDRESS_LINE3,
        {{ clean_string('DISPLAYCONTACT_MAILADDRESS_CITY') }}      AS MAILADDRESS_CITY,
        {{ clean_string('DISPLAYCONTACT_MAILADDRESS_STATE') }}     AS MAILADDRESS_STATE,
        {{ clean_string('DISPLAYCONTACT_MAILADDRESS_ZIP') }}       AS MAILADDRESS_ZIP,
        {{ clean_string('DISPLAYCONTACT_MAILADDRESS_COUNTRY') }}   AS MAILADDRESS_COUNTRY,
        {{ clean_string('DISPLAYCONTACT_MAILADDRESS_COUNTRYCODE') }} AS MAILADDRESS_COUNTRYCODE,

        -- ===========================
        -- BOOLEANS
        -- ===========================
        EMAILOPTIN                         AS EMAILOPTIN,
        ENABLEONLINEACHPAYMENT             AS ENABLEONLINEACHPAYMENT,
        ENABLEONLINECARDPAYMENT            AS ENABLEONLINECARDPAYMENT,
        DISPLAYCONTACT_TAXABLE             AS DISPLAYCONTACT_TAXABLE,
        DISPLAYCONTACT_VISIBLE             AS DISPLAYCONTACT_VISIBLE,
        ONETIME                            AS ONETIME,
        ONHOLD                             AS ONHOLD,

        -- ===========================
        -- NUMERICS
        -- ===========================
        TRY_CAST(TOTALDUE AS FLOAT)        AS TOTALDUE,
        {{ safe_integer('CREATEDBY', 38, 0) }} AS CREATEDBY,
        {{ safe_integer('MODIFIEDBY', 38, 0) }} AS MODIFIEDBY,

        -- ===========================
        -- DATES / TIMESTAMPS
        -- ===========================
        LAST_INVOICEDATE,
        LAST_STATEMENTDATE,
        WHENCREATED,
        WHENMODIFIED,

        -- ===========================
        -- SILVER / LOAD AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()               AS SILVER_LOAD_DATE,

        -- ===========================
        -- METADATA
        -- ===========================
        _INGESTION_TIMESTAMP,
        _SOURCE_OBJECT,
        _BATCH_ID

    FROM raw
)

SELECT *
FROM cleaned
