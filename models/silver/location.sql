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
    unique_key = 'locationid',
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('sage_bronze', 'location') }}
    WHERE 1 = 1
    {{ incremental_filter() }}
),

cleaned AS (


SELECT
    addresscountrydefault,
    businessdays,
    close_date,

    contactinfo_contactname,
    contactinfo_firstname,
    contactinfo_lastname,
    contactinfo_mailaddress_address_1,
    contactinfo_mailaddress_city,
    contactinfo_mailaddress_country,
    contactinfo_mailaddress_countrycode,
    contactinfo_mailaddress_state,
    contactinfo_mailaddress_zip,
    contactinfo_printas,

    contactkey,
    cpm,
    createdby,
    createdbyloginid,
    custtitle,
    district_manager,

    entity,
    federalid,
    firstmonth,
    firstmonthtax,
    has_ie_relation,
    isroot,

    location_cohort_open_date,
    location_open_date,
    locationid,
    locationtype,
    model_type,

    modifiedby,
    modifiedbyloginid,

    name,
    parentid,
    parentkey,
    parentname,

    project_type,
    record_url,
    recordno,
    reportprintas,
    revenue_stream,

    site_status,
    site_type,
    sort_order,
    stage,
    startdate,
    status,
    taxid,
    weekends,

    whencreated,
    whenmodified,
    current_timestamp()::timestamp_ntz                             as silver_load_date

    FROM raw
)

SELECT *
FROM cleaned
