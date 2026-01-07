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

select
    {{ clean_string_lower('addresscountrydefault') }}              as addresscountrydefault,
    {{ clean_string_lower('businessdays') }}                       as businessdays,
    {{ safe_date('close_date') }}                                  as close_date,

    {{ clean_string_lower('contactinfo_contactname') }}            as contactinfo_contactname,
    {{ clean_string_lower('contactinfo_firstname') }}              as contactinfo_firstname,
    {{ clean_string_lower('contactinfo_lastname') }}               as contactinfo_lastname,
    {{ clean_string_lower('contactinfo_mailaddress_address_1') }}  as contactinfo_mailaddress_address_1,
    {{ clean_string_lower('contactinfo_mailaddress_city') }}       as contactinfo_mailaddress_city,
    {{ clean_string_lower('contactinfo_mailaddress_country') }}    as contactinfo_mailaddress_country,
    {{ clean_string_lower('contactinfo_mailaddress_countrycode') }}as contactinfo_mailaddress_countrycode,
    {{ clean_string_lower('contactinfo_mailaddress_state') }}      as contactinfo_mailaddress_state,
    {{ safe_integer('contactinfo_mailaddress_zip') }}              as contactinfo_mailaddress_zip,
    {{ clean_string_lower('contactinfo_printas') }}                as contactinfo_printas,

    {{ safe_integer('contactkey') }}                               as contactkey,
    {{ clean_string_lower('cpm') }}                                as cpm,
    {{ safe_integer('createdby') }}                                as createdby,
    {{ clean_string_lower('createdbyloginid') }}                   as createdbyloginid,
    {{ clean_string_lower('custtitle') }}                          as custtitle,
    {{ clean_string_lower('district_manager') }}                   as district_manager,

    {{ clean_string_lower('entity') }}                             as entity,
    {{ clean_string_lower('federalid') }}                          as federalid,
    {{ safe_integer('firstmonth') }}                               as firstmonth,
    {{ safe_integer('firstmonthtax') }}                            as firstmonthtax,
    {{ safe_integer('has_ie_relation') }}                          as has_ie_relation,
    {{ safe_integer('isroot') }}                                   as isroot,

    {{ safe_date('location_cohort_open_date') }}                  as location_cohort_open_date,
    {{ safe_date('location_open_date') }}                         as location_open_date,
    {{ clean_string_lower('locationid') }}                        as locationid,
    {{ clean_string_lower('locationtype') }}                      as locationtype,
    {{ clean_string_lower('model_type') }}                        as model_type,

    {{ safe_integer('modifiedby') }}                               as modifiedby,
    {{ clean_string_lower('modifiedbyloginid') }}                 as modifiedbyloginid,

    {{ clean_string_lower('name') }}                               as name,
    {{ clean_string_lower('parentid') }}                           as parentid,
    {{ safe_integer('parentkey') }}                                as parentkey,
    {{ clean_string_lower('parentname') }}                         as parentname,

    {{ clean_string_lower('project_type') }}                      as project_type,
    {{ clean_string_lower('record_url') }}                        as record_url,
    {{ safe_integer('recordno') }}                                 as recordno,
    {{ clean_string_lower('reportprintas') }}                     as reportprintas,
    {{ clean_string_lower('revenue_stream') }}                    as revenue_stream,

    {{ clean_string_lower('site_status') }}                       as site_status,
    {{ clean_string_lower('site_type') }}                         as site_type,
    {{ safe_integer('sort_order') }}                               as sort_order,
    {{ clean_string_lower('stage') }}                              as stage,
    {{ safe_date('startdate') }}                                  as startdate,
    {{ clean_string_lower('status') }}                             as status,
    {{ clean_string_lower('taxid') }}                              as taxid,
    {{ clean_string_lower('weekends') }}                           as weekends,

    {{ safe_timestamp_ntz('whencreated') }}                       as whencreated,
    {{ safe_timestamp_ntz('whenmodified') }}                      as whenmodified,

    current_timestamp()::timestamp_ntz                             as silver_load_date

    FROM raw
)

SELECT *
FROM cleaned
