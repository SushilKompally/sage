/*
-- Description: Incremental Load Script for Silver Layer - item Table
-- Script Name: silver_item.sql
-- Created on: 05-Jan-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incrementally load and standardize Sage 'item' data from Bronze to Silver.
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
    FROM {{ source('sage_bronze', 'item') }}
    WHERE 1 = 1
    {{ incremental_filter() }}
),

cleaned AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        {{ safe_integer('RECORDNO', 38, 0) }}                     AS RECORDNO,

        -- ===========================
        -- STRINGS / IDENTIFIERS
        -- ===========================
        {{ clean_string('BASEUOM') }}                             AS BASEUOM,
        {{ clean_string('COST_METHOD') }}                         AS COST_METHOD,
        {{ clean_string('DEFAULT_CONVERSIONTYPE') }}              AS DEFAULT_CONVERSIONTYPE,
        {{ clean_string('DEFAULT_REPLENISHMENT_UOM') }}           AS DEFAULT_REPLENISHMENT_UOM,
        {{ clean_string('DEFCONTRACTDEFERRALSTATUS') }}            AS DEFCONTRACTDEFERRALSTATUS,
        {{ clean_string('DEFCONTRACTDELIVERYSTATUS') }}            AS DEFCONTRACTDELIVERYSTATUS,
        {{ clean_string('EXTENDED_DESCRIPTION') }}                AS EXTENDED_DESCRIPTION,
        {{ clean_string('GLGROUP') }}                             AS GLGROUP,
        {{ clean_string('ITEMID') }}                              AS ITEMID,
        {{ clean_string('ITEMTYPE') }}                            AS ITEMTYPE,
        {{ clean_string('NAME') }}                                AS NAME,
        {{ clean_string('NOTE') }}                                AS NOTE,
        {{ clean_string('PRODUCTLINEID') }}                       AS PRODUCTLINEID,
        {{ clean_string('PRODUCTTYPE') }}                         AS PRODUCTTYPE,
        {{ clean_string('RECORD_URL') }}                          AS RECORD_URL,
        {{ clean_string('REPLENISHMENT_METHOD') }}                AS REPLENISHMENT_METHOD,
        {{ clean_string('REVPRINTING') }}                         AS REVPRINTING,
        {{ clean_string('STATUS') }}                              AS STATUS,
        {{ clean_string('UOM_INVUOMDETAIL_UNIT') }}               AS UOM_INVUOMDETAIL_UNIT,
        {{ clean_string('UOM_POUOMDETAIL_UNIT') }}                AS UOM_POUOMDETAIL_UNIT,
        {{ clean_string('UOM_SOUOMDETAIL_UNIT') }}                AS UOM_SOUOMDETAIL_UNIT,
        {{ clean_string('UOMGRP') }}                              AS UOMGRP,
        {{ clean_string('VSOECATEGORY') }}                        AS VSOECATEGORY,
        {{ clean_string('VSOEDLVRSTATUS') }}                      AS VSOEDLVRSTATUS,
        {{ clean_string('VSOEREVDEFSTATUS') }}                    AS VSOEREVDEFSTATUS,

        -- ===========================
        -- NUMERICS / MEASURES
        -- ===========================
        BASEPRICE                                                 AS BASEPRICE,
        IONHAND                                                   AS IONHAND,
        IONORDER                                                  AS IONORDER,
        IUNCOMMITTED                                              AS IUNCOMMITTED,
        MAX_ORDER_QTY                                             AS MAX_ORDER_QTY,
        PRODUCTLINERECORDNO                                       AS PRODUCTLINERECORDNO,
        REORDER_POINT                                             AS REORDER_POINT,
        REORDER_QTY                                               AS REORDER_QTY,
        SAFETY_STOCK                                              AS SAFETY_STOCK,
        STANDARD_COST                                             AS STANDARD_COST,
        UOM_POUOMDETAIL_CONVFACTOR                                 AS UOM_POUOMDETAIL_CONVFACTOR,
        UOM_SOUOMDETAIL_CONVFACTOR                                 AS UOM_SOUOMDETAIL_CONVFACTOR,

        {{ safe_integer('CREATEDBY', 38, 0) }}                     AS CREATEDBY,
        {{ safe_integer('DEFAULTREVRECTEMPLKEY', 38, 0) }}         AS DEFAULTREVRECTEMPLKEY,
        {{ safe_integer('DEFERREDREVACCTKEY', 38, 0) }}             AS DEFERREDREVACCTKEY,
        {{ safe_integer('GLGRPKEY', 38, 0) }}                       AS GLGRPKEY,
        {{ safe_integer('ITEMGLGROUP_DEFRRGLACCOUNT_ACCTNO', 38, 0) }} AS ITEMGLGROUP_DEFRRGLACCOUNT_ACCTNO,
        {{ safe_integer('MODIFIEDBY', 38, 0) }}                    AS MODIFIEDBY,
        {{ safe_integer('UOMGRPKEY', 38, 0) }}                      AS UOMGRPKEY,

        -- ===========================
        -- BOOLEANS
        -- ===========================
        TRY_CAST(ALLOW_BACKORDER AS BOOLEAN)                        AS ALLOW_BACKORDER,
        TRY_CAST(ALLOWMULTIPLETAXGRPS AS BOOLEAN)                   AS ALLOWMULTIPLETAXGRPS,
        TRY_CAST(AUTOPRINTLABEL AS BOOLEAN)                         AS AUTOPRINTLABEL,
        TRY_CAST(BUYTOORDER AS BOOLEAN)                             AS BUYTOORDER,
        TRY_CAST(CNDEFAULTBUNDLE AS BOOLEAN)                        AS CNDEFAULTBUNDLE,
        TRY_CAST(COMPLIANTITEM AS BOOLEAN)                          AS COMPLIANTITEM,
        TRY_CAST(COMPUTEFORSHORTTERM AS BOOLEAN)                    AS COMPUTEFORSHORTTERM,
        TRY_CAST(CONTRACTENABLED AS BOOLEAN)                        AS CONTRACTENABLED,
        TRY_CAST(DROPSHIP AS BOOLEAN)                               AS DROPSHIP,
        TRY_CAST(ENABLE_REPLENISHMENT AS BOOLEAN)                   AS ENABLE_REPLENISHMENT,
        TRY_CAST(ENABLEFULFILLMENT AS BOOLEAN)                      AS ENABLEFULFILLMENT,
        TRY_CAST(ENABLELANDEDCOST AS BOOLEAN)                       AS ENABLELANDEDCOST,
        TRY_CAST(ENGINEERINGAPPROVAL AS BOOLEAN)                    AS ENGINEERINGAPPROVAL,
        TRY_CAST(GIFTCARD AS BOOLEAN)                               AS GIFTCARD,
        TRY_CAST(HASSTARTENDDATES AS BOOLEAN)                       AS HASSTARTENDDATES,
        TRY_CAST(ISSUPPLYITEM AS BOOLEAN)                           AS ISSUPPLYITEM,
        TRY_CAST(MRR AS BOOLEAN)                                    AS MRR,
        TRY_CAST(QUALITYCONTROLAPPROVAL AS BOOLEAN)                 AS QUALITYCONTROLAPPROVAL,
        TRY_CAST(RESTRICTEDITEM AS BOOLEAN)                         AS RESTRICTEDITEM,
        TRY_CAST(SAFETYITEM AS BOOLEAN)                             AS SAFETYITEM,
        TRY_CAST(SALESAPPROVAL AS BOOLEAN)                          AS SALESAPPROVAL,
        TRY_CAST(TAXABLE AS BOOLEAN)                                AS TAXABLE,
        TRY_CAST(WEBENABLED AS BOOLEAN)                             AS WEBENABLED,

        -- ===========================
        -- DATES / TIMESTAMPS
        -- ===========================
        WHENCREATED                                                AS WHENCREATED,
        WHENLASTRECEIVED                                           AS WHENLASTRECEIVED,
        WHENLASTSOLD                                               AS WHENLASTSOLD,
        WHENMODIFIED                                               AS WHENMODIFIED,

        -- ===========================
        -- SILVER LOAD AUDIT
        -- ===========================
        CURRENT_TIMESTAMP()                                        AS SILVER_LOAD_DATE,

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
