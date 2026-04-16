-- run the following snowflake SQL script every 24 hours to ingest new alliance docket json files, 
-- parse and insert into master table, and 
-- move processed files to ingested folder in stage

select current_timestamp();

SELECT
    CURRENT_TIMESTAMP() AS local_ts,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS utc_ts
    CONVERT_TIMEZONE('Asia/Hong_Kong', CURRENT_TIMESTAMP()) AS hk_ts;

SELECT 
    CURRENT_TIMESTAMP() AS local_ts,
    TRY_TO_TIMESTAMP_TZ('2026-03-19T09:41:43.287' || '+08:00') as hk_ts,
    CONVERT_TIMEZONE('UTC', hk_ts) AS utc_ts;


--script
    -- step 1: ingest raw json into RAW_ORDERS_JSON table
    TRUNCATE TABLE "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON";
    COPY INTO "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON"
    FROM (
        SELECT $1:orders::VARCHAR
        FROM '@"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"'
    )

    PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$'
    FILE_FORMAT = (
        TYPE=JSON,
        STRIP_OUTER_ARRAY=TRUE,
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;

    -- step 2: parse the raw json and insert into the master table
    insert into "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER" (
        PROJECT_ID,
        ORDER_ID,
        LINE_ID,
        ORDER_QUANTITY,
        SITE_LAT,
        SITE_LNG,
        SITE_ID,
        SITE_NAME,
        DOCKET_COUNT,
        DOCKET_ID,
        ORDER_DATE,
        LOCATION,
        CONCRETE_GRADE,
        MIX_ID,
        TRUCK_NUMBER,
        FLEET_NUMBER,
        DELIVERY_QUANTITY,
        LOADED_TIME,
        SITE_ARRIVAL_TIME,
        START_DISCHARGE_TIME,
        FINISH_DISCHARGE_TIME,
        COMPLIANCE,
        SITE_CONTACT,
        GRADE,
        RATIO_OPC,
        RATIO_PFA,
        RATIO_GGBS,
        RATIO_CSF,
        PLANT_NAME,
        CUM_TOTAL,
        CUSTOMER_ORDER_ID,
        INGESTION_TS
    )
    with parsed_raw as (
        select
            parse_json(RAW_PAYLOAD) as orders_arr
        from "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON"
        where RAW_PAYLOAD is not null
    ),
    exploded as (
        select
            o.value:projectId::string                                      as PROJECT_ID,
            o.value:orderId::string                                        as ORDER_ID,
            o.value:lineId::string                                         as LINE_ID,
            o.value:quantity::number(18,6)                                 as ORDER_QUANTITY,

            o.value:siteAddress.lat::number(18,9)                          as SITE_LAT,
            o.value:siteAddress.lng::number(18,9)                          as SITE_LNG,
            o.value:siteAddress.siteId::string                             as SITE_ID,
            o.value:siteAddress.siteName::string                           as SITE_NAME,

            o.value:docketCount::number                                    as DOCKET_COUNT,

            d.value:docketId::string                                       as DOCKET_ID,
            try_to_timestamp_ntz(d.value:orderDate::string)                as ORDER_DATE,
            d.value:location::string                                       as LOCATION,
            d.value:concreteGrade::string                                  as CONCRETE_GRADE,
            d.value:mixId::string                                          as MIX_ID,
            d.value:truckNumber::string                                    as TRUCK_NUMBER,
            d.value:fleetNumber::string                                    as FLEET_NUMBER,
            d.value:deliveryQuantity::number(18,6)                         as DELIVERY_QUANTITY,

            try_to_timestamp_ntz(d.value:loadedTime::string)               as LOADED_TIME,
            -- try_to_timestamp_ntz(d.value:siteArrivalTime::string)          as SITE_ARRIVAL_TIME,
            try_to_timestamp_ntz(nullif(d.value:siteArrivalTime::string, '1900-01-01T00:00:00'))          
                                                                        as SITE_ARRIVAL_TIME,
            try_to_timestamp_ntz(nullif(d.value:startDischargeTime::string, '1900-01-01T00:00:00'))
                                                                        as START_DISCHARGE_TIME,
            try_to_timestamp_ntz(nullif(d.value:finishDischargeTime::string, '1900-01-01T00:00:00'))
                                                                        as FINISH_DISCHARGE_TIME,

            d.value:compliance::string                                     as COMPLIANCE,
            d.value:siteContact::string                                    as SITE_CONTACT,
            d.value:grade::string                                          as GRADE,
            d.value:ratioOPC::number                                       as RATIO_OPC,
            d.value:ratioPFA::number                                       as RATIO_PFA,
            d.value:ratioGGBS::number                                      as RATIO_GGBS,
            d.value:ratioCSF::number                                       as RATIO_CSF,
            d.value:plantName::string                                      as PLANT_NAME,
            d.value:cumTotal::number(18,6)                                 as CUM_TOTAL,
            d.value:customerOrderID::string                                as CUSTOMER_ORDER_ID,

            current_timestamp()                                            as INGESTION_TS
        from parsed_raw r,
            lateral flatten(input => r.orders_arr) o,
            lateral flatten(input => o.value:dockets) d
    ),
    deduped as (
        select *
        from exploded
        qualify row_number() over (
            partition by ORDER_ID, LINE_ID, DOCKET_ID
            order by INGESTION_TS desc
        ) = 1
    )
    select
        PROJECT_ID,
        ORDER_ID,
        LINE_ID,
        ORDER_QUANTITY,
        SITE_LAT,
        SITE_LNG,
        SITE_ID,
        SITE_NAME,
        DOCKET_COUNT,
        DOCKET_ID,
        ORDER_DATE,
        LOCATION,
        CONCRETE_GRADE,
        MIX_ID,
        TRUCK_NUMBER,
        FLEET_NUMBER,
        DELIVERY_QUANTITY,
        LOADED_TIME,
        SITE_ARRIVAL_TIME,
        START_DISCHARGE_TIME,
        FINISH_DISCHARGE_TIME,
        COMPLIANCE,
        SITE_CONTACT,
        GRADE,
        RATIO_OPC,
        RATIO_PFA,
        RATIO_GGBS,
        RATIO_CSF,
        PLANT_NAME,
        CUM_TOTAL,
        CUSTOMER_ORDER_ID,
        INGESTION_TS
    from deduped;

    -- step 3: clear RAW_ORDERS_JSON and move processed files to ingested folder in stage
    TRUNCATE TABLE "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON";
    COPY FILES
        INTO @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/ingested/
        FROM @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/tobe_ingested/
        PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$'
        DETAILED_OUTPUT = TRUE;
    REMOVE @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/tobe_ingested/
        PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$';


CREATE OR REPLACE PROCEDURE BYCN_IT_PRD.DD_HK_DATA."SP_INGEST_ALLIANCE_DOCKETS_TZ"()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$

BEGIN
-- step 1: ingest raw json into RAW_ORDERS_JSON table
    TRUNCATE TABLE "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON";
    COPY INTO "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON"
    FROM (
        SELECT $1:orders::VARCHAR
        FROM '@"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"'
    )

    PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$'
    FILE_FORMAT = (
        TYPE=JSON,
        STRIP_OUTER_ARRAY=TRUE,
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=ABORT_STATEMENT;

-- step 2: parse the raw json and insert into the master table
    insert into "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER" (
        PROJECT_ID,
        ORDER_ID,
        LINE_ID,
        ORDER_QUANTITY,
        SITE_LAT,
        SITE_LNG,
        SITE_ID,
        SITE_NAME,
        DOCKET_COUNT,
        DOCKET_ID,
        ORDER_DATE,
        LOCATION,
        CONCRETE_GRADE,
        MIX_ID,
        TRUCK_NUMBER,
        FLEET_NUMBER,
        DELIVERY_QUANTITY,
        LOADED_TIME,
        SITE_ARRIVAL_TIME,
        START_DISCHARGE_TIME,
        FINISH_DISCHARGE_TIME,
        COMPLIANCE,
        SITE_CONTACT,
        GRADE,
        RATIO_OPC,
        RATIO_PFA,
        RATIO_GGBS,
        RATIO_CSF,
        PLANT_NAME,
        CUM_TOTAL,
        CUSTOMER_ORDER_ID,
        INGESTION_TS
    )
    with parsed_raw as (
        select
            parse_json(RAW_PAYLOAD) as orders_arr
        from "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON"
        where RAW_PAYLOAD is not null
    ),
    exploded as (
        select
            o.value:projectId::string                                      as PROJECT_ID,
            o.value:orderId::string                                        as ORDER_ID,
            o.value:lineId::string                                         as LINE_ID,
            o.value:quantity::number(18,6)                                 as ORDER_QUANTITY,

            o.value:siteAddress.lat::number(18,9)                          as SITE_LAT,
            o.value:siteAddress.lng::number(18,9)                          as SITE_LNG,
            o.value:siteAddress.siteId::string                             as SITE_ID,
            o.value:siteAddress.siteName::string                           as SITE_NAME,

            o.value:docketCount::number                                    as DOCKET_COUNT,

            d.value:docketId::string                                       as DOCKET_ID,
            try_to_timestamp_ntz(d.value:orderDate::string || '+08:00')    as ORDER_DATE,
            d.value:location::string                                       as LOCATION,
            d.value:concreteGrade::string                                  as CONCRETE_GRADE,
            d.value:mixId::string                                          as MIX_ID,
            d.value:truckNumber::string                                    as TRUCK_NUMBER,
            d.value:fleetNumber::string                                    as FLEET_NUMBER,
            d.value:deliveryQuantity::number(18,6)                         as DELIVERY_QUANTITY,



            try_to_timestamp_ntz(d.value:loadedTime::string  || '+08:00')  as LOADED_TIME,
            try_to_timestamp_ntz(d.value:siteArrivalTime::string || '+08:00')
                                                                        as SITE_ARRIVAL_TIME,
            try_to_timestamp_ntz(nullif(d.value:startDischargeTime::string || '+08:00', '1900-01-01T00:00:00+08:00'))
                                                                        as START_DISCHARGE_TIME,
            try_to_timestamp_ntz(nullif(d.value:finishDischargeTime::string || '+08:00', '1900-01-01T00:00:00+08:00'))
                                                                        as FINISH_DISCHARGE_TIME,

            d.value:compliance::string                                     as COMPLIANCE,
            d.value:siteContact::string                                    as SITE_CONTACT,
            d.value:grade::string                                          as GRADE,
            d.value:ratioOPC::number                                       as RATIO_OPC,
            d.value:ratioPFA::number                                       as RATIO_PFA,
            d.value:ratioGGBS::number                                      as RATIO_GGBS,
            d.value:ratioCSF::number                                       as RATIO_CSF,
            d.value:plantName::string                                      as PLANT_NAME,
            d.value:cumTotal::number(18,6)                                 as CUM_TOTAL,
            d.value:customerOrderID::string                                as CUSTOMER_ORDER_ID,

            -- current_timestamp()                                            as INGESTION_TS
            CONVERT_TIMEZONE('Asia/Hong_Kong', CURRENT_TIMESTAMP())                    as INGESTION_TS

        from parsed_raw r,
            lateral flatten(input => r.orders_arr) o,
            lateral flatten(input => o.value:dockets) d
    ),
    deduped as (
        select *
        from exploded
        qualify row_number() over (
            partition by ORDER_ID, LINE_ID, DOCKET_ID
            order by INGESTION_TS desc
        ) = 1
    )
    select
        PROJECT_ID,
        ORDER_ID,
        LINE_ID,
        ORDER_QUANTITY,
        SITE_LAT,
        SITE_LNG,
        SITE_ID,
        SITE_NAME,
        DOCKET_COUNT,
        DOCKET_ID,
        ORDER_DATE,
        LOCATION,
        CONCRETE_GRADE,
        MIX_ID,
        TRUCK_NUMBER,
        FLEET_NUMBER,
        DELIVERY_QUANTITY,
        LOADED_TIME,
        SITE_ARRIVAL_TIME,
        START_DISCHARGE_TIME,
        FINISH_DISCHARGE_TIME,
        COMPLIANCE,
        SITE_CONTACT,
        GRADE,
        RATIO_OPC,
        RATIO_PFA,
        RATIO_GGBS,
        RATIO_CSF,
        PLANT_NAME,
        CUM_TOTAL,
        CUSTOMER_ORDER_ID,
        INGESTION_TS
    from deduped;

-- step 3: clear RAW_ORDERS_JSON and move processed files to ingested folder in stage
    TRUNCATE TABLE "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON";
    COPY FILES
        INTO @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/ingested/
        FROM @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/tobe_ingested/
        PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$'
        DETAILED_OUTPUT = TRUE;
    REMOVE @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/tobe_ingested/
        PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$';
    
    RETURN 'Alliance docket ingestion completed successfully';

END;
$$;
    
    
call "BYCN_IT_PRD"."DD_HK_DATA"."SP_INGEST_ALLIANCE_DOCKETS"();    

select count(distinct DOCKET_ID) from "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER";

CREATE OR REPLACE TASK "BYCN_IT_PRD"."DD_HK_DATA"."TASK_INGEST_ALLIANCE_DOCKETS"
    WAREHOUSE = DD_HK_WAREHOUSE
    SCHEDULE = 'USING CRON 14 10 * * * UTC'
    COMMENT = 'Runs every day at 04:00 HKT to ingest alliance docket JSON files'
AS
    CALL "BYCN_IT_PRD"."DD_HK_DATA"."SP_INGEST_ALLIANCE_DOCKETS"();
--enable the task
ALTER TASK "BYCN_IT_PRD"."DD_HK_DATA"."TASK_INGEST_ALLIANCE_DOCKETS" RESUME;


-- Manually run the task for testing
CALL "BYCN_IT_PRD"."DD_HK_DATA"."SP_INGEST_ALLIANCE_DOCKETS"();

truncate "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER";

CALL "BYCN_IT_PRD"."DD_HK_DATA"."SP_INGEST_ALLIANCE_DOCKETS_TZ"();