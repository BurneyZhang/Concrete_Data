-- =========================================================
-- 0) Context
-- =========================================================
use warehouse DD_HK_WAREHOUSE;
use database BYCN_IT_PRD;
use schema DD_HK_DATA;

-- Optional: make timestamps predictable
alter session set timezone = 'Asia/Hong_Kong';

-- =========================================================
-- 1) File format for JSON
--    Do NOT use STRIP_OUTER_ARRAY=TRUE because your top-level
--    JSON is an OBJECT, not an ARRAY.
-- =========================================================
create or replace file format FF_ORDERS_JSON
  type = json;

-- =========================================================
-- 2) Raw landing table
--    Store original payload in VARIANT for traceability.
-- =========================================================
create or replace table RAW_ORDERS_JSON (
    RAW_PAYLOAD        variant
    -- SOURCE_FILENAME    string,
    -- SOURCE_ROW_NUMBER  number,
    -- RAW_LOAD_TS        timestamp_ntz default current_timestamp()
);

-- =========================================================
-- 3) Load ALL JSON files from the source stage folder
--    Strict filename pattern:
--    alliance_data_<some strings>.json
-- =========================================================
TRUNCATE TABLE "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON";
COPY INTO "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON"
FROM (
    SELECT $1:orders::VARCHAR
    FROM '@"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"'
)
-- BYCN_IT_PRD.DD_HK_DATA.HK_DATA/bycnbbidhk.blob.core.windows.net/hk-data/alliance_docket_jsons/tobe_ingested/
-- FILES = ('alliance_docket_jsons/tobe_ingested/alliance_data_1201_20260319.json')
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
-- For more details, see: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

select * from RAW_ORDERS_JSON limit 10;
-- =========================================================
-- 4) Main master table
--    Grain = one row per docket
--    ingestion_ts = time when record is loaded into master table
-- =========================================================
create or replace table ALLIANCE_DOCKETS_MASTER (
    PROJECT_ID              string,
    ORDER_ID                string,
    LINE_ID                 string,
    ORDER_QUANTITY          number(18,6),

    SITE_LAT                number(18,9),
    SITE_LNG                number(18,9),
    SITE_ID                 string,
    SITE_NAME               string,

    DOCKET_COUNT            number,

    DOCKET_ID               string,
    ORDER_DATE              timestamp_ntz,
    LOCATION                string,
    CONCRETE_GRADE          string,
    MIX_ID                  string,
    TRUCK_NUMBER            string,
    FLEET_NUMBER            string,
    DELIVERY_QUANTITY       number(18,6),
    LOADED_TIME             timestamp_ntz,
    SITE_ARRIVAL_TIME       timestamp_ntz,
    START_DISCHARGE_TIME    timestamp_ntz,
    FINISH_DISCHARGE_TIME   timestamp_ntz,
    COMPLIANCE              string,
    SITE_CONTACT            string,
    GRADE                   string,
    RATIO_OPC               number,
    RATIO_PFA               number,
    RATIO_GGBS              number,
    RATIO_CSF               number,
    PLANT_NAME              string,
    CUM_TOTAL               number(18,6),
    CUSTOMER_ORDER_ID       string,

    -- SOURCE_FILENAME         string,
    -- SOURCE_ROW_NUMBER       number,
    -- RAW_LOAD_TS             timestamp_ntz,

    INGESTION_TS            timestamp_ntz not null
);

-- Optional but recommended:
-- if your natural business key is unique at docket level,
-- you can enforce via a comment/constraint pattern.
-- Snowflake doesn't enforce PK constraints, but documenting helps.
alter table ALLIANCE_DOCKETS_MASTER
  add constraint PK_ALLIANCE_DOCKETS_MASTER
--   primary key (ORDER_ID, LINE_ID, DOCKET_ID);
  primary key ( DOCKET_ID);




insert into ALLIANCE_DOCKETS_MASTER (ORDER_ID, LINE_ID, DOCKET_ID, INGESTION_TS)
values ('TEST_ORDER', 'TEST_LINE', 'TEST_DOCKET', current_timestamp());
-- =========================================================
-- 5) Flatten JSON and MERGE into master table
--    - Expand top-level orders array
--    - Expand nested dockets array
--    - Convert sentinel timestamps 1900-01-01T00:00:00 to NULL
--    - Stamp ingestion_ts at load time
-- =========================================================


-- OPTION 1 check for duplicates then merge
    merge into "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER" tgt
    using (
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
                try_to_timestamp_ntz(d.value:siteArrivalTime::string)          as SITE_ARRIVAL_TIME,

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
        select * from deduped
    ) src
    on  tgt.ORDER_ID  = src.ORDER_ID
    and tgt.LINE_ID   = src.LINE_ID
    and tgt.DOCKET_ID = src.DOCKET_ID

    when matched then update set
        tgt.PROJECT_ID            = src.PROJECT_ID,
        tgt.ORDER_QUANTITY        = src.ORDER_QUANTITY,
        tgt.SITE_LAT              = src.SITE_LAT,
        tgt.SITE_LNG              = src.SITE_LNG,
        tgt.SITE_ID               = src.SITE_ID,
        tgt.SITE_NAME             = src.SITE_NAME,
        tgt.DOCKET_COUNT          = src.DOCKET_COUNT,
        tgt.ORDER_DATE            = src.ORDER_DATE,
        tgt.LOCATION              = src.LOCATION,
        tgt.CONCRETE_GRADE        = src.CONCRETE_GRADE,
        tgt.MIX_ID                = src.MIX_ID,
        tgt.TRUCK_NUMBER          = src.TRUCK_NUMBER,
        tgt.FLEET_NUMBER          = src.FLEET_NUMBER,
        tgt.DELIVERY_QUANTITY     = src.DELIVERY_QUANTITY,
        tgt.LOADED_TIME           = src.LOADED_TIME,
        tgt.SITE_ARRIVAL_TIME     = src.SITE_ARRIVAL_TIME,
        tgt.START_DISCHARGE_TIME  = src.START_DISCHARGE_TIME,
        tgt.FINISH_DISCHARGE_TIME = src.FINISH_DISCHARGE_TIME,
        tgt.COMPLIANCE            = src.COMPLIANCE,
        tgt.SITE_CONTACT          = src.SITE_CONTACT,
        tgt.GRADE                 = src.GRADE,
        tgt.RATIO_OPC             = src.RATIO_OPC,
        tgt.RATIO_PFA             = src.RATIO_PFA,
        tgt.RATIO_GGBS            = src.RATIO_GGBS,
        tgt.RATIO_CSF             = src.RATIO_CSF,
        tgt.PLANT_NAME            = src.PLANT_NAME,
        tgt.CUM_TOTAL             = src.CUM_TOTAL,
        tgt.CUSTOMER_ORDER_ID     = src.CUSTOMER_ORDER_ID,
        tgt.INGESTION_TS          = src.INGESTION_TS

    when not matched then insert (
    -- insert (
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
    ) values (
        src.PROJECT_ID,
        src.ORDER_ID,
        src.LINE_ID,
        src.ORDER_QUANTITY,
        src.SITE_LAT,
        src.SITE_LNG,
        src.SITE_ID,
        src.SITE_NAME,
        src.DOCKET_COUNT,
        src.DOCKET_ID,
        src.ORDER_DATE,
        src.LOCATION,
        src.CONCRETE_GRADE,
        src.MIX_ID,
        src.TRUCK_NUMBER,
        src.FLEET_NUMBER,
        src.DELIVERY_QUANTITY,
        src.LOADED_TIME,
        src.SITE_ARRIVAL_TIME,
        src.START_DISCHARGE_TIME,
        src.FINISH_DISCHARGE_TIME,
        src.COMPLIANCE,
        src.SITE_CONTACT,
        src.GRADE,
        src.RATIO_OPC,
        src.RATIO_PFA,
        src.RATIO_GGBS,
        src.RATIO_CSF,
        src.PLANT_NAME,
        src.CUM_TOTAL,
        src.CUSTOMER_ORDER_ID,
        src.INGESTION_TS
    );


-- OPTION 2 append directly from parsed_raw without checking duplicates
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
            try_to_timestamp_ntz(d.value:siteArrivalTime::string)          as SITE_ARRIVAL_TIME,

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
    )

    select * from exploded;

-- OPTION 3 check remove duplicates in from parsed "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON". insert directly into master table without checking 
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
            try_to_timestamp_ntz(d.value:siteArrivalTime::string)          as SITE_ARRIVAL_TIME,

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


-- =========================================================
-- 6) Validation queries
-- =========================================================
select * from BYCN_IT_PRD.DD_HK_DATA.ALLIANCE_DOCKETS_MASTER limit 1000;
select count(*) from BYCN_IT_PRD.DD_HK_DATA.ALLIANCE_DOCKETS_MASTER;
truncate table BYCN_IT_PRD.DD_HK_DATA.ALLIANCE_DOCKETS_MASTER;
select count(distinct(DOCKET_ID)) from BYCN_IT_PRD.DD_HK_DATA.ALLIANCE_DOCKETS_MASTER;
select * from BYCN_IT_PRD.DD_HK_DATA.RAW_ORDERS_JSON;