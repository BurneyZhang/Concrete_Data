-- create view for master table
-- for every unique DOCKET_ID, PROJECT_ID, ORDER_ID, keep the record with the latest INGESTION_TS
create or replace view "BYCN_IT_PRD"."DD_HK_DATA"."VW_ALLIANCE_DOCKETS_MASTER" as
select  * from (
    select *,
        row_number() over (partition by DOCKET_ID, PROJECT_ID, ORDER_ID order by INGESTION_TS desc) as rn   
    from "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER"
) where rn = 1;

-- create view for master table with partitioning by order date (for more efficient querying)
-- create or replace view "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER_VW_PARTITIONED" as
select  * from (
    select *,
        row_number() over (partition by DOCKET_ID, PROJECT_ID, ORDER_ID order by INGESTION_TS desc) as rn   
    from "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER"
) where rn = 1;




-- italk2.0 says this is no good
SELECT * FROM
    "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER"
QUALIFY ROW_NUMBER() OVER (PARTITION BY DOCKET_ID ORDER BY INGESTION_TS DESC) = 1;
