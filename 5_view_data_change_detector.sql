create or replace view "BYCN_IT_PRD"."DD_HK_DATA"."VW_ALLIANCE_DOCKETS_DOCKET_ID_DIFFS" as
with hashed as (
    select
        "DOCKET_ID",
        hash(* exclude ("DOCKET_ID", "INGESTION_TS")) as row_hash
    from "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER"
),
grouped as (
    select
        "DOCKET_ID",
        count(*) as row_count,
        count(distinct row_hash) as distinct_non_ingestion_versions
    from hashed
    group by "DOCKET_ID"
)
select
    "DOCKET_ID",
    row_count,
    distinct_non_ingestion_versions
from grouped
where row_count > 1
  and distinct_non_ingestion_versions > 1;

  select * from "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER"
  where "DOCKET_ID" in (select "DOCKET_ID" from "BYCN_IT_PRD"."DD_HK_DATA"."VW_ALLIANCE_DOCKETS_DOCKET_ID_DIFFS");

select 
    hash(* exclude ("DOCKET_ID", "INGESTION_TS", "DOCKET_COUNT")) as row_hash, 
    DOCKET_COUNT
from "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER"
where "DOCKET_ID" = '82639562';

create or replace view "BYCN_IT_PRD"."DD_HK_DATA"."VW_ALLIANCE_DOCKETS_DOCKET_ID_DIFFS" as
with versioned as (
    select
        t.*,
        row_number() over (
            partition by t."DOCKET_ID"
            order by t."INGESTION_TS"
        ) as version_num,
        lag(t."INGESTION_TS") over (
            partition by t."DOCKET_ID"
            order by t."INGESTION_TS"
        ) as previous_ingestion_ts
    from "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER" t
),
versioned_with_obj as (
    select
        v.*,
        object_construct_keep_null(*) as row_obj
    from versioned v
),
curr_kv as (
    select
        v."DOCKET_ID",
        v.version_num,
        v.previous_ingestion_ts,
        v."INGESTION_TS" as current_ingestion_ts,
        f.key as column_name,
        f.value as current_value
    from versioned_with_obj v,
         lateral flatten(input => v.row_obj) f
    where f.key not in (
        'DOCKET_ID',
        'INGESTION_TS',
        'VERSION_NUM',
        'PREVIOUS_INGESTION_TS',
        'ROW_OBJ'
    )
),
prev_kv as (
    select
        v."DOCKET_ID",
        v.version_num + 1 as version_num,
        f.key as column_name,
        f.value as previous_value
    from versioned_with_obj v,
         lateral flatten(input => v.row_obj) f
    where f.key not in (
        'DOCKET_ID',
        'INGESTION_TS',
        'VERSION_NUM',
        'PREVIOUS_INGESTION_TS',
        'ROW_OBJ'
    )
)
select
    c."DOCKET_ID",
    c.version_num as current_version_num,
    c.previous_ingestion_ts,
    c.current_ingestion_ts,
    c.column_name,
    p.previous_value::string as old_value,
    c.current_value::string as new_value
from curr_kv c
join prev_kv p
    on c."DOCKET_ID" = p."DOCKET_ID"
   and c.version_num = p.version_num
   and c.column_name = p.column_name
where c.version_num > 1
  and (
        (p.previous_value is null and c.current_value is not null)
     or (p.previous_value is not null and c.current_value is null)
     or (p.previous_value != c.current_value)
  )
order by c."DOCKET_ID", c.version_num, c.column_name;