
<<<<<<< 
# Concrete-Data

=======
# DHK DIT Alliance Concrete Data Pipeline

Automated data pipeline for ingesting alliance docket JSON files from Azure Blob Storage into Snowflake, with change tracking and Power Automate orchestration.

## Architecture Overview

```
┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Alliance    │     │  Power Automate  │     │    Azure Blob   │
│  API         │────>│  (Hourly Fetch)  │────>│    Storage      │
│              │     │                  │     │  (tobe_ingested)│
└──────────────┘     └──────────────────┘     └────────┬────────┘
                                                       │
                                                       v
┌─────────────────────────────────────────────────────────────────────────────┐
│                                        Snowflake                            |
│       HK_DATA Stage (Azure Blob)                                            |
│        │                      |                                             |
│        │                      |                                             |
│        v                      v                                             │
│  ┌───────────────┐     ┌───────────────┐     ┌───────────────┐              │
│  │ RAW_ORDERS_   │────>│ ALLIANCE_     │────>│    Views      │              │
│  │ JSON          │     │ DOCKETS_MASTER│     │ • VW_MASTER   │              |
│  │ (Landing)     │     │               │     │ • VW_DIFFS    │              │
│  └───────────────┘     └───────────────┘     └───────────────┘              |
└─────────────────────────────────────────────────────────────────────────────┘
```

**Data Flow:**
1. **Power Automate** fetches docket data from Alliance API and saves JSON to Azure Blob Storage
2. **Snowflake COPY INTO** reads from Azure Blob stage into `RAW_ORDERS_JSON`
3. **Snowflake INSERT** parses JSON and upserts into `ALLIANCE_DOCKETS_MASTER`
4. **Views** provide deduplicated data and change detection

## Snowflake Objects

### Database & Schema
- **Database**: `BYCN_IT_PRD`
- **Schema**: `DD_HK_DATA`
- **Warehouse**: `DD_HK_WAREHOUSE`

### Tables

| Table | Purpose |
|-------|---------|
| `RAW_ORDERS_JSON` | Landing table for raw JSON payloads (VARIANT type) |
| `ALLIANCE_DOCKETS_MASTER` | Main master table with one row per docket |

### Stage

| Stage | Purpose |
|-------|---------|
| `HK_DATA` | Azure Blob Storage stage pointing to `azure://bycnbbidhk.blob.core.windows.net/hk-data` |

**Created with:**
```sql
CREATE STAGE HK_DATA 
    URL = 'azure://bycnbbidhk.blob.core.windows.net/hk-data' 
    CREDENTIALS = ( AZURE_SAS_TOKEN = '*****' ) 
    DIRECTORY = ( ENABLE = true );
```

#### ALLIANCE_DOCKETS_MASTER Schema

| Column | Type | Description |
|--------|------|-------------|
| `PROJECT_ID` | STRING | Project identifier |
| `ORDER_ID` | STRING | Order identifier |
| `LINE_ID` | STRING | Line item identifier |
| `ORDER_QUANTITY` | NUMBER(18,6) | Ordered quantity |
| `SITE_LAT` | NUMBER(18,9) | Site latitude |
| `SITE_LNG` | NUMBER(18,9) | Site longitude |
| `SITE_ID` | STRING | Site identifier |
| `SITE_NAME` | STRING | Site name |
| `DOCKET_COUNT` | NUMBER | Number of dockets |
| `DOCKET_ID` | STRING | Unique docket identifier |
| `ORDER_DATE` | TIMESTAMP_TZ | Order timestamp |
| `LOCATION` | STRING | Delivery location |
| `CONCRETE_GRADE` | STRING | Concrete grade specification |
| `MIX_ID` | STRING | Mix identifier |
| `TRUCK_NUMBER` | STRING | Truck number |
| `FLEET_NUMBER` | STRING | Fleet number |
| `DELIVERY_QUANTITY` | NUMBER(18,6) | Delivered quantity |
| `LOADED_TIME` | TIMESTAMP_TZ | Time loaded |
| `SITE_ARRIVAL_TIME` | TIMESTAMP_TZ | Time arrived at site |
| `START_DISCHARGE_TIME` | TIMESTAMP_TZ | Start discharge time |
| `FINISH_DISCHARGE_TIME` | TIMESTAMP_TZ | Finish discharge time |
| `COMPLIANCE` | STRING | Compliance status |
| `SITE_CONTACT` | STRING | Site contact person |
| `GRADE` | STRING | Grade classification |
| `RATIO_OPC` | NUMBER | OPC ratio |
| `RATIO_PFA` | NUMBER | PFA ratio |
| `RATIO_GGBS` | NUMBER | GGBS ratio |
| `RATIO_CSF` | NUMBER | CSF ratio |
| `PLANT_NAME` | STRING | Plant name |
| `CUM_TOTAL` | NUMBER(18,6) | Cumulative total |
| `CUSTOMER_ORDER_ID` | STRING | Customer order ID |
| `INGESTION_TS` | TIMESTAMP_TZ | When record was loaded |

**Primary Key Constraint:**
```sql
ALTER TABLE ALLIANCE_DOCKETS_MASTER
  ADD CONSTRAINT PK_ALLIANCE_DOCKETS_MASTER
  PRIMARY KEY (DOCKET_ID);
```

### Views

| View | Purpose |
|------|---------|
| `VW_ALLIANCE_DOCKETS_MASTER` | Deduplicated view (latest INGESTION_TS per DOCKET_ID/PROJECT_ID/ORDER_ID) |
| `VW_ALLIANCE_DOCKETS_DOCKET_ID_DIFFS` | Change detection across data versions |

#### VW_ALLIANCE_DOCKETS_MASTER
- Partitions by `DOCKET_ID`, `PROJECT_ID`, `ORDER_ID`
- Keeps only the record with the latest `INGESTION_TS`
- Uses `QUALIFY ROW_NUMBER()` for cleaner deduplication
- Use this view for all queries to avoid duplicates

#### VW_ALLIANCE_DOCKETS_DOCKET_ID_DIFFS
- Compares consecutive versions of each `DOCKET_ID`
- Identifies changes in any column except `DOCKET_ID`, `INGESTION_TS`, `VERSION_NUM`, `PREVIOUS_INGESTION_TS`, `ROW_OBJ`
- Shows: `DOCKET_ID`, version numbers, column name, old value, new value
- Useful for auditing data changes over time

### File Format
- `FF_ORDERS_JSON` - JSON file format for parsing

### Stage Storage
- **Stage**: `HK_DATA`
- **Source path**: `alliance_docket_jsons/tobe_ingested/`
- **Archive path**: `alliance_docket_jsons/ingested/`
- **File pattern**: `^alliance_data_[^_/]+_[^_/]+\.json$`

---

## Pipeline Scripts

### 1_data_ingestion.sql
Initial setup and one-time data loading.

**Operations:**
1. Sets session timezone to `Asia/Hong_Kong`
2. Creates `FF_ORDERS_JSON` file format
3. Creates `RAW_ORDERS_JSON` landing table
4. Loads JSON files via `COPY INTO` command
5. Creates `ALLIANCE_DOCKETS_MASTER` table

**Run once** for initial setup or re-setup.

### 2_move_stage_files.sql
File movement operations in Snowflake stage storage.

**Operations:**
1. `LIST` - Preview files matching the pattern
2. `COPY FILES` - Move files from `tobe_ingested` to `ingested/`
3. `REMOVE` - Delete original files after verification

**Use case:** Manual or automated file archival after processing.

### 3_task_24hours.sql
Scheduled 24-hour task for incremental data processing.

**Steps:**
1. **TRUNCATE** `RAW_ORDERS_JSON` landing table
2. **COPY INTO** `RAW_ORDERS_JSON` from stage (new files only)
3. **INSERT** parsed data into `ALLIANCE_DOCKETS_MASTER`
4. **Move processed files** from `tobe_ingested/` to `ingested/`

**JSON Parsing Logic:**
- Parses `RAW_PAYLOAD` VARIANT column
- Explodes the `orders` array
- Extracts nested fields using `:key::type` syntax
- Uses `TRY_TO_TIMESTAMP_NTZ` for safe timestamp parsing
- Uses `NULLIF` to convert sentinel timestamp `1900-01-01T00:00:00` to NULL

**Deduplication:**
- Uses `QUALIFY ROW_NUMBER()` to deduplicate on `(ORDER_ID, LINE_ID, DOCKET_ID)`
- Keeps latest record per natural key

**Timezone Handling:**
```sql
-- Set session timezone for predictable timestamps
alter session set timezone = 'Asia/Hong_Kong';
```

### 3_task_24hours.sql - Stored Procedure

The script includes a stored procedure `SP_INGEST_ALLIANCE_DOCKETS` for automated execution:

```sql
CREATE OR REPLACE PROCEDURE BYCN_IT_PRD.DD_HK_DATA."SP_INGEST_ALLIANCE_DOCKETS"()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
-- Step 1: Ingest raw JSON
-- Step 2: Parse and insert into master table
-- Step 3: Move processed files to ingested folder
$$;
```

**Usage:**
```sql
CALL BYCN_IT_PRD.DD_HK_DATA."SP_INGEST_ALLIANCE_DOCKETS"();
```

### 4_view_master_table.sql
Creates deduplication view for the master table.

```sql
-- Deduplication logic
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY DOCKET_ID, PROJECT_ID, ORDER_ID 
            ORDER BY INGESTION_TS DESC
        ) AS rn   
    FROM ALLIANCE_DOCKETS_MASTER
) WHERE rn = 1;
```

### 5_view_data_change_detector.sql
Creates view for tracking data changes across versions.

**Logic:**
1. Window function assigns version numbers per `DOCKET_ID`
2. `LAG()` retrieves previous `INGESTION_TS`
3. `OBJECT_CONSTRUCT_KEEP_NULL()` creates object representation
4. `LATERAL FLATTEN` explodes key-value pairs
5. Join current vs previous values to detect changes

---

## Power Automate Flow

Refer to [6_powerautomate.json.md](6_powerautomate.json.md) for detailed flow documentation.

### Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Scheduled   │────>│  Get Projects│────>│  API Call    │
│  (X days)    │     │  from Morta  │     │  to Alliance │
└──────────────┘     └──────────────┘     │  API         │
                                          └──────┬───────┘
                                                 │
                                                 v
                                          ┌──────────────┐
                                          │  Save JSON   │
                                          │  to Blob     │
                                          └──────┬───────┘
                                                 │
                                                 v
                                          ┌──────────────┐
                                          │  Teams       │
                                          │  Notification│
                                          └──────────────┘
```

### API Endpoint
```
POST https://dhk-apims.azure-api.net/alliance-api/v1/Docket/List
Content-Type: application/json
Ocp-Apim-Subscription-Key: <redacted>
```

### Request Body
```json
{
    "projectId": "9350000141",
    "orderDateFrom": "2026-04-02",
    "orderDateTo": "2026-04-02",
    "customerOrderID": ""
}
```

---

## Maintenance Guide

### Adding New Columns to Master Table

1. **Update table definition** in `1_data_ingestion.sql`:
```sql
ALTER TABLE ALLIANCE_DOCKETS_MASTER ADD COLUMN NEW_COLUMN STRING;
```

2. **Update INSERT statement** in `3_task_24hours.sql`:
```sql
, NEW_COLUMN
```

3. **Update both view definitions** in `4_view_master_table.sql` and `5_view_data_change_detector.sql` to include/exclude the new column as appropriate.

### Modifying JSON Parsing Logic

If the source JSON structure changes, update the `exploded` CTE in `3_task_24hours.sql`:

```sql
exploded AS (
    SELECT
        o.value:newFieldPath::STRING AS NEW_COLUMN,
        -- ... existing fields
    FROM parsed_raw
)
```

### Resetting the Pipeline

To re-ingest all data from scratch:

1. **Reset RAW_ORDERS_JSON:**
```sql
TRUNCATE TABLE "BYCN_IT_PRD"."DD_HK_DATA"."RAW_ORDERS_JSON";
```

2. **Reset ALLIANCE_DOCKETS_MASTER:**
```sql
TRUNCATE TABLE "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER";
```

3. **Restore files** from `ingested/` back to `tobe_ingested/` (manual step).

4. **Re-run** `3_task_24hours.sql`.

### Monitoring

1. **Check for new files:**
```sql
LIST @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/tobe_ingested
PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$';
```

2. **Check recent ingestion:**
```sql
SELECT MAX(INGESTION_TS), COUNT(*) 
FROM "BYCN_IT_PRD"."DD_HK_DATA"."ALLIANCE_DOCKETS_MASTER";
```

3. **View data changes:**
```sql
SELECT * FROM "BYCN_IT_PRD"."DD_HK_DATA"."VW_ALLIANCE_DOCKETS_DOCKET_ID_DIFFS"
ORDER BY DOCKET_ID, VERSION_NUM;
```

---

## Reusing for Other Purposes

### Adapting to New JSON Structure

1. **Analyze new JSON:**
```sql
SELECT RAW_PAYLOAD FROM RAW_ORDERS_JSON LIMIT 1;
```

2. **Update file pattern** if filename format changes:
```sql
PATTERN = '^new_path_prefix/your_file_[^_/]+_[^_/]+\.json$'
```

3. **Update COPY INTO** to extract correct field:
```sql
SELECT $1:yourNewRootField::VARCHAR
```

4. **Update exploded CTE** with new column mappings.

### Creating a New Pipeline

1. Copy `1_data_ingestion.sql` → rename → modify table schema
2. Copy `2_move_stage_files.sql` → update stage paths
3. Copy `3_task_24hours.sql` → update parsing logic
4. Copy `4_view_master_table.sql` → update partition keys
5. Copy `5_view_data_change_detector.sql` → adjust excluded columns

### Key Patterns

**Window Function Deduplication:**
```sql
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY key1, key2
            ORDER BY ts DESC
        ) AS rn
    FROM table_name
) WHERE rn = 1;
```

**Change Detection:**
```sql
WITH versioned AS (
    SELECT 
        t.*,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts) AS version_num,
        LAG(ts) OVER (PARTITION BY key ORDER BY ts) AS prev_ts
    FROM table_name t
),
curr_kv AS (
    SELECT v.key, v.version_num, f.key AS col, f.value AS val
    FROM versioned v, LATERAL FLATTEN(INPUT => OBJECT_CONSTRUCT_KEEP_NULL(v.*)) f
),
prev_kv AS (
    SELECT v.key, v.version_num + 1 AS version_num, f.key AS col, f.value AS val
    FROM versioned v, LATERAL FLATTEN(INPUT => OBJECT_CONSTRUCT_KEEP_NULL(v.*)) f
)
SELECT c.key, c.col, p.val AS old, c.val AS new
FROM curr_kv c
JOIN prev_kv p ON c.key = p.key AND c.version_num = p.version_num AND c.col = p.col
WHERE c.val != p.val OR (p.val IS NULL) != (c.val IS NULL);
```

---

## File Inventory

| File | Purpose |
|------|---------|
| `1_data_ingestion.sql` | Initial setup, table/stage creation, one-time load |
| `2_move_stage_files.sql` | Stage file movement operations |
| `3_task_24hours.sql` | Scheduled 24-hour ingestion task + stored procedure |
| `4_view_master_table.sql` | Deduplication view |
| `5_view_data_change_detector.sql` | Change detection view |
| `6_powerautomate.json` | Power Automate flow definition (exported JSON) |
| `6_powerautomate.json.md` | Power Automate flow documentation |
| `6_powerautomate_planning.md` | Power Automate design notes and decisions |
| `README.md` | Full pipeline documentation |
