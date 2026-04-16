-- 1) Optional: preview the files that will be moved

LIST @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"
PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$';
LIST @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/tobe_ingested
PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$';
;
-- 2) Copy matching files to the ingested folder
COPY FILES
    INTO @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/ingested/
    FROM @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/tobe_ingested/
    PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$'
    DETAILED_OUTPUT = TRUE;

-- 3) After verifying the COPY FILES output, remove the originals
REMOVE @"BYCN_IT_PRD"."DD_HK_DATA"."HK_DATA"/alliance_docket_jsons/tobe_ingested/
    PATTERN = '^alliance_docket_jsons/tobe_ingested/alliance_data_[^_/]+_[^_/]+\.json$';
