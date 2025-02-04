

      create or replace transient table "AIRBYTE_DATABASE".TEST_NORMALIZATION."DEDUP_EXCHANGE_RATE_SCD"  as
      (
-- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
select
    ID,
    CURRENCY,
    DATE,
    TIMESTAMP_COL,
    "HKD@spéçiäl & characters",
    HKD_SPECIAL___CHARACTERS,
    NZD,
    USD,
  DATE as _AIRBYTE_START_AT,
  lag(DATE) over (
    partition by ID, CURRENCY, cast(NZD as 
    varchar
)
    order by DATE is null asc, DATE desc, _AIRBYTE_EMITTED_AT desc
  ) as _AIRBYTE_END_AT,
  case when lag(DATE) over (
    partition by ID, CURRENCY, cast(NZD as 
    varchar
)
    order by DATE is null asc, DATE desc, _AIRBYTE_EMITTED_AT desc
  ) is null  then 1 else 0 end as _AIRBYTE_ACTIVE_ROW,
  _AIRBYTE_EMITTED_AT,
  _AIRBYTE_DEDUP_EXCHANGE_RATE_HASHID
from "AIRBYTE_DATABASE"._AIRBYTE_TEST_NORMALIZATION."DEDUP_EXCHANGE_RATE_AB4"
-- DEDUP_EXCHANGE_RATE from "AIRBYTE_DATABASE".TEST_NORMALIZATION._AIRBYTE_RAW_DEDUP_EXCHANGE_RATE
where _airbyte_row_num = 1
      );
    