select
    count(0) as record_count
from {{ ref('stg_greenery__addresses') }}