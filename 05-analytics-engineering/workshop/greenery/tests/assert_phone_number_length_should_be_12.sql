select
    length(phone_number)

from {{ ref('my_users') }} #ref to model ที่เราวางไว้ใน modeks folder -> in dbt, it will show the lineage
where length(phone_number) != 12