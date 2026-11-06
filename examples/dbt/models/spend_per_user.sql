-- The faker appends every sync's rows, so count each purchase once.
with purchases as (
    select distinct on (id) id, user_id, amount
    from {{ source('faker', 'purchases') }}
    order by id
)
select user_id, count(*) as purchases, sum(amount) as spend
from purchases
group by user_id
