with country_code_map as 
(
    select distinct country_code, country from dev.gold.gold_alo_metrics_international_daily 
    where country in ('Austria', 'Belgium', 'Croatia', 'Cyprus', 'Estonia', 
    'Finland', 'France', 'Germany', 'Greece', 
    'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 
    'Netherlands', 'Portugal', 'Slovakia', 'Slovenia', 'Spain', 
    'South Korea', 'United States')
),

-- Austria, Belgium, Croatia, Cyprus, Estonia, Finland, France, Germany, Greece, Ireland, Italy, Latvia, Lithuania, Luxembourg, Malta, the Netherlands, Portugal, Slovakia, Slovenia, Spain
order_data as (
    select
        a.attributed_country_code, 
        b.country, 
        a.order_date,
        sum(a.orders) as total_orders,
        sum(a.revenue) as total_revenue,
        sum(a.product_qty) as total_units -- exclude gwp and lion discount is still applied total_order_qty->product_qty
    from dev.silver.sil_post_shopify_orders_daily_rollup a 
    inner join 
    country_code_map b 
    on upper(a.attributed_country_code) = upper(b.country_code)
    where
        source_name_adj not in ('retail', 'shopify_draft_order')
        and order_date >= CURRENT_DATE - INTERVAL '2 year'
    group by 1, 2, 3 
),

traffic_data as (
    select
    country_code,
        date,
        coalesce(sum(sessions),0) as visits
    from dev.silver.sil_sessions_fact where
        date >= CURRENT_DATE - INTERVAL '2 year'
    group by 1,2
),

pre_6wk_stats as 
(select
    o.country,
    o.order_date,
    t.visits as total_sessions,
    o.total_orders,
    o.total_units,
    o.total_revenue
from order_data as o
left join traffic_data as t
    on upper(o.attributed_country_code) = upper(t.country_code) and o.order_date = t.date
where 
order_date >= '2025-04-10'::DATE - INTERVAL '6 week'
and order_date < '2025-04-10'
order by order_date, total_orders desc),

test_period_stats as 
(select
    o.country,
    o.order_date,
    t.visits as total_sessions,
    o.total_orders,
    o.total_units,
    o.total_revenue
from order_data as o
left join traffic_data as t
    on upper(o.attributed_country_code) = upper(t.country_code) and o.order_date = t.date
where 
order_date > '2025-04-10'
and order_date <= '2025-04-25'

order by order_date, total_orders desc) 


select *, 'pre-test' as period, case when country = 'United States' or country ='South Korea' then country else 'EU' end as group 
,CURRENT_TIMESTAMP - INTERVAL '7 hour' as last_refreshed_at
from pre_6wk_stats
union 
select *, 'test' as period, case when country = 'United States' or country ='South Korea' then country else 'EU' end as group 
,CURRENT_TIMESTAMP - INTERVAL '7 hour' as last_refreshed_at

from test_period_stats
order by order_date asc, total_orders desc