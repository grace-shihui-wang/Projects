--New vs Returning Customer Check---

with customer_first_purchase as (
    select 
        customer_id, 
        min(order_processed_date_pst) as min_purchase_date
    from dev.silver.sil_shopify_order_line_items_dim olid
    left join dev.silver.sil_shopify_order_dim d 
        on olid.order_id = d.order_id
    where olid.is_item_gwp_adj is false
      and olid.is_lion_discount_applied is false
      and olid.gift_card is false
      and d.cancelled_at is null 
    group by customer_id
), 

--Digital Traffic (for national reporting)---

traffic_data as (
    select
        date,
        coalesce(sum(sessions), 0) as visits
    from dev.silver.sil_sessions_fact
    where date >= current_date - interval '2 year'
      and country_code = 'us'
    group by date
),

--Holdout Region (manual input)---

holdout_regions as (
    select 'HARTFORD & NEW HAVEN' as region_name union all
    select 'DETROIT' union all
    select 'NEW YORK' union all
    select 'HOUSTON' union all
    select 'PHILADELPHIA' union all
    select 'NEW ORLEANS' union all
    select 'SANTABARBRA - SANMAR - SANLUOB' union all
    select 'NASHVILLE' union all
    select 'COLUMBUS, OH' union all
    select 'ALEXANDRIA, LA' union all
    select 'EUREKA' union all
    select 'VICTORIA' union all
    select 'GREAT FALLS' union all
    select 'WATERTOWN' union all
    select 'SAN ANGELO' union all
    select 'MERIDIAN' union all
    select 'JOPLIN - PITTSBURG' union all
    select 'WAUSAU - RHINELANDER' union all
    select 'ROCKFORD' union all
    select 'ALBANY, GA' union all
    select 'HATTIESBURG - LAUREL' union all
    select 'MONTGOMERY (SELMA)' union all
    select 'LAFAYETTE, LA' union all
    select 'BEAUMONT - PORT ARTHUR' union all
    select 'CORPUS CHRISTI' union all
    select 'SPRINGFIELD, MO' union all
    select 'DAVENPORT - R. ISLAND - MOLINE' union all
    select 'COLUMBIA - JEFFERSON CITY' union all
    select 'GAINESVILLE' union all
    select 'DAYTON' union all
    select 'SOUTH BEND - ELKHART' union all
    select 'LUBBOCK' union all
    select 'FLINT - SAGINAW - BAY CITY' union all
    select 'TULSA' union all
    select 'LITTLE ROCK - PINE BLUFF' union all
    select 'MADISON' union all
    select 'RICHMOND - PETERSBURG' union all
    select 'BIRMINGHAM (ANN & TUSC)' union all
    select 'ALBUQUERQUE - SANTA FE' union all
    select 'FT. MYERS - NAPLES'
),

--Retail Store level New vs Returning Customer Count Daily---
regional_store_customer_cnt as (
    select 
        location_id, order_date,
        count(distinct case when cfp.min_purchase_date = golid.order_date then golid.customer_id end) as new_customers,
        count(distinct case when cfp.min_purchase_date != golid.order_date then golid.customer_id end) as returning_customers
    from gold.gold_omni_order_line_item_detail golid
    left join customer_first_purchase cfp 
        on golid.customer_id = cfp.customer_id
    where digital_vs_retail = 'retail' 
        and order_date > '2024-01-01'
    group by location_id, order_date
),

-- Digital Daily Report in Test --
digital_test_daily as (
    select 
        golid.fiscal_year, 
        golid.fiscal_week, 
        extract(dow from golid.order_date) as day_of_week, 
        golid.order_date as day,
        cast(
            case 
                when golid.fiscal_year = 2025 then to_char(golid.order_date, 'YYYY-MM-DD')
                else to_char(golid.order_date + interval '364 days', 'YYYY-MM-DD')
            end as varchar(20)
        ) as date_label,
        dma_map.dma_description as dma_region,
        cast('empty' as varchar(50)) as store_region,
        cast('daily' as varchar(20)) as period,
        cast('digital' as varchar(20)) as channel,
        case when golid.fiscal_year = 2025 then 'current' else 'yoy' end as yoy_flag,
        cast(null as numeric(18,2)) as traffic,
        cast(null as numeric(6,4)) as conversion_rate,
        sum(gross_sales_usd - line_item_duties_usd - total_discounts_usd) / nullif(count(distinct golid.order_id), 0) as aov,
        sum(gross_sales_usd - line_item_duties_usd - total_discounts_usd) as revenue,
        count(distinct golid.order_id) as orders,
        count(distinct case when cfp.min_purchase_date = golid.order_date then golid.customer_id end) as new_customers,
        count(distinct case when cfp.min_purchase_date != golid.order_date then golid.customer_id end) as returning_customers,
        0 as total_traffic,
        0 as total_revenue,
        0 as total_orders,
        0 as total_new_customers,
        0 as total_returning_customers,
        case when dma_map.dma_description in (select region_name from holdout_regions) then 1 else 0 end as holdout_signal
    from gold.gold_omni_order_line_item_detail golid
    left join customer_first_purchase cfp 
        on golid.customer_id = cfp.customer_id
    left join dev.silver.sil_shopify_order_dim d 
        on golid.order_id = d.order_id
    left join dev.da.haus_zip_to_dma_export dma_map 

        on lpad(trim(dma_map.zip_code), 5, '0') = lpad(cast(d.destination_zip as varchar), 5, '0')
    where golid.is_domestic is true and digital_vs_retail = 'digital' and dma_map.dma_description is not null 
      and (golid.order_date >= '2025-06-03'
        or (golid.order_date >= '2025-06-03'::date - interval '364 days' 
            and golid.order_date <= '2025-06-03'::date - interval '364 days' + interval '2 months'))
    group by 1,2,3,4,5,6,7,8,9,10
),

-- Retail Daily Report in Test --

store_test_daily as (
    select 
        r.fiscal_year, 
        r.fiscal_week, 
        extract(dow from r.day) as day_of_week, 
        r.day,
        cast(
            case 
                when r.fiscal_year = 2025 then to_char(r.day, 'YYYY-MM-DD')
                else to_char(r.day + interval '364 days', 'YYYY-MM-DD')
            end as varchar(20)
        ) as date_label,
        dma_map.dma_description as dma_region,
        case when l.city = 'Miami' then 'Miami' else l.region end as store_region,
        cast('daily' as varchar(20)) as period,
        cast('retail' as varchar(20)) as channel,
        case when r.fiscal_year = 2025 then 'current' else 'yoy' end as yoy_flag,
        cast(coalesce(sum(r.traffic),0) as numeric(18,2)) as traffic, 
        cast(round(1.0 * coalesce(sum(r.total_orders),0) / nullif(coalesce(sum(r.traffic),0), 0), 2) as numeric(6,4)) as conversion_rate,
        coalesce(sum(r.net_sales),0) / nullif(coalesce(sum(r.total_orders),0), 0) as aov,
        coalesce(sum(r.net_sales),0) as revenue,
        sum(r.total_orders) as orders,
        coalesce(sum(new_customers),0) as new_customers,
        coalesce(sum(returning_customers),0) as returning_customers, 
        0 as total_traffic,
        0 as total_revenue,
        0 as total_orders,
        0 as total_new_customers,
        0 as total_returning_customers,
        case when dma_map.dma_description in (select region_name from holdout_regions) then 1 else 0 end as holdout_signal
    from dev.gold.gold_retail_daily_report r
    left join silver.sil_retail_locations l
        on r.location_id = l.location_id
    left join dev.da.haus_zip_to_dma_export dma_map
        on lpad(trim(dma_map.zip_code), 5, '0') = lpad(cast(l.zip as varchar), 5, '0')
    left join regional_store_customer_cnt cc 
        on r.location_id = cc.location_id and r.day = cc.order_date
    where (l.country = 'United States' or l.country = 'US')  and dma_map.dma_description is not null 
      and (r.day >= '2025-06-03' 
           or (r.day >= '2025-06-03'::date - interval '364 days' 
               and r.day <= '2025-06-03'::date - interval '364 days' + interval '2 months'))
      and l.region is not null 
    group by 1,2,3,4,5,6,7,8,9,10
),

----PRE-TEST STARTED -------

-- Digital Daily Report in Pre-Test --

digital_pre_test_daily as (
    select 
        golid.fiscal_year,
        golid.order_date,
        dma_map.dma_description as dma_region,
        sum(gross_sales_usd - line_item_duties_usd - total_discounts_usd) / nullif(count(distinct golid.order_id), 0) as aov,
        sum(gross_sales_usd - line_item_duties_usd - total_discounts_usd) as revenue,
        count(distinct golid.order_id) as orders,
        count(distinct case when cfp.min_purchase_date::date = golid.order_date then golid.customer_id end) as new_customers,
        count(distinct case when cfp.min_purchase_date::date < golid.order_date then golid.customer_id end) as returning_customers
    from gold.gold_omni_order_line_item_detail golid
    left join customer_first_purchase cfp 
        on golid.customer_id = cfp.customer_id
    left join dev.silver.sil_shopify_order_dim d 
        on golid.order_id = d.order_id
    left join dev.da.haus_zip_to_dma_export dma_map
        on lpad(trim(dma_map.zip_code), 5, '0') = lpad(cast(d.destination_zip as varchar), 5, '0')
    where golid.is_domestic is true and digital_vs_retail = 'digital' and dma_map.dma_description is not null 
      and (golid.order_date between '2025-06-01'::date - interval '29 days' and '2025-06-01'::date
           or golid.order_date between '2024-06-02'::date - interval '29 days' and '2024-06-02'::date)
    group by golid.fiscal_year, golid.order_date, dma_map.dma_description
),

-- Digital Aggregated Report in Pre-Test --
digital_pre_test as (
    select
        fiscal_year,
        -1 as fiscal_week,
        -1 as day_of_week,
        case when fiscal_year = 2025 then'2025-06-01'::date else '2024-06-01'::date end as day,
        cast('2025-06-01' as varchar(20)) as date_label,
        dma_region,
        cast('empty' as varchar(50)) as store_region,
        cast('pre-test' as varchar(20)) as period,
        cast('digital' as varchar(20)) as channel,
        case when fiscal_year = 2025 then 'current' else 'yoy' end as yoy_flag,
        cast(null as numeric(18,2)) as traffic,
        cast(null as numeric(6,4)) as conversion_rate,
        avg(aov) as aov,
        avg(revenue) as revenue,
        avg(orders) as orders,
        avg(new_customers) as new_customers,
        avg(returning_customers) as returning_customers,
        cast(null as numeric(18,2)) as total_traffic,
        sum(revenue) as total_revenue,
        sum(orders) as total_orders,
        sum(new_customers) as total_new_customers,
        sum(returning_customers) as total_returning_customers,
        case when dma_region in (select region_name from holdout_regions) then 1 else 0 end as holdout_signal
    from digital_pre_test_daily
    group by fiscal_year, day, dma_region
),

-- Store Daily Report in Pre-Test --
store_pre_test_daily as (
    select 
        r.fiscal_year, r.day, 
        case when l.city = 'Miami' then 'Miami' else l.region end as store_region, 
        dma_map.dma_description as dma_region,
        sum(coalesce(r.traffic, 0)) as total_traffic,
        sum(coalesce(r.net_sales,0)) as total_revenue,
        sum(coalesce(r.total_orders,0)) as total_orders,
        sum(coalesce(cc.new_customers,0)) as total_new_customers,
        sum(coalesce(cc.returning_customers,0)) as total_returning_customers, 
        case when  dma_map.dma_description in (select region_name from holdout_regions) then 1 else 0 end as holdout_signal

    from dev.gold.gold_retail_daily_report r
    left join silver.sil_retail_locations l
        on r.location_id = l.location_id
    left join (
        select zip_code, max(dma_description) as dma_description
        from dev.da.haus_zip_to_dma_export
        group by zip_code
    ) dma_map    
        on lpad(trim(dma_map.zip_code), 5, '0') = lpad(cast(l.zip as varchar), 5, '0')
    left join regional_store_customer_cnt cc 
        on r.location_id = cc.location_id and r.day = cc.order_date
    where (l.country = 'United States' or l.country = 'US') and dma_map.dma_description is not null 
    and (
        (r.day between '2025-06-02'::date - interval '29 days' and '2025-06-02'::date)
        or
        (r.day between ('2025-06-02'::date - interval '29 days' - interval '364 days') and ('2025-06-02'::date - interval '364 days'))
    )
    and l.region is not null
    group by fiscal_year, 
    r.day, l.region, 
    case when l.city = 'Miami' then 'Miami' else l.region end,
    dma_map.dma_description
),

-- Store Aggregated Report in Pre-Test --

store_pre_test as (
    select 
        fiscal_year, 
        -1 as fiscal_week, 
        -1 as day_of_week, 
        case when fiscal_year = 2025 then'2025-06-01'::date else '2024-06-01'::date end as day,
        cast('2025-06-01' as varchar(20)) as date_label,
        dma_region,
        store_region,
        cast('pre-test' as varchar(20)) as period,
        cast('retail' as varchar(20)) as channel,
        case when r.fiscal_year = 2025 then 'current' else 'yoy' end as yoy_flag,
        cast(avg(coalesce(r.total_traffic, 0)) as numeric(18,2)) as traffic,
        coalesce(cast(avg(round(1.0 * coalesce(r.total_orders,0) / nullif(coalesce(r.total_traffic,0),0), 2)) as numeric(6,4)),0) as conversion_rate,
        coalesce(avg(coalesce(r.total_revenue, 0) / nullif(coalesce(r.total_orders,0),0)),0) as aov,
        avg(coalesce(r.total_revenue,0)) as revenue,
        avg(coalesce(r.total_orders,0)) as orders,
        avg(coalesce(r.total_new_customers,0)) as new_customers,
        avg(coalesce(r.total_returning_customers,0)) as returning_customers,
        sum(coalesce(r.total_traffic, 0)) as total_traffic,
        sum(coalesce(r.total_revenue,0)) as total_revenue,
        sum(coalesce(r.total_orders,0)) as total_orders,
        sum(coalesce(r.total_new_customers,0)) as total_new_customers,
        sum(coalesce(r.total_returning_customers,0)) as total_returning_customers,
        max(holdout_signal) as holdout_signal
    from store_pre_test_daily r
    group by fiscal_year, 
    case when fiscal_year = 2025 then'2025-06-01'::date else '2024-06-01'::date end, 
    dma_region, store_region
),
----PRE-TEST ENDED-------


-- Final Daily-Regional Output --
test_daily as (
    select * from store_test_daily
    union all
    select * from digital_test_daily
),

pre_test as (
    select * from store_pre_test
    union all
    select * from digital_pre_test
),

-- ALL CODES BELOW ARE DESIGNED FOR TABLEAU OUTPUT -- 

-- ====================
-- REGION LEVEL OUTPUT
-- ====================
region_level_output as (
    select fiscal_year, fiscal_week, day_of_week, day, date_label,
           dma_region, store_region, period, channel, yoy_flag,
           traffic, orders, conversion_rate, aov, revenue, new_customers, returning_customers, holdout_signal
    from test_daily
    where (
        (
            (store_region in ('NYC', 'Miami') or (store_region = 'LA' and holdout_signal = 0))
    
            and channel = 'retail'
            )

          or dma_region in ('NEW YORK', 'LOS ANGELES', 'MIAMI - FT. LAUDERDALE') and channel = 'digital')

    union all

    select fiscal_year, fiscal_week, day_of_week, day, date_label,
           dma_region, store_region, period, channel, yoy_flag,
           traffic, orders, conversion_rate, aov, revenue, new_customers, returning_customers, holdout_signal
    from pre_test
    where (
        (
            (store_region in ('NYC', 'Miami') or (store_region = 'LA' and holdout_signal = 0))
    
            and channel = 'retail'
            )

          or dma_region in ('NEW YORK', 'LOS ANGELES', 'MIAMI - FT. LAUDERDALE') and channel = 'digital')
),

-- ====================
-- ALL HOLDOUT
-- ====================
all_holdout as (
    select fiscal_year, fiscal_week, day_of_week, day, date_label,
           cast('All Hold Out' as varchar(50)) as dma_region,
           cast('All Hold Out' as varchar(50)) as store_region,
           period, channel, yoy_flag,
           cast(sum(traffic) as numeric(18,2)) as traffic,
           cast(sum(orders) as numeric(18,2)) as orders,
           cast(sum(orders) / nullif(sum(traffic),0) as numeric(6,4)) as conversion_rate,
           cast(sum(revenue) / nullif(sum(orders),0) as numeric(18,2)) as aov,
           cast(sum(revenue) as numeric(18,2)) as revenue,
           cast(sum(new_customers) as numeric(18,2)) as new_customers,
           cast(sum(returning_customers) as numeric(18,2)) as returning_customers,
           avg(holdout_signal) as holdout_signal
    from test_daily
    where holdout_signal = 1
    group by fiscal_year, fiscal_week, day_of_week, day, date_label,
             period, channel, yoy_flag

    union all

    select fiscal_year, fiscal_week, day_of_week, day, date_label,
           cast('All Hold Out' as varchar(50)) as dma_region,
           cast('All Hold Out' as varchar(50)) as store_region,
           period, channel, yoy_flag,
           cast(sum(total_traffic)/29 as numeric(18,2)) as traffic,
           cast(sum(total_orders)/29 as numeric(18,2)) as orders,
           cast(sum(total_orders) / nullif(sum(total_traffic),0) as numeric(6,4)) as conversion_rate,
           cast(sum(total_revenue) / nullif(sum(total_orders),0) as numeric(18,2)) as aov,
           cast(sum(total_revenue)/29 as numeric(18,2)) as revenue,
           cast(sum(total_new_customers)/29 as numeric(18,2)) as new_customers,
           cast(sum(total_returning_customers)/29 as numeric(18,2)) as returning_customers,
           avg(holdout_signal) as holdout_signal
    from pre_test
    where holdout_signal = 1
    group by fiscal_year, fiscal_week, day_of_week, day, date_label,
             period, channel, yoy_flag
),

-- ====================
-- ALL CONTROL
-- ====================
all_control as (
    select fiscal_year, fiscal_week, day_of_week, day, date_label,
           cast('All Control' as varchar(50)) as dma_region,
           cast('All Control' as varchar(50)) as store_region,
           period, channel, yoy_flag,
           cast(sum(traffic) as numeric(18,2)) as traffic,
           cast(sum(orders) as numeric(18,2)) as orders,
           cast(sum(orders) / nullif(sum(traffic),0) as numeric(6,4)) as conversion_rate,
           cast(sum(revenue) / nullif(sum(orders),0) as numeric(18,2)) as aov,
           cast(sum(revenue) as numeric(18,2)) as revenue,
           cast(sum(new_customers) as numeric(18,2)) as new_customers,
           cast(sum(returning_customers) as numeric(18,2)) as returning_customers,
           avg(holdout_signal) as holdout_signal
    from test_daily
    where holdout_signal = 0
    group by fiscal_year, fiscal_week, day_of_week, day, date_label,
             period, channel, yoy_flag

    union all

    select fiscal_year, fiscal_week, day_of_week, day, date_label,
           cast('All Control' as varchar(50)) as dma_region,
           cast('All Control' as varchar(50)) as store_region,
           period, channel, yoy_flag,
           cast(sum(total_traffic)/29 as numeric(18,2)) as traffic,
           cast(sum(total_orders)/29 as numeric(18,2)) as orders,
           cast(sum(total_orders) / nullif(sum(total_traffic),0) as numeric(6,4)) as conversion_rate,
           cast(sum(total_revenue) / nullif(sum(total_orders),0) as numeric(18,2)) as aov,
           cast(sum(total_revenue)/29 as numeric(18,2)) as revenue,
           cast(sum(total_new_customers)/29 as numeric(18,2)) as new_customers,
           cast(sum(total_returning_customers)/29 as numeric(18,2)) as returning_customers,
           avg(holdout_signal) as holdout_signal
    from pre_test
    where holdout_signal = 0
    group by fiscal_year, fiscal_week, day_of_week, day, date_label,
             period, channel, yoy_flag
),

-- ====================
-- NATIONAL
-- ====================
national as (

    -- test period
    select
        td.fiscal_year,
        td.fiscal_week,
        td.day_of_week,
        td.day,
        td.date_label,
        cast('National' as varchar(50)) as dma_region,
        cast('National' as varchar(50)) as store_region,
        td.period,
        td.channel,
        td.yoy_flag,

        -- traffic
        cast(
            case 
                when td.channel = 'digital' then coalesce(max(t.national_visits), 0)
                else sum(td.traffic)
            end as numeric(18,2)
        ) as traffic,

        -- orders
        cast(sum(td.orders) as numeric(18,2)) as orders,

        --CVR, placeholder, will do in tableau
        cast(null as numeric(6,4)) as conversion_rate,

        -- aov
        cast(sum(td.revenue) / nullif(sum(td.orders), 0) as numeric(18,2)) as aov,

        -- revenue
        cast(sum(td.revenue) as numeric(18,2)) as revenue,

        -- new customers
        cast(sum(td.new_customers) as numeric(18,2)) as new_customers,

        -- returning customers
        cast(sum(td.returning_customers) as numeric(18,2)) as returning_customers,

        -- holdout
        avg(td.holdout_signal) as holdout_signal

    from test_daily td

    left join (
        -- pre-aggregate national traffic per day → avoids duplication
        select
            date,
            sum(visits) as national_visits
        from traffic_data
        group by date
    ) t
    on t.date = td.day::date
    and td.channel = 'digital'

    group by td.fiscal_year, td.fiscal_week, td.day_of_week, td.day, td.date_label,
             td.period, td.channel, td.yoy_flag

    union all

    -- pre-test period
    select
        pt.fiscal_year,
        pt.fiscal_week,
        pt.day_of_week,
        pt.day,
        pt.date_label,
        cast('National' as varchar(50)) as dma_region,
        cast('National' as varchar(50)) as store_region,
        pt.period,
        pt.channel,
        pt.yoy_flag,

        -- traffic
        cast(
            case 
                when pt.channel = 'digital' then coalesce(max(t.national_visits), 0)
                else sum(pt.total_traffic) / 29
            end as numeric(18,2)
        ) as traffic,

        -- orders
        cast(sum(pt.total_orders) / 29 as numeric(18,2)) as orders,

        --CVR, placeholder, will do in tableau
        cast(null as numeric(6,4)) as conversion_rate,


        -- aov
        cast(sum(pt.total_revenue) / nullif(sum(pt.total_orders), 0) as numeric(18,2)) as aov,

        -- revenue
        cast(sum(pt.total_revenue) / 29 as numeric(18,2)) as revenue,

        -- new customers
        cast(sum(pt.total_new_customers) / 29 as numeric(18,2)) as new_customers,

        -- returning customers
        cast(sum(pt.total_returning_customers) / 29 as numeric(18,2)) as returning_customers,

        -- holdout
        avg(pt.holdout_signal) as holdout_signal

    from pre_test pt

    left join (
        select
            date,
            sum(visits) as national_visits
        from traffic_data
        group by date
    ) t
    on t.date = pt.day::date
    and pt.channel = 'digital'

    group by pt.fiscal_year, pt.fiscal_week, pt.day_of_week, pt.day, pt.date_label,
             pt.period, pt.channel, pt.yoy_flag
)


-- ====================
-- FINAL SELECT
-- ====================

    select *,CURRENT_TIMESTAMP - INTERVAL '7 hour' as last_refreshed_at from region_level_output
    union all
    select *,CURRENT_TIMESTAMP - INTERVAL '7 hour' as last_refreshed_at  from all_holdout
    union all
    select *,CURRENT_TIMESTAMP - INTERVAL '7 hour' as last_refreshed_at  from all_control
    union all
    select *,CURRENT_TIMESTAMP - INTERVAL '7 hour' as last_refreshed_at  from national