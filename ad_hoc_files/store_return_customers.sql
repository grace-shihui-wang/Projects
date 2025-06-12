-- Customers who returned online purchase in store and repurchase in the same store within the same day

-- Step 1: Filtered store return data
with store_return as (
    select
        oli.order_id,
        cus.customer_id,
        cus.digital_vs_retail,
        orl.location_id as refund_location_id,
        sld.location_name as refund_location_name,
        o.refund_date_pst::date as refund_date,
        coalesce(orl.agg_subtotal, 0) * coalesce(rate, 1) as subtotal_refunded_usd
    from silver.sil_pre_shopify_order_line_item oli
    left join silver.sil_pre_shopify_order_refund_line_agg orl
        on oli.order_id = orl.order_id 
        and oli.line_items_id = orl.refund_line_item_id 
        and orl.latest_refund_line_item_id = 1
    left join silver.sil_shopify_order_line_items_dim o
        on oli.order_id = o.order_id 
        and oli.line_items_id = o.line_items_id
    left join (
        select distinct order_id, customer_id, digital_vs_retail
        from gold.gold_omni_order_line_item_detail
    ) cus
        on oli.order_id = cus.order_id
    left join (
        select location_id, max(name) as location_name
        from silver.sil_location_dim group by 1
    ) sld
        on sld.location_id = orl.location_id
    where subtotal_refunded_usd > 0 
      and o.order_date >= date('2024-05-29')
      and o.order_date <= date('2025-05-29')
      and cus.digital_vs_retail = 'digital'
      and (refund_location_name is not null and refund_location_name != 'Alo Distribution Centers')
),

-- Step 2: Unique customer return events
customer_return_events as (
    select distinct 
        customer_id,
        order_id as refunded_order_id,
        refund_location_id,
        refund_date
    from store_return
),

-- Step 3: Deduplicated orders - intra-day logic
deduped_orders as (
    select
        r.customer_id,
        r.refund_location_id,
        o.order_id,
        r.refund_date,
        row_number() over (partition by r.customer_id, o.order_id order by r.refund_date asc) as rn_order
    from customer_return_events r
    join silver.sil_shopify_order_line_items_dim oli
    
        on oli.order_processed_date_pst = r.refund_date
    join dev.silver.sil_shopify_order_dim o
         on o.customer_id = r.customer_id  
     and o.location_id = r.refund_location_id
        and o.order_id = oli.order_id
        and o.cancelled_at is null
        and o.order_id != r.refunded_order_id
),


-- Step 4: Join deduped_orders to all line items
customer_repurchase_same_day as (
    select
        d.customer_id,
        d.refund_location_id,
        d.refund_date,
        d.order_id,
        oli.line_items_id
    from deduped_orders d
    join silver.sil_shopify_order_line_items_dim oli
        on oli.order_id = d.order_id
    where d.rn_order = 1
      and oli.gift_card is false
),

-- Step 5: Bring in revenue for these repurchases
repurchase_revenue as (
    select 
        p.customer_id,
        p.refund_location_id,
        p.refund_date,
        p.order_id,
        sum(coalesce(li.gross_sales_usd - li.total_discounts_usd - li.line_item_duties_usd, 0)) as revenue
    from customer_repurchase_same_day p
    join dev.silver.sil_shopify_order_line_item_facts li
        on p.order_id = li.order_id
        and p.line_items_id = li.line_items_id
    where li.is_item_gwp_adj is false 
      and li.is_lion_discount_applied is false
    group by 1, 2, 3, 4
)

-- Step 6: Final summary with AOV
select 
    count(distinct r.customer_id) as total_return_customers,
    count(distinct rr.customer_id) as return_then_buy_customers,
    sum(rr.revenue)::decimal(10,2) as total_revenue,
    round(sum(rr.revenue) / nullif(count(distinct rr.order_id), 0), 2) as AOV,
    round(100.0 * count(distinct rr.customer_id) / nullif(count(distinct r.customer_id), 0), 2) as pct_returners_who_bought_same_day
from customer_return_events r
left join repurchase_revenue rr
    on r.customer_id = rr.customer_id
    and r.refund_location_id = rr.refund_location_id
    and r.refund_date = rr.refund_date;











-- Customers who returned online purchase in store and repurchase in the same store within 2 weeks

-- Store return events from Christian
with store_return as (
    select
        oli.order_id,
        cus.customer_id,
        cus.digital_vs_retail,
        orl.location_id as refund_location_id,
        sld.location_name as refund_location_name,
        o.refund_date_pst::date as refund_date,
        coalesce(orl.agg_subtotal, 0) * coalesce(rate, 1) as subtotal_refunded_usd
    from silver.sil_pre_shopify_order_line_item oli
    left join silver.sil_pre_shopify_order_refund_line_agg orl
        on oli.order_id = orl.order_id 
        and oli.line_items_id = orl.refund_line_item_id 
        and orl.latest_refund_line_item_id = 1
    left join silver.sil_shopify_order_line_items_dim o
        on oli.order_id = o.order_id 
        and oli.line_items_id = o.line_items_id
    left join (
        select distinct order_id, customer_id, digital_vs_retail
        from gold.gold_omni_order_line_item_detail
    ) cus
        on oli.order_id = cus.order_id
    left join (
        select location_id, max(name) as location_name
        from silver.sil_location_dim group by 1
    ) sld
        on sld.location_id = orl.location_id
    where subtotal_refunded_usd > 0 
      and o.order_date >= date('2024-05-29')
      and o.order_date <= date('2025-05-29')
      and cus.digital_vs_retail = 'digital'
      and (refund_location_name is not null and refund_location_name != 'Alo Distribution Centers')
),

-- Unique customer returners (add refund_date + one refunded_order_id)
customer_return_events as (
    select distinct 
        customer_id, 
        refund_location_id, 
        refund_date, 
        max(order_id) as refunded_order_id
    from store_return
    group by 1,2,3
),

-- 🚩 Step 1: Deduplicate orders per customer_id + order_id
deduped_orders as (
    select
        r.customer_id,
        r.refund_location_id,
        o.order_id,
        r.refund_date,
        row_number() over (partition by r.customer_id, o.order_id order by r.refund_date asc) as rn_order
    from customer_return_events r
    join dev.silver.sil_shopify_order_dim o
        on o.customer_id = r.customer_id
        and o.location_id = r.refund_location_id
        and o.order_id != r.refunded_order_id
    where o.cancelled_at is null
),

-- 🚩 Step 2: Join deduped_orders to ALL line_items
customer_repurchase_within_14d as (
    select
        d.customer_id,
        d.refund_location_id,
        d.order_id,
        oli.line_items_id
    from deduped_orders d
    join silver.sil_shopify_order_line_items_dim oli
        on oli.order_id = d.order_id
        and oli.order_processed_date_pst >= d.refund_date
        and oli.order_processed_date_pst <= d.refund_date + interval '15 day'
        and oli.order_processed_date_pst <= date('2025-05-29')
    where d.rn_order = 1 -- 🚩 Correct: FIRST refund window per order
      and oli.gift_card is false
),

-- 🚩 Step 3: Sum all line_items for each order
repurchase_revenue_2week as (
    select 
        p.customer_id,
        p.refund_location_id,
        p.order_id,
        sum(coalesce(li.gross_sales_usd - li.total_discounts_usd - li.line_item_duties_usd, 0)) as revenue
    from customer_repurchase_within_14d p
    join dev.silver.sil_shopify_order_line_item_facts li
        on p.order_id = li.order_id
        and p.line_items_id = li.line_items_id
    where li.is_item_gwp_adj is false 
      and li.is_lion_discount_applied is false
    group by 1, 2, 3
),

-- PRE-AGGREGATION to avoid duplication in summary
repurchase_summary as (
    select 
        customer_id,
        refund_location_id,
        count(distinct order_id) as total_repurchase_orders,
        sum(revenue) as total_revenue
    from repurchase_revenue_2week
    group by customer_id, refund_location_id
),

-- Final summary with no double counting
summary as (
    select 
        count(distinct r.customer_id) as total_return_customers,
        count(distinct rs.customer_id) as return_then_buy_customers,
        sum(coalesce(rs.total_revenue, 0))::decimal(18,2) as total_revenue,
        sum(coalesce(rs.total_repurchase_orders, 0)) as total_repurchase_orders
    from customer_return_events r
    left join repurchase_summary rs
        on r.customer_id = rs.customer_id
        and r.refund_location_id = rs.refund_location_id
)

-- Final query
select 
    total_return_customers,
    return_then_buy_customers,
    total_revenue,
    round(
        case when total_repurchase_orders = 0 then 0 
             else total_revenue / total_repurchase_orders
        end, 
        2
    ) as AOV,
    round(
        100.0 * return_then_buy_customers / nullif(total_return_customers, 0),
        2
    ) as pct_returners_who_bought_within_2weeks
from summary;