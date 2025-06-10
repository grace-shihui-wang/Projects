--  Updated: Move all metrics except L12M sales/order count to lifetime scope
-- order_date -> order_processed_date_pst; current_date to pst 
-- Now uses latest stylist tag per customer and first date that stylist was tagged
with current_customers as (
  select customer_id, updated_at, tags
  from dev.silver.sil_pre_shopify_customer
  where lower(tags) like '%#esclient%'
     or lower(tags) like '%#sydneyclient%'
     or lower(tags) like '%#hallieclient%'
     or lower(tags) like '%#abbeyclient%'
     or lower(tags) like '%#formerpc%'
),

first_tagged_dates as (
  select customer_id,
         min(updated_at) as stylist_tagged_date
  from dev.silver.sil_pre_shopify_customer_historical
  where lower(tags) like '%#esclient%'
     or lower(tags) like '%#sydneyclient%'
     or lower(tags) like '%#hallieclient%'
     or lower(tags) like '%#abbeyclient%'
     or lower(tags) like '%#formerpc%'
  group by customer_id
),

tagged_data as (
  select cc.customer_id,
         case
           when lower(cc.tags) like '%#esclient%' then '#esclient'
           when lower(cc.tags) like '%#sydneyclient%' then '#sydneyclient'
           when lower(cc.tags) like '%#hallieclient%' then '#hallieclient'
           when lower(cc.tags) like '%#abbeyclient%' then '#abbeyclient'
           when lower(cc.tags) like '%#formerpc%' then '#formerpc'
         end as stylist_tags,
         coalesce(cast(ftd.stylist_tagged_date::timestamptz AT TIME ZONE 'America/Los_Angeles' as date), cc.updated_at) as stylist_tagged_date
  from current_customers cc
  left join first_tagged_dates ftd on cc.customer_id = ftd.customer_id
),

top_product_l12m as (
  select customer_id, product_title, total_units
  from (
    select 
      o.customer_id,
      d.product_title,
      sum(coalesce(li.order_quantity, 0)) as total_units,
      row_number() over (partition by o.customer_id order by sum(coalesce(li.order_quantity, 0)) desc) as rn
    from dev.silver.sil_shopify_order_dim o
    left join dev.silver.sil_shopify_order_line_item_facts li on o.order_id = li.order_id
    left join dev.silver.sil_shopify_order_line_items_dim d on li.order_id = d.order_id and li.line_items_id = d.line_items_id
    where d.order_processed_date_pst >= ((CURRENT_DATE::timestamp) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')::date - INTERVAL '1 day' - interval '12 months'
      and d.order_processed_date_pst <= ((CURRENT_DATE::timestamp) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')::date - INTERVAL '1 day'
      and o.cancelled_at is null
      and li.is_item_gwp_adj is false
      and li.is_lion_discount_applied is false
      and d.gift_card is false
      and d.digital_vs_retail in ('digital', 'retail')
    group by o.customer_id, d.product_title
  ) ranked_products
  where rn = 1
),

order_data_l12m as (
  select o.customer_id, o.order_id, li.gross_sales_usd, li.net_sales_usd
  from dev.silver.sil_shopify_order_dim o
  left join dev.silver.sil_shopify_order_line_item_facts li on o.order_id = li.order_id
  left join silver.sil_shopify_order_line_items_dim olid 	on olid.order_id = li.order_id and olid.line_items_id = li.line_items_id
  where olid.order_processed_date_pst >= ((CURRENT_DATE::timestamp) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')::date - INTERVAL '1 day' - interval '12 months' 
  and olid.order_processed_date_pst <= ((CURRENT_DATE::timestamp) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')::date - INTERVAL '1 day'
  and o.cancelled_at is null
  	and li.is_item_gwp_adj is false
	and li.is_lion_discount_applied is false
    and olid.gift_card is false
    and olid.digital_vs_retail in ('digital', 'retail')
),


l12m_metrics as (
  select 
    customer_id,
    sum(coalesce(gross_sales_usd, 0)) as l12m_gross_sales,
    sum(coalesce(net_sales_usd, 0)) as l12m_net_sales,
    count(distinct order_id) as l12m_num_orders, 
    case when sum(coalesce(net_sales_usd, 0)) >= 7000 and sum(coalesce(net_sales_usd, 0)) < 10000 then 'Tier 2'
    when sum(coalesce(net_sales_usd, 0)) >= 10000 then 'Tier 3'
    when sum(coalesce(net_sales_usd, 0)) < 7000 and sum(coalesce(net_sales_usd, 0)) >=5000 then 'Tier 1'
    else null
    end as spend_level
  from order_data_l12m
  group by customer_id
),

latest_billing_info as (
  select 
    o.customer_id,
    coalesce(o.billing_city, o.destination_city) as billing_city,
    coalesce(o.billing_country, o.destination_country) as billing_country,
    coalesce(o.billing_zip, o.destination_zip) as billing_zip,
    li.order_date,
    row_number() over (
      partition by o.customer_id 
      order by li.order_date desc
    ) as rn
  from dev.silver.sil_shopify_order_dim o
  inner join 
  (select order_id, 
  max(order_processed_date_pst) as order_date
  from 
  dev.silver.sil_shopify_order_line_items_dim
  where 
  	is_item_gwp_adj is false
	and gift_card is false
	and is_lion_discount_applied is false
    and order_date is not null 
    and order_date <= ((CURRENT_DATE::timestamp) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')::date - INTERVAL '1 day'
    and order_date >= ((CURRENT_DATE::timestamp) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')::date - INTERVAL '3 year'
  group by order_id
  ) as li on o.order_id = li.order_id 
  where 
    o.cancelled_at is null
    and ((o.billing_city is not null and o.billing_country is not null) or (o.destination_city is not null and o.destination_country is not null))
)

select 
  td.customer_id,
  td.stylist_tags,
  td.stylist_tagged_date,
  c.first_name,
  c.last_name,
  c.birthday,
  c.email, 
  o.order_id,
  o.platform_adj,
  li.line_items_id,
  d.order_processed_date_pst as order_date,
  cal.fiscal_week, 
  cal.fiscal_month, 
  cal.fiscal_quarter, 
  cal.fiscal_year,
  li.gross_sales_usd,
  li.net_sales_usd,
  li.order_quantity,
  li.subtotal_ordered_usd,
  li.subtotal_refunded_usd,
  lbi.billing_city as billing_city,
  lbi.billing_country as billing_country,
  lbi.billing_zip
  lm.l12m_gross_sales,
  lm.l12m_net_sales,
  lm.l12m_num_orders, 
  lm.spend_level,
  tp.product_title as most_purchased_product_title,
  tp.total_units as most_purchased_units
from tagged_data td
left join dev.silver.sil_customer_dim c on td.customer_id = c.customer_id
left join dev.silver.sil_shopify_order_dim o on td.customer_id = o.customer_id
left join dev.silver.sil_shopify_order_line_item_facts li on o.order_id = li.order_id 
left join dev.silver.sil_shopify_order_line_items_dim d on li.order_id = d.order_id and li.line_items_id = d.line_items_id
left join l12m_metrics lm on td.customer_id = lm.customer_id
left join top_product_l12m tp on td.customer_id = tp.customer_id
left join mgt.dates cal on d.order_processed_date_pst = cal.date_dt
left join latest_billing_info lbi on td.customer_id = lbi.customer_id and lbi.rn = 1


where d.digital_vs_retail in ('digital', 'retail')
	and d.is_item_gwp_adj is false
	and d.gift_card is false
	and d.is_lion_discount_applied is false
	and o.cancelled_at is null
    and d.order_processed_date_pst <= ((CURRENT_DATE::timestamp) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')::date - INTERVAL '1 day'