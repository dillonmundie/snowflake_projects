with sales as(
    select 
    case 
        when collection_group ilike '%heatgear%'
        and article_product_division_description = 'Apparel'
        then 'Heatgear'
        when collection_group ilike '%coldgear%'
        and article_product_division_description = 'Apparel'
        then 'Coldgear'
        when collection_group ilike '%meridian%'
        and article_product_division_description = 'Apparel'
        then 'Meridian'
        when collection_group ilike '%unstoppable%'
        and article_product_division_description = 'Apparel'
        then 'Unstoppable'
        when collection_group ilike '%curry%'
        and article_product_division_description = 'Footwear'
        then 'Curry Footwear'
        when collection_group ilike '%infinite%'
        and article_product_division_description = 'Footwear'
        then 'Infinite' 
        when collection_group ilike '%slipspeed%'
        and article_product_division_description = 'Footwear'
        then 'Slipspeed' 
    end as franchise_prop, transaction_date, country, 
    case when distribution_channel_description = 'Wholesale' then 'Wholesale'
        when distribution_channel_description in ('Factory House', 'Brand House') then 'Retail' 
        when so_customer_po_type = 'APP' then 'App' else 'e-Commerce' end as purchase_channel, 
    case when distribution_channel_description = 'Wholesale' then retailername
        when distribution_channel_description = 'e-Commerce' then so_customer_po_type 
        else distribution_channel_description end as sub_purchase_channel, 
    sum(sales_units) as quantity, sum(net_units) as net_quantity, sum(abs(returned_units)) as returned_quantity, 
    sum(sales_dollars_usd) as total_rev, 
    sum(case when net_dollars_usd!= sales_dollars_usd and returned_dollars_usd = 0 then sales_dollars_usd 
            else net_dollars_usd end) as net_rev, 
    sum(abs(returned_dollars_usd)) as returned_dollars_usd, sum(so_demand_pm_value_usd) as margin, 
    sum(case when sales_units = 0 then 0 else so_cost_usd end) as cost
    from "ENTERPRISE_PRD_DB"."CONSUMER_SALES"."MDL_CONSUMER_SALES_UNION"
    where (acct_std_inclusion = 'Y' or acct_std_inclusion is null)
    and (acct_having_sales = 'Y' or acct_having_sales is null)
    and (retailername not in ('SPORTSDIRECTINTERNATIONAL') or retailername is null)
    and (customer_group in ('25', '04') or customer_group is null)
    and franchise_prop is not null
    and article_product_division_description in ('Apparel', 'Accessories', 'Footwear')
    group by 1,2,3,4,5
),
clean_sales as (
    select transaction_date as date, franchise_prop, country, purchase_channel, sub_purchase_channel, 
    quantity, case when net_quantity != quantity - returned_quantity then quantity-returned_quantity else net_quantity end as net_quantity, returned_quantity, 
    total_rev, case when net_rev != total_rev and returned_dollars_usd = 0 then total_rev else net_rev end as net_rev, returned_dollars_usd as total_return_rev, 
    cost as total_cost, 
    case when margin is null and (cost is not null and total_rev is not null) then total_rev-cost else margin end as total_margin, 
    null as total_discount
    from sales 
),
prev_sales_dt_wholesale as (
    select distinct transaction_date, 
    case when dayofweek(transaction_date) = 6 then dateadd('week', -52, transaction_date)
        when dayofweek(transaction_date) = 0 then next_day(dateadd('week', -53, transaction_date), 'su')
        when dayofweek(transaction_date) = 1 then next_day(dateadd('week', -53, transaction_date), 'mo')
        when dayofweek(transaction_date) = 2 then next_day(dateadd('week', -53, transaction_date), 'tu')
        when dayofweek(transaction_date) = 3 then next_day(dateadd('week', -53, transaction_date), 'we')
        when dayofweek(transaction_date) = 4 then next_day(dateadd('week', -53, transaction_date), 'th')
        when dayofweek(transaction_date) = 5 then next_day(dateadd('week', -53, transaction_date), 'fr')
    end as ws_prev_date, 
    from "ENTERPRISE_PRD_DB"."CONSUMER_SALES"."MDL_CONSUMER_SALES_UNION"
    where distribution_channel_description = 'Wholesale'
), 
agg_wholesale_sales as (
    select a.*, 'USD' as currency, 'USD' as to_currency, null as conversion_rate,
    b.quantity as py_quantity, b.net_quantity as py_net_quantity, b.returned_quantity as py_return_quantity,
    b.total_rev as py_total_rev, b.net_rev as py_net_rev, b.total_return_rev as py_total_return_rev, 
    b.total_cost as py_total_cost, b.total_margin as py_total_margin, b.total_discount as py_total_discount 
    from clean_sales a
    left join prev_sales_dt_wholesale py
    on a.date = py.transaction_date
    left join clean_sales b
    on py.ws_prev_date = b.date
    and a.franchise_prop = b.franchise_prop
    and a.country = b.country
    and a.purchase_channel = b.purchase_channel
    and a.sub_purchase_channel = b.sub_purchase_channel
    where a.date between date('2021-04-01') and current_date
    and a.purchase_channel = 'Wholesale'
    order by date desc
), 
agg_dtc_sales as (
    select a.*, 'USD' as currency, 'USD' as to_currency, null as conversion_rate,
    b.quantity as py_quantity, b.net_quantity as py_net_quantity, b.returned_quantity as py_return_quantity,
    b.total_rev as py_total_rev, b.net_rev as py_net_rev, b.total_return_rev as py_total_return_rev, b.total_cost as py_total_cost, b.total_margin as py_total_margin,
    b.total_discount as py_total_discount 
    from clean_sales a
    left join clean_sales b
    on a.date = dateadd('year', 1, b.date)
    and a.franchise_prop = b.franchise_prop
    and a.country = b.country
    and a.purchase_channel = b.purchase_channel
    and a.sub_purchase_channel = b.sub_purchase_channel
    where a.date between date('2021-04-01') and current_date
    and a.purchase_channel != 'Wholesale'
    order by date desc
)
select *
from agg_dtc_sales
where country is not null
union all
select *
from agg_wholesale_sales
where country is not null


-- Testing
select *
from  "ENTERPRISE_PRD_DB"."CONSUMER_SALES"."MDL_CONSUMER_SALES_UNION"
where distribution_channel_description = 'e-Commerce'
and collection_group ilike '%curry%'
and transaction_date = '2023-12-23'
and customer_group in ('25', '04')
and country = 'US'
-- 124 Quant
-- 114 Net Quant
-- 10 Returned
-- 17370.25 Rev
-- 16000.25 Rev
-- 1370 Returned
-- 11869.29 Margin
-- 5615.78 Cost
-- (114.82) Difference in Margin
select *
from  "ENTERPRISE_PRD_DB"."CONSUMER_SALES"."MDL_CONSUMER_SALES_UNION"
where distribution_channel_description = 'e-Commerce'
and collection_group ilike '%infinite%'
and transaction_date = '2024-03-05'
and customer_group in ('25', '04')
and so_customer_po_type = 'MRKT'
and country = 'KR'
-- No net quant; applied logic to clean data here

select *
from  "ENTERPRISE_PRD_DB"."CONSUMER_SALES"."MDL_CONSUMER_SALES_UNION"
-- where so_cost_usd != 0
-- and so_demand_pm_value_usd != 0 
-- and returned_units > 0 
where sales_units = 0
and distribution_channel_description not in ('Wholesale', 'Factory House', 'Brand House')
limit 100
-- Returned Units 

select *
from  "ENTERPRISE_PRD_DB"."CONSUMER_SALES"."MDL_CONSUMER_SALES_UNION"
where collection_group ilike '%meridian%'
and transaction_date = '2024-03-05'
and (customer_group in ('25', '04') or customer_group is null)
and distribution_channel_description = 'Factory House'
and country = 'KR'

-- Other incidents to log:
-- .01 return dollars



select sales_dollars_usd, net_dollars_usd, so_cost_usd, so_demand_pm_value_usd, *
from "ENTERPRISE_PRD_DB"."CONSUMER_SALES"."MDL_CONSUMER_SALES_UNION"
where distribution_channel_description in ('Factory House', 'Brand House')
and distribution_channel_description != 'Wholesale'
and so_demand_pm_value_usd is null
limit 100
