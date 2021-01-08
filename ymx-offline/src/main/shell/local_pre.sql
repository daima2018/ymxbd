A:
select 
     t2.site
    ,t1.user_account
    ,t1.asin
    ,t1.seller_sku
    ,t1.is_parent
    ,t2.currency_code currency_site   --站点币种  
    ,t3.currency_local                --本位币
from ec_amazon_parent_listing t1 
left join ec_platform_user t2
on t1.user_account = t2.user_account
left join ec_company t3
on t1.company_code = t3.company_code
where t1.company_code != null and t1.company_code != 'A20080135'

B1:
select 
     tt1.site
    ,tt1.user_account
    ,tt2.asin1            as asin 
    ,tt2.seller_sku    
    ,1 as asin_type
    ,tt1.currency_site   
    ,tt1.currency_local           
from A tt1 join ec_amazon_get_merchant_listings_data tt2
on tt1.is_parent=1 
    and tt2.parent_asin != tt2.asin1 
    and tt2.item_status='on_sale'
    and tt1.user_account=tt2.user_account
    and tt1.asin=tt2.parent_asin

B2:
select 
     tt1.site
    ,tt1.user_account
    ,tt1.asin
    ,tt1.seller_sku    
    ,2 as asin_type
    ,tt1.currency_site   
    ,tt1.currency_local     
from A tt1
where tt1.is_parent!=1 

B: 
B1 union all B2

C:
select 
     ttt1.site
    ,ttt1.user_account
    ,ttt1.asin
    ,ttt1.seller_sku    
    ,ttt1.asin_type
    ,ttt1.currency_site   
    ,ttt1.currency_local 
    ,min(case when ttt1.currency_site = ttt1.currency_local then 1 
          when ttt1.currency_local= ttt2.crrd_currency_code then ttt2.crrd_currency_rate
     end)  as local_rate 
    ,min(case when ttt2.crrd_currency_code='USD' then ttt2.ccrd_currency_rate end) as usd_rate
    ,min(case when ttt2.crrd_currency_code='EUR' then ttt2.ccrd_currency_rate end) as eur_rate
    ,min(case when ttt2.crrd_currency_code='GBP' then ttt2.ccrd_currency_rate end) as gbp_rate
    ,min(case when ttt2.crrd_currency_code='JPY' then ttt2.ccrd_currency_rate end) as jpy_rate
from B ttt1 left join 汇率 ttt2                --汇率有多条会发散,聚合取一条
on ttt1.currency_site = ttt2.crr_local
group by  ttt1.site
        ,ttt1.user_account
        ,ttt1.asin
        ,ttt1.seller_sku    
        ,ttt1.asin_type
        ,ttt1.currency_site   
        ,ttt1.currency_local 

D:
select                                                                                                          
     c.user_account                                           -- '店铺账号'                                                                                                   
    ,c.site                                                   --  '站点'                                                                                          
    ,c.seller_sku                                             --                                                                                                  
    ,c.asin                                                   --                                                                                            
    ,d.sale_amount as qty                                     -- '销售数量',                                                                                              
    ,'' as summary_date                                           -- '统计日期',                                                                                                    
    ,nvl(case when local_rate=1 then floor(100*d.sale_money_original)/100
              else floor(100*d.sale_money_original/c.local_rate)/100
         end,0) as sale_amount                                                      -- '销售额(本位币)',                                                                                                   
    ,nvl(floor(100*d.sale_money_original/c.usd_rate)/100,0) as  sale_amount_usd     -- '销售额美元',                                                                                                       
    ,nvl(floor(100*d.sale_money_original/c.eur_rate)/100,0) as  sale_amount_eur     -- '销售额欧元',                                                                                                       
    ,nvl(floor(100*d.sale_money_original/c.gbp_rate)/100,0) as  sale_amount_gbp     -- '销售额英镑',                                                                                                       
    ,nvl(floor(100*d.sale_money_original/c.jpy_rate)/100,0) as  sale_amount_jpy     -- '销售额日元',                                                                                                       
    ,nvl(d.sale_money_original,0) as sale_amount_original                           -- '销售额（站点原始金额）',                                                                                                            
    ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as created_time         -- '创建时间',                                                                                                    
    ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as updated_time         -- '更新时间',                                                                                                    
    ,d.sale_order_num                                                               -- '订单数',                                                                                                      
    ,f.refund_amount                                                                -- '退款listing个数',                                                                                                     
    ,nvl(floor(100*f.refund_money)/100           ,0)                                -- '退款金额(站点币种)',                                                                                                    
    ,nvl(floor(100*f.refund_money/c.usd_rate)/100,0) as refund_money_usd            -- '退款金额美元',                                                                                                        
    ,nvl(floor(100*f.refund_money/c.eur_rate)/100,0) as refund_money_eur            -- '退款金额欧元',                                                                                                        
    ,nvl(floor(100*f.refund_money/c.gbp_rate)/100,0) as refund_money_gbp            -- '退款金额英镑',                                                                                                        
    ,nvl(floor(100*f.refund_money/c.jpy_rate)/100,0) as refund_money_jpy            -- '退款金额日元',                                                                                                        
    ,nvl(case when local_rate=1 then floor(100*f.refund_money)/100
              else floor(100*f.refund_money/c.local_rate)/100
         end,0) as refund_money_local                                              -- '退款金额(本位币)',                                                                                                          
    ,md5(concat(c.user_account,c.asin,c.seller_sku)) as key1                       -- 'user_account+asin+seller_sku 的MD5值',                                                                                            
    ,g.return_amount                                                               -- '退货数',                                                                                                     
    ,c.asin_type                                                                   -- '0未确定, 子asin:1，独立产品:2, 父asin3',                                                                                                 
    ,e.ad_sale_amount as ad_qty                                                    -- '广告销售数量',                                                                                              
    ,nvl(case when local_rate=1 then floor(100*e.ad_sale_money_original)/100
              else floor(100*e.ad_sale_money_original/c.local_rate)/100
         end,0) as ad_sale_amount                                                         -- '广告销售额(本位币)',                                                                                                      
    ,nvl(floor(100*e.ad_sale_money_original/c.usd_rate)/100,0) as ad_sale_amount_usd      -- '广告销售额美元',                                                                                                          
    ,nvl(floor(100*e.ad_sale_money_original/c.eur_rate)/100,0) as ad_sale_amount_eur      -- '广告销售额欧元',                                                                                                          
    ,nvl(floor(100*e.ad_sale_money_original/c.gbp_rate)/100,0) as ad_sale_amount_gbp      -- '广告销售额英镑',                                                                                                          
    ,nvl(floor(100*e.ad_sale_money_original/c.jpy_rate)/100,0) as ad_sale_amount_jpy      -- '广告销售额日元',                                                                                                          
    ,nvl(e.ad_sale_order_num,0)                                as ad_sale_order_num       -- '广告订单数',                                                                                                         
    ,nvl(e.ad_sale_money_original,0)                           as ad_sale_amount_original -- '广告销售额（站点原始金额）',                                                                                                               
    ,nvl(e.cost,0)                                             as cost                    -- '广告花费(站点币种)',                                                                                            
    ,nvl(case when local_rate=1 then floor(100*e.cost)/100
              else floor(100*e.cost/c.local_rate)/100
         end,0) as cost_local                                              -- '广告花费(本位币)',                                                                                                  
    ,nvl(floor(100*e.cost/c.usd_rate)/100,0) as cost_usd                                                                                                                
    ,nvl(floor(100*e.cost/c.eur_rate)/100,0) as cost_eur                                                                                                               
    ,nvl(floor(100*e.cost/c.gbp_rate)/100,0) as cost_gbp                                                                                                               
    ,nvl(floor(100*e.cost/c.jpy_rate)/100,0) as cost_jpy                                                                                                               
    ,nvl(e.clicks     ,0) as clicks                           -- '广告访问次数,也即广告点击数'                                                                                             
    ,nvl(e.impressions,0) as impressions                      -- 广告曝光量                                                                                                  
    ,nvl(h.sessions             ,0) as sessions                 -- 访客次数                                                                                               
    ,nvl(h.page_views           ,0) as page_views               --浏览次数                                                                                                  
    ,nvl(h.buy_box_percentage   ,0) as buy_box_percentage       --  buy_box_percentage                                                                                                        
    ,nvl(h.session_percentage   ,0) as session_percentage       --买家访问次数比率                                                                                                          
    ,nvl(h.page_views_percentage,0) as page_views_percentage    --   浏览次数比率        
from C c 
left join 订单 d
on  c.user_account = d.user_account
    and c.asin = d.asin        
    and c.seller_sku = d.seller_sku  
left join 广告 e
on c.user_account = e.user_account
    and c.asin = e.asin     
    and c.seller_sku = e.sku 
left join 退款 f 
on c.user_account = f.user_account
    and c.seller_sku = f.sku 
left join 退货 g 
on c.user_account = g.user_account
    and c.asin = g.asin     
    and c.seller_sku = g.sku 
left join 流量报告 h 
on  c.user_account = h.user_account
    and c.asin = h.child_asin        
    and c.seller_sku = h.seller_sku  


--ec_amazon_listing_extend_summary_local  asin_type应该没有为0的,

--获取公司的本地币种    company_status     客户状态 0=已注销 1=待激活 2=已停用 3=已激活
select currency_local from ec_company where company_code='' 
--获取站点的币种   $currencyOriginal
select currency_code from ec_platform_user where user_account='' 
status=1 and  is_del=0

--汇率
select 
     a.crr_local  --本位币
    ,b.crrd_currency_code  --币种
    ,b.crrd_currency_rate  --汇率
from ec_currency_rate_rule a
left join ec_currency_rate_rule_detail b
on a.crr_id=b.crr_id
where a.crr_date=''

--订单
SELECT 
   aod.user_account  --'店铺账号',
  ,aod.asin          -- '商品的亚马逊商品编码',
  ,aod.seller_sku    --'销售SKU',
  ,COUNT(DISTINCT aod.amazon_order_id) AS sale_order_num  --订单数
  ,IFNULL(SUM(aod.quantity_ordered), 0) AS sale_amount               --销售额(本位币)  需要关联汇率表得到其他币种的金额
  ,IFNULL(SUM(aod.item_sale_amount), 0) AS sale_money_original  
FROM  ec_amazon_order_original aoo  
JOIN ec_amazon_order_detail aod 
    ON aoo.aoo_id = aod.aoo_id 
where date_format(purchase_date_local,'%Y-%m-%d') = ''
    and aoo.order_status != 'Canceled' 
group by   aod.user_account
		  ,aod.asin        
  		  ,aod.seller_sku  

--广告
SELECT 	
     user_account
    ,asin
    ,sku
    ,IFNULL(SUM(conversions7d_same_sku),0) AS ad_sale_order_num
    ,IFNULL(SUM(units_ordered7d_same_sku),0) AS ad_sale_amount
    ,IFNULL(SUM(sales7d_same_sku), 0) AS ad_sale_money_original
    ,IFNULL(SUM(impressions), 0) AS impressions
    ,IFNULL(SUM(clicks), 0) AS clicks
    ,IFNULL(SUM(cost), 0) AS cost
FROM product_ad_products_report_daily
WHERE generated_date=?
group by user_account
        ,asin
        ,sku


--refund
SELECT 
    user_account
    ,sku
    ,IFNULL(SUM(quantity_purchased),0) AS refund_amount
    ,IFNULL(SUM(amount),0) AS refund_money 
from ec_amazon_v2_settlement_detail 
WHERE transaction_type='Refund' 
    AND posted_date_time>=?     --$localStart = date('Y-m-d 00:00:00',strtotime($date));
                                --$start = CommonService::siteDateConversionZone($localStart,'CN','UTC');
    AND posted_date_time<=? 
group by user_account
        ,sku    

from_utc_timestamp(to_utc_timestamp("2021-01-08 00:00:00","GMT+8"),"UTC")
from_utc_timestamp(to_utc_timestamp("2021-01-08 23:59:59","GMT+8"),"UTC")

--退货
SELECT 
     user_account
    ,asin
    ,sku
    ,IFNULL(sum(quantity),0) as return_amount
FROM ec_amazon_fba_fulfillment_customer_returns_data
WHERE  return_date>=?  -- $localStart = date('Y-m-d 00:00:00',strtotime($date));
                    -- $start = CommonService::siteDateConversionZone($localStart,'CN','UTC','Y-m-d\TH:i:s+00:00');
AND return_date<=?
group by user_account
        ,asin
        ,sku

--流量报告 子
SELECT 
     IFNULL(sessions,0) as sessions
    ,IFNULL(page_views,0) as page_views 
    ,IFNULL(buy_box_percentage,0) as buy_box_percentage
    ,IFNULL(session_percentage,0) as session_percentage
    ,IFNULL(page_views_percentage,0) as page_views_percentage
from ec_amazon_business_report_by_child
WHERE generate_date=? 
    AND user_account=? 
    AND child_asin=? 
    AND seller_sku=? 
      limit 1

SELECT 
     user_account 
    ,child_asin 
    ,seller_sku 
    ,IFNULL(max(sessions),0) as sessions                 --取一条即可
    ,IFNULL(max(page_views),0) as page_views 
    ,IFNULL(max(buy_box_percentage),0) as buy_box_percentage
    ,IFNULL(max(session_percentage),0) as session_percentage
    ,IFNULL(max(page_views_percentage),0) as page_views_percentage
from ec_amazon_business_report_by_child
WHERE generate_date=? 
group by user_account 
        ,child_asin 
        ,seller_sku 

--流量报告 父
SELECT 
     IFNULL(sessions,0) as sessions
    ,IFNULL(page_views,0) as page_views 
    ,IFNULL(buy_box_percentage,0) as buy_box_percentage
    ,IFNULL(session_percentage,0) as session_percentage
    ,IFNULL(page_views_percentage,0) as page_views_percentage
from ec_amazon_business_report_by_parent
WHERE generate_date=? 
    AND user_account=?
    AND parent_asin=?  
      limit 1      

SELECT 
     user_account
    ,parent_asin
    ,IFNULL(max(sessions),0) as sessions
    ,IFNULL(max(page_views),0) as page_views 
    ,IFNULL(max(buy_box_percentage),0) as buy_box_percentage
    ,IFNULL(max(session_percentage),0) as session_percentage
    ,IFNULL(max(page_views_percentage),0) as page_views_percentage
from ec_amazon_business_report_by_parent
WHERE generate_date=? 
group by user_account
        ,parent_asin
    