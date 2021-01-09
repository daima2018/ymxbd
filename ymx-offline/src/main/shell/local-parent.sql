C:
select
      ttt1.site
     ,ttt1.user_account
     ,ttt1.parent_asin
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
from (select
          tt1.site
           ,tt1.user_account
           ,tt1.asin   as parent_asin
           ,tt2.asin1            as asin
           ,tt2.seller_sku
           ,1 as asin_type
           ,tt1.currency_site
           ,tt1.currency_local
      from (select
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
           ) tt1 join ec_amazon_get_merchant_listings_data tt2
                      on tt1.is_parent=1
                          and tt2.parent_asin != tt2.asin1
                          and tt2.item_status='on_sale'
                          and tt1.user_account=tt2.user_account
                          and tt1.asin=tt2.parent_asin

      union all

      select
          tt1.site
              ,tt1.user_account
              ,tt1.asin   as parent_asin
              ,tt1.asin
              ,tt1.seller_sku
              ,2 as asin_type
              ,tt1.currency_site
              ,tt1.currency_local
      from (select
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
          ) tt1
      where tt1.is_parent!=1
) ttt1 left join (
    select
        a.crr_local  --本位币
         ,b.crrd_currency_code  --币种
         ,b.crrd_currency_rate  --汇率
    from ec_currency_rate_rule a
             left join ec_currency_rate_rule_detail b
                       on a.crr_id=b.crr_id
    where a.crr_date=''
) ttt2                --汇率有多条会发散,聚合取一条
on ttt1.currency_site = ttt2.crr_local      --会导致数据倾斜
group by  ttt1.site
       ,ttt1.user_account
       ,ttt1.parent_asin
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
left join local_child d
on  c.user_account = d.user_account
    and c.site = d.site
    and c.asin = d.asin
    and c.seller_sku = d.seller_sku
left join (SELECT
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
) h
on  c.user_account = h.user_account
    and c.asin = h.child_asin
    and c.seller_sku = h.seller_sku
