/* ----- params start -----
# YAML format, only `table_name` is mandatory
#
# ### Sample params #####
# database : access_aus_bi
# table_name : impala_connection_test
# if_exist : append # enums {‘fail’, ‘replace’, ‘append’}, default ‘replace’
# start_date : 2021-03-31T00:00:00+11:00 # optional, default is the fine modification time
# schedule_interval : 1 8 * * * # check https://crontab.guru/ to verify your schedule interval

database : access_aus_bi
table_name : cbo_report_seller_recruit_i
if_exist : replace 
schedule_interval : 1 8 * * * 

   ----- params end ----- */

WITH t_user_dim AS
(
  SELECT dt,
  		 id_code,
         CASE
           WHEN is_sharer = 1 THEN 'sharer'
           ELSE 'buyer'
         END AS is_sharer,
         sharer_level,
         CASE
           WHEN user_area = '澳新区' THEN 'ANZ'
           WHEN user_area = '中国区' THEN 'CN'
           ELSE 'Other'
         END AS user_region
  FROM access_cdm.dwt_seller_topic_dd_f
  WHERE dt > '2020'
)
SELECT to_date (tmain.audit_at) AS `date`,
       t_user_dim.is_sharer,
       t_user_dim.sharer_level,
       t_user_dim.user_region,
       tmain.applay_level AS level_post,
       tmain.old_level AS level_pre,
       tmain.`type` AS level_change_type /* 升级类型 0注册,1充值,2活动,3业绩,4招募,5降级,6积分 */,
       COUNT(*) AS reseller_count
FROM access_cdm.dwd_seller_level_applay_dd_f AS tmain
  LEFT JOIN t_user_dim
         ON t_user_dim.dt = to_date (tmain.audit_at)
        AND t_user_dim.id_code = tmain.id_code
WHERE tmain.dt = to_date(now() - INTERVAL 1 day)
AND   tmain.audit_at > '2020'
AND   tmain.status = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7
order by `date`;