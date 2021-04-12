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
table_name : cbo_report_seller_recruit_f
if_exist : replace 
schedule_interval : 1 8 * * * 

   ----- params end ----- */

SELECT dt AS `date`,
       `level`,
       CASE
         WHEN is_sharer = 1 THEN 'sharer'
         ELSE 'buyer'
       END AS is_sharer,
       sharer_level,
       CASE
         WHEN user_area = '澳新区' THEN 'ANZ'
         WHEN user_area = '中国区' THEN 'CN'
         ELSE 'Other'
       END AS user_region,
       COUNT(*) AS reseller_count
FROM access_cdm.dwt_seller_topic_dd_f
WHERE dt > '2020'
GROUP BY 1, 2, 3, 4, 5 
