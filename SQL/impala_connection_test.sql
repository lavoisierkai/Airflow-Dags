/* ----- params start -----
# YAML format, only `table_name` is mandatory
database : access_aus_bi
table_name : impala_connection_test
if_exist : append # enums {‘fail’, ‘replace’, ‘append’}, default ‘replace’
# start_date : 2021-03-31T00:00:00+11:00 # default is the fine modification time
schedule_interval : 1-60/15 * * * * # check https://crontab.guru/ to verify your schedule interval
   ----- params end ----- */
SELECT NOW() as test_time_cn