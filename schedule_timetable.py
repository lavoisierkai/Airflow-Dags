from datetime import datetime, timedelta, date

schedule_interval1="*/30 08,12 * * *"
schedule_interval2="*/5 08,12 * * *"
dt=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")