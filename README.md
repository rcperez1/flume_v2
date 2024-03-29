# flume_v2

In this project, data is pulled daily via the Flume Water API to run various analyses on water usage in my home. The water usage was integrated along with gas/electric usage and temperature data to determine correlation between utilities and weather.

In addition to the PowerPoint, there is a folder with various .py files:

# glue_api_to_s3.py
Script to query data from Flume API once a day, using a SecretManager secret for login password.
Dumped files into an s3 bucket folder once a day, named by the date the file represents

# glue_s3_aggregate_daily_dfs.py
Script to then combine daily values from previous trigger in order to create a single parquet file to run analysis

# flume_water_analytics.py
All analysis that extended on what the Flume dashboard already provides

# flume_utilities_temp.py
All analyses related to other utilities as well as temperature
