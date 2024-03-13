#!/usr/bin/env python
# coding: utf-8


## Plot to show daily average compared to various studies
### This does not have the dataset loaded before, as it was deleted when we got logged out of AWS

import matplotlib.pyplot as plt
import pandas as pd

# Assuming df_hourly_cumulative is defined somewhere in your code
# df_hourly_cumulative = ...

# Convert 'datetime' column to datetime type if it's not already
df_hourly_cumulative['datetime'] = pd.to_datetime(df_hourly_cumulative['datetime'])

# Calculate the number of unique days
num_days = df_hourly_cumulative['datetime'].dt.date.nunique()

# Calculate the daily total value
daily_total_value = df_hourly_cumulative['value'].sum() / num_days

# Static values for other DataFrames
static_values_1999 = {'state': 'California', 'year': 1999, 'total': 69.3}
static_values_2016 = {'state': 'California', 'year': 2016, 'total': 58.6}
static_values_flume_2021 = {'state': 'Flume', 'year': 2021, 'total': 46.5}

# Create DataFrames
df_california_total_1999 = pd.DataFrame([static_values_1999])
df_california_total_2016 = pd.DataFrame([static_values_2016])
df_daily_total = pd.DataFrame({'daily_total_gal': [daily_total_value]})
df_flume_2021 = pd.DataFrame([static_values_flume_2021])

# Add 'daily_total_gal' to existing DataFrames
df_california_total_1999['daily_total_gal'] = df_daily_total['daily_total_gal'].iloc[0]
df_california_total_2016['daily_total_gal'] = df_daily_total['daily_total_gal'].iloc[0]
df_flume_2021['daily_total_gal'] = df_daily_total['daily_total_gal'].iloc[0]

# Plotting
plt.barh(['CA 1999', 'CA 2016', 'Flume 2021', 'Ryan Daily Average'],
         [df_california_total_1999['total'].iloc[0],
          df_california_total_2016['total'].iloc[0],
          df_flume_2021['total'].iloc[0],
          df_daily_total['daily_total_gal'].iloc[0] / 2],
         color=['#4682B4', '#87CEEB', '#2E8B57', '#3CB371'])

# Set background color
plt.gca().set_facecolor('#F0F0F0')  # Light grey background

# Customize the plot
plt.xlabel('Total Gallons')
plt.title('Comparative Bar Chart')
plt.xlim(0)  # Set the x-axis to start from 0

plt.show()

# # Read in water data from S3

# In[88]:


import boto3
import pandas as pd
from sagemaker import get_execution_role
#Correctly define your bucket name and object key
bucket_name = 'flume-analysis'  # Just the bucket name
object_key = 'Combined_Data/combined_20240313.parquet'  # Path to the file within the bucket

#Get the SageMaker execution role and create an S3 client
role = get_execution_role()
s3_client = boto3.client('s3')

#Generate a presigned URL for the S3 object
url = s3_client.generate_presigned_url('get_object',
                                       Params={'Bucket': bucket_name, 'Key': object_key},
                                       ExpiresIn=3600)

#Load the CSV data into a pandas DataFrame
combined_df = pd.read_parquet(url)



combined_df['datetime'] = pd.to_datetime(combined_df['datetime']) # make to datetime variable 
combined_df


# # Shower Counts Analysis

# In[90]:


def shower_time_counts(minute_df):
    # For each day, start with 0 showers
    shower_count = 0
    shower_lengths = []
    times = []

    shower_started = False # shower is off
    shower_start_time = None
    
    
    for index, row in minute_df.iterrows():
        if row['shower'] > 0 and not shower_started: # once shower starts
            shower_started = True
            shower_start_time = row['datetime']
            times.append(row['datetime'])
            shower_count += 1

        elif row['shower'] == 0 and shower_started: # once shower stops
            shower_started = False
            shower_end_time = row['datetime']

            # record shower length
            shower_length = shower_end_time - shower_start_time
            shower_lengths.append(shower_length)

    shower_count
    minutes_lengths = [td.total_seconds()/60 for td in shower_lengths]
    
    return shower_count, minutes_lengths, times


# In[91]:


shower_time_counts(combined_df)


# In[92]:


# Average Shower Time
sum(shower_time_counts(combined_df)[1]) / len(shower_time_counts(combined_df)[1])


# In[93]:


# Average Shower Gallons
combined_df['shower'].sum()/shower_time_counts(combined_df)[0]


# In[94]:


# Plot of History of Showers
from plotnine import *

shower_times = [ts.strftime("%m-%d %H:%M") for ts in shower_time_counts(combined_df)[2]]  # Format to month-day hour:minute
df = pd.DataFrame({'value': shower_time_counts(combined_df)[1],
                  'times': shower_times})

(ggplot(df, aes(x='times', y='value')) + geom_bar(fill='skyblue', color='black', stat='identity') +
 labs(x="",y="Shower Length (Minutes)", title="") +
 theme_bw() + theme(axis_text_x=element_text(angle=45, hjust=1)) +
 geom_hline(yintercept=8, linetype='solid', color='red', size=1.25) +
 theme(plot_background=element_rect(color='#f3f9fc', fill='#f3f9fc'), panel_background=element_rect(color='#f3f9fc', fill='#f3f9fc')))


# # Toilet Flush Analysis

# In[96]:


def toilet_counts(minute_df):
    # For each day, start with 0 showers
    toilet_count = 0
    toilet_started = False 
    toilet_time = []
    toilet_flush = []
    
    
    for index, row in minute_df.iterrows():
        if row['toilet'] > 0 and not toilet_started: 
            toilet_started = True
            toilet_count += 1
            toilet_time.append(row['datetime'])
            toilet_flush.append(1)

        elif row['toilet'] == 0 and toilet_started: # once shower stops
            toilet_started = False

    toilet_count
    
    return toilet_count, toilet_time, toilet_flush


# In[97]:


toilet_counts(combined_df)


# In[98]:


flush_df = pd.DataFrame({'Time': toilet_counts(combined_df)[1], 'Flush': toilet_counts(combined_df)[2]})
flush_df = flush_df.groupby(flush_df['Time'].dt.date)['Flush'].sum().reset_index()
flush_df


# In[99]:


# Plot of Toilet Flushes

flush_times = [ts.strftime("%m-%d") for ts in flush_df['Time']]  # Format to month-day
flush_df['Time'] = flush_times

(ggplot(flush_df, aes(x='Time', y='Flush')) + geom_bar(fill='skyblue', color='black', stat='identity') +
 labs(x="", y="Number of Flushes", title="History of Toilet Flushes") +
 theme_bw() + theme(axis_text_x=element_text(angle=45, hjust=1)) +
 geom_hline(yintercept=10, linetype='solid', color='red', size=1.25) +
 theme(plot_background=element_rect(color='#f3f9fc', fill='#f3f9fc'), panel_background=element_rect(color='#f3f9fc', fill='#f3f9fc')))



# Average gallons of water per flush
combined_df['toilet'].sum()/toilet_counts(combined_df)[0]


# In[102]:


# Average number of flushes per day
toilet_counts(combined_df)[0] / len(flush_df)


# # Washer Load Analysis

# In[103]:


def washer_counts(minute_df):
    # For each day, start with 0 showers
    washer_count = 0
    washer_started = False 
    washer_time = []
    water_use = []
    water_consumption = 0
    
    
    for index, row in minute_df.iterrows():
        if row['washer'] > 0 and not washer_started: 
            washer_started = True
            washer_count += 1
            washer_time.append(row['datetime'])
            water_consumption = water_consumption + row['washer']
            
        elif row['washer'] > 0 and washer_started:
            water_consumption = water_consumption + row['washer']

        elif row['washer'] == 0 and washer_started: # once shower stops
            washer_started = False
            water_use.append(water_consumption)
            water_consumption = 0
            
    return washer_count, washer_time, water_use


# In[104]:


washer_counts(combined_df)


# In[105]:


washer_df = pd.DataFrame({'time':washer_counts(combined_df)[1], 'usage': washer_counts(combined_df)[2]})
washer_df['time'] = washer_df['time'].astype(str)
washer_df


# In[106]:


# Graph of Washer Usage

washer_df['time'] = pd.to_datetime(washer_df['time'])
washer_df['time'] = washer_df['time'].dt.strftime("%m-%d %H:%M")  # Format to month-day hour:minute

(ggplot(washer_df, aes(x='time', y='usage')) + geom_bar(fill='skyblue', stat='identity', color='black') +
 labs(x="", y="Gallons", title="Washer Water Usage") +
 theme_bw() + theme(axis_text_x=element_text(angle=45, hjust=1)) +
 theme(plot_background=element_rect(color='#f3f9fc', fill='#f3f9fc'), panel_background=element_rect(color='#f3f9fc', fill='#f3f9fc')))


# In[108]:


# Average gallons of water per wash
combined_df['washer'].sum()/(washer_counts(combined_df)[0]/2)


# In[109]:


# Data Frame for Proportion Graph
test = combined_df[['datetime', 'washer', 'dishwasher', 'toilet', 'faucet', 'shower']]
test = test.rename(columns = {'faucet': 'miscellaneous'})
test = test[['datetime', 'washer', 'dishwasher', 'toilet', 'miscellaneous', 'shower']]
test = pd.melt(test, id_vars = 'datetime', var_name = 'appliance', value_name = 'usage')
test = test.groupby('appliance')['usage'].sum().reset_index()
total_usage = test['usage'].sum()
test['Proportion'] = test['usage'] / total_usage
test


# In[54]:


# Graph of Proportion of Water Usage by Appliance
(ggplot(test, aes(x='appliance', y='Proportion')) + geom_bar(fill = 'skyblue',stat='identity', color='black')+ labs(x = "Appliance", y = "Proportion of Water Usage", title = "Proportion of Water Usage by Appliance") 
 + theme_bw() + theme(axis_text_x=element_text(angle=45, hjust=1))  
 + theme(plot_background=element_rect(color = '#f3f9fc',fill='#f3f9fc'), panel_background=element_rect(color = '#f3f9fc',fill='#f3f9fc')))


# In[55]:


# Proportion of usage that is by Washer

combined_df['washer'].sum()/combined_df['value'].sum()


# # Breakdown of Daily Proportions Analysis

# In[56]:


daily_df = combined_df.groupby(combined_df['datetime'].dt.date)[['value', 'washer', 'dishwasher', 'toilet','faucet', 'shower']].sum().reset_index()
daily_df


# In[57]:


# Graph of Breakdown of Daily Water Usage
prop_df = daily_df[['datetime','washer','dishwasher', 'toilet', 'faucet', 'shower']]
prop_df.rename(columns={'faucet': 'miscellaneous'}, inplace = True)
prop_df = pd.melt(prop_df, id_vars=['datetime'], value_vars=['washer', 'dishwasher', 'toilet', 'miscellaneous', 'shower'], var_name='appliance', value_name='usage')
prop_df['Appliance'] = prop_df['appliance']


appliance_colors = {
    'washer': '#89c9a8', 
    'dishwasher': '#ed8568',  
    'toilet': '#68b6ed',  
    'miscellaneous': '#edc882', 
    'shower': '#64d8e3' 
}

# appliance_colors = {
#     'washer': '#336e94', 
#     'dishwasher': '#0d07ba',  
#     'toilet': '#1E90FF',  
#     'miscellaneous': '#9ed2f2', 
#     'shower': '#0097a7' 
# }

ggplot(prop_df, aes(x='datetime', y='usage', fill='Appliance')) + \
       geom_bar(stat='identity', color = 'black') + \
       labs(x="Date", y="Gallons", title = "Breakdown of Daily Water Usage") + \
       scale_fill_manual(values=appliance_colors) + theme_bw() + \
       theme(axis_text_x=element_text(angle=45, hjust=1))+ geom_hline(yintercept=60, linetype='solid', color='red', size = 1.25)+ theme(plot_background=element_rect(color = '#f3f9fc',fill='#f3f9fc'), panel_background=element_rect(color = '#f3f9fc',fill='#f3f9fc'), legend_background=element_rect(color = '#f3f9fc',fill='#f3f9fc'))


# In[58]:


# Proportion of Shower, Toilet, and (Shower + Toilet)
tot = prop_df['usage'].sum()

a = prop_df[prop_df['Appliance'] == 'shower']['usage'].sum()
b = prop_df[prop_df['Appliance'] == 'toilet']['usage'].sum()

print((a/tot, b/tot), (a+b)/tot)


# # Hour of the Day Analysis

# In[59]:


hour_df = combined_df.groupby(combined_df['datetime'].dt.hour)[['value', 'washer', 'dishwasher', 'toilet','faucet', 'shower']].sum().reset_index()
hour_df


# In[60]:


# Graph of Breakdown of Hourly Water Usage
prop_df = hour_df[['datetime','washer','dishwasher', 'toilet', 'faucet', 'shower']]
prop_df.rename(columns={'faucet': 'miscellaneous'}, inplace = True)
prop_df = pd.melt(prop_df, id_vars=['datetime'], value_vars=['washer', 'dishwasher', 'toilet', 'miscellaneous', 'shower'], var_name='appliance', value_name='usage')
prop_df['Appliance'] = prop_df['appliance']

appliance_colors = {
    'washer': '#89c9a8', 
    'dishwasher': '#ed8568',  
    'toilet': '#68b6ed',  
    'miscellaneous': '#edc882', 
    'shower': '#64d8e3' 
}

ggplot(prop_df, aes(x='datetime', y='usage', fill='Appliance')) + \
       geom_bar(stat='identity', color = 'black') + \
       labs(x="Hour", y="Gallons", title = "Breakdown of Hourly Water Usage") + \
       scale_fill_manual(values=appliance_colors) + theme_bw() + \
       theme(axis_text_x=element_text(angle=45, hjust=1)) + theme(figure_size = (10,6)) + scale_x_continuous(breaks=range(0,24)) +\
        theme(plot_background=element_rect(color = '#f3f9fc',fill='#f3f9fc'), panel_background=element_rect(color = '#f3f9fc',fill='#f3f9fc'), legend_background=element_rect(color = '#f3f9fc',fill='#f3f9fc'))


