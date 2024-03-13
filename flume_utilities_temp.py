#!/usr/bin/env python
# coding: utf-8


# Ryan's Correlation Work
# In[1]:


import boto3
import pandas as pd
from sagemaker import get_execution_role

# Reading in water data
#Correctly define your bucket name and object key
bucket_name = 'flume-analysis'  # Just the bucket name
object_key = 'Combined_Data/combined_20240311.parquet'  # Path to the file within the bucket

#Get the SageMaker execution role and create an S3 client
role = get_execution_role()
s3_client = boto3.client('s3')

#Generate a presigned URL for the S3 object
url = s3_client.generate_presigned_url('get_object',
                                       Params={'Bucket': bucket_name, 'Key': object_key},
                                       ExpiresIn=3600)

#Load the CSV data into a pandas DataFrame
combined_df = pd.read_parquet(url)
combined_df = combined_df.rename(columns={'value': 'usage'})


# In[2]:


# Reorder columns

columns_order = [col for col in combined_df.columns if col not in ['shower', 'toilet', 'dishwasher', 'washer', 'faucet']]
water_df = combined_df[columns_order]

water_df = water_df.copy()
water_df['datetime'] = pd.to_datetime(water_df['datetime'])
water_df = water_df.resample('h', on='datetime').sum().reset_index()

water_df.loc[:, 'units'] = 'gallons'
water_df


# In[3]:


import pandas as pd

# Assuming water_df is already defined

# Make a copy of the dataframe to avoid modifying the original one
water_df_copy = water_df.copy()

# Extract date component from the datetime column
water_df_copy['date'] = water_df_copy['datetime'].dt.date

# Calculate the total usage
total_usage = water_df_copy['usage'].sum()

# Determine the number of unique days
unique_days = water_df_copy['date'].nunique()

# Calculate the average usage per day
average_usage_per_day = total_usage / unique_days

# Create a new dataframe to store the result
summary_df = pd.DataFrame({
    'Total Usage': [total_usage],
    'Unique Days': [unique_days],
    'Average Usage per Day': [average_usage_per_day]
})

print(summary_df)


# In[4]:


# Reading in electric data
bucket_name = 'flume-analysis'  # Just the bucket name
object_key = 'utilities/ryan_electric.csv'  # Path to the file within the bucket

#Get the SageMaker execution role and create an S3 client
role = get_execution_role()
s3_client = boto3.client('s3')

#Generate a presigned URL for the S3 object
url = s3_client.generate_presigned_url('get_object',
                                       Params={'Bucket': bucket_name, 'Key': object_key},
                                       ExpiresIn=3600)

#Load the CSV data into a pandas DataFrame
electric_df = pd.read_csv(url)

# Concatenate the 'DATE' and 'START TIME' columns and convert to datetime format
electric_df['datetime'] = pd.to_datetime(electric_df['date'] + ' ' + electric_df['start time'])

# Reorder the columns to place 'datetime' at the front
columns_order = ['datetime'] + [col for col in electric_df.columns if col not in ['datetime', 'date', 'start time']]
electric_df = electric_df[columns_order]
electric_df = electric_df.rename(columns={'UNITS': 'units'})

electric_df


#Reading in gas data
bucket_name = 'flume-analysis'  # Just the bucket name
object_key = 'utilities/ryan_gas.xlsx'  # Path to the file within the bucket

#Get the SageMaker execution role and create an S3 client
role = get_execution_role()
s3_client = boto3.client('s3')

#Generate a presigned URL for the S3 object
url = s3_client.generate_presigned_url('get_object',
                                       Params={'Bucket': bucket_name, 'Key': object_key},
                                       ExpiresIn=3600)

#Load the CSV data into a pandas DataFrame
gas_df = pd.read_excel(url)

# Delete extra hour that shouldn't exist due to DST
gas_df = gas_df.drop(index=170)

# Adding in units for gas
gas_df['units'] = 'therms'

# Concatenate the 'DATE' and 'START TIME' columns and convert to datetime format
gas_df['datetime'] = pd.to_datetime(gas_df['date'] + ' ' + gas_df['time'], format='%m/%d/%Y %I:%M %p')

# Reorganizing columns
columns_order = ['datetime'] + [col for col in gas_df.columns if col not in ['datetime', 'date', 'time']]
gas_df = gas_df[columns_order]


gas_df


# In[7]:


from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score


# In[8]:

# Merging utilities data sets
df_merged = pd.merge(water_df, electric_df, on='datetime', how='outer')
df_merged = pd.merge(df_merged, gas_df, on='datetime', how='outer')

# Rename the columns to more relevant names
df_merged.rename(columns={'usage_x': 'gallons', 'usage_y': 'kWh', 'usage': 'therms'}, inplace=True)

# Creating one row per datetime stamp that includes the following additional values
df_merged = df_merged[['datetime', 'gallons', 'kWh', 'therms', 'temp']]
df_merged = df_merged.drop(index=191)
df_merged


import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates  # Importing the necessary module for date formatting


# Water vs Temperature Time Series
        
# Now proceed with the plotting assuming 'datetime' is the index
fig, ax1 = plt.subplots(figsize=(10, 6))

color = 'tab:blue'
ax1.set_ylabel('Gallons', color=color)

# Plotting a line graph for gallons with thicker line
ax1.plot(df_merged.index, df_merged['gallons'], label='Gallons', color=color, linewidth=2)

# Filling the space below the line with the same color
ax1.fill_between(df_merged.index, df_merged['gallons'], color=color, alpha=0.3)

# Creating a second axis for temperature
ax2 = ax1.twinx()

color = 'tab:red'
ax2.set_ylabel('Temperature', color=color)
ax2.plot(df_merged.index, df_merged['temp'], label='Temperature', color=color, linewidth=2)
ax2.tick_params(axis='y', labelcolor=color)

# Format x-axis labels to show only month and date
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))

# Setting background color to light grey
ax1.set_facecolor('#F0F0F0')

plt.title('Time Series Data - Water Usage & Average Hourly Temperature')
fig.tight_layout()
plt.show()


# Time series for Electric vs Temp        
# Now proceed with the plotting assuming 'datetime' is the index
fig, ax1 = plt.subplots(figsize=(10, 6))

# kWh
color_kwh = 'tab:orange'
ax1.set_ylabel('kWh', color=color_kwh)

# Plotting a line graph for kWh
line_kwh = ax1.plot(df_merged.index, df_merged['kWh'], label='kWh', color=color_kwh)[0]

# Filling the space below the line with the same color
ax1.fill_between(df_merged.index, df_merged['kWh'], color=color_kwh, alpha=0.3)

# Set the color of the y-axis tick labels to match the kWh label
ax1.tick_params(axis='y', labelcolor=color_kwh)

# Set the color of the y-axis numbers to match the kWh label
ax1.yaxis.get_offset_text().set_color(color_kwh)
ax1.yaxis.get_offset_text().set_alpha(0.7)

# Creating a second axis for temperature
ax2 = ax1.twinx()

color_temp = 'tab:red'
ax2.set_ylabel('Temperature', color=color_temp)
ax2.plot(df_merged.index, df_merged['temp'], label='Temperature', color=color_temp)
ax2.tick_params(axis='y', labelcolor=color_temp)

# Format x-axis labels to show only month and date
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))

plt.title('Time Series Data - Electricity Usage and Average Hourly Temperature')
fig.tight_layout()
plt.show()


### Electricity and Gas Utility Graph

#Load the CSV data into a pandas DataFrame
combined_df = pd.read_parquet(url)
combined_df = combined_df.rename(columns={'value': 'usage'})


# Reorder columns
columns_order = [col for col in combined_df.columns if col not in ['shower', 'toilet', 'dishwasher', 'washer', 'faucet']]
water_df = combined_df[columns_order]

water_df = water_df.copy()
water_df['datetime'] = pd.to_datetime(water_df['datetime'])
water_df = water_df.resample('h', on='datetime').sum().reset_index()

water_df.loc[:, 'units'] = 'gallons'
water_df



### Reading in electric data
bucket_name = 'flume-analysis'  # Just the bucket name
object_key = 'utilities/ryan_electric.csv'  # Path to the file within the bucket

#Get the SageMaker execution role and create an S3 client
role = get_execution_role()
s3_client = boto3.client('s3')

# Generate a presigned URL for the S3 object
url = s3_client.generate_presigned_url('get_object',
                                       Params={'Bucket': bucket_name, 'Key': object_key},
                                       ExpiresIn=3600)

# Load the CSV data into a pandas DataFrame
electric_df = pd.read_csv(url)



# Concatenate the 'DATE' and 'START TIME' columns and convert to datetime format
electric_df['datetime'] = pd.to_datetime(electric_df['date'] + ' ' + electric_df['start time'])

# Reorder the columns to place 'datetime' at the front
columns_order = ['datetime'] + [col for col in electric_df.columns if col not in ['datetime', 'date', 'start time']]
electric_df = electric_df[columns_order]
electric_df = electric_df.rename(columns={'UNITS': 'units'})

electric_df


### Reading in gas data
bucket_name = 'flume-analysis'  # Just the bucket name
object_key = 'utilities/ryan_gas.xlsx'  # Path to the file within the bucket

# Get the SageMaker execution role and create an S3 client
role = get_execution_role()
s3_client = boto3.client('s3')

# Generate a presigned URL for the S3 object
url = s3_client.generate_presigned_url('get_object',
                                       Params={'Bucket': bucket_name, 'Key': object_key},
                                       ExpiresIn=3600)

# Load the CSV data into a pandas DataFrame
gas_df = pd.read_excel(url)

# Delete extra hour that shouldn't exist due to DST
gas_df = gas_df.drop(index=170)

# Adding in units for gas
gas_df['units'] = 'therms'

# Concatenate the 'DATE' and 'START TIME' columns and convert to datetime format
gas_df['datetime'] = pd.to_datetime(gas_df['date'] + ' ' + gas_df['time'], format='%m/%d/%Y %I:%M %p')

# Reorganizing columns
columns_order = ['datetime'] + [col for col in gas_df.columns if col not in ['datetime', 'date', 'time']]
gas_df = gas_df[columns_order]


gas_df


# In[32]:


## Prepping for merging data
df_merged = pd.merge(water_df, electric_df, on='datetime', how='outer')
df_merged = pd.merge(df_merged, gas_df, on='datetime', how='outer')

# Rename the columns to more meaningful names
df_merged.rename(columns={'usage_x': 'gallons', 'usage_y': 'kWh', 'usage': 'therms'}, inplace=True)

# Now select only the columns you want to keep
df_merged = df_merged[['datetime', 'gallons', 'kWh', 'therms', 'temp']]
df_merged = df_merged.drop(index=191)
df_merged


# In[34]:


# Plotting
import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(12, 6))

# Plot kWh
ax.plot(df_merged['datetime'], df_merged['kWh'], label='Electricity (kWh)', color='tab:orange')

# Plot therms
ax.plot(df_merged['datetime'], df_merged['therms'], label='Natural Gas (therms)', color='tab:green')

# Set labels and title
ax.set_xlabel('Datetime')
ax.set_ylabel('Consumption')
ax.set_title('Electricity and Natural Gas Consumption Over Time')
ax.legend()

# Rotate x-axis labels for better readability
plt.xticks(rotation=45, ha='right')

# Show plot
plt.show()



# Calculate the correlation matrix
correlation_df = df_merged
correlation_matrix = correlation_df.corr()

# Display the correlation matrix
print(correlation_matrix)