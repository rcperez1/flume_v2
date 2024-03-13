import sys
import os
import pandas as pd
import pyarrow.parquet as pq
import s3fs

# Initialize S3 filesystem
fs = s3fs.S3FileSystem()

# Define your input and output directories
input_directory = 's3://flume-analysis/Appliance_data/'
output_directory = 's3://flume-analysis/Combined_Data/'

# List files in the input directory
input_files = [file for file in fs.ls(input_directory) if file.endswith('.parquet')]

# Initialize an empty DataFrame
combined_df = pd.DataFrame()

# Read each Parquet file and append to the combined DataFrame
for file in input_files:
    file_path = f's3://{file}'
    df = pd.read_parquet(file_path, engine='pyarrow')
    combined_df = combined_df.append(df)

# Write the combined DataFrame to a new Parquet file in the output directory
# You can include the current date in the filename to avoid overwriting existing files
from datetime import datetime
output_file = f'{output_directory}combined_{datetime.now().strftime("%Y%m%d")}.parquet'

# Write to Parquet
combined_df.to_parquet(output_file, engine='pyarrow', index=False)