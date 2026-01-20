#import necessary modules. Sys module is used to handle command line 
#arguments
import sys

import pandas as pd


#Here we print the command line arguments to verify input
print("arguments", sys.argv)

#Here we extract the month number from command line arguments
month = int(sys.argv[1])

#Here we create a sample dataframe and save it as a parquet file
df = pd.DataFrame({"day": [1, 2], "Num_passengers": [3, 4]})
df['month'] = month


#Here we print the first few rows of the dataframe
print(df.head())

#Here we save the dataframe to a parquet file named according to the
# month. Parquet is a columnar storage file format. It is a binary file format
# that is optimized for use with big data processing frameworks.

df.to_parquet(f"output_month_{month}.parquet")

print(f"Running pipeline for month {month}")