"""
fire_pollution_analysis.py
Author: Sparsh Sharma

Description:
This script reads wildfire and pollution datasets, 
joins them based on state and date proximity, 
calculates distances between fire incidents and pollution measurement points, 
filters by distance and relevant columns, and writes the output to Parquet.

Key Features:
- Data cleaning and preprocessing
- State name standardization
- Date conversions and filtering
- Haversine distance calculation
- Distance and date-based filtering
- Output to Parquet
"""

# -------------------------
# Setup Spark Session
# -------------------------
try:
    spark.stop()  # Stop existing session if present
except NameError:
    pass

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, col, when, year, month, datediff

spark = SparkSession.builder \
    .appName("FirePollutionAnalysis") \
    .config("spark.driver.host", "localhost") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# -------------------------
# Load CSV Data
# -------------------------
path_fires = r'fire_data_v2.csv'
path_pollution = r'pollution_data_v2.csv'

df_fires = spark.read.csv(path_fires, header=True, inferSchema=True)
df_pollution = spark.read.csv(path_pollution, header=True, inferSchema=True)

# -------------------------
# Preprocess Pollution Data
# -------------------------
df_pollution = df_pollution.withColumn(
    "Date_Local", to_date(col("Date Local"), "M/d/yy")
).dropna(subset=['Final_Latitude', 'Final_Longitude'])

# Filter for relevant states
df_pollution = df_pollution.filter(col("State").isin(['Arizona', 'California']))

# -------------------------
# Preprocess Fire Data
# -------------------------
state_mapping = {'CA': 'California', 'AZ': 'Arizona'}

for abbrev, full_name in state_mapping.items():
    df_fires = df_fires.withColumn(
        "STATE", when(col("STATE") == abbrev, full_name).otherwise(col("STATE"))
    )

df_fires = df_fires.withColumnRenamed("STATE", "Fire_State") \
                   .withColumnRenamed("COUNTY", "Fire_County") \
                   .withColumnRenamed("_c0", "Fire_ID") \
                   .withColumn("DISCOVERY_DATE", to_date("DISCOVERY_DATE")) \
                   .withColumn("CONT_DATE", to_date("CONT_DATE"))

# -------------------------
# Join DataFrames on State
# -------------------------
df_joined = df_fires.join(df_pollution, df_fires["Fire_State"] == df_pollution["State"])

# Filter rows within ±7 days of start_date
df_filtered = df_joined.filter(
    (col("Date_Local") >= col("start_date") - F.expr("INTERVAL 7 DAY")) &
    (col("Date_Local") <= col("start_date") + F.expr("INTERVAL 7 DAY"))
)

# -------------------------
# Define Haversine Function
# -------------------------
def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate great-circle distance in miles between two points
    using latitude and longitude in degrees.
    """
    R = 3958.8  # Earth radius in miles
    return F.acos(
        F.sin(F.radians(lat1)) * F.sin(F.radians(lat2)) +
        F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) *
        F.cos(F.radians(lon2 - lon1))
    ) * R

# -------------------------
# Calculate Distance and Filter
# -------------------------
df_filtered = df_filtered.withColumn(
    "distance_miles",
    haversine(F.col("LATITUDE"), F.col("LONGITUDE"), F.col("Final_Latitude"), F.col("Final_Longitude"))
)

filtered_df_20 = df_filtered.filter(F.col("distance_miles") < 20)
filtered_df_20 = filtered_df_20.na.drop(how='any', subset=['CO AQI', 'SO2 AQI'])

# -------------------------
# Date Calculations
# -------------------------
filtered_df_20 = filtered_df_20.withColumn("start_date", to_date("start_date"))
filtered_df_20 = filtered_df_20.withColumn("Date_Local", to_date("Date_Local"))
filtered_df_20 = filtered_df_20.withColumn("date_diff_fire_pollution", datediff("Date_Local", "start_date"))
filtered_df_20 = filtered_df_20.withColumn("FIRE_MONTH", month("start_date"))

# -------------------------
# Filter by Years and Specific Date Range
# -------------------------
filtered_years_df = filtered_df_20.filter(filtered_df_20["FIRE_YEAR"].between(2008, 2014))
filtered_years_df = filtered_years_df.filter(
    (col("start_date") >= "2014-05-02") & (col("start_date") <= "2014-06-02")
)

# -------------------------
# Write Output
# -------------------------
filtered_years_df.write.mode("overwrite").parquet(r"output")

# Stop Spark session
spark.stop()
