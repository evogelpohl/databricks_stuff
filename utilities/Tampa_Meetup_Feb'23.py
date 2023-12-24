# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Tampa Data Meetup - Feb '23
# MAGIC #### Databricks & PowerBI w/ ChatGPT

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img src = 'https://raw.githubusercontent.com/Evogelpohl/linkArtifacts/main/mj02.jpg' >
# MAGIC
# MAGIC
# MAGIC <b>Midjourney</b>: prompt: group of 30 different cartoonish happy and impressed robots sitting at a presentation meeting room looking at the speaker on stage large room. facing the speaker. vfx. sharp focus --ar 16:9 --s 250

# COMMAND ----------

# MAGIC %md
# MAGIC # WARNING: LLMs are not always right. 
# MAGIC
# MAGIC <img src='https://raw.githubusercontent.com/Evogelpohl/linkArtifacts/main/oops_age.png' width=125% img>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Challenge: Have ChatGPT Help Us Create a DW Entry for NY Taxi Rides & Analyze it

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src='https://raw.githubusercontent.com/Evogelpohl/linkArtifacts/main/DLT_graphic_tiers.jpg'>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SETUP: Use the Unity Catalog by Default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Switch to our demo catalog
# MAGIC USE CATALOG demos;
# MAGIC
# MAGIC --Switch to our demo schema/database
# MAGIC USE SCHEMA meetup;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # DATA: CSVs for NYC Yellow Taxi Rides:
# MAGIC
# MAGIC <img src='https://raw.githubusercontent.com/Evogelpohl/linkArtifacts/main/nytaxi_schema.png' img>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Let's ask ChatGPT to create a python snippet to read those files into a dataframe. 
# MAGIC #### All we know is the path to our CSV on Azure Blog Storage (like S3):

# COMMAND ----------

nycTaxi_CSVs = 'abfss://data@bpdemostorage01.dfs.core.windows.net/raw_data/yellow_taxi_csv/'

display(dbutils.fs.ls(nycTaxi_CSVs))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # CREATE: Bronze table from df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prompt: 
# MAGIC
# MAGIC I'm using Databricks, in pySpark dialect. 
# MAGIC
# MAGIC I have all NY City Yellow Taxi rides for the past few years as CSVs in a path on azure storage. The abfss:// path is in variable 'nycTaxi_CSVs'
# MAGIC
# MAGIC Each CSV has a header. Create a dataframe for all the records please.
# MAGIC

# COMMAND ----------

# Infer the schema from the first CSV file and create a DataFrame for it
sample_csv = nycTaxi_CSVs + "yellow_tripdata_2022-01.csv"
sample_df = spark.read.format("csv") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .load(sample_csv)

# Create the final DataFrame for all CSV files, using the schema inferred from the sample DataFrame
df = spark.read.format("csv") \
              .option("header", "true") \
              .schema(sample_df.schema) \
              .load(nycTaxi_CSVs)


# COMMAND ----------

# Drop the table if it exists.
sql('DROP TABLE IF EXISTS trips_bronze;')

# Create the bronze table
df.write.saveAsTable('trips_bronze')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # CREATE: Silver. Read into df_cleansed

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Prompt
# MAGIC
# MAGIC I've written the df to a delta table for speed. I now have a new dataframe, df_cleased for future processing.

# COMMAND ----------

display(df.printSchema())

# COMMAND ----------

df_cleansed = spark.table('trips_bronze')
df_cleansed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## TRANSFORM: Clean the dataframe with business-friendly column names

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Prompt #3:
# MAGIC
# MAGIC The schema for our df_cleansed dataframe is as follows. Can you please write the code to apply a new schema that is has business-friendly column names, with proper case and let's use underscores for spaces? We can keep the same df_cleansed name. 
# MAGIC
# MAGIC root
# MAGIC  |-- VendorID: integer (nullable = true)
# MAGIC  |-- tpep_pickup_datetime: timestamp (nullable = true)
# MAGIC  |-- tpep_dropoff_datetime: timestamp (nullable = true)
# MAGIC  |-- passenger_count: double (nullable = true)
# MAGIC  |-- trip_distance: double (nullable = true)
# MAGIC  |-- RatecodeID: double (nullable = true)
# MAGIC  |-- store_and_fwd_flag: string (nullable = true)
# MAGIC  |-- PULocationID: integer (nullable = true)
# MAGIC  |-- DOLocationID: integer (nullable = true)
# MAGIC  |-- payment_type: integer (nullable = true)
# MAGIC  |-- fare_amount: double (nullable = true)
# MAGIC  |-- extra: double (nullable = true)
# MAGIC  |-- mta_tax: double (nullable = true)
# MAGIC  |-- tip_amount: double (nullable = true)
# MAGIC  |-- tolls_amount: double (nullable = true)
# MAGIC  |-- improvement_surcharge: double (nullable = true)
# MAGIC  |-- total_amount: double (nullable = true)
# MAGIC  |-- congestion_surcharge: double (nullable = true)
# MAGIC  |-- airport_fee: double (nullable = true)
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# create a dictionary of old and new column names
new_column_names = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee"
}

# create a list of expressions for the new schema
new_schema_exprs = [col(c).alias(new_column_names.get(c, c)) for c in df_cleansed.columns]

# apply the new schema to the dataframe
df_cleansed = df_cleansed.select(new_schema_exprs)

# show the new schema
df_cleansed.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## TRANSFORM: Apply business rules to our data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Prompt:
# MAGIC
# MAGIC That schema is great. It worked. Next, I've done some analysis and determined that the following business rules we need to use to update the dataframe:
# MAGIC
# MAGIC - when Vendor_id is 1, replace with 'CMTech', when it's 2, replace with 'Verifone'.
# MAGIC - when payment_type is 1, 'credit', when 2, cash, 3, No_Charge, 4, Dispute, 5, Unknown, 6, Voided_Trip
# MAGIC - when Rate_Code_id is 1, Standard_rate, 2, JFK, 3, Newark, 4, Nassau/Westchester, 5, Negotiated_Fare, 6, Group_Ride
# MAGIC
# MAGIC all other columns can stay as they are. Can you write the code to update our df_cleansed dataframe?

# COMMAND ----------

from pyspark.sql.functions import when

# define the mapping for vendor_id
vendor_id_map = {
    1: "CMTech",
    2: "Verifone"
}

# define the mapping for payment_type
payment_type_map = {
    1: "credit",
    2: "cash",
    3: "No_Charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided_Trip"
}

# define the mapping for rate_code_id
rate_code_id_map = {
    1: "Standard_rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau/Westchester",
    5: "Negotiated_Fare",
    6: "Group_Ride"
}

# apply the mappings to the relevant columns
df_cleansed = df_cleansed.withColumn("vendor_id", when(col("vendor_id") == 1, "CMTech").otherwise("Verifone"))
df_cleansed = df_cleansed.withColumn("payment_type", when(col("payment_type") == 1, "credit")
                                     .when(col("payment_type") == 2, "cash")
                                     .when(col("payment_type") == 3, "No_Charge")
                                     .when(col("payment_type") == 4, "Dispute")
                                     .when(col("payment_type") == 5, "Unknown")
                                     .otherwise("Voided_Trip"))
df_cleansed = df_cleansed.withColumn("rate_code_id", when(col("rate_code_id") == 1, "Standard_rate")
                                     .when(col("rate_code_id") == 2, "JFK")
                                     .when(col("rate_code_id") == 3, "Newark")
                                     .when(col("rate_code_id") == 4, "Nassau/Westchester")
                                     .when(col("rate_code_id") == 5, "Negotiated_Fare")
                                     .otherwise("Group_Ride"))

# show the updated dataframe
display(df_cleansed)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src='https://raw.githubusercontent.com/Evogelpohl/linkArtifacts/main/thankyou.png' width=120%>
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## TRANSFORM: Look for bad data

# COMMAND ----------

# This code drops an existing table named trips_silver, if it exists. It then saves the cleaned DataFrame as a new table named trips_silver, and optimizes it for better performance. Finally, it reloads the table as a new DataFrame.

# Drop existing table trips_silver, if it exists
sql('drop table if exists trips_silver;')

# Save cleaned DataFrame as new table trips_silver, and overwrite if it already exists
df_cleansed.write.mode('overwrite').saveAsTable('trips_silver')

# Optimize trips_silver table for better performance
sql('OPTIMIZE trips_silver')

# Reload trips_silver table as new DataFrame
df_cleansed = spark.table('trips_silver')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Prompt:
# MAGIC
# MAGIC Thank you. Our df_cleansed dataframe has business rules applied now, but we might have bad data in our data set. Can you suggest ways of removing them? For example, pickup and dropoff dates should only be in the years 2021 and 2022.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# Filter out bad data by checking if Fare_Amount, Tip_Amount, Tolls_Amount, and Total_Amount are non-negative
df_cleansed = df_cleansed.filter((col("Fare_Amount") >= 0) &
                                 (col("Tip_Amount") >= 0) &
                                 (col("Tolls_Amount") >= 0) &
                                 (col("Total_Amount") >= 0))



# filter out records with invalid pickup and dropoff dates
df_cleansed = df_cleansed.filter(
    (year(col("pickup_datetime")).isin([2021, 2022])) & 
    (year(col("dropoff_datetime")).isin([2021, 2022]))
)


# Print the updated DataFrame
display(df_cleansed.count())

# COMMAND ----------

df_cleansed.write.mode('overwrite').saveAsTable('trips_silver')

sql('OPTIMIZE trips_silver')

df_cleansed = spark.table('trips_silver')

df_cleansed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ANALYZE: Like a boss!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src='https://raw.githubusercontent.com/Evogelpohl/linkArtifacts/main/impress_boss.jpg' width=120%>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Prompt:
# MAGIC
# MAGIC I wrote the df_cleansed dataframe to a delta table. So, I can switch to SQL and impress the boss with my analysis of the taxi data. Can you suggest a few insightful queries showing interesting trends or facts within the dataset? Please use SQL, not Python. My table is called trips_silver and has the following schema.
# MAGIC
# MAGIC trips_silver schema: 
# MAGIC
# MAGIC root
# MAGIC  |-- vendor_id: string (nullable = true)
# MAGIC  |-- pickup_datetime: timestamp (nullable = true)
# MAGIC  |-- dropoff_datetime: timestamp (nullable = true)
# MAGIC  |-- passenger_count: double (nullable = true)
# MAGIC  |-- trip_distance: double (nullable = true)
# MAGIC  |-- rate_code_id: string (nullable = true)
# MAGIC  |-- store_and_fwd_flag: string (nullable = true)
# MAGIC  |-- pickup_location_id: integer (nullable = true)
# MAGIC  |-- dropoff_location_id: integer (nullable = true)
# MAGIC  |-- payment_type: string (nullable = true)
# MAGIC  |-- fare_amount: double (nullable = true)
# MAGIC  |-- extra: double (nullable = true)
# MAGIC  |-- mta_tax: double (nullable = true)
# MAGIC  |-- tip_amount: double (nullable = true)
# MAGIC  |-- tolls_amount: double (nullable = true)
# MAGIC  |-- improvement_surcharge: double (nullable = true)
# MAGIC  |-- total_amount: double (nullable = true)
# MAGIC  |-- congestion_surcharge: double (nullable = true)
# MAGIC  |-- airport_fee: double (nullable = true)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vendor_id, SUM(total_amount) as total_revenue
# MAGIC FROM trips_silver
# MAGIC GROUP BY vendor_id
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src='https://raw.githubusercontent.com/Evogelpohl/linkArtifacts/main/bam.png' width=120%>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Prompt
# MAGIC
# MAGIC Can we write a SQL query to show the average fare_amount per mile (trip_distance) for the top 2 pickup_location_ids for each month-year? I'll use this to build a visual to show a trend. 
# MAGIC
# MAGIC And to be fair for the comparison, how should we normalize trip_distance for the two locations so we remove extreme outliers?

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC WITH pickup_location_counts AS (
# MAGIC   SELECT pickup_location_id, COUNT(*) AS num_trips
# MAGIC   FROM trips_silver
# MAGIC   GROUP BY pickup_location_id
# MAGIC   ORDER BY num_trips DESC
# MAGIC   LIMIT 2
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC pickup_location_fares AS (
# MAGIC   SELECT 
# MAGIC     DATE_TRUNC('month', pickup_datetime) AS pickup_month,
# MAGIC     YEAR(pickup_datetime) AS pickup_year,
# MAGIC     pickup_location_id, 
# MAGIC     fare_amount / trip_distance AS fare_per_mile
# MAGIC   FROM trips_silver
# MAGIC   WHERE pickup_location_id IN (SELECT pickup_location_id FROM pickup_location_counts)
# MAGIC )
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC   CONCAT(pickup_year, '-', LPAD(MONTH(pickup_month), 2, '0')) AS pickup_month_year,
# MAGIC   pickup_location_id, 
# MAGIC   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fare_per_mile) AS median_fare_per_mile
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     pickup_month,
# MAGIC     pickup_year,
# MAGIC     pickup_location_id,
# MAGIC     fare_per_mile,
# MAGIC     PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fare_per_mile) OVER (PARTITION BY pickup_month, pickup_location_id) AS q3,
# MAGIC     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fare_per_mile) OVER (PARTITION BY pickup_month, pickup_location_id) AS q1
# MAGIC   FROM pickup_location_fares
# MAGIC ) filtered_fares
# MAGIC WHERE fare_per_mile BETWEEN q1 - 1.5 * (q3 - q1) AND q3 + 1.5 * (q3 - q1)
# MAGIC GROUP BY pickup_year, MONTH(pickup_month), pickup_location_id
# MAGIC ORDER BY pickup_year, MONTH(pickup_month), pickup_location_id
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prompt 
# MAGIC
# MAGIC I might have embellished my resume a bit. Can you help me with some data science to impress my boss? 
# MAGIC
# MAGIC Can you suggest a good Python module for forecasting data like this? Let's write the Python using df_cleansed and forecast trips for the next 6 months based on hourly data. And can you please document the code so I can learn?
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from fbprophet import Prophet
from pyspark.sql.functions import to_date, hour

# Convert the pickup_datetime column to a date column
df_cleansed = df_cleansed.withColumn('pickup_date', to_date('pickup_datetime'))

# Group data by hour and sum the total_amount column
df_hourly_grouped = df_cleansed.groupBy(date_trunc('hour', 'pickup_datetime').alias('pickup_hour')).sum('total_amount')

# Rename columns to match Prophet's requirements
df_hourly = df_hourly_grouped.selectExpr("pickup_hour as ds", "`sum(total_amount)` as y")

# Initialize Prophet model
model = Prophet()

# Fit the model to the data
model.fit(df_hourly.toPandas())

# Create dataframe with future dates
future = model.make_future_dataframe(periods=6*30*24, freq='H')

# Generate forecast for future dates
forecast_pd = model.predict(future)
forecast = spark.createDataFrame(forecast_pd)

# Print the forecast
forecast.select('ds', 'yhat', 'yhat_lower', 'yhat_upper').show()

