-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Module 3
-- MAGIC #Data Preparation: Bronze → Silver Table Creation
-- MAGIC In this notebook, we'll begin to clean, parse and transform the bronze tables created in the previous notebook. This will allow us to get a deeper understanding of our data and prepare it for further downstream analytical use. We'll be using `SQL` throughout this notebook, as it lends itself well to data manipulation. Keep in mind - SQL within Databricks still runs atop the SparkSQL engine, which means that all of these queries will take advantage of Spark's distributed processing.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###**_What are Silver Tables?_** 
-- MAGIC Silver table refers to a layer of data that has been cleansed and conformed. Specifically, the Silver layer involves taking raw data from the Bronze layer and performing data cleaning, validation, deduplication, and normalization. This process results in a more refined and structured dataset that can support self-service analytics, ad-hoc reporting, advanced analytics, and machine learning by offering a more consumable format for downstream processing. This layer ensures that the data is matched, merged, and cleansed just enough to be useful for further analysis and projects in the Gold layer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's start by specifying in which directory we'll be working out of. In this case, our source `bronze` data and our target `silver` data will be stored in the same `catalog` and `schema`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #target path to the data - in metastore
-- MAGIC catalog = 'shared'
-- MAGIC schema = '`romina-kirchmaier`'
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {schema}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Step 1: Data Cleansing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Cleaning the Sensor Dataset
-- MAGIC
-- MAGIC Let's begin by exploring the `bronze_sensor_data` table. We'll be running a `SELECT *` statement, and taking advantage of the **Data Profile** feature to automatically analyze some stats about our data. You can swith between `Table` and `Data Profile` view with the tabs below.

-- COMMAND ----------

-- DBTITLE 1,Exploring Fleet Sensor Data
select * from bronze_sensor_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  As you can see, we have some nested data 
-- MAGIC in the `tire_pressure` column, containing the tire pressure of each tire in a nested field.
-- MAGIC  
-- MAGIC The `Data Profile` tab in the cell above shows me that this field is of `STRUCT` type. 
-- MAGIC
-- MAGIC Let's work on getting this data cleaned. 
-- MAGIC **We'll be using a `Temporary View` to make explorable step-by-step modifications to the data without saving each step to the metastore.**
-- MAGIC
-- MAGIC We'll first begin by flattening the nested `tire_pressure` column, followed by rounding the numerical values.
-- MAGIC

-- COMMAND ----------

-- Flattening the tire_pressure column
CREATE OR REPLACE TEMPORARY VIEW flattened_bronze_data as
SELECT *, 
  tire_pressure.front_left as front_left_tire_pressure, 
  tire_pressure.front_right as front_right_tire_pressure, 
  tire_pressure.rear_right as rear_right_tire_pressure, 
  tire_pressure.rear_left as rear_left_tire_pressure
FROM bronze_sensor_data;

-- Rounding numerical values to one decimal point, and dropping nested tire_pressure column
CREATE OR REPLACE TEMPORARY VIEW rounded_flattened_bronze_data AS
SELECT vehicle_id,
       timestamp,
       round(battery_voltage,2) AS battery_voltage,
       round(engine_temp, 2) AS engine_temp,
       round(fuel_level,2) AS fuel_level,
       round(mileage, 2) AS mileage,
       round(oil_pressure, 2) AS oil_pressure,
       round(front_left_tire_pressure, 2) as front_left_tire_pressure,
       round(front_right_tire_pressure,2) as front_right_tire_pressure,
       round(rear_right_tire_pressure,2) as rear_right_tire_pressure,
       round(rear_left_tire_pressure, 2) as rear_left_tire_pressure
FROM flattened_bronze_data;

SELECT * from rounded_flattened_bronze_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Since we're happy with the above results, **let's proceed to save this to a permanent `silver` table inside the metastore.**
-- MAGIC
-- MAGIC Here, we'll be using a standard `CTAS` statement, which will create a _permanent_ table from the latest temporary view we were manipulating above.
-- MAGIC
-- MAGIC **Remember:** When saving the table to the metastore, we don't have to specify a `schema` nor a `catalog` as it was already specified at the beginning of this notebook using the `USE` statements. If you ever forget to specify a `catalog` and `schema` (by either forgetting to run the `USE SCHEMA` & `USE CATALOG` statements, or by forgetting to include it in the `CATALOG.SCHEMA.TABLE` namespace notation), then the data will be stored within the `MAIN` catalog under the `SHARED` schema. 

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_sensor_data
AS SELECT * FROM rounded_flattened_bronze_data;

SELECT * FROM silver_sensor_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Perfect, table has been saved!
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Cleaning the Environmental Data
-- MAGIC
-- MAGIC Let's start by exploring our `bronze_environmental_data` dataset, which contains our raw environmental data.

-- COMMAND ----------

select * from bronze_environmental_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As we can see above, the data would benefit from some minor cleaning. Let's begin by applying the following logic:
-- MAGIC - Rounding temperature to nearest degree
-- MAGIC - Rounding precipitation to nearest mm
-- MAGIC - Rounding humidity to nearest one-tenth of a percentage point

-- COMMAND ----------

CREATE or REPLACE TEMPORARY VIEW rounded_environmental_data AS
SELECT 
  timestamp,
  round(temperature,1) as temperature,
  highest_wind_speed,
  wind_direction,
  round(precipitation, 1) as precipitation,
  round(humidity,1) as humidity
FROM bronze_environmental_data;

SELECT * FROM rounded_environmental_data
  


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Since all looks good, let's proceed to save this as a permanent table, and then let's explore the newly created table using the `DESCRIBE EXTENDED` command.

-- COMMAND ----------

CREATE or REPLACE TABLE silver_environmental_data AS
select * from rounded_environmental_data;

DESCRIBE EXTENDED silver_environmental_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Cleaning the Maintenance Log Data
-- MAGIC
-- MAGIC Let's continue by exploring our `bronze_fleet_maintenance_logs` dataset, which contains our raw maintenance data.

-- COMMAND ----------

SELECT * FROM bronze_fleet_maintenance_logs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The only thing that stands out from this dataset is that cost should always be to the second decimal. Let's fix that, and save it as a silver table.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW clean_fleet_logs AS
SELECT * EXCEPT(COST), CAST(cost AS DECIMAL(10, 2)) AS Cost FROM bronze_fleet_maintenance_logs; --Here we are simultaneously removing the existing column COST and renaming the column with decimal logic applied to 'COST'

CREATE OR REPLACE TABLE silver_fleet_maintenance_logs AS 
SELECT * FROM clean_fleet_logs;

SELECT * FROM Silver_fleet_maintenance_logs

-- COMMAND ----------

-- DBTITLE 0,Examine Bus Schedules
-- MAGIC %md
-- MAGIC ####Cleaning the Fleet Metadata Dataset
-- MAGIC
-- MAGIC Let's round out our data preparation exercise by examining our `bronze_fleet_metadata` dimension dataset.
-- MAGIC

-- COMMAND ----------

SELECT * FROM bronze_fleet_metadata

-- COMMAND ----------

-- MAGIC %md Most of the data looks good as is. However, we have received notice that the `in_service` flag is out of date, as is the `current_mileage` tracker and both should be removed from the data. Let's save it as a silver while removing that column from further use.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_fleet_metadata AS
SELECT * EXCEPT(in_service, current_mileage) FROM bronze_fleet_metadata;

SELECT * FROM silver_fleet_metadata;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Step 2: Data Enriching
-- MAGIC
-- MAGIC Let's continue creating our data preparation by enriching some of our data assets. In particular, we are referring to adding additional context to the data so we may have a holistic view. In this step, we may want to:
-- MAGIC - Join our `silver_sensor_data` fact table with elements from our `silver_fleet_metadata dimension` table to create a unified table `silver_sensor_data_with_metadata` with a more comprehensive view

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_sensor_data_with_metadata AS
  SELECT a.*, 
    b.assigned_driver,
    b.fuel_type,
    b.make,
    b.model,
    b.vehicle_type,
    b.year as vehicle_year
  FROM silver_sensor_data as a
  LEFT JOIN silver_fleet_metadata as b ON 
  a.vehicle_id = b.vehicle_id;

SELECT * FROM silver_sensor_data_with_metadata;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We are now complete with our silver table creation. Let's move on to creating Gold Tables!