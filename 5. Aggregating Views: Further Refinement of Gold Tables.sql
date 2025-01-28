-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Module 5
-- MAGIC #Aggregating Views: Further Refinement of Gold Tables
-- MAGIC _Congrats!_ We've reached the last and final step in many data pipelines - data aggregation and consolidation. This is where we'll want to refine and consolidate our data, usually by continuing to denormalize and group the data to paint a specific picture. In most scenarios, the datasets we create here will be used to feed specific dashboards and generate reports to executives.
-- MAGIC
-- MAGIC At this point, we should only manipulate our data to answer specific business question, such as:
-- MAGIC - What make and model of bus is most likely to break down?
-- MAGIC -  Which extreme climate condition causes the most abnormal engine sensor readings?
-- MAGIC - Is there a correlation between miles traveled and numbers of repairs in the event log?
-- MAGIC
-- MAGIC Let's proceed to answer these questions!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC #####Remind me again...
-- MAGIC ###**_What are Gold Tables?_** 
-- MAGIC Gold tables are the highest level of data quality in the Databricks Medallion Architecture. They contain curated, business-level data that is ready for consumption. These tables are typically organized in a way that is optimized for reporting and analytics, often using de-normalized data models with fewer joins. The data in gold tables has undergone final transformations and data quality checks, making it suitable for various business intelligence and analytics projects such as customer analytics, product quality analytics, inventory analytics, and more.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As in previous notebooks, start by specifying in which directory we'll be working out of. In this case, our source `bronze` data and our target `silver` data will be stored in the same `catalog` and `schema`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #target path to the data - in metastore
-- MAGIC catalog = 'shared'
-- MAGIC schema = '`romina-kirchmaier`'
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {schema}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Question #1: Which make and model of bus is most likely to break down?
-- MAGIC
-- MAGIC Let's put some pieces together. We know we'll need to use our `silver_fleet_metadata` which contains columns such as car `make`, `model` and join it with our `silver_fleet_maintenance_logs` to understand how often each make and model is likely to break down. We will only want to take a look at `Reactive` types of logs, meaning that the bus maintenance repair was not planned and it was indeed a breakdown.
-- MAGIC
-- MAGIC Let's begin by joining these datasets into a temporary view.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW maintenance_log_by_make AS
  SELECT a.*, b.* EXCEPT(vehicle_id) 
  FROM silver_fleet_metadata as a
  LEFT JOIN silver_fleet_maintenance_logs as b
  ON a.vehicle_id = b.vehicle_id
  WHERE type = 'Reactive';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Great! Now, we need a simplified way of looking at how many entries there are per make-model combination. 

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_breakdowns_by_make_model AS
  SELECT count(*) as breakdown_count, CONCAT(make,' ', model) AS vehicle_make_model
  FROM maintenance_log_by_make
  GROUP BY vehicle_make_model
  ORDER BY breakdown_count DESC;

SELECT * FROM gold_breakdowns_by_make_model;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Awesome! 
-- MAGIC
-- MAGIC **Observe that we created a gold table `gold_breakdowns_by_make_models`, aimed at answering a specific business need.** We were able to deduce that Audi AE24s break down at the highest rate, and we may want to recommend to the business that we do further analysis and/or pause the purchase of this model. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Question #2: Which extreme climate condition causes the most abnormal engine sensor readings?
-- MAGIC
-- MAGIC To tackle this question, we'll need to use our `gold_environmental_data_with_risk` table which contains hourly environmental data with climate tags, and join it with the `gold_sensor_engine_health` dataset which contains every vehicle's sensor readings and  abnormal temperature flags, located in the `temp_warning` column which we calculated in the previous notebook. 
-- MAGIC
-- MAGIC Let's begin by joining these datasets on the `timestamp` values into a `TEMPORARY VIEW`. We will then use that view to group our results into an aggregated view. We will then visualize our output and save the temporary view as a permanent table in the metastore.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW engine_temp_with_environmental_data AS
  SELECT  a.timestamp,
          a.climate_tag,
          a.climate_risk,
          b.vehicle_id,
          b.peak_flag
  FROM gold_environmental_data_with_risk as a
  INNER JOIN gold_sensor_engine_temp_with_flags as b
  ON a.timestamp = b.timestamp
  WHERE b.peak_flag = '1';

  SELECT * FROM engine_temp_with_environmental_data; 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW engine_abnormalities_by_climate AS
  SELECT climate_tag, count(*) as num_of_abnormalities
  FROM engine_temp_with_environmental_data
  WHERE climate_tag is not NULL
  GROUP BY climate_tag
  ORDER BY count(*) DESC;

SELECT * FROM engine_abnormalities_by_climate;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_engine_abnormalities_by_climate AS
  SELECT * FROM engine_abnormalities_by_climate;

SELECT * FROM engine_abnormalities_by_climate;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Great! We were able to deduce that `Snow Storm` and `Windy Rain` caused the most engine abnormalities out of all other extreme weather climate tags, and saved our table output.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Question #3: Is there a correlation between miles traveled and numbers of repairs in the event log?
-- MAGIC
-- MAGIC To answer this question, we'll to combine the `silver_fleet_maintenance_logs` dataset which contains repair log information, with the `silver_sensor_data` table that contains mileage information for each vehicle.
-- MAGIC
-- MAGIC Let's assume that any log where `action = repair` OR `action = replacement` count as "repairs".
-- MAGIC
-- MAGIC For this exercise, we'll take the following steps:
-- MAGIC - Create a temporary view that aggregates the numbers of repairs by vehicle using the `silver_fleet_maintenance_logs`
-- MAGIC - Create another temporary view that finds the total miles traveled by vehicle using the `silver_sensor_data`
-- MAGIC - Create a temporary view that joins both datasets using `vehicle_id` 
-- MAGIC - Visualize the output into a scatter plot to explore the correlation
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- Creating a temporary view that aggregates the numbers of repairs by vehicle using the silver_fleet_maintenance_logs

CREATE OR REPLACE TEMPORARY VIEW repairs_by_vehicles AS
  SELECT vehicle_id, count(*) as num_of_repairs
  FROM silver_fleet_maintenance_logs
  WHERE action_taken in ("Repair", "Replacement")
  GROUP BY vehicle_id
  ORDER BY vehiclE_id;

SELECT * FROM repairs_by_vehicles;

-- COMMAND ----------

--Creating another temporary view that finds the total miles traveled by vehicle using the silver_sensor_data

CREATE OR REPLACE TEMPORARY VIEW mileage_diff_by_vehicle AS 
  SELECT vehicle_id, min(mileage) as min_mileage, max(mileage) as max_mileage, max_mileage - min_mileage as mileage_diff
  FROM silver_sensor_data
  GROUP BY vehicle_id
  ORDER BY vehicle_id;

SELECT * FROM mileage_diff_by_vehicle;

-- COMMAND ----------

-- Create a temporary view that joins both datasets using `vehicle_id` 

CREATE OR REPLACE TEMPORARY VIEW vehicle_logs_by_mile_traveled AS
  SELECT a.vehicle_id,
         a.num_of_repairs,
         CAST (b.mileage_diff as INT)
  FROM repairs_by_vehicles as a
  INNER JOIN mileage_diff_by_vehicle as b
  ON a.vehicle_id = b.vehicle_id
  ORDER BY vehicle_id;

SELECT * FROM vehicle_logs_by_mile_traveled;

-- COMMAND ----------

--Visualizing Results

SELECT * FROM vehicle_logs_by_mile_traveled;

-- COMMAND ----------

-- Saving output table to metastore

CREATE OR REPLACE TEMPORARY VIEW gold_vehicle_logs_by_mile_traveled AS 
  SELECT * FROM vehicle_logs_by_mile_traveled

-- COMMAND ----------

-- MAGIC %md
-- MAGIC While we weren't able to find a correlation (and understandably so, as this is all generated data), we were able to come to a conclusion: **there is no correlation between miles traveled and number of repairs**. 