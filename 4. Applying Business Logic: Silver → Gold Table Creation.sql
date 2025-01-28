-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Module 4
-- MAGIC #Applying Business Logic: Silver → Gold Table Creation
-- MAGIC Now that our base silver tables have been created, we can begin refining these even further by creating **Gold Tables**. This is the time to begin considering the business's downstream use of the data and what use cases it will be applied to. Since in this example the purpose of the data is to learn more about fleet breakdowns, lower risk for fleet breakdown, and mitigate repair costs, it makes sense for us to add some additional logic to the data at this point. For example, we may want to:
-- MAGIC - Flag certain sensor readings as `warning` when there is an anomaly in the trend
-- MAGIC - Denominate certain environmental conditions in our `silver_environmental_data` table as low, med, or high risk
-- MAGIC - Classify environmental conditions with tags such as `icy roads`, `windy rain`, `strong winds`, etc.
-- MAGIC
-- MAGIC Let's proceed to tackle these tasks!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###**_What are Gold Tables?_** 
-- MAGIC Gold tables are the highest level of data quality in the Databricks Medallion Architecture. They contain curated, business-level data that is ready for consumption. These tables are typically organized in a way that is optimized for reporting and analytics, often using de-normalized data models with fewer joins. The data in gold tables has undergone final transformations and data quality checks, making it suitable for various business intelligence and analytics projects such as customer analytics, product quality analytics, inventory analytics, and more.

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
-- MAGIC ####Flagging Sensor Readings
-- MAGIC
-- MAGIC Let's begin by exploring some of our sensor readings. In this example, we'll explore a single vehicle's `engine_temp` reading over time.

-- COMMAND ----------

SELECT timestamp, vehicle_id, engine_temp FROM silver_sensor_data
WHERE vehicle_id = (SELECT min(vehicle_id) FROM silver_sensor_data)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As you can see above in the **Visualizations Tab**, it's exactly those peaks we want to flag, because they indicate moments when the engine was overheated. <br>
-- MAGIC We'll use the `mean` and `standard deviation` to create a baseline, and anything that falls outside of that baseline will be flagged as a "peak". This will be done in two steps:
-- MAGIC - Create a temp view which each vehicle's temperature `mean` and `std dev` are calculated.
-- MAGIC - Use those values via a join to determine whether the given reading falls outside accepted values, using the formula: `peak > mean + k*(standard_dev)` where k is our threshold multiple that quantifies how sensitive our detection method is.

-- COMMAND ----------

--Calculating the mean and standard deviation values for each vehicle_id
CREATE OR REPLACE TEMPORARY VIEW engine_temp_readings AS
SELECT VEHICLE_ID,
    AVG(engine_temp) AS mean_temp,
    STDDEV(engine_temp) AS std_temp
FROM silver_sensor_data
GROUP BY vehicle_id
ORDER BY vehicle_id;

SELECT * FROM engine_temp_readings

-- COMMAND ----------

-- Joining the Tables
CREATE OR REPLACE TEMPORARY VIEW silver_sensor_data_with_stats AS
  SELECT a.vehicle_id, a.timestamp, a.engine_temp, b.* EXCEPT(vehicle_id) FROM silver_sensor_data a
  LEFT JOIN engine_temp_readings b
  ON a.vehicle_id = b.vehicle_id;

-- Applying the Logic, where k=3
CREATE OR REPLACE TEMPORARY VIEW sensor_engine_temp_with_flags_view AS
  SELECT *,
    CASE 
        WHEN engine_temp > mean_temp + 2 * std_temp THEN 1
        ELSE NULL 
    END as peak_flag
  FROM silver_sensor_data_with_stats;

-- Let's explore the final table with a visualization to make sure it's properly flagging peaks
SELECT *, CASE WHEN peak_flag = 1 THEN 100 ELSE NULL END as peak_flag_augmented --augmenting the flag amount so it's visible in the visualization
FROM sensor_engine_temp_with_flags_view
where vehicle_id = (SELECT min(vehicle_id) FROM sensor_engine_temp_with_flags_view);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Great - looks like our flag is working properly! Let's proceed to save the engine health view as a gold table below.  

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_sensor_engine_temp_with_flags AS
  SELECT vehicle_id, timestamp, engine_temp, peak_flag FROM sensor_engine_temp_with_flags_view;

SELECT * FROM gold_sensor_engine_temp_with_flags

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Environmental Risk Designation
-- MAGIC
-- MAGIC In the next step, we'll be assigning each hour of the day within our `silver_environmental_data` table an environmental risk designation of `low`, `med`, or `high`, along with a corresponding tag in a `climate_tag` column, based on the following logic:
-- MAGIC - **Windy Rain:** When the wind speed is greater than 20 knots an hour, the precipitation is above 5mm an hour, and the temperature is above 32 degrees, we will assign a `high` risk value and a tag of `windy rain`
-- MAGIC - **Icy Roads:** When the precipitation is above 3 and the temperature is below 25 degrees, we will assign a `high` risk value and a tag of `icy roads`
-- MAGIC - **Snow Storms:** When the precipitation is above 5 and the temperature is below 32 degrees, we will assign a `high` risk value and a tag of `snow storm`
-- MAGIC - **Mild Rain:** When the precipitation is above 0, the wind speed is below 5 knots, and the temperature is above 32 degrees, we will assign a `med` risk value and a tag of `mild rain`.
-- MAGIC - **Windy:** When the highest wind speeds are above 10 knots, we will assign a `med` risk value and a tag of `Windy`.
-- MAGIC - Anything else can be flagged with a risk of `low` and a `NULL` tag.
-- MAGIC
-- MAGIC Let's begin by creating a temporary view called `environmental_data_view`

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW environmental_data_view
AS SELECT *, CASE
    WHEN precipitation > 3 AND temperature < 25 THEN 'icy roads'
    WHEN highest_wind_speed > 20 AND precipitation > 5 AND temperature > 32 THEN 'windy rain'
    WHEN precipitation > 5 AND temperature < 32 THEN 'snow storm'
    WHEN precipitation > 0 AND highest_wind_speed < 0 AND temperature > 32 THEN 'mild rain'
    WHEN highest_wind_speed > 20 THEN 'windy'
    ELSE NULL
  END AS climate_tag,
  CASE
    WHEN precipitation > 3 AND temperature < 25 THEN 'high'
    WHEN highest_wind_speed > 20 AND precipitation > 5 AND temperature > 32 THEN 'high'
    WHEN precipitation > 5 AND temperature < 32 THEN 'high'
    WHEN precipitation > 0 AND highest_wind_speed < 0 AND temperature > 32 THEN 'med'
    WHEN highest_wind_speed > 20 THEN 'med'
    ELSE 'low'
  END AS climate_risk
FROM silver_environmental_data;

SELECT * FROM environmental_data_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The above looks great! Let's save this into a gold table below.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_environmental_data_with_risk AS
SELECT * FROM environmental_data_view;

SELECT * FROM gold_environmental_data_with_risk

-- COMMAND ----------

-- MAGIC %md
-- MAGIC