-- Databricks notebook source
--Calculating the mean and standard deviation values for each vehicle_id's sensor reading
CREATE OR REPLACE TEMPORARY VIEW sensor_readings_mean_std AS
SELECT vehicle_id,
    AVG(engine_temp) AS mean_temp,
    STDDEV(engine_temp) AS std_temp,
    AVG(battery_voltage) AS mean_voltage,
    STDDEV(battery_voltage) AS std_voltage,
    AVG(oil_pressure) AS mean_pressure,
    STDDEV(oil_pressure) AS std_pressure,
    AVG(front_left_tire_pressure) AS mean_fl_tire_pressure,
    STDDEV(front_left_tire_pressure) AS std_fl_tire_pressure,
    AVG(front_right_tire_pressure) AS mean_fr_tire_pressure,
    STDDEV(front_right_tire_pressure) AS std_fr_tire_pressure,
    AVG(rear_left_tire_pressure) AS mean_rl_tire_pressure,
    STDDEV(rear_left_tire_pressure) AS std_rl_tire_pressure,
    AVG(rear_right_tire_pressure) AS mean_rr_tire_pressure,
    STDDEV(rear_right_tire_pressure) AS std_rr_tire_pressure
FROM shared.`romina-kirchmaier`.silver_sensor_data
GROUP BY vehicle_id
ORDER BY vehicle_id;

SELECT * FROM sensor_readings_mean_std

-- COMMAND ----------

-- Joining the Tables
CREATE OR REPLACE TEMPORARY VIEW sensor_data_with_avg_std AS
  SELECT a.*, b.* EXCEPT(vehicle_id) FROM sensor_readings_mean_std a
  LEFT JOIN shared.`romina-kirchmaier`.silver_sensor_data b
  ON a.vehicle_id = b.vehicle_id;

SELECT * FROM sensor_data_with_avg_std

-- COMMAND ----------

SELECT timestamp, vehicle_id, front_left_tire_pressure 
FROM sensor_data_with_avg_std
WHERE vehicle_id = 'V00001'

-- COMMAND ----------

CREATE OR REPLACE TABLE shared.`romina-kirchmaier`.gold_sensor_reading_peaks AS
  SELECT vehicle_id, TO_TIMESTAMP(timestamp) AS converted_timestamp,
    CASE WHEN ABS(battery_voltage - mean_voltage) > 3 * std_voltage THEN 1 ELSE 0 END AS peak_voltage,
    CASE WHEN ABS(engine_temp - mean_temp) > 3 * std_temp THEN 1 ELSE 0 END AS peak_temp,
    CASE WHEN oil_pressure > mean_pressure + 3 * std_pressure THEN 1 ELSE 0 END AS peak_pressure,
    CASE WHEN ABS(front_left_tire_pressure - mean_fl_tire_pressure) > 3 * std_fl_tire_pressure THEN 1 ELSE 0 END AS peak_fl_tire_pressure,
    CASE WHEN ABS(front_right_tire_pressure - mean_fr_tire_pressure) > 3 * std_fr_tire_pressure THEN 1 ELSE 0 END AS peak_fr_tire_pressure,
    CASE WHEN ABS(rear_left_tire_pressure - mean_rl_tire_pressure) > 3 * std_rl_tire_pressure THEN 1 ELSE 0 END AS peak_rl_tire_pressure,
    CASE WHEN ABS(rear_right_tire_pressure - mean_rr_tire_pressure) > 3 * std_rr_tire_pressure THEN 1 ELSE 0 END AS peak_rr_tire_pressure
  FROM sensor_data_with_avg_std;

SELECT * FROM shared.`romina-kirchmaier`.gold_sensor_reading_peaks
