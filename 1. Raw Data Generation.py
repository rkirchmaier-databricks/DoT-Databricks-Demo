# Databricks notebook source
# DBTITLE 1,Importing Libraries
import json
import random
import os
import numpy as np
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Specifying Catalog & Schema Location
spark.sql("USE CATALOG shared")
spark.sql("USE SCHEMA `romina-kirchmaier`")
spark.sql("CREATE VOLUME IF NOT EXISTS fleet_data_raw")

# COMMAND ----------

# DBTITLE 1,Parameters
# Parameters
num_vehicles = 20
days_in_month = 30
measurements_per_day = 8  # hourly measurements
start_date = datetime.today().date() - timedelta(days=32)
date_list = [start_date + timedelta(days=i) for i in range(days_in_month)]
weeks_in_month = 4  # Assuming 4 weeks in a month for simplicity
components = ["Engine", "Tires", "Brakes", "Battery", "Transmission"]
vehicle_ids = [f"V{str(i).zfill(5)}" for i in range(1, num_vehicles + 1)]
weather_conditions = ["Sunny", "Rain", "Cloudy", "Fog", "Snow", "Windy"]
road_conditions = ["Dry", "Wet", "Icy", "Snowy"]
hours_per_day = 24
vehicle_ids = [f"V{str(i).zfill(5)}" for i in range(1, num_vehicles + 1)] # Vehicle IDs
driver_id_list = [f"D_{str(i).zfill(5)}" for i in range(1, num_vehicles + 1)] #Driver ID List
root_volume_directory = "/Volumes/shared/romina-kirchmaier/fleet_data_raw/"  # Path to save data in Databricks volume

#Car Model specific data
make_list = ["Toyota", "Mercedes-Benz", "Audi"]
model_data = {
    "Toyota": {
        "Pilaris": "Van",
        "Ero-200": "Bus",
        "LinkSwift": "Van"
    },
    "Mercedes-Benz": {
        "S900": "Bus",
        "CX-14": "Van",
        "L90w": "Bus"
    },
    "Audi": {
        "A700": "Van",
        "AE24": "Bus",
        "AE26": "Van"
    }
}
fuel_type_list = ["Gasoline", "Diesel", "Electric"]

print(f"Parameters Updated:\n"
      f"Number of Vehicles: {num_vehicles},\n"
      f"Days in Month: {days_in_month},\n"
      f"Measurements per Day: {measurements_per_day},\n"
      f"Start Date: {start_date},\n"
      f"Days To Cover: {', '.join(str(i) for i in date_list)},\n"
      f"Vehicle IDs: {vehicle_ids},\n"
      f"Weather Conditions: {weather_conditions},\n"
      f"Road Conditions: {road_conditions},\n"
      f"Volume Directory: {root_volume_directory}")

# COMMAND ----------

# DBTITLE 1,Functions
import numpy as np

# Function to generate sensor readings for a given vehicle and day
def generate_sensor_data(vehicle_id, start_date, start_time, hours, mileage, fuel_level):
    start_date = datetime.combine(start_date, datetime.min.time())
    data = []
    timestamp = start_date + timedelta(hours=start_time)

    # Baseline ranges
    tire_pressure_range = (30, 35)
    engine_temp_range = (70, 100)
    oil_pressure_range = (20, 60)
    battery_voltage_range = (12, 14)
    mileage_traveled = random.uniform(0, 15)  # Increment mileage per hour

    for hour in range(hours):
        # Generate normal sensor readings
        tire_pressure = {
            "front_left": random.uniform(*tire_pressure_range),
            "front_right": random.uniform(*tire_pressure_range),
            "rear_left": random.uniform(*tire_pressure_range),
            "rear_right": random.uniform(*tire_pressure_range),
        }
        engine_temp = random.uniform(*engine_temp_range)
        oil_pressure = random.uniform(*oil_pressure_range)
        battery_voltage = random.uniform(*battery_voltage_range)
        fuel_level = fuel_level - mileage_traveled
        if max(fuel_level,0) < random.uniform(1,20):
            fuel_level = 100
        mileage += mileage_traveled

        # Randomly inject faults
        if random.random() < 0.05:  # 5% chance of a fault
            fault_type = random.choice(["flat_tire", "overheat", "low_battery", "oil_pressure_spike"])
            if fault_type == "flat_tire":
                tire_to_deflate = random.choice(list(tire_pressure.keys()))
                tire_pressure[tire_to_deflate] = random.uniform(10, 20)  # Flat tire
            elif fault_type == "overheat":
                engine_temp = random.uniform(120, 150)  # Overheating
            elif fault_type == "low_battery":
                battery_voltage = random.uniform(9, 11)  # Low battery
            elif fault_type == "oil_pressure_spike":
                oil_pressure = random.choice([random.uniform(10, 15), random.uniform(70, 100)])  # Spike/drop

        # Append the sensor reading
        data.append({
            "vehicle_id": vehicle_id,
            "timestamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "tire_pressure": tire_pressure,
            "engine_temp": engine_temp,
            "oil_pressure": oil_pressure,
            "fuel_level": fuel_level,
            "battery_voltage": battery_voltage,
            "mileage": mileage,
        })

        # Increment timestamp by 1 hour
        timestamp += timedelta(hours=1)

    return data
  
# Function to generate maintenance logs for one week
def generate_maintenance_logs(vehicle_id, day):
    logs = []
    for _ in range(random.randint(1, 3)):  # 1 to 3 logs per vehicle per week
        log_date = week_start_date + timedelta(days=random.randint(0, 6))
        component = random.choice(components)
        log = {
            "vehicle_id": vehicle_id,
            "date": log_date.strftime('%Y-%m-%d'),
            "type": random.choice(["Planned", "Reactive"]),
            "component_serviced": component,
            "issue_detected": random.choice(
                [
                    f"{component} overheating",
                    f"{component} wear and tear",
                    f"{component} malfunction",
                    "Routine check",
                    "No issues detected"
                ]
            ),
            "action_taken": random.choice(["Repair", "Replacement", "Inspection"]),
            "cost": round(random.uniform(50, 500), 2)  # Cost between $50 and $500
        }
        logs.append(log)
    return logs
  

# Function to generate environmental data for a single day
def generate_environmental_data(date, hours_per_day):
    timestamps = [date + timedelta(hours=i) for i in range(hours_per_day)]
    starting_temp = round(random.uniform(35, 55), 1)  # Temperature in Fahrenheit
    starting_precipitation = abs(np.random.normal(loc=0, scale=3, size=1)[0])  # Precipitation in mm, heavily skewed to 0
    starting_humidity = round(random.uniform(0, 70), 1)
    data = []
    for ts in timestamps:
        hour = {
            "timestamp": ts.strftime('%Y-%m-%d %H:%M:%S'),
                "temperature": starting_temp,
                "highest_wind_speed": round(random.uniform(0, 25), 1),  # Wind speed in km/h,
                "wind_direction": round(random.uniform(0, 360), 1),
                "precipitation": starting_precipitation,
                "humidity": starting_humidity,
                }
        data.append(hour)

        #establishing how much values can deviate hour by hour
        precip_change = np.random.normal(loc=0, scale=3, size=1)[0]
        humidity_change = round(random.uniform(-10, 10), 1)

        #setting new values for the next hour
        starting_temp += (round(random.uniform(-8, 8), 1)) #add anywhere from -8 to 8 degrees every hour.
        if precip_change > starting_precipitation: 
            starting_precipitation = 0
        else: starting_precipitation = starting_precipitation - precip_change
        if humidity_change > starting_humidity:
            starting_humidity = 0
        else: starting_humidity = starting_humidity - humidity_change
        
    print(f"daily values are: {data}")
    return data

#Creating unique car models
def create_car_model(): 
  make = random.choice(make_list)
  model = random.choice(list(model_data[make].keys()))
  vehicle_type = model_data[make][model]

  data = {
      "model": model,
      "make" : make,
      "year" : random.randint(2015, 2021),
      "mileage_cap" : round(random.randint(70000, 150000),-4),
      "vehicle_type": vehicle_type,
      "fuel_type": random.choice(fuel_type_list),
    }

  return data

#Create a unique vehicle to driver combination
def create_vehicles(vehicle_id):
  model = create_car_model()
  vehicle = {
    "vehicle_id": vehicle_id,
    "model": model["model"],
    "make" : model["make"],
    "year" : model["year"],
    "mileage_cap" : model["mileage_cap"],
    "vehicle_type": model["vehicle_type"],
    "fuel_type": model["fuel_type"],
    "current_mileage" : random.randint(0, model["mileage_cap"]),
    "assigned_driver" : driver_assignment[vehicle_id],
    "in_service" : random.choices([True, False], weights=[90, 10], k=1)[0]
  }

  return vehicle

#run through every vehicle ID
def generate_fleet_data(vehicle_ids):
    data = []
    for vehicle_id in vehicle_ids:
        data.append(create_vehicles(vehicle_id))
    return data

# COMMAND ----------

# DBTITLE 1,Logic to see if volume exist, if not, create directories

def check_volume_exists(root_volume_directory):
    try:
        dbutils.fs.ls(root_volume_directory)
        return True
    except:
        return False

def create_directory(directory_path):
    try:
        dbutils.fs.mkdirs(directory_path)
        print(f"Directory created at: {directory_path}")
    except Exception as e:
        print(f"Error creating directory: {e}")

volume_exists = check_volume_exists(root_volume_directory)

create_directory(root_volume_directory)
print(f"Volume exists: {volume_exists}")

# COMMAND ----------

# DBTITLE 0,Generate Sensor Data
#Sensor Data Path
sensor_data_path = f"{root_volume_directory}raw_sensor_data"

# Generate beginning BATCH and save data for each vehicle and each day
if check_volume_exists(sensor_data_path) == False: 
    print(f"Volume is being created.")
    for vehicle_id in vehicle_ids:
        mileage = random.randint(0, 10000)
        fuel_level = random.randint(50, 100)  # Start with 50-100%
        start_time = random.randint(5, 15)
        print(f"Beginning mileage for {vehicle_id} is {mileage}")
        file_count = 0
        for day in date_list:
            daily_readings = generate_sensor_data(vehicle_id, day, start_time, measurements_per_day, mileage, fuel_level)
            mileage = daily_readings[-1]["mileage"]
            fuel_level = daily_readings[-1]["fuel_level"]
            file_count += 1
            total_files = file_count*num_vehicles
            #Save to JSON file in volume
            create_directory(sensor_data_path)
            file_path = f"{sensor_data_path}/{vehicle_id}_{day.strftime('%Y-%m-%d')}.json"
            with open(file_path, "w") as file:  # Standard write, no need for dbfs adjustments
                json.dump(daily_readings, file, indent=4)

    print(f"Generated raw sensor data for {num_vehicles} vehicles over {days_in_month} days in {root_volume_directory}raw_sensor_data spanning {total_files}.")
else:
    print(f"Volume already exists. Skipping volume creation.")

# COMMAND ----------

# DBTITLE 1,Generate maintenance logs
import pandas as pd

#Maintenance Data Path
maintenance_data_path = f"{root_volume_directory}raw_maintenance_data"

if check_volume_exists(maintenance_data_path) == False:  
    # Generate logs for all vehicles over 4 weeks
    maintenance_logs = []
    for vehicle_id in vehicle_ids:  # Reusing the existing vehicle_ids
        for week in range(weeks_in_month):
            week_start_date = start_date + timedelta(weeks=week)
            maintenance_logs.extend(generate_maintenance_logs(vehicle_id, week_start_date))

    # Convert to a Spark DataFrame
    maintenance_logs_df = pd.DataFrame(maintenance_logs)
    spark_maintenance_logs_df = spark.createDataFrame(maintenance_logs_df)
    spark_maintenance_logs_df.show()

    # Save as Delta Table
    spark_maintenance_logs_df.write.format("delta").mode("overwrite").save(maintenance_data_path)

    print("Maintenance logs written to Delta table at:", maintenance_data_path)
else: 
    print(f"Volume already exists. Skipping volume creation.")

# COMMAND ----------

# DBTITLE 1,Genearte environmental data

#Maintenance Data Path
environmental_data_path = f"{root_volume_directory}raw_environmental_data"

if check_volume_exists(environmental_data_path) == False:  
# Generate environmental data for 4 weeks
    environmental_data = []
    for week in range(weeks_in_month):
        for day in range(7):  # 7 days per week
            date = datetime.combine(start_date, datetime.min.time()) + timedelta(weeks=week, days=day)
            environmental_data.extend(generate_environmental_data(date, hours_per_day))

    # Convert to DataFrame and save as a Delta table
    environmental_data_df = pd.DataFrame(environmental_data)

    # Ensure the directory exists
    #dbutils.fs.mkdirs(environmental_data)

    # Save as Delta Table
    spark_environmental_data_df = spark.createDataFrame(environmental_data_df)
    spark_environmental_data_df.write.format("delta").mode("overwrite").save(environmental_data_path)
    print("Environmental data written to Delta table at:", environmental_data_path)
else:
    print(f"Volume already exists. Skipping volume creation.")

# COMMAND ----------

# DBTITLE 1,Generating Fleet Metadata Table

#Maintenance Data Path
fleet_data_path = f"{root_volume_directory}raw_fleet_metadata"

#dictionary of driver to vehicle assignment
driver_assignment = {}
for i in vehicle_ids: 
  driver_assignment[i] = random.choice(driver_id_list)
  driver_id_list.remove(driver_assignment[i])

#Car specific data
# current_mileage needs to be calculated in loop
# assigned_driver needs to be calculated in loop

if check_volume_exists(fleet_data_path) == False:
  fleet_data = generate_fleet_data(vehicle_ids)

  # Convert fleet data to a DataFrame
  fleet_df = spark.createDataFrame(fleet_data)

  # Save as Delta table
  fleet_df.write.format("delta").mode("overwrite").save(fleet_data_path)
  print("Fleet data written to Delta table at:", fleet_data_path)
else:
  print(f"Volume already exists. Skipping volume creation.")