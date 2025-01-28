# Databricks notebook source
# MAGIC %md
# MAGIC ##Module 2
# MAGIC #Data Ingestion: Raw Data → Bronze Table Creation
# MAGIC
# MAGIC In this notebook, we're going to be importing the raw data saved to our managed volumes in Step 1 into Bronze Delta tables.
# MAGIC
# MAGIC This spans:
# MAGIC - Sensor Data from Vehicles
# MAGIC - Maintenance Logs
# MAGIC - Environmental Data
# MAGIC - Fleet Metadata
# MAGIC
# MAGIC We'll be importing these as delta tables, and saving them under the `shared` catalog under the `romina` schema, with the `bronze` prefix to indicate they are bronze tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ###**_What are Bronze Tables?_** <br>
# MAGIC Bronze data serves as a storage layer for raw, unprocessed data in tabular format. These tables are designed to closely mirror the original data as received, with no transformations or modifications applied. This approach ensures accurate lineage for troubleshooting and provides a clear view of the data's initial state within the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1: Set Local Variables
# MAGIC We'll begin by specifying some important variables, such as the root path of where our raw data is stored and the target catalog and schema of where we want our bronze tables to be created. 

# COMMAND ----------

# DBTITLE 1,Location of source data and target data

#source root path to the raw data
volume_path = '/Volumes/shared/romina-kirchmaier/fleet_data_raw'

#target path to the data - in metastore
catalog = 'shared'
schema = '`romina-kirchmaier`'
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2: Data Ingestion
# MAGIC Once we've established the source and target locations, we'll move on to actually ingesting the data. Here, we'll be using a loop that iterates over the desired table names and subfolder path names, to avoid repetitive code. Notice that we've included an `if` statement on `Line 21` to account for raw sensor data being stored in `JSON` format, whereas the rest of the raw data is stored in `DELTA` format.
# MAGIC
# MAGIC **Remember:** When saving the table to the metastore in the code below, we don't have to specify a schema nor a catalog as it was already specified at the beginning of this notebook using the `USE` statements. If you ever forget to specify a catalog and schema (by either forgetting to run the `USE SCHEMA` & `USE CATALOG` statements, or by forgetting to include it in the `CATALOG.SCHEMA.TABLE` namespace notation), then the data will be stored within the `MAIN` catalog under the `SHARED` schema.

# COMMAND ----------

# DBTITLE 0,Fleet Environmental Data & Maintenance Log Ingestion | Delta Format
from pyspark.sql.functions import col, max, to_date
from datetime import datetime

# Source path to the fleet metadata historical and new data
fleet_root_path = '/Volumes/shared/romina-kirchmaier/fleet_data_raw/'
table_names = [
    {"table_name": "bronze_environmental_data", "path_name": "raw_environmental_data"},
    {"table_name": "bronze_fleet_maintenance_logs", "path_name": "raw_maintenance_data"},
    {"table_name": "bronze_sensor_data", "path_name": "raw_sensor_data"}, 
    {"table_name": "bronze_fleet_metadata", "path_name": "raw_fleet_metadata"}
]

# Process each table
for table in table_names:
    table_name = table["table_name"]
    path = f"{fleet_root_path}{table['path_name']}"
    

    #Get raw data and read it into a spark dataframe. If the table is sensor data, it is in JSON format, which means we need to create a slightly different ingestion pipeline.
    if table_name == 'bronze_sensor_data':
        new_data_df = spark.read.format("json").option("multiline", "true").load(path)
        print(f"{path}JSON Data Successfully read into Spark Dataframe")
    else:
        new_data_df = spark.read.format("delta").load(path)
        print(f"{path} Delta Data Successfully read into Spark Dataframe")

    #Write the data to a delta table
    new_data_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"{path} Delta Data Successfully written to Delta Table")


print("All Data Processed Succesfully")


# COMMAND ----------

df = spark.read.format("delta").load(f"{fleet_root_path}/raw_environmental_data")
df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3: Data Exploration _(Optional)_
# MAGIC
# MAGIC ####Statistical Data Exploration
# MAGIC Now that the tables have been created, we can use regular `SQL` expressions to explore the freshly ingested raw data, which is now stored in Bronze tables. <br>
# MAGIC
# MAGIC Let's begin by exploring the `bronze_sensor_data` table. We'll be running a `SELECT *` statement, and taking advantage of the **Data Profile** feature to automatically analyze some stats about our data. You can access this by clicking on the `+` icon next to the `TABLE` tab. You can them seamlessly swith between `Table` and `Data Profile` view with the tabs below.
# MAGIC
# MAGIC The **Data Profile** tab has many useful stats about our data, broken down by column, such as:
# MAGIC - The count of values
# MAGIC - Data Type
# MAGIC - The number of null/missing values
# MAGIC - Statistical Calculations such as min, max, median & standard deviation
# MAGIC - Categorical Feature frequency & uniqueness
# MAGIC
# MAGIC The best part: No SQL code needed - these are automatically generated with the click of a button!
# MAGIC

# COMMAND ----------

# DBTITLE 1,Bronze Fleet Metadata
# MAGIC %sql
# MAGIC SELECT * FROM bronze_fleet_metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ####Metadata Exploration
# MAGIC Beyond exploring our actual data above, we can explore the metadata about our table. Let's begin by using the `DESCRIBE` command below.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_sensor_data;

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see, the `DESCRIBE` command above helps us see some metadata about our data, most notably each column's data type and whether we've chosen to write any `COMMENT` for each column (a much recommended optional step!). However, perhaps I want to ensure this data was saved to the right location. For even more metadata, we can use the `DESCRIBE EXTENDED` command.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bronze_sensor_data;

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see above, the **`DESCRIBE EXTENDED`** command helps me understand a lot more about my table. I can now see things like:
# MAGIC - the table's metastore location
# MAGIC - the table's physical location
# MAGIC - the table's owner
# MAGIC - whether the table is managed or external
# MAGIC - which table features are enabled
# MAGIC
# MAGIC This level of visibility helps ensure that nothing gets missed when creating or manipulating tables.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's begin cleansing and manipulating our data, by creating some Silver tables. 