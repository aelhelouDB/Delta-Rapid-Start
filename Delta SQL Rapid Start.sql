-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta SQL Rapidstart
-- MAGIC 
-- MAGIC ## Objectives
-- MAGIC 
-- MAGIC This notebook will discuss and demonstrate many ways of interacting with data and the Delta Lake format using Spark SQL. You will learn how to query various sources of data, how to store that data in Delta, how to manage and interrogate metadata, how to refine data using typical SQL DML commands (MERGE, UPDATE, DELETE), and how to optimize and manage your Delta Lake. 
-- MAGIC 
-- MAGIC By the end of this notebook, you will have performed the following activities and produced your very own Delta Lake:
-- MAGIC 
-- MAGIC 1. Direct queried different data sources using Spark SQL
-- MAGIC 2. Create managed tables (both Delta and non-Delta tables)
-- MAGIC 3. Create, manipulate and explore metadata
-- MAGIC 4. Refine datasets using MERGE, UDPATE and DELETE
-- MAGIC 5. Explore historical versions of data
-- MAGIC 6. Directly query a streaming data set via SQL
-- MAGIC 7. Combine and aggregate datasets to produce refinement layers
-- MAGIC 8. Optimize tables and manage the lake
-- MAGIC 
-- MAGIC ⚠️ Please use DBR 8.* to run this notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC 
-- MAGIC Run the below cell to set up our temporary database.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC databricks_user = spark.sql("SELECT current_user()").collect()[0][0].split('@')[0].replace(".", "_")
-- MAGIC print(databricks_user)
-- MAGIC 
-- MAGIC spark.sql("DROP DATABASE IF EXISTS delta_{}_db CASCADE".format(str(databricks_user)))
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS delta_{}_db".format(str(databricks_user)))
-- MAGIC spark.sql("USE delta_{}_db".format(str(databricks_user)))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Query our Data Lake
-- MAGIC 
-- MAGIC Databricks lets you query files stored in cloud storage like S3/ADLS/Google Storage.
-- MAGIC 
-- MAGIC In this case, we'll be querying files stored in Databricks hosted storage.
-- MAGIC 
-- MAGIC [These files are mounted to your Databricks File System under the /databricks-datasets/ path](https://docs.databricks.com/data/databricks-datasets.html), which makes them easier to reference from notebook code.
-- MAGIC 
-- MAGIC You can find instructions on how to mount your own buckets [here](https://docs.databricks.com/data/databricks-file-system.html#mount-storage).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Magic Commands
-- MAGIC 
-- MAGIC [Magic commands](https://docs.databricks.com/notebooks/notebooks-use.html#mix-languages) are shortcuts that allow for language switching in a notebook
-- MAGIC 
-- MAGIC `%fs` is a shortcut for interacting with [dbutils files sytem utilities](https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utilities). In this case, we are listing out the files under our IOT streaming data directory.
-- MAGIC 
-- MAGIC Notice that the data-device data is broken into many compressed JSON files. We will read these in by specifying their directory, not by specifying individual files.

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-user

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-device

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CSV Data
-- MAGIC 
-- MAGIC CSV data can be direct queried this way, but if your data has headers or you'd like to specify options, you'll need to create a tempoary view instead.

-- COMMAND ----------

SELECT * FROM csv.`/databricks-datasets/iot-stream/data-user/userData.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As you can see, this particular dataset has headers, which means we'll need to create a temporary view in order to properly query this inforamtion. 
-- MAGIC 
-- MAGIC Specifically, we'll need the temporary view in order to specify the header and the schema. Specifying the schema can improve performance by preventing an initial inference scan. However, because this dataset is small, we'll infer the schema.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW userData
USING csv
OPTIONS(
  path '/databricks-datasets/iot-stream/data-user/userData.csv',
  header true,
  inferSchema true
);

SELECT * FROM userData;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JSON Data
-- MAGIC 
-- MAGIC JSON files can also be directly queried much like CSVs. Spark can also read in a full directory of JSON files and allow you to directly query them. We'll use a similar syntax to the CSV direct read, but we'll specify 'JSON' instead for this read. 

-- COMMAND ----------

SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device` 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Tables
-- MAGIC 
-- MAGIC Tables can be registered with the metastore and made available to other users.
-- MAGIC 
-- MAGIC New databases and tables that are registered with the metastore can be found in the Data tab on the left. 
-- MAGIC 
-- MAGIC There are two types of tables that can be created: [managed and unmanaged](https://docs.databricks.com/data/tables.html#managed-and-unmanaged-tables)
-- MAGIC - Managed tables are stored in the Metastore and are enitrely managed by it. Dropping a managed table will delete the underlying files and data.
-- MAGIC - Unmanaged tables are pointers to files and data stored in other locations. Dropping an unmanaged table will delete the metadata and remove the referencable alias for the dataset. However, the underlying data and files will remain undeleted in their orginal location. 
-- MAGIC 
-- MAGIC This rapid start will be creating managed tables. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CSV User Table
-- MAGIC 
-- MAGIC Let's create our first table! Note that in production, it is a best practice to specify a `LOCATION` for the table (making it unmanaged) so that it can be stored in your S3/ADLS bucket. For simplicity, we'll be leaving that out for these exercises.
-- MAGIC 
-- MAGIC Our initial table will be the user_data_raw_csv file. This table will be used 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_raw_csv(
  userid STRING,
  gender STRING,
  age INT,
  height INT,
  weight INT,
  smoker STRING,
  familyhistory STRING,
  chosestlevs STRING,
  bp STRING,
  risk INT)
USING csv
OPTIONS (path "/databricks-datasets/iot-stream/data-user/userData.csv", header "true");

DESCRIBE TABLE user_data_raw_csv

-- COMMAND ----------

SELECT * FROM user_data_raw_csv

-- COMMAND ----------

DROP TABLE user_data_raw_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### JSON Device Table

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-device

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_raw_json(
  calories_burnt DOUBLE,
  device_id INT,
  id STRING,
  miles_walked DOUBLE,
  num_steps INT,
  `timestamp` TIMESTAMP,
  user_id STRING,
  value STRING
)
USING JSON
OPTIONS (path "/databricks-datasets/iot-stream/data-device")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Delta Table from JSON

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_bronze_delta
USING DELTA
PARTITIONED BY (device_id)
AS SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device`

SELECT * FROM device_data_bronze_delta

-- COMMAND ----------

DESCRIBE TABLE device_data_bronze_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Delta Table from CSV Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_bronze_delta 
USING DELTA
PARTITIONED BY (gender)
COMMENT "User Data Raw Table - No Transformations"
AS SELECT * FROM user_data_raw_csv;

SELECT * FROM user_data_bronze_delta

-- COMMAND ----------

DESCRIBE TABLE user_data_bronze_delta 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Add Metadata
-- MAGIC 
-- MAGIC Comments can be added by column or at the table level. Comments are useful metadata that can help other users better understand data definitions or any caveats about using a particular field.
-- MAGIC 
-- MAGIC To add a comment to table or column use the `ALTER TABLE` syntax. [Comments can also be added to tables, columns and partitions when they are created](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-hiveformat.html#create-table-with-hive-format) (see example above).
-- MAGIC 
-- MAGIC Comments will also appear in the UI to further help other users understand the table and columms:

-- COMMAND ----------

ALTER TABLE user_data_bronze_delta ALTER COLUMN bp COMMENT 'Can be High or Normal';
ALTER TABLE user_data_bronze_delta ALTER COLUMN smoker COMMENT 'Can be Y or N';
ALTER TABLE user_data_bronze_delta ALTER COLUMN familyhistory COMMENT 'Can be Y or N';
ALTER TABLE user_data_bronze_delta ALTER COLUMN chosestlevs COMMENT 'Can be High or Normal';
ALTER TABLE user_data_bronze_delta SET TBLPROPERTIES ('comment' = 'User Data Raw Table describing risk levels based on health factors');

-- COMMAND ----------

-- this shows the column schema and comments
DESCRIBE TABLE user_data_bronze_delta

-- COMMAND ----------

-- you can see the table comment in the detailed Table Information at the bottom - under the columns
DESCRIBE TABLE EXTENDED user_data_bronze_delta

-- COMMAND ----------

-- this one has number of files and sizeInBytes
DESCRIBE DETAIL user_data_bronze_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Show Table in Data Tab
-- MAGIC 
-- MAGIC Click on the Data Explorer tab.
-- MAGIC <img src="https://docs.databricks.com/_images/data-icon.png" /></a>
-- MAGIC 
-- MAGIC You can see your `catalog`.`schema`.`table` in the data explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DELETE, UPDATE, UPSERT
-- MAGIC 
-- MAGIC Parquet has traditionally been the primary data storage layer in data lakes. Parquet has some substantial drawbacks.
-- MAGIC 
-- MAGIC Parquet cannot support full SQL DML as Parquet is immuatable and append only. In order to delete a row the whole file must be recreated.
-- MAGIC 
-- MAGIC Delta manages this process and allows for full use of SQL DML on a data lake.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DELETE
-- MAGIC 
-- MAGIC Delta allows for SQL DML deletes, which can allow Delta to support GDPR and CCPA use cases. 
-- MAGIC 
-- MAGIC We have two 25 year olds in or dataset and we have just recieved a right to be forgotten request from user 21

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE age = 25

-- COMMAND ----------

DELETE FROM user_data_bronze_delta where userid = 21;

SELECT * FROM user_data_bronze_delta WHERE age = 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### UPDATE
-- MAGIC 
-- MAGIC Delta also allows for rows to be updated in place using simple SQL DML.

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE userid = 1

-- COMMAND ----------

-- MAGIC %md We have recieved updated information that user 1's chosesterol levels have droped to normal levels

-- COMMAND ----------

UPDATE user_data_bronze_delta SET chosestlevs = 'Normal' WHERE userid = 1;

SELECT * FROM user_data_bronze_delta where userid = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UPSERT / MERGE
-- MAGIC 
-- MAGIC Delta allows for simple upserts using a Merge syntax. Note the initial set of 34 year old users in our data below.

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE age = 34

-- COMMAND ----------

-- MAGIC %md Currently, there are no 80 year old users in our dataset

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE age = 80

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's create the new data, which we will merge into our original data set. User 2 has information that will be updated (They have lost 10 pounds) and a new user (userid = 39), who is 80 and was not previously in the data set, will be added. 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_upsert_values
USING DELTA
AS
SELECT 2 AS userid,  "M" AS gender, 34 AS age, 69 AS height, 140 AS weight, "N" AS smoker, "Y" AS familyhistory, "Normal" AS chosestlevs, "Normal" AS bp, -10 AS risk
UNION
SELECT 39 AS userid,  "M" AS gender, 80 AS age, 72 AS height, 155 AS weight, "N" AS smoker, "Y" AS familyhistory, "Normal" AS chosestlevs, "Normal" AS bp, 10 AS risk;

SELECT * FROM user_data_upsert_values;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's do the merge (upsert) from `user_data_upsert_values` into `user_data_bronze_delta`
-- MAGIC 
-- MAGIC Note that user 2 now has a weight of 140 and we have a new user 39

-- COMMAND ----------

MERGE INTO user_data_bronze_delta as target
USING user_data_upsert_values as source
ON target.userid = source.userid
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *;
  
SELECT * FROM user_data_bronze_delta WHERE age = 34 OR age = 80;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # History
-- MAGIC 
-- MAGIC Delta keeps track of all previous commits to the table. We can see that history, query previous states, and rollback.

-- COMMAND ----------

DESCRIBE HISTORY user_data_bronze_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We query the latest version by default

-- COMMAND ----------

SELECT *
FROM user_data_bronze_delta
WHERE age = 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, let's try querying with the `VERSION AS OF` command. We can see the data we previously deleted. There is also `TIMESTAMP AS OF`:
-- MAGIC ```
-- MAGIC SELECT * FROM table_identifier TIMESTAMP AS OF timestamp_expression
-- MAGIC SELECT * FROM table_identifier VERSION AS OF version
-- MAGIC ```

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta
VERSION AS OF 2
WHERE age = 25

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Restore a Previous Version
-- MAGIC You can restore a Delta table to its earlier state by using the `RESTORE` command
-- MAGIC 
-- MAGIC ⚠️ Databricks Runtime 7.4 and above

-- COMMAND ----------

RESTORE TABLE user_data_bronze_delta TO VERSION AS OF 1;

SELECT *
FROM user_data_bronze_delta
WHERE age = 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Streaming Visualizations
-- MAGIC 
-- MAGIC Save to RAW table (aka "bronze table")
-- MAGIC Raw data is unaltered data that is collected into a data lake, either via bulk upload or through streaming sources.
-- MAGIC 
-- MAGIC The following function reads the Wikipedia IRC channels that has been dumped into our Kafka server.
-- MAGIC 
-- MAGIC The Kafka server acts as a sort of "firehose" and dumps raw data into our data lake.
-- MAGIC 
-- MAGIC Below, the first step is to set up schema. The fields we use further down in the notebook are commented.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def untilStreamIsReady(name):
-- MAGIC   queries = list(filter(lambda query: query.name == name, spark.streams.active))
-- MAGIC 
-- MAGIC   if len(queries) == 0:
-- MAGIC     print("The stream is not active.")
-- MAGIC 
-- MAGIC   else:
-- MAGIC     while (queries[0].isActive and len(queries[0].recentProgress) == 0):
-- MAGIC       pass # wait until there is any type of progress
-- MAGIC 
-- MAGIC     if queries[0].isActive:
-- MAGIC       queries[0].awaitTermination(5)
-- MAGIC       print("The stream is active and ready.")
-- MAGIC     else:
-- MAGIC       print("The stream is not active.")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
-- MAGIC from pyspark.sql.functions import from_json, col
-- MAGIC 
-- MAGIC schema = StructType([
-- MAGIC   StructField("channel", StringType(), True),
-- MAGIC   StructField("comment", StringType(), True),
-- MAGIC   StructField("delta", IntegerType(), True),
-- MAGIC   StructField("flag", StringType(), True),
-- MAGIC   StructField("geocoding", StructType([                 # (OBJECT): Added by the server, field contains IP address geocoding information for anonymous edit.
-- MAGIC     StructField("city", StringType(), True),
-- MAGIC     StructField("country", StringType(), True),
-- MAGIC     StructField("countryCode2", StringType(), True),
-- MAGIC     StructField("countryCode3", StringType(), True),
-- MAGIC     StructField("stateProvince", StringType(), True),
-- MAGIC     StructField("latitude", DoubleType(), True),
-- MAGIC     StructField("longitude", DoubleType(), True),
-- MAGIC   ]), True),
-- MAGIC   StructField("isAnonymous", BooleanType(), True),      # (BOOLEAN): Whether or not the change was made by an anonymous user
-- MAGIC   StructField("isNewPage", BooleanType(), True),
-- MAGIC   StructField("isRobot", BooleanType(), True),
-- MAGIC   StructField("isUnpatrolled", BooleanType(), True),
-- MAGIC   StructField("namespace", StringType(), True),         # (STRING): Page's namespace. See https://en.wikipedia.org/wiki/Wikipedia:Namespace 
-- MAGIC   StructField("page", StringType(), True),              # (STRING): Printable name of the page that was edited
-- MAGIC   StructField("pageURL", StringType(), True),           # (STRING): URL of the page that was edited
-- MAGIC   StructField("timestamp", StringType(), True),         # (STRING): Time the edit occurred, in ISO-8601 format
-- MAGIC   StructField("url", StringType(), True),
-- MAGIC   StructField("user", StringType(), True),              # (STRING): User who made the edit or the IP address associated with the anonymous editor
-- MAGIC   StructField("userURL", StringType(), True),
-- MAGIC   StructField("wikipediaURL", StringType(), True),
-- MAGIC   StructField("wikipedia", StringType(), True),         # (STRING): Short name of the Wikipedia that was edited (e.g., "en" for the English)
-- MAGIC ])
-- MAGIC 
-- MAGIC # start our stream
-- MAGIC (spark.readStream
-- MAGIC   .format("kafka")  
-- MAGIC   .option("kafka.bootstrap.servers", "server1.databricks.training:9092")  # Oregon
-- MAGIC   #.option("kafka.bootstrap.servers", "server2.databricks.training:9092") # Singapore
-- MAGIC   .option("subscribe", "en")
-- MAGIC   .load()
-- MAGIC   .withColumn("json", from_json(col("value").cast("string"), schema))
-- MAGIC   .select(col("timestamp").alias("kafka_timestamp"), col("json.*"))
-- MAGIC   .writeStream
-- MAGIC   .format("delta")
-- MAGIC   .option("checkpointLocation", '/tmp/delta_rapid_start/{}/checkpoint/'.format(str(databricks_user)))
-- MAGIC   .outputMode("append")
-- MAGIC   .queryName("stream_1p")
-- MAGIC   .toTable('wikiIRC')
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC untilStreamIsReady('stream_1p')

-- COMMAND ----------

SELECT * FROM wikiIRC  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Medallion Architecture

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver Tables
-- MAGIC The Silver zone is where data is filtered, cleansed and augmented.  
-- MAGIC 
-- MAGIC **User Data Silver**
-- MAGIC * rename columns to align to company standards
-- MAGIC 
-- MAGIC **Device Data Silver**
-- MAGIC * add a date column derived from the timestamp
-- MAGIC * drop the value column as this is no longer needed
-- MAGIC * create a column for stride length (miles walked / steps)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_silver
USING DELTA
  SELECT 
    userid AS user_id,
    gender,
    age,
    height,
    weight,
    smoker,
    familyhistory AS family_history,
    chosestlevs AS cholest_levs,
    bp AS blood_pressure,
    risk
  FROM user_data_bronze_delta;
  
SELECT * FROM user_data_silver

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_silver
USING DELTA
  SELECT
    id,
    device_id,
    user_id,
    calories_burnt,
    miles_walked,
    num_steps,
    miles_walked/num_steps as stride,
    timestamp,
    DATE(timestamp) as date
  FROM device_data_bronze_delta;
  
SELECT * FROM device_data_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold Tables
-- MAGIC The Gold zone is business level aggregates that are used for analytics and reporting.  Common transformations include joining silver tables together and performing aggregations. 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_daily_averages_gold
USING DELTA
  SELECT
      u.user_id,
      u.gender,
      u.age,
      u.blood_pressure,
      u.cholest_levs,
      AVG(calories_burnt) as avg_calories_burnt,
      AVG(num_steps) as avg_num_steps,
      AVG(miles_walked) as avg_miles_walked
    FROM user_data_silver u
    LEFT JOIN
      (SELECT 
        user_id,
        date,
        MAX(calories_burnt) as calories_burnt,
        MAX(num_steps) as num_steps,
        MAX(miles_walked) as miles_walked
      FROM device_data_silver
      GROUP BY user_id, date) as daily
    ON daily.user_id = u.user_id
    GROUP BY u.user_id,
      u.gender,
      u.age,
      u.blood_pressure,
      u.cholest_levs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Schema Enforcement & Evolution
-- MAGIC **Schema enforcement**, also known as schema validation, is a safeguard in Delta Lake that ensures data quality.  Delta Lake uses schema validation *on write*, which means that all new writes to a table are checked for compatibility with the target table’s schema at write time. If the schema is not compatible, Delta Lake cancels the transaction altogether (no data is written), and raises an exception to let the user know about the mismatch.
-- MAGIC 
-- MAGIC **Schema evolution** is a feature that allows users to easily change a table’s current schema to accommodate data that is changing over time. Most commonly, it’s used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns.
-- MAGIC 
-- MAGIC ### Schema Enforcement
-- MAGIC To determine whether a write to a table is compatible, Delta Lake uses the following rules. The DataFrame to be written:
-- MAGIC * Cannot contain any additional columns that are not present in the target table’s schema. 
-- MAGIC * Cannot have column data types that differ from the column data types in the target table.
-- MAGIC * Can not contain column names that differ only by case.

-- COMMAND ----------

-- You can uncomment the next line to see the error (remove the -- at the beginning of the line)
INSERT INTO TABLE user_data_bronze_delta VALUES ('this is a test')

-- COMMAND ----------

-- You can uncomment the next line to see the error (remove the -- at the beginning of the line)
INSERT INTO user_data_bronze_delta VALUES (39, 'M', 44, 65, 150, 'N','N','Normal','High',20, 'John Doe')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema Evolution

-- COMMAND ----------

-- Let's create a new table
CREATE OR REPLACE TABLE user_data_bronze_delta_new
USING DELTA
SELECT * FROM user_data_bronze_delta

-- COMMAND ----------

-- Set this configuration to allow for schema evolution
SET spark.databricks.delta.schema.autoMerge.enabled = true

-- COMMAND ----------

-- Create new data to append
ALTER TABLE user_data_bronze_delta_new ADD COLUMN (Name string);

UPDATE user_data_bronze_delta_new
SET Name = 'J. Doe',
  userid = userid + 5;

SELECT * FROM user_data_bronze_delta_new

-- COMMAND ----------

-- Name is now in user_data_bronze_delta as well
INSERT INTO user_data_bronze_delta
SELECT * FROM user_data_bronze_delta_new;

SELECT * FROM user_data_bronze_delta

-- COMMAND ----------

-- shows schema history as of previous version
SELECT * FROM user_data_bronze_delta VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Optimize & Z-Ordering
-- MAGIC To improve query speed, Delta Lake on Databricks supports the ability to optimize the layout of data stored in cloud storage. 
-- MAGIC 
-- MAGIC The `OPTIMIZE` command can be used to coalesce small files into larger ones.
-- MAGIC 
-- MAGIC Z-Ordering is a technique to colocate related information in the same set of files to improve query performance by reducing the amount of data that needs to be read.  If you expect a column to be commonly used in query predicates and if that column has high cardinality (that is, a large number of distinct values), then use `ZORDER BY`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimize

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/delta_{}_db.db/device_data_bronze_delta/device_id=1/'.format(str(databricks_user))))

-- COMMAND ----------

SELECT * FROM device_data_bronze_delta
WHERE num_steps > 7000 AND calories_burnt > 500

-- COMMAND ----------

OPTIMIZE device_data_bronze_delta 
ZORDER BY num_steps, calories_burnt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that the metrics show that we removed more files than we added 

-- COMMAND ----------

SELECT * FROM device_data_bronze_delta where num_steps > 7000 AND calories_burnt > 500

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC 
-- MAGIC folder_loc = "/dbfs/user/hive/warehouse/delta_amine_elhelou_db.db/device_data_bronze_delta" # CHANGE ME
-- MAGIC with open(f"{folder_loc}/_delta_log/00000000000000000001.json") as f:
-- MAGIC   for l in f.readlines():
-- MAGIC     print(json.dumps(json.loads(l), indent = 2))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Auto-Optimize
-- MAGIC Auto Optimize is an optional set of features that automatically compact small files during individual writes to a Delta table. Paying a small cost during writes offers significant benefits for tables that are queried actively. Auto Optimize consists of two complementary features: **Optimized Writes** and **Auto Compaction**. More information on Auto Optimze can be found [here](https://docs.microsoft.com/en-us/azure/databricks/delta/optimizations/auto-optimize).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Update Existing Tables

-- COMMAND ----------

ALTER TABLE user_data_bronze_delta
SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL Configuration
-- MAGIC To ensure all new Delta tables have these features enabled, set the SQL configuration

-- COMMAND ----------

SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CLONE
-- MAGIC You can create a copy of an existing Delta table at a specific version using the `clone` command. There are two types of clones:
-- MAGIC * A **deep clone** is a clone that copies the source table data to the clone target in addition to the metadata of the existing table. 
-- MAGIC * A **shallow clone** is a clone that does not copy the data files to the clone target. The table metadata is equivalent to the source. These clones are cheaper to create.
-- MAGIC 
-- MAGIC Any changes made to either deep or shallow clones affect only the clones themselves and not the source table.

-- COMMAND ----------

CREATE TABLE user_data_bronze_delta_clone
SHALLOW CLONE user_data_bronze_delta
VERSION AS OF 1;

SELECT * FROM user_data_bronze_delta_clone;

-- COMMAND ----------

CREATE TABLE user_data_bronze_delta_clone_deep
DEEP CLONE user_data_bronze_delta;

SELECT * FROM user_data_bronze_delta_clone_deep

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Environment Clean Up
-- MAGIC Run the below code to delete the created database and tables
-- MAGIC 
-- MAGIC Please uncomment the last cell to clean things up. You can remove the `#` and run the cell again

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for s in spark.streams.active:
-- MAGIC    s.stop()
-- MAGIC spark.sql("DROP DATABASE IF EXISTS delta_{}_db CASCADE".format(str(databricks_user)))
-- MAGIC dbutils.fs.rm('/tmp/delta_rapid_start/{}/'.format(str(databricks_user)), True)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
